# HU-01 a HU-06: orquestador principal Gemini con function calling + RAG
# Registra automáticamente I1, I2, I3 en logs_ejecucion al finalizar
import asyncio
import logging
from datetime import datetime, timezone
import time
import re
from typing import Any

from supabase import Client

from services.anonimizacion import AnonimizacionService
from services.rag_service import RAGService
from config import settings

logger = logging.getLogger(__name__)

MAX_REINTENTOS   = 3
UMBRAL_TIEMPO_MS = 60_000   # warning si supera 60 segundos

# ── Variables globales para Rate Limiting ────────────────────────────────────
_gemini_api_lock = asyncio.Lock()
_ultimo_llamado_api: float = 0.0

# ── Function declarations para Gemini function calling ───────────────────────
# Formato esperado por el SDK: tools=[{"function_declarations": FUNCIONES_DISPONIBLES}]

FUNCIONES_DISPONIBLES: list[dict] = [
    {
        "name": "generar_campana_completa",
        "description": (
            "Determina criterios exactos de segmentación, volumen estimado, "
            "y genera el texto completo del mensaje personalizado para la campaña."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "dimension": {
                    "type": "string",
                    "description": "Dimensión del ciclo de vida: onboarding | fidelizacion | reactivacion",
                },
                "criterios": {
                    "type": "object",
                    "description": (
                        "Criterios de segmentación como pares campo-condición. "
                        "Ejemplo: {\"score_crediticio\": {\"gte\": 600}}"
                    ),
                },
                "tamanio_audiencia": {
                    "type": "integer",
                    "description": "Volumen estimado de clientes en el segmento",
                },
                "tipo_campana": {
                    "type": "string",
                    "description": "Tipo específico de campaña",
                },
                "tono": {
                    "type": "string",
                    "description": "Tono del mensaje",
                    "enum": ["formal", "cercano", "urgente"],
                },
                "mensaje": {
                    "type": "string",
                    "description": "Texto completo del mensaje personalizado para el cliente",
                },
                "canal_optimo": {
                    "type": "object",
                    "description": "Determina el mejor canal de comunicación para el segmento basándose en el comportamiento de los clientes",
                    "properties": {
                        "canal_principal": {
                            "type": "string",
                            "enum": ["email", "sms", "push", "in_app", "whatsapp"],
                            "description": "Canal con mayor probabilidad de éxito"
                        },
                        "canales_alternativos": {
                            "type": "array",
                            "items": {
                                "type": "string",
                                "enum": ["email", "sms", "push", "in_app", "whatsapp"]
                            },
                            "description": "Canales secundarios ordenados por efectividad"
                        },
                        "justificacion": {
                            "type": "string",
                            "description": "Razón de la recomendación basada en el perfil del segmento"
                        },
                        "horario_optimo": {
                            "type": "string",
                            "description": "Franja horaria sugerida (ej: '09:00-12:00 días laborables')"
                        },
                        "score_confianza": {
                            "type": "integer",
                            "description": "Confianza de la recomendación entre 0 y 100"
                        }
                    },
                    "required": ["canal_principal", "justificacion", "score_confianza"]
                }
            },
            "required": ["dimension", "criterios", "tamanio_audiencia", "tipo_campana", "tono", "mensaje", "canal_optimo"],
        },
    }
]

_PROMPT_TEMPLATE = """\
Eres un asistente experto en marketing bancario peruano.
Fecha actual del sistema: {fecha_actual}

INSTRUCCIÓN DEL ANALISTA:
{instruccion}

TIPO DE CAMPAÑA:
{tipo_campana}

CONTEXTO DE CLIENTES (datos anonimizados):
{contexto}

REGLAS CRÍTICAS (PENALIZACIÓN SI NO SE CUMPLEN):
1. DEBES completar TODOS los campos de la función generar_campana_completa.
2. El campo `mensaje` NO PUEDE ESTAR VACÍO. Debes redactar un mensaje completo y persuasivo (ej. "¡Hola! ...").
3. El campo `criterios` NO PUEDE ESTAR VACÍO y DEBE USAR ÚNICAMENTE las siguientes llaves válidas: ["fecha_apertura_cuenta", "fecha_ultima_transaccion", "canal_principal", "productos_activos", "score_crediticio", "operaciones_ultimo_mes"]. Si usas "productos_activos", los únicos valores válidos son "cuenta_ahorro" o "tarjeta_credito". Ejemplo válido: {{"operaciones_ultimo_mes": {{"lte": 0}}}}. NUNCA inventes nombres de campos.
4. Para rangos (fechas o números), usa SIEMPRE objetos anidados con "gte", "lte", "eq". NUNCA uses strings con dos puntos como "2026-01-01:2026-01-31". Ejemplo de fecha correcto: {{"fecha_apertura_cuenta": {{"gte": "2026-04-01", "lte": "2026-04-30"}}}}.
5. Responde SIEMPRE usando la función generar_campana_completa con los datos llenos. No respondas en texto plano.
6. Responde siempre en español.
7. Para la recomendación de `canal_optimo` considera:
   - El `canal_principal` histórico de los clientes en el contexto.
   - La frecuencia de operaciones digitales.
   - El tipo de campaña (onboarding/fidelización/reactivacion) y urgencia.\
"""


class OrquestadorGemini:

    def __init__(
        self,
        modelo,
        db: Client,
        anonimizador: AnonimizacionService,
        rag: RAGService,
    ) -> None:
        self._model      = modelo
        self._db         = db
        self._anonimizador = anonimizador
        self._rag        = rag

    # ── Punto de entrada principal ────────────────────────────────────────────

    async def procesar_instruccion(
        self,
        instruccion: str,
        dimension: str,
        registros: list[dict],
        usuario_id: str,
        campana_id: str | None = None,
        tipo_campana: str = "general",
    ) -> dict:
        """
        Flujo completo:
          a) timestamp_instruccion
          b) Anonimizar lote → I2
          c) RAG: recuperar contexto relevante
          d) Construir prompt
          e) Llamar Gemini con function calling (hasta 3 reintentos)
          f) timestamp_respuesta → I1
          g) Procesar function calls → segmento + mensaje
          h) Calcular precisión de segmentación → I3
          i) Registrar log con I1, I2, I3
          j) Retornar resultado estructurado
        """
        # a) Inicio
        ts_instruccion = datetime.now(timezone.utc)

        # b) Anonimización — si falla, bloquear el flujo
        try:
            resultado_anon = self._anonimizador.verificar_lote(registros)
        except Exception as exc:
            logger.error("Error en anonimización — flujo bloqueado: %s", exc)
            return {"error": "anonimizacion_fallida", "detalle": str(exc)}

        registros_limpios    = resultado_anon["registros_limpios"]
        tasa_anonimizacion   = resultado_anon["tasa_anonimizacion"]
        registros_con_pii    = resultado_anon["registros_con_pii"]
        total_procesados     = resultado_anon["total_procesados"]

        logger.info(
            "Anonimización: %d/%d registros limpios — I2=%.2f%%",
            total_procesados - registros_con_pii, total_procesados, tasa_anonimizacion,
        )

        # c) RAG
        clientes_relevantes = await self._rag.recuperar_contexto(instruccion, dimension)
        contexto            = self._rag.construir_contexto_prompt(clientes_relevantes)

        # d) Prompt
        fecha_actual = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        prompt = _PROMPT_TEMPLATE.format(
            instruccion=instruccion, 
            contexto=contexto, 
            tipo_campana=tipo_campana.upper(),
            fecha_actual=fecha_actual
        )

        # e) Llamada a Gemini con reintentos
        try:
            respuesta_gemini = await self._llamar_gemini_con_reintentos(prompt)
        except Exception as exc:
            logger.error("Gemini falló tras %d reintentos: %s", MAX_REINTENTOS, exc)
            return {"error": "gemini_no_disponible", "detalle": str(exc)}

        # f) Fin — calcular I1
        ts_respuesta       = datetime.now(timezone.utc)
        tiempo_respuesta_ms = int((ts_respuesta - ts_instruccion).total_seconds() * 1000)

        if tiempo_respuesta_ms > UMBRAL_TIEMPO_MS:
            logger.warning(
                "I1=%d ms supera umbral de %d ms", tiempo_respuesta_ms, UMBRAL_TIEMPO_MS
            )

        # g) Procesar function calling
        fc_resultado = self._procesar_respuesta_fc(respuesta_gemini)

        criterios         = fc_resultado.get("criterios", {})
        tamanio_audiencia = fc_resultado.get("tamanio_audiencia", 0)
        mensaje           = fc_resultado.get("mensaje", "")
        tipo_campana      = fc_resultado.get("tipo_campana", dimension)
        tono              = fc_resultado.get("tono", "formal")
        canal_optimo      = fc_resultado.get("canal_optimo", None)

        # h) Calcular I3
        precision_segmento = self._calcular_precision_segmento(criterios, registros_limpios)
        clientes_segmentados = int(len(registros_limpios) * precision_segmento / 100)

        logger.info(
            "I1=%dms | I2=%.2f%% | I3=%.2f%%",
            tiempo_respuesta_ms, tasa_anonimizacion, precision_segmento,
        )

        # i) Registrar log
        if campana_id:
            await self._registrar_log(
                campana_id         = campana_id,
                ts_instruccion     = ts_instruccion,
                ts_respuesta       = ts_respuesta,
                tiempo_ms          = tiempo_respuesta_ms,
                tasa_anon          = tasa_anonimizacion,
                precision          = precision_segmento,
                total_procesados   = total_procesados,
                registros_anon     = registros_con_pii,
                clientes_segmentados = clientes_segmentados,
            )

        # j) Resultado
        return {
            "segmento":         criterios,
            "tamanio_audiencia": clientes_segmentados,
            "mensaje":          mensaje,
            "tipo_campana":     tipo_campana,
            "tono":             tono,
            "canal_optimo":     canal_optimo,
            "metricas": {
                "tiempo_respuesta_ms": tiempo_respuesta_ms,
                "tasa_anonimizacion":  tasa_anonimizacion,
                "precision_segmento":  precision_segmento,
            },
        }

    # ── Gemini con reintentos ─────────────────────────────────────────────────

    async def _llamar_gemini_con_reintentos(self, prompt: str):
        global _ultimo_llamado_api
        ultimo_error: Exception | None = None
        
        # Calcular el tiempo mínimo entre requests
        rpm = getattr(settings, "gemini_rpm_limit", 15)
        segundos_entre_requests = 60.0 / max(1, rpm)

        for intento in range(MAX_REINTENTOS):
            try:
                # ── Aplicar Rate Limiting global (Throttle) ──
                async with _gemini_api_lock:
                    ahora = time.time()
                    tiempo_transcurrido = ahora - _ultimo_llamado_api
                    if tiempo_transcurrido < segundos_entre_requests:
                        tiempo_espera = segundos_entre_requests - tiempo_transcurrido
                        logger.info("Rate limit (RPM=%d): Esperando %.2fs antes de Gemini", rpm, tiempo_espera)
                        await asyncio.sleep(tiempo_espera)
                    
                    _ultimo_llamado_api = time.time()
                    
                    # Ejecutar en thread pool para no bloquear el loop, y deshabilitar
                    # los reintentos internos del SDK para poder atrapar los 429 rápido.
                    respuesta = await asyncio.to_thread(
                        self._model.generate_content,
                        prompt,
                        tools=[{"function_declarations": FUNCIONES_DISPONIBLES}],
                        request_options={"retry": None}
                    )
                return respuesta
            except Exception as exc:
                ultimo_error = exc
                error_str = str(exc).lower()
                logger.warning(
                    "Gemini intento %d/%d falló: %s", intento + 1, MAX_REINTENTOS, exc
                )
                
                # Si la API explicitly nos arroja un 429 Quota Exceeded, penalizamos más fuerte
                if "429" in error_str or "quota exceeded" in error_str:
                    espera_429 = 20.0 * (2 ** intento) # default backup
                    
                    # Extraer el tiempo sugerido por Gemini: "Please retry in 42.51s"
                    match = re.search(r"retry in (\d+\.?\d*)s", error_str)
                    if match:
                        espera_429 = float(match.group(1)) + 1.0
                    
                    # Si la espera es muy larga, abortamos rápido para no dejar al usuario "pensando" en frontend
                    if espera_429 > 30.0:
                        logger.error("Cuota de Gemini excedida. Espera requerida (%.1fs) es muy alta. Abortando.", espera_429)
                        raise Exception(f"Límite de cuota alcanzado (Google Gemini). Por favor, intenta de nuevo en {int(espera_429)} segundos.")
                        
                    logger.warning("Error 429 (Quota Exceeded) detectado. Forzando cooldown de %.1fs", espera_429)
                    await asyncio.sleep(espera_429)
                elif intento < MAX_REINTENTOS - 1:
                    await asyncio.sleep(2 ** intento)
                    
        raise ultimo_error  # type: ignore[misc]

    # ── Procesar function calling ─────────────────────────────────────────────

    def _procesar_respuesta_fc(self, respuesta) -> dict:
        """
        Itera sobre respuesta.candidates[0].content.parts.
        Por cada part con function_call extrae y convierte los args.
        Acumula los resultados de segmentar_clientes y generar_mensaje_campana.
        """
        resultado: dict = {}
        try:
            partes = respuesta.candidates[0].content.parts
        except (IndexError, AttributeError) as exc:
            logger.error("Respuesta de Gemini sin candidates válidos: %s", exc)
            return resultado

        for parte in partes:
            try:
                fn = parte.function_call
                if not fn.name:
                    continue
                args = _proto_to_python(fn.args)

                if fn.name in ["generar_campana_completa", "segmentar_clientes", "generar_mensaje_campana"]:
                    if "criterios" in args:
                        resultado["criterios"]         = dict(args.get("criterios") or {})
                    if "tamanio_audiencia" in args:
                        resultado["tamanio_audiencia"] = int(args.get("tamanio_audiencia") or 0)
                    if "dimension" in args:
                        resultado["dimension"]         = str(args.get("dimension") or "")
                    if "mensaje" in args:
                        resultado["mensaje"]           = str(args.get("mensaje") or "")
                    if "tipo_campana" in args:
                        resultado["tipo_campana"]      = str(args.get("tipo_campana") or "")
                    if "tono" in args:
                        resultado["tono"]              = str(args.get("tono") or "formal")
                    if "canal_optimo" in args:
                        canal_data = dict(args.get("canal_optimo") or {})
                        if "score_confianza" in canal_data and canal_data["score_confianza"] is not None:
                            try:
                                canal_data["score_confianza"] = int(float(canal_data["score_confianza"]))
                            except (ValueError, TypeError):
                                canal_data["score_confianza"] = 80
                        resultado["canal_optimo"] = canal_data

            except Exception as exc:
                logger.warning("Error procesando part de function call: %s", exc)

        return resultado

    # ── Precisión de segmentación (I3) ────────────────────────────────────────

    def _calcular_precision_segmento(
        self, criterios: dict, registros: list[dict]
    ) -> float:
        """
        I3: PS = (clientes_que_cumplen_criterios / total_segmento) × 100
        Soporta operadores: eq, gt, gte, lt, lte, in, not_in
        """
        if not registros:
            return 0.0
        if not criterios:
            return 100.0

        correctos = sum(
            1 for r in registros if self._cumple_todos_criterios(r, criterios)
        )
        return round((correctos / len(registros)) * 100, 2)

    def _cumple_todos_criterios(self, registro: dict, criterios: dict) -> bool:
        for campo, condicion in criterios.items():
            valor = registro.get(campo)
            if valor is None:
                return False
            if not self._evaluar_condicion(valor, condicion):
                return False
        return True

    def _evaluar_condicion(self, valor: Any, condicion: Any) -> bool:
        if not isinstance(condicion, dict):
            if isinstance(valor, list):
                return condicion in valor
            return valor == condicion
        for op, ref in condicion.items():
            if not self._evaluar_op(valor, op, ref):
                return False
        return True

    @staticmethod
    def _evaluar_op(valor: Any, op: str, ref: Any) -> bool:
        try:
            # Manejo especial si el campo de la BD es una lista (ej: productos_activos)
            if isinstance(valor, list):
                if op == "eq": 
                    return ref in valor
                if op == "in": 
                    ref_list = ref if isinstance(ref, list) else [ref]
                    return any(r in valor for r in ref_list)
                if op == "not_in": 
                    ref_list = ref if isinstance(ref, list) else [ref]
                    return not any(r in valor for r in ref_list)
                return False

            match op:
                case "eq":     return valor == ref
                case "gt":     return valor > ref
                case "gte":    return valor >= ref
                case "lt":     return valor < ref
                case "lte":    return valor <= ref
                case "in":     return valor in ref
                case "not_in": return valor not in ref
                case _:
                    logger.warning("Operador de criterio desconocido: '%s' — ignorado", op)
                    return True
        except (TypeError, ValueError):
            return False

    # ── Registro de log ───────────────────────────────────────────────────────

    async def _registrar_log(
        self,
        campana_id: str,
        ts_instruccion: datetime,
        ts_respuesta: datetime,
        tiempo_ms: int,
        tasa_anon: float,
        precision: float,
        total_procesados: int,
        registros_anon: int,
        clientes_segmentados: int,
    ) -> None:
        """Inserta fila en logs_ejecucion con I1, I2, I3."""
        payload = {
            "campana_id":             campana_id,
            "timestamp_instruccion":  ts_instruccion.isoformat(),
            "timestamp_respuesta":    ts_respuesta.isoformat(),
            "tiempo_respuesta_ms":    tiempo_ms,
            "registros_procesados":   total_procesados,
            "registros_anonimizados": registros_anon,
            "tasa_anonimizacion":     float(tasa_anon),
            "clientes_segmentados":   clientes_segmentados,
            "precision_segmento":     float(precision),
        }
        try:
            await asyncio.to_thread(
                lambda: self._db.table("logs_ejecucion").insert(payload).execute()
            )
        except Exception as exc:
            logger.error("Error registrando log en logs_ejecucion: %s", exc)


# ── Utilidad proto → Python ───────────────────────────────────────────────────

def _proto_to_python(obj: Any) -> Any:
    """Convierte recursivamente MapComposite/ListComposite de protobuf a tipos Python."""
    if hasattr(obj, "items"):
        return {k: _proto_to_python(v) for k, v in obj.items()}
    if hasattr(obj, "__iter__") and not isinstance(obj, (str, bytes)):
        return [_proto_to_python(v) for v in obj]
    return obj
