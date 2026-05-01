# HU-05: detección automática de clientes inactivos ≥ 30 días
# HU-06: mensajes de reactivación diferenciados por perfil de inactividad
# Invocado manualmente vía POST /reactivacion/ejecutar-deteccion
# (APScheduler nocturno se configura en Sprint 4 / main.py)
import logging
from datetime import datetime, timezone, timedelta
from typing import Any

from supabase import Client

from config import settings

logger = logging.getLogger(__name__)

# Umbrales de días de inactividad por perfil
_UMBRAL_RECIENTE_MIN    = 30   # inclusive
_UMBRAL_RECIENTE_MAX    = 45   # inclusive
_UMBRAL_MODERADA_MIN    = 46   # inclusive
_UMBRAL_MODERADA_MAX    = 90   # inclusive
_UMBRAL_PROLONGADA_MIN  = 91   # inclusive (> 90 días)


class ReactivacionService:
    """
    Detecta clientes inactivos en Supabase y genera propuestas de
    reactivación diferenciadas por perfil usando Gemini (HU-05, HU-06).
    """

    def __init__(self, db: Client, orquestador=None) -> None:
        self._db          = db
        self._orquestador = orquestador  # OrquestadorGemini — puede inyectarse desde el router

    # ── Punto de entrada principal ────────────────────────────────────────────

    async def detectar_clientes_inactivos(self, usuario_id: str = None) -> dict:
        """
        1. Consulta registros_campania dimension='reactivacion' con
           fecha_ultima_transaccion < hoy - 30 días.
        2. Clasifica en 3 grupos: reciente, moderada, prolongada.
        3. Por cada grupo genera una campaña con Gemini y la guarda
           en tabla campanas (usuario_id=NULL, estado='borrador',
           generada_automaticamente=True).
        4. Retorna resumen con fechas, conteos y errores.
        """
        ejecutado_at = datetime.now(timezone.utc)
        errores: list[str] = []
        campanas_generadas = 0
        grupos_generados: list[dict] = []
        # 0. Limpiar borradores previos para evitar duplicados
        try:
            self._db.table("campanas").delete().eq("dimension", "reactivacion").eq("estado", "borrador").execute()
            logger.info("Borradores previos de reactivación limpiados.")
        except Exception as exc:
            logger.warning("No se pudieron limpiar borradores previos: %s", exc)

        # 1. Obtener registros de reactivación
        try:
            resp = (
                self._db.table("registros_campania")
                .select("*")
                .eq("dimension_ciclo_vida", "reactivacion")
                .execute()
            )
            registros = resp.data or []
        except Exception as exc:
            logger.error("Error consultando registros_campania: %s", exc)
            return {
                "ejecutado_at":       ejecutado_at,
                "grupos_detectados":  {"reciente": 0, "moderada": 0, "prolongada": 0},
                "campanas_generadas": 0,
                "errores":            [f"Error consultando base de datos: {exc}"],
            }

        logger.info("ReactivacionService: %d registros recuperados", len(registros))

        # 2. Clasificar
        grupos = self._clasificar_por_inactividad(registros)

        grupos_conteos = {
            "reciente":   len(grupos["reciente"]),
            "moderada":   len(grupos["moderada"]),
            "prolongada": len(grupos["prolongada"]),
        }
        logger.info(
            "Grupos detectados — reciente=%d, moderada=%d, prolongada=%d",
            grupos_conteos["reciente"], grupos_conteos["moderada"], grupos_conteos["prolongada"],
        )

        # 2.5 Resolve usuario_id for system jobs
        if not usuario_id:
            user_resp = self._db.table("usuarios").select("id").limit(1).execute()
            if user_resp.data:
                usuario_id = user_resp.data[0]["id"]

        # 3. Generar campaña por cada grupo con clientes
        for perfil, clientes in grupos.items():
            if not clientes:
                continue

            instruccion = self._construir_instruccion_por_perfil(perfil, len(clientes))

            try:
                # Crear campaña en borrador (usuario_id=NULL → campaña del sistema)
                insert_resp = (
                    self._db.table("campanas")
                    .insert({
                        "usuario_id":                usuario_id,
                        "dimension":                 "reactivacion",
                        "instruccion_original":      instruccion,
                        "estado":                    "borrador",
                    })
                    .execute()
                )
                if not insert_resp.data:
                    raise RuntimeError("Insert en campanas no retornó datos")

                campana_id = insert_resp.data[0]["id"]

                # Llamar al orquestador con dimension='reactivacion'
                resultado = {}
                if self._orquestador is not None:
                    resultado = await self._orquestador.procesar_instruccion(
                        instruccion=instruccion,
                        dimension="reactivacion",
                        registros=clientes,
                        usuario_id=usuario_id,
                        campana_id=campana_id,
                    )

                    if "error" not in resultado:
                        metricas = resultado.get("metricas", {})
                        segmento_final = resultado.get("segmento") or {}
                        
                        # Inyectar métricas reales del perfil que Gemini desconoce u olvida
                        dias_min = _UMBRAL_RECIENTE_MIN if perfil == "reciente" else (_UMBRAL_MODERADA_MIN if perfil == "moderada" else _UMBRAL_PROLONGADA_MIN)
                        dias_max = _UMBRAL_RECIENTE_MAX if perfil == "reciente" else (_UMBRAL_MODERADA_MAX if perfil == "moderada" else 999)
                        
                        segmento_final["perfil_inactividad"] = perfil
                        segmento_final["dias_inactividad_min"] = dias_min
                        segmento_final["dias_inactividad_max"] = dias_max
                        segmento_final["tamanio_audiencia"] = len(clientes)

                        self._db.table("campanas").update({
                            "segmento_generado":   segmento_final,
                            "mensaje_generado":    resultado.get("mensaje"),
                            "tiempo_respuesta_ms": metricas.get("tiempo_respuesta_ms"),
                            "canal_recomendado": resultado.get("canal_optimo", {}).get("canal_principal") if resultado.get("canal_optimo") else None,
                            "canales_alternativos": resultado.get("canal_optimo", {}).get("canales_alternativos") if resultado.get("canal_optimo") else None,
                            "justificacion_canal": resultado.get("canal_optimo", {}).get("justificacion") if resultado.get("canal_optimo") else None,
                            "score_confianza_canal": resultado.get("canal_optimo", {}).get("score_confianza") if resultado.get("canal_optimo") else None,
                            "horario_optimo_canal": resultado.get("canal_optimo", {}).get("horario_optimo") if resultado.get("canal_optimo") else None,
                        }).eq("id", campana_id).execute()
                    else:
                        logger.warning(
                            "Orquestador retornó error para perfil=%s: %s",
                            perfil, resultado,
                        )
                        errores.append(
                            f"Perfil {perfil}: error Gemini — {resultado.get('detalle', 'desconocido')}"
                        )

                grupos_generados.append({
                    "perfil": perfil,
                    "campana_id": campana_id,
                    "mensaje": resultado.get("mensaje") if "error" not in resultado else None,
                    "segmento": resultado.get("segmento") if "error" not in resultado else None
                })
                campanas_generadas += 1
                logger.info("Campaña generada para perfil=%s campana_id=%s", perfil, campana_id)

            except Exception as exc:
                msg = f"Perfil {perfil}: {exc}"
                logger.error("Error generando campaña de reactivación: %s", msg)
                errores.append(msg)

        return {
            "ejecutado_at":       ejecutado_at,
            "grupos_detectados":  grupos_conteos,
            "campanas_generadas": campanas_generadas,
            "grupos":             grupos_generados,
            "errores":            errores,
        }

    # ── Helpers privados ──────────────────────────────────────────────────────

    def _construir_instruccion_por_perfil(self, perfil: str, n_clientes: int) -> str:
        """
        Genera la instrucción que se enviará a Gemini según el perfil de inactividad.
        """
        instrucciones = {
            "reciente": (
                f"Generar campaña de reactivación para {n_clientes} clientes con "
                "30-45 días sin transacciones. Tono cercano y recordatorio de "
                "beneficios disponibles."
            ),
            "moderada": (
                f"Generar campaña de reactivación para {n_clientes} clientes con "
                "46-90 días sin transacciones. Tono con oferta especial limitada "
                "para incentivar el retorno."
            ),
            "prolongada": (
                f"Generar campaña de reactivación para {n_clientes} clientes con "
                "más de 90 días sin transacciones. Tono win-back con incentivo "
                "fuerte para recuperarlos antes del abandono definitivo."
            ),
        }
        return instrucciones.get(perfil, f"Generar campaña de reactivación para {n_clientes} clientes.")

    def _clasificar_por_inactividad(self, registros: list[dict]) -> dict[str, list[dict]]:
        """
        Clasifica registros en grupos según días transcurridos desde
        fecha_ultima_transaccion hasta hoy.

        - reciente:   30–45 días
        - moderada:   46–90 días
        - prolongada: > 90 días

        Registros con < 30 días de inactividad o sin fecha válida son excluidos.
        """
        grupos: dict[str, list[dict]] = {
            "reciente":   [],
            "moderada":   [],
            "prolongada": [],
        }
        ahora = datetime.now(timezone.utc)

        for registro in registros:
            dias = _dias_inactividad(registro.get("fecha_ultima_transaccion"), ahora)
            if dias is None or dias < _UMBRAL_RECIENTE_MIN:
                continue  # activo o sin fecha → ignorar

            if _UMBRAL_RECIENTE_MIN <= dias <= _UMBRAL_RECIENTE_MAX:
                grupos["reciente"].append(registro)
            elif _UMBRAL_MODERADA_MIN <= dias <= _UMBRAL_MODERADA_MAX:
                grupos["moderada"].append(registro)
            else:  # dias >= _UMBRAL_PROLONGADA_MIN
                grupos["prolongada"].append(registro)

        return grupos


# ── Utilidades de módulo ──────────────────────────────────────────────────────

def _dias_inactividad(fecha_ultima_transaccion: Any, ahora: datetime) -> int | None:
    """
    Calcula días de inactividad entre fecha_ultima_transaccion y ahora.
    Retorna None si la fecha es inválida o ausente.
    """
    if not fecha_ultima_transaccion:
        return None
    try:
        dt = datetime.fromisoformat(
            str(fecha_ultima_transaccion).replace("Z", "+00:00")
        )
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return max(0, (ahora - dt).days)
    except (ValueError, TypeError):
        return None


async def ejecutar_reactivacion_automatica() -> None:
    """
    Wrapper para el job nocturno de APScheduler (Sprint 4).
    Instancia el servicio sin orquestador (modo silencioso) y ejecuta el ciclo.
    """
    from supabase import create_client

    db = create_client(settings.supabase_url, settings.supabase_service_key)
    servicio = ReactivacionService(db)
    resumen = await servicio.detectar_clientes_inactivos()
    logger.info("Job reactivacion completado: %s", resumen)
