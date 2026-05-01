# HU-03: campañas de fidelización personalizadas (NPS + cross-selling)
# HU-04: recomendación de productos adicionales por perfil
import logging
from datetime import datetime, timezone
from typing import Annotated, Literal
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from supabase import Client

from dependencies import get_current_user, get_orquestador, get_supabase_client
from services.orquestador import OrquestadorGemini

logger = logging.getLogger(__name__)
router = APIRouter()

# ── Catálogo de productos del banco ──────────────────────────────────────────
_CATALOGO = [
    "cuenta_ahorro",
    "tarjeta_credito",
    "tarjeta_credito_premium",
    "prestamo_personal",
    "deposito_plazo",
    "seguro_vida",
    "fondo_inversion",
]

# ── Modelos Pydantic ──────────────────────────────────────────────────────────

class FiltrosOpcionales(BaseModel):
    score_minimo:              int | None = Field(None, ge=300, le=850)
    operaciones_minimas_mes:   int | None = Field(None, ge=0)
    productos_excluir:         list[str] | None = None
    canal_preferido:           str | None = None


class InstruccionFidelizacion(BaseModel):
    instruccion:        str = Field(..., min_length=10, max_length=1000)
    objetivo:           Literal["cross_selling", "up_selling", "retencion", "satisfaccion"]
    filtros_opcionales: FiltrosOpcionales | None = None
    campana_id:         UUID | None = None
    audiencia_id:       UUID | None = None


class RespuestaCampanaFidelizacion(BaseModel):
    campana_id:            UUID
    segmento:              dict
    tamanio_audiencia:     int
    mensaje:               str
    tipo_campana:          str
    tono:                  str
    objetivo:              str
    productos_recomendados: list[str]
    metricas:              dict
    estado:                str
    canal_optimo:          dict | None = None
    created_at:            datetime


# ── POST /generar ─────────────────────────────────────────────────────────────

@router.post("/generar", response_model=RespuestaCampanaFidelizacion, status_code=200)
async def generar_campana_fidelizacion(
    body: InstruccionFidelizacion,
    usuario: Annotated[dict, Depends(get_current_user)],
    orquestador: Annotated[OrquestadorGemini, Depends(get_orquestador)],
    db: Annotated[Client, Depends(get_supabase_client)],
):
    """
    HU-03 — Genera segmento + mensaje de fidelización a partir de instrucción natural.
    Aplica filtros opcionales antes de llamar al orquestador.
    """
    logger.info(
        "usuario=%s genera campaña fidelizacion objetivo=%s",
        usuario["id"], body.objetivo,
    )

    # 1. Obtener clientes de fidelizacion
    if body.audiencia_id:
        clientes_resp = (
            db.table("audiencia_registros")
            .select("*")
            .eq("audiencia_id", str(body.audiencia_id))
            .eq("dimension_ciclo_vida", "fidelizacion")
            .execute()
        )
    else:
        clientes_resp = (
            db.table("registros_campania")
            .select("*")
            .eq("dimension_ciclo_vida", "fidelizacion")
            .execute()
        )
    clientes = clientes_resp.data or []

    # 2. Aplicar filtros opcionales
    if body.filtros_opcionales:
        clientes = _aplicar_filtros(clientes, body.filtros_opcionales)

    if not clientes:
        raise HTTPException(
            status_code=404,
            detail="No se encontraron clientes de fidelización que cumplan los filtros",
        )

    # 3. Crear campaña en borrador o reusar
    if body.campana_id:
        campana_id = str(body.campana_id)
        existente = db.table("campanas").select("estado, usuario_id").eq("id", campana_id).single().execute()
        if not existente.data or existente.data["estado"] != "borrador":
            raise HTTPException(status_code=400, detail="Solo se pueden refactorizar borradores.")
        if existente.data["usuario_id"] != usuario["id"]:
            raise HTTPException(status_code=403, detail="No tienes permisos.")
        
        db.table("campanas").update({"instruccion_original": body.instruccion}).eq("id", campana_id).execute()
    else:
        insert_resp = (
            db.table("campanas")
            .insert({
                "usuario_id":           usuario["id"],
                "dimension":            "fidelizacion",
                "instruccion_original": body.instruccion,
                "estado":               "borrador",
            })
            .execute()
        )
        if not insert_resp.data:
            raise HTTPException(status_code=500, detail="Error al crear la campaña")
        campana_id = insert_resp.data[0]["id"]

    # 4. Enriquecer instrucción con objetivo y filtros para mejor contexto en Gemini
    instruccion_enriquecida = _enriquecer_instruccion(body)

    # 5. Llamar al orquestador
    resultado = await orquestador.procesar_instruccion(
        instruccion=instruccion_enriquecida,
        dimension="fidelizacion",
        tipo_campana=body.objetivo,
        registros=clientes,
        usuario_id=usuario["id"],
        campana_id=campana_id,
    )

    if "error" in resultado:
        logger.error("Orquestador falló: %s", resultado)
        raise HTTPException(
            status_code=500,
            detail=f"Error generando campaña: {resultado.get('detalle', 'error desconocido')}",
        )

    # 6. Actualizar campaña con datos generados
    metricas = resultado["metricas"]
    update_resp = (
        db.table("campanas")
        .update({
            "segmento_generado": {
                "criterios": resultado["segmento"],
                "tamanio_audiencia": resultado.get("tamanio_audiencia", 0),
                "tipo_campana": resultado.get("tipo_campana", body.objetivo),
                "tono": resultado.get("tono", "cercano")
            },
            "mensaje_generado":   resultado["mensaje"],
            "tiempo_respuesta_ms": metricas["tiempo_respuesta_ms"],
            "canal_recomendado": resultado.get("canal_optimo", {}).get("canal_principal") if resultado.get("canal_optimo") else None,
            "canales_alternativos": resultado.get("canal_optimo", {}).get("canales_alternativos") if resultado.get("canal_optimo") else None,
            "justificacion_canal": resultado.get("canal_optimo", {}).get("justificacion") if resultado.get("canal_optimo") else None,
            "score_confianza_canal": resultado.get("canal_optimo", {}).get("score_confianza") if resultado.get("canal_optimo") else None,
            "horario_optimo_canal": resultado.get("canal_optimo", {}).get("horario_optimo") if resultado.get("canal_optimo") else None,
        })
        .eq("id", campana_id)
        .execute()
    )

    campana_final = (
        update_resp.data[0] if update_resp.data else insert_resp.data[0]
    )
    created_at_raw = campana_final.get("created_at", datetime.now(timezone.utc).isoformat())
    
    # 7. Extraer productos recomendados del segmento generado
    productos_recomendados = _extraer_productos_del_segmento(resultado["segmento"])

    return RespuestaCampanaFidelizacion(
        campana_id=UUID(campana_id),
        segmento=resultado["segmento"],
        tamanio_audiencia=resultado["tamanio_audiencia"],
        mensaje=resultado["mensaje"],
        tipo_campana=resultado.get("tipo_campana", "fidelizacion"),
        tono=resultado.get("tono", "cercano"),
        objetivo=body.objetivo,
        productos_recomendados=productos_recomendados,
        metricas=metricas,
        estado=campana_final["estado"],
        canal_optimo=resultado.get("canal_optimo"),
        created_at=datetime.fromisoformat(str(created_at_raw).replace("Z", "+00:00")),
    )


# ── GET /mis-campanas ─────────────────────────────────────────────────────────

@router.get("/mis-campanas")
async def listar_mis_campanas(
    usuario: Annotated[dict, Depends(get_current_user)],
    db: Annotated[Client, Depends(get_supabase_client)],
    skip:  int = Query(0,  ge=0),
    limit: int = Query(20, ge=1, le=100),
):
    """Lista campañas de fidelización del usuario autenticado, paginadas."""
    resp = (
        db.table("campanas")
        .select("*")
        .eq("usuario_id", usuario["id"])
        .eq("dimension", "fidelizacion")
        .order("created_at", desc=True)
        .range(skip, skip + limit - 1)
        .execute()
    )
    return resp.data or []


# ── GET /{campana_id} ─────────────────────────────────────────────────────────

@router.get("/{campana_id}")
async def obtener_campana(
    campana_id: UUID,
    usuario: Annotated[dict, Depends(get_current_user)],
    db: Annotated[Client, Depends(get_supabase_client)],
):
    """Detalle de una campaña de fidelización con sus logs de ejecución."""
    camp_resp = (
        db.table("campanas")
        .select("*")
        .eq("id", str(campana_id))
        .single()
        .execute()
    )
    campana = camp_resp.data
    if not campana:
        raise HTTPException(status_code=404, detail="Campaña no encontrada")

    if campana["usuario_id"] != usuario["id"] and usuario["rol"] != "coordinador":
        raise HTTPException(status_code=403, detail="No tienes permisos sobre esta campaña")

    logs_resp = (
        db.table("logs_ejecucion")
        .select("*")
        .eq("campana_id", str(campana_id))
        .execute()
    )
    campana["logs"] = logs_resp.data or []

    return campana


# ── PATCH /{campana_id}/enviar-aprobacion ─────────────────────────────────────

@router.patch("/{campana_id}/enviar-aprobacion")
async def enviar_a_aprobacion(
    campana_id: UUID,
    usuario: Annotated[dict, Depends(get_current_user)],
    db: Annotated[Client, Depends(get_supabase_client)],
):
    """Cambia el estado de 'borrador' → 'pendiente_aprobacion'."""
    camp_resp = (
        db.table("campanas")
        .select("id, usuario_id, estado")
        .eq("id", str(campana_id))
        .single()
        .execute()
    )
    campana = camp_resp.data
    if not campana:
        raise HTTPException(status_code=404, detail="Campaña no encontrada")

    if campana["usuario_id"] != usuario["id"]:
        raise HTTPException(
            status_code=403,
            detail="Solo el creador puede enviar la campaña a aprobación",
        )
    if campana["estado"] != "borrador":
        raise HTTPException(
            status_code=409,
            detail=f"La campaña está en estado '{campana['estado']}' y no puede enviarse a aprobación",
        )

    db.table("campanas").update({"estado": "pendiente_aprobacion"}).eq("id", str(campana_id)).execute()

    return {
        "mensaje":     "Campaña enviada a aprobación exitosamente",
        "campana_id":  str(campana_id),
        "estado_nuevo": "pendiente_aprobacion",
    }


# ── GET /clientes/{cliente_id}/recomendaciones ────────────────────────────────

@router.get("/clientes/{cliente_id}/recomendaciones")
async def recomendaciones_cliente(
    cliente_id: str,
    usuario: Annotated[dict, Depends(get_current_user)],
    db: Annotated[Client, Depends(get_supabase_client)],
):
    """
    HU-04 — Recomendaciones de productos adicionales para un cliente activo.
    Reglas basadas en score, productos actuales, operaciones y antigüedad.
    """
    resp = (
        db.table("registros_campania")
        .select("*")
        .eq("cliente_id_anonimizado", cliente_id)
        .single()
        .execute()
    )
    cliente = resp.data
    if not cliente:
        raise HTTPException(status_code=404, detail="Cliente no encontrado")

    productos_actuales   = _normalizar_lista(cliente.get("productos_activos", []))
    recomendaciones      = _generar_recomendaciones(cliente, productos_actuales)
    score_compatibilidad = _calcular_score_compatibilidad(cliente, recomendaciones)

    return {
        "cliente_id":            cliente_id,
        "productos_actuales":    productos_actuales,
        "productos_recomendados": recomendaciones,
        "score_compatibilidad":  score_compatibilidad,
    }


# ── Lógica de recomendaciones (HU-04) ────────────────────────────────────────

def _generar_recomendaciones(cliente: dict, productos_actuales: list[str]) -> list[dict]:
    score  = int(cliente.get("score_crediticio") or 0)
    ops    = int(cliente.get("operaciones_ultimo_mes") or 0)
    antig  = _antiguedad_meses(cliente.get("fecha_apertura_cuenta"))
    tiene  = set(productos_actuales)
    recs   = []

    # Regla 1: cuenta_ahorro + score >= 650 + sin tarjeta_credito
    if "cuenta_ahorro" in tiene and score >= 650 and "tarjeta_credito" not in tiene:
        recs.append({
            "producto":  "tarjeta_credito",
            "razon":     "Perfil crediticio sólido y cuenta activa califican para tarjeta",
            "prioridad": "alta",
        })

    # Regla 2: cuenta_ahorro + score >= 700 + ops_mes >= 10
    if "cuenta_ahorro" in tiene and score >= 700 and ops >= 10 and "prestamo_personal" not in tiene:
        recs.append({
            "producto":  "prestamo_personal",
            "razon":     "Alta actividad transaccional y buen historial crediticio",
            "prioridad": "media",
        })

    # Regla 3: cuenta_ahorro + antigüedad >= 12 meses
    if "cuenta_ahorro" in tiene and antig >= 12 and "deposito_plazo" not in tiene:
        recs.append({
            "producto":  "deposito_plazo",
            "razon":     "Antigüedad de cuenta superior a 12 meses permite acceder a depósitos a plazo",
            "prioridad": "media",
        })

    # Regla 4: tarjeta_credito + ops_mes >= 15
    if "tarjeta_credito" in tiene and ops >= 15 and "tarjeta_credito_premium" not in tiene:
        recs.append({
            "producto":  "tarjeta_credito_premium",
            "razon":     "Uso intensivo de tarjeta de crédito habilita upgrade a cuenta premium",
            "prioridad": "baja",
        })

    return recs


def _calcular_score_compatibilidad(cliente: dict, recomendaciones: list[dict]) -> float:
    """Puntuación 0-100 que refleja qué tan buen candidato es el cliente para fidelización."""
    if not recomendaciones:
        return 0.0
    score_cred = min(int(cliente.get("score_crediticio") or 0), 850)
    base = round((score_cred / 850) * 70, 2)
    bonus_alta  = sum(10 for r in recomendaciones if r["prioridad"] == "alta")
    bonus_media = sum(5  for r in recomendaciones if r["prioridad"] == "media")
    total = min(base + bonus_alta + bonus_media, 100.0)
    return round(total, 2)


# ── Helpers internos ──────────────────────────────────────────────────────────

def _aplicar_filtros(registros: list[dict], filtros: FiltrosOpcionales) -> list[dict]:
    """
    Filtra la lista de registros según los filtros opcionales del request.
    Cada filtro activo es una condición AND sobre los registros.
    """
    resultado = registros

    if filtros.score_minimo is not None:
        resultado = [
            r for r in resultado
            if int(r.get("score_crediticio") or 0) >= filtros.score_minimo
        ]

    if filtros.operaciones_minimas_mes is not None:
        resultado = [
            r for r in resultado
            if int(r.get("operaciones_ultimo_mes") or 0) >= filtros.operaciones_minimas_mes
        ]

    if filtros.productos_excluir:
        excluidos = set(filtros.productos_excluir)
        resultado = [
            r for r in resultado
            if not excluidos.intersection(set(_normalizar_lista(r.get("productos_activos", []))))
        ]

    if filtros.canal_preferido:
        resultado = [
            r for r in resultado
            if r.get("canal_principal") == filtros.canal_preferido
        ]

    return resultado


def _enriquecer_instruccion(body: InstruccionFidelizacion) -> str:
    """Construye una instrucción enriquecida con objetivo y filtros para Gemini."""
    partes = [body.instruccion, f"Objetivo de la campaña: {body.objetivo}."]

    if body.filtros_opcionales:
        f = body.filtros_opcionales
        if f.score_minimo:
            partes.append(f"Score crediticio mínimo del segmento: {f.score_minimo}.")
        if f.operaciones_minimas_mes:
            partes.append(f"Operaciones mínimas mensuales: {f.operaciones_minimas_mes}.")
        if f.canal_preferido:
            partes.append(f"Canal preferido: {f.canal_preferido}.")
        if f.productos_excluir:
            partes.append(f"Excluir clientes con productos: {', '.join(f.productos_excluir)}.")

    return " ".join(partes)


def _extraer_productos_del_segmento(segmento: dict) -> list[str]:
    """Intenta extraer productos del segmento generado por Gemini."""
    for clave in ("productos_recomendados", "productos", "productos_activos"):
        valor = segmento.get(clave)
        if isinstance(valor, list):
            return [str(p) for p in valor]
        if isinstance(valor, str):
            return [valor]
    return []


def _normalizar_lista(valor) -> list[str]:
    if isinstance(valor, list):
        return [str(v) for v in valor]
    if isinstance(valor, str) and valor:
        return [valor]
    return []


def _antiguedad_meses(fecha_apertura) -> int:
    if not fecha_apertura:
        return 0
    try:
        dt = datetime.fromisoformat(str(fecha_apertura).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return max(0, (datetime.now(timezone.utc) - dt).days // 30)
    except (ValueError, TypeError):
        return 0

# ── DELETE /{campana_id} ──────────────────────────────────────────────────────

@router.delete("/{campana_id}")
async def eliminar_borrador(
    campana_id: UUID,
    usuario: Annotated[dict, Depends(get_current_user)],
    db: Annotated[Client, Depends(get_supabase_client)],
):
    """
    Permite a un analista eliminar su propia campaña SI está en estado 'borrador'.
    """
    camp_resp = (
        db.table("campanas")
        .select("id, usuario_id, estado")
        .eq("id", str(campana_id))
        .single()
        .execute()
    )
    campana = camp_resp.data
    if not campana:
        raise HTTPException(status_code=404, detail="Campaña no encontrada")

    if campana["usuario_id"] != usuario["id"]:
        raise HTTPException(status_code=403, detail="No puedes eliminar campañas de otros usuarios")

    if campana["estado"] != "borrador":
        raise HTTPException(status_code=400, detail="Solo se pueden eliminar campañas en estado 'borrador'")

    delete_resp = (
        db.table("campanas")
        .delete()
        .eq("id", str(campana_id))
        .execute()
    )

    if not delete_resp.data:
        raise HTTPException(status_code=500, detail="Error al eliminar la campaña")

    return {"message": "Borrador eliminado correctamente"}
