# HU-01: descripción en lenguaje natural → segmento + mensaje (≤ 30 s)
# HU-02: detección de etapa de activación del prospecto
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

# ── Modelos Pydantic ──────────────────────────────────────────────────────────

class InstruccionOnboarding(BaseModel):
    instruccion: str = Field(..., min_length=10, max_length=1000)
    tipo_campana: Literal["bienvenida", "activacion", "seguimiento"]
    campana_id: UUID | None = None
    audiencia_id: UUID | None = None


class RespuestaCampana(BaseModel):
    campana_id: UUID
    segmento: dict
    tamanio_audiencia: int
    mensaje: str
    tipo_campana: str
    tono: str
    metricas: dict
    estado: str
    canal_optimo: dict | None = None
    created_at: datetime


class AccionCampana(BaseModel):
    campana_id: UUID


# ── POST /generar ─────────────────────────────────────────────────────────────

@router.post("/generar", response_model=RespuestaCampana, status_code=200)
async def generar_campana(
    body: InstruccionOnboarding,
    usuario: Annotated[dict, Depends(get_current_user)],
    orquestador: Annotated[OrquestadorGemini, Depends(get_orquestador)],
    db: Annotated[Client, Depends(get_supabase_client)],
):
    """
    HU-01 — Genera segmento + mensaje de onboarding a partir de instrucción natural.
    Registra I1, I2, I3 automáticamente en logs_ejecucion.
    """
    logger.info("usuario=%s genera campaña onboarding tipo=%s", usuario["id"], body.tipo_campana)

    # 1. Obtener prospectos de onboarding
    if body.audiencia_id:
        prospectos_resp = (
            db.table("audiencia_registros")
            .select("*")
            .eq("audiencia_id", str(body.audiencia_id))
            .eq("dimension_ciclo_vida", "onboarding")
            .execute()
        )
    else:
        prospectos_resp = (
            db.table("registros_campania")
            .select("*")
            .eq("dimension_ciclo_vida", "onboarding")
            .execute()
        )
    prospectos = prospectos_resp.data or []
    if not prospectos:
        raise HTTPException(status_code=404, detail="No se encontraron prospectos de onboarding")

    # 2. Crear registro o reusar uno existente
    if body.campana_id:
        campana_id = str(body.campana_id)
        # Validar que exista y esté en borrador
        existente = db.table("campanas").select("estado, usuario_id").eq("id", campana_id).single().execute()
        if not existente.data or existente.data["estado"] != "borrador":
            raise HTTPException(status_code=400, detail="Solo se pueden refactorizar borradores.")
        if existente.data["usuario_id"] != usuario["id"]:
            raise HTTPException(status_code=403, detail="No tienes permisos.")
        
        # Actualizar instrucción original por si se concatenó un ajuste
        db.table("campanas").update({"instruccion_original": body.instruccion}).eq("id", campana_id).execute()
    else:
        nueva_campana = {
            "usuario_id":           usuario["id"],
            "dimension":            "onboarding",
            "instruccion_original": body.instruccion,
            "estado":               "borrador",
        }
        insert_resp = db.table("campanas").insert(nueva_campana).execute()
        if not insert_resp.data:
            raise HTTPException(status_code=500, detail="Error al crear la campaña")
        campana_id = insert_resp.data[0]["id"]

    # 3. Llamar al orquestador (Anonimización + RAG + Gemini + Log)
    resultado = await orquestador.procesar_instruccion(
        instruccion=body.instruccion,
        dimension="onboarding",
        tipo_campana=body.tipo_campana,
        registros=prospectos,
        usuario_id=usuario["id"],
        campana_id=campana_id,
    )

    if "error" in resultado:
        logger.error("Error en orquestador: %s", resultado)
        raise HTTPException(
            status_code=500,
            detail=f"Error generando campaña: {resultado.get('detalle', 'error desconocido')}",
        )

    # 4. Actualizar campaña con los datos generados
    metricas = resultado["metricas"]
    update_resp = (
        db.table("campanas")
        .update({
            "segmento_generado": {
                "criterios": resultado["segmento"],
                "tamanio_audiencia": resultado.get("tamanio_audiencia", 0),
                "tipo_campana": resultado.get("tipo_campana", body.tipo_campana),
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

    campana_final = update_resp.data[0] if update_resp.data else insert_resp.data[0]
    created_at = campana_final.get("created_at", datetime.now(timezone.utc).isoformat())

    return RespuestaCampana(
        campana_id=UUID(campana_id),
        segmento=resultado["segmento"],
        tamanio_audiencia=resultado["tamanio_audiencia"],
        mensaje=resultado["mensaje"],
        tipo_campana=resultado.get("tipo_campana", body.tipo_campana),
        tono=resultado.get("tono", "cercano"),
        metricas=metricas,
        estado=campana_final["estado"],
        canal_optimo=resultado.get("canal_optimo"),
        created_at=datetime.fromisoformat(str(created_at).replace("Z", "+00:00"))
        if isinstance(created_at, str)
        else created_at,
    )


# ── GET /mis-campanas ─────────────────────────────────────────────────────────

@router.get("/mis-campanas")
async def listar_mis_campanas(
    usuario: Annotated[dict, Depends(get_current_user)],
    db: Annotated[Client, Depends(get_supabase_client)],
    skip: int = Query(0, ge=0),
    limit: int = Query(20, ge=1, le=100),
):
    """Lista campañas de onboarding del usuario autenticado, paginadas."""
    resp = (
        db.table("campanas")
        .select("*")
        .eq("usuario_id", usuario["id"])
        .eq("dimension", "onboarding")
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
    """Detalle de una campaña con sus métricas de ejecución."""
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

    # Solo el creador o un coordinador pueden ver la campaña
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


# ── PATCH /{campana_id}/enviar-aprobacion ────────────────────────────────────

@router.patch("/{campana_id}/enviar-aprobacion")
async def enviar_a_aprobacion(
    campana_id: UUID,
    usuario: Annotated[dict, Depends(get_current_user)],
    db: Annotated[Client, Depends(get_supabase_client)],
):
    """
    Cambia el estado de 'borrador' → 'pendiente_aprobacion'.
    Solo el creador puede hacerlo.
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
        raise HTTPException(status_code=403, detail="Solo el creador puede enviar la campaña a aprobación")

    if campana["estado"] != "borrador":
        raise HTTPException(
            status_code=409,
            detail=f"La campaña está en estado '{campana['estado']}' y no puede enviarse a aprobación",
        )

    db.table("campanas").update({"estado": "pendiente_aprobacion"}).eq("id", str(campana_id)).execute()

    return {
        "mensaje": "Campaña enviada a aprobación exitosamente",
        "campana_id": str(campana_id),
        "estado_nuevo": "pendiente_aprobacion",
    }


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


# ── GET /prospectos/{cliente_id}/etapa ────────────────────────────────────────

@router.get("/prospectos/{cliente_id}/etapa")
async def detectar_etapa_prospecto(
    cliente_id: str,
    usuario: Annotated[dict, Depends(get_current_user)],
    db: Annotated[Client, Depends(get_supabase_client)],
):
    """
    HU-02 — Detecta la etapa de activación de un prospecto:
      sin_activar | activacion_parcial | activo_completo
    """
    resp = (
        db.table("registros_campania")
        .select("*")
        .eq("cliente_id_anonimizado", cliente_id)
        .single()
        .execute()
    )
    registro = resp.data
    if not registro:
        raise HTTPException(status_code=404, detail="Prospecto no encontrado")

    etapa, recomendacion = _detectar_etapa(registro)

    return {
        "cliente_id": cliente_id,
        "etapa": etapa,
        "recomendacion": recomendacion,
        "datos": {
            "operaciones_ultimo_mes": registro.get("operaciones_ultimo_mes"),
            "canal_principal":        registro.get("canal_principal"),
            "score_crediticio":       registro.get("score_crediticio"),
        },
    }


# ── Lógica de etapa (HU-02) ───────────────────────────────────────────────────

def _detectar_etapa(registro: dict) -> tuple[str, str]:
    """
    Analiza fecha_apertura_cuenta y fecha_ultima_transaccion para
    determinar en qué etapa de activación está el prospecto.
    """
    ops = int(registro.get("operaciones_ultimo_mes") or 0)
    ahora = datetime.now(timezone.utc)

    def _dias(campo: str) -> int:
        val = registro.get(campo)
        if not val:
            return 999
        try:
            dt = datetime.fromisoformat(str(val).replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return max(0, (ahora - dt).days)
        except (ValueError, TypeError):
            return 999

    dias_apertura = _dias("fecha_apertura_cuenta")
    dias_sin_tx   = _dias("fecha_ultima_transaccion")

    if ops == 0 or dias_sin_tx > 60:
        return (
            "sin_activar",
            "Contactar con beneficio de bienvenida para activar primera transacción",
        )
    if ops >= 5 and dias_apertura >= 90:
        return (
            "activo_completo",
            "Cliente activado — considerar migración al módulo de fidelización",
        )
    return (
        "activacion_parcial",
        "Incentivar segunda y tercera transacción — enviar recordatorio personalizado",
    )
