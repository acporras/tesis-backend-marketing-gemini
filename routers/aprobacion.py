# HU-08: flujo de aprobación de campañas por coordinador
# HU-10: historial y auditoría de aprobaciones
import logging
from datetime import datetime, timezone
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from supabase import Client

from dependencies import get_current_user, get_supabase_client, require_role

logger = logging.getLogger(__name__)
router = APIRouter()

# Estados de campaña usados en este módulo
_ESTADO_PENDIENTE  = "pendiente_aprobacion"
_ESTADO_APROBADA   = "aprobada"
_ESTADO_RECHAZADA  = "rechazada"


# ── Modelos Pydantic ──────────────────────────────────────────────────────────

class AccionAprobacion(BaseModel):
    comentario: str | None = Field(None, max_length=500)


class AccionRechazo(BaseModel):
    comentario: str = Field(..., min_length=10, max_length=500,
                            description="Motivo del rechazo (obligatorio)")


# ── GET /pendientes ───────────────────────────────────────────────────────────

@router.get("/pendientes")
async def listar_pendientes(
    coordinador: Annotated[dict, Depends(require_role("coordinador"))],
    db: Annotated[Client, Depends(get_supabase_client)],
    skip:  int = Query(0,  ge=0),
    limit: int = Query(20, ge=1, le=100),
):
    """
    HU-08 — Lista campañas en estado 'pendiente_aprobacion' de todas las
    dimensiones, ordenadas por created_at ASC (más antiguas primero).
    Solo coordinadores.
    """
    resp = (
        db.table("campanas")
        .select(
            "id, dimension, instruccion_original, segmento_generado, "
            "mensaje_generado, tiempo_respuesta_ms, usuario_id, created_at, "
            "usuarios!campanas_usuario_id_fkey(nombre)"
        )
        .eq("estado", _ESTADO_PENDIENTE)
        .order("created_at", desc=False)
        .range(skip, skip + limit - 1)
        .execute()
    )
    campanas = resp.data or []

    return [
        {
            "campana_id":            c["id"],
            "dimension":             c.get("dimension"),
            "instruccion_original":  c.get("instruccion_original"),
            "segmento_generado":     c.get("segmento_generado"),
            "mensaje_generado":      c.get("mensaje_generado"),
            "tiempo_respuesta_ms":   c.get("tiempo_respuesta_ms"),
            "usuario_id":            c.get("usuario_id"),
            "usuarios":              c.get("usuarios"),
            "created_at":            c.get("created_at"),
        }
        for c in campanas
    ]


# ── POST /{campana_id}/aprobar ────────────────────────────────────────────────

@router.post("/{campana_id}/aprobar")
async def aprobar_campana(
    campana_id: UUID,
    body: AccionAprobacion,
    coordinador: Annotated[dict, Depends(require_role("coordinador"))],
    db: Annotated[Client, Depends(get_supabase_client)],
):
    """
    HU-08 — Aprueba una campaña pendiente.
    Cambia estado a 'aprobada', registra coordinador_id y timestamp.
    """
    campana = _obtener_campana_o_404(db, campana_id)

    if campana["estado"] != _ESTADO_PENDIENTE:
        raise HTTPException(
            status_code=409,
            detail=(
                f"La campaña está en estado '{campana['estado']}' y no puede aprobarse. "
                f"Solo se pueden aprobar campañas en estado '{_ESTADO_PENDIENTE}'."
            ),
        )

    ahora = datetime.now(timezone.utc)
    db.table("campanas").update({
        "estado":       _ESTADO_APROBADA,
        "aprobado_por": coordinador["id"],
        "aprobado_at":  ahora.isoformat(),
    }).eq("id", str(campana_id)).execute()

    logger.info(
        "coordinador=%s aprobó campaña=%s",
        coordinador["id"], str(campana_id),
    )

    return {
        "campana_id":   str(campana_id),
        "estado_nuevo": _ESTADO_APROBADA,
        "aprobado_por": coordinador["id"],
        "timestamp":    ahora.isoformat(),
    }


# ── POST /{campana_id}/rechazar ───────────────────────────────────────────────

@router.post("/{campana_id}/rechazar")
async def rechazar_campana(
    campana_id: UUID,
    body: AccionRechazo,
    coordinador: Annotated[dict, Depends(require_role("coordinador"))],
    db: Annotated[Client, Depends(get_supabase_client)],
):
    """
    HU-08 — Rechaza una campaña pendiente.
    El coordinador debe proporcionar el motivo del rechazo (obligatorio).
    """
    campana = _obtener_campana_o_404(db, campana_id)

    if campana["estado"] != _ESTADO_PENDIENTE:
        raise HTTPException(
            status_code=409,
            detail=(
                f"La campaña está en estado '{campana['estado']}' y no puede rechazarse. "
                f"Solo se pueden rechazar campañas en estado '{_ESTADO_PENDIENTE}'."
            ),
        )

    ahora = datetime.now(timezone.utc)
    db.table("campanas").update({
        "estado": _ESTADO_RECHAZADA,
        "comentario_rechazo": body.comentario,
    }).eq("id", str(campana_id)).execute()

    logger.info(
        "coordinador=%s rechazó campaña=%s motivo='%s'",
        coordinador["id"], str(campana_id), body.comentario[:50],
    )

    return {
        "campana_id":    str(campana_id),
        "estado_nuevo":  _ESTADO_RECHAZADA,
        "rechazado_por": coordinador["id"],
        "comentario":    body.comentario,
        "timestamp":     ahora.isoformat(),
    }


# ── GET /historial ────────────────────────────────────────────────────────────

@router.get("/historial")
async def historial_aprobaciones(
    coordinador: Annotated[dict, Depends(require_role("coordinador"))],
    db: Annotated[Client, Depends(get_supabase_client)],
    dimension:    str | None = Query(None),
    estado:       str | None = Query(None),
    fecha_desde:  str | None = Query(None, description="ISO 8601, ej: 2024-01-01"),
    fecha_hasta:  str | None = Query(None, description="ISO 8601, ej: 2024-12-31"),
    skip:         int = Query(0,  ge=0),
    limit:        int = Query(20, ge=1, le=100),
):
    """
    HU-10 — Historial de campañas aprobadas y rechazadas.
    Soporta filtros opcionales: dimension, estado, fecha_desde, fecha_hasta.
    """
    query = (
        db.table("campanas")
        .select("*, usuarios!campanas_usuario_id_fkey(nombre)")
        .in_("estado", [_ESTADO_APROBADA, _ESTADO_RECHAZADA])
    )

    if dimension:
        query = query.eq("dimension", dimension)

    # Filtro por estado específico dentro de los terminales
    if estado:
        query = query.eq("estado", estado)

    if fecha_desde:
        query = query.gte("created_at", fecha_desde)

    if fecha_hasta:
        query = query.lte("created_at", fecha_hasta)

    resp = (
        query
        .order("created_at", desc=True)
        .range(skip, skip + limit - 1)
        .execute()
    )
    return resp.data or []


# ── GET /{campana_id} ─────────────────────────────────────────────────────────

@router.get("/{campana_id}")
async def obtener_campana_para_review(
    campana_id: UUID,
    coordinador: Annotated[dict, Depends(require_role("coordinador"))],
    db: Annotated[Client, Depends(get_supabase_client)],
):
    """
    HU-08 — Detalle completo de una campaña para revisión del coordinador.
    Incluye segmento_generado, mensaje_generado, y logs con I1/I2/I3.
    """
    campana = _obtener_campana_o_404(db, campana_id)

    user_resp = db.table("usuarios").select("nombre").eq("id", campana["usuario_id"]).single().execute()
    if user_resp.data:
        campana["usuarios"] = {"nombre": user_resp.data.get("nombre")}

    logs_resp = (
        db.table("logs_ejecucion")
        .select("*")
        .eq("campana_id", str(campana_id))
        .execute()
    )
    campana["logs"] = logs_resp.data or []
    return campana


# ── Helpers privados ──────────────────────────────────────────────────────────

def _obtener_campana_o_404(db: Client, campana_id: UUID) -> dict:
    """Busca la campaña en BD y eleva 404 si no existe."""
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
    return campana


    return campana
