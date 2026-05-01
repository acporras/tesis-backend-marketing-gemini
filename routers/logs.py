# Logs de ejecución — visualización de I1, I2, I3 por campaña
import logging
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from supabase import Client

from dependencies import get_current_user, get_supabase_client, require_role

logger = logging.getLogger(__name__)
router = APIRouter()


# ── GET /logs ─────────────────────────────────────────────────────────────────

@router.get("")
async def listar_logs(
    coordinador: Annotated[dict, Depends(require_role("coordinador"))],
    db: Annotated[Client, Depends(get_supabase_client)],
    campana_id:   str | None = Query(None, description="Filtrar por ID de campaña"),
    fecha_desde:  str | None = Query(None, description="ISO 8601, ej: 2024-01-01"),
    fecha_hasta:  str | None = Query(None, description="ISO 8601, ej: 2024-12-31"),
    skip:  int = Query(0,  ge=0),
    limit: int = Query(20, ge=1, le=100),
):
    """
    Lista logs de ejecución con métricas I1, I2, I3.
    Solo coordinadores. Soporta filtros opcionales.
    """
    query = db.table("logs_ejecucion").select("*")

    if campana_id:
        query = query.eq("campana_id", campana_id)

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


# ── GET /logs/{campana_id} ────────────────────────────────────────────────────

@router.get("/{campana_id}")
async def logs_por_campana(
    campana_id: UUID,
    usuario: Annotated[dict, Depends(get_current_user)],
    db: Annotated[Client, Depends(get_supabase_client)],
):
    """
    Logs de una campaña específica.
    Coordinadores ven cualquiera; analistas solo sus propias campañas.
    """
    # Verificar que la campaña existe
    camp_resp = (
        db.table("campanas")
        .select("id, usuario_id")
        .eq("id", str(campana_id))
        .single()
        .execute()
    )
    campana = camp_resp.data
    if not campana:
        raise HTTPException(status_code=404, detail="Campaña no encontrada")

    # Verificar permisos: analistas solo ven sus propias campañas
    if usuario["rol"] == "analista":
        if campana.get("usuario_id") != usuario["id"]:
            raise HTTPException(
                status_code=403,
                detail="No tienes permiso para ver los logs de esta campaña",
            )

    logs_resp = (
        db.table("logs_ejecucion")
        .select("*")
        .eq("campana_id", str(campana_id))
        .order("created_at", desc=True)
        .execute()
    )
    return logs_resp.data or []
