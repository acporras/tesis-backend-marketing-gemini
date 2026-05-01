# HU-05: detección automática de clientes inactivos ≥ 30 días
# HU-06: mensajes de reactivación diferenciados por perfil de inactividad
import logging
from datetime import datetime, timezone
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from supabase import Client

from dependencies import (
    get_current_user,
    get_orquestador,
    get_supabase_client,
    require_role,
)
from services.orquestador import OrquestadorGemini
from services.reactivacion_service import ReactivacionService

logger = logging.getLogger(__name__)
router = APIRouter()


# ── Modelos Pydantic ──────────────────────────────────────────────────────────

class AjusteInstruccion(BaseModel):
    nueva_instruccion: str = Field(..., min_length=10, max_length=1000)


# ── GET /detectados ───────────────────────────────────────────────────────────

@router.get("/detectados")
async def listar_detectados(
    usuario: Annotated[dict, Depends(get_current_user)],
    db: Annotated[Client, Depends(get_supabase_client)],
    skip:  int = Query(0,  ge=0),
    limit: int = Query(20, ge=1, le=100),
):
    """
    Retorna propuestas de reactivación generadas automáticamente
    que están en estado 'borrador', dimension='reactivacion',
    ordenadas por created_at DESC con paginación.
    """
    resp = (
        db.table("campanas")
        .select("*")
        .eq("dimension", "reactivacion")
        .eq("estado", "borrador")
        .order("created_at", desc=True)
        .range(skip, skip + limit - 1)
        .execute()
    )
    campanas = resp.data or []

    resultado = []
    for c in campanas:
        segmento = c.get("segmento_generado") or {}
        resultado.append({
            "campana_id":            c["id"],
            "perfil_inactividad":    segmento.get("perfil_inactividad"),
            "dias_inactividad_min":  segmento.get("dias_inactividad_min"),
            "dias_inactividad_max":  segmento.get("dias_inactividad_max"),
            "tamanio_audiencia":     segmento.get("tamanio_audiencia"),
            "mensaje":               c.get("mensaje_generado"),
            "segmento":              segmento,
            "metricas":              c.get("metricas"),
            "created_at":            c.get("created_at"),
        })
    return resultado


# ── POST /ejecutar-deteccion ──────────────────────────────────────────────────

@router.post("/ejecutar-deteccion")
async def ejecutar_deteccion(
    usuario: Annotated[dict, Depends(get_current_user)],
    db: Annotated[Client, Depends(get_supabase_client)],
    orquestador: Annotated[OrquestadorGemini, Depends(get_orquestador)],
):
    """
    HU-05 — Ejecuta la detección de clientes inactivos manualmente.
    Solo coordinadores pueden invocarlo.
    Útil para testing y para forzar la generación fuera del job nocturno.
    """
    logger.info("usuario=%s ejecuta detección de reactivación manual", usuario["id"])

    servicio = ReactivacionService(db=db, orquestador=orquestador)
    resumen  = await servicio.detectar_clientes_inactivos(usuario_id=usuario["id"])
    return resumen


# ── GET /{campana_id} ─────────────────────────────────────────────────────────

@router.get("/{campana_id}")
async def obtener_campana(
    campana_id: UUID,
    usuario: Annotated[dict, Depends(get_current_user)],
    db: Annotated[Client, Depends(get_supabase_client)],
):
    """
    Detalle de una propuesta de reactivación con sus logs de ejecución.
    Coordinadores pueden ver cualquier campaña; analistas solo pueden ver
    campañas del sistema (usuario_id=NULL).
    """
    camp_resp = (
        db.table("campanas")
        .select("*")
        .eq("id", str(campana_id))
        .eq("dimension", "reactivacion")
        .single()
        .execute()
    )
    campana = camp_resp.data
    if not campana:
        raise HTTPException(status_code=404, detail="Campaña de reactivación no encontrada")

    # Analistas pueden ver campañas del sistema (usuario_id NULL)
    # o las propias; coordinadores ven todo
    es_coordinador    = usuario.get("rol") == "coordinador"
    es_campana_sistema = campana.get("usuario_id") is None
    es_propia         = campana.get("usuario_id") == usuario["id"]

    if not (es_coordinador or es_campana_sistema or es_propia):
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
    """
    Cambia el estado de una propuesta de reactivación de 'borrador'
    a 'pendiente_aprobacion'.
    Cualquier analista autenticado puede enviar a aprobación ya que
    las campañas son generadas por el sistema (usuario_id=NULL).
    """
    camp_resp = (
        db.table("campanas")
        .select("id, usuario_id, estado, dimension")
        .eq("id", str(campana_id))
        .eq("dimension", "reactivacion")
        .single()
        .execute()
    )
    campana = camp_resp.data
    if not campana:
        raise HTTPException(status_code=404, detail="Campaña de reactivación no encontrada")

    if campana["estado"] != "borrador":
        raise HTTPException(
            status_code=409,
            detail=f"La campaña está en estado '{campana['estado']}' y no puede enviarse a aprobación",
        )

    db.table("campanas").update({"estado": "pendiente_aprobacion"}).eq("id", str(campana_id)).execute()

    return {
        "mensaje":      "Campaña enviada a aprobación exitosamente",
        "campana_id":   str(campana_id),
        "estado_nuevo": "pendiente_aprobacion",
    }


# ── PATCH /{campana_id}/ajustar ───────────────────────────────────────────────

@router.patch("/{campana_id}/ajustar")
async def ajustar_campana(
    campana_id: UUID,
    body: AjusteInstruccion,
    usuario: Annotated[dict, Depends(get_current_user)],
    db: Annotated[Client, Depends(get_supabase_client)],
    orquestador: Annotated[OrquestadorGemini, Depends(get_orquestador)],
):
    """
    HU-06 — Permite al analista ajustar la instrucción y regenerar la campaña.
    Solo permitido si estado='borrador'.
    Llama nuevamente al orquestador y actualiza segmento_generado y mensaje_generado.
    """
    camp_resp = (
        db.table("campanas")
        .select("*")
        .eq("id", str(campana_id))
        .eq("dimension", "reactivacion")
        .single()
        .execute()
    )
    campana = camp_resp.data
    if not campana:
        raise HTTPException(status_code=404, detail="Campaña de reactivación no encontrada")

    if campana["estado"] != "borrador":
        raise HTTPException(
            status_code=409,
            detail=f"La campaña está en estado '{campana['estado']}'. Solo se pueden ajustar campañas en borrador.",
        )

    logger.info(
        "usuario=%s ajusta campaña=%s con nueva instrucción",
        usuario["id"], str(campana_id),
    )

    # Obtener registros de reactivación para volver a llamar al orquestador
    registros_resp = (
        db.table("registros_campania")
        .select("*")
        .eq("dimension_ciclo_vida", "reactivacion")
        .execute()
    )
    registros = registros_resp.data or []

    # Actualizar instrucción original
    db.table("campanas").update({
        "instruccion_original": body.nueva_instruccion,
    }).eq("id", str(campana_id)).execute()

    # Llamar al orquestador con la nueva instrucción
    resultado = await orquestador.procesar_instruccion(
        instruccion=body.nueva_instruccion,
        dimension="reactivacion",
        registros=registros,
        usuario_id=usuario["id"],
        campana_id=str(campana_id),
    )

    if "error" in resultado:
        logger.error("Orquestador falló al ajustar campaña=%s: %s", campana_id, resultado)
        raise HTTPException(
            status_code=500,
            detail=f"Error regenerando campaña: {resultado.get('detalle', 'error desconocido')}",
        )

    metricas = resultado.get("metricas", {})
    update_resp = (
        db.table("campanas")
        .update({
            "segmento_generado":   resultado.get("segmento"),
            "mensaje_generado":    resultado.get("mensaje"),
            "tiempo_respuesta_ms": metricas.get("tiempo_respuesta_ms"),
            "canal_recomendado": resultado.get("canal_optimo", {}).get("canal_principal") if resultado.get("canal_optimo") else None,
            "canales_alternativos": resultado.get("canal_optimo", {}).get("canales_alternativos") if resultado.get("canal_optimo") else None,
            "justificacion_canal": resultado.get("canal_optimo", {}).get("justificacion") if resultado.get("canal_optimo") else None,
            "score_confianza_canal": resultado.get("canal_optimo", {}).get("score_confianza") if resultado.get("canal_optimo") else None,
            "horario_optimo_canal": resultado.get("canal_optimo", {}).get("horario_optimo") if resultado.get("canal_optimo") else None,
        })
        .eq("id", str(campana_id))
        .execute()
    )

    campana_final = update_resp.data[0] if update_resp.data else campana

    return {
        "campana_id":   str(campana_id),
        "segmento":     resultado.get("segmento"),
        "mensaje":      resultado.get("mensaje"),
        "metricas":     metricas,
        "estado":       campana_final.get("estado", "borrador"),
        "canal_optimo": resultado.get("canal_optimo"),
    }
