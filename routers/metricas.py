# Métricas globales del sistema — I1, I2, I3 (HU-08, HU-09)
import logging
from typing import Annotated
from collections import Counter

from fastapi import APIRouter, Depends
from supabase import Client

from dependencies import get_supabase_client, get_current_user, require_role

logger = logging.getLogger(__name__)
router = APIRouter()

# ── Umbrales de los indicadores ───────────────────────────────────────────────

_META_I1_MS         = 30_000    # tiempo respuesta ≤ 30 000 ms
_META_I2_ANONIMIZACION = 99.5   # tasa anonimización ≥ 99.5 %
_META_I3_PRECISION  = 85.0      # precisión segmento ≥ 85 %


# ── GET /metricas/resumen ─────────────────────────────────────────────────────

@router.get("/resumen")
async def resumen_metricas(
    usuario: Annotated[dict, Depends(get_current_user)],
    db: Annotated[Client, Depends(get_supabase_client)],
):
    """
    Resumen ejecutivo de métricas I1, I2, I3 + totales por estado y dimensión.
    Solo coordinadores.
    """
    # ── Calcular I1, I2, I3 desde logs_ejecucion ─────────────────────────────
    logs_resp = db.table("logs_ejecucion").select(
        "tiempo_respuesta_ms, tasa_anonimizacion, precision_segmento"
    ).execute()
    logs = logs_resp.data or []

    if logs:
        i1_valor = sum(l["tiempo_respuesta_ms"] for l in logs) / len(logs)
        i2_valor = sum(float(l["tasa_anonimizacion"]) for l in logs) / len(logs)
        i3_valor = sum(float(l["precision_segmento"]) for l in logs) / len(logs)
    else:
        i1_valor = i2_valor = i3_valor = 0.0

    # ── Totales de campañas ───────────────────────────────────────────────────
    camps_resp = db.table("campanas").select("estado, dimension").execute()
    campanas   = camps_resp.data or []

    total_campanas = len(campanas)
    campanas_por_estado     = dict(Counter(c["estado"]     for c in campanas))
    campanas_por_dimension  = dict(Counter(c["dimension"]  for c in campanas))

    return {
        "I1": {
            "valor":       round(i1_valor, 2),
            "meta":        _META_I1_MS,
            "cumple_meta": i1_valor <= _META_I1_MS,
            "unidad":      "ms",
        },
        "I2": {
            "valor":       round(i2_valor, 4),
            "meta":        _META_I2_ANONIMIZACION,
            "cumple_meta": i2_valor >= _META_I2_ANONIMIZACION,
            "unidad":      "%",
        },
        "I3": {
            "valor":       round(i3_valor, 4),
            "meta":        _META_I3_PRECISION,
            "cumple_meta": i3_valor >= _META_I3_PRECISION,
            "unidad":      "%",
        },
        "total_campanas":         total_campanas,
        "campanas_por_estado":    campanas_por_estado,
        "campanas_por_dimension": campanas_por_dimension,
    }


# ── GET /metricas/indicadores ─────────────────────────────────────────────────

@router.get("/indicadores")
async def indicadores(
    coordinador: Annotated[dict, Depends(require_role("coordinador"))],
    db: Annotated[Client, Depends(get_supabase_client)],
):
    """
    Retorna I1, I2, I3 cada uno con valor, meta y cumple_meta.
    Solo coordinadores.
    """
    logs_resp = db.table("logs_ejecucion").select(
        "tiempo_respuesta_ms, tasa_anonimizacion, precision_segmento"
    ).execute()
    logs = logs_resp.data or []

    if logs:
        i1 = sum(l["tiempo_respuesta_ms"] for l in logs) / len(logs)
        i2 = sum(float(l["tasa_anonimizacion"]) for l in logs) / len(logs)
        i3 = sum(float(l["precision_segmento"]) for l in logs) / len(logs)
    else:
        i1 = i2 = i3 = 0.0

    return {
        "I1_tiempo_respuesta_ms": {
            "nombre":      "Tiempo de respuesta del sistema",
            "valor":       round(i1, 2),
            "meta":        _META_I1_MS,
            "cumple_meta": i1 <= _META_I1_MS,
            "unidad":      "ms",
            "total_logs":  len(logs),
        },
        "I2_tasa_anonimizacion": {
            "nombre":      "Tasa de anonimización de datos",
            "valor":       round(i2, 4),
            "meta":        _META_I2_ANONIMIZACION,
            "cumple_meta": i2 >= _META_I2_ANONIMIZACION,
            "unidad":      "%",
            "total_logs":  len(logs),
        },
        "I3_precision_segmento": {
            "nombre":      "Precisión de segmentación",
            "valor":       round(i3, 4),
            "meta":        _META_I3_PRECISION,
            "cumple_meta": i3 >= _META_I3_PRECISION,
            "unidad":      "%",
            "total_logs":  len(logs),
        },
    }
