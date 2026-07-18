from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Form
from typing import Annotated
from supabase import Client
from dependencies import get_supabase_client, get_current_user
from services.dataset_service import dataset_service
from services.rag_service import RAGService
import asyncio
import logging

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/admin/dataset", tags=["admin"])


def require_admin(usuario: dict = Depends(get_current_user)):
    if usuario.get('rol') != 'coordinador':
        raise HTTPException(status_code=403, detail="Solo administradores (coordinadores) pueden acceder a esta función")
    return usuario


# ── POST /cargar-archivo ──────────────────────────────────────────────────────

@router.post("/cargar-archivo")
async def cargar_archivo(
    modo: Annotated[str, Form()],
    archivo: UploadFile = File(...),
    usuario: dict = Depends(require_admin),
    db: Client = Depends(get_supabase_client)
):
    if modo not in ['upsert', 'reemplazo', 'agregar']:
        raise HTTPException(status_code=400, detail="Modo de carga inválido")
    try:
        content = await archivo.read()
        res = await dataset_service.cargar_archivo(db, content, archivo.filename, modo, usuario['id'])
        return {"status": "success", "data": res}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── POST /regenerar-sintetico ─────────────────────────────────────────────────

@router.post("/regenerar-sintetico")
async def regenerar_sintetico(
    payload: dict,
    usuario: dict = Depends(require_admin),
    db: Client = Depends(get_supabase_client)
):
    cantidad = payload.get('cantidad', 10000)
    distribucion = payload.get('distribucion', {})
    try:
        res = await dataset_service.regenerar_sintetico(db, cantidad, distribucion, usuario['id'])
        return {"status": "success", "data": res}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── POST /indexar ─────────────────────────────────────────────────────────────

@router.post("/indexar")
async def indexar_embeddings(
    usuario: dict = Depends(require_admin),
    db: Client = Depends(get_supabase_client),
):
    """
    Genera embeddings vectoriales para todos los registros en registros_campania
    y los guarda en clientes_embeddings. Necesario para que el RAG funcione.
    Procesa en batches de 50 para respetar el rate limit de la API de Gemini.
    Solo coordinadores.
    """
    rag = RAGService(db)

    try:
        resp = db.table("registros_campania").select(
            "cliente_id_anonimizado, dimension_ciclo_vida, score_crediticio, "
            "operaciones_ultimo_mes, canal_principal, productos_activos, fecha_apertura_cuenta"
        ).execute()
        registros = resp.data or []
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error leyendo dataset: {str(e)}")

    if not registros:
        raise HTTPException(
            status_code=404,
            detail="No hay registros en el dataset. Carga datos primero."
        )

    total      = len(registros)
    indexados  = 0
    errores    = 0
    batch_size = 50   # conservador para respetar RPM de Gemini Embedding

    logger.info("Iniciando indexación RAG de %d registros (usuario=%s)", total, usuario["id"])

    for i in range(0, total, batch_size):
        batch = registros[i:i + batch_size]
        tareas = []
        for r in batch:
            metadata = {
                "dimension_ciclo_vida":   r.get("dimension_ciclo_vida"),
                "score_crediticio":       r.get("score_crediticio"),
                "operaciones_ultimo_mes": r.get("operaciones_ultimo_mes"),
                "canal_principal":        r.get("canal_principal"),
                "productos_activos":      r.get("productos_activos"),
                "fecha_apertura_cuenta":  r.get("fecha_apertura_cuenta"),
            }
            tareas.append(
                rag.indexar_cliente(
                    cliente_id=r["cliente_id_anonimizado"],
                    metadata=metadata,
                    dimension=r.get("dimension_ciclo_vida", "fidelizacion"),
                )
            )

        resultados = await asyncio.gather(*tareas, return_exceptions=True)
        for res in resultados:
            if isinstance(res, Exception):
                errores += 1
                logger.warning("Error indexando cliente: %s", res)
            else:
                indexados += 1

        # Pausa entre batches para no saturar el rate limit de embeddings
        if i + batch_size < total:
            await asyncio.sleep(1.2)

    logger.info(
        "Indexación completada: %d/%d indexados, %d errores",
        indexados, total, errores,
    )
    return {
        "total":     total,
        "indexados": indexados,
        "errores":   errores,
        "mensaje":   f"RAG listo: {indexados} perfiles indexados correctamente.",
    }


# ── GET /estado-indexacion ────────────────────────────────────────────────────

@router.get("/estado-indexacion")
async def estado_indexacion(
    usuario: dict = Depends(require_admin),
    db: Client = Depends(get_supabase_client),
):
    """Cuántos clientes están indexados vs el total del dataset."""
    try:
        total_dataset   = db.table("registros_campania").select("id", count="exact").limit(1).execute().count or 0
        total_indexados = db.table("clientes_embeddings").select("id", count="exact").limit(1).execute().count or 0
        ultima = db.table("clientes_embeddings").select("updated_at").order("updated_at", desc=True).limit(1).execute()
        ultima_fecha = ultima.data[0]["updated_at"] if ultima.data else None

        return {
            "total_dataset":     total_dataset,
            "total_indexados":   total_indexados,
            "pendientes":        max(0, total_dataset - total_indexados),
            "porcentaje":        round(total_indexados / total_dataset * 100, 1) if total_dataset else 0,
            "ultima_indexacion": ultima_fecha,
            "rag_listo":         total_indexados > 0,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── GET /estado ───────────────────────────────────────────────────────────────

@router.get("/estado")
async def obtener_estado(
    usuario: dict = Depends(require_admin),
    db: Client = Depends(get_supabase_client)
):
    try:
        total_res = db.table('registros_campania').select('id', count='exact').limit(1).execute()
        total = total_res.count

        last_load = db.table('dataset_general_cargas').select('*').order('created_at', desc=True).limit(1).execute()

        ultima = None
        origen = "Desconocido"
        if last_load.data:
            ultima = last_load.data[0]['created_at']
            origen = last_load.data[0]['origen']

        return {
            "total_registros":    total,
            "ultima_actualizacion": ultima,
            "origen_actual":      origen,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── GET /historial ────────────────────────────────────────────────────────────

@router.get("/historial")
async def obtener_historial(
    limit: int = 50,
    usuario: dict = Depends(require_admin),
    db: Client = Depends(get_supabase_client)
):
    try:
        res = db.table('dataset_general_cargas').select('*, usuarios(nombre)').order('created_at', desc=True).limit(limit).execute()
        return res.data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
