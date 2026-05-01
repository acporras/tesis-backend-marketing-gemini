from typing import Annotated
from fastapi import APIRouter, Depends, File, UploadFile, Form, HTTPException
from supabase import Client
from uuid import UUID

from dependencies import get_current_user, get_supabase_client
from services.importacion_service import importacion_service

router = APIRouter()

@router.post("/importar")
async def importar_audiencia(
    nombre: Annotated[str, Form()],
    descripcion: Annotated[str, Form()] = "",
    archivo: UploadFile = File(...),
    usuario: dict = Depends(get_current_user),
    db: Client = Depends(get_supabase_client)
):
    """Sube un archivo de audiencia y lo procesa."""
    try:
        content = await archivo.read()
        resultado = await importacion_service.importar_archivo(
            db=db,
            archivo_content=content,
            filename=archivo.filename,
            nombre=nombre,
            descripcion=descripcion,
            usuario_id=UUID(usuario['id'])
        )
        return resultado
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error interno: {str(e)}")

@router.get("")
async def listar_audiencias(
    estado: str = "activa",
    usuario: dict = Depends(get_current_user),
    db: Client = Depends(get_supabase_client)
):
    """Lista las audiencias del usuario."""
    resp = (db.table("audiencias_importadas")
            .select("*")
            .eq("usuario_id", usuario["id"])
            .eq("estado", estado)
            .order("created_at", desc=True)
            .execute())
    return resp.data

@router.get("/{audiencia_id}")
async def obtener_audiencia(
    audiencia_id: str,
    usuario: dict = Depends(get_current_user),
    db: Client = Depends(get_supabase_client)
):
    """Detalle de una audiencia."""
    resp = db.table("audiencias_importadas").select("*").eq("id", audiencia_id).eq("usuario_id", usuario["id"]).execute()
    if not resp.data:
        raise HTTPException(status_code=404, detail="Audiencia no encontrada")
    
    # Composición real
    resp_onb = db.table("audiencia_registros").select("id", count="exact").eq("audiencia_id", audiencia_id).eq("dimension_ciclo_vida", "onboarding").limit(1).execute()
    resp_fid = db.table("audiencia_registros").select("id", count="exact").eq("audiencia_id", audiencia_id).eq("dimension_ciclo_vida", "fidelizacion").limit(1).execute()
    resp_rea = db.table("audiencia_registros").select("id", count="exact").eq("audiencia_id", audiencia_id).eq("dimension_ciclo_vida", "reactivacion").limit(1).execute()
    
    aud = resp.data[0]
    aud["composicion"] = {
        "onboarding": resp_onb.count or 0,
        "fidelizacion": resp_fid.count or 0,
        "reactivacion": resp_rea.count or 0
    }
    return aud

@router.patch("/{audiencia_id}/archivar")
async def archivar_audiencia(
    audiencia_id: str,
    usuario: dict = Depends(get_current_user),
    db: Client = Depends(get_supabase_client)
):
    resp = db.table("audiencias_importadas").update({"estado": "archivada"}).eq("id", audiencia_id).eq("usuario_id", usuario["id"]).execute()
    if not resp.data:
        raise HTTPException(status_code=404, detail="Audiencia no encontrada")
    return {"status": "ok"}

@router.delete("/{audiencia_id}")
async def eliminar_audiencia(
    audiencia_id: str,
    usuario: dict = Depends(get_current_user),
    db: Client = Depends(get_supabase_client)
):
    # Soft delete
    resp = db.table("audiencias_importadas").update({"estado": "eliminada"}).eq("id", audiencia_id).eq("usuario_id", usuario["id"]).execute()
    if not resp.data:
        raise HTTPException(status_code=404, detail="Audiencia no encontrada")
    return {"status": "ok"}
