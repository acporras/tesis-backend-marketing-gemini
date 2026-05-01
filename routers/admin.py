from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Form
from typing import Dict, Any, Annotated
from supabase import Client
from dependencies import get_supabase_client, get_current_user
from services.dataset_service import dataset_service

router = APIRouter(prefix="/admin/dataset", tags=["admin"])

def require_admin(usuario: dict = Depends(get_current_user)):
    if usuario.get('rol') != 'coordinador':
        raise HTTPException(status_code=403, detail="Solo administradores (coordinadores) pueden acceder a esta función")
    return usuario

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
            "total_registros": total,
            "ultima_actualizacion": ultima,
            "origen_actual": origen
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

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
