from fastapi import APIRouter, Depends, HTTPException, Header
from typing import Dict, Any, List
from supabase import Client
from dependencies import get_supabase_client
import os

router = APIRouter(prefix="/api/sistema", tags=["api_externa"])

def verify_api_key(x_api_key: str = Header(...)):
    # Simulación de API key robusta para el banco
    valid_key = os.environ.get("BANK_API_KEY", "banco-test-key-12345")
    if x_api_key != valid_key:
        raise HTTPException(status_code=401, detail="API Key inválida")
    return x_api_key

@router.post("/ingesta-dataset")
async def ingesta_dataset(
    payload: dict,
    api_key: str = Depends(verify_api_key),
    db: Client = Depends(get_supabase_client)
):
    # payload: { "registros": [...], "modo": "upsert" }
    registros = payload.get("registros", [])
    modo = payload.get("modo", "upsert")
    
    if not isinstance(registros, list) or len(registros) == 0:
        raise HTTPException(status_code=400, detail="Debe proveer una lista de registros no vacía")
        
    if modo not in ['upsert']:
        raise HTTPException(status_code=400, detail="Modo de carga inválido para la ingesta externa (solo upsert)")
        
    # We create a dummy JSON file content from the records to reuse the service logic
    import json
    file_content = json.dumps(registros).encode('utf-8')
    filename = "ingesta_api.json"
    
    from services.dataset_service import dataset_service
    try:
        # usuario_id is None since this is an automated ETL
        res = await dataset_service.cargar_archivo(db, file_content, filename, modo, None)
        return {"status": "success", "data": res}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
