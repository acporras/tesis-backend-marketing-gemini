from typing import Annotated
from fastapi import APIRouter, Depends
from supabase import Client
from dependencies import get_supabase_client, get_current_user
import logging

logger = logging.getLogger(__name__)
router = APIRouter()

@router.get("/recientes")
async def campanas_recientes(
    usuario: Annotated[dict, Depends(get_current_user)],
    db: Annotated[Client, Depends(get_supabase_client)],
):
    """
    Retorna las últimas 5 campañas generadas en el sistema,
    para mostrarlas en el Dashboard principal.
    Si el usuario es analista, solo ve las suyas. Si es coordinador, ve todas.
    """
    query = db.table("campanas").select("id, dimension, estado, created_at, segmento_generado, usuarios!campanas_usuario_id_fkey(nombre)")
    
    if usuario["rol"] == "analista":
        query = query.eq("usuario_id", usuario["id"])
        
    resp = query.order("created_at", desc=True).limit(5).execute()
    campanas = resp.data or []
    
    # Format for frontend
    resultado = []
    for c in campanas:
        # Calcular cantidad de clientes en el segmento (si existe tamanio_audiencia)
        tamanio = 0
        if c.get("segmento_generado") and isinstance(c["segmento_generado"], dict):
            tamanio = c["segmento_generado"].get("tamanio_audiencia", 0)
            
        resultado.append({
            "id": c["id"],
            "dimension": c["dimension"],
            "estado": c["estado"],
            "created_at": c["created_at"],
            "tamanio_audiencia": tamanio,
            "analista": c.get("usuarios", {}).get("nombre", "Desconocido") if c.get("usuarios") else "Desconocido"
        })
        
    return resultado
