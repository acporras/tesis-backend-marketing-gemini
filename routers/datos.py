from typing import Annotated
from fastapi import APIRouter, Depends
from supabase import Client

from dependencies import get_current_user, get_supabase_client

router = APIRouter()

@router.get("/resumen")
async def obtener_resumen_datos(
    usuario: Annotated[dict, Depends(get_current_user)],
    db: Annotated[Client, Depends(get_supabase_client)]
):
    """
    Retorna estadísticas agregadas de los datos ingestados.
    No retorna registros individuales.
    """
    # Total
    resp_total = db.table("registros_campania").select("id", count="exact").limit(1).execute()
    total = resp_total.count or 0

    # Dimensiones
    resp_onb = db.table("registros_campania").select("id", count="exact").eq("dimension_ciclo_vida", "onboarding").limit(1).execute()
    resp_fid = db.table("registros_campania").select("id", count="exact").eq("dimension_ciclo_vida", "fidelizacion").limit(1).execute()
    resp_rea = db.table("registros_campania").select("id", count="exact").eq("dimension_ciclo_vida", "reactivacion").limit(1).execute()

    # Canales
    resp_app = db.table("registros_campania").select("id", count="exact").eq("canal_principal", "app_movil").limit(1).execute()
    resp_web = db.table("registros_campania").select("id", count="exact").eq("canal_principal", "digital").limit(1).execute()
    resp_age = db.table("registros_campania").select("id", count="exact").eq("canal_principal", "presencial").limit(1).execute()
    
    # Score crediticio
    # excelent > 750, bueno 650-750, regular 550-649, bajo < 550
    resp_exc = db.table("registros_campania").select("id", count="exact").gt("score_crediticio", 750).limit(1).execute()
    resp_bue = db.table("registros_campania").select("id", count="exact").gte("score_crediticio", 650).lte("score_crediticio", 750).limit(1).execute()
    resp_reg = db.table("registros_campania").select("id", count="exact").gte("score_crediticio", 550).lt("score_crediticio", 650).limit(1).execute()
    resp_baj = db.table("registros_campania").select("id", count="exact").lt("score_crediticio", 550).limit(1).execute()

    # Última actualización real
    resp_ultima = db.table("registros_campania").select("updated_at").order("updated_at", desc=True).limit(1).execute()
    if resp_ultima.data and len(resp_ultima.data) > 0:
        ultima_ingesta = resp_ultima.data[0]["updated_at"]
    else:
        ultima_ingesta = "Sin registros"

    return {
        "total_registros": total,
        "ultima_ingesta": ultima_ingesta,
        "proxima_ingesta": "No programada (ingesta manual)",
        "por_dimension": {
            "onboarding": resp_onb.count or 0,
            "fidelizacion": resp_fid.count or 0,
            "reactivacion": resp_rea.count or 0
        },
        "por_canal": {
            "app_movil": resp_app.count or 0,
            "web": resp_web.count or 0,
            "agencia": resp_age.count or 0
        },
        "score_distribucion": {
            "excelente": resp_exc.count or 0,
            "bueno": resp_bue.count or 0,
            "regular": resp_reg.count or 0,
            "bajo": resp_baj.count or 0
        },
        "historial": []
    }
