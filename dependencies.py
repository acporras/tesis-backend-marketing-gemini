import logging
from typing import Annotated

from fastapi import Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from supabase import Client, create_client

from config import get_gemini_model, settings
from services.anonimizacion import AnonimizacionService
from services.orquestador import OrquestadorGemini
from services.rag_service import RAGService

logger = logging.getLogger(__name__)

# auto_error=False → devuelve None en vez de 403 cuando no hay header,
# así podemos elevar 401 nosotros mismos
security = HTTPBearer(auto_error=False)


def get_supabase_client() -> Client:
    """Crea un cliente Supabase usando service_key (bypasa RLS) o anon_key."""
    return create_client(settings.supabase_url, settings.supabase_backend_key)


async def get_current_user(
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(security)],
    db: Annotated[Client, Depends(get_supabase_client)],
) -> dict:
    """
    Valida el Bearer token contra Supabase Auth y devuelve el usuario
    de la tabla `usuarios`. Eleva 401 si el token es inválido o ausente.
    """
    if credentials is None:
        raise HTTPException(status_code=401, detail="No autenticado")

    try:
        auth_resp = db.auth.get_user(credentials.credentials)
        if not auth_resp.user:
            raise HTTPException(status_code=401, detail="Token inválido o expirado")

        user_resp = (
            db.table("usuarios")
            .select("id, nombre, email, rol, activo")
            .eq("id", auth_resp.user.id)
            .eq("activo", True)
            .single()
            .execute()
        )
        if not user_resp.data:
            raise HTTPException(status_code=401, detail="Usuario no encontrado o inactivo")

        return user_resp.data

    except HTTPException:
        raise
    except Exception as exc:
        logger.warning("Error validando token Supabase: %s", exc)
        raise HTTPException(status_code=401, detail="No autenticado")


def get_orquestador(
    db: Annotated[Client, Depends(get_supabase_client)],
) -> OrquestadorGemini:
    """Factoría del orquestador con todas sus dependencias inyectadas."""
    return OrquestadorGemini(
        modelo=get_gemini_model(),
        db=db,
        anonimizador=AnonimizacionService(),
        rag=RAGService(db),
    )


def require_role(rol: str):
    """
    Decorador de dependencia que verifica que el usuario tenga el rol indicado.
    Uso: Depends(require_role("coordinador"))
    """
    async def _verificar(
        usuario: Annotated[dict, Depends(get_current_user)],
    ) -> dict:
        if usuario.get("rol") != rol:
            raise HTTPException(
                status_code=403,
                detail=f"Acceso restringido. Rol requerido: '{rol}'",
            )
        return usuario

    return _verificar
