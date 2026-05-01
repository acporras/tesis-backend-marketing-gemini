# Infraestructura transversal — autenticación Supabase Auth (HU-11)
import logging
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from supabase import Client

from dependencies import get_current_user, get_supabase_client

logger = logging.getLogger(__name__)
router = APIRouter()


# ── Modelos Pydantic ──────────────────────────────────────────────────────────

class LoginRequest(BaseModel):
    email: str
    password: str


# ── POST /auth/login ──────────────────────────────────────────────────────────

@router.post("/login")
async def login(
    body: LoginRequest,
    db: Annotated[Client, Depends(get_supabase_client)],
):
    """
    Autentica al usuario con Supabase Auth.
    Busca su perfil en la tabla 'usuarios' para obtener el rol.
    Retorna access_token + datos del usuario.
    """
    try:
        auth_resp = db.auth.sign_in_with_password({
            "email":    body.email,
            "password": body.password,
        })
    except Exception as exc:
        logger.warning("Login fallido para %s: %s", body.email, exc)
        raise HTTPException(status_code=401, detail="Credenciales incorrectas")

    if not auth_resp or not auth_resp.session:
        raise HTTPException(status_code=401, detail="Credenciales incorrectas")

    access_token = auth_resp.session.access_token
    auth_user    = auth_resp.user

    # Buscar datos completos del usuario (incluyendo rol) en la tabla usuarios
    try:
        user_resp = (
            db.table("usuarios")
            .select("id, nombre, email, rol, activo")
            .eq("id", str(auth_user.id))
            .eq("activo", True)
            .single()
            .execute()
        )
        usuario = user_resp.data
    except Exception as exc:
        logger.warning("Perfil no encontrado o inactivo para %s: %s", auth_user.email, exc)
        raise HTTPException(
            status_code=401,
            detail="Usuario no encontrado o inactivo en el sistema",
        )
    logger.info("Login exitoso: usuario=%s rol=%s", usuario["email"], usuario["rol"])

    return {
        "access_token": access_token,
        "token_type":   "bearer",
        "usuario": {
            "id":     usuario["id"],
            "nombre": usuario["nombre"],
            "email":  usuario["email"],
            "rol":    usuario["rol"],
        },
    }


# ── POST /auth/logout ─────────────────────────────────────────────────────────

@router.post("/logout")
async def logout(
    usuario: Annotated[dict, Depends(get_current_user)],
    db: Annotated[Client, Depends(get_supabase_client)],
):
    """
    Cierra la sesión del usuario autenticado en Supabase Auth.
    """
    try:
        db.auth.sign_out()
    except Exception as exc:
        logger.warning("Error al cerrar sesión para %s: %s", usuario.get("email"), exc)
        # No elevamos error — la sesión del lado cliente ya caducará

    logger.info("Logout exitoso: usuario=%s", usuario.get("email"))
    return {"mensaje": "Sesión cerrada correctamente"}


# ── GET /auth/me ──────────────────────────────────────────────────────────────

@router.get("/me")
async def me(
    usuario: Annotated[dict, Depends(get_current_user)],
):
    """
    Retorna los datos del usuario autenticado.
    """
    return {
        "id":     usuario.get("id"),
        "nombre": usuario.get("nombre"),
        "email":  usuario.get("email"),
        "rol":    usuario.get("rol"),
        "activo": usuario.get("activo"),
    }
