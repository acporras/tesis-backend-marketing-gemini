"""
Tests del router /auth (HU-11) usando FastAPI TestClient.
Las dependencias (Supabase, get_current_user) se mockean con
app.dependency_overrides.
"""
from unittest.mock import MagicMock
from fastapi.testclient import TestClient

from main import app
from dependencies import get_current_user, get_supabase_client

# ── Constantes ────────────────────────────────────────────────────────────────

_USUARIO = {
    "id":     "uid-auth-001",
    "nombre": "Ana García",
    "email":  "ana@banco.pe",
    "rol":    "analista",
    "activo": True,
}

_ACCESS_TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.test"

# ── Fixtures ──────────────────────────────────────────────────────────────────

import pytest

@pytest.fixture(autouse=True)
def limpiar_overrides():
    yield
    app.dependency_overrides.clear()


@pytest.fixture
def client():
    return TestClient(app, raise_server_exceptions=True)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_db_login_ok() -> MagicMock:
    """DB mock para login exitoso: Supabase Auth OK + tabla usuarios OK."""
    # Simular sesión Auth
    mock_session  = MagicMock()
    mock_session.access_token = _ACCESS_TOKEN
    mock_auth_user = MagicMock()
    mock_auth_user.id = _USUARIO["id"]

    mock_auth_resp = MagicMock()
    mock_auth_resp.session = mock_session
    mock_auth_resp.user    = mock_auth_user

    # Tabla usuarios → single() devuelve _USUARIO
    mock_single_exec      = MagicMock()
    mock_single_exec.data = _USUARIO
    mock_single           = MagicMock()
    mock_single.execute.return_value = mock_single_exec
    mock_eq2              = MagicMock()
    mock_eq2.single.return_value = mock_single
    mock_eq1              = MagicMock()
    mock_eq1.eq.return_value = mock_eq2
    mock_select           = MagicMock()
    mock_select.eq.return_value = mock_eq1
    t_usuarios            = MagicMock()
    t_usuarios.select.return_value = mock_select

    mock_db = MagicMock()
    mock_db.auth.sign_in_with_password.return_value = mock_auth_resp
    mock_db.table.side_effect = lambda n: t_usuarios if n == "usuarios" else MagicMock()

    return mock_db


def _make_db_login_fail() -> MagicMock:
    """DB mock para login fallido: Supabase Auth lanza excepción."""
    mock_db = MagicMock()
    mock_db.auth.sign_in_with_password.side_effect = Exception("Invalid credentials")
    return mock_db


# ── Test 1: POST /login con credenciales válidas retorna token y usuario ──────

def test_login_credenciales_validas_retorna_token(client):
    app.dependency_overrides[get_supabase_client] = lambda: _make_db_login_ok()

    resp = client.post("/auth/login", json={
        "email":    "ana@banco.pe",
        "password": "Password123!",
    })

    assert resp.status_code == 200
    data = resp.json()
    assert "access_token" in data
    assert data["access_token"] == _ACCESS_TOKEN
    assert data["token_type"]   == "bearer"
    assert "usuario" in data


def test_login_retorna_datos_del_usuario(client):
    app.dependency_overrides[get_supabase_client] = lambda: _make_db_login_ok()

    resp = client.post("/auth/login", json={
        "email":    "ana@banco.pe",
        "password": "Password123!",
    })

    assert resp.status_code == 200
    usuario = resp.json()["usuario"]
    for campo in ("id", "nombre", "email", "rol"):
        assert campo in usuario, f"Campo '{campo}' ausente en usuario"
    assert usuario["email"] == "ana@banco.pe"
    assert usuario["rol"]   == "analista"



# ── Test 2: POST /login con credenciales inválidas retorna 401 ────────────────

def test_login_credenciales_invalidas_retorna_401(client):
    app.dependency_overrides[get_supabase_client] = lambda: _make_db_login_fail()

    resp = client.post("/auth/login", json={
        "email":    "ana@banco.pe",
        "password": "contraseña_incorrecta",
    })

    assert resp.status_code == 401
    assert "detalle" in resp.json() or "detail" in resp.json()


def test_login_usuario_inactivo_retorna_401(client):
    """Si el usuario existe en Auth pero está inactivo en la tabla, retorna 401."""
    mock_session   = MagicMock()
    mock_session.access_token = _ACCESS_TOKEN
    mock_auth_user = MagicMock()
    mock_auth_user.id = _USUARIO["id"]
    mock_auth_resp = MagicMock()
    mock_auth_resp.session = mock_session
    mock_auth_resp.user    = mock_auth_user

    # Tabla usuarios → single() devuelve None (inactivo)
    mock_single_exec      = MagicMock()
    mock_single_exec.data = None
    mock_single           = MagicMock()
    mock_single.execute.return_value = mock_single_exec
    mock_eq2              = MagicMock()
    mock_eq2.single.return_value = mock_single
    mock_eq1              = MagicMock()
    mock_eq1.eq.return_value = mock_eq2
    mock_select           = MagicMock()
    mock_select.eq.return_value = mock_eq1
    t_usuarios            = MagicMock()
    t_usuarios.select.return_value = mock_select

    mock_db = MagicMock()
    mock_db.auth.sign_in_with_password.return_value = mock_auth_resp
    mock_db.table.side_effect = lambda n: t_usuarios

    app.dependency_overrides[get_supabase_client] = lambda: mock_db

    resp = client.post("/auth/login", json={
        "email":    "ana@banco.pe",
        "password": "Password123!",
    })
    assert resp.status_code == 401


# ── Test 3: GET /auth/me sin token retorna 401 ────────────────────────────────

def test_me_sin_token_retorna_401(client):
    resp = client.get("/auth/me")
    assert resp.status_code == 401


# ── Test 4: GET /auth/me con token válido retorna datos del usuario ───────────

def test_me_con_token_retorna_usuario(client):
    app.dependency_overrides[get_current_user] = lambda: _USUARIO

    resp = client.get("/auth/me")

    assert resp.status_code == 200
    data = resp.json()
    for campo in ("id", "nombre", "email", "rol", "activo"):
        assert campo in data, f"Campo '{campo}' ausente"
    assert data["email"]  == "ana@banco.pe"
    assert data["rol"]    == "analista"
    assert data["activo"] is True


# ── Test 5: POST /auth/logout retorna 200 ─────────────────────────────────────

def test_logout_retorna_200(client):
    mock_db = MagicMock()
    mock_db.auth.sign_out.return_value = None

    app.dependency_overrides[get_current_user]    = lambda: _USUARIO
    app.dependency_overrides[get_supabase_client] = lambda: mock_db

    resp = client.post("/auth/logout")

    assert resp.status_code == 200
    assert "mensaje" in resp.json()
    assert "cerrada" in resp.json()["mensaje"].lower()


def test_logout_sin_autenticacion_retorna_401(client):
    resp = client.post("/auth/logout")
    assert resp.status_code == 401


def test_logout_llama_supabase_sign_out(client):
    mock_db = MagicMock()
    mock_db.auth.sign_out.return_value = None

    app.dependency_overrides[get_current_user]    = lambda: _USUARIO
    app.dependency_overrides[get_supabase_client] = lambda: mock_db

    client.post("/auth/logout")
    mock_db.auth.sign_out.assert_called_once()
