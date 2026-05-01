"""
Tests del router /logs usando FastAPI TestClient.
"""
import pytest
from unittest.mock import MagicMock
from fastapi.testclient import TestClient

from main import app
from dependencies import get_current_user, get_supabase_client

_CAMPANA_ID = "66666666-6666-6666-6666-666666666666"

_USUARIO_ANALISTA = {
    "id": "uid-analista-log-001", "nombre": "Pedro Rios",
    "email": "pedro@banco.pe", "rol": "analista", "activo": True,
}
_USUARIO_COORDINADOR = {**_USUARIO_ANALISTA, "id": "uid-coord-log-001", "rol": "coordinador"}

_LOG = {
    "id": "log-uuid-001", "campana_id": _CAMPANA_ID,
    "tiempo_respuesta_ms": 2000, "tasa_anonimizacion": 100.0,
    "precision_segmento": 90.0, "created_at": "2024-06-01T10:00:02+00:00",
}
_CAMPANA = {"id": _CAMPANA_ID, "usuario_id": _USUARIO_ANALISTA["id"]}


@pytest.fixture(autouse=True)
def limpiar_overrides():
    yield
    app.dependency_overrides.clear()


@pytest.fixture
def client():
    return TestClient(app, raise_server_exceptions=True)


def _t(data) -> MagicMock:
    t = MagicMock()
    t.select.return_value = t
    t.eq.return_value = t
    t.gte.return_value = t
    t.lte.return_value = t
    t.order.return_value = t
    t.range.return_value = t
    t.single.return_value = t
    t.execute.return_value.data = data
    return t


def _make_db(logs=None, campana=None) -> MagicMock:
    db = MagicMock()
    db.table.side_effect = lambda n: {
        "logs_ejecucion": _t(logs if logs is not None else [_LOG]),
        "campanas":       _t(campana if campana is not None else _CAMPANA),
    }.get(n, _t([]))
    return db


# GET /logs — coordinador

def test_logs_coordinador_retorna_lista(client):
    app.dependency_overrides[get_current_user]    = lambda: _USUARIO_COORDINADOR
    app.dependency_overrides[get_supabase_client] = lambda: _make_db()
    resp = client.get("/logs")
    assert resp.status_code == 200
    assert isinstance(resp.json(), list)
    assert len(resp.json()) == 1


def test_logs_incluye_campos_indicadores(client):
    app.dependency_overrides[get_current_user]    = lambda: _USUARIO_COORDINADOR
    app.dependency_overrides[get_supabase_client] = lambda: _make_db()
    resp = client.get("/logs")
    log = resp.json()[0]
    for c in ("tiempo_respuesta_ms", "tasa_anonimizacion", "precision_segmento"):
        assert c in log


def test_logs_lista_vacia(client):
    app.dependency_overrides[get_current_user]    = lambda: _USUARIO_COORDINADOR
    app.dependency_overrides[get_supabase_client] = lambda: _make_db(logs=[])
    resp = client.get("/logs")
    assert resp.status_code == 200
    assert resp.json() == []


def test_logs_paginacion(client):
    app.dependency_overrides[get_current_user]    = lambda: _USUARIO_COORDINADOR
    app.dependency_overrides[get_supabase_client] = lambda: _make_db(logs=[])
    resp = client.get("/logs?skip=0&limit=5")
    assert resp.status_code == 200


def test_logs_filtro_campana_id(client):
    app.dependency_overrides[get_current_user]    = lambda: _USUARIO_COORDINADOR
    app.dependency_overrides[get_supabase_client] = lambda: _make_db()
    resp = client.get(f"/logs?campana_id={_CAMPANA_ID}")
    assert resp.status_code == 200


def test_logs_filtro_fechas(client):
    app.dependency_overrides[get_current_user]    = lambda: _USUARIO_COORDINADOR
    app.dependency_overrides[get_supabase_client] = lambda: _make_db(logs=[])
    resp = client.get("/logs?fecha_desde=2024-01-01&fecha_hasta=2024-12-31")
    assert resp.status_code == 200


# GET /logs — sin auth / analista

def test_logs_sin_autenticacion_retorna_401(client):
    resp = client.get("/logs")
    assert resp.status_code == 401


def test_logs_analista_retorna_403(client):
    app.dependency_overrides[get_current_user]    = lambda: _USUARIO_ANALISTA
    app.dependency_overrides[get_supabase_client] = lambda: _make_db()
    resp = client.get("/logs")
    assert resp.status_code == 403


# GET /logs/{campana_id}

def test_logs_por_campana_coordinador(client):
    app.dependency_overrides[get_current_user]    = lambda: _USUARIO_COORDINADOR
    app.dependency_overrides[get_supabase_client] = lambda: _make_db()
    resp = client.get(f"/logs/{_CAMPANA_ID}")
    assert resp.status_code == 200
    assert isinstance(resp.json(), list)


def test_logs_por_campana_analista_propia(client):
    app.dependency_overrides[get_current_user]    = lambda: _USUARIO_ANALISTA
    app.dependency_overrides[get_supabase_client] = lambda: _make_db()
    resp = client.get(f"/logs/{_CAMPANA_ID}")
    assert resp.status_code == 200


def test_logs_por_campana_analista_ajena_retorna_403(client):
    campana_ajena = {**_CAMPANA, "usuario_id": "otro-uid"}
    app.dependency_overrides[get_current_user]    = lambda: _USUARIO_ANALISTA
    app.dependency_overrides[get_supabase_client] = lambda: _make_db(campana=campana_ajena)
    resp = client.get(f"/logs/{_CAMPANA_ID}")
    assert resp.status_code == 403


def test_logs_por_campana_no_existente_retorna_404(client):
    ex = MagicMock(); ex.data = None
    s  = MagicMock(); s.execute.return_value = ex
    eq = MagicMock(); eq.single.return_value = s
    sel= MagicMock(); sel.eq.return_value = eq
    tc = MagicMock(); tc.select.return_value = sel

    db = MagicMock()
    db.table.side_effect = lambda n: tc if n == "campanas" else _t([])

    app.dependency_overrides[get_current_user]    = lambda: _USUARIO_COORDINADOR
    app.dependency_overrides[get_supabase_client] = lambda: db
    resp = client.get(f"/logs/{_CAMPANA_ID}")
    assert resp.status_code == 404


def test_logs_por_campana_sin_autenticacion_retorna_401(client):
    resp = client.get(f"/logs/{_CAMPANA_ID}")
    assert resp.status_code == 401
