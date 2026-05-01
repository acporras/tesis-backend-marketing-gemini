"""
Tests del router /metricas usando FastAPI TestClient.
"""
import pytest
from unittest.mock import MagicMock
from fastapi.testclient import TestClient

from main import app
from dependencies import get_current_user, get_supabase_client

_USUARIO_ANALISTA    = {"id": "uid-met-001", "email": "m@b.pe", "rol": "analista",    "activo": True, "nombre": "M"}
_USUARIO_COORDINADOR = {**_USUARIO_ANALISTA, "id": "uid-met-coord", "rol": "coordinador"}

_LOGS = [
    {"tiempo_respuesta_ms": 2000, "tasa_anonimizacion": 100.0, "precision_segmento": 90.0},
    {"tiempo_respuesta_ms": 3000, "tasa_anonimizacion": 99.5,  "precision_segmento": 88.0},
]
_CAMPANAS = [
    {"estado": "borrador",             "dimension": "onboarding"},
    {"estado": "pendiente_aprobacion", "dimension": "fidelizacion"},
    {"estado": "aprobada",             "dimension": "onboarding"},
]


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
    t.execute.return_value.data = data
    return t


def _make_db(logs=None, campanas=None) -> MagicMock:
    db = MagicMock()
    db.table.side_effect = lambda n: {
        "logs_ejecucion": _t(logs     if logs     is not None else _LOGS),
        "campanas":       _t(campanas if campanas is not None else _CAMPANAS),
    }.get(n, _t([]))
    return db


# GET /metricas/resumen

def test_resumen_coordinador_retorna_200(client):
    app.dependency_overrides[get_current_user]    = lambda: _USUARIO_COORDINADOR
    app.dependency_overrides[get_supabase_client] = lambda: _make_db()
    resp = client.get("/metricas/resumen")
    assert resp.status_code == 200


def test_resumen_incluye_i1_i2_i3(client):
    app.dependency_overrides[get_current_user]    = lambda: _USUARIO_COORDINADOR
    app.dependency_overrides[get_supabase_client] = lambda: _make_db()
    data = client.get("/metricas/resumen").json()
    for key in ("I1", "I2", "I3"):
        assert key in data
        assert "valor"       in data[key]
        assert "meta"        in data[key]
        assert "cumple_meta" in data[key]
        assert "unidad"      in data[key]


def test_resumen_incluye_totales_campanas(client):
    app.dependency_overrides[get_current_user]    = lambda: _USUARIO_COORDINADOR
    app.dependency_overrides[get_supabase_client] = lambda: _make_db()
    data = client.get("/metricas/resumen").json()
    assert "total_campanas"         in data
    assert "campanas_por_estado"    in data
    assert "campanas_por_dimension" in data
    assert data["total_campanas"] == 3


def test_resumen_calcula_promedios_correctos(client):
    app.dependency_overrides[get_current_user]    = lambda: _USUARIO_COORDINADOR
    app.dependency_overrides[get_supabase_client] = lambda: _make_db()
    data = client.get("/metricas/resumen").json()
    # I1 promedio = (2000 + 3000) / 2 = 2500
    assert data["I1"]["valor"] == 2500.0
    # I2 promedio = (100.0 + 99.5) / 2 = 99.75
    assert data["I2"]["valor"] == 99.75
    # I3 promedio = (90.0 + 88.0) / 2 = 89.0
    assert data["I3"]["valor"] == 89.0


def test_resumen_cumple_meta_i1(client):
    """Con promedio 2500ms (< 30000ms meta), I1 debe cumplir meta."""
    app.dependency_overrides[get_current_user]    = lambda: _USUARIO_COORDINADOR
    app.dependency_overrides[get_supabase_client] = lambda: _make_db()
    data = client.get("/metricas/resumen").json()
    assert data["I1"]["cumple_meta"] is True


def test_resumen_sin_logs_retorna_ceros(client):
    app.dependency_overrides[get_current_user]    = lambda: _USUARIO_COORDINADOR
    app.dependency_overrides[get_supabase_client] = lambda: _make_db(logs=[], campanas=[])
    data = client.get("/metricas/resumen").json()
    assert data["I1"]["valor"] == 0.0
    assert data["total_campanas"] == 0


# GET /metricas/resumen — sin auth / analista

def test_resumen_sin_autenticacion_retorna_401(client):
    resp = client.get("/metricas/resumen")
    assert resp.status_code == 401


def test_resumen_analista_retorna_403(client):
    app.dependency_overrides[get_current_user]    = lambda: _USUARIO_ANALISTA
    app.dependency_overrides[get_supabase_client] = lambda: _make_db()
    resp = client.get("/metricas/resumen")
    assert resp.status_code == 403


# GET /metricas/indicadores

def test_indicadores_coordinador_retorna_200(client):
    app.dependency_overrides[get_current_user]    = lambda: _USUARIO_COORDINADOR
    app.dependency_overrides[get_supabase_client] = lambda: _make_db()
    resp = client.get("/metricas/indicadores")
    assert resp.status_code == 200


def test_indicadores_incluye_tres_indicadores(client):
    app.dependency_overrides[get_current_user]    = lambda: _USUARIO_COORDINADOR
    app.dependency_overrides[get_supabase_client] = lambda: _make_db()
    data = client.get("/metricas/indicadores").json()
    assert "I1_tiempo_respuesta_ms"  in data
    assert "I2_tasa_anonimizacion"   in data
    assert "I3_precision_segmento"   in data


def test_indicadores_tienen_cumple_meta(client):
    app.dependency_overrides[get_current_user]    = lambda: _USUARIO_COORDINADOR
    app.dependency_overrides[get_supabase_client] = lambda: _make_db()
    data = client.get("/metricas/indicadores").json()
    for key in ("I1_tiempo_respuesta_ms", "I2_tasa_anonimizacion", "I3_precision_segmento"):
        assert "cumple_meta" in data[key], f"'cumple_meta' ausente en {key}"
        assert isinstance(data[key]["cumple_meta"], bool)


def test_indicadores_sin_autenticacion_retorna_401(client):
    resp = client.get("/metricas/indicadores")
    assert resp.status_code == 401


def test_indicadores_analista_retorna_403(client):
    app.dependency_overrides[get_current_user]    = lambda: _USUARIO_ANALISTA
    app.dependency_overrides[get_supabase_client] = lambda: _make_db()
    resp = client.get("/metricas/indicadores")
    assert resp.status_code == 403
