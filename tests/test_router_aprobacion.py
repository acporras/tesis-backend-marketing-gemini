"""
Tests del router /aprobacion (HU-08, HU-10) usando FastAPI TestClient.
Las dependencias externas (Supabase) se mockean con
app.dependency_overrides para aislar la lógica HTTP.
"""
import pytest
from datetime import datetime, timezone
from unittest.mock import MagicMock
from fastapi.testclient import TestClient

from main import app
from dependencies import get_current_user, get_supabase_client

# ── Constantes de prueba ──────────────────────────────────────────────────────

_CAMPANA_ID = "44444444-4444-4444-4444-444444444444"

_USUARIO_ANALISTA = {
    "id":     "uid-analista-apro-001",
    "nombre": "Luis Torres",
    "email":  "luis@banco.pe",
    "rol":    "analista",
    "activo": True,
}

_USUARIO_COORDINADOR = {
    **_USUARIO_ANALISTA,
    "id":  "uid-coord-apro-001",
    "rol": "coordinador",
}

_CAMPANA_PENDIENTE = {
    "id":                    _CAMPANA_ID,
    "usuario_id":            _USUARIO_ANALISTA["id"],
    "dimension":             "onboarding",
    "instruccion_original":  "Campaña de bienvenida para nuevos clientes digitales",
    "segmento_generado":     {"dimension_ciclo_vida": {"eq": "onboarding"}},
    "mensaje_generado":      "Bienvenido a nuestro banco digital.",
    "estado":                "pendiente_aprobacion",
    "tiempo_respuesta_ms":   2100,
    "aprobado_por":          None,
    "aprobado_at":           None,
    "created_at":            "2024-06-01T08:00:00+00:00",
}

_CAMPANA_APROBADA = {**_CAMPANA_PENDIENTE, "estado": "aprobada"}
_CAMPANA_BORRADOR = {**_CAMPANA_PENDIENTE, "estado": "borrador"}


# ── Helpers de mocks ──────────────────────────────────────────────────────────

def _tabla_con_data(data) -> MagicMock:
    t = MagicMock()
    t.select.return_value  = t
    t.eq.return_value      = t
    t.in_.return_value     = t
    t.gte.return_value     = t
    t.lte.return_value     = t
    t.order.return_value   = t
    t.range.return_value   = t
    t.single.return_value  = t
    t.insert.return_value  = t
    t.update.return_value  = t
    t.execute.return_value.data = data
    return t


def _make_db(
    campana_detail=None,
    campanas_lista: list | None = None,
) -> MagicMock:
    """
    Construye un mock de DB con chains explícitas.

    El router /pendientes usa:   .select().eq(estado).order().range().execute()
    El router /historial usa:    .select().in_(estados).eq/gte/lte...order().range().execute()
    Los endpoints /{id} usan:    .select().eq(id).single().execute()
    """
    detalle = campana_detail if campana_detail is not None else _CAMPANA_PENDIENTE
    lista   = campanas_lista if campanas_lista  is not None else [_CAMPANA_PENDIENTE]

    # ── single() chain: .select().eq(id).single().execute().data = detalle ────
    mock_single_exec      = MagicMock()
    mock_single_exec.data = detalle
    mock_single           = MagicMock()
    mock_single.execute.return_value = mock_single_exec

    # ── range() terminus: .range().execute().data = lista ─────────────────────
    mock_range_exec      = MagicMock()
    mock_range_exec.data = lista
    mock_range           = MagicMock()
    mock_range.execute.return_value = mock_range_exec

    # ── order() → range() ────────────────────────────────────────────────────
    mock_order = MagicMock()
    mock_order.range.return_value = mock_range

    # ── chain para /pendientes: .select().eq(estado).order().range().execute()
    #    y para /{id}:           .select().eq(id).single().execute()
    mock_eq_for_select = MagicMock()
    mock_eq_for_select.order.return_value  = mock_order
    mock_eq_for_select.range.return_value  = mock_range
    mock_eq_for_select.single.return_value = mock_single
    # encadenamiento adicional .eq().eq() (ej. .eq(id).eq(dim))
    mock_eq_for_select.eq.return_value     = mock_eq_for_select

    # ── chain para /historial: .select().in_(...).eq().gte().lte().order().range()
    mock_in_ = MagicMock()
    mock_in_.eq.return_value  = mock_in_
    mock_in_.gte.return_value = mock_in_
    mock_in_.lte.return_value = mock_in_
    mock_in_.order.return_value = mock_order
    mock_in_.execute.return_value.data = lista

    # ── select() raiz ─────────────────────────────────────────────────────────
    mock_select = MagicMock()
    mock_select.eq.return_value  = mock_eq_for_select
    mock_select.in_.return_value = mock_in_

    # ── update().eq().execute() ───────────────────────────────────────────────
    mock_update_eq_exec      = MagicMock()
    mock_update_eq_exec.data = [detalle] if detalle else []
    mock_update_eq           = MagicMock()
    mock_update_eq.execute.return_value = mock_update_eq_exec
    mock_update              = MagicMock()
    mock_update.eq.return_value = mock_update_eq

    # ── insert().execute() ────────────────────────────────────────────────────
    mock_insert_exec      = MagicMock()
    mock_insert_exec.data = [{"id": "log-id"}]
    mock_insert           = MagicMock()
    mock_insert.execute.return_value = mock_insert_exec

    # ── tabla campanas ────────────────────────────────────────────────────────
    t_camp = MagicMock()
    t_camp.select.return_value = mock_select
    t_camp.update.return_value = mock_update
    t_camp.insert.return_value = mock_insert

    t_logs = _tabla_con_data([])

    mock_db = MagicMock()
    mock_db.table.side_effect = lambda nombre: {
        "campanas":       t_camp,
        "logs_ejecucion": t_logs,
    }.get(nombre, _tabla_con_data([]))

    return mock_db


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def limpiar_overrides():
    yield
    app.dependency_overrides.clear()


@pytest.fixture
def client():
    return TestClient(app, raise_server_exceptions=True)


# ── Test 1: GET /pendientes retorna lista correcta ────────────────────────────

def test_pendientes_retorna_lista_de_campanas(client):
    app.dependency_overrides[get_current_user]    = lambda: _USUARIO_COORDINADOR
    app.dependency_overrides[get_supabase_client] = lambda: _make_db(
        campanas_lista=[_CAMPANA_PENDIENTE]
    )

    resp = client.get("/aprobacion/pendientes")

    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, list)
    assert len(data) == 1
    assert data[0]["campana_id"] == _CAMPANA_ID
    assert data[0]["dimension"] == "onboarding"


def test_pendientes_incluye_campos_requeridos(client):
    app.dependency_overrides[get_current_user]    = lambda: _USUARIO_COORDINADOR
    app.dependency_overrides[get_supabase_client] = lambda: _make_db()

    resp = client.get("/aprobacion/pendientes")

    assert resp.status_code == 200
    item = resp.json()[0]
    for campo in ("campana_id", "dimension", "instruccion_original",
                  "segmento_generado", "mensaje_generado",
                  "tiempo_respuesta_ms", "usuario_id", "created_at"):
        assert campo in item, f"Campo '{campo}' ausente"


def test_pendientes_lista_vacia_cuando_no_hay(client):
    app.dependency_overrides[get_current_user]    = lambda: _USUARIO_COORDINADOR
    app.dependency_overrides[get_supabase_client] = lambda: _make_db(campanas_lista=[])

    resp = client.get("/aprobacion/pendientes")
    assert resp.status_code == 200
    assert resp.json() == []


# ── Test 2: GET /pendientes sin autenticación retorna 401 ─────────────────────

def test_pendientes_sin_autenticacion_retorna_401(client):
    resp = client.get("/aprobacion/pendientes")
    assert resp.status_code == 401


# ── Test 3: GET /pendientes como analista retorna 403 ─────────────────────────

def test_pendientes_analista_retorna_403(client):
    app.dependency_overrides[get_current_user]    = lambda: _USUARIO_ANALISTA
    app.dependency_overrides[get_supabase_client] = lambda: _make_db()

    resp = client.get("/aprobacion/pendientes")
    assert resp.status_code == 403


def test_pendientes_paginacion(client):
    app.dependency_overrides[get_current_user]    = lambda: _USUARIO_COORDINADOR
    app.dependency_overrides[get_supabase_client] = lambda: _make_db(campanas_lista=[])

    resp = client.get("/aprobacion/pendientes?skip=10&limit=5")
    assert resp.status_code == 200


# ── Test 4: POST /aprobar cambia estado a 'aprobada' ─────────────────────────

def test_aprobar_campaña_pendiente_retorna_200(client):
    db_mock = _make_db(campana_detail=_CAMPANA_PENDIENTE)

    app.dependency_overrides[get_current_user]    = lambda: _USUARIO_COORDINADOR
    app.dependency_overrides[get_supabase_client] = lambda: db_mock

    resp = client.post(f"/aprobacion/{_CAMPANA_ID}/aprobar", json={})

    assert resp.status_code == 200
    data = resp.json()
    assert data["estado_nuevo"] == "aprobada"
    assert data["campana_id"]   == _CAMPANA_ID
    assert data["aprobado_por"] == _USUARIO_COORDINADOR["id"]
    assert "timestamp" in data


def test_aprobar_campaña_con_comentario(client):
    db_mock = _make_db(campana_detail=_CAMPANA_PENDIENTE)

    app.dependency_overrides[get_current_user]    = lambda: _USUARIO_COORDINADOR
    app.dependency_overrides[get_supabase_client] = lambda: db_mock

    resp = client.post(
        f"/aprobacion/{_CAMPANA_ID}/aprobar",
        json={"comentario": "Campaña revisada y aceptada para producción"},
    )
    assert resp.status_code == 200


def test_aprobar_sin_autenticacion_retorna_401(client):
    resp = client.post(f"/aprobacion/{_CAMPANA_ID}/aprobar", json={})
    assert resp.status_code == 401


def test_aprobar_como_analista_retorna_403(client):
    app.dependency_overrides[get_current_user]    = lambda: _USUARIO_ANALISTA
    app.dependency_overrides[get_supabase_client] = lambda: _make_db()

    resp = client.post(f"/aprobacion/{_CAMPANA_ID}/aprobar", json={})
    assert resp.status_code == 403


# ── Test 5: POST /rechazar sin comentario retorna 422 ─────────────────────────

def test_rechazar_sin_comentario_retorna_422(client):
    app.dependency_overrides[get_current_user]    = lambda: _USUARIO_COORDINADOR
    app.dependency_overrides[get_supabase_client] = lambda: _make_db()

    # Body sin comentario (campo obligatorio)
    resp = client.post(f"/aprobacion/{_CAMPANA_ID}/rechazar", json={})
    assert resp.status_code == 422


def test_rechazar_comentario_demasiado_corto_retorna_422(client):
    app.dependency_overrides[get_current_user]    = lambda: _USUARIO_COORDINADOR
    app.dependency_overrides[get_supabase_client] = lambda: _make_db()

    resp = client.post(
        f"/aprobacion/{_CAMPANA_ID}/rechazar",
        json={"comentario": "corto"},
    )
    assert resp.status_code == 422


# ── Test 6: POST /rechazar con comentario cambia estado a 'rechazada' ─────────

def test_rechazar_campaña_pendiente_retorna_200(client):
    db_mock = _make_db(campana_detail=_CAMPANA_PENDIENTE)

    app.dependency_overrides[get_current_user]    = lambda: _USUARIO_COORDINADOR
    app.dependency_overrides[get_supabase_client] = lambda: db_mock

    resp = client.post(
        f"/aprobacion/{_CAMPANA_ID}/rechazar",
        json={"comentario": "El mensaje no cumple el tono corporativo requerido por el banco"},
    )

    assert resp.status_code == 200
    data = resp.json()
    assert data["estado_nuevo"]  == "rechazada"
    assert data["campana_id"]    == _CAMPANA_ID
    assert data["rechazado_por"] == _USUARIO_COORDINADOR["id"]
    assert "comentario" in data
    assert "timestamp" in data


def test_rechazar_sin_autenticacion_retorna_401(client):
    resp = client.post(
        f"/aprobacion/{_CAMPANA_ID}/rechazar",
        json={"comentario": "Motivo de rechazo suficientemente largo para pasar validación"},
    )
    assert resp.status_code == 401


# ── Test 7: POST /aprobar campaña que no está pendiente retorna 409 ───────────

def test_aprobar_campaña_no_pendiente_retorna_409(client):
    # Campaña ya aprobada → no se puede aprobar de nuevo
    db_mock = _make_db(campana_detail=_CAMPANA_APROBADA)

    app.dependency_overrides[get_current_user]    = lambda: _USUARIO_COORDINADOR
    app.dependency_overrides[get_supabase_client] = lambda: db_mock

    resp = client.post(f"/aprobacion/{_CAMPANA_ID}/aprobar", json={})

    assert resp.status_code == 409
    assert "pendiente_aprobacion" in resp.json()["detail"]


def test_rechazar_campaña_no_pendiente_retorna_409(client):
    # Campaña en borrador → no se puede rechazar
    db_mock = _make_db(campana_detail=_CAMPANA_BORRADOR)

    app.dependency_overrides[get_current_user]    = lambda: _USUARIO_COORDINADOR
    app.dependency_overrides[get_supabase_client] = lambda: db_mock

    resp = client.post(
        f"/aprobacion/{_CAMPANA_ID}/rechazar",
        json={"comentario": "No se puede rechazar porque está en borrador todavía"},
    )
    assert resp.status_code == 409


def test_aprobar_campaña_no_encontrada_retorna_404(client):
    """Cuando la campána no existe el router devuelve 404."""
    mock_exec = MagicMock()
    mock_exec.data = None
    mock_single = MagicMock()
    mock_single.execute.return_value = mock_exec
    mock_eq = MagicMock()
    mock_eq.single.return_value = mock_single
    mock_select = MagicMock()
    mock_select.eq.return_value = mock_eq
    t_camp = MagicMock()
    t_camp.select.return_value = mock_select

    db_none = MagicMock()
    db_none.table.side_effect = lambda n: t_camp if n == "campanas" else _tabla_con_data([])

    app.dependency_overrides[get_current_user]    = lambda: _USUARIO_COORDINADOR
    app.dependency_overrides[get_supabase_client] = lambda: db_none

    resp = client.post(f"/aprobacion/{_CAMPANA_ID}/aprobar", json={})
    assert resp.status_code == 404


# ── Test 8: GET /historial retorna lista con filtros ─────────────────────────

def test_historial_retorna_lista(client):
    campanas_historial = [
        {**_CAMPANA_PENDIENTE, "estado": "aprobada"},
        {**_CAMPANA_PENDIENTE, "id": "55555555-5555-5555-5555-555555555555", "estado": "rechazada"},
    ]
    app.dependency_overrides[get_current_user]    = lambda: _USUARIO_COORDINADOR
    app.dependency_overrides[get_supabase_client] = lambda: _make_db(
        campanas_lista=campanas_historial
    )

    resp = client.get("/aprobacion/historial")

    assert resp.status_code == 200
    assert isinstance(resp.json(), list)


def test_historial_sin_autenticacion_retorna_401(client):
    resp = client.get("/aprobacion/historial")
    assert resp.status_code == 401


def test_historial_analista_retorna_403(client):
    app.dependency_overrides[get_current_user]    = lambda: _USUARIO_ANALISTA
    app.dependency_overrides[get_supabase_client] = lambda: _make_db()

    resp = client.get("/aprobacion/historial")
    assert resp.status_code == 403


def test_historial_con_filtros_dimension(client):
    app.dependency_overrides[get_current_user]    = lambda: _USUARIO_COORDINADOR
    app.dependency_overrides[get_supabase_client] = lambda: _make_db(campanas_lista=[])

    resp = client.get("/aprobacion/historial?dimension=fidelizacion")
    assert resp.status_code == 200


def test_historial_con_filtros_fecha(client):
    app.dependency_overrides[get_current_user]    = lambda: _USUARIO_COORDINADOR
    app.dependency_overrides[get_supabase_client] = lambda: _make_db(campanas_lista=[])

    resp = client.get("/aprobacion/historial?fecha_desde=2024-01-01&fecha_hasta=2024-12-31")
    assert resp.status_code == 200


def test_historial_paginacion(client):
    app.dependency_overrides[get_current_user]    = lambda: _USUARIO_COORDINADOR
    app.dependency_overrides[get_supabase_client] = lambda: _make_db(campanas_lista=[])

    resp = client.get("/aprobacion/historial?skip=0&limit=10")
    assert resp.status_code == 200


# ── Tests de GET /{campana_id} ────────────────────────────────────────────────

def test_detalle_campaña_para_review_incluye_logs(client):
    db_mock = _make_db(campana_detail=_CAMPANA_PENDIENTE)
    logs = [{"id": "log-1", "campana_id": _CAMPANA_ID, "tiempo_respuesta_ms": 2100}]
    db_mock.table("logs_ejecucion").execute.return_value.data = logs

    app.dependency_overrides[get_current_user]    = lambda: _USUARIO_COORDINADOR
    app.dependency_overrides[get_supabase_client] = lambda: db_mock

    resp = client.get(f"/aprobacion/{_CAMPANA_ID}")

    assert resp.status_code == 200
    assert "logs" in resp.json()


def test_detalle_campaña_analista_retorna_403(client):
    app.dependency_overrides[get_current_user]    = lambda: _USUARIO_ANALISTA
    app.dependency_overrides[get_supabase_client] = lambda: _make_db()

    resp = client.get(f"/aprobacion/{_CAMPANA_ID}")
    assert resp.status_code == 403


def test_detalle_campaña_no_encontrada_retorna_404(client):
    # Mock autónomo con data=None en single()
    mock_exec = MagicMock()
    mock_exec.data = None
    mock_single = MagicMock()
    mock_single.execute.return_value = mock_exec
    mock_eq = MagicMock()
    mock_eq.single.return_value = mock_single
    mock_select = MagicMock()
    mock_select.eq.return_value = mock_eq
    t_camp = MagicMock()
    t_camp.select.return_value = mock_select

    db_none = MagicMock()
    db_none.table.side_effect = lambda n: t_camp if n == "campanas" else _tabla_con_data([])

    app.dependency_overrides[get_current_user]    = lambda: _USUARIO_COORDINADOR
    app.dependency_overrides[get_supabase_client] = lambda: db_none

    resp = client.get(f"/aprobacion/{_CAMPANA_ID}")
    assert resp.status_code == 404
