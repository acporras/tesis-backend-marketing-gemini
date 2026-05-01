"""
Tests del router /onboarding usando FastAPI TestClient.
Las dependencias externas (Supabase, Orquestador) se mockean
mediante app.dependency_overrides para aislar la lógica HTTP.
"""
import pytest
from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock, MagicMock
from fastapi.testclient import TestClient

from main import app
from dependencies import get_current_user, get_supabase_client, get_orquestador
from routers.onboarding import _detectar_etapa

# ── Constantes de prueba ──────────────────────────────────────────────────────

_USUARIO_ANALISTA = {
    "id": "uid-analista-001",
    "nombre": "Ana Pérez",
    "email": "ana@banco.pe",
    "rol": "analista",
    "activo": True,
}

_CAMPANA_ID     = "11111111-1111-1111-1111-111111111111"
_PROSPECTO_ID   = "a" * 64

_PROSPECTO      = {
    "cliente_id_anonimizado": _PROSPECTO_ID,
    "dimension_ciclo_vida":   "onboarding",
    "fecha_apertura_cuenta":  "2024-01-01T00:00:00+00:00",
    "fecha_ultima_transaccion": "2024-02-01T00:00:00+00:00",
    "operaciones_ultimo_mes": 2,
    "canal_principal":        "digital",
    "score_crediticio":       700,
}

_CAMPANA_BD = {
    "id":               _CAMPANA_ID,
    "usuario_id":       _USUARIO_ANALISTA["id"],
    "dimension":        "onboarding",
    "instruccion_original": "Campaña para nuevos clientes digitales",
    "estado":           "borrador",
    "created_at":       "2024-06-01T10:00:00+00:00",
}

_RESULTADO_ORQUESTADOR = {
    "segmento":          {"dimension_ciclo_vida": {"eq": "onboarding"}},
    "tamanio_audiencia": 800,
    "mensaje":           "Bienvenido a nuestro banco digital.",
    "tipo_campana":      "bienvenida",
    "tono":              "cercano",
    "metricas": {
        "tiempo_respuesta_ms": 3200,
        "tasa_anonimizacion":  100.0,
        "precision_segmento":  90.0,
    },
}


# ── Helpers para construir mocks de DB ────────────────────────────────────────

def _tabla_con_data(data) -> MagicMock:
    """Mock de tabla Supabase que devuelve `data` al ejecutar cualquier query."""
    t = MagicMock()
    t.select.return_value = t
    t.eq.return_value = t
    t.order.return_value = t
    t.range.return_value = t
    t.single.return_value = t
    t.insert.return_value = t
    t.update.return_value = t
    t.execute.return_value.data = data
    return t


def _make_db(
    prospectos=None,
    campana_insertada=None,
    campana_detail=None,
    campanas_lista=None,
    campana_update=None,
) -> MagicMock:
    """
    Construye un cliente Supabase mockeado con respuestas configurables
    por nombre de tabla.
    """
    tablas = {
        "registros_campania": _tabla_con_data(
            prospectos if prospectos is not None else [_PROSPECTO]
        ),
        "campanas": MagicMock(),
        "logs_ejecucion": _tabla_con_data([]),
    }

    # Configurar tabla campanas con múltiples operaciones
    tc = tablas["campanas"]
    tc.select.return_value = tc
    tc.eq.return_value = tc
    tc.order.return_value = tc
    tc.range.return_value = tc
    tc.single.return_value = tc
    tc.update.return_value = tc

    # insert devuelve la campaña con su id
    insert_data = campana_insertada if campana_insertada is not None else [_CAMPANA_BD]
    tc.insert.return_value.execute.return_value.data = insert_data

    # update devuelve campaña actualizada
    update_data = campana_update if campana_update is not None else [_CAMPANA_BD]
    tc.update.return_value.execute.return_value.data = update_data
    tc.eq.return_value.execute.return_value.data = update_data

    # select puede devolver detalle o lista según configuración
    detail_data = campana_detail if campana_detail is not None else _CAMPANA_BD
    lista_data  = campanas_lista if campanas_lista is not None else [_CAMPANA_BD]
    tc.execute.return_value.data = detail_data   # para .single()
    tc.range.return_value.execute.return_value.data = lista_data

    mock_db = MagicMock()
    mock_db.table.side_effect = lambda nombre: tablas.get(nombre, _tabla_con_data([]))
    return mock_db


def _make_orquestador(resultado=None) -> MagicMock:
    orq = MagicMock()
    orq.procesar_instruccion = AsyncMock(
        return_value=resultado if resultado is not None else _RESULTADO_ORQUESTADOR
    )
    return orq


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def limpiar_overrides():
    """Garantiza que los overrides se resetean después de cada test."""
    yield
    app.dependency_overrides.clear()


@pytest.fixture
def client():
    """TestClient sin autenticación configurada."""
    return TestClient(app, raise_server_exceptions=True)


@pytest.fixture
def client_autenticado(client):
    """Configura auth y DB básicos para tests que requieren login."""
    app.dependency_overrides[get_current_user]    = lambda: _USUARIO_ANALISTA
    app.dependency_overrides[get_supabase_client] = lambda: _make_db()
    app.dependency_overrides[get_orquestador]     = lambda: _make_orquestador()
    return client


# ── Test 1: POST /generar con instrucción válida retorna 200 ──────────────────

def test_generar_instruccion_valida_retorna_200(client):
    app.dependency_overrides[get_current_user]    = lambda: _USUARIO_ANALISTA
    app.dependency_overrides[get_supabase_client] = lambda: _make_db()
    app.dependency_overrides[get_orquestador]     = lambda: _make_orquestador()

    resp = client.post(
        "/onboarding/generar",
        json={
            "instruccion": "Captación de nuevos clientes digitales con score alto",
            "tipo_campana": "bienvenida",
        },
    )

    assert resp.status_code == 200
    data = resp.json()
    assert "campana_id" in data
    assert data["mensaje"] == _RESULTADO_ORQUESTADOR["mensaje"]
    assert data["tamanio_audiencia"] == 800
    assert data["estado"] == "borrador"
    assert "metricas" in data
    assert data["metricas"]["tiempo_respuesta_ms"] == 3200


# ── Test 2: POST /generar sin autenticación retorna 401 ───────────────────────

def test_generar_sin_autenticacion_retorna_401(client):
    # Sin override de get_current_user → HTTPBearer devuelve None → 401
    resp = client.post(
        "/onboarding/generar",
        json={
            "instruccion": "Campaña de onboarding para clientes nuevos",
            "tipo_campana": "bienvenida",
        },
    )
    assert resp.status_code == 401


# ── Test 3: POST /generar con instruccion muy corta retorna 422 ───────────────

def test_generar_instruccion_corta_retorna_422(client):
    app.dependency_overrides[get_current_user]    = lambda: _USUARIO_ANALISTA
    app.dependency_overrides[get_supabase_client] = lambda: _make_db()
    app.dependency_overrides[get_orquestador]     = lambda: _make_orquestador()

    resp = client.post(
        "/onboarding/generar",
        json={"instruccion": "corta", "tipo_campana": "bienvenida"},  # < 10 chars
    )
    assert resp.status_code == 422


# ── Test 4: POST /generar sin prospectos disponibles retorna 404 ──────────────

def test_generar_sin_prospectos_retorna_404(client):
    app.dependency_overrides[get_current_user]    = lambda: _USUARIO_ANALISTA
    app.dependency_overrides[get_supabase_client] = lambda: _make_db(prospectos=[])
    app.dependency_overrides[get_orquestador]     = lambda: _make_orquestador()

    resp = client.post(
        "/onboarding/generar",
        json={
            "instruccion": "Campaña para nuevos clientes digitales en Lima",
            "tipo_campana": "activacion",
        },
    )
    assert resp.status_code == 404
    assert "prospectos" in resp.json()["detail"].lower()


# ── Test 5: GET /mis-campanas solo retorna campañas del usuario ───────────────

def test_mis_campanas_solo_del_usuario(client):
    campanas_usuario = [_CAMPANA_BD]
    db_mock = _make_db(campanas_lista=campanas_usuario)
    app.dependency_overrides[get_current_user]    = lambda: _USUARIO_ANALISTA
    app.dependency_overrides[get_supabase_client] = lambda: db_mock

    resp = client.get("/onboarding/mis-campanas")

    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, list)

    # Verificar que el filtro por usuario_id fue aplicado
    mock_table = db_mock.table("campanas")
    mock_table.eq.assert_any_call("usuario_id", _USUARIO_ANALISTA["id"])
    mock_table.eq.assert_any_call("dimension", "onboarding")


# ── Test 6: GET /mis-campanas soporta paginación ─────────────────────────────

def test_mis_campanas_paginacion(client):
    app.dependency_overrides[get_current_user]    = lambda: _USUARIO_ANALISTA
    app.dependency_overrides[get_supabase_client] = lambda: _make_db()

    resp = client.get("/onboarding/mis-campanas?skip=10&limit=5")
    assert resp.status_code == 200


# ── Test 7: PATCH enviar-aprobacion cambia estado correctamente ───────────────

def test_enviar_aprobacion_cambia_estado(client):
    campana_borrador = {**_CAMPANA_BD, "estado": "borrador"}

    db = _make_db(campana_detail=campana_borrador)
    # Hacer que .single().execute().data devuelva la campaña
    db.table("campanas").execute.return_value.data = campana_borrador

    app.dependency_overrides[get_current_user]    = lambda: _USUARIO_ANALISTA
    app.dependency_overrides[get_supabase_client] = lambda: db

    resp = client.patch(f"/onboarding/{_CAMPANA_ID}/enviar-aprobacion")

    assert resp.status_code == 200
    data = resp.json()
    assert data["estado_nuevo"] == "pendiente_aprobacion"
    assert data["campana_id"] == _CAMPANA_ID


# ── Test 8: PATCH enviar-aprobacion desde estado incorrecto retorna 409 ───────

def test_enviar_aprobacion_estado_incorrecto_retorna_409(client):
    campana_pendiente = {**_CAMPANA_BD, "estado": "pendiente_aprobacion"}

    db = _make_db(campana_detail=campana_pendiente)
    db.table("campanas").execute.return_value.data = campana_pendiente

    app.dependency_overrides[get_current_user]    = lambda: _USUARIO_ANALISTA
    app.dependency_overrides[get_supabase_client] = lambda: db

    resp = client.patch(f"/onboarding/{_CAMPANA_ID}/enviar-aprobacion")

    assert resp.status_code == 409
    assert "pendiente_aprobacion" in resp.json()["detail"]


# ── Test 9: GET /prospectos/{id}/etapa retorna etapa correcta ─────────────────

def test_etapa_prospecto_sin_activar(client):
    prospecto_inactivo = {
        **_PROSPECTO,
        "operaciones_ultimo_mes": 0,        # sin transacciones
        "fecha_ultima_transaccion": "2023-01-01T00:00:00+00:00",
    }
    db = _make_db()
    db.table("registros_campania").execute.return_value.data = prospecto_inactivo

    app.dependency_overrides[get_current_user]    = lambda: _USUARIO_ANALISTA
    app.dependency_overrides[get_supabase_client] = lambda: db

    resp = client.get(f"/onboarding/prospectos/{_PROSPECTO_ID}/etapa")

    assert resp.status_code == 200
    data = resp.json()
    assert data["etapa"] == "sin_activar"
    assert "recomendacion" in data


def test_etapa_prospecto_activo_completo(client):
    apertura = (datetime.now(timezone.utc) - timedelta(days=120)).isoformat()
    ultima_tx = (datetime.now(timezone.utc) - timedelta(days=5)).isoformat()

    prospecto_activo = {
        **_PROSPECTO,
        "operaciones_ultimo_mes":   10,
        "fecha_apertura_cuenta":    apertura,
        "fecha_ultima_transaccion": ultima_tx,
    }
    db = _make_db()
    db.table("registros_campania").execute.return_value.data = prospecto_activo

    app.dependency_overrides[get_current_user]    = lambda: _USUARIO_ANALISTA
    app.dependency_overrides[get_supabase_client] = lambda: db

    resp = client.get(f"/onboarding/prospectos/{_PROSPECTO_ID}/etapa")

    assert resp.status_code == 200
    assert resp.json()["etapa"] == "activo_completo"


def test_etapa_prospecto_no_encontrado_retorna_404(client):
    db = _make_db()
    db.table("registros_campania").execute.return_value.data = None

    app.dependency_overrides[get_current_user]    = lambda: _USUARIO_ANALISTA
    app.dependency_overrides[get_supabase_client] = lambda: db

    resp = client.get(f"/onboarding/prospectos/id-inexistente/etapa")

    assert resp.status_code == 404


# ── Test 10: _detectar_etapa — lógica pura (sin HTTP) ────────────────────────

def test_detectar_etapa_sin_activar_ops_cero():
    registro = {"operaciones_ultimo_mes": 0}
    etapa, _ = _detectar_etapa(registro)
    assert etapa == "sin_activar"


def test_detectar_etapa_activacion_parcial():
    apertura = (datetime.now(timezone.utc) - timedelta(days=20)).isoformat()
    ultima   = (datetime.now(timezone.utc) - timedelta(days=3)).isoformat()
    registro = {
        "operaciones_ultimo_mes":   3,
        "fecha_apertura_cuenta":    apertura,
        "fecha_ultima_transaccion": ultima,
    }
    etapa, rec = _detectar_etapa(registro)
    assert etapa == "activacion_parcial"
    assert "transacción" in rec.lower()


def test_detectar_etapa_activo_completo():
    apertura = (datetime.now(timezone.utc) - timedelta(days=150)).isoformat()
    ultima   = (datetime.now(timezone.utc) - timedelta(days=2)).isoformat()
    registro = {
        "operaciones_ultimo_mes":   8,
        "fecha_apertura_cuenta":    apertura,
        "fecha_ultima_transaccion": ultima,
    }
    etapa, _ = _detectar_etapa(registro)
    assert etapa == "activo_completo"
