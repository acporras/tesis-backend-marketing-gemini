"""
Tests del router /reactivacion (HU-05 y HU-06) usando FastAPI TestClient.
Las dependencias externas (Supabase, Orquestador) se mockean con
app.dependency_overrides para aislar la lógica HTTP y las reglas de negocio.
"""
import pytest
from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock, MagicMock
from fastapi.testclient import TestClient

from main import app
from dependencies import get_current_user, get_supabase_client, get_orquestador


# ── Constantes de prueba ──────────────────────────────────────────────────────

_CAMPANA_ID = "33333333-3333-3333-3333-333333333333"

_USUARIO_ANALISTA = {
    "id":     "uid-analista-react-001",
    "nombre": "Carlos Ramos",
    "email":  "carlos@banco.pe",
    "rol":    "analista",
    "activo": True,
}

_USUARIO_COORDINADOR = {**_USUARIO_ANALISTA, "id": "uid-coord-react-001", "rol": "coordinador"}

_CAMPANA_BORRADOR = {
    "id":                         _CAMPANA_ID,
    "usuario_id":                 None,            # campaña del sistema
    "dimension":                  "reactivacion",
    "instruccion_original":       "Generar campaña de reactivación para 10 clientes con 30-45 días sin transacciones.",
    "estado":                     "borrador",
    "generada_automaticamente":   True,
    "segmento_generado":          {
        "perfil_inactividad":   "reciente",
        "dias_inactividad_min": 30,
        "dias_inactividad_max": 45,
        "tamanio_audiencia":    10,
    },
    "mensaje_generado":           "Hola, te extrañamos. Vuelve y aprovecha tus beneficios.",
    "metricas":                   {"tiempo_respuesta_ms": 1800},
    "created_at":                 "2024-06-01T10:00:00+00:00",
}

_RESULTADO_ORQUESTADOR = {
    "segmento": {
        "perfil_inactividad":   "reciente",
        "dias_inactividad_min": 30,
        "dias_inactividad_max": 45,
        "tamanio_audiencia":    10,
    },
    "tamanio_audiencia": 10,
    "mensaje":           "Hola, te extrañamos. Vuelve y aprovecha tus beneficios.",
    "tipo_campana":      "reactivacion",
    "tono":              "cercano",
    "metricas": {
        "tiempo_respuesta_ms": 1800,
        "tasa_anonimizacion":  100.0,
        "precision_segmento":  88.0,
    },
}

_RESUMEN_DETECCION = {
    "ejecutado_at":      datetime.now(timezone.utc).isoformat(),
    "grupos_detectados": {"reciente": 5, "moderada": 3, "prolongada": 2},
    "campanas_generadas": 3,
    "errores":           [],
}


# ── Helpers de mocks ──────────────────────────────────────────────────────────

def _tabla_con_data(data) -> MagicMock:
    t = MagicMock()
    t.select.return_value = t
    t.eq.return_value     = t
    t.order.return_value  = t
    t.range.return_value  = t
    t.single.return_value = t
    t.insert.return_value = t
    t.update.return_value = t
    t.execute.return_value.data = data
    return t


def _make_db(
    campanas_lista: list | None  = None,
    campana_detail               = None,
    campana_update: list | None  = None,
    registros: list | None       = None,
) -> MagicMock:
    lista   = campanas_lista if campanas_lista is not None else [_CAMPANA_BORRADOR]
    detalle = campana_detail if campana_detail is not None else _CAMPANA_BORRADOR
    update  = campana_update if campana_update is not None else [_CAMPANA_BORRADOR]

    # Construimos el mock de la tabla campanas con chains bien definidas
    t_camp = MagicMock()

    # Mock para single() → devuelve objeto cuyo .execute().data == detalle
    mock_single_exec = MagicMock()
    mock_single_exec.data = detalle
    mock_single = MagicMock()
    mock_single.execute.return_value = mock_single_exec

    # Mock para range() → devuelve objeto cuyo .execute().data == lista
    mock_range_exec = MagicMock()
    mock_range_exec.data = lista
    mock_range = MagicMock()
    mock_range.execute.return_value = mock_range_exec

    # Mock para update().eq().execute()
    mock_update_eq_exec = MagicMock()
    mock_update_eq_exec.data = update
    mock_update_eq = MagicMock()
    mock_update_eq.execute.return_value = mock_update_eq_exec
    mock_update = MagicMock()
    mock_update.eq.return_value = mock_update_eq

    # Mock para insert().execute()
    mock_insert_exec = MagicMock()
    mock_insert_exec.data = [{"id": _CAMPANA_ID}]
    mock_insert = MagicMock()
    mock_insert.execute.return_value = mock_insert_exec

    # Chain: .select().eq().eq().single() → mock_single
    #        .select().eq().eq().order().range() → mock_range
    #        .update() → mock_update
    #        .insert() → mock_insert
    mock_eq2 = MagicMock()
    mock_eq2.single.return_value    = mock_single
    mock_eq2.order.return_value     = MagicMock(
        range=MagicMock(return_value=mock_range)
    )

    mock_eq1 = MagicMock()
    mock_eq1.eq.return_value     = mock_eq2
    mock_eq1.single.return_value = mock_single   # para cadenas con solo un .eq()
    mock_eq1.order.return_value  = MagicMock(range=MagicMock(return_value=mock_range))

    mock_select = MagicMock()
    mock_select.eq.return_value = mock_eq1

    t_camp.select.return_value = mock_select
    t_camp.update.return_value = mock_update
    t_camp.insert.return_value = mock_insert

    t_reg  = _tabla_con_data(registros if registros is not None else [])
    t_logs = _tabla_con_data([])

    mock_db = MagicMock()
    mock_db.table.side_effect = lambda nombre: {
        "campanas":           t_camp,
        "registros_campania": t_reg,
        "logs_ejecucion":     t_logs,
    }.get(nombre, _tabla_con_data([]))

    return mock_db


def _make_orquestador(resultado: dict | None = None, error: bool = False) -> MagicMock:
    orq = MagicMock()
    ret = resultado if resultado is not None else _RESULTADO_ORQUESTADOR
    if error:
        ret = {"error": "gemini_no_disponible", "detalle": "timeout"}
    orq.procesar_instruccion = AsyncMock(return_value=ret)
    return orq


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def limpiar_overrides():
    yield
    app.dependency_overrides.clear()


@pytest.fixture
def client():
    return TestClient(app, raise_server_exceptions=True)


# ── Test 1: GET /detectados retorna lista de borradores ──────────────────────

def test_detectados_retorna_lista_borradores(client):
    app.dependency_overrides[get_current_user]    = lambda: _USUARIO_ANALISTA
    app.dependency_overrides[get_supabase_client] = lambda: _make_db()

    resp = client.get("/reactivacion/detectados")

    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, list)
    assert len(data) == 1
    assert data[0]["campana_id"] == _CAMPANA_ID


def test_detectados_incluye_campos_esperados(client):
    app.dependency_overrides[get_current_user]    = lambda: _USUARIO_ANALISTA
    app.dependency_overrides[get_supabase_client] = lambda: _make_db()

    resp = client.get("/reactivacion/detectados")

    assert resp.status_code == 200
    item = resp.json()[0]
    for campo in ("campana_id", "segmento", "mensaje", "created_at"):
        assert campo in item, f"Campo '{campo}' ausente en la respuesta"


# ── Test 2: GET /detectados sin autenticación retorna 401 ────────────────────

def test_detectados_sin_autenticacion_retorna_401(client):
    resp = client.get("/reactivacion/detectados")
    assert resp.status_code == 401


def test_detectados_paginacion(client):
    app.dependency_overrides[get_current_user]    = lambda: _USUARIO_ANALISTA
    app.dependency_overrides[get_supabase_client] = lambda: _make_db(campanas_lista=[])

    resp = client.get("/reactivacion/detectados?skip=5&limit=10")
    assert resp.status_code == 200


# ── Test 3: POST /ejecutar-deteccion como analista retorna 403 ───────────────

def test_ejecutar_deteccion_analista_retorna_403(client):
    app.dependency_overrides[get_current_user]    = lambda: _USUARIO_ANALISTA
    app.dependency_overrides[get_supabase_client] = lambda: _make_db()
    app.dependency_overrides[get_orquestador]     = lambda: _make_orquestador()

    resp = client.post("/reactivacion/ejecutar-deteccion")
    assert resp.status_code == 403


# ── Test 4: POST /ejecutar-deteccion como coordinador ejecuta el servicio ─────

def test_ejecutar_deteccion_coordinador_llama_servicio(client):
    # Simular DB con un cliente inactivo (60 días → moderada)
    fecha_inactiva = (datetime.now(timezone.utc) - timedelta(days=60)).isoformat()
    registro_inactivo = {
        "cliente_id_anonimizado":   "abc123",
        "dimension_ciclo_vida":     "reactivacion",
        "fecha_ultima_transaccion": fecha_inactiva,
        "score_crediticio":         600,
    }

    db_mock  = _make_db(registros=[registro_inactivo])
    orq_mock = _make_orquestador()

    app.dependency_overrides[get_current_user]    = lambda: _USUARIO_COORDINADOR
    app.dependency_overrides[get_supabase_client] = lambda: db_mock
    app.dependency_overrides[get_orquestador]     = lambda: orq_mock

    resp = client.post("/reactivacion/ejecutar-deteccion")

    assert resp.status_code == 200
    data = resp.json()
    assert "campanas_generadas"  in data
    assert "grupos_detectados"   in data
    assert "ejecutado_at"        in data
    assert "errores"             in data
    assert data["campanas_generadas"] == 1


def test_ejecutar_deteccion_sin_autenticacion_retorna_401(client):
    resp = client.post("/reactivacion/ejecutar-deteccion")
    assert resp.status_code == 401


# ── Test 5: PATCH /ajustar regenera la campaña con nueva instrucción ──────────

def test_ajustar_campaña_borrador_regenera_con_nueva_instruccion(client):
    db_mock  = _make_db()
    orq_mock = _make_orquestador()

    app.dependency_overrides[get_current_user]    = lambda: _USUARIO_ANALISTA
    app.dependency_overrides[get_supabase_client] = lambda: db_mock
    app.dependency_overrides[get_orquestador]     = lambda: orq_mock

    nueva = "Campaña ajustada con oferta especial de bienvenida de retorno exclusiva para clientes"
    resp  = client.patch(
        f"/reactivacion/{_CAMPANA_ID}/ajustar",
        json={"nueva_instruccion": nueva},
    )

    assert resp.status_code == 200
    data = resp.json()
    assert "segmento" in data
    assert "mensaje"  in data
    assert "metricas" in data

    # El orquestador fue llamado con la nueva instrucción
    call_kwargs = orq_mock.procesar_instruccion.call_args
    instruccion_llamada = call_kwargs.kwargs.get("instruccion") or call_kwargs.args[0]
    assert instruccion_llamada == nueva


# ── Test 6: PATCH /ajustar con estado != borrador retorna 409 ─────────────────

def test_ajustar_campaña_no_borrador_retorna_409(client):
    campana_pendiente = {**_CAMPANA_BORRADOR, "estado": "pendiente_aprobacion"}
    db_mock = _make_db(campana_detail=campana_pendiente)
    # Hacer que single() devuelva la campaña pendiente
    db_mock.table("campanas").execute.return_value.data = campana_pendiente

    app.dependency_overrides[get_current_user]    = lambda: _USUARIO_ANALISTA
    app.dependency_overrides[get_supabase_client] = lambda: db_mock
    app.dependency_overrides[get_orquestador]     = lambda: _make_orquestador()

    resp = client.patch(
        f"/reactivacion/{_CAMPANA_ID}/ajustar",
        json={"nueva_instruccion": "Nueva instrucción completamente diferente para la campaña"},
    )

    assert resp.status_code == 409
    assert "pendiente_aprobacion" in resp.json()["detail"]


def test_ajustar_instruccion_muy_corta_retorna_422(client):
    app.dependency_overrides[get_current_user]    = lambda: _USUARIO_ANALISTA
    app.dependency_overrides[get_supabase_client] = lambda: _make_db()
    app.dependency_overrides[get_orquestador]     = lambda: _make_orquestador()

    resp = client.patch(
        f"/reactivacion/{_CAMPANA_ID}/ajustar",
        json={"nueva_instruccion": "corta"},
    )
    assert resp.status_code == 422


# ── Test 7: PATCH /enviar-aprobacion cambia estado correctamente ──────────────

def test_enviar_aprobacion_cambia_a_pendiente(client):
    db_mock = _make_db()
    # single() devuelve campaña borrador
    db_mock.table("campanas").execute.return_value.data = _CAMPANA_BORRADOR

    app.dependency_overrides[get_current_user]    = lambda: _USUARIO_ANALISTA
    app.dependency_overrides[get_supabase_client] = lambda: db_mock

    resp = client.patch(f"/reactivacion/{_CAMPANA_ID}/enviar-aprobacion")

    assert resp.status_code == 200
    data = resp.json()
    assert data["estado_nuevo"] == "pendiente_aprobacion"
    assert data["campana_id"]   == _CAMPANA_ID


def test_enviar_aprobacion_sin_autenticacion_retorna_401(client):
    resp = client.patch(f"/reactivacion/{_CAMPANA_ID}/enviar-aprobacion")
    assert resp.status_code == 401


def test_enviar_aprobacion_campana_ya_pendiente_retorna_409(client):
    campana_pendiente = {**_CAMPANA_BORRADOR, "estado": "pendiente_aprobacion"}
    db_mock = _make_db(campana_detail=campana_pendiente)
    db_mock.table("campanas").execute.return_value.data = campana_pendiente

    app.dependency_overrides[get_current_user]    = lambda: _USUARIO_ANALISTA
    app.dependency_overrides[get_supabase_client] = lambda: db_mock

    resp = client.patch(f"/reactivacion/{_CAMPANA_ID}/enviar-aprobacion")

    assert resp.status_code == 409
    assert "pendiente_aprobacion" in resp.json()["detail"]


# ── Tests de GET /{campana_id} ────────────────────────────────────────────────

def test_obtener_campana_sistema_cualquier_analista_puede_ver(client):
    """Las campañas del sistema (usuario_id=None) son visibles por cualquier analista."""
    db_mock = _make_db()
    db_mock.table("campanas").execute.return_value.data = _CAMPANA_BORRADOR

    app.dependency_overrides[get_current_user]    = lambda: _USUARIO_ANALISTA
    app.dependency_overrides[get_supabase_client] = lambda: db_mock

    resp = client.get(f"/reactivacion/{_CAMPANA_ID}")
    assert resp.status_code == 200


def test_obtener_campana_no_encontrada_retorna_404(client):
    """Cuando la campaña no existe en BD, el router eleva 404."""
    # Construimos un mock autónomo que retorna data=None
    mock_exec = MagicMock()
    mock_exec.data = None
    mock_single = MagicMock()
    mock_single.execute.return_value = mock_exec
    mock_eq2 = MagicMock()
    mock_eq2.single.return_value = mock_single
    mock_eq1 = MagicMock()
    mock_eq1.eq.return_value = mock_eq2
    mock_select = MagicMock()
    mock_select.eq.return_value = mock_eq1
    t_camp = MagicMock()
    t_camp.select.return_value = mock_select

    db_none = MagicMock()
    db_none.table.side_effect = lambda n: t_camp if n == "campanas" else _tabla_con_data([])

    app.dependency_overrides[get_current_user]    = lambda: _USUARIO_ANALISTA
    app.dependency_overrides[get_supabase_client] = lambda: db_none

    resp = client.get(f"/reactivacion/{_CAMPANA_ID}")
    assert resp.status_code == 404


def test_obtener_campana_incluye_logs(client):
    logs = [{"id": "log-1", "campana_id": _CAMPANA_ID, "tiempo_respuesta_ms": 1200}]
    db_mock = _make_db()
    db_mock.table("campanas").execute.return_value.data = _CAMPANA_BORRADOR
    db_mock.table("logs_ejecucion").execute.return_value.data = logs

    app.dependency_overrides[get_current_user]    = lambda: _USUARIO_ANALISTA
    app.dependency_overrides[get_supabase_client] = lambda: db_mock

    resp = client.get(f"/reactivacion/{_CAMPANA_ID}")

    assert resp.status_code == 200
    assert "logs" in resp.json()
