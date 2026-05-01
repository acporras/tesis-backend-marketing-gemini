"""
Tests del router /fidelizacion (HU-03 y HU-04) usando FastAPI TestClient.
Las dependencias externas (Supabase, Orquestador) se mockean con
app.dependency_overrides para aislar la lógica HTTP y las reglas de negocio.
"""
import pytest
from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock, MagicMock
from fastapi.testclient import TestClient

from main import app
from dependencies import get_current_user, get_supabase_client, get_orquestador
from routers.fidelizacion import (
    _generar_recomendaciones,
    _calcular_score_compatibilidad,
    _aplicar_filtros,
    _antiguedad_meses,
    _enriquecer_instruccion,
    _extraer_productos_del_segmento,
    _normalizar_lista,
    FiltrosOpcionales,
    InstruccionFidelizacion,
)

# ── Constantes de prueba ──────────────────────────────────────────────────────

_USUARIO_ANALISTA = {
    "id":     "uid-analista-001",
    "nombre": "Ana Pérez",
    "email":  "ana@banco.pe",
    "rol":    "analista",
    "activo": True,
}

_USUARIO_COORDINADOR = {**_USUARIO_ANALISTA, "id": "uid-coord-001", "rol": "coordinador"}

_CAMPANA_ID   = "22222222-2222-2222-2222-222222222222"
_CLIENTE_ID   = "b" * 64

_CLIENTE_FIDELIZACION = {
    "cliente_id_anonimizado":  _CLIENTE_ID,
    "dimension_ciclo_vida":    "fidelizacion",
    "score_crediticio":        720,
    "operaciones_ultimo_mes":  12,
    "fecha_apertura_cuenta":   (datetime.now(timezone.utc) - timedelta(days=400)).isoformat(),
    "fecha_ultima_transaccion": (datetime.now(timezone.utc) - timedelta(days=3)).isoformat(),
    "productos_activos":       ["cuenta_ahorro"],
    "canal_principal":         "digital",
}

_CAMPANA_BD = {
    "id":                  _CAMPANA_ID,
    "usuario_id":          _USUARIO_ANALISTA["id"],
    "dimension":           "fidelizacion",
    "instruccion_original": "Campaña para clientes leales con alto score crediticio",
    "estado":              "borrador",
    "created_at":          "2024-06-01T10:00:00+00:00",
}

_RESULTADO_ORQUESTADOR = {
    "segmento": {
        "dimension_ciclo_vida": {"eq": "fidelizacion"},
        "productos_recomendados": ["tarjeta_credito", "deposito_plazo"],
    },
    "tamanio_audiencia": 1200,
    "mensaje":           "Querido cliente, te ofrecemos beneficios exclusivos.",
    "tipo_campana":      "fidelizacion",
    "tono":              "cercano",
    "metricas": {
        "tiempo_respuesta_ms": 2800,
        "tasa_anonimizacion":  100.0,
        "precision_segmento":  92.0,
    },
}


# ── Helpers para construir mocks de DB ────────────────────────────────────────

def _tabla_con_data(data) -> MagicMock:
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
    clientes=None,
    campana_insertada=None,
    campana_detail=None,
    campanas_lista=None,
    campana_update=None,
) -> MagicMock:
    tablas = {
        "registros_campania": _tabla_con_data(
            clientes if clientes is not None else [_CLIENTE_FIDELIZACION]
        ),
        "campanas":      MagicMock(),
        "logs_ejecucion": _tabla_con_data([]),
    }

    tc = tablas["campanas"]
    tc.select.return_value = tc
    tc.eq.return_value = tc
    tc.order.return_value = tc
    tc.range.return_value = tc
    tc.single.return_value = tc
    tc.update.return_value = tc

    insert_data = campana_insertada if campana_insertada is not None else [_CAMPANA_BD]
    tc.insert.return_value.execute.return_value.data = insert_data

    update_data = campana_update if campana_update is not None else [_CAMPANA_BD]
    tc.update.return_value.execute.return_value.data = update_data
    tc.eq.return_value.execute.return_value.data = update_data

    detail_data = campana_detail if campana_detail is not None else _CAMPANA_BD
    lista_data  = campanas_lista  if campanas_lista  is not None else [_CAMPANA_BD]
    tc.execute.return_value.data = detail_data
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
    yield
    app.dependency_overrides.clear()


@pytest.fixture
def client():
    return TestClient(app, raise_server_exceptions=True)


@pytest.fixture
def client_auth(client):
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
        "/fidelizacion/generar",
        json={
            "instruccion": "Campaña de cross-selling para clientes con score alto y cuenta activa",
            "objetivo":    "cross_selling",
        },
    )

    assert resp.status_code == 200
    data = resp.json()
    assert "campana_id" in data
    assert data["mensaje"] == _RESULTADO_ORQUESTADOR["mensaje"]
    assert data["tamanio_audiencia"] == 1200
    assert data["estado"] == "borrador"
    assert data["objetivo"] == "cross_selling"
    assert "metricas" in data
    assert data["metricas"]["tiempo_respuesta_ms"] == 2800


# ── Test 2: POST /generar sin autenticación retorna 401 ───────────────────────

def test_generar_sin_autenticacion_retorna_401(client):
    resp = client.post(
        "/fidelizacion/generar",
        json={
            "instruccion": "Campaña de retención para clientes fieles con buen historial",
            "objetivo":    "retencion",
        },
    )
    assert resp.status_code == 401


# ── Test 3: POST /generar con instruccion muy corta retorna 422 ───────────────

def test_generar_instruccion_corta_retorna_422(client):
    app.dependency_overrides[get_current_user]    = lambda: _USUARIO_ANALISTA
    app.dependency_overrides[get_supabase_client] = lambda: _make_db()
    app.dependency_overrides[get_orquestador]     = lambda: _make_orquestador()

    resp = client.post(
        "/fidelizacion/generar",
        json={"instruccion": "corta", "objetivo": "retencion"},
    )
    assert resp.status_code == 422


# ── Test 4: POST /generar con objetivo inválido retorna 422 ───────────────────

def test_generar_objetivo_invalido_retorna_422(client):
    app.dependency_overrides[get_current_user]    = lambda: _USUARIO_ANALISTA
    app.dependency_overrides[get_supabase_client] = lambda: _make_db()
    app.dependency_overrides[get_orquestador]     = lambda: _make_orquestador()

    resp = client.post(
        "/fidelizacion/generar",
        json={"instruccion": "Campaña para clientes con productos activos y buen historial", "objetivo": "invalido"},
    )
    assert resp.status_code == 422


# ── Test 5: POST /generar sin clientes fidelización retorna 404 ───────────────

def test_generar_sin_clientes_retorna_404(client):
    app.dependency_overrides[get_current_user]    = lambda: _USUARIO_ANALISTA
    app.dependency_overrides[get_supabase_client] = lambda: _make_db(clientes=[])
    app.dependency_overrides[get_orquestador]     = lambda: _make_orquestador()

    resp = client.post(
        "/fidelizacion/generar",
        json={
            "instruccion": "Campaña para clientes fieles con alta actividad transaccional",
            "objetivo":    "satisfaccion",
        },
    )
    assert resp.status_code == 404
    assert "fidelización" in resp.json()["detail"].lower() or "fidelizaci" in resp.json()["detail"]


# ── Test 6: POST /generar con filtro score_minimo deja clientes válidos ───────

def test_generar_con_filtro_score_minimo(client):
    clientes = [
        {**_CLIENTE_FIDELIZACION, "score_crediticio": 750},
        {**_CLIENTE_FIDELIZACION, "cliente_id_anonimizado": "c" * 64, "score_crediticio": 500},
    ]
    orq_mock = _make_orquestador()
    app.dependency_overrides[get_current_user]    = lambda: _USUARIO_ANALISTA
    app.dependency_overrides[get_supabase_client] = lambda: _make_db(clientes=clientes)
    app.dependency_overrides[get_orquestador]     = lambda: orq_mock

    resp = client.post(
        "/fidelizacion/generar",
        json={
            "instruccion": "Campaña premium para clientes con alto score crediticio verificado",
            "objetivo":    "up_selling",
            "filtros_opcionales": {"score_minimo": 700},
        },
    )
    assert resp.status_code == 200
    # Solo el cliente con score=750 pasa el filtro
    call_kwargs = orq_mock.procesar_instruccion.call_args
    registros_pasados = call_kwargs.kwargs.get("registros") or call_kwargs.args[2]
    assert len(registros_pasados) == 1
    assert registros_pasados[0]["score_crediticio"] == 750


# ── Test 7: POST /generar filtro quita todos → 404 ───────────────────────────

def test_generar_filtro_elimina_todos_retorna_404(client):
    clientes = [
        {**_CLIENTE_FIDELIZACION, "score_crediticio": 400},
    ]
    app.dependency_overrides[get_current_user]    = lambda: _USUARIO_ANALISTA
    app.dependency_overrides[get_supabase_client] = lambda: _make_db(clientes=clientes)
    app.dependency_overrides[get_orquestador]     = lambda: _make_orquestador()

    resp = client.post(
        "/fidelizacion/generar",
        json={
            "instruccion": "Campaña de fidelización para clientes con alto score crediticio en Lima",
            "objetivo":    "retencion",
            "filtros_opcionales": {"score_minimo": 700},
        },
    )
    assert resp.status_code == 404


# ── Test 8: GET /mis-campanas solo retorna campañas del usuario ───────────────

def test_mis_campanas_solo_del_usuario(client):
    db_mock = _make_db(campanas_lista=[_CAMPANA_BD])
    app.dependency_overrides[get_current_user]    = lambda: _USUARIO_ANALISTA
    app.dependency_overrides[get_supabase_client] = lambda: db_mock

    resp = client.get("/fidelizacion/mis-campanas")

    assert resp.status_code == 200
    assert isinstance(resp.json(), list)

    tc = db_mock.table("campanas")
    tc.eq.assert_any_call("usuario_id", _USUARIO_ANALISTA["id"])
    tc.eq.assert_any_call("dimension", "fidelizacion")


# ── Test 9: GET /mis-campanas soporta paginación ─────────────────────────────

def test_mis_campanas_paginacion(client):
    app.dependency_overrides[get_current_user]    = lambda: _USUARIO_ANALISTA
    app.dependency_overrides[get_supabase_client] = lambda: _make_db()

    resp = client.get("/fidelizacion/mis-campanas?skip=5&limit=10")
    assert resp.status_code == 200


# ── Test 10: PATCH enviar-aprobacion cambia estado correctamente ──────────────

def test_enviar_aprobacion_cambia_estado(client):
    campana_borrador = {**_CAMPANA_BD, "estado": "borrador"}
    db = _make_db(campana_detail=campana_borrador)
    db.table("campanas").execute.return_value.data = campana_borrador

    app.dependency_overrides[get_current_user]    = lambda: _USUARIO_ANALISTA
    app.dependency_overrides[get_supabase_client] = lambda: db

    resp = client.patch(f"/fidelizacion/{_CAMPANA_ID}/enviar-aprobacion")

    assert resp.status_code == 200
    data = resp.json()
    assert data["estado_nuevo"] == "pendiente_aprobacion"
    assert data["campana_id"] == _CAMPANA_ID


# ── Test 11: PATCH enviar-aprobacion estado incorrecto retorna 409 ────────────

def test_enviar_aprobacion_estado_incorrecto_retorna_409(client):
    campana_pendiente = {**_CAMPANA_BD, "estado": "pendiente_aprobacion"}
    db = _make_db(campana_detail=campana_pendiente)
    db.table("campanas").execute.return_value.data = campana_pendiente

    app.dependency_overrides[get_current_user]    = lambda: _USUARIO_ANALISTA
    app.dependency_overrides[get_supabase_client] = lambda: db

    resp = client.patch(f"/fidelizacion/{_CAMPANA_ID}/enviar-aprobacion")

    assert resp.status_code == 409
    assert "pendiente_aprobacion" in resp.json()["detail"]


# ── Test 12: PATCH enviar-aprobacion por otro usuario retorna 403 ─────────────

def test_enviar_aprobacion_otro_usuario_retorna_403(client):
    campana_otro = {**_CAMPANA_BD, "usuario_id": "otro-usuario-id", "estado": "borrador"}
    db = _make_db(campana_detail=campana_otro)
    db.table("campanas").execute.return_value.data = campana_otro

    app.dependency_overrides[get_current_user]    = lambda: _USUARIO_ANALISTA
    app.dependency_overrides[get_supabase_client] = lambda: db

    resp = client.patch(f"/fidelizacion/{_CAMPANA_ID}/enviar-aprobacion")

    assert resp.status_code == 403


# ── Test 13: GET /clientes/{id}/recomendaciones — HU-04 cliente califica ──────

def test_recomendaciones_cliente_califica_tarjeta_y_plazo(client):
    cliente = {
        **_CLIENTE_FIDELIZACION,
        "score_crediticio":      720,
        "productos_activos":     ["cuenta_ahorro"],
        "fecha_apertura_cuenta": (datetime.now(timezone.utc) - timedelta(days=400)).isoformat(),
    }
    db = _make_db()
    db.table("registros_campania").execute.return_value.data = cliente

    app.dependency_overrides[get_current_user]    = lambda: _USUARIO_ANALISTA
    app.dependency_overrides[get_supabase_client] = lambda: db

    resp = client.get(f"/fidelizacion/clientes/{_CLIENTE_ID}/recomendaciones")

    assert resp.status_code == 200
    data = resp.json()
    assert data["cliente_id"] == _CLIENTE_ID
    assert "cuenta_ahorro" in data["productos_actuales"]
    productos_recomendados = [r["producto"] for r in data["productos_recomendados"]]
    assert "tarjeta_credito" in productos_recomendados
    assert "deposito_plazo" in productos_recomendados
    assert data["score_compatibilidad"] > 0


# ── Test 14: GET /clientes/{id}/recomendaciones — cliente sin productos ────────

def test_recomendaciones_cliente_sin_cuenta_ahorro(client):
    cliente = {
        **_CLIENTE_FIDELIZACION,
        "score_crediticio":  800,
        "productos_activos": [],
    }
    db = _make_db()
    db.table("registros_campania").execute.return_value.data = cliente

    app.dependency_overrides[get_current_user]    = lambda: _USUARIO_ANALISTA
    app.dependency_overrides[get_supabase_client] = lambda: db

    resp = client.get(f"/fidelizacion/clientes/{_CLIENTE_ID}/recomendaciones")

    assert resp.status_code == 200
    data = resp.json()
    assert data["productos_recomendados"] == []
    assert data["score_compatibilidad"] == 0.0


# ── Test 15: GET /clientes/{id}/recomendaciones — cliente no encontrado ────────

def test_recomendaciones_cliente_no_encontrado_retorna_404(client):
    db = _make_db()
    db.table("registros_campania").execute.return_value.data = None

    app.dependency_overrides[get_current_user]    = lambda: _USUARIO_ANALISTA
    app.dependency_overrides[get_supabase_client] = lambda: db

    resp = client.get(f"/fidelizacion/clientes/id-inexistente/recomendaciones")

    assert resp.status_code == 404


# ── Test 16: GET /{campana_id} — coordinador puede ver campañas ajenas ─────────

def test_obtener_campana_coordinador_ve_ajena(client):
    campana_ajena = {**_CAMPANA_BD, "usuario_id": "otro-usuario-id"}
    db = _make_db(campana_detail=campana_ajena)
    db.table("campanas").execute.return_value.data = campana_ajena

    app.dependency_overrides[get_current_user]    = lambda: _USUARIO_COORDINADOR
    app.dependency_overrides[get_supabase_client] = lambda: db

    resp = client.get(f"/fidelizacion/{_CAMPANA_ID}")

    assert resp.status_code == 200


# ── Test 17: GET /{campana_id} — analista no puede ver campaña ajena ──────────

def test_obtener_campana_analista_no_puede_ver_ajena(client):
    campana_ajena = {**_CAMPANA_BD, "usuario_id": "otro-usuario-id"}
    db = _make_db(campana_detail=campana_ajena)
    db.table("campanas").execute.return_value.data = campana_ajena

    app.dependency_overrides[get_current_user]    = lambda: _USUARIO_ANALISTA
    app.dependency_overrides[get_supabase_client] = lambda: db

    resp = client.get(f"/fidelizacion/{_CAMPANA_ID}")

    assert resp.status_code == 403


# ══════════════════════════════════════════════════════════════════════════════
# Tests de lógica pura (sin HTTP)
# ══════════════════════════════════════════════════════════════════════════════

# ── _generar_recomendaciones ──────────────────────────────────────────────────

def test_generar_recs_tarjeta_credito_score_alto():
    cliente = {
        "score_crediticio":        680,
        "operaciones_ultimo_mes":  5,
        "fecha_apertura_cuenta":   (datetime.now(timezone.utc) - timedelta(days=50)).isoformat(),
    }
    recs = _generar_recomendaciones(cliente, ["cuenta_ahorro"])
    productos = [r["producto"] for r in recs]
    assert "tarjeta_credito" in productos
    rec_tc = next(r for r in recs if r["producto"] == "tarjeta_credito")
    assert rec_tc["prioridad"] == "alta"


def test_generar_recs_no_sugiere_tarjeta_score_bajo():
    cliente = {
        "score_crediticio":        600,
        "operaciones_ultimo_mes":  5,
        "fecha_apertura_cuenta":   (datetime.now(timezone.utc) - timedelta(days=50)).isoformat(),
    }
    recs = _generar_recomendaciones(cliente, ["cuenta_ahorro"])
    productos = [r["producto"] for r in recs]
    assert "tarjeta_credito" not in productos


def test_generar_recs_prestamo_alta_actividad():
    cliente = {
        "score_crediticio":        750,
        "operaciones_ultimo_mes":  15,
        "fecha_apertura_cuenta":   (datetime.now(timezone.utc) - timedelta(days=200)).isoformat(),
    }
    recs = _generar_recomendaciones(cliente, ["cuenta_ahorro"])
    productos = [r["producto"] for r in recs]
    assert "prestamo_personal" in productos


def test_generar_recs_no_duplica_productos_existentes():
    cliente = {
        "score_crediticio":        750,
        "operaciones_ultimo_mes":  15,
        "fecha_apertura_cuenta":   (datetime.now(timezone.utc) - timedelta(days=200)).isoformat(),
    }
    recs = _generar_recomendaciones(cliente, ["cuenta_ahorro", "tarjeta_credito", "prestamo_personal"])
    productos = [r["producto"] for r in recs]
    assert "tarjeta_credito" not in productos
    assert "prestamo_personal" not in productos


def test_generar_recs_upgrade_tarjeta_premium():
    cliente = {
        "score_crediticio":        700,
        "operaciones_ultimo_mes":  20,
        "fecha_apertura_cuenta":   (datetime.now(timezone.utc) - timedelta(days=100)).isoformat(),
    }
    recs = _generar_recomendaciones(cliente, ["cuenta_ahorro", "tarjeta_credito"])
    productos = [r["producto"] for r in recs]
    assert "tarjeta_credito_premium" in productos


def test_generar_recs_deposito_plazo_antiguedad():
    cliente = {
        "score_crediticio":        550,
        "operaciones_ultimo_mes":  3,
        "fecha_apertura_cuenta":   (datetime.now(timezone.utc) - timedelta(days=400)).isoformat(),
    }
    recs = _generar_recomendaciones(cliente, ["cuenta_ahorro"])
    productos = [r["producto"] for r in recs]
    assert "deposito_plazo" in productos


def test_generar_recs_sin_cuenta_ahorro_no_sugiere_nada():
    cliente = {
        "score_crediticio":        800,
        "operaciones_ultimo_mes":  20,
        "fecha_apertura_cuenta":   (datetime.now(timezone.utc) - timedelta(days=500)).isoformat(),
    }
    recs = _generar_recomendaciones(cliente, [])
    productos = [r["producto"] for r in recs]
    assert "tarjeta_credito" not in productos
    assert "prestamo_personal" not in productos
    assert "deposito_plazo" not in productos


# ── _calcular_score_compatibilidad ────────────────────────────────────────────

def test_score_compatibilidad_sin_recomendaciones_es_cero():
    cliente = {"score_crediticio": 800}
    assert _calcular_score_compatibilidad(cliente, []) == 0.0


def test_score_compatibilidad_con_recomendacion_alta():
    cliente = {"score_crediticio": 850}
    recs = [{"prioridad": "alta"}]
    score = _calcular_score_compatibilidad(cliente, recs)
    assert score > 0
    assert score <= 100.0


def test_score_compatibilidad_maximo_cien():
    cliente = {"score_crediticio": 850}
    recs = [
        {"prioridad": "alta"},
        {"prioridad": "alta"},
        {"prioridad": "alta"},
        {"prioridad": "media"},
        {"prioridad": "media"},
    ]
    score = _calcular_score_compatibilidad(cliente, recs)
    assert score == 100.0


def test_score_compatibilidad_sin_score_crediticio():
    # score_crediticio ausente → base=0, solo se acumula el bonus de recomendaciones
    cliente = {}
    recs = [{"prioridad": "media"}]
    score = _calcular_score_compatibilidad(cliente, recs)
    assert score == 5.0  # base=0 + bonus_media=5


# ── _aplicar_filtros ──────────────────────────────────────────────────────────

def test_aplicar_filtros_score_minimo():
    registros = [
        {"score_crediticio": 700, "operaciones_ultimo_mes": 5, "productos_activos": [], "canal_principal": "digital"},
        {"score_crediticio": 600, "operaciones_ultimo_mes": 5, "productos_activos": [], "canal_principal": "digital"},
    ]
    filtros = FiltrosOpcionales(score_minimo=650)
    resultado = _aplicar_filtros(registros, filtros)
    assert len(resultado) == 1
    assert resultado[0]["score_crediticio"] == 700


def test_aplicar_filtros_operaciones_minimas():
    registros = [
        {"score_crediticio": 700, "operaciones_ultimo_mes": 10, "productos_activos": [], "canal_principal": "digital"},
        {"score_crediticio": 700, "operaciones_ultimo_mes": 3,  "productos_activos": [], "canal_principal": "digital"},
    ]
    filtros = FiltrosOpcionales(operaciones_minimas_mes=5)
    resultado = _aplicar_filtros(registros, filtros)
    assert len(resultado) == 1
    assert resultado[0]["operaciones_ultimo_mes"] == 10


def test_aplicar_filtros_productos_excluir():
    registros = [
        {"score_crediticio": 700, "operaciones_ultimo_mes": 5, "productos_activos": ["cuenta_ahorro"], "canal_principal": "digital"},
        {"score_crediticio": 700, "operaciones_ultimo_mes": 5, "productos_activos": ["prestamo_personal"], "canal_principal": "digital"},
    ]
    filtros = FiltrosOpcionales(productos_excluir=["prestamo_personal"])
    resultado = _aplicar_filtros(registros, filtros)
    assert len(resultado) == 1
    assert "prestamo_personal" not in resultado[0]["productos_activos"]


def test_aplicar_filtros_canal_preferido():
    registros = [
        {"score_crediticio": 700, "operaciones_ultimo_mes": 5, "productos_activos": [], "canal_principal": "digital"},
        {"score_crediticio": 700, "operaciones_ultimo_mes": 5, "productos_activos": [], "canal_principal": "presencial"},
    ]
    filtros = FiltrosOpcionales(canal_preferido="digital")
    resultado = _aplicar_filtros(registros, filtros)
    assert len(resultado) == 1
    assert resultado[0]["canal_principal"] == "digital"


def test_aplicar_filtros_sin_filtros_activos_retorna_todos():
    registros = [
        {"score_crediticio": 700, "operaciones_ultimo_mes": 5, "productos_activos": [], "canal_principal": "digital"},
        {"score_crediticio": 600, "operaciones_ultimo_mes": 2, "productos_activos": [], "canal_principal": "presencial"},
    ]
    filtros = FiltrosOpcionales()
    resultado = _aplicar_filtros(registros, filtros)
    assert len(resultado) == 2


# ── _antiguedad_meses ─────────────────────────────────────────────────────────

def test_antiguedad_meses_con_fecha_valida():
    fecha = (datetime.now(timezone.utc) - timedelta(days=365)).isoformat()
    meses = _antiguedad_meses(fecha)
    assert 11 <= meses <= 13


def test_antiguedad_meses_sin_fecha_retorna_cero():
    assert _antiguedad_meses(None) == 0
    assert _antiguedad_meses("") == 0


def test_antiguedad_meses_fecha_invalida_retorna_cero():
    assert _antiguedad_meses("fecha-invalida") == 0


def test_antiguedad_meses_cuenta_reciente():
    fecha = (datetime.now(timezone.utc) - timedelta(days=10)).isoformat()
    assert _antiguedad_meses(fecha) == 0


# ── _enriquecer_instruccion ───────────────────────────────────────────────────

def test_enriquecer_instruccion_agrega_objetivo():
    body = InstruccionFidelizacion(
        instruccion="Campaña para clientes con buen historial bancario",
        objetivo="cross_selling",
    )
    resultado = _enriquecer_instruccion(body)
    assert "cross_selling" in resultado
    assert "Campaña para clientes con buen historial bancario" in resultado


def test_enriquecer_instruccion_agrega_filtros():
    body = InstruccionFidelizacion(
        instruccion="Campaña premium para clientes con score alto y actividad frecuente",
        objetivo="up_selling",
        filtros_opcionales=FiltrosOpcionales(
            score_minimo=700,
            canal_preferido="digital",
            productos_excluir=["prestamo_personal"],
        ),
    )
    resultado = _enriquecer_instruccion(body)
    assert "700" in resultado
    assert "digital" in resultado
    assert "prestamo_personal" in resultado


# ── _extraer_productos_del_segmento ──────────────────────────────────────────

def test_extraer_productos_lista():
    segmento = {"productos_recomendados": ["tarjeta_credito", "deposito_plazo"]}
    assert _extraer_productos_del_segmento(segmento) == ["tarjeta_credito", "deposito_plazo"]


def test_extraer_productos_string():
    segmento = {"productos_recomendados": "tarjeta_credito"}
    assert _extraer_productos_del_segmento(segmento) == ["tarjeta_credito"]


def test_extraer_productos_fallback_clave_alternativa():
    segmento = {"productos": ["fondo_inversion"]}
    assert _extraer_productos_del_segmento(segmento) == ["fondo_inversion"]


def test_extraer_productos_segmento_vacio():
    assert _extraer_productos_del_segmento({}) == []


# ── _normalizar_lista ─────────────────────────────────────────────────────────

def test_normalizar_lista_desde_lista():
    assert _normalizar_lista(["a", "b"]) == ["a", "b"]


def test_normalizar_lista_desde_string():
    assert _normalizar_lista("cuenta_ahorro") == ["cuenta_ahorro"]


def test_normalizar_lista_desde_none():
    assert _normalizar_lista(None) == []


def test_normalizar_lista_desde_string_vacio():
    assert _normalizar_lista("") == []
