import pytest
from unittest.mock import AsyncMock, MagicMock, patch, call

from services.orquestador import OrquestadorGemini, FUNCIONES_DISPONIBLES


# ── Helpers ───────────────────────────────────────────────────────────────────

def _mock_gemini_respuesta(
    criterios: dict | None = None,
    tamanio: int = 1000,
    mensaje: str = "Estimado cliente, le ofrecemos una promoción exclusiva.",
    tipo: str = "onboarding",
    tono: str = "cercano",
    dimension: str = "onboarding",
) -> MagicMock:
    """Construye un objeto respuesta de Gemini con dos function calls."""
    if criterios is None:
        criterios = {"dimension_ciclo_vida": {"eq": "onboarding"}}

    part_seg = MagicMock()
    part_seg.function_call.name = "segmentar_clientes"
    part_seg.function_call.args = {
        "dimension": dimension,
        "criterios": criterios,
        "tamanio_audiencia": tamanio,
    }

    part_msg = MagicMock()
    part_msg.function_call.name = "generar_mensaje_campana"
    part_msg.function_call.args = {
        "tipo_campana": tipo,
        "tono": tono,
        "mensaje": mensaje,
    }

    mock_cand = MagicMock()
    mock_cand.content.parts = [part_seg, part_msg]

    mock_resp = MagicMock()
    mock_resp.candidates = [mock_cand]
    return mock_resp


def _registros_onboarding(n: int = 10) -> list[dict]:
    return [
        {
            "cliente_id_anonimizado": "a" * 64,
            "score_crediticio": 700,
            "operaciones_ultimo_mes": 5,
            "dimension_ciclo_vida": "onboarding",
            "canal_principal": "digital",
        }
        for _ in range(n)
    ]


def _resultado_anon(registros: list[dict], registros_con_pii: int = 0) -> dict:
    return {
        "registros_limpios": registros,
        "total_procesados": len(registros),
        "registros_con_pii": registros_con_pii,
        "tasa_anonimizacion": round(
            ((len(registros) - registros_con_pii) / max(len(registros), 1)) * 100, 2
        ),
        "reporte": [],
    }


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def mock_model():
    m = MagicMock()
    m.generate_content.return_value = _mock_gemini_respuesta()
    return m


@pytest.fixture
def mock_db():
    db = MagicMock()
    db.table.return_value.insert.return_value.execute.return_value = MagicMock()
    return db


@pytest.fixture
def mock_anon():
    anon = MagicMock()
    anon.verificar_lote.return_value = _resultado_anon(_registros_onboarding())
    return anon


@pytest.fixture
def mock_rag():
    rag = MagicMock()
    rag.recuperar_contexto = AsyncMock(return_value=[])
    rag.construir_contexto_prompt.return_value = "No se encontraron perfiles relevantes."
    return rag


@pytest.fixture
def orquestador(mock_model, mock_db, mock_anon, mock_rag):
    return OrquestadorGemini(
        modelo=mock_model,
        db=mock_db,
        anonimizador=mock_anon,
        rag=mock_rag,
    )


# ── Test 1: flujo completo exitoso ────────────────────────────────────────────

@pytest.mark.anyio
async def test_procesar_instruccion_flujo_completo(
    orquestador, mock_anon, mock_rag, mock_model
):
    registros = _registros_onboarding(10)
    mock_anon.verificar_lote.return_value = _resultado_anon(registros)
    mock_model.generate_content.return_value = _mock_gemini_respuesta(
        criterios={"dimension_ciclo_vida": {"eq": "onboarding"}},
        tamanio=800,
        mensaje="Bienvenido a nuestro banco.",
        tipo="onboarding",
        tono="cercano",
    )

    resultado = await orquestador.procesar_instruccion(
        instruccion="Campaña para nuevos clientes digitales",
        dimension="onboarding",
        registros=registros,
        usuario_id="user-123",
        campana_id="camp-456",
    )

    assert "error" not in resultado
    assert resultado["mensaje"] == "Bienvenido a nuestro banco."
    assert resultado["tamanio_audiencia"] == 800
    assert resultado["tono"] == "cercano"
    assert "segmento" in resultado
    assert "metricas" in resultado

    metricas = resultado["metricas"]
    assert "tiempo_respuesta_ms" in metricas
    assert "tasa_anonimizacion" in metricas
    assert "precision_segmento" in metricas
    assert metricas["tasa_anonimizacion"] == 100.0

    # Verificar que se llamaron los servicios en el orden correcto
    mock_anon.verificar_lote.assert_called_once_with(registros)
    mock_rag.recuperar_contexto.assert_awaited_once()
    mock_model.generate_content.assert_called_once()


# ── Test 2: tiempo_respuesta_ms se calcula y es entero ────────────────────────

@pytest.mark.anyio
async def test_tiempo_respuesta_ms_calculado(orquestador):
    resultado = await orquestador.procesar_instruccion(
        instruccion="Campaña de prueba",
        dimension="fidelizacion",
        registros=_registros_onboarding(5),
        usuario_id="user-001",
    )

    assert "error" not in resultado
    tiempo = resultado["metricas"]["tiempo_respuesta_ms"]
    assert isinstance(tiempo, int)
    assert tiempo >= 0


# ── Test 3: _procesar_respuesta_fc extrae correctamente los args ──────────────

def test_procesar_respuesta_fc_extrae_correctamente(orquestador):
    criterios_esperados = {"score_crediticio": {"gte": 600}, "canal_principal": "digital"}
    mock_resp = _mock_gemini_respuesta(
        criterios=criterios_esperados,
        tamanio=1500,
        mensaje="Hola, tenemos una oferta para usted.",
        tipo="fidelizacion",
        tono="formal",
    )

    resultado = orquestador._procesar_respuesta_fc(mock_resp)

    assert resultado["criterios"] == criterios_esperados
    assert resultado["tamanio_audiencia"] == 1500
    assert resultado["mensaje"] == "Hola, tenemos una oferta para usted."
    assert resultado["tipo_campana"] == "fidelizacion"
    assert resultado["tono"] == "formal"


def test_procesar_respuesta_fc_sin_candidates_retorna_vacio(orquestador):
    mock_resp = MagicMock()
    mock_resp.candidates = []

    resultado = orquestador._procesar_respuesta_fc(mock_resp)

    assert resultado == {}


# ── Test 4: _calcular_precision_segmento con criterio único (gte) ─────────────

def test_calcular_precision_criterio_gte(orquestador):
    registros = [
        {"score_crediticio": 700},  # cumple gte 600
        {"score_crediticio": 500},  # no cumple
        {"score_crediticio": 600},  # cumple (igual)
        {"score_crediticio": 400},  # no cumple
        {"score_crediticio": 800},  # cumple
    ]
    criterios = {"score_crediticio": {"gte": 600}}

    precision = orquestador._calcular_precision_segmento(criterios, registros)

    assert precision == pytest.approx(60.0)   # 3 de 5 cumplen


# ── Test 5: _calcular_precision_segmento con múltiples criterios ──────────────

def test_calcular_precision_multiples_criterios(orquestador):
    registros = [
        {"score_crediticio": 700, "operaciones_ultimo_mes": 10, "dimension_ciclo_vida": "fidelizacion"},
        {"score_crediticio": 700, "operaciones_ultimo_mes": 2,  "dimension_ciclo_vida": "fidelizacion"},  # ops falla
        {"score_crediticio": 500, "operaciones_ultimo_mes": 10, "dimension_ciclo_vida": "fidelizacion"},  # score falla
        {"score_crediticio": 700, "operaciones_ultimo_mes": 10, "dimension_ciclo_vida": "onboarding"},    # dim falla
        {"score_crediticio": 650, "operaciones_ultimo_mes": 5,  "dimension_ciclo_vida": "fidelizacion"},  # cumple
    ]
    criterios = {
        "score_crediticio":       {"gte": 600},
        "operaciones_ultimo_mes": {"gte": 5},
        "dimension_ciclo_vida":   {"eq": "fidelizacion"},
    }

    precision = orquestador._calcular_precision_segmento(criterios, registros)

    assert precision == pytest.approx(40.0)   # 2 de 5 cumplen (índices 0 y 4)


# ── Test 6: _calcular_precision_segmento con criterio 'in' ────────────────────

def test_calcular_precision_criterio_in(orquestador):
    registros = [
        {"canal_principal": "digital"},
        {"canal_principal": "app_movil"},
        {"canal_principal": "presencial"},
        {"canal_principal": "call_center"},
        {"canal_principal": "digital"},
    ]
    criterios = {"canal_principal": {"in": ["digital", "app_movil"]}}

    precision = orquestador._calcular_precision_segmento(criterios, registros)

    assert precision == pytest.approx(60.0)   # 3 de 5 son canales digitales


def test_calcular_precision_lista_vacia_retorna_cero(orquestador):
    assert orquestador._calcular_precision_segmento({"score": {"gte": 600}}, []) == 0.0


def test_calcular_precision_criterios_vacios_retorna_cien(orquestador):
    registros = [{"score_crediticio": 700}] * 5
    assert orquestador._calcular_precision_segmento({}, registros) == 100.0


# ── Test 7: flujo bloqueado si anonimización falla ────────────────────────────

@pytest.mark.anyio
async def test_flujo_bloqueado_si_anonimizacion_falla(
    orquestador, mock_anon, mock_rag, mock_model
):
    mock_anon.verificar_lote.side_effect = Exception("Error crítico de anonimización")

    resultado = await orquestador.procesar_instruccion(
        instruccion="Campaña cualquiera",
        dimension="onboarding",
        registros=_registros_onboarding(),
        usuario_id="user-001",
    )

    assert resultado["error"] == "anonimizacion_fallida"
    assert "Error crítico" in resultado["detalle"]
    # Gemini NO debe ser llamado
    mock_rag.recuperar_contexto.assert_not_called()
    mock_model.generate_content.assert_not_called()


# ── Test 8: log se inserta en logs_ejecucion al finalizar ─────────────────────

@pytest.mark.anyio
async def test_log_insertado_en_logs_ejecucion(orquestador, mock_db):
    await orquestador.procesar_instruccion(
        instruccion="Campaña con log",
        dimension="reactivacion",
        registros=_registros_onboarding(5),
        usuario_id="user-002",
        campana_id="camp-999",
    )

    # Verificar que se insertó en la tabla correcta
    mock_db.table.assert_called_with("logs_ejecucion")
    mock_db.table.return_value.insert.assert_called_once()

    # Verificar campos obligatorios en el payload insertado
    payload = mock_db.table.return_value.insert.call_args[0][0]
    assert payload["campana_id"] == "camp-999"
    assert "timestamp_instruccion" in payload
    assert "timestamp_respuesta" in payload
    assert "tiempo_respuesta_ms" in payload
    assert "tasa_anonimizacion" in payload
    assert "precision_segmento" in payload


@pytest.mark.anyio
async def test_sin_campana_id_no_inserta_log(orquestador, mock_db):
    """Si campana_id es None, el log no debe registrarse."""
    await orquestador.procesar_instruccion(
        instruccion="Sin campana",
        dimension="onboarding",
        registros=_registros_onboarding(),
        usuario_id="user-003",
        campana_id=None,
    )

    mock_db.table.assert_not_called()


# ── Test 9: herramienta Gemini falla → retorna error estructurado ─────────────

@pytest.mark.anyio
async def test_gemini_falla_retorna_error(orquestador, mock_model):
    mock_model.generate_content.side_effect = Exception("Rate limit excedido")

    resultado = await orquestador.procesar_instruccion(
        instruccion="Campaña cualquiera",
        dimension="fidelizacion",
        registros=_registros_onboarding(),
        usuario_id="user-004",
    )

    assert resultado["error"] == "gemini_no_disponible"
    assert "Rate limit" in resultado["detalle"]
