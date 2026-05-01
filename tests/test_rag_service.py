import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from services.rag_service import RAGService, _SIN_PERFILES, _calcular_antiguedad_meses


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def mock_supabase():
    """Cliente de Supabase completamente mockeado."""
    return MagicMock()


@pytest.fixture
def service(mock_supabase):
    return RAGService(client=mock_supabase)


def _fake_embedding(dims: int = 768) -> list[float]:
    return [0.1] * dims


def _fake_cliente(n: int = 1) -> dict:
    return {
        "id": f"uuid-{n}",
        "cliente_id_anonimizado": "a" * 64,
        "metadata": {
            "dimension_ciclo_vida": "fidelizacion",
            "score_crediticio": 700 + n * 10,
            "operaciones_ultimo_mes": n * 5,
            "canal_principal": "digital",
            "productos_activos": ["cuenta_ahorro", "tarjeta_credito"],
            "fecha_apertura_cuenta": "2022-01-01T00:00:00",
        },
        "similarity": round(0.95 - n * 0.05, 2),
    }


# ── Test 1: obtener_embedding retorna lista de 768 floats ─────────────────────

@pytest.mark.anyio
async def test_obtener_embedding_retorna_768_floats(service):
    embedding_mock = _fake_embedding(768)

    with patch(
        "google.generativeai.embed_content",
        return_value={"embedding": embedding_mock},
    ):
        resultado = await service.obtener_embedding("clientes inactivos bancarios")

    assert isinstance(resultado, list)
    assert len(resultado) == 768
    assert all(isinstance(v, float) for v in resultado)


# ── Test 2: obtener_embedding falla → propaga excepción ───────────────────────

@pytest.mark.anyio
async def test_obtener_embedding_propaga_error(service):
    with patch(
        "google.generativeai.embed_content",
        side_effect=Exception("API no disponible"),
    ):
        with pytest.raises(Exception, match="API no disponible"):
            await service.obtener_embedding("texto cualquiera")


# ── Test 3: construir_contexto_prompt con lista vacía ─────────────────────────

def test_construir_contexto_vacio(service):
    resultado = service.construir_contexto_prompt([])
    assert resultado == _SIN_PERFILES


# ── Test 4: construir_contexto_prompt con 3 clientes ─────────────────────────

def test_construir_contexto_con_3_clientes(service):
    clientes = [_fake_cliente(i) for i in range(1, 4)]
    resultado = service.construir_contexto_prompt(clientes)

    assert "PERFILES DE CLIENTES RELEVANTES:" in resultado
    assert "Cliente 1:" in resultado
    assert "Cliente 2:" in resultado
    assert "Cliente 3:" in resultado
    # Campos clave presentes en cada entrada
    assert resultado.count("Segmento:") == 3
    assert resultado.count("Score:") == 3
    assert resultado.count("Similitud:") == 3


# ── Test 5: construir_contexto_prompt formatea campos correctamente ────────────

def test_construir_contexto_formato_correcto(service):
    cliente = _fake_cliente(1)
    resultado = service.construir_contexto_prompt([cliente])

    assert "fidelizacion" in resultado
    assert "710" in resultado                  # score_crediticio 700+1*10
    assert "5" in resultado                    # operaciones_ultimo_mes
    assert "digital" in resultado
    assert "cuenta_ahorro" in resultado
    assert "0.90" in resultado                 # similarity 0.95-1*0.05


# ── Test 6: recuperar_contexto llama RPC con parámetros correctos ─────────────

@pytest.mark.anyio
async def test_recuperar_contexto_llama_rpc_correctamente(service, mock_supabase):
    embedding_fijo = _fake_embedding(768)
    perfiles_mock = [_fake_cliente(1)]

    # Mock del RPC chain: .rpc(...).execute() → MagicMock con .data
    mock_execute = MagicMock()
    mock_execute.data = perfiles_mock
    mock_rpc = MagicMock()
    mock_rpc.execute.return_value = mock_execute
    mock_supabase.rpc.return_value = mock_rpc

    # Evitar llamada real a Gemini
    service.obtener_embedding = AsyncMock(return_value=embedding_fijo)

    resultado = await service.recuperar_contexto(
        instruccion="clientes con alta morosidad",
        dimension="reactivacion",
        top_k=5,
        similarity_threshold=0.75,
    )

    # Verifica que se llamó al RPC correcto con los parámetros exactos
    mock_supabase.rpc.assert_called_once_with(
        "match_clientes_embeddings",
        {
            "query_embedding":      embedding_fijo,
            "match_dimension":      "reactivacion",
            "match_count":          5,
            "similarity_threshold": 0.75,
        },
    )
    assert resultado == perfiles_mock


# ── Test 7: recuperar_contexto retorna [] si Gemini falla ─────────────────────

@pytest.mark.anyio
async def test_recuperar_contexto_retorna_vacio_si_embedding_falla(service):
    service.obtener_embedding = AsyncMock(side_effect=Exception("Gemini caído"))

    resultado = await service.recuperar_contexto("instrucción", "onboarding")

    assert resultado == []


# ── Test 8: recuperar_contexto retorna [] si Supabase falla ───────────────────

@pytest.mark.anyio
async def test_recuperar_contexto_retorna_vacio_si_supabase_falla(service, mock_supabase):
    service.obtener_embedding = AsyncMock(return_value=_fake_embedding())
    mock_supabase.rpc.side_effect = Exception("Supabase no disponible")

    resultado = await service.recuperar_contexto("instrucción", "fidelizacion")

    assert resultado == []


# ── Test 9: calcular_precision cubre casos límite ─────────────────────────────

def test_calcular_precision_casos_limite(service):
    assert service.calcular_precision(0, 0) == 0.0
    assert service.calcular_precision(100, 90) == pytest.approx(90.0)
    assert service.calcular_precision(200, 200) == pytest.approx(100.0)
    assert service.calcular_precision(100, 85) >= 85.0   # cumple umbral I3
