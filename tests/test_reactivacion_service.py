"""
Tests del servicio ReactivacionService (HU-05, HU-06).
Se mockean Supabase y el Orquestador para aislar la lógica de negocio.
"""
import asyncio
from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock, MagicMock

from services.reactivacion_service import ReactivacionService, _dias_inactividad


# ── Helpers para generar registros de prueba ──────────────────────────────────

def _registro(dias_inactivo: int, *, cliente_id: str = "c001") -> dict:
    """Crea un registro con fecha_ultima_transaccion hace N días."""
    fecha = (datetime.now(timezone.utc) - timedelta(days=dias_inactivo)).isoformat()
    return {
        "cliente_id_anonimizado":    cliente_id,
        "dimension_ciclo_vida":      "reactivacion",
        "fecha_ultima_transaccion":  fecha,
        "score_crediticio":          600,
    }


# ── Tests de _clasificar_por_inactividad ──────────────────────────────────────

def test_clasificar_seis_registros_dos_por_grupo():
    """6 registros (2 por grupo) se clasifican correctamente."""
    servicio = ReactivacionService(db=MagicMock())

    registros = [
        _registro(30, cliente_id="r1"),
        _registro(45, cliente_id="r2"),
        _registro(46, cliente_id="m1"),
        _registro(90, cliente_id="m2"),
        _registro(91, cliente_id="p1"),
        _registro(180, cliente_id="p2"),
    ]

    grupos = servicio._clasificar_por_inactividad(registros)

    assert len(grupos["reciente"])   == 2, "Esperados 2 en reciente"
    assert len(grupos["moderada"])   == 2, "Esperados 2 en moderada"
    assert len(grupos["prolongada"]) == 2, "Esperados 2 en prolongada"


def test_clasificar_excluye_registros_activos():
    """Registros con < 30 días de inactividad son excluidos de todos los grupos."""
    servicio = ReactivacionService(db=MagicMock())

    registros = [
        _registro(0,  cliente_id="activo1"),
        _registro(15, cliente_id="activo2"),
        _registro(29, cliente_id="activo3"),
        _registro(30, cliente_id="inactivo1"),
    ]

    grupos = servicio._clasificar_por_inactividad(registros)

    total = len(grupos["reciente"]) + len(grupos["moderada"]) + len(grupos["prolongada"])
    assert total == 1, "Solo el registro con 30 días debe clasificarse"
    assert len(grupos["reciente"]) == 1


def test_clasificar_excluye_registros_sin_fecha():
    """Registros sin fecha_ultima_transaccion no se clasifican."""
    servicio = ReactivacionService(db=MagicMock())

    registros = [
        {"cliente_id_anonimizado": "sin_fecha", "fecha_ultima_transaccion": None},
        {"cliente_id_anonimizado": "sin_campo"},
        _registro(60, cliente_id="con_fecha"),
    ]

    grupos = servicio._clasificar_por_inactividad(registros)

    total = len(grupos["reciente"]) + len(grupos["moderada"]) + len(grupos["prolongada"])
    assert total == 1
    assert len(grupos["moderada"]) == 1


def test_clasificar_lista_vacia_retorna_grupos_vacios():
    """Lista vacía produce grupos vacíos sin errores."""
    servicio = ReactivacionService(db=MagicMock())
    grupos = servicio._clasificar_por_inactividad([])
    assert grupos == {"reciente": [], "moderada": [], "prolongada": []}


# ── Tests de _construir_instruccion_por_perfil ────────────────────────────────

def test_instruccion_perfil_reciente():
    servicio = ReactivacionService(db=MagicMock())
    instruccion = servicio._construir_instruccion_por_perfil("reciente", 10)
    assert "10" in instruccion
    assert "30-45 días" in instruccion
    assert "cercano" in instruccion.lower() or "beneficios" in instruccion.lower()


def test_instruccion_perfil_moderada():
    servicio = ReactivacionService(db=MagicMock())
    instruccion = servicio._construir_instruccion_por_perfil("moderada", 25)
    assert "25" in instruccion
    assert "46-90 días" in instruccion
    assert "oferta especial" in instruccion.lower() or "especial" in instruccion.lower()


def test_instruccion_perfil_prolongada():
    servicio = ReactivacionService(db=MagicMock())
    instruccion = servicio._construir_instruccion_por_perfil("prolongada", 5)
    assert "5" in instruccion
    assert "90 días" in instruccion
    assert "win-back" in instruccion.lower() or "incentivo" in instruccion.lower()


def test_instruccion_perfil_desconocido_retorna_generico():
    servicio = ReactivacionService(db=MagicMock())
    instruccion = servicio._construir_instruccion_por_perfil("otro", 7)
    assert "7" in instruccion
    assert "reactivación" in instruccion.lower()


# ── Tests de detectar_clientes_inactivos ─────────────────────────────────────

def test_detectar_sin_clientes_inactivos_retorna_cero_campanas():
    """Si no hay clientes inactivos (todos recientes < 30 días), no se generan campañas."""
    registros_activos = [
        _registro(5,  cliente_id="a1"),
        _registro(10, cliente_id="a2"),
    ]
    mock_db = _make_db_con_registros(registros_activos)
    servicio = ReactivacionService(db=mock_db, orquestador=None)

    resumen = asyncio.run(servicio.detectar_clientes_inactivos())

    assert resumen["campanas_generadas"] == 0
    assert resumen["grupos_detectados"]["reciente"]   == 0
    assert resumen["grupos_detectados"]["moderada"]   == 0
    assert resumen["grupos_detectados"]["prolongada"] == 0
    assert resumen["errores"] == []


def test_detectar_con_tres_grupos_genera_tres_campanas():
    """Con 1 cliente por grupo se generan 3 campañas."""
    registros = [
        _registro(35,  cliente_id="r1"),
        _registro(60,  cliente_id="m1"),
        _registro(120, cliente_id="p1"),
    ]
    mock_db = _make_db_con_registros(registros, campana_id="nueva-camp-id")
    mock_orquestador = _make_orquestador()

    servicio = ReactivacionService(db=mock_db, orquestador=mock_orquestador)
    resumen  = asyncio.run(servicio.detectar_clientes_inactivos())

    assert resumen["campanas_generadas"] == 3
    assert resumen["grupos_detectados"]["reciente"]   == 1
    assert resumen["grupos_detectados"]["moderada"]   == 1
    assert resumen["grupos_detectados"]["prolongada"] == 1
    assert resumen["errores"] == []
    # El orquestador fue llamado 3 veces (una por grupo)
    assert mock_orquestador.procesar_instruccion.call_count == 3


def test_detectar_maneja_error_gemini_sin_romper():
    """Si Gemini falla, el error se registra pero el loop continúa."""
    registros = [
        _registro(35,  cliente_id="r1"),
        _registro(120, cliente_id="p1"),
    ]
    mock_db = _make_db_con_registros(registros, campana_id="camp-error-test")

    # Orquestador retorna error en todas las llamadas
    mock_orquestador = MagicMock()
    mock_orquestador.procesar_instruccion = AsyncMock(
        return_value={"error": "gemini_no_disponible", "detalle": "timeout"}
    )

    servicio = ReactivacionService(db=mock_db, orquestador=mock_orquestador)
    resumen  = asyncio.run(servicio.detectar_clientes_inactivos())

    # Las campañas se crean (insert) aunque Gemini falle
    assert resumen["campanas_generadas"] == 2
    # Se registran errores por las respuestas de Gemini
    assert len(resumen["errores"]) == 2
    for err in resumen["errores"]:
        assert "timeout" in err or "Gemini" in err or "gemini" in err.lower()


def test_detectar_error_de_bd_retorna_resumen_con_error():
    """Si la consulta a Supabase falla, se retorna resumen con error y 0 campañas."""
    mock_db = MagicMock()
    mock_db.table.return_value.select.return_value.eq.return_value.execute.side_effect = (
        RuntimeError("connexion error")
    )

    servicio = ReactivacionService(db=mock_db)
    resumen  = asyncio.run(servicio.detectar_clientes_inactivos())

    assert resumen["campanas_generadas"] == 0
    assert len(resumen["errores"]) >= 1


# ── Tests de _dias_inactividad (utilidad de módulo) ───────────────────────────

def test_dias_inactividad_fecha_valida():
    ahora = datetime.now(timezone.utc)
    fecha = (ahora - timedelta(days=50)).isoformat()
    dias  = _dias_inactividad(fecha, ahora)
    assert dias == 50


def test_dias_inactividad_fecha_none_retorna_none():
    assert _dias_inactividad(None, datetime.now(timezone.utc)) is None


def test_dias_inactividad_fecha_invalida_retorna_none():
    assert _dias_inactividad("no-es-fecha", datetime.now(timezone.utc)) is None


# ── Helpers de mocks ──────────────────────────────────────────────────────────

def _make_db_con_registros(registros: list[dict], campana_id: str = "test-camp-id") -> MagicMock:
    """Crea un mock de DB que retorna los registros dados y simula insert de campana."""
    mock_db = MagicMock()

    # Tabla registros_campania → devuelve registros
    t_reg = MagicMock()
    t_reg.select.return_value = t_reg
    t_reg.eq.return_value     = t_reg
    t_reg.execute.return_value.data = registros

    # Tabla campanas → insert retorna campana con id
    t_camp = MagicMock()
    t_camp.insert.return_value.execute.return_value.data = [{"id": campana_id}]
    t_camp.update.return_value.eq.return_value.execute.return_value.data = [{"id": campana_id}]

    mock_db.table.side_effect = lambda nombre: {
        "registros_campania": t_reg,
        "campanas":           t_camp,
    }.get(nombre, MagicMock())

    return mock_db


def _make_orquestador(resultado: dict | None = None) -> MagicMock:
    orq = MagicMock()
    orq.procesar_instruccion = AsyncMock(
        return_value=resultado if resultado is not None else {
            "segmento":         {"dimension_ciclo_vida": {"eq": "reactivacion"}},
            "tamanio_audiencia": 100,
            "mensaje":           "Vuelve, te extrañamos.",
            "tipo_campana":      "reactivacion",
            "tono":              "cercano",
            "metricas": {
                "tiempo_respuesta_ms": 1200,
                "tasa_anonimizacion":  100.0,
                "precision_segmento":  90.0,
            },
        }
    )
    return orq
