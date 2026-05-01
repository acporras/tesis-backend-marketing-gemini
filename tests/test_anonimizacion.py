import hashlib
import pytest

from services.anonimizacion import AnonimizacionService
from services.data_generator import generar_dataset

service = AnonimizacionService()

ID_HASH_VALIDO = "a" * 64          # 64 chars hex válido
ID_SIN_HASH   = "12345678"         # DNI de 8 dígitos — debe ser hasheado


# ── Helpers ───────────────────────────────────────────────────────────────────

def _sha256(valor: str) -> str:
    return hashlib.sha256(valor.encode()).hexdigest()


def _registro_limpio(**extra) -> dict:
    """Registro base sin ningún campo PII."""
    base = {
        "cliente_id_anonimizado": ID_HASH_VALIDO,
        "canal_principal": "digital",
        "score_crediticio": 720,
        "operaciones_ultimo_mes": 5,
        "dimension_ciclo_vida": "fidelizacion",
    }
    base.update(extra)
    return base


# ── Test 1: registro limpio pasa sin modificaciones ──────────────────────────

def test_registro_limpio_pasa_sin_modificaciones():
    resultado = service.verificar_registro(_registro_limpio())

    assert resultado["fue_modificado"] is False
    assert resultado["es_seguro"] is True
    assert resultado["pii_encontrado"] == []
    # Los campos del registro original se conservan íntegros
    assert resultado["registro_limpio"]["canal_principal"] == "digital"
    assert resultado["registro_limpio"]["score_crediticio"] == 720
    assert resultado["registro_limpio"]["cliente_id_anonimizado"] == ID_HASH_VALIDO


# ── Test 2: campo prohibido DNI es eliminado ──────────────────────────────────

def test_campo_prohibido_dni_es_eliminado():
    registro = _registro_limpio(dni="87654321")
    resultado = service.verificar_registro(registro)

    assert resultado["fue_modificado"] is True
    assert "dni" not in resultado["registro_limpio"]
    assert resultado["es_seguro"] is True

    tipos = [p["tipo"] for p in resultado["pii_encontrado"]]
    assert "campo_prohibido" in tipos

    campos = [p["campo"] for p in resultado["pii_encontrado"]]
    assert "dni" in campos


# ── Test 3: campo prohibido email es eliminado ────────────────────────────────

def test_campo_prohibido_email_es_eliminado():
    registro = _registro_limpio(email="juan.perez@banco.pe")
    resultado = service.verificar_registro(registro)

    assert resultado["fue_modificado"] is True
    assert "email" not in resultado["registro_limpio"]
    assert resultado["es_seguro"] is True

    campos = [p["campo"] for p in resultado["pii_encontrado"]]
    assert "email" in campos


# ── Test 4: valor con email en campo genérico es detectado por patrón ─────────

def test_valor_email_en_campo_generico_detectado_por_patron():
    # El campo no se llama 'email' pero el valor es un email
    registro = _registro_limpio(dato_contacto="carlos@ejemplo.com")
    resultado = service.verificar_registro(registro)

    assert resultado["fue_modificado"] is True
    assert "dato_contacto" not in resultado["registro_limpio"]

    tipos = [p["tipo"] for p in resultado["pii_encontrado"]]
    assert "email" in tipos


# ── Test 5: ID sin hashear es hasheado automáticamente ────────────────────────

def test_id_sin_hashear_es_hasheado_automaticamente():
    registro = _registro_limpio()
    registro["cliente_id_anonimizado"] = ID_SIN_HASH   # 8 dígitos → no es hash

    resultado = service.verificar_registro(registro)

    id_resultante = resultado["registro_limpio"]["cliente_id_anonimizado"]
    assert id_resultante == _sha256(ID_SIN_HASH)
    assert len(id_resultante) == 64

    tipos = [p["tipo"] for p in resultado["pii_encontrado"]]
    assert "id_sin_hashear" in tipos


# ── Test 6: lote de 100 registros calcula tasa correctamente ──────────────────

def test_lote_calcula_tasa_correctamente():
    # 90 registros limpios + 10 con PII
    lote = [_registro_limpio() for _ in range(90)]
    con_pii = [_registro_limpio(dni=f"{i:08d}") for i in range(10)]
    lote.extend(con_pii)

    resultado = service.verificar_lote(lote)

    assert resultado["total_procesados"] == 100
    assert resultado["registros_con_pii"] == 10
    # Tasa = (90 / 100) × 100 = 90.0
    assert resultado["tasa_anonimizacion"] == pytest.approx(90.0)
    assert len(resultado["registros_limpios"]) == 100
    assert len(resultado["reporte"]) == 10


# ── Test 7: tasa ≥ 99.5% con lote estándar del generador ─────────────────────

def test_tasa_cumple_umbral_con_lote_estandar():
    # data_generator produce registros ya anonimizados (IDs hasheados, sin PII)
    df = generar_dataset(n=200, seed=42)
    registros = df.to_dict(orient="records")

    # Convertir listas a JSON serializable
    for r in registros:
        if isinstance(r.get("productos_activos"), list):
            r["productos_activos"] = r["productos_activos"]   # ya es lista, ok

    resultado = service.verificar_lote(registros)

    assert resultado["tasa_anonimizacion"] >= 99.5, (
        f"I2 = {resultado['tasa_anonimizacion']}% — por debajo del umbral del 99.5%\n"
        f"Registros con PII detectado: {resultado['reporte']}"
    )
