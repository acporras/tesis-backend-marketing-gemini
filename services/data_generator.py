# Sprint 1 — generador de datos sintéticos bancarios
# Produce registros realistas sin PII real para poblar registros_campania
# y clientes_embeddings durante desarrollo y pruebas
import hashlib
import random
from datetime import datetime, timedelta

import pandas as pd
from faker import Faker

faker = Faker("es_ES")   # es_PE no existe en faker; es_ES es el más cercano disponible

CANALES = ["digital", "presencial", "call_center", "app_movil"]
DIMENSIONES = ["onboarding", "fidelizacion", "reactivacion"]
PRODUCTOS = ["cuenta_ahorro", "tarjeta_credito", "prestamo_personal", "seguro_vida", "fondo_inversion"]


def _hash_id(raw: str) -> str:
    return hashlib.sha256(raw.encode()).hexdigest()


def generar_cliente(seed: int | None = None) -> dict:
    """Genera un registro sintético de cliente sin PII."""
    if seed is not None:
        random.seed(seed)
        faker.seed_instance(seed)

    fake_dni = str(random.randint(10_000_000, 99_999_999))
    cliente_id = _hash_id(fake_dni)

    fecha_apertura = faker.date_time_between(start_date="-5y", end_date="-1m")
    dias_inactivo = random.randint(0, 180)
    fecha_ultima_tx = datetime.now() - timedelta(days=dias_inactivo)

    dimension = (
        "onboarding" if (datetime.now() - fecha_apertura).days < 90
        else "reactivacion" if dias_inactivo >= 30
        else "fidelizacion"
    )

    return {
        "cliente_id_anonimizado": cliente_id,
        "fecha_apertura_cuenta": fecha_apertura.isoformat(),
        "fecha_ultima_transaccion": fecha_ultima_tx.isoformat(),
        "canal_principal": random.choice(CANALES),
        "productos_activos": random.sample(PRODUCTOS, k=random.randint(1, 3)),
        "score_crediticio": random.randint(300, 850),
        "operaciones_ultimo_mes": random.randint(0, 50),
        "dimension_ciclo_vida": dimension,
    }


def generar_dataset(n: int = 100, seed: int = 42) -> pd.DataFrame:
    """Genera un DataFrame con n clientes sintéticos."""
    random.seed(seed)
    faker.seed_instance(seed)
    registros = [generar_cliente() for _ in range(n)]
    return pd.DataFrame(registros)


def exportar_csv(n: int = 100, ruta: str = "datos_sinteticos.csv") -> str:
    """Exporta el dataset a CSV y retorna la ruta."""
    df = generar_dataset(n)
    df.to_csv(ruta, index=False)
    return ruta


if __name__ == "__main__":
    ruta = exportar_csv(n=500, ruta="datos_sinteticos.csv")
    print(f"Dataset generado: {ruta} — 500 registros")
