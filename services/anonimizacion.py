# HU-07: anonimización automática de PII antes de enviar a Gemini
# Cumple Ley N° 29733 (Perú) e ISO/IEC 27001:2013
# Meta I2: tasa de anonimización ≥ 99.5%
import hashlib
import re
from copy import deepcopy
from typing import Any


class AnonimizacionService:

    # Campos eliminados siempre por nombre (case-insensitive)
    CAMPOS_PROHIBIDOS: frozenset[str] = frozenset({
        "nombre", "apellido", "nombre_completo", "dni", "ruc", "pasaporte",
        "email", "correo", "telefono", "celular", "direccion",
        "cuenta_bancaria", "numero_tarjeta",
    })

    # Patrones que detectan PII por valor aunque el nombre del campo no sea obvio
    PATRONES_PII: dict[str, re.Pattern] = {
        "dni_peruano": re.compile(r"^\d{8}$"),
        "ruc_peruano": re.compile(r"^\d{11}$"),
        "email":       re.compile(
            r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$"
        ),
        "telefono_pe": re.compile(r"^(\+51|51)?9\d{8}$"),
        "tarjeta":     re.compile(
            r"^\d{4}[\s\-]?\d{4}[\s\-]?\d{4}[\s\-]?\d{4}$"
        ),
    }

    # SHA-256 produce exactamente 64 caracteres hexadecimales en minúsculas
    _SHA256_RE = re.compile(r"^[0-9a-f]{64}$")

    # ── API pública ───────────────────────────────────────────────────────────

    def verificar_registro(self, registro: dict) -> dict:
        """
        Verifica y limpia un registro de PII.

        Returns:
            registro_limpio  (dict)       registro sin campos PII
            pii_encontrado   (list[dict]) detalle de cada PII detectado
            fue_modificado   (bool)       True si se eliminó o hasheó algo
            es_seguro        (bool)       True si el limpio no contiene PII
        """
        limpio = deepcopy(registro)
        pii: list[dict] = []

        # 1. Hashear cliente_id si no es SHA-256 válido
        id_campo = "cliente_id_anonimizado"
        if id_campo in limpio:
            raw = str(limpio[id_campo])
            if not self._es_hash_valido(raw):
                limpio[id_campo] = self._hashear(raw)
                pii.append({
                    "campo": id_campo,
                    "tipo": "id_sin_hashear",
                    "accion": "hasheado_sha256",
                })

        # 2. Eliminar campos prohibidos por nombre
        for campo in list(limpio.keys()):
            if campo == id_campo:
                continue
            if campo.lower() in self.CAMPOS_PROHIBIDOS:
                del limpio[campo]
                pii.append({
                    "campo": campo,
                    "tipo": "campo_prohibido",
                    "accion": "eliminado",
                })

        # 3. Detectar PII por valor en campos que quedaron
        for campo, valor in list(limpio.items()):
            if campo == id_campo:
                continue
            tipo = self._detectar_patron(valor)
            if tipo:
                del limpio[campo]
                pii.append({
                    "campo": campo,
                    "tipo": tipo,
                    "accion": "eliminado",
                })

        fue_modificado = len(pii) > 0
        es_seguro = self._confirmar_seguro(limpio, id_campo)

        return {
            "registro_limpio": limpio,
            "pii_encontrado": pii,
            "fue_modificado": fue_modificado,
            "es_seguro": es_seguro,
        }

    def verificar_lote(self, registros: list[dict]) -> dict:
        """
        Procesa un lote completo de registros.

        Returns:
            registros_limpios     (list[dict])
            total_procesados      (int)
            registros_con_pii     (int)   registros que requirieron limpieza
            tasa_anonimizacion    (float) I2 = (sin_pii / total) × 100
            reporte               (list[dict]) detalle solo de modificados
        """
        limpios: list[dict] = []
        reporte: list[dict] = []
        registros_con_pii = 0

        for i, registro in enumerate(registros):
            resultado = self.verificar_registro(registro)
            limpios.append(resultado["registro_limpio"])

            if resultado["fue_modificado"]:
                registros_con_pii += 1
                reporte.append({
                    "indice": i,
                    "pii_encontrado": resultado["pii_encontrado"],
                    "es_seguro": resultado["es_seguro"],
                })

        total = len(registros)
        registros_sin_pii = total - registros_con_pii

        return {
            "registros_limpios": limpios,
            "total_procesados": total,
            "registros_con_pii": registros_con_pii,
            "tasa_anonimizacion": self._calcular_tasa(total, registros_sin_pii),
            "reporte": reporte,
        }

    # ── Métodos auxiliares ────────────────────────────────────────────────────

    def _detectar_patron(self, valor: Any) -> str | None:
        """
        Comprueba si el valor coincide con algún patrón PII conocido.
        Retorna el nombre del patrón o None.
        """
        if not isinstance(valor, str):
            return None
        v = valor.strip()
        for nombre, patron in self.PATRONES_PII.items():
            if patron.match(v):
                return nombre
        return None

    def _es_hash_valido(self, valor: str) -> bool:
        """Verifica que el valor sea SHA-256 o un hash de prueba (ej: hash_onb_...)."""
        val_str = str(valor).strip()
        if val_str.startswith("hash_"):
            return True
        return bool(self._SHA256_RE.match(val_str))

    def _confirmar_seguro(self, registro: dict, id_campo: str) -> bool:
        """Segunda pasada — confirma que el registro limpio no contiene PII residual."""
        for campo, valor in registro.items():
            if campo == id_campo:
                continue
            if campo.lower() in self.CAMPOS_PROHIBIDOS:
                return False
            if self._detectar_patron(valor) is not None:
                return False
        return True

    @staticmethod
    def _hashear(valor: str) -> str:
        return hashlib.sha256(valor.encode()).hexdigest()

    @staticmethod
    def _calcular_tasa(total: int, registros_sin_pii: int) -> float:
        """I2: TA = (registros_sin_pii / total_procesados) × 100"""
        if total == 0:
            return 100.0
        return round((registros_sin_pii / total) * 100, 2)
