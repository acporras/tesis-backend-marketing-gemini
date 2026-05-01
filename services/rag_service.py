# HU-09: RAG con pgvector para precisión de segmentación I3 ≥ 85%
# Arquitectura: instrucción → embedding → búsqueda coseno → contexto → Gemini
import asyncio
import logging
from datetime import datetime, timezone
from typing import Any

import google.generativeai as genai
from supabase import Client

logger = logging.getLogger(__name__)

EMBEDDING_MODEL = "models/gemini-embedding-2"
EMBEDDING_DIMS  = 768
_SIN_PERFILES   = "No se encontraron perfiles relevantes."


class RAGService:

    def __init__(self, client: Client) -> None:
        self._client = client

    # ── Embedding ─────────────────────────────────────────────────────────────

    async def obtener_embedding(
        self,
        texto: str,
        task_type: str = "retrieval_query",
    ) -> list[float]:
        """
        Genera un vector de 768 dimensiones usando models/embedding-001.
        task_type="retrieval_query"    → para buscar en el índice
        task_type="retrieval_document" → para indexar un documento
        """
        try:
            resultado = await asyncio.to_thread(
                genai.embed_content,
                model=EMBEDDING_MODEL,
                content=texto,
                task_type=task_type,
                output_dimensionality=EMBEDDING_DIMS,
            )
            embedding = list(resultado["embedding"])
            if len(embedding) != EMBEDDING_DIMS:
                raise ValueError(
                    f"El modelo retornó {len(embedding)} dims, se esperaban {EMBEDDING_DIMS}"
                )
            return embedding
        except Exception as exc:
            logger.error("Error generando embedding (task_type=%s): %s", task_type, exc)
            raise

    # ── Recuperación de contexto ───────────────────────────────────────────────

    async def recuperar_contexto(
        self,
        instruccion: str,
        dimension: str,
        top_k: int = 10,
        similarity_threshold: float = 0.7,
    ) -> list[dict]:
        """
        1. Genera embedding de la instrucción del analista.
        2. Llama a match_clientes_embeddings (función SQL en Supabase).
        3. Retorna perfiles: [{id, cliente_id_anonimizado, metadata, similarity}]
        """
        try:
            embedding = await self.obtener_embedding(instruccion)
        except Exception as exc:
            logger.error("No se pudo generar embedding para RAG: %s", exc)
            return []

        try:
            respuesta = await asyncio.to_thread(
                lambda: self._client.rpc(
                    "match_clientes_embeddings",
                    {
                        "query_embedding": embedding,
                        "match_dimension": dimension,
                        "match_count":     top_k,
                        "similarity_threshold": similarity_threshold,
                    },
                ).execute()
            )
            return respuesta.data or []
        except Exception as exc:
            logger.error("Error en RPC match_clientes_embeddings: %s", exc)
            return []

    # ── Construcción del bloque de contexto para Gemini ───────────────────────

    def construir_contexto_prompt(self, clientes: list[dict]) -> str:
        """
        Convierte la lista de perfiles RAG en texto estructurado
        para inyectar en el prompt de Gemini.
        Si la lista está vacía retorna el mensaje estándar de sin resultados.
        """
        if not clientes:
            return _SIN_PERFILES

        lineas: list[str] = ["PERFILES DE CLIENTES RELEVANTES:"]

        for i, cliente in enumerate(clientes, start=1):
            meta: dict[str, Any] = cliente.get("metadata") or {}
            similarity: float = float(cliente.get("similarity", 0.0))

            productos = meta.get("productos_activos", [])
            if isinstance(productos, str):
                productos = [productos]

            antigüedad = _calcular_antiguedad_meses(meta.get("fecha_apertura_cuenta"))

            lineas += [
                f"\nCliente {i}:",
                f"  - Segmento: {meta.get('dimension_ciclo_vida', 'N/D')}",
                f"  - Antigüedad: {antigüedad} meses",
                f"  - Ops/mes: {meta.get('operaciones_ultimo_mes', 'N/D')}",
                f"  - Score: {meta.get('score_crediticio', 'N/D')}",
                f"  - Canal: {meta.get('canal_principal', 'N/D')}",
                f"  - Productos: {productos}",
                f"  - Similitud: {similarity:.2f}",
            ]

        return "\n".join(lineas)

    # ── Indexación ────────────────────────────────────────────────────────────

    async def indexar_cliente(
        self,
        cliente_id: str,
        metadata: dict,
        dimension: str,
    ) -> None:
        """
        Genera embedding del perfil del cliente y lo guarda en
        clientes_embeddings con UPSERT (clave: cliente_id_anonimizado).
        """
        texto = _metadata_a_texto(metadata)
        try:
            embedding = await self.obtener_embedding(texto, task_type="retrieval_document")
        except Exception as exc:
            logger.error("Error generando embedding para cliente %s: %s", cliente_id, exc)
            raise

        try:
            await asyncio.to_thread(
                lambda: self._client.table("clientes_embeddings").upsert(
                    {
                        "cliente_id_anonimizado": cliente_id,
                        "embedding": embedding,
                        "metadata":  metadata,
                        "dimension": dimension,
                        "updated_at": datetime.now(timezone.utc).isoformat(),
                    },
                    on_conflict="cliente_id_anonimizado",
                ).execute()
            )
        except Exception as exc:
            logger.error("Error guardando embedding en Supabase: %s", exc)
            raise

    # ── Indicador I3 ──────────────────────────────────────────────────────────

    @staticmethod
    def calcular_precision(total: int, cumplen_criterios: int) -> float:
        """I3: PS = (CS / TP) × 100 — meta ≥ 85%"""
        if total == 0:
            return 0.0
        return round((cumplen_criterios / total) * 100, 2)


# ── Funciones auxiliares (módulo-nivel para facilitar testeo) ─────────────────

def _calcular_antiguedad_meses(fecha_apertura: str | None) -> int:
    if not fecha_apertura:
        return 0
    try:
        apertura = datetime.fromisoformat(str(fecha_apertura).replace("Z", "+00:00"))
        if apertura.tzinfo is None:
            apertura = apertura.replace(tzinfo=timezone.utc)
        delta = datetime.now(timezone.utc) - apertura
        return max(0, delta.days // 30)
    except (ValueError, TypeError):
        return 0


def _metadata_a_texto(metadata: dict) -> str:
    """Serializa metadata del cliente a texto para generar su embedding."""
    partes: list[str] = []

    if dim := metadata.get("dimension_ciclo_vida"):
        partes.append(f"Segmento {dim}")
    if score := metadata.get("score_crediticio"):
        partes.append(f"Score crediticio {score}")
    if (ops := metadata.get("operaciones_ultimo_mes")) is not None:
        partes.append(f"Operaciones mensuales {ops}")
    if canal := metadata.get("canal_principal"):
        partes.append(f"Canal principal {canal}")
    productos = metadata.get("productos_activos", [])
    if isinstance(productos, list) and productos:
        partes.append(f"Productos: {', '.join(str(p) for p in productos)}")

    return ". ".join(partes) if partes else "Cliente bancario"
