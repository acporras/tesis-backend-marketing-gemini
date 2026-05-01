import csv
import json
import io
import hashlib
from datetime import datetime, timezone
from typing import Dict, Any, Tuple, List
from uuid import UUID

class ImportacionService:
    FORMATOS_SOPORTADOS = ['csv', 'json', 'xlsx']
    TAMANIO_MAXIMO_MB = 10
    
    MAPEO_COLUMNAS = {
        'cliente_hash':          'cliente_id_anonimizado',
        'cliente_id':            'cliente_id_anonimizado',
        'id_cliente':            'cliente_id_anonimizado',
        'fecha_apertura':        'fecha_apertura_cuenta',
        'fec_apertura':          'fecha_apertura_cuenta',
        'fecha_ult_tx':          'fecha_ultima_transaccion',
        'fecha_ultima_tx':       'fecha_ultima_transaccion',
        'canal':                 'canal_principal',
        'productos':             'productos_activos',
        'score':                 'score_crediticio',
        'ops_mes':               'operaciones_ultimo_mes',
        'operaciones_mes':       'operaciones_ultimo_mes'
    }

    async def validar_archivo(self, file_content: bytes, filename: str) -> Tuple[str, List[Dict[str, Any]]]:
        if len(file_content) > self.TAMANIO_MAXIMO_MB * 1024 * 1024:
            raise ValueError(f"Archivo excede el tamaño máximo de {self.TAMANIO_MAXIMO_MB}MB")

        ext = filename.split('.')[-1].lower()
        if ext not in self.FORMATOS_SOPORTADOS:
            raise ValueError(f"Formato no soportado. Usa: {', '.join(self.FORMATOS_SOPORTADOS)}")

        registros = []
        if ext == 'csv':
            decoded = file_content.decode('utf-8')
            reader = csv.DictReader(io.StringIO(decoded))
            registros = list(reader)
        elif ext == 'json':
            decoded = file_content.decode('utf-8')
            registros = json.loads(decoded)
            if not isinstance(registros, list):
                raise ValueError("El JSON debe contener un arreglo de objetos")
        elif ext == 'xlsx':
            try:
                import pandas as pd
                df = pd.read_excel(io.BytesIO(file_content))
                registros = df.to_dict(orient='records')
            except ImportError:
                raise ValueError("Instala pandas y openpyxl para soportar archivos excel")
            except Exception as e:
                raise ValueError(f"Error procesando excel: {str(e)}")

        return ext, registros

    def _mapear_y_anonimizar(self, registro: Dict[str, Any]) -> Dict[str, Any]:
        mapeado = {}
        datos_adicionales = {}

        # Mapeo
        for k, v in registro.items():
            k_lower = str(k).lower().strip()
            mapped_k = self.MAPEO_COLUMNAS.get(k_lower, k_lower)
            
            # Known columns
            known = [
                'cliente_id_anonimizado', 'fecha_apertura_cuenta', 'fecha_ultima_transaccion',
                'canal_principal', 'productos_activos', 'score_crediticio', 'operaciones_ultimo_mes'
            ]
            if mapped_k in known:
                mapeado[mapped_k] = v
            else:
                datos_adicionales[k] = v

        # Anonimizar si no viene hash sino algo que parece un id real (simplificado)
        if 'cliente_id_anonimizado' in mapeado:
            val = str(mapeado['cliente_id_anonimizado'])
            # Si no es un hash largo, lo hasheamos
            if len(val) < 32:
                mapeado['cliente_id_anonimizado'] = hashlib.sha256(val.encode()).hexdigest()
        else:
            # Si no hay ID, generamos uno temporal basado en los datos para el mock
            mapeado['cliente_id_anonimizado'] = hashlib.sha256(str(registro).encode()).hexdigest()

        # Parsear productos si viene como string
        if 'productos_activos' in mapeado and isinstance(mapeado['productos_activos'], str):
            try:
                mapeado['productos_activos'] = json.loads(mapeado['productos_activos'].replace("'", '"'))
            except:
                mapeado['productos_activos'] = [p.strip() for p in mapeado['productos_activos'].split(',')]

        mapeado['datos_adicionales'] = datos_adicionales
        return mapeado

    def _calcular_dimension(self, registro: Dict[str, Any]) -> str:
        # Lógica heurística simple basada en fechas u operaciones
        try:
            ops = int(registro.get('operaciones_ultimo_mes', 1))
            if ops == 0:
                return 'reactivacion'
            
            # Si tiene fecha apertura reciente -> onboarding
            if 'fecha_apertura_cuenta' in registro:
                apertura = datetime.fromisoformat(str(registro['fecha_apertura_cuenta']).replace("Z", "+00:00"))
                ahora = datetime.now(timezone.utc)
                if (ahora - apertura).days < 90:
                    return 'onboarding'
        except:
            pass
        
        return 'fidelizacion'

    async def importar_archivo(
        self,
        db,
        archivo_content: bytes,
        filename: str,
        nombre: str,
        descripcion: str,
        usuario_id: UUID
    ) -> dict:
        
        ext, registros_raw = await self.validar_archivo(archivo_content, filename)
        if not registros_raw:
            raise ValueError("El archivo está vacío")

        columnas_origen = list(registros_raw[0].keys())
        total_registros = len(registros_raw)

        # 1. Crear Audiencia
        audiencia_data = {
            "usuario_id": str(usuario_id),
            "nombre": nombre,
            "descripcion": descripcion,
            "formato_origen": ext,
            "total_registros": total_registros,
            "columnas_origen": columnas_origen,
            "metadata": {"filename": filename, "hash": hashlib.md5(archivo_content).hexdigest()},
            "estado": "activa"
        }
        
        res_aud = db.table("audiencias_importadas").insert(audiencia_data).execute()
        audiencia_id = res_aud.data[0]['id']

        # 2. Procesar e insertar registros
        composicion = {"onboarding": 0, "fidelizacion": 0, "reactivacion": 0}
        registros_procesados = []

        for raw in registros_raw:
            mapeado = self._mapear_y_anonimizar(raw)
            dimension = self._calcular_dimension(mapeado)
            
            composicion[dimension] += 1
            
            reg_insert = {
                "audiencia_id": audiencia_id,
                "cliente_id_anonimizado": mapeado['cliente_id_anonimizado'],
                "dimension_ciclo_vida": dimension,
                "datos_adicionales": mapeado.get('datos_adicionales', {})
            }
            # Agregamos campos opcionales si existen
            for field in ['fecha_apertura_cuenta', 'fecha_ultima_transaccion', 'canal_principal', 'productos_activos', 'score_crediticio', 'operaciones_ultimo_mes']:
                if field in mapeado and mapeado[field] != "":
                    reg_insert[field] = mapeado[field]
            
            registros_procesados.append(reg_insert)

        # Inserción en lotes de a 1000 para no ahogar a supabase
        batch_size = 1000
        for i in range(0, len(registros_procesados), batch_size):
            batch = registros_procesados[i:i + batch_size]
            db.table("audiencia_registros").insert(batch).execute()

        return {
            "audiencia_id": audiencia_id,
            "total_registros": total_registros,
            "composicion": composicion
        }

importacion_service = ImportacionService()
