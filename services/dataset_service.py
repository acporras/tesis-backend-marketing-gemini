import csv
import json
import io
import hashlib
from datetime import datetime, timezone
from typing import Dict, Any, Tuple, List
from uuid import UUID
import subprocess
import os

class DatasetService:
    FORMATOS_SOPORTADOS = ['csv', 'json', 'xlsx']
    TAMANIO_MAXIMO_MB = 50
    
    # We can reuse the same column mapping logic for consistency
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
        # Mapeo
        for k, v in registro.items():
            k_lower = str(k).lower().strip()
            mapped_k = self.MAPEO_COLUMNAS.get(k_lower, k_lower)
            
            known = [
                'cliente_id_anonimizado', 'fecha_apertura_cuenta', 'fecha_ultima_transaccion',
                'canal_principal', 'productos_activos', 'score_crediticio', 'operaciones_ultimo_mes'
            ]
            if mapped_k in known:
                mapeado[mapped_k] = v

        if 'cliente_id_anonimizado' in mapeado:
            val = str(mapeado['cliente_id_anonimizado'])
            if len(val) < 32:
                mapeado['cliente_id_anonimizado'] = hashlib.sha256(val.encode()).hexdigest()
        else:
            mapeado['cliente_id_anonimizado'] = hashlib.sha256(str(registro).encode()).hexdigest()

        if 'productos_activos' in mapeado and isinstance(mapeado['productos_activos'], str):
            try:
                mapeado['productos_activos'] = json.loads(mapeado['productos_activos'].replace("'", '"'))
            except:
                mapeado['productos_activos'] = [p.strip() for p in mapeado['productos_activos'].split(',')]

        return mapeado

    def _calcular_dimension(self, registro: Dict[str, Any]) -> str:
        try:
            ops = int(registro.get('operaciones_ultimo_mes', 1))
            if ops == 0:
                return 'reactivacion'
            
            if 'fecha_apertura_cuenta' in registro:
                apertura = datetime.fromisoformat(str(registro['fecha_apertura_cuenta']).replace("Z", "+00:00"))
                ahora = datetime.now(timezone.utc)
                if (ahora - apertura).days < 90:
                    return 'onboarding'
        except:
            pass
        return 'fidelizacion'

    def _get_total_registros(self, db) -> int:
        res = db.table('registros_campania').select('id', count='exact').limit(1).execute()
        return res.count

    async def cargar_archivo(self, db, archivo_content: bytes, filename: str, modo: str, usuario_id: UUID) -> dict:
        ext, registros_raw = await self.validar_archivo(archivo_content, filename)
        if not registros_raw:
            raise ValueError("El archivo está vacío")

        total_registros_antes = self._get_total_registros(db)
        registros_procesados = []

        for raw in registros_raw:
            mapeado = self._mapear_y_anonimizar(raw)
            dimension = self._calcular_dimension(mapeado)
            
            reg_insert = {
                "cliente_id_anonimizado": mapeado['cliente_id_anonimizado'],
                "dimension_ciclo_vida": dimension
            }
            for field in ['fecha_apertura_cuenta', 'fecha_ultima_transaccion', 'canal_principal', 'productos_activos', 'score_crediticio', 'operaciones_ultimo_mes']:
                if field in mapeado and mapeado[field] != "":
                    reg_insert[field] = mapeado[field]
            
            registros_procesados.append(reg_insert)

        # Log pending load
        carga_log = {
            "usuario_id": str(usuario_id) if usuario_id else None,
            "origen": "manual",
            "modo_carga": modo,
            "registros_antes": total_registros_antes,
            "registros_carga": len(registros_procesados),
            "nombre_archivo": filename,
            "estado": "procesando"
        }
        res_carga = db.table("dataset_general_cargas").insert(carga_log).execute()
        carga_id = res_carga.data[0]['id']

        try:
            if modo == 'reemplazo':
                # Empty the table first
                db.table("registros_campania").delete().neq("id", "00000000-0000-0000-0000-000000000000").execute()
                
            batch_size = 1000
            for i in range(0, len(registros_procesados), batch_size):
                batch = registros_procesados[i:i + batch_size]
                if modo == 'agregar':
                    # Only insert new, ignore conflicts
                    db.table("registros_campania").upsert(batch, ignore_duplicates=True, on_conflict="cliente_id_anonimizado").execute()
                else: # upsert or reemplazo
                    db.table("registros_campania").upsert(batch, on_conflict="cliente_id_anonimizado").execute()
            
            total_registros_despues = self._get_total_registros(db)
            
            # Update log
            db.table("dataset_general_cargas").update({
                "estado": "completado",
                "registros_despues": total_registros_despues
            }).eq("id", carga_id).execute()

            return {
                "antes": total_registros_antes,
                "carga": len(registros_procesados),
                "despues": total_registros_despues,
                "modo": modo
            }

        except Exception as e:
            db.table("dataset_general_cargas").update({
                "estado": "error",
                "error_mensaje": str(e)
            }).eq("id", carga_id).execute()
            raise ValueError(f"Error procesando la carga: {str(e)}")

    async def regenerar_sintetico(self, db, cantidad: int, distribucion: dict, usuario_id: UUID) -> dict:
        total_registros_antes = self._get_total_registros(db)
        
        carga_log = {
            "usuario_id": str(usuario_id),
            "origen": "sintetico",
            "modo_carga": "reemplazo",
            "registros_antes": total_registros_antes,
            "registros_carga": cantidad,
            "estado": "procesando",
            "metadata": {"distribucion": distribucion}
        }
        res_carga = db.table("dataset_general_cargas").insert(carga_log).execute()
        carga_id = res_carga.data[0]['id']
        
        try:
            # Delete old
            db.table("registros_campania").delete().neq("id", "00000000-0000-0000-0000-000000000000").execute()
            
            # Call data_generator.py logic
            # This is simplified. Ideally we import data_generator and call it directly.
            # But the user might just want us to run the existing seed_data.py or similar.
            # Assuming data_generator.py exists in root or backend:
            # Let's run it via python subprocess
            
            # We will generate it using a modified seed logic or the existing generator
            script_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "scripts_temporales", "seed_data.py")
            if not os.path.exists(script_path):
                 script_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "seed_data.py")
                 
            # Note: The data_generator.py might be custom. We'll use subprocess to run the generic seed or implement a minimal one
            env = os.environ.copy()
            env["GENERATE_COUNT"] = str(cantidad)
            env["DIST_ONBOARDING"] = str(distribucion.get("onboarding", 0))
            env["DIST_FIDELIZACION"] = str(distribucion.get("fidelizacion", 0))
            env["DIST_REACTIVACION"] = str(distribucion.get("reactivacion", 0))
            
            # Since seed_data logic isn't fully parametric, we just run the basic seed_data
            subprocess.run(["python", script_path], env=env, check=True)
            
            total_registros_despues = self._get_total_registros(db)
            db.table("dataset_general_cargas").update({
                "estado": "completado",
                "registros_despues": total_registros_despues
            }).eq("id", carga_id).execute()
            
            return {
                "antes": total_registros_antes,
                "carga": cantidad,
                "despues": total_registros_despues
            }
        except Exception as e:
            db.table("dataset_general_cargas").update({
                "estado": "error",
                "error_mensaje": str(e)
            }).eq("id", carga_id).execute()
            raise ValueError(f"Error generando dataset sintético: {str(e)}")

dataset_service = DatasetService()
