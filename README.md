# ADCAMI — Backend API

> **A**gente de **D**ecisión con **C**anal **A**daptado mediante **M**odelos de **I**nteligencia Artificial  
> Backend del sistema de orquestación de marketing bancario impulsado por **Google Gemini**.

[![Python](https://img.shields.io/badge/Python-3.12-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.110-009688?logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com/)
[![Supabase](https://img.shields.io/badge/Supabase-2.4-3ECF8E?logo=supabase&logoColor=white)](https://supabase.com/)
[![Gemini](https://img.shields.io/badge/Gemini-2.5%20Pro-4285F4?logo=google&logoColor=white)](https://deepmind.google/technologies/gemini/)
[![Railway](https://img.shields.io/badge/Deploy-Railway-0B0D0E?logo=railway&logoColor=white)](https://railway.app/)

---

## 📋 Descripción

ADCAMI es el backend de una plataforma de tesis que utiliza **LLMs (Gemini 2.5 Pro)** para generar y orquestar campañas de marketing personalizadas en el sector bancario. El sistema implementa tres módulos de ciclo de vida del cliente:

| Módulo | Descripción |
|---|---|
| **Onboarding** | Generación de mensajes de bienvenida y campañas iniciales para nuevos clientes |
| **Fidelización** | Estrategias de retención y activación para clientes activos |
| **Reactivación** | Recuperación automática (vía cron job nocturno) de clientes inactivos |

El modelo de Gemini decide el **canal óptimo** (email, SMS, push, WhatsApp) y genera el contenido de la campaña, todo trazable y auditable mediante logs persistidos en Supabase.

---

## 🏗️ Arquitectura

```
┌─────────────────────────────────────────────────────┐
│                    FastAPI App                       │
│                                                      │
│  ┌──────────┐  ┌────────────┐  ┌────────────────┐   │
│  │ Routers  │→ │  Services  │→ │  Gemini LLM    │   │
│  │ (REST)   │  │(Orquestador│  │  (canal_optimo │   │
│  └──────────┘  │  RAG, etc) │  │   + contenido) │   │
│                └─────┬──────┘  └────────────────┘   │
│                      │                               │
│              ┌───────▼────────┐                      │
│              │   Supabase     │                      │
│              │ (PostgreSQL +  │                      │
│              │  pgvector RAG) │                      │
│              └────────────────┘                      │
│                                                      │
│  ⏰ APScheduler → Reactivación nocturna (cron UTC)   │
└─────────────────────────────────────────────────────┘
```

---

## 🚀 Stack Tecnológico

| Categoría | Tecnología |
|---|---|
| Framework | FastAPI 0.110 |
| Runtime | Python 3.12 |
| LLM | Google Gemini 2.5 Pro (`google-generativeai`) |
| Base de datos | Supabase (PostgreSQL) |
| Embeddings / RAG | pgvector + LangChain |
| Scheduler | APScheduler 3.10 (cron jobs async) |
| Deploy | Railway (Nixpacks) |
| Testing | Pytest |

---

## 📁 Estructura del Proyecto

```
backend/
├── main.py                  # Entry point — FastAPI app + lifespan scheduler
├── config.py                # Settings con pydantic-settings
├── dependencies.py          # Dependencias de inyección (JWT, DB client)
├── requirements.txt
├── Procfile                 # Heroku-compatible start command
├── railway.toml             # Configuración de deploy Railway
├── runtime.txt              # python-3.12
│
├── routers/                 # Endpoints REST
│   ├── auth.py              # Login / registro
│   ├── onboarding.py        # Módulo onboarding
│   ├── fidelizacion.py      # Módulo fidelización
│   ├── reactivacion.py      # Módulo reactivación
│   ├── aprobacion.py        # Aprobación de campañas
│   ├── dashboard.py         # Métricas visuales
│   ├── metricas.py          # KPIs por campaña
│   ├── logs.py              # Auditoría de eventos
│   ├── datos.py             # Gestión de datasets
│   ├── audiencias.py        # Segmentación de audiencias
│   ├── admin.py             # Panel de administración
│   └── api_ingesta.py       # Ingesta de datos externos
│
├── services/                # Lógica de negocio
│   ├── orquestador.py       # Orquestación central con Gemini
│   ├── rag_service.py       # Recuperación aumentada (RAG)
│   ├── reactivacion_service.py  # Job nocturno de reactivación
│   ├── vault_service.py     # Gestión segura de secretos
│   ├── dataset_service.py   # Procesamiento de datasets
│   ├── importacion_service.py   # Importación de datos
│   ├── anonimizacion.py     # Anonimización de PII
│   └── data_generator.py    # Generación de datos sintéticos (Faker)
│
└── tests/                   # Suite de tests con Pytest
    ├── conftest.py
    ├── test_orquestador.py
    ├── test_rag_service.py
    ├── test_reactivacion_service.py
    ├── test_anonimizacion.py
    └── test_router_*.py     # Tests de integración por router
```

---

## ⚙️ Configuración Local

### 1. Prerrequisitos

- Python 3.12+
- Una cuenta en [Supabase](https://supabase.com/) con el proyecto creado
- Una API Key de [Google AI Studio](https://aistudio.google.com/)

### 2. Clonar e instalar

```bash
git clone git@github.com:acporras/tesis-backend-marketing-gemini.git
cd tesis-backend-marketing-gemini

python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate

pip install -r requirements.txt
```

### 3. Variables de entorno

```bash
cp .env.example .env
```

Edita `.env` con tus credenciales:

```env
# Gemini
GEMINI_API_KEY=AIza...
GEMINI_MODEL=gemini-2.5-pro
GEMINI_TEMPERATURE=0.3
GEMINI_MAX_OUTPUT_TOKENS=1024

# Supabase
SUPABASE_URL=https://<project-id>.supabase.co
SUPABASE_ANON_KEY=eyJ...
SUPABASE_SERVICE_KEY=eyJ...   # service_role key (bypasa RLS)

# App
DEBUG=true
ALLOWED_ORIGINS=http://localhost:3000,http://localhost:5173

# Scheduler (hora UTC — equivale a 02:00 Lima)
REACTIVATION_CRON_HOUR=7
REACTIVATION_CRON_MINUTE=0
INACTIVITY_THRESHOLD_DAYS=30
```

### 4. Ejecutar en desarrollo

```bash
uvicorn main:app --reload --port 8000
```

La documentación interactiva estará disponible en:
- Swagger UI: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc
- Health check: http://localhost:8000/health

---

## 🌐 Endpoints Principales

| Método | Ruta | Descripción |
|---|---|---|
| `GET` | `/health` | Health check |
| `POST` | `/auth/login` | Autenticación |
| `POST` | `/onboarding/generar` | Generar campaña de onboarding |
| `POST` | `/fidelizacion/generar` | Generar campaña de fidelización |
| `POST` | `/reactivacion/generar` | Generar campaña de reactivación |
| `GET` | `/aprobacion/pendientes` | Listar campañas pendientes de aprobación |
| `PATCH` | `/aprobacion/{id}` | Aprobar / rechazar campaña |
| `GET` | `/dashboard/resumen` | Resumen ejecutivo de campañas |
| `GET` | `/metricas/kpis` | KPIs por módulo y periodo |
| `GET` | `/logs` | Historial de auditoría |
| `POST` | `/datos/importar` | Importar dataset de clientes |
| `GET` | `/audiencias` | Listar segmentos de audiencia |

> Ver documentación completa en `/docs` (requiere `DEBUG=true`).

---

## 🧪 Tests

```bash
pytest tests/ -v
```

Para ejecutar un módulo específico:

```bash
pytest tests/test_orquestador.py -v
pytest tests/test_router_onboarding.py -v
```

---

## 🚂 Deploy en Railway

El proyecto está preconfigurado para Railway con `railway.toml`:

1. Conecta tu repositorio en [Railway](https://railway.app/)
2. Agrega las variables de entorno desde `.env.example`
3. Railway detectará automáticamente la configuración Nixpacks y desplegará

El health check en `/health` es utilizado por Railway para verificar el estado del servicio.

---

## 📄 Licencia

Proyecto académico — Tesis de Licenciatura. Todos los derechos reservados.
