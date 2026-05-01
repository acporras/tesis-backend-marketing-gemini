from contextlib import asynccontextmanager

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from config import settings
from routers import auth, onboarding, fidelizacion, reactivacion, aprobacion, logs, metricas, dashboard, datos, audiencias, admin, api_ingesta

scheduler = AsyncIOScheduler(timezone="UTC")


async def _job_reactivacion():
    """Job nocturno: detecta clientes inactivos y genera campañas de reactivación."""
    from services.reactivacion_service import ejecutar_reactivacion_automatica
    await ejecutar_reactivacion_automatica()


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    scheduler.add_job(
        _job_reactivacion,
        trigger=CronTrigger(
            hour=settings.reactivation_cron_hour,
            minute=settings.reactivation_cron_minute,
        ),
        id="reactivacion_nocturna",
        replace_existing=True,
    )
    scheduler.start()

    yield

    # Shutdown
    scheduler.shutdown(wait=False)


app = FastAPI(
    title="ADCAMI API",
    description="Sistema LLM Gemini para marketing bancario.",
    version="0.1.0",
    docs_url="/docs" if settings.debug else None,
    redoc_url="/redoc" if settings.debug else None,
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(auth.router, prefix="/auth", tags=["auth"])
app.include_router(onboarding.router, prefix="/onboarding", tags=["onboarding"])
app.include_router(fidelizacion.router, prefix="/fidelizacion", tags=["fidelizacion"])
app.include_router(reactivacion.router, prefix="/reactivacion", tags=["reactivacion"])
app.include_router(aprobacion.router, prefix="/aprobacion", tags=["aprobacion"])
app.include_router(logs.router, prefix="/logs", tags=["logs"])
app.include_router(metricas.router, prefix="/metricas", tags=["metricas"])
app.include_router(dashboard.router, prefix="/dashboard", tags=["dashboard"])
app.include_router(datos.router, prefix="/datos", tags=["datos"])
app.include_router(audiencias.router, prefix="/audiencias", tags=["audiencias"])
app.include_router(admin.router)
app.include_router(api_ingesta.router)


@app.get("/health", tags=["health"])
async def health():
    return {"status": "ok", "version": "0.1.0"}
