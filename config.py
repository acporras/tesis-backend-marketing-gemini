from pydantic_settings import BaseSettings
from pydantic import Field
import google.generativeai as genai
from google.generativeai.types import HarmCategory, HarmBlockThreshold


class Settings(BaseSettings):
    # Gemini
    gemini_api_key: str = Field(..., alias="GEMINI_API_KEY")
    gemini_model: str = Field("gemini-2.5-pro", alias="GEMINI_MODEL")
    gemini_temperature: float = Field(0.3, alias="GEMINI_TEMPERATURE")
    gemini_max_output_tokens: int = Field(1024, alias="GEMINI_MAX_OUTPUT_TOKENS")
    gemini_rpm_limit: int = Field(15, alias="GEMINI_RPM_LIMIT")

    # Supabase
    supabase_url: str = Field(..., alias="SUPABASE_URL")
    supabase_anon_key: str = Field(..., alias="SUPABASE_ANON_KEY")
    # SERVICE_KEY bypasa RLS — solo disponible en Settings > API > service_role
    # Se puede dejar vacío en desarrollo; el backend usa anon_key como fallback
    supabase_service_key: str = Field("", alias="SUPABASE_SERVICE_KEY")

    @property
    def supabase_backend_key(self) -> str:
        """Clave que usa el backend: service_key si existe, anon_key si no."""
        return self.supabase_service_key or self.supabase_anon_key

    # App
    debug: bool = Field(False, alias="DEBUG")
    allowed_origins: list[str] = Field(
        default=["http://localhost:3000", "http://localhost:5173"],
        alias="ALLOWED_ORIGINS",
    )

    # Scheduler — reactivación nocturna 02:00 Lima (UTC-5 = 07:00 UTC)
    reactivation_cron_hour: int = Field(7, alias="REACTIVATION_CRON_HOUR")
    reactivation_cron_minute: int = Field(0, alias="REACTIVATION_CRON_MINUTE")
    inactivity_threshold_days: int = Field(30, alias="INACTIVITY_THRESHOLD_DAYS")

    model_config = {
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "populate_by_name": True,
    }


# Configuración de safety para Gemini — bloquea contenido medio y alto
GEMINI_SAFETY_SETTINGS = {
    HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_MEDIUM_AND_ABOVE,
    HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_MEDIUM_AND_ABOVE,
    HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_MEDIUM_AND_ABOVE,
    HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_MEDIUM_AND_ABOVE,
}

settings = Settings()

# Inicializa el cliente de Gemini con la API key cargada desde .env
genai.configure(api_key=settings.gemini_api_key)


def get_gemini_model() -> genai.GenerativeModel:
    """Retorna una instancia configurada del modelo Gemini."""
    generation_config = genai.types.GenerationConfig(
        temperature=settings.gemini_temperature,
        max_output_tokens=settings.gemini_max_output_tokens,
    )
    return genai.GenerativeModel(
        model_name=settings.gemini_model,
        generation_config=generation_config,
        safety_settings=GEMINI_SAFETY_SETTINGS,
    )
