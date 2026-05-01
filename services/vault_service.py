# Infraestructura — acceso a Supabase Vault para credenciales cifradas
from supabase import Client


class VaultService:
    def __init__(self, client: Client):
        self._client = client

    async def get_secret(self, secret_name: str) -> str:
        """Recupera un secreto almacenado en Supabase Vault."""
        raise NotImplementedError
