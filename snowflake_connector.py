import snowflake.connector
import streamlit as st
from connectors.base_connector import AbstractConnector


class SnowflakeConnector(AbstractConnector):
    """Connecteur pour Snowflake."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        required_params = ["user", "password", "account"]
        self._validate_params(required_params)
    
    def _is_valid_connection(self):
        """Vérifie si la connexion à la base de données est valide."""
        return not self._engine.is_closed()

    def _create_connection(self):
        """Établit une connexion à la base de données."""
        return snowflake.connector.connect(
            account=self._params["account"],
            user=self._params["user"],
            password=self._params["password"],
            #warehouse=self._params.get("warehouse", None),
            #role=self._params.get("role", None)
        )
        
    def _close_connection(self):
        """Ferme la connexion à la base de données."""
        self._engine.close()

    @staticmethod
    @st.cache_data(ttl=240)
    def _execute_cached_query(_engine, query: str):
        """Exécute une requête SQL et retourne le résultat."""
        with _engine.cursor() as cur:
            cur.execute(query)
            return cur.fetchall()

    def get_databases(self) -> list:
        query = "SHOW DATABASES"
        result = self._execute_query(query)
        return [row[1] for row in result] if result is not None else []

    def get_schemas(self, database: str) -> list:
        query = f"SHOW SCHEMAS IN DATABASE {database}"
        result = self._execute_query(query)
        return [row[1] for row in result] if result is not None else []

    def get_tables(self, database: str, schema: str) -> list:
        query = f"SHOW TABLES IN SCHEMA {database}.{schema}"
        result = self._execute_query(query)
        return [row[1] for row in result] if result is not None else []

    def get_columns(self, database: str, schema: str, table: str) -> list:
        query = f"SHOW COLUMNS IN TABLE {database}.{schema}.{table}"
        result = self._execute_query(query)
        return [row[2] for row in result] if result is not None else []
    
    def change_user_role(self, role: str) -> None:
        """Change le rôle de l'utilisateur."""
        query = f'USE ROLE "{role}"'
        self._execute_query(query)

    def change_warehouse(self, warehouse: str) -> None:
        """Change l'entrepôt de l'utilisateur."""
        query = f"USE WAREHOUSE {warehouse}"
        self._execute_query(query)
            
    def configure_session(self):
        """Configure la session utilisateur."""
        role = self._params.get("role", None)
        warehouse = self._params.get("warehouse", None)
        if role:
            self.change_user_role(role)
        if warehouse:
            self.change_warehouse(warehouse)