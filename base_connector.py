from abc import ABC, abstractmethod
from utils.validators import validate_sql_query

class AbstractConnector(ABC): # It's an abstract class, so it can't be instantiated
    """Classe abstraite pour les connecteurs de base de données."""

    def __init__(self, **kwargs):
        self._params = kwargs
        self._engine = None
    
    def __enter__(self):
        """Establishes a connection to the database when entering a context.
        This method is called when the context manager is entered using the 'with' statement.
        """
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        """
        Handles the exit of the runtime context related to this object.
        Parameters:
        _exc_type (type): The exception type.
        _exc_value (Exception): The exception instance.
        _traceback (traceback): The traceback object.
        """
        self.disconnect()

    def __del__(self):
        self.disconnect()

    @property
    def is_connected(self):
        """Etat de connexion à la base de données."""
        return self._engine is not None and self._is_valid_connection()

    def connect(self):
        """Établit une connexion à la base de données."""
        if not self.is_connected:
            try:
                if self._engine is not None:
                    self.disconnect()
                self._engine = self._create_connection()
            except Exception as e:
                self._engine = None
                raise ConnectionError(f"Échec de la connexion : {e}")

    def disconnect(self):
        """Ferme la connexion à la base de données."""
        if self._engine is not None:
            try:
                self._close_connection()
            except Exception: 
                pass
            finally:
                self._engine = None

    def _validate_params(self, required_params: list):
        """Vérifie que les paramètres requis sont présents."""
        missing_params = [param for param in required_params if param not in self._params]
        if missing_params:
            raise ValueError(f"Paramètres manquants : {', '.join(missing_params)}")

    def _execute_query(self, query: str):
        """Exécute une requête SQL et retourne le résultat."""
        try:
            #validate_sql_query(query)
            self.connect()
            return self._execute_cached_query(self._engine, query)
        except Exception as e:
            raise ValueError(f"Requête invalide : {e}")
    
    @abstractmethod
    def _is_valid_connection(self):
        """Vérifie si la connexion est valide."""
        pass

    @abstractmethod
    def _create_connection(self):
        """Crée une connexion à la base de données."""
        pass

    @abstractmethod
    def _close_connection(self):
        """Ferme la connexion à la base de données."""
        pass

    @staticmethod
    @abstractmethod
    def _execute_cached_query(_engine, query: str):
        """Abstract method to execute a SQL query and return the result.
        This method must be implemented by subclasses.
        """
        pass
        
    @abstractmethod
    def get_databases(self):
        """Retourne la liste des bases de données disponibles."""
        pass
    
    @abstractmethod 
    def get_schemas(self, database: str):
        """Retourne la liste des schémas disponibles dans une base de données."""
        pass
    
    @abstractmethod
    def get_tables(self, database: str, schema: str):
        """Retourne la liste des tables disponibles dans un schéma."""
        pass
    
    @abstractmethod
    def get_columns(self, database: str, schema: str, table: str):
        """Retourne la liste des colonnes d'une table."""
        pass