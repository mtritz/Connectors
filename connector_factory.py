import importlib
from connectors.base_connector import AbstractConnector
from connectors import __all__ as all_connectors

class ConnectorFactory:
    """Fabrique pour créer dynamiquement des connecteurs selon le système sélectionné."""
    
    _instance = None
    connectors: dict = {connector.split("Connector")[0].lower(): connector for connector in all_connectors}

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls, *args, **kwargs)
        return cls._instance

    @classmethod
    def _import_connector_class(cls, system: str):
        """
        Tente d'importer dynamiquement la classe de connecteur pour un système donné.
        
        :param system: Le nom du système pour lequel le connecteur doit être importé.
        :return: La classe de connecteur importée.
        :raises ImportError: Si le module ou la classe ne peut pas être importée.
        """
        #module_name, class_name = cls.connectors[system].rsplit(".", 1)
        module_name = "connectors"
        class_name = cls.connectors[system]
        
        try:
            # Import dynamique du module
            module = importlib.import_module(module_name)
            # Récupération de la classe de connecteur
            connector_class = getattr(module, class_name)
        except ModuleNotFoundError as e:
            raise ImportError(f"Le module '{module_name}' est introuvable pour le système '{system}': {e}")
        except AttributeError as e:
            raise ImportError(f"La classe '{class_name}' n'existe pas dans le module '{module_name}' pour le système '{system}': {e}")
        
        return connector_class

    @classmethod
    def create_connector(cls, system: str, **kwargs) -> AbstractConnector:
        """
        Instancie et retourne le connecteur correspondant au système.
        
        :param system: Le type de système (par exemple, "snowflake").
        :param kwargs: Les paramètres nécessaires pour le connecteur.
        :return: Une instance du connecteur spécifique.
        :raises ValueError: Si le système n'est pas supporté.
        :raises ImportError: Si le module ou la classe ne peut pas être importé.
        """
        if system not in cls.connectors.keys():
            raise ValueError(f"Le système '{system}' n'est pas supporté.")
        
        # Appel de la méthode d'importation du connecteur
        connector = cls._import_connector_class(system)
        
        # Retourne l'instance du connecteur
        return connector(**kwargs)
