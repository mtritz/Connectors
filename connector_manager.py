from connectors.connector_factory import ConnectorFactory
import time

class ConnectorManager:
    def __init__(self):
        self._connectors: dict = {}
        self.__conn_params: dict = {}

    """@property
    def connectors(self):
        return self._connectors
    """
    def __getitem__(self, key):
        return self._connectors.get(key)
    
    def __setitem__(self, key, value):
        self.__conn_params[key] = value

    def get_connector(self, system: str):
        return self._connectors.get(system)

    def _create_connector(self, system: str):
        connector = self._connectors.get(system)

        if connector and connector.is_connected:
            raise ValueError(f"Le connecteur {system.title()} est déjà connecté.")
        
        try:
            self._connectors[system] = ConnectorFactory.create_connector(
                system, **self.__conn_params.get(system, {})
            )
        except ConnectionError as e:
            #self.reload_connection(system)
            raise ConnectionError(f"Erreur lors de la connexion à {system.title()}: {e}")
        except Exception as e:
            raise Exception(f"Erreur lors de la création du connecteur {system.title()}: {e}")
        
    def connect(self, system: str):
        connector = self._connectors.get(system)
        try:
            if connector:
                connector.connect()
            else:
                self._create_connector(system)
                self.connect(system)
        except Exception as e:
            raise ConnectionError(f"Erreur lors de la connexion à {system.title()}: {e}")
        
    def disconnect(self, system: str):
        connector = self._connectors.get(system)
        if connector:
            try:
                connector.disconnect()
                del self._connectors[system]
            except Exception as e:
                raise ConnectionError(f"Erreur lors de la déconnexion de {system.title()}: {e}")

    def set_connector(self, system: str, params: dict):
        system = system.lower()
        if system in ConnectorFactory.connectors.keys():
            self.__conn_params[system] = params
        else:
            raise ValueError(f"Le système {system} n'est pas reconnu.")
        
    def reload_connection(self, system: str):
        connector = self._connectors.get(system)
        if connector and connector.is_connected:
            connector.disconnect()
        time.sleep(1)
        self._create_connector(system)

    def close(self):
        for connector in self._connectors.values():
            connector.disconnect()

    def is_connected(self, system: str) -> bool:
        connector = self._connectors.get(system)
        return connector.is_connected if connector else False