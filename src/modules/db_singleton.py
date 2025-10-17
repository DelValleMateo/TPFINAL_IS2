# src/modules/db_singleton.py

import boto3
import botocore
import sys

# *----------------------------------------------------------------------------
# * UADER-FCyT
# * Ingeniería de Software II
# *
# * db_singleton.py
# * Módulo que implementa el patrón Singleton para el acceso a la
# * base de datos DynamoDB.
# *----------------------------------------------------------------------------


class DatabaseSingleton:
    """
    Implementa el patrón Singleton para gestionar una única conexión
    y acceso a las tablas de DynamoDB en toda la aplicación.
    """

    _instance = None

    def __new__(cls):
        """
        Sobrescribe el método __new__ para controlar la creación de instancias.
        """
        if cls._instance is None:
            print("Creando nueva instancia de DatabaseSingleton...")
            cls._instance = super(DatabaseSingleton, cls).__new__(cls)
            cls._instance._initialized = False
        else:
            print("Usando instancia existente de DatabaseSingleton...")

        return cls._instance

    def __init__(self):
        """
        Inicializador. Se ejecuta solo una vez gracias a la bandera _initialized.
        """
        if self._initialized:
            return

        print("Inicializando conexión a DynamoDB...")
        try:
            self.dynamodb = boto3.resource('dynamodb')

            # Cargar las tablas
            self.table_corporate_data = self.dynamodb.Table('CorporateData')
            self.table_corporate_log = self.dynamodb.Table('CorporateLog')

            # Forzar una conexión para verificar credenciales
            self.table_corporate_data.load()
            self.table_corporate_log.load()

            print("Tablas 'CorporateData' y 'CorporateLog' cargadas exitosamente.")

            self._initialized = True

        except botocore.exceptions.ClientError as e:
            print(
                f"Error de Boto3 al conectar o cargar tablas: {e.response['Error']['Message']}", file=sys.stderr)
            print("Verifique sus credenciales de AWS (aws configure) y la existencia de las tablas.", file=sys.stderr)
            sys.exit(1)
        except Exception as e:
            print(
                f"Error inesperado en DatabaseSingleton: {e}", file=sys.stderr)
            sys.exit(1)

    def get_corporate_data_table(self):
        """
        Devuelve el objeto de la tabla 'CorporateData'.
        """
        return self.table_corporate_data

    def get_corporate_log_table(self):
        """
        Devuelve el objeto de la tabla 'CorporateLog'.
        """
        return self.table_corporate_log
