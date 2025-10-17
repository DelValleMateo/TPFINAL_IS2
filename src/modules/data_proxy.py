# src/modules/data_proxy.py

import sys
import uuid
from datetime import datetime
from decimal import Decimal
import botocore
from botocore.exceptions import ClientError
import json

# Importamos nuestro módulo Singleton
from modules.db_singleton import DatabaseSingleton

# *----------------------------------------------------------------------------
# * UADER-FCyT
# * Ingeniería de Software II
# *
# * data_proxy.py
# * Módulo que implementa el patrón Proxy para el acceso a datos.
# * Abstrae el acceso a DynamoDB y gestiona la auditoría.
# *----------------------------------------------------------------------------


class DataProxy:
    """
    Implementa el patrón Proxy. Actúa como intermediario para
    el acceso a los datos corporativos, gestionando también
    la auditoría de las operaciones.
    """

    def __init__(self):
        """
        Inicializa el Proxy obteniendo la instancia única del Singleton
        y las tablas de base de datos.
        """
        try:
            self.db_instance = DatabaseSingleton()
            self.table_data = self.db_instance.get_corporate_data_table()
            self.table_log = self.db_instance.get_corporate_log_table()
            print("DataProxy inicializado y listo.")
        except Exception as e:
            print(
                f"Error fatal al inicializar DataProxy: {e}", file=sys.stderr)
            sys.exit(1)

    def _log_action(self, client_uuid, session_id, action, details=""):
        """
        Método privado para registrar una acción en la tabla CorporateLog.
        """
        try:
            now = datetime.now()
            ts = now.strftime("%Y-%m-%d %H:%M:%S")
            log_id = str(uuid.uuid4())  # ID único para la entrada de log

            item_to_log = {
                'id': log_id,
                'CPUid': str(client_uuid),
                'sessionid': str(session_id),
                'timestamp': ts,
                'action': action,
                'details': details
            }

            self.table_log.put_item(Item=item_to_log)
            print(
                f"AUDITORÍA: Acción '{action}' registrada para CPUid {client_uuid}.")

        except ClientError as e:
            print(
                f"Error de Boto3 al registrar log: {e.response['Error']['Message']}", file=sys.stderr)
        except Exception as e:
            print(f"Error inesperado al registrar log: {e}", file=sys.stderr)

    def get_item(self, item_id, client_uuid, session_id):
        """
        Obtiene un ítem específico de la tabla CorporateData.
        """
        self._log_action(client_uuid, session_id, "get",
                         f"ID solicitado: {item_id}")

        try:
            response = self.table_data.get_item(
                Key={'id': item_id}
            )
            if 'Item' in response:
                return response['Item'], 200
            else:
                return {"error": "Missing ID", "message": f"No se encontró el ítem con id '{item_id}'"}, 404

        except ClientError as e:
            return {"error": "DB Error", "message": e.response['Error']['Message']}, 500

    def set_item(self, item_data, client_uuid, session_id):
        """
        Crea o actualiza un ítem en la tabla CorporateData.
        Convierte automáticamente floats de JSON a Decimal para DynamoDB.
        """
        try:
            # Convertir floats a Decimal recursivamente
            item_data_decimal = json.loads(
                json.dumps(item_data), parse_float=Decimal)

            # Log ANTES de la operación
            self._log_action(client_uuid, session_id, "set",
                             f"Datos a modificar: {item_data}")

            response = self.table_data.put_item(
                Item=item_data_decimal
            )

            status_code = response['ResponseMetadata']['HTTPStatusCode']
            if status_code == 200:
                return item_data, 200  # Devuelve el ítem insertado
            else:
                return {"error": "Set Failed", "message": "La operación put_item no retornó 200"}, status_code

        except ClientError as e:
            return {"error": "DB Error", "message": e.response['Error']['Message']}, 500
        except TypeError as e:
            return {"error": "Data Error", "message": f"Error de tipo de dato. ¿Campos vacíos? Detalle: {e}"}, 400
        except Exception as e:
            return {"error": "Data Error", "message": str(e)}, 400

    def list_items(self, client_uuid, session_id):
        """
        Obtiene TODOS los ítems de la tabla CorporateData.
        """
        self._log_action(client_uuid, session_id, "list",
                         "Solicitud de listado completo")

        try:
            response = self.table_data.scan()

            if 'Items' in response:
                return response['Items'], 200
            else:
                return {"error": "Scan Failed", "message": "La operación scan no devolvió ítems"}, 500

        except ClientError as e:
            return {"error": "DB Error", "message": e.response['Error']['Message']}, 500
