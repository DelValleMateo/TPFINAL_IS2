# src/modules/data_proxy.py
import sys
import uuid
import json
from datetime import datetime
from decimal import Decimal
from botocore.exceptions import ClientError
# uuid: Para generar IDs únicos para cada entrada de log.
# datetime: Para poner la fecha y hora (timestamp) en el log.
# Decimal: Para convertir tipos de datos para DynamoDB.
from modules.db_singleton import DatabaseSingleton
# Importa el Singleton para obtener la conexión a la BD.


class DataProxy:
    def __init__(self):
        # El constructor del Proxy.
        try:
            db = DatabaseSingleton()
            # Línea 15: ¡IMPORTANTE! No crea una conexión nueva.
            # Pide la instancia ÚNICA que el Singleton administra.

            # Línea 17-18: Guarda las referencias a las tablas que obtuvo del Singleton.
            self.table_data = db.get_corporate_data_table()
            self.table_log = db.get_corporate_log_table()
            print("DataProxy inicializado.")
        except Exception as e:
            print(
                f"Error fatal al inicializar DataProxy: {e}", file=sys.stderr)
            sys.exit(1)

    def _log_action(self, client_uuid, session_id, action, details=""):
        # Línea 26: Esta es la FUNCIÓN CLAVE del Proxy.
        # Es una función "privada" (por el '_') que hace el trabajo de auditoría.
        try:
            # Línea 29-37: Crea el diccionario 'item' que se guardará en CorporateLog.
            # Esto cumple con la consigna[cite: 378, 380, 382].
            item = {
                'id': str(uuid.uuid4()),  # ID único para ESTA entrada de log
                'CPUid': str(client_uuid),
                'sessionid': str(session_id),
                'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'action': action,  # ej: "get", "set", "list"
                'details': details  # ej: "ID: UADER-FCYT-IS2"
            }
            # Línea 39: Escribe el 'item' de log en la tabla CorporateLog.
            # Esta es la escritura de auditoría.
            self.table_log.put_item(Item=item)
            print(f"AUDITORÍA: Acción '{action}' registrada.")
        except Exception as e:
            # Línea 42: Si falla la escritura del log (ej. permisos de AWS),
            # solo imprime el error pero NO detiene el programa.
            print(f"Error al registrar log: {e}", file=sys.stderr)

    # --- Métodos Públicos (La interfaz del Proxy) ---

    def get_item(self, item_id, client_uuid, session_id):
        # --- LÓGICA DEL PROXY ---
        # 1. Registrar la auditoría (la funcionalidad extra).
        self._log_action(client_uuid, session_id, "get", f"ID: {item_id}")

        # 2. Ejecutar la acción real.
        try:
            response = self.table_data.get_item(Key={'id': item_id})
            # 3. Devolver el resultado.
            # Comprueba si el 'Item' existe en la respuesta de AWS.
            return (response['Item'], 200) if 'Item' in response else ({"error": "Missing ID"}, 404)
        except ClientError as e:
            # Si AWS da un error (ej. tabla no existe, error de throttling).
            return {"error": e.response['Error']['Message']}, 500

    def set_item(self, item_data, client_uuid, session_id):
        # --- LÓGICA DEL PROXY ---
        # 1. Registrar la auditoría.
        self._log_action(client_uuid, session_id, "set",
                         f"ID: {item_data.get('id')}")

        # 2. Ejecutar la acción real.
        try:
            # DynamoDB no acepta 'float' de Python, solo 'Decimal'.
            # Esta línea es un truco para convertir todos los números en el JSON
            # de 'float' (ej. 12.3) a 'Decimal' (ej. Decimal('12.3')).
            item_data_decimal = json.loads(
                json.dumps(item_data), parse_float=Decimal)

            # Escribe el ítem en la tabla de datos.
            self.table_data.put_item(Item=item_data_decimal)
            # Devuelve los datos guardados y un 'OK' (200).
            return item_data, 200
        except Exception as e:
            # Si la conversión de JSON/Decimal falla o el 'put_item' falla.
            return {"error": str(e)}, 400

    def list_items(self, client_uuid, session_id):
        # --- LÓGICA DEL PROXY ---
        # 1. Registrar la auditoría.
        self._log_action(client_uuid, session_id, "list")

        # 2. Ejecutar la acción real.
        try:
            # .scan() es la operación de DynamoDB para "leer toda la tabla".
            response = self.table_data.scan()
            return (response['Items'], 200) if 'Items' in response else ([], 200)
        except ClientError as e:
            return {"error": e.response['Error']['Message']}, 500
