# src/singletonproxyobserver.py

import socket
import sys
import argparse
import json
import uuid
from decimal import Decimal
import threading  # Para manejar múltiples clientes

# Importamos nuestros módulos
from modules.db_singleton import DatabaseSingleton
from modules.data_proxy import DataProxy
from modules.observer import Subject

# *----------------------------------------------------------------------------
# * UADER-FCyT
# * Ingeniería de Software II
# *
# * singletonproxyobserver.py
# * Servidor principal que implementa los patrones Singleton, Proxy y Observer.
# *----------------------------------------------------------------------------

VERSION = "1.0 (Final)"

# Clase auxiliar para convertir Decimal a string en JSON


class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return str(obj)
        return super(DecimalEncoder, self).default(obj)


class Server:
    """
    Clase principal del servidor.
    Gestiona las conexiones TCP y orquesta los componentes.
    """

    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.server_socket = None

        print("Inicializando componentes del servidor...")
        self.db_singleton = DatabaseSingleton()
        self.data_proxy = DataProxy()
        self.subject = Subject()  # Inicializa el Sujeto (Observer)
        print("--- Servidor listo para escuchar ---")

    def handle_client_connection(self, conn, addr):
        """
        Maneja CADA conexión de cliente en su propio hilo.
        Implementa el diagrama de flujo principal y el de estados.
        """
        print(
            f"Manejando nueva conexión de {addr} en el hilo {threading.current_thread().name}")
        request_data = b""
        client_uuid = "UUID_DESCONOCIDO"
        is_subscriber = False  # Bandera para este hilo

        try:
            # 1. Recibir la solicitud inicial
            while True:
                chunk = conn.recv(1024)
                if not chunk:
                    break
                request_data += chunk
                # Suponemos que un JSON de solicitud no será fragmentado
                # (Para robustez, se necesitaría un protocolo de fin de mensaje)
                break

            if not request_data:
                print(f"Cliente {addr} desconectado sin enviar datos.")
                return

            print(f"Datos recibidos de {addr}: {request_data.decode('utf-8')}")

            # 2. Validar y Decodificar (Parsear JSON)
            try:
                data = json.loads(request_data.decode('utf-8'))
            except json.JSONDecodeError:
                response = {"error": "Invalid JSON",
                            "message": "La solicitud no es un JSON válido."}
                conn.sendall(json.dumps(response).encode('utf-8'))
                return

            # 3. Bifurcación basada en ACTION
            action = data.get("ACTION")
            client_uuid = data.get("UUID", client_uuid)
            session_id = str(uuid.uuid4())

            response_data = {}
            status_code = 200

            if action == "get":
                item_id = data.get("ID")
                if item_id:
                    response_data, status_code = self.data_proxy.get_item(
                        item_id, client_uuid, session_id)
                else:
                    response_data, status_code = {
                        "error": "Missing ID", "message": "La acción 'get' requiere un 'ID'."}, 400

            elif action == "set":
                if "id" in data:
                    response_data, status_code = self.data_proxy.set_item(
                        data, client_uuid, session_id)
                    # --- LÓGICA OBSERVER ---
                    if status_code == 200:
                        print("Acción 'set' exitosa. Notificando a suscriptores...")
                        self.subject.notify(response_data, DecimalEncoder)
                else:
                    response_data, status_code = {
                        "error": "Missing ID", "message": "La acción 'set' requiere un 'id' en el objeto."}, 400

            elif action == "list":
                response_data, status_code = self.data_proxy.list_items(
                    client_uuid, session_id)

            elif action == "subscribe":
                # --- LÓGICA OBSERVER ---
                self.data_proxy._log_action(
                    client_uuid, session_id, "subscribe")
                self.subject.subscribe(conn, client_uuid)
                is_subscriber = True
                response_data = {"status": "OK",
                                 "message": f"Cliente {client_uuid} suscripto."}
                status_code = 200

            else:
                response_data, status_code = {
                    "error": "Unknown Action", "message": f"Acción '{action}' no reconocida."}, 400

            # 4. Enviar respuesta al cliente
            response_json = json.dumps(
                response_data, cls=DecimalEncoder, indent=4)
            print(f"Enviando respuesta a {addr} (Status: {status_code})")
            conn.sendall(response_json.encode('utf-8'))

            # 5. Lógica de conexión
            if is_subscriber:
                # Si es suscriptor, mantenemos la conexión abierta
                print(
                    f"Cliente {addr} (UUID: {client_uuid}) ahora es un suscriptor. Hilo en espera.")
                # Entramos en un bucle para detectar desconexión
                while True:
                    data = conn.recv(1024)
                    if not data:
                        # El cliente cerró la conexión
                        print(
                            f"Suscriptor {addr} (UUID: {client_uuid}) se ha desconectado.")
                        break

        except (socket.error, ConnectionResetError) as e:
            print(f"Error de Socket con el cliente {addr}: {e}")
        except Exception as e:
            print(
                f"Error inesperado procesando la solicitud de {addr}: {e}", file=sys.stderr)
        finally:
            # 6. Cerrar/Limpiar conexión
            if is_subscriber:
                self.subject.unsubscribe(conn)
            print(f"Cerrando conexión y finalizando hilo para {addr}.")
            conn.close()

    def start(self):
        """
        Inicia el bucle principal del servidor para escuchar conexiones.
        Lanza un nuevo hilo por cada cliente.
        """
        try:
            self.server_socket = socket.socket(
                socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.setsockopt(
                socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server_socket.bind((self.host, self.port))
            self.server_socket.listen(5)
            print(
                f"Servidor versión {VERSION} escuchando en {self.host}:{self.port}")

            while True:
                conn, addr = self.server_socket.accept()

                # Lanzar un nuevo hilo para manejar este cliente
                client_thread = threading.Thread(
                    target=self.handle_client_connection,
                    args=(conn, addr)
                )
                client_thread.daemon = True  # Permite cerrar el servidor con Ctrl+C
                client_thread.start()

        except socket.error as e:
            print(f"Error de Socket: {e}", file=sys.stderr)
        except KeyboardInterrupt:
            print("\nCerrando el servidor... (Ctrl+C presionado)")
        finally:
            if self.server_socket:
                self.server_socket.close()
            print("Servidor detenido.")


# --- Punto de entrada del programa ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Servidor SingletonProxyObserver TPFI")
    parser.add_argument('-p', '--port', type=int, default=8080,
                        help='Puerto TCP (default: 8080)')
    args = parser.parse_args()

    HOST = '0.0.0.0'  # Escucha en todas las interfaces
    PORT = args.port

    server = Server(HOST, PORT)
    server.start()
