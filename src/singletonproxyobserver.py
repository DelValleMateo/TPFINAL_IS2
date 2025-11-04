# --- Importaciones ---
import socket
import sys
import argparse
import json
import uuid
import threading
# socket: Para crear el servidor y manejar conexiones de red.
# sys: Para interactuar con el sistema (como sys.exit para errores fatales).
# argparse: Para leer argumentos de la consola (como el puerto -p).
# json: Para codificar (convertir a texto) y decodificar (convertir a diccionario) mensajes.
# uuid: Para generar IDs de sesión únicos[cite: 221].
# threading: ¡La clave! Permite que el servidor maneje MÚLTIPLES clientes al mismo tiempo.

from decimal import Decimal
# Decimal: La base de datos DynamoDB no usa 'float' (números con coma), usa 'Decimal'.
# Esto es necesario para la conversión de datos.

# --- Importación de tus Patrones de Diseño ---
from modules.db_singleton import DatabaseSingleton
# Importa tu patrón Singleton.
from modules.data_proxy import DataProxy
# Importa tu patrón Proxy.
from modules.observer import Subject
# Importa tu patrón Observer.

VERSION = "1.0-conciso"


class DecimalEncoder(json.JSONEncoder):
    # Esta es una clase "ayudante".
    # DynamoDB devuelve números como objetos 'Decimal', pero la librería 'json' no sabe
    # cómo convertir 'Decimal' a texto.
    # Esta clase le dice a 'json.dumps': "Cuando veas un 'Decimal', conviértelo a 'str'".
    def default(self, obj):
        return str(obj) if isinstance(obj, Decimal) else super(DecimalEncoder, self).default(obj)


class Server:
    # Esta es la clase principal del Servidor.

    def __init__(self, host, port):
        # El constructor, se ejecuta 1 vez al crear el Servidor.
        self.host, self.port = host, port
        print("Inicializando componentes del servidor...")

        # --- ¡Patrones en Acción! ---
        # 1. Crea la instancia del PROXY.
        self.data_proxy = DataProxy()
        #    Internamente, cuando DataProxy() se inicializa,
        #    llama a DatabaseSingleton(),
        #    que crea la ÚNICA conexión a la BD.

        # 2. Crea la instancia del "Sujeto" (Observer).
        self.subject = Subject()
        #    Este objeto mantendrá la lista de todos los 'observerclient' suscritos.

        print("--- Servidor listo para escuchar ---")

    def _send_response(self, conn, data, status_code=200):
        # Función interna de ayuda para enviar respuestas JSON al cliente.
        print(f"Enviando respuesta (Status: {status_code})")
        # Usa 'DecimalEncoder' para convertir cualquier 'Decimal' que venga de la BD.
        conn.sendall(json.dumps(data, cls=DecimalEncoder,
                     indent=4).encode('utf-8'))

    def handle_client_connection(self, conn, addr):
        # ¡¡Esta es la función MÁS IMPORTANTE del servidor!!
        # Se ejecuta en un HILO NUEVO por CADA cliente que se conecta.
        print(
            f"Manejando conexión de {addr} en hilo {threading.current_thread().name}")
        # Un "flag" para saber si este cliente es un Observer.
        is_subscriber = False
        data = {}

        try:
            # --- 1. Recibir y Decodificar la Petición ---
            request_raw = conn.recv(4096)  # Recibe hasta 4096 bytes de datos.
            if not request_raw:
                # Si no recibe datos (cliente se conectó y desconectó), cierra el hilo.
                return print(f"Cliente {addr} desconectado sin datos.")

            print(f"Datos recibidos de {addr}: {request_raw.decode('utf-8')}")
            # Convierte el texto JSON a un diccionario Python.
            data = json.loads(request_raw.decode('utf-8'))

            # --- 2. Entender la Petición ---
            # ¿Qué quiere hacer? 'get', 'set', etc.
            action = data.get("ACTION")
            client_uuid = data.get("UUID", "UUID_DESCONOCIDO")  # ¿Quién es?
            # Genera un ID de sesión para el log[cite: 221].
            session_id = str(uuid.uuid4())

            # --- 3. Ejecutar la Acción (Delegando a los Patrones) ---

            if action == "get":
                # (Esta es la corrección que hicimos)
                item_id = data.get("id")  # Busca la clave 'id' (en minúscula).
                if item_id:
                    # Delega el trabajo al PROXY.
                    # El Proxy se encargará de registrar el log Y de obtener el dato.
                    resp_data, status = self.data_proxy.get_item(
                        item_id, client_uuid, session_id)
                else:
                    # Cumple con el test de "datos mínimos"[cite: 242].
                    resp_data, status = {"error": "Missing ID"}, 400

            elif action == "set":
                if "id" in data:
                    # 1. Delega el trabajo al PROXY (que registra el log y escribe en BD).
                    resp_data, status = self.data_proxy.set_item(
                        data, client_uuid, session_id)

                    # 2. ¡PATRÓN OBSERVER!
                    if status == 200:
                        # Si la escritura fue exitosa...
                        # ...le dice al "Sujeto" que NOTIFIQUE a todos los suscriptores.
                        self.subject.notify(resp_data, DecimalEncoder)
                else:
                    resp_data, status = {"error": "Missing ID"}, 400

            elif action == "list":
                # Delega el trabajo al PROXY (que registra el log y escanea la BD).
                resp_data, status = self.data_proxy.list_items(
                    client_uuid, session_id)

            elif action == "subscribe":
                # 1. Llama al Proxy SÓLO para que registre el log de auditoría.
                self.data_proxy._log_action(
                    client_uuid, session_id, "subscribe")

                # 2. ¡PATRÓN OBSERVER!
                # Le dice al "Sujeto" que AÑADA este socket de cliente a su lista de suscriptores.
                self.subject.subscribe(conn, client_uuid)
                # Marca este hilo/cliente como un suscriptor.
                is_subscriber = True
                resp_data, status = {"status": "OK",
                                     "message": "Suscrito"}, 200

            else:
                resp_data, status = {"error": "Unknown Action"}, 400

            # --- 4. Enviar Respuesta ---
            # Envía la respuesta (los datos, o el "OK" de suscripción).
            self._send_response(conn, resp_data, status)

            # --- 5. Lógica de Suscriptor ---
            if is_subscriber:
                # Si el cliente era un 'observerclient'...
                print(
                    f"Cliente {addr} (UUID: {client_uuid}) suscrito. Hilo en espera.")
                # Este hilo NO DEBE CERRARSE. Se queda en este bucle 'while'
                # esperando a que el cliente se desconecte.
                # conn.recv(1024) es "bloqueante": el hilo se pausa aquí.
                # Si el cliente cierra la conexión, recv() devuelve 0 bytes (falso)
                # y el bucle 'while' se rompe.
                # Esto cumple la consigna de "mantener el puerto abierto"[cite: 152, 155].
                while conn.recv(1024):
                    pass

        # --- 6. Manejo de Errores del Hilo ---
        except json.JSONDecodeError:
            # Si el cliente envió un JSON corrupto.
            self._send_response(conn, {"error": "Invalid JSON"}, 400)
        except (socket.error, ConnectionResetError) as e:
            # Si el cliente se desconectó de golpe (ej. cerró la terminal).
            print(f"Error de Socket con {addr}: {e}")
        except Exception as e:
            # Cualquier otro error inesperado. Se registra pero no tumba el servidor.
            print(f"Error inesperado con {addr}: {e}", file=sys.stderr)
        finally:
            # --- 7. Limpieza del Hilo ---
            # Este bloque se ejecuta SIEMPRE (al final del 'try' o si ocurre un 'except').
            if is_subscriber:
                # Si el cliente que se desconectó era un suscriptor...
                # ...le dice al "Sujeto" que lo quite de la lista de notificaciones.
                self.subject.unsubscribe(conn)
            print(f"Cerrando conexión y finalizando hilo para {addr}.")
            # Cierra el socket de ESTE cliente. El hilo muere aquí.
            conn.close()

    def start(self):
        # Esta es la función que ARRANCA el servidor.
        try:
            # --- 1. Configuración del Socket Principal ---
            self.server_socket = socket.socket(
                socket.AF_INET, socket.SOCK_STREAM)
            # AF_INET = familia de direcciones (Internet IPv4).
            # SOCK_STREAM = tipo de socket (TCP, confiable).

            # self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            # (Esta línea está comentada en tu código, pero es útil.
            # Permite reiniciar el servidor rápidamente sin que el OS diga "puerto ya en uso").

            self.server_socket.bind((self.host, self.port))
            # "Abre" el puerto 8080 (o el que sea) en la máquina ('0.0.0.0' = todas las IPs).
            # Esto es lo que falla en el Test 05 (doble servidor)[cite: 244].

            self.server_socket.listen(5)  # Se pone en modo "escucha".
            print(f"Servidor {VERSION} escuchando en {self.host}:{self.port}")

            # --- 2. Bucle Infinito de Aceptación de Clientes ---
            while True:
                # server_socket.accept() es una llamada "BLOQUEANTE".
                # El programa se congela en esta línea, esperando que un cliente se conecte.
                conn, addr = self.server_socket.accept()

                # ¡¡MAGIA MULTIHILO!!
                # Apenas se conecta un cliente, .accept() se desbloquea.
                # El código crea un HILO NUEVO para ejecutar 'handle_client_connection'.
                # Y el bucle 'while True' principal vuelve INMEDIATAMENTE a .accept()
                # para esperar al SIGUIENTE cliente.
                # daemon=True significa que si el programa principal (start) muere, todos los hilos mueren.
                threading.Thread(target=self.handle_client_connection, args=(
                    conn, addr), daemon=True).start()

        except socket.error as e:
            # Captura si el puerto ya está en uso.
            print(f"Error de Socket: {e}", file=sys.stderr)
            sys.exit(1)
        except KeyboardInterrupt:
            # Captura si el usuario presiona Ctrl+C.
            print("\nCerrando el servidor...")
        finally:
            # Limpieza final del servidor.
            if hasattr(self, 'server_socket') and self.server_socket:
                # Cierra el socket principal de escucha.
                self.server_socket.close()
            print("Servidor detenido.")


if __name__ == "__main__":
    # Este bloque solo se ejecuta si corres este archivo directamente.

    # --- 3. Lectura de Argumentos de Consola ---
    parser = argparse.ArgumentParser(description="Servidor TPFI")
    # Define el argumento '-p' o '--port', como pide la consigna[cite: 161].
    parser.add_argument('-p', '--port', type=int,
                        default=8080, help='Puerto (default: 8080)')
    args = parser.parse_args()

    # --- 4. Iniciar el Servidor ---
    # Crea la instancia y llama a start().
    Server('0.0.0.0', args.port).start()
