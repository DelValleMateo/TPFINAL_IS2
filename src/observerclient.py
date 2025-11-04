# src/observerclient.py
import socket
import sys
import argparse
import json
import uuid
import time
# 'time' es nuevo aquí: se usa para el 'sleep' del reintento de conexión.


def get_cpu_id():
    return str(uuid.getnode())  # Igual que el otro cliente[cite: 223].


def connect_and_listen(host, port, client_uuid, verbose):
    # Prepara el único mensaje que enviará: la solicitud de suscripción.
    request_json = json.dumps(
        {"ACTION": "subscribe", "UUID": client_uuid})[cite: 141]

    # Define el tiempo de reintento, como pide la consigna[cite: 156].
    retry_delay = 30

    # --- Bucle 1: El BUCLE DE RECONEXIÓN (Exterior) ---
    # Si el servidor se cae, el código saldrá al 'except' y luego este 'while'
    # hará que todo el bloque 'try' se reintente después de 30 segundos.
    while True:
        try:
            # --- 1. Conectar y Suscribir ---
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                if verbose:
                    print(f"Intentando conectar a {host}:{port}...")
                sock.connect((host, port))  # Intenta conectar.

                if verbose:
                    print("¡Conectado! Enviando suscripción...")
                # Envía la solicitud 'subscribe'.
                sock.sendall(request_json.encode('utf-8'))

                # Recibe la PRIMERA respuesta: la confirmación ("status": "OK").
                response = json.loads(sock.recv(1024).decode('utf-8'))
                if response.get("status") != "OK":
                    # Si el servidor rechaza la suscripción, espera y reintenta.
                    print(
                        f"Error de suscripción: {response.get('message')}. Reintentando...")
                    time.sleep(retry_delay / 2)
                    # Salta al siguiente ciclo del 'while True' (reintento).
                    continue

                print(
                    f"Suscripción exitosa (UUID: {client_uuid}). Escuchando...")

                # --- Bucle 2: El BUCLE DE ESCUCHA (Interior) ---
                # Si la suscripción fue exitosa, entra en el bucle de escucha.
                while True:
                    # --- Espera por Notificaciones ---
                    # sock.recv(4096) es "BLOQUEANTE".
                    # El programa se congela aquí, esperando datos del servidor.
                    # No usa CPU mientras espera.
                    notification_raw = sock.recv(4096)

                    if not notification_raw:
                        # Si recibe 0 bytes, significa que el servidor cerró la conexión.
                        # Lanza un error para ser capturado por el 'except' de abajo.
                        raise ConnectionError("Servidor cerró la conexión.")

                    # --- Imprimir Notificación ---
                    # Si recibe datos (una notificación de 'set'), los imprime.
                    # Cumple la consigna de "mostrará por salida estándar"[cite: 154].
                    print("\n--- NOTIFICACIÓN RECIBIDA ---")
                    try:
                        parsed = json.loads(notification_raw.decode('utf-8'))
                        # Formateado "bonito".
                        print(json.dumps(parsed, indent=4))
                    except json.JSONDecodeError:
                        print(notification_raw.decode('utf-8'))  # Raw.
                    print("-----------------------------")
                    # El bucle 'while True' interior vuelve arriba, a sock.recv(4096),
                    # para esperar la *siguiente* notificación.

        # --- 2. Manejo de Desconexión (Requerimiento de la Consigna) ---
        except (socket.error, ConnectionError, ConnectionResetError) as e:
            # Si algo falla (la conexión inicial o el 'sock.recv' del bucle interior)...
            # ...se captura el error aquí.
            print(f"\nError de conexión: {e}", file=sys.stderr)
            print(f"Servidor caído. Reintentando en {retry_delay} segundos...")
            # Espera 30 segundos, como pide la consigna[cite: 156].
            time.sleep(retry_delay)
            # Al terminar el 'sleep', el 'while True' exterior (Reconexión)
            # hace que el código vuelva a intentar conectarse desde el principio.

        except KeyboardInterrupt:
            # Si el usuario presiona Ctrl+C.
            print("\nCerrando cliente observador...")
            break  # Rompe el 'while True' exterior y termina el programa.
        except Exception as e:
            # Captura cualquier otro error, espera y reintenta.
            print(f"Error inesperado: {e}. Reintentando...", file=sys.stderr)
            time.sleep(retry_delay / 2)


if __name__ == "__main__":
    # Lee los argumentos de consola (host, puerto, verboso)[cite: 139].
    parser = argparse.ArgumentParser(description="Cliente Observador TPFI")
    parser.add_argument('-s', '--server', default='localhost',
                        help='Host del servidor')
    parser.add_argument('-p', '--port', type=int,
                        default=8080, help='Puerto del servidor')
    parser.add_argument('-v', '--verbose',
                        action='store_true', help='Modo verboso')
    args = parser.parse_args()

    # Llama a la función principal que contiene los bucles infinitos.
    connect_and_listen(args.server, args.port, get_cpu_id(), args.verbose)
