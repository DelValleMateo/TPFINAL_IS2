# src/observerclient.py

import socket
import sys
import argparse
import json
import uuid
import time
import os

# *----------------------------------------------------------------------------
# * UADER-FCyT
# * Ingeniería de Software II
# *
# * observerclient.py
# * Cliente Observador. Se suscribe al servidor y escucha por
# * notificaciones de actualización.
# *----------------------------------------------------------------------------

VERSION = "1.0"


def get_cpu_id():
    """ Obtiene el UUID de la máquina (CPUid). """
    return str(uuid.getnode())


def connect_and_listen(host, port, client_uuid, output_file, verbose):
    """
    Función principal que maneja la conexión, suscripción y
    lógica de reconexión.
    """

    # Crear el JSON de suscripción
    subscribe_request = {
        "ACTION": "subscribe",
        "UUID": client_uuid
    }
    request_json = json.dumps(subscribe_request)

    while True:  # Bucle principal de reconexión
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:

                # --- ESTADO: Conectando ---
                if verbose:
                    print(f"Intentando conectar a {host}:{port}...")
                sock.connect((host, port))

                # --- ESTADO: Suscribiendo ---
                if verbose:
                    print("¡Conectado! Enviando solicitud de suscripción...")
                sock.sendall(request_json.encode('utf-8'))

                # Esperar la confirmación de suscripción
                response_raw = sock.recv(1024)
                if not response_raw:
                    raise ConnectionError(
                        "El servidor cerró la conexión prematuramente.")

                response = json.loads(response_raw.decode('utf-8'))
                if verbose:
                    print(f"Respuesta del servidor: {response}")

                if response.get("status") == "OK":
                    print(
                        f"Suscripción exitosa (UUID: {client_uuid}). Escuchando por notificaciones...")
                else:
                    print(
                        f"Error en la suscripción: {response.get('message')}. Reintentando...")
                    time.sleep(10)  # Espera antes de reintentar
                    continue

                # --- ESTADO: Suscripto/Activo ---
                # Bucle de escucha de notificaciones
                while True:
                    notification_raw = sock.recv(4096)
                    if not notification_raw:
                        # Conexión cerrada por el servidor
                        raise ConnectionError("El servidor cerró la conexión.")

                    # --- Evento recibido ---
                    notification_str = notification_raw.decode('utf-8')
                    print("\n--- NOTIFICACIÓN RECIBIDA ---")

                    try:
                        parsed_json = json.loads(notification_str)
                        print(json.dumps(parsed_json, indent=4))

                        # Guardar en archivo si se especificó
                        if output_file:
                            try:
                                # 'a' (append) para no sobrescribir
                                with open(output_file, 'a') as f:
                                    f.write(json.dumps(
                                        parsed_json, indent=4) + "\n---\n")
                                print(
                                    f"Notificación guardada en {output_file}")
                            except IOError as e:
                                print(
                                    f"Error al escribir en {output_file}: {e}", file=sys.stderr)

                    except json.JSONDecodeError:
                        print(notification_str)  # Imprimir raw si no es JSON

                    print("-----------------------------")
                    print("...escuchando por más notificaciones...")

        except (socket.error, ConnectionError, ConnectionResetError) as e:
            # --- ESTADO: Reintentando ---
            print(f"\nError de conexión: {e}", file=sys.stderr)
            print(f"Diagrama de Estado: (Socket cerrado/ error E/S) -> 'Reintentando'")
            retry_delay = 30  # 30 segundos según consigna
            print(
                f"Se perdió la conexión con el servidor. Reintentando en {retry_delay} segundos...")
            time.sleep(retry_delay)
        except KeyboardInterrupt:
            print("\nCerrando cliente observador...")
            break
        except Exception as e:
            print(f"Error inesperado: {e}", file=sys.stderr)
            time.sleep(10)  # Espera antes de reintentar


def main():
    parser = argparse.ArgumentParser(
        description=f"ObserverClient (versión {VERSION})")
    parser.add_argument('-s', '--server', default='localhost',
                        help='Host del servidor (default: localhost)')
    parser.add_argument('-p', '--port', type=int, default=8080,
                        help='Puerto TCP del servidor (default: 8080)')
    parser.add_argument(
        '-o', '--output', help='(Opcional) Archivo para guardar notificaciones.')
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='Activar modo verboso')

    args = parser.parse_args()

    client_uuid = get_cpu_id()

    if args.verbose:
        print(f"Iniciando ObserverClient para UUID: {client_uuid}")

    connect_and_listen(args.server, args.port, client_uuid,
                       args.output, args.verbose)


if __name__ == "__main__":
    main()
