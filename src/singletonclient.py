# src/singletonclient.py

import socket
import sys
import argparse
import json
import uuid
import os

# *----------------------------------------------------------------------------
# * UADER-FCyT
# * Ingeniería de Software II
# *
# * singletonclient.py
# * Cliente para el servidor. Envía solicitudes 'get', 'set' o 'list'.
# *----------------------------------------------------------------------------

VERSION = "1.0"


def get_cpu_id():
    """ Obtiene el UUID de la máquina (CPUid). """
    return str(uuid.getnode())


def main():
    parser = argparse.ArgumentParser(
        description=f"SingletonClient (versión {VERSION})")
    parser.add_argument('-i', '--input', required=True,
                        help='Archivo JSON de entrada con la solicitud.')
    parser.add_argument(
        '-o', '--output', help='(Opcional) Archivo JSON de salida para la respuesta.')
    parser.add_argument('-s', '--server', default='localhost',
                        help='Host del servidor (default: localhost)')
    parser.add_argument('-p', '--port', type=int, default=8080,
                        help='Puerto TCP del servidor (default: 8080)')
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='Activar modo verboso')

    args = parser.parse_args()

    if args.verbose:
        print(f"Modo verboso activado. Conectando a {args.server}:{args.port}")

    # --- 2. Leer y preparar el JSON de solicitud ---
    try:
        with open(args.input, 'r') as f:
            request_data = json.load(f)
    except FileNotFoundError:
        print(
            f"Error: No se encontró el archivo de entrada '{args.input}'", file=sys.stderr)
        sys.exit(1)
    except json.JSONDecodeError:
        print(
            f"Error: El archivo '{args.input}' no contiene un JSON válido.", file=sys.stderr)
        sys.exit(1)

    # --- 3. Asegurar el UUID del cliente ---
    if "UUID" not in request_data:
        client_uuid = get_cpu_id()
        request_data["UUID"] = client_uuid
        if args.verbose:
            print(f"Agregando UUID de esta CPU: {client_uuid}")

    request_json = json.dumps(request_data)

    # --- 4. Conectar al servidor y enviar datos ---
    response_data = ""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            if args.verbose:
                print(f"Intentando conectar...")

            sock.connect((args.server, args.port))

            if args.verbose:
                print(f"¡Conectado! Enviando: {request_json}")

            sock.sendall(request_json.encode('utf-8'))

            if args.verbose:
                print("Esperando respuesta...")

            buffer = b""
            while True:
                data_chunk = sock.recv(1024)
                if not data_chunk:
                    break
                buffer += data_chunk

            response_data = buffer.decode('utf-8')

    except socket.error as e:
        print(
            f"Error de Socket: No se pudo conectar a {args.server}:{args.port}. ¿Está el servidor corriendo?", file=sys.stderr)
        sys.exit(1)

    # --- 6. Manejar la salida ---
    if args.output:
        try:
            # Intentamos formatear el JSON
            parsed_json = json.loads(response_data)
            with open(args.output, 'w') as f:
                json.dump(parsed_json, f, indent=4)
            print(f"Respuesta guardada en {args.output}")
        except (json.JSONDecodeError, IOError):
            # Si no es JSON o hay error, se guarda tal cual
            with open(args.output, 'w') as f:
                f.write(response_data)
            print(f"Respuesta (raw) guardada en {args.output}")
    else:
        # Imprimir en salida estándar
        print("\n--- Respuesta del Servidor ---")
        try:
            parsed_json = json.loads(response_data)
            print(json.dumps(parsed_json, indent=4))
        except json.JSONDecodeError:
            print(response_data)
        print("------------------------------")


if __name__ == "__main__":
    main()
