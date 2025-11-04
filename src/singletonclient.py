# src/singletonclient.py
import socket
import sys
import argparse
import json
import uuid
# Importaciones estándar para manejo de red, consola, JSON y UUID.


def get_cpu_id():
    # Obtiene la dirección MAC de la máquina (un número único).
    # str(uuid.getnode()) lo convierte en el "UUID" de CPU que pide la consigna[cite: 223].
    return str(uuid.getnode())


def main():
    # --- 1. Leer Argumentos de Consola ---
    # Define todos los argumentos que la consigna pide.
    parser = argparse.ArgumentParser(description="Cliente 'get/set/list' TPFI")
    parser.add_argument('-i', '--input', required=True,
                        help='Archivo JSON de entrada.')
    parser.add_argument(
        '-o', '--output', help='(Opcional) Archivo JSON de salida.')
    parser.add_argument('-s', '--server', default='localhost',
                        help='Host del servidor')
    parser.add_argument('-p', '--port', type=int,
                        default=8080, help='Puerto del servidor')
    parser.add_argument('-v', '--verbose',
                        action='store_true', help='Modo verboso')
    args = parser.parse_args()

    # --- 2. Leer el Archivo JSON de Entrada ---
    try:
        # Abre el archivo JSON especificado en '-i' (ej. 'data/test_get.json').
        with open(args.input, 'r') as f:
            # Lo convierte de texto a diccionario Python.
            request_data = json.load(f)
    except Exception as e:
        # Maneja si el archivo no existe o está corrupto.
        print(
            f"Error al leer el archivo de entrada '{args.input}': {e}", file=sys.stderr)
        sys.exit(1)

    # --- 3. Preparar la Petición ---
    if "UUID" not in request_data:
        # Si el JSON no especifica un UUID, añade el de esta máquina[cite: 125].
        request_data["UUID"] = get_cpu_id()

    # Convierte el diccionario (con el UUID añadido) de vuelta a un string de texto JSON.
    request_json = json.dumps(request_data)

    if args.verbose:
        # Si se usa '-v', muestra qué va a enviar.
        print(
            f"Conectando a {args.server}:{args.port} -> Enviando: {request_json}")

    # --- 4. Conectar, Enviar y Recibir ---
    try:
        # 'with' asegura que el socket se cierre al final, incluso si hay un error.
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:

            # 1. CONECTAR
            # Intenta conectarse al servidor (ej. localhost:8080).
            # Esta es la línea que falla en el Test 04 (Servidor Caído)[cite: 243].
            sock.connect((args.server, args.port))

            # 2. ENVIAR
            # Envía el string JSON (convertido a bytes con .encode('utf-8')).
            sock.sendall(request_json.encode('utf-8'))

            # 3. RECIBIR
            # Prepara un 'buffer' para recibir la respuesta.
            buffer = b""
            # Bucle de recepción: La respuesta puede ser grande (ej. en 'list')
            # y llegar en varios "paquetes" (chunks).
            while True:
                # .recv(1024) es "bloqueante": espera a recibir datos.
                data_chunk = sock.recv(1024)
                if not data_chunk:
                    # Si no recibe más datos (0 bytes), es la señal del servidor
                    # de que terminó de enviar y cerró la conexión. Rompe el bucle.
                    break
                buffer += data_chunk  # Acumula los paquetes.

            # Convierte todos los bytes recibidos a un string de texto.
            response_data = buffer.decode('utf-8')

    except socket.error as e:
        # ¡Manejo de Servidor Caído! (Test 04)
        # Si sock.connect() falla, captura el error aquí.
        print(
            f"Error: No se pudo conectar a {args.server}:{args.port}. ¿Servidor caído?", file=sys.stderr)
        sys.exit(1)  # Termina el cliente con un código de error.

    # --- 5. Mostrar la Respuesta ---
    if args.output:
        # Si se usó '-o=output.json', guarda la respuesta en ese archivo[cite: 123].
        try:
            with open(args.output, 'w') as f:
                f.write(response_data)  # Guarda el texto raw.
            print(f"Respuesta guardada en {args.output}")
        except IOError as e:
            print(
                f"Error al escribir en el archivo de salida: {e}", file=sys.stderr)
    else:
        # Si no se usó '-o', imprime en la consola (salida estándar)[cite: 123].
        print("\n--- Respuesta del Servidor ---")
        try:
            # Intenta formatear el JSON para que se vea "bonito".
            print(json.dumps(json.loads(response_data), indent=4))
        except json.JSONDecodeError:
            # Si el servidor devolvió un error que no es JSON, imprime el texto tal cual.
            print(response_data)
        print("------------------------------")


if __name__ == "__main__":
    main()  # Llama a la función principal.
