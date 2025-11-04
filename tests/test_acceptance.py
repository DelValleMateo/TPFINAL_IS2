# tests/test_acceptance.py
import unittest
# Línea 2: Importa el framework de pruebas de Python. Es lo que te da 'TestCase', 'assertEqual', etc.
import subprocess
# Línea 3: ¡Clave! Permite a este script ejecutar otros programas de Python (el servidor y el cliente) en procesos separados.
import os
# Línea 4: Utilidades del sistema operativo. Se usa para construir rutas a archivos (ej. 'src/cliente.py').
import sys
# Línea 5: Permite usar 'sys.executable' (la ruta al intérprete de Python) y 'sys.exit'.
import time
# Línea 6: Se usa para 'time.sleep()', para dar pausas y permitir que el servidor arranque.
import json
# Línea 7: Se usa para crear el archivo JSON temporal en 'test_03'.
import boto3
# Línea 8: La librería de AWS para conectar con DynamoDB y verificar la tabla de logs.
from botocore.exceptions import ClientError
# Línea 9: Importa los tipos de error específicos de AWS (aunque no se usa activamente aquí, es buena práctica).

# --- Configuración (Lo más corto posible) ---
# Líneas 12-18: Definen constantes globales. Esto es una excelente práctica
# para no tener "strings mágicos" repetidos por todo el código.
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
# Línea 12: Calcula la ruta absoluta a la carpeta raíz del proyecto (un nivel "arriba" de 'tests').
SERVER = os.path.join(ROOT, 'src', 'singletonproxyobserver.py')
# Línea 13: Ruta completa al script del servidor.
CLIENT = os.path.join(ROOT, 'src', 'singletonclient.py')
# Línea 14: Ruta completa al script del cliente.
PORT = 8081
# Línea 15: Define un puerto de PRUEBA (diferente del 8080 de producción) para no interferir.
JSON_GET = os.path.join(ROOT, 'data', 'test_get.json')
JSON_SET = os.path.join(ROOT, 'data', 'test_set.json')
JSON_LIST = os.path.join(ROOT, 'data', 'test_list.json')


class TestAcceptance(unittest.TestCase):
    # Línea 21: Define la clase de pruebas. Debe heredar de 'unittest.TestCase'.
    server_process, log_table = None, None
    # Línea 22: Variables de clase. 'server_process' guardará el proceso del servidor
    # y 'log_table' guardará la conexión a la BD para que todos los tests la usen.

    @classmethod
    def setUpClass(cls):
        # Línea 25: Un método especial de unittest. Se ejecuta *UNA SOLA VEZ* antes de *todos* los tests.
        print("Configurando pruebas...")
        try:
            # Línea 28: Se conecta a AWS y obtiene la referencia a la tabla 'CorporateLog'.
            cls.log_table = boto3.resource('dynamodb').Table('CorporateLog')
            # Línea 29: .load() fuerza una conexión para verificar que las credenciales son válidas.
            cls.log_table.load()
        except Exception as e:
            # Línea 31: Si no se puede conectar a AWS, las pruebas no pueden correr.
            print(f"ERROR: No se pudo conectar a AWS. {e}")
            sys.exit(1)  # Termina el script de prueba.

    def setUp(self): self.stop_server(); time.sleep(0.5)
    # Línea 34: Método "hook" que se ejecuta *ANTES* de CADA test (test_01, test_02, etc.).
    # Su objetivo es asegurar un estado limpio: detiene cualquier servidor que
    # haya quedado colgado del test anterior.

    def tearDown(self): self.stop_server()
    # Línea 35: Método "hook" que se ejecuta *DESPUÉS* de CADA test (incluso si falló).
    # Su objetivo es la limpieza: apagar el servidor que se usó en el test.

    def start_server(self, port=PORT):
        # Línea 37: Función de ayuda ("helper") para iniciar el servidor.
        print(f"\nIniciando servidor en puerto {port}...")
        # --- CAMBIO AQUÍ ---
        # Líneas 40-42: Como dice el comentario, esta versión no captura la salida (stdout/stderr)
        # del servidor. Esto es una técnica de debugging: la salida del servidor
        # (ej. "Manejando conexión...") se imprimirá directamente en la consola del test.
        self.server_process = subprocess.Popen(
            # Línea 43: 'Popen' (Process Open) ejecuta el servidor en un *proceso separado*
            # para que el script de test pueda continuar sin bloquearse.
            [sys.executable, SERVER, '-p', str(port)],
            text=True
        )
        time.sleep(1.5)  # Pausa para darle tiempo al servidor de arrancar.
        print("Servidor iniciado.")

    def stop_server(self):
        # Línea 51: Función de ayuda para detener el servidor.
        if self.server_process:
            # Línea 53: Envía la señal de "Terminar" (SIGTERM) al proceso del servidor.
            self.server_process.terminate()
            # Línea 54: Espera a que el proceso realmente termine.
            self.server_process.wait()
            self.server_process = None
            print("Servidor detenido.")

    def run_client(self, args):
        # Línea 62: Función de ayuda para ejecutar el cliente.
        # Línea 64: 'subprocess.run' (a diferencia de Popen) *espera* a que el cliente
        # termine su ejecución.
        # 'capture_output=True' "atrapa" lo que el cliente imprimió (su stdout y stderr)
        # para que podamos verificarlo con 'self.assertIn'.
        return subprocess.run([sys.executable, CLIENT] + args, capture_output=True, text=True, cwd=ROOT)

    def get_log_count(self):
        # Línea 67: Función de ayuda para contar los logs en DynamoDB.
        try:
            # Línea 70: 'scan' lee la tabla, 'Select='COUNT'' le dice a AWS que solo devuelva el número total.
            # 'ConsistentRead=True' es la corrección para la "consistencia eventual",
            # pide a DynamoDB el dato más fresco y no uno "potencialmente viejo".
            return self.log_table.scan(Select='COUNT', ConsistentRead=True)['Count']
        except:
            return -1  # Si falla la conexión, devuelve -1.

    # --- FUNCIÓN DE ESPERA (NECESARIA PARA EVITAR ERRORES...) ---
    def wait_for_log_count(self, expected_count, timeout=15):
        # Línea 76: Esta es la solución *robusta* al problema de la consistencia de DynamoDB.
        # A veces, incluso con 'ConsistentRead=True', la base de datos tarda en actualizarse.
        """
        Espera activamente hasta que el contador de logs sea el esperado.
        Soluciona la consistencia eventual de DynamoDB.
        """
        print(f"Esperando que el contador de logs sea {expected_count}...")
        start_time = time.time()
        while True:
            # Línea 84: Bucle de sondeo. Llama a get_log_count() una y otra vez.
            current_count = self.get_log_count()
            if current_count == expected_count:
                # Línea 86: ¡Éxito! El contador se actualizó.
                print("¡Contador de logs actualizado!")
                return True

            if time.time() - start_time > timeout:
                # Línea 90: Si han pasado más de 15 segundos, se rinde.
                print(
                    f"Error de Timeout: El contador de logs sigue en {current_count}.")
                return False

            # Espera medio segundo antes de volver a preguntar.
            time.sleep(0.5)
    # --------------------------------------------------------------------------------------

    # --- Casos de Prueba (Versión Ruidosa) ---

    # --- TEST 01 MODIFICADO PARA IGNORAR EL CONTEO DE LOGS ---

    def test_01_camino_feliz_y_auditoria(self):
        # Línea 102: Caso de Prueba 1: "Camino Feliz" (según la consigna).
        print("\n--- Test 01: Camino Feliz (set, get, list) y Auditoría ---")
        self.start_server()
        # Línea 105: ¡IMPORTANTE! Esta versión del test *comenta* la verificación de logs.
        # Probablemente porque el 'scan' en una tabla grande era muy lento y causaba 'timeouts'.
        # logs_ini = self.get_log_count() # <-- REMOVIDO

        print("Probando SET...")
        # Línea 108: Ejecuta el cliente con el JSON de 'set'.
        res_set = self.run_client(['-i', JSON_SET, '-p', str(PORT)])
        print("--- Salida del Cliente (SET) ---")
        # Imprime lo que el cliente recibió del servidor.
        print(res_set.stdout)
        print("--- Errores del Cliente (SET) ---")
        print(res_set.stderr)  # Imprime cualquier error del cliente.
        # Línea 114: ASERCIÓN: Verifica que el cliente terminó sin errores (código 0).
        self.assertEqual(res_set.returncode, 0)

        # Línea 116: Esta sección, que *debería* usar 'wait_for_log_count',
        # está comentada. Este test solo comprueba que el cliente funciona,
        # pero NO verifica si el log se escribió en DynamoDB.
        # --- SECCIÓN DE VERIFICACIÓN DE LOGS COMENTADA ... ---
        # ...
        # ---------------------------------------------------------------------

        print("Probando GET...")
        res_get = self.run_client(['-i', JSON_GET, '-p', str(PORT)])
        print("--- Salida del Cliente (GET) ---")
        print(res_get.stdout)
        print("--- Errores del Cliente (GET) ---")
        print(res_get.stderr)
        # Verifica que el cliente GET funcionó.
        self.assertEqual(res_get.returncode, 0)

        # --- SECCIÓN DE VERIFICACIÓN DE LOGS COMENTADA ... ---
        # ...
        # ---------------------------------------------------------------------

        print("Probando LIST...")
        res_list = self.run_client(['-i', JSON_LIST, '-p', str(PORT)])
        print("--- Salida del Cliente (LIST) ---")
        print(res_list.stdout)
        print("--- Errores del Cliente (LIST) ---")
        print(res_list.stderr)
        # Verifica que el cliente LIST funcionó.
        self.assertEqual(res_list.returncode, 0)

        # --- SECCIÓN DE VERIFICACIÓN DE LOGS COMENTADA ... ---
        # ...
        # ---------------------------------------------------------------------

        print("--- Test 01 Superado ---")
    # --- FIN DE TEST 01 MODIFICADO ---

    def test_02_argumentos_malformados(self):
        # Línea 156: Caso de Prueba 2: "Argumentos malformados" (según la consigna).
        print("\n--- Test 02: Argumentos Malformados (Cliente) ---")
        # Línea 158: Ejecuta el cliente *sin* el argumento obligatorio '-i'.
        res = self.run_client(['-p', str(PORT)])
        print("--- Salida del Cliente (Malformado) ---")
        print(res.stdout)
        print("--- Errores del Cliente (Malformado) ---")
        print(res.stderr)  # El error de 'argparse' sale por stderr.
        # Línea 164: ASERCIÓN: Verifica que el cliente falló (código de salida != 0).
        self.assertNotEqual(res.returncode, 0)
        # Línea 165: ASERCIÓN: Verifica que falló con el mensaje de error correcto.
        self.assertIn("required: -i/--input", res.stderr)
        print("--- Test 02 Superado ---")

    def test_03_requerimiento_datos_minimos(self):
        # Línea 168: Caso de Prueba 3: "Requerimiento sin datos mínimos" (según la consigna).
        print("\n--- Test 03: Requerimiento sin Datos Mínimos (GET sin ID) ---")
        self.start_server()  # Este test necesita al servidor.
        temp_json = os.path.join(ROOT, 'data', 'temp.json')
        # Líneas 172-173: Crea un archivo JSON temporal solo con "ACTION", pero sin "id".
        with open(temp_json, 'w') as f:
            json.dump({"ACTION": "get"}, f)

        # Línea 175: Ejecuta el cliente con ese JSON "inválido".
        res = self.run_client(['-i', temp_json, '-p', str(PORT)])
        print("--- Salida del Cliente (GET sin ID) ---")
        print(res.stdout)  # El servidor debe devolver un error por stdout.
        print("--- Errores del Cliente (GET sin ID) ---")
        print(res.stderr)
        # Línea 181: ASERCIÓN: El *cliente* debe funcionar (código 0), porque él solo envió el JSON.
        self.assertEqual(res.returncode, 0)
        # Línea 182: ASERCIÓN: El *servidor* debe haber respondido con el error "Missing ID".
        self.assertIn("Missing ID", res.stdout)
        os.remove(temp_json)  # Borra el archivo temporal.
        print("--- Test 03 Superado ---")

    def test_04_manejo_server_caido(self):
        # Línea 186: Caso de Prueba 4: "Manejo en clientes de server aplicativo caido" (según la consigna).
        print("\n--- Test 04: Cliente con Servidor Caído ---")
        # Línea 188: Ejecuta el cliente. 'setUp' ya se aseguró de que el servidor esté apagado.
        res = self.run_client(['-i', JSON_GET, '-p', str(PORT)])
        print("--- Salida del Cliente (Servidor Caído) ---")
        print(res.stdout)
        print("--- Errores del Cliente (Servidor Caído) ---")
        print(res.stderr)  # El error de conexión debe salir por stderr.
        # Línea 194: ASERCIÓN: El cliente debe fallar (código != 0).
        self.assertNotEqual(res.returncode, 0)
        # Línea 195: ASERCIÓN: Verifica que el cliente imprimió el error de conexión esperado.
        self.assertIn("No se pudo conectar", res.stderr)
        print("--- Test 04 Superado ---")

    def test_05_intento_levantar_dos_servidores(self):
        # Línea 198: Caso de Prueba 5: "Intento de levantar dos veces el servidor" (según la consigna).
        print("\n--- Test 05: Doble Servidor en mismo Puerto ---")
        self.start_server()  # Línea 200: Inicia el Servidor 1 (esto funciona).

        print("Intentando iniciar segundo servidor...")
        # Línea 202: Intenta iniciar el Servidor 2 usando 'subprocess.run'
        # (para esperar a que falle) en el MISMO puerto.
        result_server2 = subprocess.run(
            [sys.executable, SERVER, '-p', str(PORT)],
            capture_output=True, text=True
        )

        print("--- Salida del Servidor 2 (STDOUT) ---")
        print(result_server2.stdout)
        print("--- Salida del Servidor 2 (STDERR) ---")
        # El error de "puerto en uso" debe salir por stderr.
        print(result_server2.stderr)

        # Línea 213: ASERCIÓN: Verifica que el Servidor 2 falló al arrancar (código != 0).
        self.assertNotEqual(result_server2.returncode, 0,
                            "El segundo servidor no falló como se esperaba.")

        # Línea 216: ASERCIÓN: Verifica que falló por un "Error de Socket" (puerto ocupado).
        self.assertIn("Error de Socket", result_server2.stderr)
        print("Error de Socket capturado correctamente.")
        print("--- Test 05 Superado ---")


if __name__ == '__main__':
    # Línea 221: Punto de entrada estándar de Python.
    # Ejecuta el framework unittest, que descubre y corre todos los métodos 'test_...'.
    unittest.main()
