# src/modules/db_singleton.py
import boto3
import botocore
import sys
# boto3: Es la librería oficial de Amazon para interactuar con AWS (DynamoDB).
# botocore: Contiene las excepciones (errores) de boto3.
# sys: Se usa para sys.exit, que permite terminar el programa si la conexión a la BD falla.


class DatabaseSingleton:
    _instance = None
    # Línea 5: Esta es la variable de clase que guardará la *única* instancia.
    # Al inicio, no hay ninguna instancia, por lo que es 'None' (nula).

    def __new__(cls):
        # Línea 7: Este es el método mágico del Singleton. __new__ es un método especial de Python
        # que se llama ANTES que __init__. __new__ es responsable de CREAR la instancia.
        if cls._instance is None:
            # Línea 9: Comprueba si la instancia ÚNICA ya fue creada.
            print("Creando nueva instancia de DatabaseSingleton...")
            # Línea 11: Si es 'None' (la primera vez), llama al __new__ de la clase padre
            # (el creador de objetos base de Python) para CREAR la instancia.
            cls._instance = super(DatabaseSingleton, cls).__new__(cls)
            # Línea 13: Añade un "flag" (bandera) a la instancia.
            # La usaremos para asegurarnos de que la *configuración* (__init__) también se ejecute una sola vez.
            cls._instance._initialized = False

        # Línea 15: Devuelve la instancia (ya sea la que acaba de crear o la que ya existía).
        return cls._instance

    def __init__(self):
        # Línea 17: __init__ es el CONSTRUCTOR. Se llama CADA VEZ que alguien escribe DatabaseSingleton().
        # Aquí está el truco: __new__ asegura que sea el *mismo objeto*, y este __init__
        # tiene un "seguro" para que la conexión a la BD solo se haga una vez.

        if not hasattr(self, '_initialized'):  # Fix para el editor
            self._initialized = False

        if self._initialized:
            # Línea 23: Si el flag _initialized es 'True' (es decir, de la segunda vez en adelante)...
            # ...simplemente no hace nada y retorna.
            # Esto evita que se reconecte a AWS cada vez que se pide la instancia.
            return

        # --- Este bloque solo se ejecuta LA PRIMERA VEZ ---
        print("Inicializando conexión a DynamoDB...")
        try:
            # Línea 30: (ESTA ES LA CORRECCIÓN QUE HICIMOS)
            # Aquí es donde realmente se conecta a AWS. Llama a boto3 y especifica la región
            # para asegurarse de que el servidor y el test miren a la misma base de datos.
            self.dynamodb = boto3.resource('dynamodb', region_name='us-east-1')

            # Línea 32-33: Obtiene las "manijas" (referencias) a las dos tablas que pide la consigna.
            self.table_corporate_data = self.dynamodb.Table('CorporateData')
            self.table_corporate_log = self.dynamodb.Table('CorporateLog')

            # Línea 34-35: .load() fuerza una conexión real a AWS.
            # Si las credenciales son incorrectas o las tablas no existen, fallará aquí.
            self.table_corporate_data.load()
            self.table_corporate_log.load()

            print("Tablas 'CorporateData' y 'CorporateLog' cargadas.")
            # Línea 38: ¡Hecho! Sube la bandera _initialized a True.
            self._initialized = True
        except Exception as e:
            # Línea 39: Si algo en el bloque 'try' falla (mala región, sin credenciales, tablas no existen)...
            print(
                f"Error fatal al conectar con DynamoDB: {e}", file=sys.stderr)
            # ...el servidor no puede arrancar. Cierra el programa.
            sys.exit(1)

    # --- Métodos Públicos ---
    # Una vez que la instancia está creada, el resto del código usará estos métodos
    # para obtener las tablas.

    def get_corporate_data_table(self):
        # Devuelve la referencia a la tabla de datos.
        return self.table_corporate_data

    def get_corporate_log_table(self):
        # Devuelve la referencia a la tabla de logs.
        return self.table_corporate_log
