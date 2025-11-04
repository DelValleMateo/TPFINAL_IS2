# src/modules/observer.py
import threading
import json
import socket
# threading: ¡Crítico! Se necesita para el "Lock" (candado)
#            para proteger la lista de suscriptores en un entorno multihilo.
# json: Para empaquetar los datos de la notificación.
# socket: Para saber qué tipo de errores pueden ocurrir al enviar.


class Subject:
    # Esta es la clase "Sujeto" (el que es observado).
    def __init__(self):
        self._observers = []
        # Línea 10: Esta es la lista de suscriptores.
        # Guardará los OBJETOS SOCKET de los 'observerclient' conectados.

        self._lock = threading.Lock()
        # Línea 13: ¡CRÍTICO PARA MULTITHREADING!
        # El servidor es multihilo. Imagina que el Hilo A está agregando un cliente
        # a la lista (_observers.append) y, en el *mismo microsegundo*, el Hilo B
        # (de otro cliente) intenta quitar uno (_observers.remove). Esto corrompe la lista.
        # Un 'Lock' (candado) previene esto. Solo un hilo a la vez puede "tener" el candado.

        print("Subject (Observer) inicializado.")

    def subscribe(self, client_socket, client_uuid):
        # Este método es llamado por el Servidor cuando un cliente envía "ACTION": "subscribe".

        with self._lock:
            # Línea 21: 'with self._lock' es la forma elegante de "adquirir el candado".
            # El Hilo se detiene aquí si el candado está ocupado por otro hilo.
            # Cuando el bloque 'with' termina, el candado se libera automáticamente.

            if client_socket not in self._observers:
                # Añade el socket del cliente a la lista de suscriptores.
                self._observers.append(client_socket)
                print(
                    f"OBSERVER: Nuevo suscriptor (UUID: {client_uuid}). Total: {len(self._observers)}")

    def unsubscribe(self, client_socket):
        # Este método es llamado por el Servidor (en el bloque 'finally')
        # cuando un cliente suscriptor se desconecta.

        with self._lock:
            # Adquiere el candado para modificar la lista de forma segura.
            if client_socket in self._observers:
                try:
                    self._observers.remove(client_socket)
                    print(
                        f"OBSERVER: Suscriptor desconectado. Total: {len(self._observers)}")
                except ValueError:
                    # Una salvaguarda por si dos hilos intentan
                    # desuscribir al mismo cliente al mismo tiempo.
                    pass  # Ya fue removido por otro hilo.

    def notify(self, data, encoder_class):
        # ¡La función principal del Observer!
        # Es llamada por el Servidor DESPUÉS de un 'set' exitoso.

        with self._lock:
            # Adquiere el candado. No queremos que nadie se suscriba o
            # desuscriba MIENTRAS estamos enviando notificaciones.

            if not self._observers:
                # Si no hay nadie suscrito, no hace nada.
                return

            print(
                f"OBSERVER: Notificando a {len(self._observers)} suscriptor(es)...")

            # Prepara el mensaje (la carga) una sola vez.
            message_bytes = json.dumps(
                {"EVENT": "update", "DATA": data}, cls=encoder_class).encode('utf-8')

            # ¡TRUCO IMPORTANTE!
            # Iteramos sobre list(self._observers).
            # list() crea una COPIA de la lista.
            for obs_socket in list(self._observers):
                try:
                    # Envía el mensaje de notificación al socket del cliente.
                    obs_socket.sendall(message_bytes)
                except socket.error as e:
                    # ¿Por qué iteramos sobre una copia? Por ESTA LÍNEA.
                    # Si 'sendall' falla, es porque el cliente se desconectó.
                    # Por lo tanto, debemos sacarlo de la lista (hacer 'unsubscribe').
                    # PERO, no puedes modificar una lista (la original) mientras
                    # estás iterando sobre ella.
                    # Al iterar sobre una COPIA, podemos modificar la lista ORIGINAL
                    # (self._observers) sin problemas.
                    print(
                        f"OBSERVER: Error enviando a un suscriptor ({e}). Eliminándolo.")
                    self.unsubscribe(obs_socket)
