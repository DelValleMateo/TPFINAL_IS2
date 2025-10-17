# src/modules/observer.py

import threading
import json
import socket  # <-- LÍNEA AGREGADA

# *----------------------------------------------------------------------------
# * UADER-FCyT
# * Ingeniería de Software II
# *
# * observer.py
# * Módulo que implementa el patrón Observer (Sujeto) para notificar
# * a los clientes suscriptos.
# *----------------------------------------------------------------------------


class Subject:
    """
    Implementa el patrón Observer (lado Sujeto).
    Mantiene una lista de observadores (clientes) y les notifica
    sobre eventos, como actualizaciones de datos.
    """

    def __init__(self):
        # Lista de observadores (sockets de clientes)
        self._observers = []
        # Un candado (Lock) para hacer la lista thread-safe
        self._lock = threading.Lock()
        print("Subject (Observer) inicializado.")

    def subscribe(self, client_socket, client_uuid):
        """
        Agrega un observador a la lista.
        """
        with self._lock:
            if client_socket not in self._observers:
                self._observers.append(client_socket)
                print(
                    f"OBSERVER: Nuevo suscriptor registrado (UUID: {client_uuid}). Total: {len(self._observers)}")

    def unsubscribe(self, client_socket):
        """
        Elimina un observador de la lista (ej. si se desconecta).
        """
        with self._lock:
            if client_socket in self._observers:
                try:
                    # Intenta removerlo si aún existe
                    self._observers.remove(client_socket)
                    print(
                        f"OBSERVER: Suscriptor desconectado. Total: {len(self._observers)}")
                except ValueError:
                    # Es posible que otro hilo ya lo haya quitado, no hay problema
                    pass

    def notify(self, data, encoder_class):
        """
        Notifica a TODOS los observadores enviando los datos.
        """
        with self._lock:
            if not self._observers:
                return  # No hay nadie a quien notificar

            print(
                f"OBSERVER: Notificando a {len(self._observers)} suscriptor(es)...")

            # Crear el mensaje de notificación
            notification_message = {
                "EVENT": "update",
                "DATA": data
            }
            message_json = json.dumps(
                notification_message, cls=encoder_class, indent=4)
            message_bytes = message_json.encode('utf-8')

            # Hacemos una copia de la lista para iterar
            # en caso de que necesitemos eliminar sockets fallidos
            for observer_socket in list(self._observers):
                try:
                    observer_socket.sendall(message_bytes)
                except socket.error as e:  # <-- Ahora 'socket' está definido
                    # El socket está roto o cerrado
                    print(
                        f"OBSERVER: Error enviando a un suscriptor ({e}). Eliminándolo.")
                    # Usamos 'observer_socket' para desuscribir
                    self.unsubscribe(observer_socket)
