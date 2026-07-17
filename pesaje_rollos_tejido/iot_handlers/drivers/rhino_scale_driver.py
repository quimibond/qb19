# Part of Odoo. See LICENSE file for full copyright and licensing details.
#
# IMPORTANTE:
# - Este archivo debe vivir dentro de iot_handlers/drivers/ de TU módulo
#   de Odoo (pesaje_rollos_tejido), instalado en el servidor -- NO editado
#   directamente en el disco de la IoT Box.
# - Nombre de archivo: rhino_scale_driver.py (NO serial_scale_driver.py --
#   ese nombre colisiona con el archivo core de Odoo en
#   iot_drivers/iot_handlers/drivers/serial_scale_driver.py, que se
#   sincroniza a la misma ruta relativa y puede pisar este archivo).
# - Este driver hereda DIRECTAMENTE de SerialDriver (la clase base real de
#   Odoo), sin redefinir ScaleDriver ni Toledo8217Driver -- esas clases ya
#   las aporta el módulo core 'iot_drivers'. Aquí solo vive lo que es
#   genuinamente propio: el protocolo y el driver de la báscula Rhino.

import logging
import re
import serial
import threading
import time
import traceback

from odoo import http
from odoo.addons.iot_drivers.controllers.proxy import proxy_drivers
from odoo.addons.iot_drivers.event_manager import event_manager
from odoo.addons.iot_drivers.iot_handlers.drivers.serial_base_driver import (
    SerialDriver, SerialProtocol, serial_connection
)


_logger = logging.getLogger(__name__)

# Solo necesario para exponer la báscula vía hw_proxy (usado por el botón
# "Capturar" del widget peso_bascula, a través de /hw_proxy/scale_read).
ACTIVE_SCALE = None
new_weight_event = threading.Event()

RhinoIQYProtocol = SerialProtocol(
    name='Rhino I-QY PLABA-9',
    baudrate=9600,
    bytesize=serial.EIGHTBITS,
    stopbits=serial.STOPBITS_ONE,
    parity=serial.PARITY_NONE,
    timeout=1,
    writeTimeout=1,
    # Trama continua tipo 'P     46.5\r\n'. Signo '-' opcional: con la
    # báscula vacía, el indicador puede transmitir un pequeño drift
    # negativo de cero (ej. 'P    -0.2\r\n'). Sin el '-', esa línea no
    # matcheaba y el resultado se quedaba en el último peso válido leído
    # (el bug del "peso anterior" que ya resolvimos).
    measureRegexp=b"P?\\s*(-?[0-9.]+)\\r\\n",
    statusRegexp=None,
    commandDelay=0.2,
    measureDelay=0.5,
    newMeasureDelay=0.2,
    commandTerminator=b'',
    measureCommand=b'',  # La báscula transmite de forma continua
    emptyAnswerValid=False,
)


class ScaleReadHardwareProxy(http.Controller):
    @http.route('/hw_proxy/scale_read', type='jsonrpc', auth='none', cors='*')
    def scale_read(self):
        if ACTIVE_SCALE:
            return {'weight': ACTIVE_SCALE._scale_read_hw_proxy()}
        return None


class RhinoScaleDriver(SerialDriver):
    """Driver para indicador de peso Rhino I-QY (báscula industrial de
    rollo/subproducto en tejido). Autocontenido: no depende de ninguna
    clase intermedia propia (no hay ScaleDriver ni Toledo8217Driver aquí),
    solo de SerialDriver, que es la base real que aporta Odoo."""

    _protocol = RhinoIQYProtocol
    last_sent_value = None
    RECONNECT_DELAY = 3  # segundos entre reintentos de reconexión tras un error

    def __init__(self, identifier, device):
        super().__init__(identifier, device)
        self.device_type = 'scale'
        self.device_manufacturer = 'Rhino'
        self.device_name = 'Báscula Industrial Rhino PLABA-9 (I-QY)'
        self.net_weight_char = None
        self._set_actions()
        self._is_reading = True
        self.tare_mode = False

        # El HW Proxy solo puede exponer una báscula; se queda con la
        # última conectada.
        global ACTIVE_SCALE  # noqa: PLW0603
        ACTIVE_SCALE = self
        proxy_drivers['scale'] = ACTIVE_SCALE

    def get_status(self):
        """Permite a `hw_proxy.Proxy` consultar el estado de la báscula."""
        status = self._status
        return {'status': status['status'], 'messages': [status['message_title']]}

    def _set_actions(self):
        self._actions.update({
            'read_once': self._read_once_action,
            'start_reading': self._start_reading_action,
            'stop_reading': self._stop_reading_action,
        })

    def _start_reading_action(self, data):
        self._is_reading = True

    def _stop_reading_action(self, data):
        self._is_reading = False

    def _read_once_action(self, data):
        self._read_weight()
        self.last_sent_value = self.data['result']

    @classmethod
    def supported(cls, device):
        """Detecta la báscula leyendo datos reales y comparándolos contra
        el regex del protocolo -- 10 intentos de 1s (~10s en total), para
        tolerar que la báscula tarde en estabilizarse/transmitir justo en
        el momento del escaneo (con 3 intentos, en producción a veces no
        alcanzaba a tiempo y la báscula quedaba sin reconocer)."""
        protocol = cls._protocol
        try:
            with serial_connection(device['identifier'], protocol, is_probing=True) as connection:
                for _ in range(10):
                    response = connection.read_until(b"\n")
                    if re.search(protocol.measureRegexp, response):
                        return True
            return False
        except Exception:
            _logger.exception('Error while probing %s with protocol %s', device, protocol.name)
            return False

    @staticmethod
    def _get_raw_response(connection):
        return connection.read_until(b"\n")

    def _read_status(self, answer):
        # El protocolo Rhino no envía trama de estatus/errores separada.
        pass

    def _read_weight(self):
        """Lee un peso nuevo. Clampa lecturas negativas a 0.0 -- con la
        báscula vacía, el drift de cero puede dar un valor levemente
        negativo que físicamente equivale a "no hay nada sobre la
        báscula"; se trata como 0, no como excusa para dejar el dato
        viejo sin actualizar."""
        protocol = self._protocol
        self._connection.write(protocol.measureCommand + protocol.commandTerminator)
        answer = self._get_raw_response(self._connection)
        match = re.search(protocol.measureRegexp, answer)
        if match:
            weight = float(match.group(1))
            if weight < 0:
                weight = 0.0
            self.data.update({'result': weight, 'status': self._status})
        else:
            self._read_status(answer)

    def _scale_read_hw_proxy(self):
        """Usado por el botón 'Capturar' vía /hw_proxy/scale_read.

        - Drena activamente el buffer antes de leer (no solo
          reset_input_buffer(), que no alcanza a limpiar backlog en
          capas más bajas del chip USB-serial con algunos adaptadores).
        - Cada lectura del drenado tiene una pausa breve (20ms) y un
          límite de iteraciones -- SIN esta pausa, un drenado en ráfaga
          puede saturar el driver de Windows del adaptador USB-serial y
          causar un crash real del sistema (BugCheck 0xD1,
          DRIVER_IRQL_NOT_LESS_OR_EQUAL, confirmado en el Visor de
          Eventos de la PC de producción).
        - Resetea self.data['result'] a None antes de leer: si la
          lectura final no logra matchear una línea válida, se devuelve
          None (el frontend lo interpreta como "peso no válido") en vez
          de reciclar silenciosamente el último peso conocido.
        - El return va DENTRO del lock, para que el hilo de fondo
          (run()/_take_measure()) no pueda sobreescribir el resultado
          justo antes de que se regrese al llamador.
        """
        with self._device_lock:
            try:
                MAX_DRAIN_ITERATIONS = 20
                iterations = 0
                drained_any = True
                while drained_any and iterations < MAX_DRAIN_ITERATIONS:
                    drained_any = False
                    while self._connection.in_waiting > 0 and iterations < MAX_DRAIN_ITERATIONS:
                        self._connection.read(self._connection.in_waiting)
                        drained_any = True
                        iterations += 1
                        time.sleep(0.02)  # respiro para el driver del puerto
                    if drained_any:
                        time.sleep(0.05)
            except Exception:
                _logger.exception(
                    "No se pudo drenar el buffer serial de %s antes de leer",
                    self.device_name,
                )

            self.data['result'] = None
            self._read_weight()
            return self.data['result']

    def _take_measure(self):
        with self._device_lock:
            self._read_weight()
            if self.data['result'] != self.last_sent_value or self._status['status'] == self.STATUS_ERROR:
                self.last_sent_value = self.data['result']
                event_manager.device_changed(self)

    def run(self):
        """Igual que SerialDriver.run(), pero con reconexión automática:
        si la lectura falla (ruido eléctrico, microcorte del adaptador
        USB-serial, etc.) el hilo NO muere -- se reintenta abrir el
        puerto tras una breve espera, en vez de requerir desconectar/
        reconectar el cable físicamente cada vez.

        Esta lógica vive aquí, autocontenida en RhinoScaleDriver -- nunca
        se tocó serial_base_driver.py (core de Odoo), así que sobrevive
        updates/reinstalaciones del módulo iot_drivers estándar.
        """
        while not self._stopped.is_set():
            try:
                with serial_connection(self.device_identifier, self._protocol) as connection:
                    self._connection = connection
                    self._status = {'status': self.STATUS_CONNECTED, 'message_title': '', 'message_body': ''}
                    self._push_status()

                    while not self._stopped.is_set():
                        try:
                            self._take_measure()
                        except Exception:
                            _logger.exception(
                                "Error leyendo %s, se forzará reconexión del puerto.",
                                self.device_name,
                            )
                            break  # fuerza reapertura del puerto en la siguiente vuelta
                        time.sleep(self._protocol.newMeasureDelay)
            except Exception:
                msg = 'Error al conectar con %s' % self.device_name
                _logger.exception(msg)
                self._status = {
                    'status': self.STATUS_ERROR,
                    'message_title': msg,
                    'message_body': traceback.format_exc(),
                }
                self._push_status()

            if not self._stopped.is_set():
                time.sleep(self.RECONNECT_DELAY)

        self._status['status'] = self.STATUS_DISCONNECTED
        self._push_status()
