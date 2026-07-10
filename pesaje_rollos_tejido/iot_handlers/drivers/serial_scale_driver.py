# Part of Odoo. See LICENSE file for full copyright and licensing details.
#
# IMPORTANTE: este archivo debe vivir dentro de iot_handlers/drivers/ de TU
# módulo de Odoo (instalado en el servidor), NO editado directamente en el
# disco de la IoT Box. La box lo copia solo en cada sync/arranque; si lo
# editas solo ahí, la próxima sincronización lo sobrescribe con lo que
# haya (o no haya) en el servidor.

import logging
import re
import serial
import threading
import time
import traceback

from odoo import http
from odoo.addons.iot_drivers.controllers.proxy import proxy_drivers
from odoo.addons.iot_drivers.event_manager import event_manager
from odoo.addons.iot_drivers.iot_handlers.drivers.serial_base_driver import SerialDriver, SerialProtocol, serial_connection


_logger = logging.getLogger(__name__)

# Only needed to expose scale via hw_proxy (used by Community edition)
ACTIVE_SCALE = None
new_weight_event = threading.Event()

# 8217 Mettler-Toledo (Weight-only) Protocol, as described in the scale's Service Manual.
Toledo8217Protocol = SerialProtocol(
    name='Toledo 8217',
    baudrate=9600,
    bytesize=serial.SEVENBITS,
    stopbits=serial.STOPBITS_ONE,
    parity=serial.PARITY_EVEN,
    timeout=1,
    writeTimeout=1,
    measureRegexp=b"\x02\\s*([0-9.]+)N?\\r",
    statusRegexp=b"\x02\\s*\\?([^\x00])\\r",
    commandDelay=0.2,
    measureDelay=0.5,
    newMeasureDelay=0.2,
    commandTerminator=b'',
    measureCommand=b'W',
    emptyAnswerValid=False,
)

RhinoIQYProtocol = SerialProtocol(
    name='Rhino I-QY PLABA-9',
    baudrate=9600,
    bytesize=serial.EIGHTBITS,
    stopbits=serial.STOPBITS_ONE,
    parity=serial.PARITY_NONE,
    timeout=1,
    writeTimeout=1,
    # Trama continua tipo 'P     46.5\r\n'
    measureRegexp=b"P?\\s*([0-9.]+)\\r\\n",
    statusRegexp=None,
    commandDelay=0.2,
    measureDelay=0.5,
    newMeasureDelay=0.2,
    commandTerminator=b'',
    measureCommand=b'',  # La báscula transmite de forma continua, no necesita comando de petición
    emptyAnswerValid=False,
)


# HW Proxy is used by Community edition
class ScaleReadHardwareProxy(http.Controller):
    @http.route('/hw_proxy/scale_read', type='jsonrpc', auth='none', cors='*')
    def scale_read(self):
        if ACTIVE_SCALE:
            return {'weight': ACTIVE_SCALE._scale_read_hw_proxy()}
        return None


class ScaleDriver(SerialDriver):
    """Abstract base class for scale drivers."""
    last_sent_value = None

    RECONNECT_DELAY = 3  # segundos entre reintentos de reconexión tras un error

    def __init__(self, identifier, device):
        super().__init__(identifier, device)
        self.device_type = 'scale'
        self._set_actions()
        self._is_reading = True
        self.tare_mode = False

        # The HW Proxy can only expose one scale,
        # only the last scale connected is kept
        global ACTIVE_SCALE  # noqa: PLW0603
        ACTIVE_SCALE = self
        proxy_drivers['scale'] = ACTIVE_SCALE

    def get_status(self):
        """Allows `hw_proxy.Proxy` to retrieve the status of the scales"""
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

    @staticmethod
    def _get_raw_response(connection):
        answer = []
        while True:
            char = connection.read(1)
            if not char:
                break
            else:
                answer.append(bytes(char))
        return b''.join(answer)

    def _read_weight(self):
        protocol = self._protocol
        self._connection.write(protocol.measureCommand + protocol.commandTerminator)
        answer = self._get_raw_response(self._connection)
        match = re.search(self._protocol.measureRegexp, answer)
        if match:
            if self.net_weight_char and self.net_weight_char in answer:
                self.tare_mode = True
            else:
                self.tare_mode = False
            self.data.update({
                'result': float(match.group(1)),
                'status': self._status
            })
        else:
            self._read_status(answer)

    def _scale_read_hw_proxy(self):
        """Usado por el botón 'Capturar' vía /hw_proxy/scale_read.

        CLAVE: la báscula puede estar transmitiendo en streaming continuo,
        así que el buffer de entrada del SO puede tener líneas viejas sin
        consumir (p. ej. del rollo/artículo anterior) esperando desde antes
        de que el operador diera clic. Una lectura simple entrega la línea
        MÁS ANTIGUA del buffer (FIFO), no la más reciente. Vaciamos el
        buffer primero para garantizar que la próxima línea leída sea una
        transmitida DESPUÉS de este instante -- el peso actual real.
        """
        with self._device_lock:
            try:
                self._connection.reset_input_buffer()
            except Exception:
                _logger.exception(
                    "No se pudo vaciar el buffer serial antes de leer %s",
                    self.device_name,
                )
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
        USB-serial, etc.) el hilo NO muere -- se reintenta abrir el puerto
        tras una breve espera, en vez de requerir desconectar/reconectar
        el cable físicamente cada vez.

        Esta lógica vive aquí, en la clase compartida por los drivers de
        báscula de ESTE módulo -- nunca se tocó serial_base_driver.py
        (core de Odoo), así que sobrevive updates/reinstalaciones del
        módulo iot_drivers estándar.
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


class Toledo8217Driver(ScaleDriver):
    """Driver for the Toledo 8217 serial scale. (sin cambios funcionales)"""
    _protocol = Toledo8217Protocol

    def __init__(self, identifier, device):
        super().__init__(identifier, device)
        self.device_manufacturer = 'Toledo'
        self.net_weight_char = b'N'

    @classmethod
    def supported(cls, device):
        protocol = cls._protocol
        try:
            with serial_connection(device['identifier'], protocol, is_probing=True) as connection:
                connection.reset_input_buffer()
                connection.write(b'Ehello' + protocol.commandTerminator)
                time.sleep(protocol.commandDelay)
                answer = connection.read(8)
                if answer == b'\x02E\rhello':
                    connection.write(b'F' + protocol.commandTerminator)
                    connection.reset_input_buffer()
                    return True
        except serial.serialutil.SerialTimeoutException:
            pass
        except Exception:
            _logger.exception('Error while probing %s with protocol %s', device, protocol.name)
        return False

    @staticmethod
    def _get_raw_response(connection):
        return connection.read_until(b"\r")

    def _read_status(self, answer):
        status_char_error_bits = (
            'Scale in motion', 'Over capacity', 'Under zero',
            'Outside zero capture range', 'Center of zero', 'Net weight',
            'Bad Command from host',
        )
        status_match = self._protocol.statusRegexp and re.search(self._protocol.statusRegexp, answer)
        if status_match:
            status_char = status_match.group(1).decode()
            binary_status_char = format(ord(status_char), '08b')
            for index, bit in enumerate(binary_status_char[1:][::-1]):
                if int(bit):
                    if index == 5 and self.tare_mode:
                        continue
                    _logger.debug("Scale error: %s. Status string: %s. Scale answer: %s.", status_char_error_bits[index], binary_status_char, answer)
                    self.data.update({'result': 0, 'status': self._status})
                    break


class RhinoScaleDriver(ScaleDriver):
    """Driver para indicador de peso Rhino I-QY.

    Corregido: ya no depende del número de puerto COM (Windows puede
    reasignarlo tras un replug), sino de la firma real de datos del
    protocolo. Ya no requiere ningún parche en serial_base_driver.py.
    """
    _protocol = RhinoIQYProtocol

    def __init__(self, identifier, device):
        super().__init__(identifier, device)
        self.device_manufacturer = 'Rhino'
        self.device_name = 'Báscula Industrial Rhino PLABA-9 (I-QY)'
        self.net_weight_char = None

    @classmethod
    def supported(cls, device):
        """Detecta la báscula leyendo datos reales y comparándolos contra
        el regex del protocolo -- reintenta varias veces porque el
        timeout de sondeo (1s) puede no alinear con el ciclo de
        transmisión continua de la báscula."""
        protocol = cls._protocol
        try:
            with serial_connection(device['identifier'], protocol, is_probing=True) as connection:
                for _ in range(3):
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

    def _scale_read_hw_proxy(self):
        """Override específico para Rhino (streaming continuo).

        reset_input_buffer() (heredado de ScaleDriver) purga el buffer a
        nivel de sistema operativo, pero con algunos chips/puentes
        USB-serial puede quedar backlog en un buffer más bajo (FIFO del
        propio chip) que se rellena de inmediato después del purge -- por
        eso ese fix por sí solo no bastó.

        Aquí, en cambio, DRENAMOS ACTIVAMENTE: leemos y descartamos todo
        lo que ya esté disponible en el buffer, en un bucle corto, hasta
        que durante una ventana breve no llegue nada más. Solo entonces
        hacemos la lectura "oficial" -- así, sin importar en qué capa
        estuviera el backlog, garantizamos consumirlo todo antes de
        quedarnos con la línea que cuenta.

        Además, reseteamos self.data['result'] a None antes de leer: si
        la lectura final no logra matchear una línea válida (p. ej. por
        timeout), preferimos devolver None (que el frontend interpreta
        como "peso no válido" y así lo avisa) en vez de reciclar
        silenciosamente el último peso conocido, que es exactamente el
        bug que veníamos arrastrando.
        """
        with self._device_lock:
            try:
                # Drena todo lo disponible ahora mismo, dando pequeños
                # márgenes para que lleguen bytes que estuvieran en
                # tránsito, hasta que el buffer deje de crecer.
                drained_any = True
                while drained_any:
                    drained_any = False
                    while self._connection.in_waiting > 0:
                        self._connection.read(self._connection.in_waiting)
                        drained_any = True
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