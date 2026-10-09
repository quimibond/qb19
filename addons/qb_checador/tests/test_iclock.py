# -*- coding: utf-8 -*-
"""El protocolo PUSH de ZKTeco contra el controlador, tal como lo habla un reloj."""
from odoo.tests import tagged
from odoo.tests.common import HttpCase
from odoo.tools import mute_logger


@tagged('post_install', '-at_install')
class TestIclock(HttpCase):

    def setUp(self):
        super().setUp()
        self.equipo = self.env['qb.checador.equipo'].create({
            'name': 'Entrada planta', 'serial': 'UDP0000001', 'tz': 'America/Mexico_City', 'prefijos': 'S',
        })
        self.empleado = self.env['hr.employee'].create({'name': 'Reloj', 'qb_checador_pin': '31'})

    def test_handshake(self):
        r = self.url_open('/iclock/cdata?SN=UDP0000001&options=all&pushver=2.4.1&language=83')
        self.assertEqual(r.status_code, 200)
        self.assertTrue(r.text.startswith('GET OPTION FROM: UDP0000001\r\n'))
        self.assertIn('ATTLOGStamp=0\r\n', r.text)
        self.assertIn('Realtime=1\r\n', r.text)
        self.assertIn('TimeZone=-6\r\n', r.text)
        self.assertIn('TransFlag=TransData AttLog\t', r.text)
        self.equipo.invalidate_recordset()
        self.assertTrue(self.equipo.ultima_conexion)
        self.assertEqual(self.equipo.firmware, 'PUSH 2.4.1')

    def test_handshake_firmware_viejo(self):
        r = self.url_open('/iclock/cdata?SN=UDP0000001&options=all&language=83&pushver=1.2')
        self.assertIn('TransFlag=1111000000\r\n', r.text)

    def test_attlog(self):
        cuerpo = '31\t2026-10-05 06:58:10\t0\t1\t0\t0\t0\n31\t2026-10-05 19:02:40\t1\t1\t0\t0\t0\n'
        r = self.url_open('/iclock/cdata?SN=UDP0000001&table=ATTLOG&Stamp=98765', data=cuerpo.encode('utf-8'),
                          headers={'Content-Type': 'text/plain'})
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.text, 'OK: 2\r\n')
        checadas = self.env['qb.checada'].search([('pin', '=', '31')])
        self.assertEqual(len(checadas), 2)
        self.assertEqual(checadas.mapped('employee_id'), self.empleado)
        self.equipo.invalidate_recordset()
        self.assertEqual(self.equipo.attlog_stamp, '98765')
        # en la siguiente conexión se le devuelve la marca para que mande solo lo nuevo
        r = self.url_open('/iclock/cdata?SN=UDP0000001&options=all&pushver=2.4.1')
        self.assertIn('ATTLOGStamp=98765\r\n', r.text)

    def test_otras_tablas_y_getrequest(self):
        r = self.url_open('/iclock/cdata?SN=UDP0000001&table=options&Stamp=1',
                          data=b'~DeviceName=MB10-VL,FWVersion=Ver 6.60 Sep 19 2019,~SerialNumber=UDP0000001',
                          headers={'Content-Type': 'text/plain'})
        self.assertEqual(r.text, 'OK\r\n')
        self.equipo.invalidate_recordset()
        self.assertEqual(self.equipo.firmware, 'Ver 6.60 Sep 19 2019')
        r = self.url_open('/iclock/getrequest?SN=UDP0000001&INFO=Ver%206.60%20Sep%2019%202019,0,0,0,192.168.1.201,0')
        self.assertEqual(r.text, 'OK\r\n')
        self.equipo.invalidate_recordset()
        self.assertEqual(self.equipo.ip, '192.168.1.201')
        r = self.url_open('/iclock/devicecmd?SN=UDP0000001', data=b'ID=1&Return=0&CMD=DATA',
                          headers={'Content-Type': 'text/plain'})
        self.assertEqual(r.text, 'OK\r\n')

    def test_equipo_desconocido_queda_registrado_sin_autorizar(self):
        with mute_logger('odoo.addons.qb_checador.controllers.iclock'):
            r = self.url_open('/iclock/cdata?SN=DESCONOCIDO1&options=all&pushver=2.4.1')
        self.assertEqual(r.status_code, 403)
        nuevo = self.env['qb.checador.equipo'].with_context(active_test=False).search([('serial', '=', 'DESCONOCIDO1')])
        self.assertEqual(len(nuevo), 1)
        self.assertFalse(nuevo.active)
        self.assertFalse(nuevo.autorizado)
        # no guarda checadas de un equipo sin autorizar
        with mute_logger('odoo.addons.qb_checador.controllers.iclock'):
            r = self.url_open('/iclock/cdata?SN=DESCONOCIDO1&table=ATTLOG&Stamp=1',
                              data=b'31\t2026-10-05 06:58:10\t0\t1\t0\n', headers={'Content-Type': 'text/plain'})
        self.assertEqual(r.status_code, 403)
        self.assertEqual(self.env['qb.checada'].search_count([('equipo_id', '=', nuevo.id)]), 0)

    def test_equipo_registrado_pero_no_autorizado(self):
        self.equipo.autorizado = False
        r = self.url_open('/iclock/cdata?SN=UDP0000001&options=all')
        self.assertEqual(r.status_code, 403)
