# -*- coding: utf-8 -*-
"""Push de eventos ISAPI de un reloj Hikvision (DS-K1T…) contra el controlador."""
import json
from datetime import datetime, timedelta

from odoo import fields
from odoo.tests import tagged
from odoo.tests.common import HttpCase, TransactionCase
from odoo.tools import mute_logger

from odoo.addons.qb_checador.controllers.hikvision import (checada_del_evento, eventos_del_cuerpo,
                                                          motivo_descartado)


def evento(pin='26', minor=38, hora='2026-10-13T08:59:41-06:00', tipo='AccessControllerEvent', mayor=5):
    ace = {'deviceName': 'Ciruelos', 'majorEventType': mayor, 'subEventType': minor, 'cardReaderNo': 1,
           'serialNo': 1201, 'currentVerifyMode': 'cardOrFaceOrFp', 'attendanceStatus': 'checkIn'}
    if pin is not None:
        ace.update({'employeeNoString': pin, 'name': 'PRUEBA'})
    return {'ipAddress': '192.168.10.50', 'macAddress': 'aa:bb:cc:dd:ee:ff', 'dateTime': hora,
            'activePostCount': 1, 'eventType': tipo, 'eventState': 'active',
            'eventDescription': 'Access Controller Event', 'AccessControllerEvent': ace}


XML = b'''<?xml version="1.0" encoding="UTF-8"?>
<EventNotificationAlert version="2.0" xmlns="http://www.isapi.org/ver20/XMLSchema">
<ipAddress>192.168.10.50</ipAddress>
<dateTime>2026-10-13T18:02:05-06:00</dateTime>
<eventType>AccessControllerEvent</eventType>
<AccessControllerEvent>
<majorEventType>5</majorEventType>
<subEventType>75</subEventType>
<employeeNoString>26</employeeNoString>
<currentVerifyMode>faceOrFpOrCardOrPw</currentVerifyMode>
</AccessControllerEvent>
</EventNotificationAlert>'''


class TestHikvisionLectura(TransactionCase):
    """Lectura del cuerpo y del evento, sin servidor."""

    def test_json_xml_y_multipart(self):
        e = evento()
        self.assertEqual(eventos_del_cuerpo(json.dumps(e).encode(), 'application/json'), [e])
        xml = eventos_del_cuerpo(XML, 'application/xml')
        self.assertEqual(xml[0]['AccessControllerEvent']['employeeNoString'], '26')
        cuerpo = b''.join([
            b'--MIME_boundary\r\nContent-Disposition: form-data; name="AccessControllerEvent"\r\n',
            b'Content-Type: application/json\r\n\r\n', json.dumps(e).encode(),
            b'\r\n--MIME_boundary\r\nContent-Disposition: form-data; name="Picture"; filename="Picture.jpg"\r\n',
            b'Content-Type: image/jpeg\r\n\r\n\xff\xd8\xff\xe0FOTO\r\n--MIME_boundary--\r\n',
        ])
        self.assertEqual(eventos_del_cuerpo(cuerpo, 'multipart/mixed; boundary=MIME_boundary'), [e])
        self.assertEqual(eventos_del_cuerpo(b'basura', 'text/plain'), [])
        self.assertEqual(eventos_del_cuerpo(b'<!DOCTYPE x [<!ENTITY a "b">]><x>&a;</x>', 'text/xml'), [])

    def test_que_cuenta_como_checada(self):
        r = checada_del_evento(evento())
        self.assertEqual((r['pin'], r['hora_local'], r['workcode'], r['status']),
                         ('26', '2026-10-13 08:59:41', '38', 'checkIn'))
        self.assertIsNone(checada_del_evento(evento(pin=None)), 'huella no reconocida: sin empleado')
        self.assertIsNone(checada_del_evento(evento(pin='0')))
        self.assertIsNone(checada_del_evento(evento(minor=21, pin=None)), 'puerta abierta')
        self.assertIsNone(checada_del_evento(evento(mayor=2)), 'alarma')
        self.assertIsNone(checada_del_evento(evento(tipo='heartBeat')))
        self.assertIsNone(checada_del_evento(evento(minor=38), excluidos={'38'}))
        self.assertEqual(motivo_descartado(evento()), 'cuenta')
        self.assertIn('sin número de empleado', motivo_descartado(evento(pin=None)))
        self.assertIn('eventType', motivo_descartado(evento(tipo='heartBeat')))
        self.assertIn('excluido', motivo_descartado(evento(), excluidos={'38'}))
        envuelto = {'EventNotificationAlert': evento(hora='2026-10-13T08:59:41+08:00')}
        self.assertEqual(checada_del_evento(envuelto)['hora_local'], '2026-10-13 08:59:41',
                         'la hora es la de la pantalla del reloj, aunque traiga otra zona')


@tagged('post_install', '-at_install')
class TestHikvisionPush(HttpCase):

    def setUp(self):
        super().setUp()
        self.equipo = self.env['qb.checador.equipo'].create({
            'name': 'Ciruelos', 'serial': 'DS-K1T804AEF20230403V010400ENAG0000001', 'marca': 'hikvision',
            'tz': 'America/Mexico_City', 'prefijos': 'C',
        })
        self.empleado = self.env['hr.employee'].create({'name': 'Ciruelos 26', 'qb_checador_pin': '26'})
        self.url = '/qb_checador/hikvision/%s' % self.equipo.token

    def _post(self, cuerpo, ctype='application/json', url=None):
        return self.url_open(url or self.url, data=cuerpo, headers={'Content-Type': ctype})

    def test_token_y_url(self):
        self.assertTrue(self.equipo.token and len(self.equipo.token) >= 20)
        self.assertTrue(self.equipo.url_push.endswith(self.url))
        self.assertEqual(self.equipo.ruta_push, self.url)
        zk = self.env['qb.checador.equipo'].create({'name': 'ZK', 'serial': 'ZK0001'})
        self.assertFalse(zk.token)
        self.assertFalse(zk.url_push)
        zk.marca = 'hikvision'
        self.assertTrue(zk.token)
        viejo = self.equipo.token
        self.equipo.action_nuevo_token()
        self.assertNotEqual(self.equipo.token, viejo)

    def test_evento_json(self):
        r = self._post(json.dumps(evento()).encode())
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json()['statusCode'], 1)
        c = self.env['qb.checada'].search([('equipo_id', '=', self.equipo.id)])
        self.assertEqual(len(c), 1)
        self.assertEqual(c.employee_id, self.empleado)
        self.assertEqual(c.source, 'isapi')
        self.assertEqual(c.state, 'nueva')
        self.assertEqual(c.timestamp, datetime(2026, 10, 13, 14, 59, 41))   # 08:59:41 en CDMX (UTC-6)
        self.assertEqual((c.workcode, c.status), ('38', 'checkIn'))
        self.equipo.invalidate_recordset()
        self.assertEqual(self.equipo.ip, '192.168.10.50')
        self.assertTrue(self.equipo.ultima_checada)
        self.assertEqual(self.equipo.estado_conexion, 'conectado')
        self.assertIn('1 checada(s) nueva(s)', self.equipo.ultimo_mensaje)
        self.assertIn('- cuenta:', self.equipo.ultimo_mensaje)
        # el reloj a veces manda dos eventos por la misma verificación: queda una
        self._post(json.dumps(evento()).encode())
        self.assertEqual(self.env['qb.checada'].search_count([('equipo_id', '=', self.equipo.id)]), 1)

    def test_evento_xml_y_multipart(self):
        self.assertEqual(self._post(XML, 'application/xml').status_code, 200)
        r = self.url_open(self.url, data={'event_log': json.dumps(evento(hora='2026-10-13T19:00:00-06:00'))},
                          files={'Picture': ('Picture.jpg', b'\xff\xd8\xff\xe0FOTO', 'image/jpeg')})
        self.assertEqual(r.status_code, 200)
        horas = sorted(self.env['qb.checada'].search([('equipo_id', '=', self.equipo.id)]).mapped('timestamp'))
        self.assertEqual(horas, [datetime(2026, 10, 14, 0, 2, 5), datetime(2026, 10, 14, 1, 0, 0)])

    def test_lo_que_no_es_checada_contesta_ok_sin_guardar(self):
        for e in (evento(pin=None), evento(minor=21, pin=None), {'eventType': 'heartBeat', 'dateTime': '2026-10-13T09:00:00'}):
            self.assertEqual(self._post(json.dumps(e).encode()).status_code, 200)
        self.assertEqual(self.url_open(self.url).status_code, 200)    # GET de prueba
        self.assertEqual(self.env['qb.checada'].search_count([('equipo_id', '=', self.equipo.id)]), 0)
        self.equipo.invalidate_recordset()
        self.assertTrue(self.equipo.ultima_conexion, 'la prueba de la escucha HTTP cuenta como conexión')
        self._post(json.dumps(evento(pin=None)).encode())
        self.equipo.invalidate_recordset()
        self.assertIn('sin número de empleado', self.equipo.ultimo_mensaje)
        self._post(b'algo que no es evento', 'text/plain')
        self.equipo.invalidate_recordset()
        self.assertIn('Cuerpo: algo que no es evento', self.equipo.ultimo_mensaje)

    def test_eventos_excluidos_por_parametro(self):
        self.env['ir.config_parameter'].sudo().set_param('qb_checador.hikvision_eventos_excluidos', '38, 39')
        self._post(json.dumps(evento(minor=38)).encode())
        self._post(json.dumps(evento(minor=75, hora='2026-10-13T09:05:00-06:00')).encode())
        self.assertEqual(self.env['qb.checada'].search([('equipo_id', '=', self.equipo.id)]).mapped('workcode'), ['75'])

    def test_token_desconocido_o_no_autorizado(self):
        with mute_logger('odoo.addons.qb_checador.controllers.hikvision'):
            r = self._post(json.dumps(evento()).encode(), url='/qb_checador/hikvision/' + 'x' * 24)
        self.assertEqual(r.status_code, 403)
        self.equipo.autorizado = False
        self.assertEqual(self._post(json.dumps(evento()).encode()).status_code, 403)
        self.assertEqual(self.env['qb.checada'].search_count([('equipo_id', '=', self.equipo.id)]), 0)

    def test_semaforo_hikvision_tolera_horas_sin_checar(self):
        self.equipo.ultima_conexion = fields.Datetime.now() - timedelta(hours=5)
        self.assertEqual(self.equipo.estado_conexion, 'conectado')
        self.equipo.ultima_conexion = fields.Datetime.now() - timedelta(days=2)
        self.assertEqual(self.equipo.estado_conexion, 'sin_conexion')
