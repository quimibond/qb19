# -*- coding: utf-8 -*-
"""Usuarios y huellas entre Odoo y los relojes ZKTeco: lo que manda el reloj, la cola de comandos,
el número de checador por nómina y el paso de una huella de un reloj a los demás."""
from odoo.tests import tagged
from odoo.tests.common import HttpCase, TransactionCase

from odoo.addons.qb_checador.models.sincronizacion import (huella_de_registro, registros_de_reloj,
                                                            usuario_de_registro)
from odoo.addons.qb_checador.models.usuario import nombre_plano, puntaje_nombre

PLANTILLA = 'TVpTUzIxAAAEFBQECAUHCc7QAAAoFXYBAAAAhNMn+xRCAI8PqgCaAGob/g=='


class TestLectura(TransactionCase):

    def test_lineas_del_reloj(self):
        cuerpo = ('USER PIN=308012016\tName=MARIANO DOMINGUEZ\tPri=14\tPasswd=\tCard=\tGrp=1\n'
                  'FP PIN=308012016\tFID=6\tSize=1384\tValid=1\tTMP=%s\n'
                  'OPLOG 4\t0\t2026-10-19 08:00:00\t0\t0\t0\t0\n'
                  'BIODATA Pin=1004\tNo=3\tIndex=0\tValid=1\tDuress=0\tType=1\tMajorVer=10\tMinorVer=0\t'
                  'Format=0\tTmp=%s\n'
                  'BIODATA Pin=1004\tNo=0\tIndex=0\tValid=1\tDuress=0\tType=9\tMajorVer=5\tMinorVer=8\tTmp=QUJD\n'
                  % (PLANTILLA, PLANTILLA))
        registros = registros_de_reloj(cuerpo)
        self.assertEqual([t for t, _d in registros], ['USER', 'FP', 'BIODATA', 'BIODATA'], 'OPLOG se ignora')
        self.assertEqual(usuario_de_registro(registros[0][1]),
                         {'pin': '308012016', 'nombre': 'MARIANO DOMINGUEZ', 'privilegio': '14'})
        fp = huella_de_registro(*registros[1])
        self.assertEqual((fp['pin'], fp['dedo'], fp['tamano'], fp['formato']), ('308012016', 6, 1384, 'fingertmp'))
        self.assertEqual(fp['plantilla'], PLANTILLA, 'el «==» del base64 no se pierde')
        bio = huella_de_registro(*registros[2])
        self.assertEqual((bio['pin'], bio['dedo'], bio['formato'], bio['version']), ('1004', 3, 'biodata', '10.0'))
        self.assertIsNone(huella_de_registro(*registros[3]), 'Type=9 (rostro) no es huella digital')

    def test_nombres(self):
        self.assertEqual(nombre_plano('José Ñúñez-Peña'), 'JOSE NUNEZ PENA')
        self.assertEqual(puntaje_nombre('MARIANO DOMINGUEZ', 'MARIANO DOMINGUEZ CASTILLO'), 1.0)
        self.assertEqual(puntaje_nombre('J LUIS ALMAZAN', 'JOSE LUIS ALMAZAN DURAN'), 0.83)
        self.assertLess(puntaje_nombre('PEDRO', 'MARIANO DOMINGUEZ CASTILLO'), 0.5)


class TestNumeroDeChecador(TransactionCase):

    def setUp(self):
        super().setUp()
        if 'registration_number' not in self.env['hr.employee']._fields:
            self.skipTest('sin hr_payroll no hay Referencia de empleado')

    def test_numero_por_nomina(self):
        Emp = self.env['hr.employee']
        s4 = Emp.create({'name': 'Semanal Cuatro', 'registration_number': 'S-4'})
        q109 = Emp.create({'name': 'Quincenal Ciento Nueve', 'registration_number': 'Q-109'})
        c26 = Emp.create({'name': 'Mexico Veintiseis', 'registration_number': 'C-26'})
        raro = Emp.create({'name': 'Sin Nomina', 'registration_number': 'X-1'})
        self.assertEqual([e._qb_numero_checador() for e in (s4, q109, c26, raro)], ['1004', '2109', '3026', ''])
        self.assertEqual(Emp._qb_referencia_de_numero('1004'), 'S-4')
        self.assertEqual(Emp._qb_referencia_de_numero('2109'), 'Q-109')
        self.assertIsNone(Emp._qb_referencia_de_numero('109'))
        self.assertIsNone(Emp._qb_referencia_de_numero('9004'))
        (s4 | q109 | c26 | raro)._qb_asignar_numero_checador()
        self.assertEqual((s4.qb_checador_pin, q109.qb_checador_pin, raro.qb_checador_pin), ('1004', '2109', False))


@tagged('post_install', '-at_install')
class TestSincronizacion(HttpCase):

    def setUp(self):
        super().setUp()
        Equipo = self.env['qb.checador.equipo']
        self.rama = Equipo.create({'name': 'Rama', 'serial': 'UDPRAMA01', 'prefijos': 'S,Q',
                                   'sincronizar_usuarios': True})
        self.entretelas = Equipo.create({'name': 'Entretelas', 'serial': 'UDPENTR01', 'prefijos': 'S,Q',
                                         'sincronizar_usuarios': True})
        self.viejo = Equipo.create({'name': 'UA860', 'serial': 'UA860VIEJO', 'prefijos': 'S,Q'})
        self.tiene_referencia = 'registration_number' in self.env['hr.employee']._fields
        # Nombres y números que no existen en la base de producción (el build de Odoo.sh corre sobre una copia).
        vals = {'name': 'MARIANO PRUEBAZQ CHEC'}
        if self.tiene_referencia:
            vals['registration_number'] = 'Q-987'
        else:
            vals['qb_checador_pin'] = '2987'
        self.mariano = self.env['hr.employee'].create(vals)
        if self.tiene_referencia:
            self.mariano._qb_asignar_numero_checador()
        self.assertEqual(self.mariano.qb_checador_pin, '2987')

    def _post(self, serie, tabla, cuerpo, stamp=None):
        url = '/iclock/cdata?SN=%s&table=%s' % (serie, tabla) + ('&Stamp=%s' % stamp if stamp else '')
        return self.url_open(url, data=cuerpo.encode('utf-8'), headers={'Content-Type': 'text/plain'})

    def test_reloj_viejo_manda_usuarios_y_se_sugiere_por_nombre(self):
        r = self._post('UA860VIEJO', 'OPERLOG',
                       'USER PIN=990000001\tName=MARIANO PRUEBAZQ\tPri=0\n'
                       'FP PIN=990000001\tFID=6\tSize=60\tValid=1\tTMP=%s\n' % PLANTILLA, stamp='555')
        self.assertEqual(r.status_code, 200)
        usuario = self.env['qb.checador.usuario'].search([('equipo_id', '=', self.viejo.id)])
        self.assertEqual((usuario.pin, usuario.nombre_reloj, usuario.origen), ('990000001', 'MARIANO PRUEBAZQ', 'reloj'))
        self.assertFalse(usuario.employee_id, 'en un reloj con su propia numeración no se adivina')
        self.assertEqual(usuario.sugerido_id, self.mariano)
        huella = self.env['qb.checador.huella'].search([('equipo_id', '=', self.viejo.id)])
        self.assertEqual((huella.dedo, huella.plantilla, huella.employee_id.id), (6, PLANTILLA, False))
        self.viejo.invalidate_recordset()
        self.assertEqual(self.viejo.operlog_stamp, '555')
        # RH acepta la sugerencia: la huella pasa a ser de Mariano
        self.viejo.action_aceptar_sugerencias()
        self.assertEqual(usuario.employee_id, self.mariano)
        self.assertEqual(huella.employee_id, self.mariano)
        # y al sincronizar Rama, va su alta con el número nuevo y su huella del UA860
        self.rama.action_sincronizar_usuarios()
        comandos = self.env['qb.checador.comando'].search([('equipo_id', '=', self.rama.id)], order='id')
        self.assertIn('DATA UPDATE USERINFO PIN=2987\tName=MARIANO PRUEBAZQ CHEC\t', comandos[0].comando)
        self.assertTrue(any(c.comando == 'DATA UPDATE FINGERTMP PIN=2987\tFID=6\tSize=60\tValid=1\tTMP=%s' % PLANTILLA
                            for c in comandos))

    def test_cola_getrequest_y_devicecmd(self):
        self.env['qb.checador.comando'].search([]).unlink()
        self.rama._encolar('DATA UPDATE USERINFO PIN=2109\tName=MARIANO', 'Alta de prueba', self.mariano)
        self.rama._encolar('DATA UPDATE USERINFO PIN=2109\tName=MARIANO', 'Repetido')   # no se duplica
        self.assertEqual(self.rama.comandos_pendientes, 1)
        r = self.url_open('/iclock/getrequest?SN=UDPRAMA01')
        comando = self.env['qb.checador.comando'].search([('equipo_id', '=', self.rama.id)])
        self.assertEqual(r.text, 'C:%d:DATA UPDATE USERINFO PIN=2109\tName=MARIANO\n' % comando.id)
        self.assertEqual((comando.state, comando.intentos), ('enviado', 1))
        r = self.url_open('/iclock/getrequest?SN=UDPRAMA01')
        self.assertEqual(r.text, 'OK\r\n', 'lo enviado no se repite antes de 10 minutos')
        r = self.url_open('/iclock/devicecmd?SN=UDPRAMA01', data=('ID=%d&Return=0&CMD=DATA\n' % comando.id).encode(),
                          headers={'Content-Type': 'text/plain'})
        self.assertEqual(r.text, 'OK\r\n')
        comando.invalidate_recordset()
        self.assertEqual(comando.state, 'ok')
        # un ID de otro reloj no se toca
        otro = self.entretelas._encolar('CHECK', 'otro')
        self.url_open('/iclock/devicecmd?SN=UDPRAMA01', data=('ID=%d&Return=-1&CMD=CHECK' % otro.id).encode(),
                      headers={'Content-Type': 'text/plain'})
        otro.invalidate_recordset()
        self.assertEqual(otro.state, 'pendiente')

    def test_huella_nueva_pasa_a_los_otros_relojes(self):
        # Mariano registra su huella en Rama con su número nuevo: Entretelas la recibe sola.
        r = self._post('UDPRAMA01', 'BIODATA', 'BIODATA Pin=2987\tNo=3\tIndex=0\tValid=1\tDuress=0\tType=1\t'
                                               'MajorVer=10\tMinorVer=0\tFormat=0\tTmp=%s\n' % PLANTILLA)
        self.assertEqual(r.status_code, 200)
        huella = self.env['qb.checador.huella'].search([('equipo_id', '=', self.rama.id)])
        self.assertEqual(huella.employee_id, self.mariano)
        self.rama.invalidate_recordset()
        self.assertEqual(self.rama.formato_huella, 'biodata', 'el formato se aprende de lo que manda el reloj')
        a_entretelas = self.env['qb.checador.comando'].search([('equipo_id', '=', self.entretelas.id)])
        self.assertTrue(any('PIN=2987' in c.comando and 'USERINFO' in c.comando for c in a_entretelas))
        self.assertTrue(any(c.comando.startswith('DATA UPDATE FINGERTMP PIN=2987\tFID=3\t') for c in a_entretelas))
        self.assertFalse(self.env['qb.checador.comando'].search([('equipo_id', '=', self.viejo.id)]),
                         'al UA860 (no sincroniza) no se le manda nada')

    def test_cambio_de_numero_borra_el_viejo(self):
        if self.tiene_referencia:
            self.mariano.registration_number = 'Q-985'   # cambió de número de nómina: el de checador lo sigue
            nuevo = '2985'
        else:
            self.mariano.qb_checador_pin = '2999'
            nuevo = '2999'
        self.assertEqual(self.mariano.qb_checador_pin, nuevo)
        comandos = self.env['qb.checador.comando'].search([('equipo_id', '=', self.rama.id)]).mapped('comando')
        self.assertIn('DATA DELETE USERINFO PIN=2987', comandos)
        self.assertTrue(any(c.startswith('DATA UPDATE USERINFO PIN=%s\t' % nuevo) for c in comandos))
        self.mariano.active = False
        comandos = self.env['qb.checador.comando'].search([('equipo_id', '=', self.entretelas.id)]).mapped('comando')
        self.assertIn('DATA DELETE USERINFO PIN=%s' % nuevo, comandos, 'al archivarlo se borra de los relojes')

    def test_ingreso_nuevo_entra_solo_a_los_relojes(self):
        if not self.tiene_referencia:
            self.skipTest('sin hr_payroll no hay Referencia de empleado')
        nuevo = self.env['hr.employee'].create({'name': 'INGRESO PRUEBAZQ', 'registration_number': 'S-984'})
        self.assertEqual(nuevo.qb_checador_pin, '1984')
        Comando = self.env['qb.checador.comando']

        def cola(reloj):
            return Comando.search([('equipo_id', '=', reloj.id), ('comando', 'like', 'PIN=1984')], order='id').mapped(
                lambda c: c.comando.split('\t')[0])

        self.assertEqual(cola(self.rama), ['DATA UPDATE USERINFO PIN=1984'])
        self.assertEqual(cola(self.entretelas), ['DATA UPDATE USERINFO PIN=1984'])
        self.assertFalse(cola(self.viejo), 'el UA860 no sincroniza')
        # baja y reingreso antes de que el reloj recoja nada: la cola queda en orden (alta, baja, alta)
        nuevo.active = False
        nuevo.active = True
        self.assertEqual(cola(self.rama), ['DATA UPDATE USERINFO PIN=1984', 'DATA DELETE USERINFO PIN=1984',
                                           'DATA UPDATE USERINFO PIN=1984'])
        # y volver a sincronizar no repite lo que ya está por entregarse
        self.rama.action_sincronizar_usuarios()
        self.assertEqual(len(cola(self.rama)), 3)

    def test_checada_con_numero_de_cuatro_digitos(self):
        if not self.tiene_referencia:
            self.skipTest('sin hr_payroll no hay Referencia de empleado')
        otro = self.env['hr.employee'].create({'name': 'Semanal Prueba', 'registration_number': 'S-986'})
        self._post('UDPRAMA01', 'ATTLOG', '1986\t2026-10-19 08:00:00\t0\t1\t0\n', stamp='1')
        checada = self.env['qb.checada'].search([('equipo_id', '=', self.rama.id), ('pin', '=', '1986')])
        self.assertEqual(checada.employee_id, otro, '1986 → S-986 aunque todavía no tenga su número asignado')

    def test_opciones_piden_usuarios_desde_la_marca(self):
        r = self.url_open('/iclock/cdata?SN=UA860VIEJO&options=all&pushver=2.4.1')
        self.assertIn('OPERLOGStamp=0\r\n', r.text, 'la primera vez manda todos sus usuarios y huellas')
        self.viejo.operlog_stamp = '777'
        r = self.url_open('/iclock/cdata?SN=UA860VIEJO&options=all&pushver=2.4.1')
        self.assertIn('OPERLOGStamp=777\r\n', r.text)
        self.viejo.action_pedir_usuarios()
        self.assertEqual(self.viejo.operlog_stamp, '0')
        r = self.url_open('/iclock/getrequest?SN=UA860VIEJO')
        self.assertIn(':CHECK\n', r.text)

    def test_relojes_asignados(self):
        """«Relojes donde checa»: Mariano solo en Rama. Se borra de Entretelas, sus huellas ya no pasan ahí
        y, al quitarle la asignación, vuelve con su alta."""
        Comando = self.env['qb.checador.comando']
        Comando.search([]).unlink()

        def cola(reloj):
            # Sin mapped(): en Odoo 19 llama a la función también con la cola vacía.
            comandos = Comando.search([('equipo_id', '=', reloj.id), ('comando', 'like', 'PIN=2987')], order='id')
            return [c.comando.split('\t')[0] for c in comandos]

        self.mariano.qb_checador_equipo_ids = self.rama
        self.assertEqual(cola(self.entretelas), ['DATA DELETE USERINFO PIN=2987'])
        self.assertFalse(cola(self.rama), 'ya estaba en Rama: no se repite el alta')
        self.assertNotIn(self.mariano, self.entretelas._empleados_a_sincronizar())
        self.assertIn(self.mariano, self.rama._empleados_a_sincronizar())
        # registra una huella en Rama: ya no pasa a Entretelas
        self._post('UDPRAMA01', 'BIODATA', 'BIODATA Pin=2987\tNo=3\tIndex=0\tValid=1\tDuress=0\tType=1\t'
                                           'MajorVer=10\tMinorVer=0\tFormat=0\tTmp=%s\n' % PLANTILLA)
        self.assertEqual(cola(self.entretelas), ['DATA DELETE USERINFO PIN=2987'])
        # sincronizar Entretelas a mano tampoco lo manda
        self.entretelas.action_sincronizar_usuarios()
        self.assertEqual(cola(self.entretelas), ['DATA DELETE USERINFO PIN=2987'])
        # vuelve a todos los relojes: alta y huella en Entretelas
        self.mariano.qb_checador_equipo_ids = False
        self.assertEqual(cola(self.entretelas), ['DATA DELETE USERINFO PIN=2987', 'DATA UPDATE USERINFO PIN=2987',
                                                 'DATA UPDATE FINGERTMP PIN=2987'])
