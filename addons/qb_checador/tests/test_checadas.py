# -*- coding: utf-8 -*-
"""Entrada de checadas y emparejamiento en asistencias, con horas reales de la planta
(turnos de 12 horas, horario de Toluca)."""
from datetime import datetime

from odoo.tests.common import TransactionCase
from odoo.tools import mute_logger


class TestChecadas(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.env['ir.config_parameter'].sudo().set_param('qb_checador.minutos_duplicado', '2')
        cls.env['ir.config_parameter'].sudo().set_param('qb_checador.horas_turno_max', '16')
        cls.equipo = cls.env['qb.checador.equipo'].create({
            'name': 'Entrada planta', 'serial': 'TEST0001', 'tz': 'America/Mexico_City', 'prefijos': 'S',
        })
        # Calendario flexible: las horas trabajadas son la diferencia entre entrada y salida, sin
        # descontar la comida del calendario. Es como se pagan los turnos de 12 horas de la planta.
        cls.calendario = cls.env['resource.calendar'].create({
            'name': 'Planta 12 horas (flexible)', 'flexible_hours': True, 'tz': 'America/Mexico_City',
        })
        # La Referencia de empleado la agrega hr_payroll (Enterprise). En el CI (Community) no existe:
        # ahí «Por referencia» se liga por usuario capturado y las pruebas de referencia se saltan.
        cls.tiene_referencia = 'registration_number' in cls.env['hr.employee']._fields
        vals = {'name': 'Por referencia', 'resource_calendar_id': cls.calendario.id}
        if cls.tiene_referencia:
            vals['registration_number'] = 'S-12'
        else:
            vals['qb_checador_pin'] = '12'
        cls.por_referencia = cls.env['hr.employee'].create(vals)
        vals = {'name': 'Por pin', 'qb_checador_pin': '77', 'resource_calendar_id': cls.calendario.id}
        if cls.tiene_referencia:
            vals['registration_number'] = 'S-99'     # el usuario capturado gana sobre la referencia
        cls.por_pin = cls.env['hr.employee'].create(vals)
        cls.Checada = cls.env['qb.checada']
        cls.Att = cls.env['hr.attendance']

    def _attlog(self, *lineas):
        return self.Checada.procesar_attlog(self.equipo, '\n'.join(lineas) + '\n')

    def _asistencias(self, empleado):
        return self.Att.search([('employee_id', '=', empleado.id)], order='check_in')

    # ------------------------------------------------------------------
    def test_attlog_entra_y_resuelve_empleado(self):
        n = self._attlog('12\t2026-10-05 06:58:10\t0\t1\t0',
                         '77\t2026-10-05 07:01:00\t1\t15\t0',
                         '555\t2026-10-05 07:02:00\t0\t1\t0')
        self.assertEqual(n, 3)
        c12 = self.Checada.search([('pin', '=', '12')])
        self.assertEqual(c12.employee_id, self.por_referencia,
                         'S-12 se liga por la referencia con el prefijo (o por el usuario capturado)')
        # 06:58:10 en Toluca (UTC-6 en octubre) = 12:58:10 UTC
        self.assertEqual(c12.timestamp, datetime(2026, 10, 5, 12, 58, 10))
        self.assertEqual(c12.state, 'nueva')
        self.assertEqual(c12.verify, '1')
        c77 = self.Checada.search([('pin', '=', '77')])
        self.assertEqual(c77.employee_id, self.por_pin, 'el usuario capturado en la ficha gana')
        c555 = self.Checada.search([('pin', '=', '555')])
        self.assertFalse(c555.employee_id)
        self.assertEqual(c555.state, 'sin_empleado')

    def test_repetidas_no_se_duplican(self):
        self._attlog('12\t2026-10-05 06:58:10\t0\t1\t0')
        self._attlog('12\t2026-10-05 06:58:10\t0\t1\t0', '12\t2026-10-05 06:58:10\t0\t1\t0')
        self.assertEqual(self.Checada.search_count([('pin', '=', '12')]), 1)

    def test_pin_con_ceros_a_la_izquierda(self):
        if not self.tiene_referencia:
            self.skipTest('sin hr_payroll no hay Referencia de empleado')
        self._attlog('0012\t2026-10-05 06:58:10\t0\t1\t0')
        self.assertEqual(self.Checada.search([('pin', '=', '0012')]).employee_id, self.por_referencia)

    def test_referencia_ambigua_no_se_liga(self):
        if not self.tiene_referencia:
            self.skipTest('sin hr_payroll no hay Referencia de empleado')
        self.env['hr.employee'].create({'name': 'Quincenal 12', 'registration_number': 'Q-12'})
        self.equipo.prefijos = 'S,Q'
        self._attlog('12\t2026-10-05 06:58:10\t0\t1\t0')
        c = self.Checada.search([('pin', '=', '12')])
        self.assertFalse(c.employee_id, 'dos candidatos = nadie: mejor sin empleado que en la persona equivocada')
        self.assertEqual(c.state, 'sin_empleado')

    def test_tabla_de_usuarios_del_reloj(self):
        """Un reloj que numera distinto que la nómina (Ciruelos): la tabla del reloj gana sobre la
        referencia y el usuario capturado, solo vale para ese reloj y religa lo que quedó sin empleado."""
        otro = self.env['qb.checador.equipo'].create({
            'name': 'Ciruelos', 'serial': 'TEST0002', 'tz': 'America/Mexico_City', 'prefijos': 'S'})
        self.Checada.procesar_attlog(otro, '12\t2026-10-05 08:00:00\t0\t1\t0\n'
                                           '5\t2026-10-05 08:01:00\t0\t1\t0\n')
        c5 = self.Checada.search([('equipo_id', '=', otro.id), ('pin', '=', '5')])
        self.assertEqual(c5.state, 'sin_empleado')
        self.env['qb.checador.usuario'].create([
            {'equipo_id': otro.id, 'pin': ' 5 ', 'nombre_reloj': 'Juan', 'employee_id': self.por_pin.id},
            {'equipo_id': otro.id, 'pin': '12', 'employee_id': self.por_pin.id},
        ])
        self.assertEqual((c5.employee_id, c5.state), (self.por_pin, 'nueva'), 'se religa al capturar la fila')
        self._attlog('12\t2026-10-05 09:00:00\t0\t1\t0')
        en_toluca = self.Checada.search([('equipo_id', '=', self.equipo.id), ('pin', '=', '12')])
        self.assertEqual(en_toluca.employee_id, self.por_referencia, 'la tabla de Ciruelos no toca a Toluca')
        self.Checada.procesar_attlog(otro, '12\t2026-10-05 18:00:00\t1\t1\t0\n')
        nueva = self.Checada.search([('equipo_id', '=', otro.id), ('pin', '=', '12'),
                                     ('timestamp', '=', datetime(2026, 10, 6, 0, 0, 0))])
        self.assertEqual(nueva.employee_id, self.por_pin, 'la tabla gana sobre la referencia S-12')

    # ------------------------------------------------------------------
    def test_turno_de_dia_y_checada_doble(self):
        self._attlog('12\t2026-10-05 06:58:10\t0\t1\t0',
                     '12\t2026-10-05 06:59:05\t0\t1\t0',      # checó dos veces
                     '12\t2026-10-05 19:02:40\t1\t1\t0')
        self.Checada._cron_emparejar()
        atts = self._asistencias(self.por_referencia)
        self.assertEqual(len(atts), 1)
        self.assertEqual(atts.check_in, datetime(2026, 10, 5, 12, 58, 10))
        self.assertEqual(atts.check_out, datetime(2026, 10, 6, 1, 2, 40))
        self.assertEqual(atts.in_mode, 'checador')
        self.assertEqual(atts.out_mode, 'checador')
        self.assertAlmostEqual(atts.worked_hours, 12.075, places=2)
        estados = {c.raw.split('\t')[1]: c.state for c in self.Checada.search([('pin', '=', '12')])}
        self.assertEqual(estados['2026-10-05 06:58:10'], 'emparejada')
        self.assertEqual(estados['2026-10-05 06:59:05'], 'ignorada')
        self.assertEqual(estados['2026-10-05 19:02:40'], 'emparejada')
        self.assertEqual(atts.qb_checada_in_id.raw.split('\t')[1], '2026-10-05 06:58:10')
        self.assertEqual(atts.qb_checada_out_id.raw.split('\t')[1], '2026-10-05 19:02:40')

    def test_turno_de_noche_cruza_medianoche(self):
        self._attlog('12\t2026-10-05 18:55:00\t0\t1\t0',
                     '12\t2026-10-06 07:05:00\t1\t1\t0')
        self.Checada._cron_emparejar()
        atts = self._asistencias(self.por_referencia)
        self.assertEqual(len(atts), 1)
        self.assertAlmostEqual(atts.worked_hours, 12.1667, places=2)

    def test_entrada_sin_salida_queda_huerfana_y_en_cero(self):
        self._attlog('12\t2026-10-05 06:58:00\t0\t1\t0')
        self.Checada._cron_emparejar()
        # al día siguiente vuelve a checar entrada: la de ayer nunca tuvo salida
        self._attlog('12\t2026-10-06 06:57:00\t0\t1\t0', '12\t2026-10-06 19:01:00\t1\t1\t0')
        self.Checada._cron_emparejar()
        atts = self._asistencias(self.por_referencia)
        self.assertEqual(len(atts), 2)
        ayer, hoy = atts
        self.assertAlmostEqual(ayer.worked_hours, 1 / 60, places=3, msg='se cierra en cero horas, nunca se inventan')
        self.assertEqual(ayer.out_mode, 'technical')
        self.assertEqual(ayer.qb_checada_in_id.state, 'huerfana')
        self.assertAlmostEqual(hoy.worked_hours, 12.0667, places=2)

    def test_checada_que_llega_tarde_dentro_de_asistencia_cerrada(self):
        self._attlog('12\t2026-10-05 06:58:00\t0\t1\t0', '12\t2026-10-05 19:00:00\t1\t1\t0')
        self.Checada._cron_emparejar()
        self._attlog('12\t2026-10-05 12:00:00\t0\t1\t0')   # salió a comer; llegó después
        self.Checada._cron_emparejar()
        tarde = self.Checada.search([('pin', '=', '12'), ('raw', 'like', '12:00:00')])
        self.assertEqual(tarde.state, 'huerfana')
        self.assertEqual(len(self._asistencias(self.por_referencia)), 1)

    def test_las_de_un_empleado_no_tumban_a_los_demas(self):
        # una asistencia abierta a mano con check_in posterior a la checada provoca un ValidationError
        # al emparejar a ese empleado; el otro debe quedar emparejado igual.
        self.Att.create({'employee_id': self.por_referencia.id, 'check_in': datetime(2026, 10, 5, 20, 0, 0)})
        self._attlog('12\t2026-10-05 06:58:00\t0\t1\t0', '77\t2026-10-05 06:58:00\t0\t1\t0',
                     '77\t2026-10-05 19:00:00\t1\t1\t0')
        with mute_logger('odoo.addons.qb_checador.models.checada'):
            self.Checada._cron_emparejar()
        self.assertEqual(len(self._asistencias(self.por_pin)), 1)
        c12 = self.Checada.search([('pin', '=', '12')])
        self.assertIn(c12.state, ('huerfana', 'error'))

    def test_reintentar_resuelve_empleado_despues(self):
        self._attlog('555\t2026-10-05 06:58:00\t0\t1\t0')
        c = self.Checada.search([('pin', '=', '555')])
        self.assertEqual(c.state, 'sin_empleado')
        self.por_pin.qb_checador_pin = '555'
        c.action_reintentar()
        self.assertEqual(c.employee_id, self.por_pin)
        self.assertEqual(c.state, 'nueva')

    # ------------------------------------------------------------------
    def test_checadas_desde_ignora_el_historial(self):
        """El UA860 mandó checadas de 2024 al conectarlo: lo anterior a «Checadas desde» no es asistencia."""
        self.equipo.checadas_desde = datetime(2026, 10, 5, 12, 0, 0)      # 06:00 en Toluca
        self._attlog('12\t2024-05-28 15:16:17\t0\t1\t0', '12\t2024-05-28 23:00:00\t1\t1\t0',
                     '555\t2024-05-28 15:20:00\t0\t1\t0',
                     '12\t2026-10-05 06:58:00\t0\t1\t0', '12\t2026-10-05 19:00:00\t1\t1\t0')
        viejas = self.Checada.search([('equipo_id', '=', self.equipo.id), ('raw', 'like', '2024-05-28')])
        self.assertEqual(len(viejas), 3)
        self.assertEqual(set(viejas.mapped('state')), {'ignorada'})
        self.Checada._cron_emparejar()
        asistencias = self._asistencias(self.por_referencia)
        self.assertEqual(len(asistencias), 1, 'solo la jornada posterior al corte')
        self.assertEqual(asistencias.check_in, datetime(2026, 10, 5, 12, 58, 0))
        # Ligar el usuario 555 en la tabla del reloj no revive su checada vieja.
        self.env['qb.checador.usuario'].create({'equipo_id': self.equipo.id, 'pin': '555',
                                                'employee_id': self.por_pin.id})
        vieja_555 = self.Checada.search([('pin', '=', '555')])
        self.assertEqual(vieja_555.state, 'ignorada')
        vieja_555.action_reintentar()
        self.assertEqual(vieja_555.state, 'ignorada')

    def test_poner_checadas_desde_limpia_lo_ya_emparejado(self):
        """Si el historial ya se emparejó, poner el corte borra esas asistencias y deja lo posterior."""
        self._attlog('12\t2024-05-28 06:58:00\t0\t1\t0', '12\t2024-05-28 19:00:00\t1\t1\t0',
                     '12\t2026-10-05 06:58:00\t0\t1\t0', '12\t2026-10-05 19:00:00\t1\t1\t0')
        self.Checada._cron_emparejar()
        self.assertEqual(len(self._asistencias(self.por_referencia)), 2)
        self.equipo.checadas_desde = datetime(2026, 10, 1)
        asistencias = self._asistencias(self.por_referencia)
        self.assertEqual(len(asistencias), 1)
        self.assertEqual(asistencias.check_in, datetime(2026, 10, 5, 12, 58, 0))
        viejas = self.Checada.search([('pin', '=', '12'), ('raw', 'like', '2024-05-28')])
        self.assertEqual(set(viejas.mapped('state')), {'ignorada'})
        self.assertFalse(viejas.attendance_id)

    def test_api_del_puente(self):
        r = self.Checada.ingresar_api('TEST0001', [
            {'pin': '12', 'time': '2026-10-05 06:58:10', 'status': 0, 'verify': 1},
            {'pin': '12', 'time': '2026-10-05 06:58:10', 'status': 0, 'verify': 1},
        ])
        self.assertEqual(r, {'creadas': 1, 'repetidas': 1})
        c = self.Checada.search([('pin', '=', '12')])
        self.assertEqual(c.source, 'api')
        self.assertEqual(c.timestamp, datetime(2026, 10, 5, 12, 58, 10))
        self.assertTrue(self.equipo.ultima_checada)

    def test_estado_conexion(self):
        """Semáforo del reloj: nunca → conectado (se acaba de presentar) → sin conexión (callado > 2 min)."""
        from datetime import timedelta
        nuevo = self.env['qb.checador.equipo'].create({'name': 'Nuevo', 'serial': 'TEST0002'})
        self.assertEqual(nuevo.estado_conexion, 'nunca')
        self.assertEqual(nuevo.conexion_hace, 'nunca')
        nuevo._tocar(firmware='PUSH 3.0.6')
        self.assertEqual(nuevo.estado_conexion, 'conectado')
        self.assertEqual(nuevo.conexion_hace, 'hace menos de 1 minuto')
        from odoo import fields as ofields
        nuevo.ultima_conexion = ofields.Datetime.now() - timedelta(minutes=7)
        self.assertEqual(nuevo.estado_conexion, 'sin_conexion')
        self.assertEqual(nuevo.conexion_hace, 'hace 7 min')
        nuevo.ultima_conexion = ofields.Datetime.now() - timedelta(days=3, hours=1)
        self.assertEqual(nuevo.conexion_hace, 'hace 3 días')

    def test_api_rechaza_equipo_desconocido(self):
        from odoo.exceptions import ValidationError
        with self.assertRaises(ValidationError):
            self.Checada.ingresar_api('NOEXISTE', [{'pin': '1', 'time': '2026-10-05 06:58:10'}])
        self.equipo.autorizado = False
        with self.assertRaises(ValidationError):
            self.Checada.ingresar_api('TEST0001', [{'pin': '1', 'time': '2026-10-05 06:58:10'}])
