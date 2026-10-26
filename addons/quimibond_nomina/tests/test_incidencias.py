# -*- coding: utf-8 -*-
"""Lista de incidencias semanal desde el checador (F-P-A01-49).

Las reglas (``incidencias_calculo``) se prueban sin base; ``_generar`` con un
reloj, checadas y un contrato semanal de prueba.

No corre en el CI (depende de Enterprise); se corre en el shell de Odoo.sh:
``odoo-bin ... --test-tags /quimibond_nomina --stop-after-init``."""
from datetime import date, datetime as dt, timedelta

from odoo.tests.common import TransactionCase

from ..models import incidencias_calculo as calc
from .common import liberar_referencias

LUNES = date(2026, 10, 12)
HORARIO = {0: 10.0, 1: 10.0, 2: 10.0, 3: 10.0, 4: 8.0}
HOY = date(2026, 10, 30)


def _dia(d, entrada, salida, comida=True):
    """Checadas de un turno de día: entrada, comida y salida."""
    marcas = [dt(2026, 10, d, *entrada)]
    if comida:
        marcas += [dt(2026, 10, d, 13, 0), dt(2026, 10, d, 13, 30)]
    marcas.append(dt(2026, 10, d, *salida))
    return marcas


def _semana_normal():
    return (_dia(12, (6, 55), (17, 2)) + _dia(13, (7, 0), (17, 0)) + _dia(14, (7, 3), (17, 0))
            + _dia(15, (6, 50), (17, 0)) + _dia(16, (6, 58), (15, 1)))


class TestIncidenciasCalculo(TransactionCase):

    def test_semana_normal(self):
        r = calc.calcular_semana(LUNES, HORARIO, _semana_normal(), hoy=HOY)
        self.assertEqual(r['dias'], ['10', '10', '10', '10', '8', '', ''])
        self.assertEqual(r['horas_base'], 48.0)
        self.assertEqual(r['faltas'], 0)
        # Retardo desde el minuto 1: el miércoles entró 07:03.
        self.assertEqual((r['retardos'], r['retardo_min']), (1, 3))
        self.assertEqual(r['he_propuestas'], 0)
        self.assertEqual(r['salidas_antes'], 0)

    def test_falta_quita_el_dia_y_dos_horas(self):
        marcas = [m for m in _semana_normal() if m.day != 13]
        r = calc.calcular_semana(LUNES, HORARIO, marcas, hoy=HOY)
        self.assertEqual(r['dias'][1], '0')
        self.assertEqual(r['faltas'], 1)
        self.assertEqual(r['falta_horas'], 12.0)
        self.assertEqual(r['horas_base'], 38.0)

    def test_falta_en_viernes_sin_castigo(self):
        marcas = [m for m in _semana_normal() if m.day != 16]
        r = calc.calcular_semana(LUNES, HORARIO, marcas, hoy=HOY)
        self.assertEqual(r['falta_horas'], 8.0)

    def test_turno_de_noche_y_prima_dominical(self):
        marcas = []
        for d in (12, 13, 14, 15, 16):
            marcas += [dt(2026, 10, d, 18, 55), dt(2026, 10, d + 1, 1, 0), dt(2026, 10, d + 1, 1, 30),
                       dt(2026, 10, d + 1, 7, 1)]
        # Entra el domingo en la noche: el turno es del domingo y lleva prima.
        marcas += [dt(2026, 10, 18, 18, 58), dt(2026, 10, 19, 1, 0), dt(2026, 10, 19, 7, 0)]
        r = calc.calcular_semana(LUNES, HORARIO, marcas, hoy=HOY)
        self.assertEqual(r['dias'], ['10', '10', '10', '10', '8', '', '12'])
        self.assertEqual(r['faltas'], 0)
        self.assertEqual(r['retardos'], 0)
        self.assertEqual(r['salidas_antes'], 0)
        self.assertEqual(r['he_propuestas'], 24)
        self.assertEqual(r['dias_con_extra'], 6)
        self.assertEqual(r['dom_horas'], 12.0)

    def test_noche_del_sabado_lleva_prima(self):
        marcas = (_dia(12, (7, 0), (17, 0)) + _dia(13, (7, 0), (17, 0)) + _dia(14, (7, 0), (17, 0))
                  + _dia(15, (7, 0), (17, 0)) + _dia(16, (7, 0), (15, 0))
                  + [dt(2026, 10, 17, 19, 0), dt(2026, 10, 18, 1, 0), dt(2026, 10, 18, 7, 0)])
        r = calc.calcular_semana(LUNES, HORARIO, marcas, hoy=HOY)
        self.assertEqual(r['dias'][5], '12')
        self.assertEqual(r['dom_horas'], 12.0)
        self.assertEqual(r['he_propuestas'], 12)

    def test_salida_antes_se_anota_sin_descontar(self):
        marcas = [m for m in _semana_normal() if m.day != 13] + _dia(13, (7, 0), (15, 30))
        r = calc.calcular_semana(LUNES, HORARIO, marcas, hoy=HOY)
        self.assertEqual((r['salidas_antes'], r['salida_min']), (1, 90))
        self.assertEqual(r['horas_base'], 48.0)
        self.assertEqual(r['faltas'], 0)

    def test_sin_checadas_no_marca_faltas(self):
        r = calc.calcular_semana(LUNES, HORARIO, [], hoy=HOY)
        self.assertEqual(r['dias'][:5], ['?'] * 5)
        self.assertEqual(r['faltas'], 0)
        self.assertTrue(r['sin_checadas'])

    def test_incapacidad_y_vacaciones(self):
        ausencias = {LUNES + timedelta(days=i): ('I', 0.0) for i in range(3)}
        ausencias[LUNES + timedelta(days=3)] = ('V', 0.0)
        r = calc.calcular_semana(LUNES, HORARIO, _dia(16, (7, 0), (15, 0)), ausencias, hoy=HOY)
        self.assertEqual(r['dias'][:5], ['I', 'I', 'I', 'V', '8'])
        self.assertEqual(r['incap_dias'], 3)
        self.assertEqual(r['horas_base'], 18.0)
        self.assertEqual(r['faltas'], 0)

    def test_permiso_sin_goce_quita_horas_sin_castigo(self):
        marcas = [m for m in _semana_normal() if m.day != 13]
        r = calc.calcular_semana(LUNES, HORARIO, marcas, {LUNES + timedelta(days=1): ('SG', 10.0)}, hoy=HOY)
        self.assertEqual(r['faltas'], 0)
        self.assertEqual(r['horas_sin_goce'], 10.0)
        self.assertEqual(r['falta_horas'], 0.0)

    def test_ingreso_a_media_semana_y_dias_proyectados(self):
        r = calc.calcular_semana(LUNES, HORARIO, _dia(14, (7, 0), (17, 0)),
                                 contrato_desde=date(2026, 10, 14), hoy=date(2026, 10, 14))
        self.assertEqual(r['dias'][:5], ['—', '—', '10', '10', '8'])
        self.assertEqual(r['horas_base'], 28.0)
        self.assertTrue(r['proyectado'])
        self.assertEqual(r['faltas'], 0)

    def test_reparto_de_horas_extra(self):
        self.assertEqual(calc.repartir_horas_extra(24, 6), (9, 21))
        self.assertEqual(calc.repartir_horas_extra(5), (5, 0))
        self.assertEqual(calc.repartir_horas_extra(0, 4), (0, 4))

    def test_noche_sin_checar_a_la_comida_es_un_turno(self):
        marcas = [dt(2026, 10, 12, 18, 58), dt(2026, 10, 12, 18, 59),     # checó dos veces al entrar
                  dt(2026, 10, 13, 7, 2),
                  dt(2026, 10, 13, 18, 57), dt(2026, 10, 14, 7, 0)]
        turnos = calc.agrupar_turnos(marcas)
        self.assertEqual([(t[0], t[1]) for t in turnos],
                         [(dt(2026, 10, 12, 18, 58), dt(2026, 10, 13, 7, 2)),
                          (dt(2026, 10, 13, 18, 57), dt(2026, 10, 14, 7, 0))])

    def test_salida_olvidada_no_se_pega_al_dia_siguiente(self):
        marcas = [dt(2026, 10, 12, 7, 0), dt(2026, 10, 12, 13, 0), dt(2026, 10, 12, 13, 30),
                  dt(2026, 10, 13, 6, 58), dt(2026, 10, 13, 17, 0)]
        self.assertEqual(len(calc.agrupar_turnos(marcas)), 2)

    def test_premio_de_asistencia(self):
        self.assertEqual(calc.premio_asistencia(0, 0), 5.5)
        self.assertEqual(calc.premio_asistencia(0, 2), 4.5)
        self.assertEqual(calc.premio_asistencia(1, 0), 4.5)
        self.assertEqual(calc.premio_asistencia(0, 3), 0.0)
        self.assertEqual(calc.premio_asistencia(2, 0), 0.0)
        self.assertEqual(calc.premio_asistencia(0, 1), 4.5, 'un retardo: 4.5 mientras RH no diga otra cosa')
        self.assertEqual(calc.premio_asistencia(0, 1, pct_un_retardo=5.5), 5.5)

    def test_entradas_del_parametro(self):
        self.assertEqual(calc.leer_entradas('06:00, 18:00'), (dt(1, 1, 1, 6).time(), dt(1, 1, 1, 18).time()))
        self.assertEqual(calc.leer_entradas('basura'), calc.ENTRADAS_DEFAULT)
        self.assertEqual(calc.leer_entradas(''), calc.ENTRADAS_DEFAULT)


class TestIncidenciasGenerar(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        env = cls.env
        cls.calendario = env['resource.calendar'].create({
            'name': 'Planta prueba incidencias', 'tz': 'America/Mexico_City',
            'attendance_ids': [(5, 0, 0)] + [
                (0, 0, {'name': 'Día %s' % d, 'dayofweek': str(d), 'hour_from': 7.0,
                        'hour_to': 17.0 if d < 4 else 15.0, 'day_period': 'morning'})
                for d in range(5)],
        })
        liberar_referencias(env, 'Q-901', 'Q-902')
        cls.empleado = env['hr.employee'].create({'name': 'Prueba Incidencias', 'registration_number': 'Q-901'})
        cls.empleado.version_id.write({
            'schedule_pay': 'weekly', 'contract_date_start': date(2026, 1, 1),
            'resource_calendar_id': cls.calendario.id,
        })
        cls.equipo = env['qb.checador.equipo'].create({'name': 'Reloj prueba incidencias',
                                                       'serial': 'QBPRUEBAINC1'})

    def _checar(self, marcas):
        """Checadas en hora de la planta (UTC-6) → UTC, como las guarda el reloj."""
        self.env['qb.checada'].create([{
            'equipo_id': self.equipo.id, 'pin': '2901', 'employee_id': self.empleado.id,
            'timestamp': m + timedelta(hours=6), 'state': 'emparejada',
        } for m in marcas])

    def test_generar_marca_falta_y_conserva_lo_autorizado(self):
        marcas = [m for m in _semana_normal() if m.day != 13]
        marcas += [dt(2026, 10, 17, 7, 0), dt(2026, 10, 17, 13, 0)]     # sábado: 6 h de extra
        self._checar(marcas)
        lista = self.env['qb.nomina.incidencias'].create({'fecha_inicio': date(2026, 10, 14)})
        self.assertEqual(lista.fecha_inicio, LUNES, "La semana empieza en lunes")
        lista.action_generar()
        linea = lista.linea_ids.filtered(lambda l: l.employee_id == self.empleado)
        self.assertEqual(len(linea), 1)
        self.assertEqual(linea.dia_1, '0')
        self.assertEqual(linea.faltas, 1)
        self.assertAlmostEqual(linea.faltas_dias, 1.5)
        self.assertEqual(linea.he_propuestas, 6)
        self.assertEqual(linea.he_autorizadas, 6)
        self.assertEqual((linea.he_dobles, linea.he_sencillas), (6, 0))
        # El supervisor autoriza 4; al volver a generar no se pisa.
        linea.he_autorizadas = 4
        lista.action_generar()
        self.assertEqual(linea.he_autorizadas, 4)
        self.assertEqual(linea.he_dobles, 4)

    def test_semanas_que_paga_el_mes(self):
        Incidencias = self.env['qb.nomina.incidencias']
        octubre = Incidencias._lunes_del_mes(date(2026, 10, 1))
        self.assertEqual(octubre, [date(2026, 9, 28), date(2026, 10, 5), date(2026, 10, 12),
                                   date(2026, 10, 19), date(2026, 10, 26)], 'octubre 2026 paga 5 viernes')
        self.assertEqual(len(Incidencias._lunes_del_mes(date(2026, 11, 1))), 4)

    def test_premio_de_asistencia_del_mes(self):
        Incidencias = self.env['qb.nomina.incidencias']
        otro = self.env['hr.employee'].create({'name': 'Prueba Premio', 'registration_number': 'Q-902'})
        for lunes, faltas, retardos in ((date(2026, 11, 2), 0, 1), (date(2026, 11, 9), 0, 1),
                                        (date(2026, 11, 16), 0, 0)):
            lista = Incidencias.create({'fecha_inicio': lunes})
            self.env['qb.nomina.incidencias.linea'].create([
                {'incidencias_id': lista.id, 'employee_id': self.empleado.id, 'faltas': faltas,
                 'retardos': retardos},
                {'incidencias_id': lista.id, 'employee_id': otro.id, 'sin_checadas': lunes.day == 9},
            ])
        r = Incidencias.qb_premio_asistencia_mes(date(2026, 11, 1))
        self.assertEqual(r['faltan'], [date(2026, 11, 23)])
        yo = r['personas'][self.empleado.id]
        self.assertEqual((yo['retardos'], yo['semanas'], yo['pct']), (2, 3, 4.5))
        self.assertIsNone(r['personas'][otro.id]['pct'], 'una semana sin checadas: no hay con qué calificar')
