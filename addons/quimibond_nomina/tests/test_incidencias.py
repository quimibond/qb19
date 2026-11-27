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


def _noches(dias):
    """Turnos de 19:00 a 07:00 que entran esos días de octubre, con comida a la 01:00."""
    marcas = []
    for d in dias:
        entrada = dt(2026, 10, d, 18, 50)
        marcas += [entrada, entrada + timedelta(hours=6, minutes=10), entrada + timedelta(hours=6, minutes=40),
                   entrada + timedelta(hours=12, minutes=12)]
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

    # ------------------------------------------------------------------
    # Turnos de RH (1.20.0)
    # ------------------------------------------------------------------
    def test_noche_lunes_a_jueves_no_falta_el_viernes(self):
        marcas = _noches((12, 13, 14, 15))
        turno, _p = calc.inferir_turno(LUNES, marcas, hoy=HOY)
        self.assertEqual(turno, '4')
        r = calc.calcular_semana(LUNES, HORARIO, marcas, hoy=HOY, turno=turno)
        self.assertEqual(r['faltas'], 0, 'el viernes no es de su turno: se paga')
        self.assertEqual(r['dias'][:5], ['10', '10', '10', '10', '8'])
        self.assertEqual(r['horas_base'], 48.0)
        self.assertEqual(r['he_propuestas'], 6, 'turno 4: 6 HE fijas por completar el jueves (RH, 23-nov)')
        self.assertEqual(r['retardos'], 0)
        # Sin turno (como antes): falta el viernes y 2 horas de extra cada noche.
        antes = calc.calcular_semana(LUNES, HORARIO, marcas, hoy=HOY)
        self.assertEqual(antes['faltas'], 1)
        self.assertEqual(antes['he_propuestas'], 8)

    def test_noche_de_fin_de_semana(self):
        marcas = _noches((12, 17, 18))
        turno, _p = calc.inferir_turno(LUNES, marcas, hoy=HOY)
        self.assertEqual(turno, '6')
        r = calc.calcular_semana(LUNES, HORARIO, marcas, hoy=HOY, turno=turno)
        self.assertEqual(r['faltas'], 0)
        self.assertEqual(r['horas_base'], 48.0, 'la base de pago no cambia')
        self.assertEqual(r['dias'][5:], ['12', '12'])
        self.assertEqual(r['dom_horas'], 12.0, 'entra sábado y domingo en la noche: RH paga 12 de prima')
        self.assertEqual(r['he_propuestas'], 0)

    def test_falta_en_sabado_del_turno_quita_sus_horas(self):
        marcas = _noches((12, 18))
        r = calc.calcular_semana(LUNES, HORARIO, marcas, hoy=HOY, turno='6')
        self.assertEqual(r['dias'][5], '0')
        self.assertEqual(r['faltas'], 1)
        self.assertEqual(r['falta_horas'], 12.0)
        self.assertEqual(r['horas_base'], 36.0)

    def test_turno_cinco_con_domingo_de_dia(self):
        marcas = _noches((13, 14, 15)) + _dia(18, (6, 55), (19, 0))
        turno, _p = calc.inferir_turno(LUNES, marcas, hoy=HOY)
        self.assertEqual(turno, '5')
        r = calc.calcular_semana(LUNES, HORARIO, marcas, hoy=HOY, turno=turno)
        self.assertEqual(r['faltas'], 0, 'lunes y viernes no son de su turno')
        self.assertEqual(r['dom_horas'], 12.0)
        self.assertEqual(r['he_propuestas'], 0)

    def test_dia_de_doce_que_trabaja_el_viernes(self):
        marcas = []
        for d in (12, 13, 14, 15):
            marcas += _dia(d, (6, 50), (19, 2))
        marcas += _dia(16, (9, 44), (15, 2), comida=False)
        turno, _p = calc.inferir_turno(LUNES, marcas, hoy=HOY)
        self.assertEqual(turno, '3')
        r = calc.calcular_semana(LUNES, HORARIO, marcas, hoy=HOY, turno=turno)
        self.assertEqual(r['retardos'], 0, 'el viernes no es de su turno: llegar 09:44 no es retardo')
        self.assertEqual(r['faltas'], 0)
        self.assertEqual(r['he_propuestas'], 5, 'turno 3 sin extra en sus cuatro días; el quinto día es extra')

    def test_turno_dos_entra_a_las_ocho(self):
        marcas = []
        for d in (12, 13, 14, 15):
            marcas += _dia(d, (7, 44), (18, 1))
        marcas += _dia(16, (7, 50), (16, 0))
        self.assertEqual(calc.inferir_turno(LUNES, marcas, hoy=HOY, preferido='2')[0], '2')
        r = calc.calcular_semana(LUNES, HORARIO, marcas, hoy=HOY, turno='2')
        self.assertEqual(r['retardos'], 0)
        antes = calc.calcular_semana(LUNES, HORARIO, marcas, hoy=HOY)
        self.assertEqual(antes['retardos'], 5, 'contra las 07:00 de la planta eran cinco retardos')
        # Turno 1 en RH, llega 07:44 y sale a las 17:00: el de RH se respeta y sí son retardos.
        tarde = []
        for d in (12, 13, 14, 15):
            tarde += _dia(d, (7, 44), (17, 0))
        tarde += _dia(16, (7, 44), (15, 0))
        self.assertEqual(calc.inferir_turno(LUNES, tarde, hoy=HOY, preferido='1')[0], '1')
        r1 = calc.calcular_semana(LUNES, HORARIO, tarde, hoy=HOY, turno='1')
        self.assertEqual(r1['retardos'], 5)

    def test_el_checador_gana_si_rh_tiene_otro_turno(self):
        marcas = _noches((12, 13, 14, 15))
        self.assertEqual(calc.inferir_turno(LUNES, marcas, hoy=HOY, preferido='3')[0], '4')

    def test_falta_en_dia_del_turno_y_sin_checadas(self):
        marcas = _noches((12, 13, 15))
        r = calc.calcular_semana(LUNES, HORARIO, marcas, hoy=HOY, turno='4')
        self.assertEqual((r['faltas'], r['falta_horas']), (1, 12.0))
        self.assertEqual(r['dias'][2], '0')
        self.assertEqual(calc.inferir_turno(LUNES, [], hoy=HOY), (None, {}))
        sin = calc.calcular_semana(LUNES, HORARIO, [], hoy=HOY, turno='4')
        self.assertEqual(sin['faltas'], 0)
        self.assertTrue(sin['sin_checadas'])

    def test_turno_tres_cambia_un_dia_por_el_viernes(self):
        marcas = []
        for d in (12, 14, 15, 16):
            marcas += _dia(d, (6, 45), (19, 3))
        turno, _p = calc.inferir_turno(LUNES, marcas, hoy=HOY)
        self.assertEqual(turno, '3')
        r = calc.calcular_semana(LUNES, HORARIO, marcas, hoy=HOY, turno='3')
        self.assertEqual(r['faltas'], 0, 'no vino el martes pero repuso el viernes')
        self.assertEqual(r['horas_base'], 48.0)
        self.assertEqual(r['he_propuestas'], 0, 'turno 3: cuatro días de 12 horas sin tiempo extra')
        # Tres días: falta uno, el que no repuso.
        tres = [m for m in marcas if m.day != 14]
        r3 = calc.calcular_semana(LUNES, HORARIO, tres, hoy=HOY, turno='3')
        self.assertEqual(r3['faltas'], 1)
        self.assertEqual(r3['dias'][1], '0', 'el martes; el miércoles se toma como su día libre')

    def test_turno_tres_de_rh_con_dias_de_diez(self):
        marcas = []
        for d in (12, 13, 14, 15):
            marcas += _dia(d, (6, 55), (17, 0))
        marcas += _dia(16, (6, 55), (15, 0))
        self.assertEqual(calc.inferir_turno(LUNES, marcas, hoy=HOY)[0], '1')
        self.assertEqual(calc.inferir_turno(LUNES, marcas, hoy=HOY, preferido='3')[0], '3',
                         'a menos de un turno completo de diferencia gana el de RH')
        r = calc.calcular_semana(LUNES, HORARIO, marcas, hoy=HOY, turno='3')
        self.assertEqual(r['faltas'], 0)

    def test_comida_se_anota_sin_descontar(self):
        marcas = [dt(2026, 10, 12, 6, 55), dt(2026, 10, 12, 13, 0), dt(2026, 10, 12, 14, 25),
                  dt(2026, 10, 12, 17, 0)]
        marcas += _dia(13, (6, 55), (17, 0))                      # 30 min de comida
        r = calc.calcular_semana(LUNES, HORARIO, marcas, hoy=HOY, turno='1')
        self.assertEqual(r['comida_min'], 85 + 30)
        self.assertEqual(r['comidas_largas'], 1)
        self.assertIn('Lu comida 85 min (13:00 a 14:25)', r['notas'])
        self.assertEqual(r['he_propuestas'], 0, 'la comida no cambia las horas')
        sin_limite = calc.calcular_semana(LUNES, HORARIO, marcas, hoy=HOY, turno='1', comida_max=90)
        self.assertEqual(sin_limite['comidas_largas'], 0)
        # Con solo entrada y salida (o tres checadas) no se sabe la comida.
        self.assertIsNone(calc.comida(_dia(14, (7, 0), (17, 0), comida=False), dt(2026, 10, 14, 7), dt(2026, 10, 14, 17)))
        # Checada repetida: cuenta una vez.
        dobles = _dia(15, (7, 0), (17, 0)) + [dt(2026, 10, 15, 13, 1)]
        self.assertEqual(calc.comida(dobles, dt(2026, 10, 15, 7), dt(2026, 10, 15, 17)),
                         (dt(2026, 10, 15, 13, 1), dt(2026, 10, 15, 13, 30)))

    # ------------------------------------------------------------------
    # Reglas de RH del 23-nov-2026 (1.24.0)
    # ------------------------------------------------------------------
    def test_turno_cuatro_sin_jueves_completo_no_lleva_las_seis(self):
        marcas = _noches((12, 13, 14))
        marcas += [dt(2026, 10, 15, 18, 55), dt(2026, 10, 16, 5, 30)]        # sale 05:30 el viernes
        r = calc.calcular_semana(LUNES, HORARIO, marcas, hoy=HOY, turno='4')
        self.assertEqual(r['he_propuestas'], 0)
        self.assertEqual(r['salidas_antes'], 1)
        self.assertIn('Ju sin turno completo: sin las 6 HE del turno', r['notas'])
        # Falta el lunes pero completa el jueves: las 6 van (regla literal de RH; la falta se descuenta aparte).
        r2 = calc.calcular_semana(LUNES, HORARIO, _noches((13, 14, 15)), hoy=HOY, turno='4')
        self.assertEqual((r2['faltas'], r2['he_propuestas'], r2['dias_con_extra']), (1, 6, 1))

    def test_prima_dominical_proporcional_sin_el_turno_completo(self):
        # Turno 6 sin la noche del sábado: solo las 5 horas del domingo de la noche del domingo (RH, 24-nov).
        r = calc.calcular_semana(LUNES, HORARIO, _noches((12, 18)), hoy=HOY, turno='6')
        self.assertEqual(r['dom_horas'], 5.0)
        self.assertIn('Do sin turno completo: prima dominical de 5 h trabajadas en domingo', r['notas'])
        # Turno 5 que sale del domingo a las 15:00: 8 horas (desde la entrada de las 07:00).
        marcas = _noches((13, 14, 15)) + _dia(18, (6, 55), (15, 0))
        r5 = calc.calcular_semana(LUNES, HORARIO, marcas, hoy=HOY, turno='5')
        self.assertEqual(r5['dom_horas'], 8.0)
        # Completo: 12.
        completo = calc.calcular_semana(LUNES, HORARIO, _noches((12, 17, 18)), hoy=HOY, turno='6')
        self.assertEqual(completo['dom_horas'], 12.0)
        # Domingo todavía por venir (la lista se saca el martes): sin prima y sin nota.
        r6 = calc.calcular_semana(LUNES, HORARIO, _noches((12,)), hoy=date(2026, 10, 13), turno='6')
        self.assertEqual(r6['dom_horas'], 0.0)
        self.assertFalse([n for n in r6['notas'] if n.startswith('Do sin turno completo')])

    # ------------------------------------------------------------------
    # Respuestas de RH del 24-nov-2026 (1.26.0)
    # ------------------------------------------------------------------
    def test_seis_horas_del_turno_cuatro_solo_si_la_cuadrilla_las_paga(self):
        r = calc.calcular_semana(LUNES, HORARIO, _noches((12, 13, 14, 15)), hoy=HOY, turno='4', he_fijas=False)
        self.assertEqual(r['he_propuestas'], 0)
        self.assertFalse([n for n in r['notas'] if 'HE' in n])

    def test_tolerancia_de_diez_minutos_para_el_turno_completo(self):
        base = _noches((12, 13, 14))
        jueves = lambda h, m: [dt(2026, 10, 15, h, m), dt(2026, 10, 16, 1, 0), dt(2026, 10, 16, 1, 30),
                               dt(2026, 10, 16, 7, 5)]
        a_tiempo = calc.calcular_semana(LUNES, HORARIO, base + jueves(19, 9), hoy=HOY, turno='4')
        self.assertEqual(a_tiempo['he_propuestas'], 6)
        self.assertEqual(a_tiempo['retardos'], 1, 'el retardo se anota desde el minuto 1')
        tarde = calc.calcular_semana(LUNES, HORARIO, base + jueves(19, 15), hoy=HOY, turno='4')
        self.assertEqual(tarde['he_propuestas'], 0)
        self.assertIn('Ju sin turno completo: sin las 6 HE del turno', tarde['notas'])

    def test_comida_de_sindicalizados_y_empleados(self):
        self.assertEqual(calc.comida_permitida(10, True), 30)
        self.assertEqual(calc.comida_permitida(8, True), 30)
        self.assertEqual(calc.comida_permitida(12, True), 45)
        self.assertEqual(calc.comida_permitida(12, False), 60)
        # 40 minutos: larga para un sindicalizado de 10 horas, no para uno de 12 ni para un empleado.
        diez = [dt(2026, 10, 12, 6, 55), dt(2026, 10, 12, 13, 0), dt(2026, 10, 12, 13, 40), dt(2026, 10, 12, 17, 0)]
        self.assertEqual(calc.calcular_semana(LUNES, HORARIO, diez, hoy=HOY, turno='1',
                                              sindicalizado=True)['comidas_largas'], 1)
        self.assertEqual(calc.calcular_semana(LUNES, HORARIO, diez, hoy=HOY, turno='1')['comidas_largas'], 0)
        doce = [dt(2026, 10, 12, 6, 55), dt(2026, 10, 12, 13, 0), dt(2026, 10, 12, 13, 40), dt(2026, 10, 12, 19, 0)]
        self.assertEqual(calc.calcular_semana(LUNES, HORARIO, doce, hoy=HOY, turno='3',
                                              sindicalizado=True)['comidas_largas'], 0)

    def test_festivo_se_paga_y_trabajado_se_anota(self):
        marcas = _semana_normal()
        jueves = date(2026, 10, 15)
        sin_jueves = [m for m in marcas if m.date() != jueves]
        r = calc.calcular_semana(LUNES, HORARIO, sin_jueves, hoy=HOY, turno='1', festivos={jueves})
        self.assertEqual(r['faltas'], 0, 'el festivo no es falta')
        self.assertEqual(r['dias'][3], 'F')
        self.assertEqual(r['horas_base'], 48.0)
        self.assertEqual(r['festivos_trabajados'], 0)
        r2 = calc.calcular_semana(LUNES, HORARIO, marcas, hoy=HOY, turno='1', festivos={jueves})
        self.assertEqual(r2['festivos_trabajados'], 1)
        self.assertIn('Ju festivo trabajado', r2['notas'])
        self.assertEqual(r2['horas_base'], 48.0)

    def test_turno_siete_solo_si_rh_lo_tiene(self):
        marcas = []
        for d in (12, 13, 14, 15):
            marcas += _dia(d, (8, 58), (16, 2))
        marcas += _dia(16, (8, 58), (14, 1), comida=False)
        self.assertNotEqual(calc.inferir_turno(LUNES, marcas, hoy=HOY)[0], '7')
        self.assertEqual(calc.inferir_turno(LUNES, marcas, hoy=HOY, preferido='7')[0], '7')
        r = calc.calcular_semana(LUNES, HORARIO, marcas, hoy=HOY, turno='7')
        self.assertEqual((r['retardos'], r['salidas_antes'], r['faltas']), (0, 0, 0))

    def test_marcas_hasta_el_corte(self):
        dia = _semana_normal()
        cortadas = calc.marcas_hasta(dia, date(2026, 10, 14))
        self.assertEqual({m.day for m in cortadas}, {12, 13, 14})
        noches = _noches((14, 15))
        self.assertEqual(calc.marcas_hasta(noches, date(2026, 10, 14)), noches[:4],
                         'la noche que entra el miércoles cuenta completa aunque salga el jueves')
        self.assertEqual(calc.marcas_hasta(dia, None), dia)

    def test_entradas_del_parametro(self):
        self.assertEqual(calc.leer_entradas('06:00, 18:00'), (dt(1, 1, 1, 6).time(), dt(1, 1, 1, 18).time()))
        self.assertEqual(calc.leer_entradas('basura'), calc.ENTRADAS_DEFAULT)
        self.assertEqual(calc.leer_entradas(''), calc.ENTRADAS_DEFAULT)

    def test_salida_sin_entrada_no_es_turno_de_noche(self):
        """Mario Gómez Soria, semana 49: turno 1, el martes le faltó la entrada. Su salida de las 17:05 se
        juntaba con la entrada del miércoles 06:37 en un «turno» de 13.5 h fuera de su turno: 11 HE."""
        lunes = date(2026, 11, 23)
        marcas = [dt(2026, 11, 23, 6, 38), dt(2026, 11, 23, 17, 6), dt(2026, 11, 24, 17, 5),
                  dt(2026, 11, 25, 6, 37), dt(2026, 11, 25, 17, 13)]
        codigo, _p = calc.inferir_turno(lunes, marcas, preferido='1')
        self.assertEqual(codigo, '1')
        r = calc.calcular_semana(lunes, HORARIO, marcas, turno=codigo, hoy=date(2026, 11, 25))
        self.assertEqual(r['he_propuestas'], 0)
        self.assertIn('Ma una sola checada (17:05)', r['notas'])
        self.assertEqual(r['faltas'], 0, 'el martes checó: no es falta, es una checada que corregir')
        # La agrupación sin turno se queda como estaba (el turno de noche sin checar a la comida).
        martes = dt(2026, 11, 24, 17, 5)
        self.assertIn((martes, dt(2026, 11, 25, 6, 37), 2), calc.agrupar_turnos(marcas))
        self.assertIn((martes, martes, 1), calc.agrupar_turnos(marcas, turno='1'))

    def test_noche_sin_comida_se_junta_con_su_turno(self):
        marcas = [dt(2026, 11, 23, 18, 55), dt(2026, 11, 24, 7, 4)]
        self.assertEqual(len(calc.agrupar_turnos(marcas, turno='4')), 1)
        self.assertEqual(len(calc.agrupar_turnos(marcas, turno='1')), 2)
        codigo, _p = calc.inferir_turno(date(2026, 11, 23), marcas, preferido='1')
        self.assertNotIn(codigo, ('1', '2'), 'la noche no se parte por tener RH otro turno')

    def test_entrada_anticipada_no_es_extra(self):
        """Dirección, 26-nov-2026: llegar antes no es extra; el extra de la salida, en horas completas."""
        lunes = date(2026, 11, 23)
        marcas = [dt(2026, 11, 23, 5, 30), dt(2026, 11, 23, 17, 50),     # 1.5 h antes, 50 min después
                  dt(2026, 11, 24, 6, 55), dt(2026, 11, 24, 18, 20)]     # 1 h 20 min después
        r = calc.calcular_semana(lunes, HORARIO, marcas, turno='1', hoy=date(2026, 11, 24))
        self.assertEqual(r['he_propuestas'], 1)
        self.assertTrue(any(n.startswith('Lu entró 1 h 30 min antes (05:30)') for n in r['notas']))


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
        liberar_referencias(env, 'Q-901', 'Q-902', 'Q-903')
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
        # RH lo tiene en el turno 1. Sin turno de RH, cuatro días de 07:00 a 17:00 y el sábado los explica casi
        # igual el turno 3 (cuatro de cinco días): el martes salía libre y no falta (24.86 contra 24.73 horas).
        self.empleado.qb_turno = '1'
        marcas = [m for m in _semana_normal() if m.day != 13]
        marcas += [dt(2026, 10, 17, 7, 0), dt(2026, 10, 17, 13, 0)]     # sábado: 6 h de extra
        self._checar(marcas)
        lista = self.env['qb.nomina.incidencias'].create({'fecha_inicio': date(2026, 10, 14)})
        self.assertEqual(lista.fecha_inicio, LUNES, "La semana empieza en lunes")
        self.assertEqual(lista.corte, date(2026, 10, 14), "El corte por omisión es el miércoles, como NOI")
        lista.corte = date(2026, 10, 18)     # semana completa: esta prueba es de las reglas, no del corte
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

    def test_generar_por_turno_de_noche(self):
        otro = self.env['hr.employee'].create({'name': 'Prueba Turno Noche', 'registration_number': 'Q-903',
                                               'qb_turno': '3'})
        otro.version_id.write({'schedule_pay': 'weekly', 'contract_date_start': date(2026, 1, 1),
                               'resource_calendar_id': self.calendario.id})
        self.env['qb.checada'].create([{
            'equipo_id': self.equipo.id, 'pin': '2903', 'employee_id': otro.id,
            'timestamp': m + timedelta(hours=6), 'state': 'emparejada',
        } for m in _noches((12, 13, 14, 15))])
        lista = self.env['qb.nomina.incidencias'].create({'fecha_inicio': LUNES, 'corte': date(2026, 10, 18)})
        lista.action_generar()
        linea = lista.linea_ids.filtered(lambda l: l.employee_id == otro)
        self.assertEqual(linea.turno, '4', 'el checador lo vio de noche')
        self.assertEqual(linea.turno_rh, '3')
        self.assertEqual(linea.faltas, 0)
        self.assertEqual(linea.he_propuestas, 6, 'turno 4: 6 HE fijas por completar el jueves')
        self.assertIn('Turno 4 según checador', linea.observaciones)

    def test_cuadrilla_escribe_el_turno_y_rota(self):
        liberar_referencias(self.env, 'S-904', 'S-905')
        uno = self.env['hr.employee'].create({'name': 'Prueba Cuadrilla Uno', 'registration_number': 'S-904'})
        fijo = self.env['hr.employee'].create({'name': 'Prueba Cuadrilla Fijo', 'registration_number': 'S-905',
                                               'qb_turno': '1', 'qb_turno_fijo': True})
        cuadrilla = self.env['qb.nomina.cuadrilla'].create({'name': 'Prueba A', 'turno': '3', 'rotacion': '3,4'})
        (uno | fijo).write({'qb_cuadrilla_id': cuadrilla.id})
        self.assertEqual(uno.qb_turno, '3')
        self.assertEqual(fijo.qb_turno, '1', 'el turno fijo no rota')
        cuadrilla.action_rotar()
        self.assertEqual((cuadrilla.turno, uno.qb_turno), ('4', '4'))
        cuadrilla.action_rotar()
        self.assertEqual(uno.qb_turno, '3')

    def test_generar_con_festivo(self):
        self.env['qb.nomina.festivo'].create({'fecha': date(2026, 10, 15), 'name': 'Festivo de prueba'})
        self._checar([m for m in _semana_normal() if m.day != 15])
        lista = self.env['qb.nomina.incidencias'].create({'fecha_inicio': LUNES, 'corte': date(2026, 10, 18)})
        lista.action_generar()
        linea = lista.linea_ids.filtered(lambda l: l.employee_id == self.empleado)
        self.assertEqual((linea.dia_3, linea.faltas, linea.festivos_trabajados), ('F', 0, 0))

    def test_corte_del_miercoles_pasa_jueves_a_domingo_a_la_semana_siguiente(self):
        # Viernes 16 falta y sábado 17 seis horas de extra: la lista de la semana del 12 corta el miércoles
        # y no los ve; la del 19 los paga como «semana anterior», sin volver a pagar lunes a miércoles.
        self.empleado.qb_turno = '1'
        marcas = [m for m in _semana_normal() if m.day != 16]
        marcas += [dt(2026, 10, 14, 17, 0) + timedelta(hours=2)]          # miércoles: sale a las 19:00
        marcas = [m for m in marcas if m != dt(2026, 10, 14, 17, 0)]
        marcas += [dt(2026, 10, 17, 7, 0), dt(2026, 10, 17, 13, 0)]
        self._checar(marcas)
        Incidencias = self.env['qb.nomina.incidencias']
        semana = Incidencias.create({'fecha_inicio': LUNES})
        semana.action_generar()
        linea = semana.linea_ids.filtered(lambda l: l.employee_id == self.empleado)
        self.assertEqual(semana.corte_aplicado, date(2026, 10, 14))
        self.assertEqual((linea.faltas, linea.he_propuestas), (0, 1), 'solo la HE del miércoles')
        self.assertEqual(linea.dia_4, '8', 'el viernes, después del corte, se proyecta')
        self.assertTrue(linea.proyectado)
        siguiente = Incidencias.create({'fecha_inicio': date(2026, 10, 19)})
        siguiente.action_generar()
        self.assertEqual(siguiente.pendientes_desde, date(2026, 10, 15))
        linea2 = siguiente.linea_ids.filtered(lambda l: l.employee_id == self.empleado)
        self.assertEqual((linea2.ant_faltas, linea2.ant_he), (1, 6))
        self.assertEqual(linea2.faltas, 1)
        self.assertEqual(linea2.falta_horas, 8.0, 'el viernes no lleva las 2 horas de castigo')
        self.assertEqual(linea2.he_propuestas, 6, 'el sábado; la del miércoles ya se pagó')
        self.assertIn('Semana anterior (del 15/10 al domingo)', linea2.observaciones)
        self.assertIn('Vi falta', linea2.observaciones)

    def test_corte_en_domingo_no_deja_pendientes(self):
        self.empleado.qb_turno = '1'
        self._checar(_semana_normal() + [dt(2026, 10, 17, 7, 0), dt(2026, 10, 17, 13, 0)])
        Incidencias = self.env['qb.nomina.incidencias']
        semana = Incidencias.create({'fecha_inicio': LUNES, 'corte': date(2026, 10, 18)})
        semana.action_generar()
        self.assertEqual(semana.linea_ids.filtered(lambda l: l.employee_id == self.empleado).he_propuestas, 6)
        siguiente = Incidencias.create({'fecha_inicio': date(2026, 10, 19)})
        siguiente.action_generar()
        self.assertFalse(siguiente.pendientes_desde)
        linea = siguiente.linea_ids.filtered(lambda l: l.employee_id == self.empleado)
        self.assertEqual((linea.ant_he, linea.he_propuestas), (0, 0))

    def test_quien_paso_a_la_quincenal_no_sale_en_la_semanal(self):
        liberar_referencias(self.env, 'Q-904')
        otro = self.env['hr.employee'].create({'name': 'Prueba Pasó a Quincenal', 'registration_number': 'Q-904'})
        otro.version_id.write({'schedule_pay': 'weekly', 'contract_date_start': date(2026, 1, 1),
                               'resource_calendar_id': self.calendario.id})
        lista = self.env['qb.nomina.incidencias'].create({'fecha_inicio': LUNES})
        self.assertIn(otro.version_id, lista._contratos())
        self.env['hr.version'].create({
            'employee_id': otro.id, 'date_version': date(2026, 10, 1), 'schedule_pay': 'bi-weekly',
            'contract_date_start': date(2026, 1, 1), 'resource_calendar_id': self.calendario.id,
        })
        self.assertNotIn(otro, [v.employee_id for v in lista._contratos()],
                         'la versión quincenal más nueva manda aunque la semanal siga abierta')

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
