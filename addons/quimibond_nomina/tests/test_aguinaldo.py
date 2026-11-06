# -*- coding: utf-8 -*-
"""Aguinaldo con los días de cada grupo de nómina (1.18.0): 25 la planta
semanal, 16 los quincenales, el parámetro de la localización (15) como
respaldo, y el prorrateo de la regla 104.

No corre en el CI (depende de Enterprise); se corre en el shell de Odoo.sh:
``odoo-bin ... --test-tags /quimibond_nomina --stop-after-init``."""
from datetime import date

from odoo.tests.common import TransactionCase

from .common import liberar_referencias


class TestAguinaldo(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        env = cls.env
        mx = env.ref('base.mx')
        cls.tipo_semanal = env['hr.payroll.structure.type'].create({
            'name': 'QB Aguinaldo semanal', 'country_id': mx.id, 'default_schedule_pay': 'weekly',
            'qb_dias_aguinaldo': 25})
        cls.tipo_sin_dias = env['hr.payroll.structure.type'].create({
            'name': 'QB Aguinaldo sin días', 'country_id': mx.id, 'default_schedule_pay': 'bi-weekly'})
        cls.struct = env['hr.payroll.structure'].create({
            'name': 'QB Aguinaldo prueba', 'code': 'QB_AGUI', 'type_id': cls.tipo_semanal.id})
        cls.Param = env['ir.config_parameter'].sudo()
        cls.Param.set_param('quimibond_nomina.isr_mensual_incluye_borrador', '')
        liberar_referencias(env, 'S-901')

    def _recibo(self, tipo, inicio_contrato):
        empleado = self.env['hr.employee'].create({'name': 'Prueba aguinaldo', 'registration_number': 'S-901'})
        empleado.version_id.write({
            'structure_type_id': tipo.id,
            'schedule_pay': tipo.default_schedule_pay,
            'contract_date_start': inicio_contrato,
            'date_version': inicio_contrato,
            'wage': 3500.0,
        })
        return self.env['hr.payslip'].create({
            'name': 'Aguinaldo de prueba', 'employee_id': empleado.id, 'version_id': empleado.version_id.id,
            'struct_id': self.struct.id, 'date_from': date(2026, 12, 1), 'date_to': date(2026, 12, 31),
        })

    def test_dias_del_grupo(self):
        slip = self._recibo(self.tipo_semanal, date(2020, 1, 6))
        self.assertEqual(slip._qb_aguinaldo_dias(), 25)

    def test_sin_dias_cae_al_parametro_de_pais(self):
        slip = self._recibo(self.tipo_sin_dias, date(2020, 1, 6))
        self.assertEqual(slip._qb_aguinaldo_dias(), slip._rule_parameter('l10n_mx_christmas_bonus'))

    def test_anio_completo_paga_los_dias_del_grupo(self):
        slip = self._recibo(self.tipo_semanal, date(2020, 1, 6))
        self.assertAlmostEqual(slip._qb_aguinaldo_base(), slip.l10n_mx_daily_salary * 25, places=2)

    def test_alta_a_medio_anio_prorratea(self):
        """Daniel Galán (S-333): alta el 19-oct, 74 días de 365."""
        slip = self._recibo(self.tipo_semanal, date(2026, 10, 19))
        esperado = slip.l10n_mx_daily_salary * 25 * 74 / slip.l10n_mx_days_of_year
        self.assertAlmostEqual(slip._qb_aguinaldo_base(), esperado, places=2)

    def test_borradores_solo_con_el_interruptor_de_la_piloto(self):
        slip = self._recibo(self.tipo_semanal, date(2020, 1, 6))
        regular = self.env['hr.payroll.structure'].search([('code', '=', 'MX_REGULAR')], limit=1)
        if not regular:
            self.skipTest('sin la estructura MX_REGULAR de la localización')
        borrador = self.env['hr.payslip'].create({
            'name': 'Semana de prueba', 'employee_id': slip.employee_id.id, 'version_id': slip.version_id.id,
            'struct_id': regular.id, 'date_from': date(2026, 11, 2), 'date_to': date(2026, 11, 8),
        })
        desde, hasta = date(2026, 1, 1), date(2026, 12, 31)
        self.assertFalse(slip._qb_aguinaldo_recibos_regulares(desde, hasta))
        self.Param.set_param('quimibond_nomina.isr_mensual_incluye_borrador', '1')
        self.assertEqual(slip._qb_aguinaldo_recibos_regulares(desde, hasta), borrador)
        # Dos borradores del mismo periodo (la piloto recalcula): cuenta solo el más reciente.
        otro = borrador.copy({'name': 'Semana de prueba (recálculo)'})
        self.assertEqual(slip._qb_aguinaldo_recibos_regulares(desde, hasta), otro)

    def test_sembrar_no_pisa_lo_capturado(self):
        self.tipo_semanal.qb_dias_aguinaldo = 30
        self.env['hr.payroll.structure.type']._qb_sembrar_dias_aguinaldo()
        self.assertEqual(self.tipo_semanal.qb_dias_aguinaldo, 30)
        self.assertEqual(self.tipo_sin_dias.qb_dias_aguinaldo, 0)  # quincenal sin xmlid: no se toca
