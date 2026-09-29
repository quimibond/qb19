# -*- coding: utf-8 -*-
"""El ajuste mensual de ISR: cuál es la última nómina del mes, de dónde sale
el acumulado (y cuándo NO se ajusta), y los números contra NOI (quincenas 18
y 19 de Toluca 2026, tablas 2026: UMA 3,566.22, subsidio 15.02 %, límite
11,492.66).

No corre en el CI (depende de Enterprise); se corre en el shell de Odoo.sh:
``odoo-bin ... --test-tags /quimibond_nomina --stop-after-init``."""
from datetime import date

from odoo.tests.common import TransactionCase
from odoo.tools import mute_logger

LOG = 'odoo.addons.quimibond_nomina.models.isr_mensual'


class TestIsrMensual(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        env = cls.env
        stype = env['hr.payroll.structure.type'].create({'name': 'QB ISR prueba'})
        cls.struct = env['hr.payroll.structure'].create({'name': 'QB ISR prueba', 'code': 'QB_ISR', 'type_id': stype.id})
        cat = env.ref('hr_payroll.BASIC')
        cls.rules = {}
        for code in ('GROSS', 'ISR', 'ISR_ADJUSTMENT', 'SUBSIDY'):
            cls.rules[code] = env['hr.salary.rule'].create({
                'name': code, 'code': code, 'struct_id': cls.struct.id, 'category_id': cat.id,
                'condition_select': 'none', 'amount_select': 'code', 'amount_python_compute': 'result = 0',
            })
        cls.employee = env['hr.employee'].create({'name': 'Prueba ISR', 'registration_number': 'Q-1'})
        cls.employee.version_id.write({'schedule_pay': 'bi-weekly', 'contract_date_start': date(2018, 9, 13)})
        cls.Param = env['ir.config_parameter'].sudo()
        cls.Param.set_param('quimibond_nomina.isr_mensual_incluye_borrador', '')

    def _recibo(self, date_from, date_to, lineas=None, state=None, employee=None):
        slip = self.env['hr.payslip'].create({
            'name': 'Recibo de prueba', 'employee_id': (employee or self.employee).id,
            'struct_id': self.struct.id, 'date_from': date_from, 'date_to': date_to,
        })
        for code, total in (lineas or {}).items():
            self.env['hr.payslip.line'].create({
                'slip_id': slip.id, 'salary_rule_id': self.rules[code].id, 'name': code,
                'amount': total, 'quantity': 1.0, 'rate': 100.0, 'sequence': 5,
            })
        if state:
            slip.write({'state': state})
        return slip

    # --- 1. Última nómina del mes -------------------------------------
    def test_quincenal_ultima_del_mes(self):
        self.assertFalse(self._recibo(date(2026, 9, 1), date(2026, 9, 15))._qb_isr_es_ultima_del_mes())
        self.assertTrue(self._recibo(date(2026, 9, 16), date(2026, 9, 30))._qb_isr_es_ultima_del_mes())
        self.assertTrue(self._recibo(date(2026, 2, 16), date(2026, 2, 28))._qb_isr_es_ultima_del_mes())

    def test_semanal_ultima_del_mes(self):
        self.employee.version_id.schedule_pay = 'weekly'
        self.assertFalse(self._recibo(date(2026, 9, 14), date(2026, 9, 20))._qb_isr_es_ultima_del_mes())
        # Semana 40: termina el 27-sep y la siguiente termina el 4-oct.
        self.assertTrue(self._recibo(date(2026, 9, 21), date(2026, 9, 27))._qb_isr_es_ultima_del_mes())
        # Semana 41 cruza de mes: es de octubre y no es la última de octubre.
        self.assertFalse(self._recibo(date(2026, 9, 28), date(2026, 10, 4))._qb_isr_es_ultima_del_mes())

    def test_otra_periodicidad_no_ajusta(self):
        self.employee.version_id.schedule_pay = 'monthly'
        self.assertTrue(self._recibo(date(2026, 9, 1), date(2026, 9, 30))._qb_isr_es_ultima_del_mes())
        self.employee.version_id.schedule_pay = 'daily'
        self.assertFalse(self._recibo(date(2026, 9, 30), date(2026, 9, 30))._qb_isr_es_ultima_del_mes())

    # --- 2. El acumulado ----------------------------------------------
    @mute_logger(LOG)
    def test_previo_en_borrador_no_ajusta(self):
        self._recibo(date(2026, 9, 1), date(2026, 9, 15), {'GROSS': 5322.90, 'ISR': -399.56})
        q19 = self._recibo(date(2026, 9, 16), date(2026, 9, 30))
        previos, completo = q19._qb_isr_recibos_previos_del_mes()
        self.assertEqual(len(previos), 1)
        self.assertFalse(completo)
        self.assertIsNone(q19._qb_isr_mensual(5322.90))

    @mute_logger(LOG)
    def test_parametro_piloto_cuenta_borradores_y_toma_el_mas_reciente(self):
        self.Param.set_param('quimibond_nomina.isr_mensual_incluye_borrador', '1')
        viejo = self._recibo(date(2026, 9, 1), date(2026, 9, 15), {'GROSS': 1.0})
        nuevo = self._recibo(date(2026, 9, 1), date(2026, 9, 15), {'GROSS': 5322.90})
        q19 = self._recibo(date(2026, 9, 16), date(2026, 9, 30))
        previos, completo = q19._qb_isr_recibos_previos_del_mes()
        self.assertTrue(completo)
        self.assertEqual(previos, nuevo)
        self.assertNotIn(viejo.id, previos.ids)
        self.Param.set_param('quimibond_nomina.isr_mensual_incluye_borrador', '')

    @mute_logger(LOG)
    def test_contrato_viejo_sin_recibo_previo_no_ajusta(self):
        q19 = self._recibo(date(2026, 9, 16), date(2026, 9, 30))
        previos, completo = q19._qb_isr_recibos_previos_del_mes()
        self.assertFalse(previos)
        self.assertFalse(completo)
        self.assertIsNone(q19._qb_isr_mensual(5322.90))

    def test_alta_en_el_mes_sin_previo_si_ajusta(self):
        nuevo = self.env['hr.employee'].create({'name': 'Alta 22-sep', 'registration_number': 'Q-2'})
        nuevo.version_id.write({'schedule_pay': 'bi-weekly', 'contract_date_start': date(2026, 9, 22)})
        q19 = self._recibo(date(2026, 9, 16), date(2026, 9, 30), employee=nuevo)
        previos, completo = q19._qb_isr_recibos_previos_del_mes()
        self.assertFalse(previos)
        self.assertTrue(completo)
        # Reyes Sánchez, 9.2 días: 4,015.68 gravables → tarifa 219.17, subsidio 535.65 topado al ISR → 0.
        res = q19._qb_isr_mensual(4015.68)
        self.assertEqual((res['isr'], res['subsidio']), (219.17, 219.17))

    def test_ignora_otras_estructuras_notas_de_credito_y_cancelados(self):
        otra = self.env['hr.payroll.structure'].create({
            'name': 'Otra', 'code': 'QB_OTRA', 'type_id': self.struct.type_id.id})
        q18 = self._recibo(date(2026, 9, 1), date(2026, 9, 15), {'GROSS': 5322.90}, state='validated')
        self.env['hr.payslip'].create({
            'name': 'Otra estructura', 'employee_id': self.employee.id, 'struct_id': otra.id,
            'date_from': date(2026, 9, 1), 'date_to': date(2026, 9, 15), 'state': 'validated'})
        self._recibo(date(2026, 9, 1), date(2026, 9, 15), state='cancel')
        nc = self._recibo(date(2026, 9, 1), date(2026, 9, 15), state='validated')
        nc.credit_note = True
        q19 = self._recibo(date(2026, 9, 16), date(2026, 9, 30))
        previos, completo = q19._qb_isr_recibos_previos_del_mes()
        self.assertEqual(previos, q18)
        self.assertTrue(completo)

    # --- 3. Los números -------------------------------------------------
    def test_terron_quincena_19(self):
        """Steph Terrón: 5,322.92 en las dos quincenas. NOI: Q18 131.57,
        Q19 132.06 (suma 263.63 = 799.28 − 535.65). Odoo Q18: ISR 399.56 y
        subsidio 267.82 → la Q19 debe cerrar el mes en 263.63 netos."""
        self._recibo(date(2026, 9, 1), date(2026, 9, 15),
                     {'GROSS': 5322.90, 'ISR': -399.56, 'SUBSIDY': 267.82}, state='validated')
        q19 = self._recibo(date(2026, 9, 16), date(2026, 9, 30))
        res = q19._qb_isr_mensual(5322.90)
        self.assertEqual(res['gravable_mes'], 10645.80)
        self.assertEqual(res['isr_mes'], 799.28)
        self.assertEqual(res['subsidio_mes'], 535.65)
        self.assertEqual(res['isr'], 399.72)
        self.assertEqual(res['subsidio'], 267.83)
        self.assertEqual(round(res['isr'] - res['subsidio'] + 399.56 - 267.82, 2), 263.63)

    def test_jessica_quincena_19(self):
        """Jessica Francisco: Q18 29,975.30 (ISR 5,858.84), Q19 9,757.70.
        NOI Q19: 834.11; Odoo hoy 1,139.53."""
        self._recibo(date(2026, 9, 1), date(2026, 9, 15),
                     {'GROSS': 29975.30, 'ISR': -5858.84}, state='paid')
        q19 = self._recibo(date(2026, 9, 16), date(2026, 9, 30))
        res = q19._qb_isr_mensual(9757.70)
        self.assertEqual(res['subsidio_mes'], 0.0)
        self.assertAlmostEqual(res['isr'], 834.18, delta=0.10)

    def test_el_mes_rebasa_el_limite_y_recupera_el_subsidio(self):
        """Q18 con subsidio (5,697.03) y Q19 alta: el mes rebasa 11,492.66,
        el subsidio del mes es 0 y el de la Q19 sale negativo (se recupera)."""
        self._recibo(date(2026, 9, 1), date(2026, 9, 15),
                     {'GROSS': 5697.03, 'ISR': -440.27, 'SUBSIDY': 267.82}, state='validated')
        q19 = self._recibo(date(2026, 9, 16), date(2026, 9, 30))
        res = q19._qb_isr_mensual(8000.00)
        self.assertEqual(res['subsidio_mes'], 0.0)
        self.assertEqual(res['subsidio'], -267.82)

    def test_el_ajuste_manual_previo_cuenta_como_retenido(self):
        self._recibo(date(2026, 9, 1), date(2026, 9, 15),
                     {'GROSS': 7178.26, 'ISR': -646.23, 'ISR_ADJUSTMENT': -10.00}, state='validated')
        q19 = self._recibo(date(2026, 9, 16), date(2026, 9, 30))
        res = q19._qb_isr_mensual(7178.26)
        self.assertEqual(res['retenido_previo'], 656.23)
        self.assertEqual(res['isr_mes'], 1293.04)
        self.assertEqual(res['isr'], 636.81)
