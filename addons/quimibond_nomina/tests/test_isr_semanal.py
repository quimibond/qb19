# -*- coding: utf-8 -*-
"""ISR y subsidio de la semana como NOI (1.19.0): tarifa mensual ÷ 30.4 × 7,
subsidio a 4 decimales con el tope del mes de pago, redondeo una sola vez.
Los números son de «NOMINA 46 SMTL» de NOI (semana 46 de 2026, pagada el
viernes 6-nov; tablas 2026).

No corre en el CI (depende de Enterprise); se corre en el shell de Odoo.sh:
``odoo-bin ... --test-tags /quimibond_nomina --stop-after-init``."""
from datetime import date, timedelta

from odoo.tests.common import TransactionCase

from .common import liberar_referencias

LUNES_46 = date(2026, 11, 2)


class TestIsrSemanal(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        env = cls.env
        stype = env['hr.payroll.structure.type'].create({'name': 'QB ISR semanal prueba'})
        cls.struct = env['hr.payroll.structure'].create({'name': 'QB ISR semanal', 'code': 'QB_ISRS',
                                                          'type_id': stype.id})
        cat = env.ref('hr_payroll.BASIC')
        cls.regla_subsidio = env['hr.salary.rule'].create({
            'name': 'SUBSIDY', 'code': 'SUBSIDY', 'struct_id': cls.struct.id, 'category_id': cat.id,
            'condition_select': 'none', 'amount_select': 'code', 'amount_python_compute': 'result = 0',
        })
        cls.Param = env['ir.config_parameter'].sudo()
        cls.Param.set_param('quimibond_nomina.dia_pago_semanal', '4')
        cls.Param.set_param('quimibond_nomina.isr_mensual_incluye_borrador', '1')
        liberar_referencias(env, 'S-902')
        cls.empleado = env['hr.employee'].create({'name': 'Prueba ISR semanal', 'registration_number': 'S-902'})
        cls.empleado.version_id.write({'schedule_pay': 'weekly', 'contract_date_start': date(2020, 1, 6)})

    def _semana(self, lunes, subsidio=None):
        slip = self.env['hr.payslip'].create({
            'name': 'Semana de prueba', 'employee_id': self.empleado.id, 'version_id': self.empleado.version_id.id,
            'struct_id': self.struct.id, 'date_from': lunes, 'date_to': lunes + timedelta(days=6),
        })
        if subsidio is not None:
            self.env['hr.payslip.line'].create({
                'slip_id': slip.id, 'salary_rule_id': self.regla_subsidio.id, 'name': 'SUBSIDY',
                'amount': subsidio, 'quantity': 1.0, 'rate': 100.0, 'total': subsidio, 'sequence': 5,
            })
        return slip

    def test_redondeo_a_la_mitad_hacia_arriba(self):
        Payslip = self.env['hr.payslip']
        self.assertEqual(Payslip._qb_redondeo(904.495), 904.50)
        self.assertEqual(Payslip._qb_redondeo(123.34046, 4), 123.3405)
        self.assertEqual(Payslip._qb_redondeo(30.5644), 30.56)

    def test_sin_subsidio_como_noi(self):
        slip = self._semana(LUNES_46)
        # Clave 1: gravable 6,270.20, NOI retiene 904.50 (Odoo con la diaria × 7: 904.49).
        self.assertEqual(slip._qb_isr_semanal_partes(6270.20), (904.50, 0.0))
        # Clave 313: 2,646.50 rebasa el límite semanal (11,492.66 × 7 ÷ 30.4 = 2,646.34): sin subsidio.
        self.assertEqual(slip._qb_isr_semanal_partes(2646.50), (205.28, 0.0))

    def test_con_subsidio_como_noi(self):
        slip = self._semana(LUNES_46)
        # Clave 12: gravable 2,174.24; NOI: ISR neto 30.56 con subsidio 123.3405.
        isr, subsidio = slip._qb_isr_semanal_partes(2174.24)
        self.assertEqual(subsidio, 123.34)
        self.assertAlmostEqual(isr - subsidio, 30.56, places=2)
        # Clave 304: gravable 1,990.80; NOI: ISR neto 10.60.
        isr, subsidio = slip._qb_isr_semanal_partes(1990.80)
        self.assertAlmostEqual(isr - subsidio, 10.60, places=2)

    def test_subsidio_nunca_mayor_al_isr(self):
        slip = self._semana(LUNES_46)
        isr, subsidio = slip._qb_isr_semanal_partes(800.0)
        self.assertEqual(isr, subsidio)

    def test_tope_del_mes_de_pago(self):
        """Octubre 2026 paga cinco viernes (2, 9, 16, 23 y 30): tras cuatro semanas
        con 123.34 de subsidio (493.36) a la quinta solo le quedan 535.65 −
        493.36 = 42.29 del mes. La semana 49 (30-nov a 6-dic) se paga el 4-dic:
        es de diciembre completa, no se parte por días."""
        for lunes in (date(2026, 9, 28), date(2026, 10, 5), date(2026, 10, 12), date(2026, 10, 19)):
            self._semana(lunes, subsidio=123.34)
        s45 = self._semana(date(2026, 10, 26))
        self.assertAlmostEqual(s45._qb_subsidio_semanal_bruto(2174.24), 42.29, places=2)
        s49 = self._semana(date(2026, 11, 30))
        self.assertAlmostEqual(s49._qb_subsidio_semanal_bruto(2174.24), 123.3405, places=4)

    def test_quincenal_no_cambia(self):
        quincenal = self.env['hr.employee'].create({'name': 'Prueba quincenal'})
        quincenal.version_id.write({'schedule_pay': 'bi-weekly'})
        slip = self.env['hr.payslip'].create({
            'name': 'Quincena', 'employee_id': quincenal.id, 'version_id': quincenal.version_id.id,
            'struct_id': self.struct.id, 'date_from': date(2026, 11, 1), 'date_to': date(2026, 11, 15),
        })
        self.assertIsNone(slip._qb_isr_semanal(5000.0))
        self.assertIsNone(slip._qb_subsidio_semanal(5000.0))

    def test_gravable_en_centavos(self):
        """Semana 44, clave 5: el gravable llega como 3,571.9575 (decimales de las
        horas extra); NOI calcula con 3,571.96 y retiene 344.17 (1.19.1)."""
        slip = self._semana(LUNES_46)
        self.assertEqual(slip._qb_isr_semanal_partes(3571.9575), (344.17, 0.0))

    def test_neto_es_la_suma_de_las_lineas(self):
        """Dos percepciones de 100.004 son dos líneas de 100.00: el neto es 200.00,
        no 200.01. Las cuotas IMSS del trabajador se redondean juntas, como su
        línea de total (1.19.1)."""
        Rule = self.env['hr.salary.rule']
        basic = self.env.ref('hr_payroll.BASIC')
        imss = self.env['hr.salary.rule.category'].search([('code', '=', 'IMSS_EMPLOYEE')], limit=1)
        if not imss:
            self.skipTest('sin la categoría IMSS_EMPLOYEE de la localización')
        codigos = {'QB_A': basic, 'QB_B': basic, 'QB_IMSS1': imss, 'QB_IMSS2': imss}
        for codigo, categoria in codigos.items():
            Rule.create({'name': codigo, 'code': codigo, 'struct_id': self.struct.id,
                         'category_id': categoria.id, 'condition_select': 'none',
                         'amount_select': 'code', 'amount_python_compute': 'result = 0'})
        result_rules = {
            'QB_A': {'total': 100.004}, 'QB_B': {'total': 100.004},
            'QB_IMSS1': {'total': -0.004}, 'QB_IMSS2': {'total': -0.004},
        }
        slip = self._semana(LUNES_46)
        self.assertEqual(slip._qb_neto_lineas(result_rules), 199.99)
