# -*- coding: utf-8 -*-
"""Retención de Infonavit en cuota fija: bimestre completo entre los pagos del
bimestre (8 o 9 semanas, 4 quincenas; 1.17.0), y el
primer bimestre de un crédito que empieza a la mitad, como NOI (Reyes Sánchez,
quincena 20 de 2026: 1,171.49).

No corre en el CI (depende de Enterprise); se corre en el shell de Odoo.sh:
``odoo-bin ... --test-tags /quimibond_nomina --stop-after-init``."""
from datetime import date, timedelta

from odoo.tests.common import TransactionCase

from .common import liberar_referencias


class TestInfonavit(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        env = cls.env
        stype = env['hr.payroll.structure.type'].create({'name': 'QB Infonavit prueba'})
        cls.struct = env['hr.payroll.structure'].create({'name': 'QB Infonavit prueba', 'code': 'QB_INFO',
                                                          'type_id': stype.id})
        env['ir.config_parameter'].sudo().set_param('quimibond_nomina.dia_pago_semanal', '4')
        liberar_referencias(env, 'Q-9')

    def _empleado(self, periodicidad, inicio_contrato, cuota, inicio_credito=None):
        empleado = self.env['hr.employee'].create({'name': 'Prueba Infonavit', 'registration_number': 'Q-9'})
        empleado.version_id.write({'schedule_pay': periodicidad, 'contract_date_start': inicio_contrato})
        self.env['l10n.mx.hr.infonavit'].create({
            'version_id': empleado.version_id.id, 'status': 'in_progress',
            'infonavit_type': 'fixed_monetary_fee', 'fixed_monetary_fee': cuota,
            'qb_fecha_inicio': inicio_credito,
        })
        return empleado

    def _recibo(self, empleado, date_from, date_to):
        return self.env['hr.payslip'].create({
            'name': 'Recibo de prueba', 'employee_id': empleado.id, 'version_id': empleado.version_id.id,
            'struct_id': self.struct.id, 'date_from': date_from, 'date_to': date_to,
        })

    def test_bimestre_completo_quincenal(self):
        empleado = self._empleado('bi-weekly', date(2020, 1, 6), 5351.70)
        slip = self._recibo(empleado, date(2026, 10, 1), date(2026, 10, 15))
        self.assertAlmostEqual(slip._qb_infonavit_retencion(), 5351.70 / 4, places=2)

    def test_bimestre_completo_semanal(self):
        empleado = self._empleado('weekly', date(2020, 1, 6), 3650.24)
        slip = self._recibo(empleado, date(2026, 10, 12), date(2026, 10, 18))
        self.assertAlmostEqual(slip._qb_infonavit_retencion(), 3650.24 / 9, places=2)

    def test_primer_bimestre_como_noi(self):
        """Aviso del 22-sep por 2,668.35 mensuales: 40 de 61 días del bimestre
        septiembre–octubre + 15 de seguro, entre las quincenas 19, 20 y 21."""
        empleado = self._empleado('bi-weekly', date(2026, 9, 22), 5351.70, date(2026, 9, 22))
        q20 = self._recibo(empleado, date(2026, 10, 1), date(2026, 10, 15))
        self.assertAlmostEqual(q20._qb_infonavit_retencion(), 1171.49, places=2)
        q19 = self._recibo(empleado, date(2026, 9, 16), date(2026, 9, 30))
        self.assertAlmostEqual(q19._qb_infonavit_retencion(), 1171.49, places=2)

    def test_siguiente_bimestre_completo(self):
        empleado = self._empleado('bi-weekly', date(2026, 9, 22), 5453.44, date(2026, 9, 22))
        q22 = self._recibo(empleado, date(2026, 11, 1), date(2026, 11, 15))
        self.assertAlmostEqual(q22._qb_infonavit_retencion(), 5453.44 / 4, places=2)

    def test_credito_que_aun_no_empieza(self):
        empleado = self._empleado('bi-weekly', date(2020, 1, 6), 5351.70, date(2026, 10, 20))
        q20 = self._recibo(empleado, date(2026, 10, 1), date(2026, 10, 15))
        self.assertEqual(q20._qb_infonavit_retencion(), 0.0)
        q21 = self._recibo(empleado, date(2026, 10, 16), date(2026, 10, 31))
        # 12 de 61 días + seguro, todo en la quincena 21 (la única que queda).
        self.assertAlmostEqual(q21._qb_infonavit_retencion(), 5336.70 * 12 / 61 + 15, places=2)

    def test_contrato_posterior_al_aviso(self):
        """Si el contrato empieza después del aviso, cuenta el contrato."""
        empleado = self._empleado('bi-weekly', date(2026, 9, 22), 5351.70, date(2026, 9, 1))
        q20 = self._recibo(empleado, date(2026, 10, 1), date(2026, 10, 15))
        self.assertAlmostEqual(q20._qb_infonavit_retencion(), 1171.49, places=2)

    def test_divisor_noviembre_diciembre(self):
        """Noviembre–diciembre de 2026 tiene 8 viernes: NOI retuvo 2,158.72 ÷ 8 =
        269.84 a Filiberta Florentino en la semana 46 (Odoo 1.16.2: ÷ 9 = 239.86)."""
        empleado = self._empleado('weekly', date(2020, 1, 6), 2158.72)
        s46 = self._recibo(empleado, date(2026, 11, 2), date(2026, 11, 8))
        self.assertEqual(s46._qb_pagos_en_bimestre(), 8)
        self.assertAlmostEqual(s46._qb_infonavit_retencion(), 269.84, places=2)
        # La semana 45 se paga el viernes 30-oct: es de septiembre–octubre (9).
        s45 = self._recibo(empleado, date(2026, 10, 26), date(2026, 11, 1))
        self.assertEqual(s45._qb_pagos_en_bimestre(), 9)
        self.assertAlmostEqual(s45._qb_infonavit_retencion(), 2158.72 / 9, places=2)

    def test_divisor_por_bimestre_semanal(self):
        empleado = self._empleado('weekly', date(2020, 1, 6), 3650.24)
        casos = [
            (date(2026, 9, 7), 9), (date(2026, 11, 9), 8), (date(2027, 1, 4), 9),
            (date(2027, 3, 1), 9), (date(2026, 1, 5), 9), (date(2026, 7, 6), 9),
        ]
        for lunes, esperado in casos:
            with self.subTest(semana=lunes):
                slip = self._recibo(empleado, lunes, lunes + timedelta(days=6))
                self.assertEqual(slip._qb_pagos_en_bimestre(), esperado)

    def test_divisor_quincenal_siempre_cuatro(self):
        empleado = self._empleado('bi-weekly', date(2020, 1, 6), 5351.70)
        q22 = self._recibo(empleado, date(2026, 11, 1), date(2026, 11, 15))
        self.assertEqual(q22._qb_pagos_en_bimestre(), 4)
        self.assertAlmostEqual(q22._qb_infonavit_retencion(), 5351.70 / 4, places=2)

    def test_divisor_sigue_el_dia_de_pago(self):
        """Con el día de pago en jueves, noviembre–diciembre de 2026 tiene 9."""
        empleado = self._empleado('weekly', date(2020, 1, 6), 2158.72)
        self.env['ir.config_parameter'].sudo().set_param('quimibond_nomina.dia_pago_semanal', '3')
        s46 = self._recibo(empleado, date(2026, 11, 2), date(2026, 11, 8))
        self.assertEqual(s46._qb_pagos_en_bimestre(), 9)
