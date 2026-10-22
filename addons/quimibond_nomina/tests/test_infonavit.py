# -*- coding: utf-8 -*-
"""Retención de Infonavit en cuota fija: bimestre completo entre 9 o 4, y el
primer bimestre de un crédito que empieza a la mitad, como NOI (Reyes Sánchez,
quincena 20 de 2026: 1,171.49).

No corre en el CI (depende de Enterprise); se corre en el shell de Odoo.sh:
``odoo-bin ... --test-tags /quimibond_nomina --stop-after-init``."""
from datetime import date

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
