# -*- coding: utf-8 -*-
"""FONACOT e Infonavit: sin sueldo no se retiene, y lo que faltó se reparte
entre las nóminas que quedan, como NOI (Filiberta Florentino, semanas 42 a 44
de 2026; Jorge Matías Salinas, alta del 23-sep-2026).

No corre en el CI (depende de Enterprise); se corre en el shell de Odoo.sh:
``odoo-bin ... --test-tags /quimibond_nomina --stop-after-init``."""
from datetime import date, timedelta

from odoo import fields

from odoo.tests.common import TransactionCase

from .common import liberar_referencias

# Semanas de 2026 (lunes): 37 = 31-ago … 44 = 19-oct; se pagan el viernes.
LUNES = {n: date(2026, 8, 31) + timedelta(weeks=n - 37) for n in range(37, 46)}


class TestRetenciones(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        env = cls.env
        stype = env['hr.payroll.structure.type'].create({'name': 'QB Retenciones prueba'})
        cls.struct = env['hr.payroll.structure'].create({'name': 'QB Retenciones prueba', 'code': 'QB_RET',
                                                          'type_id': stype.id})
        cat = env.ref('hr_payroll.BASIC')
        cls.rules = {
            code: env['hr.salary.rule'].create({
                'name': code, 'code': code, 'struct_id': cls.struct.id, 'category_id': cat.id,
                'condition_select': 'none', 'amount_select': 'code', 'amount_python_compute': 'result = 0',
            })
            for code in ('FONACOT', 'INFONAVIT')
        }
        cls.Param = env['ir.config_parameter'].sudo()
        cls.Param.set_param('quimibond_nomina.dia_pago_semanal', '4')
        cls.Param.set_param('quimibond_nomina.isr_mensual_incluye_borrador', '')
        liberar_referencias(env, 'S-87', 'S-250')

    def _empleado(self, referencia, inicio_contrato, infonavit=0.0, inicio_credito=None, fonacot=()):
        empleado = self.env['hr.employee'].create({'name': 'Prueba retenciones', 'registration_number': referencia})
        empleado.version_id.write({'schedule_pay': 'weekly', 'contract_date_start': inicio_contrato})
        if infonavit:
            self.env['l10n.mx.hr.infonavit'].create({
                'version_id': empleado.version_id.id, 'status': 'in_progress',
                'infonavit_type': 'fixed_monetary_fee', 'fixed_monetary_fee': infonavit,
                'qb_fecha_inicio': inicio_credito,
            })
        for importe in fonacot:
            self.env['l10n.mx.hr.fonacot'].create({
                'version_id': empleado.version_id.id, 'status': 'in_progress', 'monthly_import': importe,
            })
        return empleado

    def _semana(self, empleado, n, lineas=None, state='validated'):
        slip = self.env['hr.payslip'].create({
            'name': 'Semana %d' % n, 'employee_id': empleado.id, 'version_id': empleado.version_id.id,
            'struct_id': self.struct.id, 'date_from': LUNES[n], 'date_to': LUNES[n] + timedelta(days=6),
        })
        for code, total in (lineas or {}).items():
            self.env['hr.payslip.line'].create({
                'slip_id': slip.id, 'salary_rule_id': self.rules[code].id, 'name': code,
                'amount': total, 'quantity': 1.0, 'rate': 100.0, 'total': total, 'sequence': 5,
            })
        vals = {'state': state}
        if state in ('validated', 'paid'):
            # hr_payroll compara la versión contra done_date; sin ella revienta al recalcular.
            vals['done_date'] = fields.Datetime.now()
        slip.write(vals)
        return slip

    # --- Sin sueldo no se retiene --------------------------------------
    def test_sin_gravable_no_retiene(self):
        empleado = self._empleado('S-87', date(2021, 11, 16), 2158.72, fonacot=(450.09, 1489.47))
        semana42 = self._semana(empleado, 42, state='draft')
        self.assertEqual(semana42._qb_fonacot_retencion(0.0), 0.0)
        self.assertEqual(semana42._qb_infonavit_retencion(0.0), 0.0)
        self.assertAlmostEqual(semana42._qb_fonacot_retencion(2375.94), 1939.56 / 5, places=2)

    # --- Sin faltantes, la cuota de siempre -----------------------------
    def test_sin_faltante_cuota_normal(self):
        empleado = self._empleado('S-87', date(2021, 11, 16), 2158.72, fonacot=(450.09, 1489.47))
        for n in range(37, 43):
            self._semana(empleado, n, {'INFONAVIT': -239.86, 'FONACOT': -387.91 if n >= 41 else -484.89})
        semana43 = self._semana(empleado, 43, state='draft')
        self.assertAlmostEqual(semana43._qb_fonacot_retencion(2935.98), 1939.56 / 5, places=4)
        self.assertAlmostEqual(semana43._qb_infonavit_retencion(2935.98), 2158.72 / 9, places=4)

    # --- Filiberta Florentino: incapacidad en la semana 42 --------------
    def test_filiberta_recupera_fonacot_e_infonavit(self):
        empleado = self._empleado('S-87', date(2021, 11, 16), 2158.72, fonacot=(450.09, 1489.47))
        for n in range(37, 41):
            self._semana(empleado, n, {'INFONAVIT': -239.84, 'FONACOT': -484.89})
        self._semana(empleado, 41, {'INFONAVIT': -239.84, 'FONACOT': -387.91})
        self._semana(empleado, 42, {'INFONAVIT': 0.0, 'FONACOT': 0.0})
        semana43 = self._semana(empleado, 43, state='draft')
        # NOI: 517.21 (sobre 1,939.55) y 319.84.
        self.assertAlmostEqual(semana43._qb_fonacot_retencion(2935.98), (1939.56 - 387.91) / 3, places=4)
        self.assertAlmostEqual(semana43._qb_infonavit_retencion(2935.98), 319.84, places=2)
        # La 43 queda como la cerró NOI y se calcula la 44.
        for code, total in (('INFONAVIT', -319.84), ('FONACOT', -517.21)):
            self.env['hr.payslip.line'].create({
                'slip_id': semana43.id, 'salary_rule_id': self.rules[code].id, 'name': code,
                'amount': total, 'quantity': 1.0, 'rate': 100.0, 'total': total, 'sequence': 5,
            })
        semana43.write({'state': 'validated', 'done_date': fields.Datetime.now()})
        semana44 = self._semana(empleado, 44, state='draft')
        self.assertAlmostEqual(semana44._qb_infonavit_retencion(2385.91), 319.84, places=2)
        self.assertAlmostEqual(semana44._qb_fonacot_retencion(2385.91), (1939.56 - 387.91 - 517.21) / 2, places=4)

    # --- Jorge Matías Salinas: aviso después de la primera semana --------
    def test_matias_alta_a_la_mitad_y_aviso_tardio(self):
        empleado = self._empleado('S-250', date(2026, 9, 23), 3650.24, date(2026, 9, 23))
        self._semana(empleado, 40)          # sin aviso todavía: no retuvo
        semana41 = self._semana(empleado, 41, state='draft')
        debe = (3650.24 - 15) * 39 / 61 + 15
        self.assertAlmostEqual(debe, 2339.17, places=2)
        self.assertAlmostEqual(semana41._qb_infonavit_retencion(2763.32), 467.83, places=2)

    # --- Cuándo no se recupera -------------------------------------------
    def test_historial_incompleto_cuota_normal(self):
        """Faltan las semanas 37 a 41: no se sabe cuánto se retuvo."""
        empleado = self._empleado('S-87', date(2021, 11, 16), 2158.72, fonacot=(450.09, 1489.47))
        self._semana(empleado, 42, {'INFONAVIT': 0.0, 'FONACOT': 0.0})
        semana43 = self._semana(empleado, 43, state='draft')
        self.assertAlmostEqual(semana43._qb_infonavit_retencion(2935.98), 2158.72 / 9, places=4)
        self.assertAlmostEqual(semana43._qb_fonacot_retencion(2935.98), 1939.56 / 5, places=4)

    def test_previo_en_borrador_cuota_normal_salvo_piloto(self):
        empleado = self._empleado('S-87', date(2021, 11, 16), fonacot=(450.09, 1489.47))
        self._semana(empleado, 41, {'FONACOT': -387.91})
        self._semana(empleado, 42, {'FONACOT': 0.0}, state='draft')
        semana43 = self._semana(empleado, 43, state='draft')
        self.assertAlmostEqual(semana43._qb_fonacot_retencion(2935.98), 1939.56 / 5, places=4)
        self.Param.set_param('quimibond_nomina.isr_mensual_incluye_borrador', '1')
        self.assertAlmostEqual(semana43._qb_fonacot_retencion(2935.98), (1939.56 - 387.91) / 3, places=4)

    def test_quincenal(self):
        """La primera quincena no retuvo: la segunda retiene el mes completo."""
        empleado = self._empleado('S-87', date(2021, 11, 16), fonacot=(1354.0,))
        empleado.version_id.write({'schedule_pay': 'bi-weekly'})
        primera = self.env['hr.payslip'].create({
            'name': 'Primera de octubre', 'employee_id': empleado.id, 'version_id': empleado.version_id.id,
            'struct_id': self.struct.id, 'date_from': date(2026, 10, 1), 'date_to': date(2026, 10, 15),
        })
        self.env['hr.payslip.line'].create({
            'slip_id': primera.id, 'salary_rule_id': self.rules['FONACOT'].id, 'name': 'FONACOT',
            'amount': 0.0, 'quantity': 1.0, 'rate': 100.0, 'total': 0.0, 'sequence': 5,
        })
        primera.write({'state': 'validated'})
        segunda = self.env['hr.payslip'].create({
            'name': 'Segunda de octubre', 'employee_id': empleado.id, 'version_id': empleado.version_id.id,
            'struct_id': self.struct.id, 'date_from': date(2026, 10, 16), 'date_to': date(2026, 10, 31),
        })
        self.assertAlmostEqual(segunda._qb_fonacot_retencion(8000.0), 1354.0, places=2)
