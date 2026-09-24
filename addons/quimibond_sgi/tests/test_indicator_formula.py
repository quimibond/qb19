# -*- coding: utf-8 -*-
"""Modo «fórmula configurable»: términos, ventanas, validación, candado de
edición, regreso a prueba con chatter y corrida en paralelo."""
from datetime import date

from odoo.exceptions import AccessError, ValidationError
from odoo.tests import TransactionCase, tagged
from odoo.tests.common import new_test_user


@tagged('post_install', '-at_install')
class TestIndicatorFormula(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        env = cls.env
        cls.Indicator = env['sgi.indicator']
        cls.Term = env['sgi.indicator.term']
        cls.Measure = env['sgi.indicator.measure']
        cls.model_measure = env['ir.model']._get('sgi.indicator.measure')
        # Fuente: mediciones de un indicador manual, con fechas controladas.
        cls.source = cls.Indicator.create({'code': 'ZF-SRC', 'name': 'Fuente fórmula'})
        for when, value in ((date(2046, 3, 5), 10.0), (date(2046, 3, 20), -30.0),
                            (date(2046, 2, 10), 100.0), (date(2045, 6, 1), 1000.0)):
            cls.Measure.create({'indicator_id': cls.source.id, 'period_date': when,
                                'value': value, 'state': 'capturado'})
        cls.period = (date(2046, 3, 1), date(2046, 3, 31))

    def _term(self, indicator, role, **vals):
        base = {'indicator_id': indicator.id, 'role': role,
                'model_id': self.model_measure.id,
                'domain': "[('indicator_id.code', '=', 'ZF-SRC')]",
                'date_field': 'period_date'}
        base.update(vals)
        return self.Term.create(base)

    def test_01_contar_sumar_y_valor_absoluto(self):
        ind = self.Indicator.create({'code': 'ZF-01', 'name': 'Fórmula %', 'uom': '%',
                                     'calc_mode': 'configurable'})
        self.assertIsNone(ind._calc_configurable(*self.period), "Sin términos: sin dato.")
        self.assertIn('Fórmula', ind._note_configurable(*self.period))
        num = self._term(ind, 'numerator', aggregation='sum_abs', field_name='value')
        den = self._term(ind, 'denominator', aggregation='count')
        detail = ind._detail_configurable(*self.period)
        self.assertEqual((detail['numerator'], detail['denominator']), (40.0, 2.0))
        self.assertEqual(detail['value'], 2000.0, "40 ÷ 2 × 100 por ser %.")
        self.assertEqual(detail['model'], 'sgi.indicator.measure')
        self.assertEqual(len(detail['ids']), 2, "Los registros del numerador.")
        num.write({'aggregation': 'sum', 'factor': -1.0})
        detail = ind._detail_configurable(*self.period)
        self.assertEqual(detail['numerator'], 20.0, "−(10 − 30).")
        ind.uom = 'MXN'
        den.write({'factor': 0.5})
        self.assertEqual(ind._calc_configurable(*self.period), 20.0, "20 ÷ 1, sin ×100.")

    def test_02_ventanas(self):
        ind = self.Indicator.create({'code': 'ZF-02', 'name': 'Ventanas', 'calc_mode': 'configurable'})
        num = self._term(ind, 'numerator', aggregation='sum', field_name='value')
        den = self._term(ind, 'denominator', aggregation='count', window='to_date')
        detail = ind._detail_configurable(*self.period)
        self.assertEqual(detail['numerator'], -20.0, "Solo marzo.")
        self.assertEqual(detail['denominator'], 4.0, "Acumulado al cierre: las cuatro.")
        den.date_field = False
        self.assertEqual(ind._detail_configurable(*self.period)['denominator'], 4.0,
                         "Sin fecha: todo lo que hay.")
        num.window = '3m'
        self.assertEqual(ind._detail_configurable(*self.period)['numerator'], 80.0, "Enero–marzo.")
        num.window = '12m'
        self.assertEqual(ind._detail_configurable(*self.period)['numerator'], 1080.0, "Abril 2045–marzo 2046.")

    def test_03_validacion(self):
        ind = self.Indicator.create({'code': 'ZF-03', 'name': 'Validación'})
        with self.assertRaises(ValidationError):
            self._term(ind, 'numerator', domain="[('no_existe', '=', 1)]")
        with self.assertRaises(ValidationError):
            self._term(ind, 'numerator', domain="__import__('os')")
        with self.assertRaises(ValidationError):
            self._term(ind, 'numerator', date_field='value')
        with self.assertRaises(ValidationError):
            self._term(ind, 'numerator', aggregation='sum', field_name='note')
        with self.assertRaises(ValidationError):
            self._term(ind, 'numerator', factor=0.0)
        with self.assertRaises(ValidationError):
            self._term(ind, 'numerator', date_field=False, window='period')
        self._term(ind, 'denominator', date_field=False, window='to_date')
        self._term(ind, 'numerator')
        with self.assertRaises(Exception):
            self._term(ind, 'numerator')  # un solo numerador por indicador

    def test_04_cambio_regresa_a_prueba_y_queda_en_chatter(self):
        ind = self.Indicator.create({'code': 'ZF-04', 'name': 'Prueba', 'status': 'oficial'})
        term = self._term(ind, 'numerator')
        self.assertEqual(ind.status, 'prueba', "Crear un término regresa a prueba.")
        ind.status = 'oficial'
        term.write({'domain': "[('indicator_id.code', '=', 'ZF-SRC'), ('state', '=', 'capturado')]"})
        self.assertEqual(ind.status, 'prueba')
        bodies = ind.message_ids.mapped('body')
        self.assertTrue(any('Fórmula modificada' in b and 'Antes:' in b for b in bodies))
        ind.status = 'oficial'
        term.write({'factor': 1.0})  # sin cambio real en los campos rastreados
        self.assertEqual(ind.status, 'prueba', "Cualquier escritura de un campo rastreado cuenta.")

    def test_05_corrida_en_paralelo(self):
        ind = self.Indicator.create({'code': 'ZF-05', 'name': 'Paralelo', 'uom': '%',
                                     'calc_mode': 'otif_ventas'})
        vals = ind._sgi_measure_vals(*self.period)
        self.assertNotIn('parallel_value', vals, "Sin fórmula no hay paralelo.")
        self._term(ind, 'numerator', aggregation='sum_abs', field_name='value')
        self._term(ind, 'denominator', aggregation='count')
        vals = ind._sgi_measure_vals(*self.period)
        self.assertEqual((vals['parallel_numerator'], vals['parallel_denominator']), (40.0, 2.0))
        self.assertEqual(vals['parallel_value'], 2000.0)
        measure = self.Measure.create(dict(vals, indicator_id=ind.id, period_date=self.period[0]))
        self.assertEqual(measure.parallel_value, 2000.0)

    def test_06_solo_administrador_edita(self):
        ind = self.Indicator.create({'code': 'ZF-06', 'name': 'Candado'})
        manager = new_test_user(self.env, login='zf_manager',
                                groups='base.group_user,quimibond_sgi.group_sgi_manager')
        admin = new_test_user(self.env, login='zf_admin',
                              groups='base.group_user,quimibond_sgi.group_sgi_admin')
        vals = {'indicator_id': ind.id, 'role': 'numerator', 'model_id': self.model_measure.id,
                'domain': '[]', 'date_field': 'period_date'}
        with self.assertRaises(AccessError):
            self.Term.with_user(manager).create(vals)
        term = self.Term.with_user(admin).create(vals)
        self.assertTrue(term.with_user(manager).read(['domain']), "Todos pueden leerla.")
        with self.assertRaises(AccessError):
            term.with_user(manager).write({'factor': 2.0})
        self.assertFalse(ind.with_user(manager).can_edit_formula)
        self.assertTrue(ind.with_user(admin).can_edit_formula)

    def test_07_formulas_sembradas(self):
        for xmlid in ('sgi_ind_desperdicio', 'sgi_ind_reproceso', 'sgi_ind_diferencia_inventario',
                      'sgi_ind_consumo_energia', 'sgi_ind_ex_compras_ventas'):
            ind = self.env.ref('quimibond_sgi.%s' % xmlid, raise_if_not_found=False)
            if not ind:
                continue
            self.assertTrue(ind.has_formula, "%s trae numerador y denominador." % xmlid)
            ind.term_ids._check_term()
            self.assertNotEqual(ind.calc_mode, 'configurable',
                                "Corre en paralelo: el modo de código sigue.")
