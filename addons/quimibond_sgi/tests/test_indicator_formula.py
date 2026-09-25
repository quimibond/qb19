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
        self._term(ind, 'numerator')  # 55.0.0: varios numeradores se suman
        with self.assertRaises(ValidationError):
            self._term(ind, 'numerator', aggregation='count_delta', field_name='period_date',
                       field_name_2='value')  # B no es fecha
        with self.assertRaises(ValidationError):
            self._term(ind, 'numerator', aggregation='avg_delta', field_name='period_date',
                       field_name_2='create_date', delta_unit='same_month')
        with self.assertRaises(ValidationError):
            self._term(ind, 'numerator', domain="[('period_date', '<', '{no_existe}')]")

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

    # ---- 55.0.0 -----------------------------------------------------------
    def test_08_fechas_relativas_en_el_filtro(self):
        ind = self.Indicator.create({'code': 'ZF-08', 'name': 'Relativas', 'calc_mode': 'configurable'})
        num = self._term(ind, 'numerator', domain="[('indicator_id.code', '=', 'ZF-SRC'), "
                                                  "('period_date', '<', '{cierre-20d}')]")
        detail = ind._detail_configurable(*self.period)
        self.assertEqual(detail['numerator'], 1.0, "Cierre 31-mar − 20 d = 11-mar: solo la del 5.")
        resolved = num._sgi_resolve_placeholders(
            "[('a', '<', '{cierre}'), ('b', '>=', '{inicio}'), ('c', '<', '{cierre-2dh}'), "
            "('d', '<', '{cierre+48h}'), ('e', '<=', '{bloqueo}'), ('f', '<', '{hoy}')]", *self.period)
        self.assertIn("'2046-03-31'", resolved)
        self.assertIn("'2046-03-01'", resolved)
        self.assertIn("'2046-03-29'", resolved, "Dos días hábiles antes del sábado 31: jueves 29.")
        self.assertIn("'2046-04-02 00:00:00'", resolved)
        self.assertNotIn('{', resolved)
        self.assertIn(num._sgi_describe(), ind.formula_text)

    def test_09_comparar_dos_fechas_del_registro(self):
        Obl = self.env['sgi.employer.obligation']
        model = self.env['ir.model']._get('sgi.employer.obligation')
        rows = [(date(2046, 3, 1), date(2046, 3, 17), date(2046, 3, 15)),   # a tiempo, 2 días antes
                (date(2046, 3, 1), date(2046, 3, 17), date(2046, 3, 20)),   # 3 días tarde
                (date(2046, 3, 1), date(2046, 3, 17), False)]              # sin presentar
        for period, due, filed in rows:
            Obl.create({'name': 'ZF9 %s' % (filed or 'pendiente'), 'period_date': period,
                        'due_date': due, 'filed_date': filed})
        ind = self.Indicator.create({'code': 'ZF-09', 'name': 'Fechas', 'calc_mode': 'configurable'})
        base = {'model_id': model.id, 'domain': "[('name', 'like', 'ZF9')]", 'date_field': 'due_date'}
        num = self._term(ind, 'numerator', aggregation='count_delta', field_name='due_date',
                         field_name_2='filed_date', delta_unit='days', delta_op='<=', delta_value=0, **base)
        den = self._term(ind, 'denominator', aggregation='count', **base)
        detail = ind._detail_configurable(*self.period)
        self.assertEqual((detail['numerator'], detail['denominator']), (1.0, 3.0), "Solo la presentada a tiempo.")
        self.assertEqual(len(detail['ids']), 1, "El detalle trae solo los registros que cumplen.")
        num.write({'delta_op': '>', 'delta_value': 0})
        self.assertEqual(ind._detail_configurable(*self.period)['numerator'], 1.0, "La tarde.")
        num.write({'aggregation': 'avg_delta', 'delta_unit': 'days'})
        self.assertEqual(ind._detail_configurable(*self.period)['numerator'], 0.5, "(−2 + 3) ÷ 2.")
        num.write({'aggregation': 'count_delta', 'delta_unit': 'next_month_day', 'delta_value': 5,
                   'field_name': 'period_date', 'field_name_2': 'filed_date'})
        self.assertEqual(ind._detail_configurable(*self.period)['numerator'], 2.0,
                         "Presentadas a más tardar el 5 de abril: las dos con fecha.")
        num.write({'delta_unit': 'same_month', 'field_name': 'due_date'})
        self.assertEqual(ind._detail_configurable(*self.period)['numerator'], 2.0, "Mismo mes que el vencimiento.")
        num.write({'delta_unit': 'business_days', 'delta_op': '<=', 'delta_value': 2,
                   'field_name': 'due_date'})
        self.assertEqual(ind._detail_configurable(*self.period)['numerator'], 2.0,
                         "Hábiles: del sábado 17 al martes 20 son 2 (lunes y martes); la del 15 da 0.")
        num.write({'delta_value': 1})
        self.assertEqual(ind._detail_configurable(*self.period)['numerator'], 1.0, "Solo la del 15.")
        den.unlink()

    def test_10_solo_conteo(self):
        ind = self.Indicator.create({'code': 'ZF-10', 'name': 'Conteo', 'calc_mode': 'configurable'})
        self._term(ind, 'numerator', domain="[('indicator_id.code', '=', 'ZF-SRC'), ('value', '>', 0)]")
        self.assertTrue(ind.has_formula, "Con numerador basta.")
        detail = ind._detail_configurable(*self.period)
        self.assertEqual((detail['value'], detail['numerator'], detail['denominator']), (1.0, 1.0, None))
        vals = ind._sgi_measure_vals(date(2040, 1, 1), date(2040, 1, 31))
        self.assertEqual((vals['state'], vals['value']), ('capturado', 0.0), "Sin registros = 0, no sin dato.")

    def test_11_varios_terminos_se_suman(self):
        ind = self.Indicator.create({'code': 'ZF-11', 'name': 'Suma', 'calc_mode': 'configurable'})
        self._term(ind, 'numerator', aggregation='sum', field_name='value')                # 10 − 30
        self._term(ind, 'numerator', aggregation='sum_abs', field_name='value', factor=-1)  # −40
        self._term(ind, 'denominator', aggregation='count')
        self._term(ind, 'denominator', aggregation='count')
        detail = ind._detail_configurable(*self.period)
        self.assertEqual((detail['numerator'], detail['denominator']), (-60.0, 4.0))
        self.assertEqual(detail['value'], -15.0)

    def test_12_recalcular_bajo_demanda(self):
        ind = self.Indicator.create({'code': 'ZF-12', 'name': 'Recalcular', 'calc_mode': 'configurable'})
        self._term(ind, 'numerator')
        result = ind.sgi_recalculate(period_date='2046-03-01')[0]
        self.assertEqual((result['code'], result['value'], result['state'], result['measure_id']),
                         ('ZF-12', 2.0, 'capturado', False))
        result = ind.sgi_recalculate(period_date=date(2046, 3, 1), save=True)[0]
        measure = self.Measure.browse(result['measure_id'])
        self.assertEqual((measure.value, measure.period_date), (2.0, date(2046, 3, 1)))
        measure.sudo().write({'state': 'validado'})
        result = ind.sgi_recalculate(period_date=date(2046, 3, 1), save=True)[0]
        self.assertIn('validada', result['note'])
        self.assertTrue(ind._sgi_default_period() < date.today())
        self.assertTrue(ind.action_recalculate_now())
