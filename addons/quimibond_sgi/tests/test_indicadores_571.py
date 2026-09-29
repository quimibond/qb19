# -*- coding: utf-8 -*-
"""57.1.0: diagnóstico de los indicadores que daban 0.

- TR-01 pasa del modo de código ``cierre_nc`` (retirado) a fórmula
  configurable. La fórmula mide lo mismo que medía el modo cuando funcionaba
  (NC del SGI cerradas en el periodo ÷ NC levantadas en el periodo) y además
  deja fuera las canceladas del denominador.
- La migración pasa a fórmula solo lo que sigue en ``cierre_nc``, respeta
  los términos que ya tenga y deja el modo anterior en el chatter.
- «Último cálculo» (``calc_status``/``calc_checked``) se llena con la última
  medición cuando está vacío, sin esperar al siguiente cierre.
"""
import datetime
from datetime import date

from odoo.tests import TransactionCase, tagged

PERIOD = (date(2044, 3, 1), date(2044, 3, 31))


@tagged('post_install', '-at_install')
class TestIndicadores571(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.Indicator = cls.env['sgi.indicator']
        cls.Alert = cls.env['quality.alert']
        cls.team = cls.env.ref('quimibond_sgi.sgi_quality_team_internal')
        cls.stage_cancel = cls.env.ref('quimibond_sgi.sgi_nc_int_stage_cancel')

    # ---- ayudas --------------------------------------------------------------
    def _nc(self, created, closed=None, cancelled=False):
        """NC del SGI con fechas fijas. Las fechas y la etapa van por SQL:
        create_date no se escribe por ORM y la cancelación tiene candado."""
        alert = self.Alert.create({'title': 'NC 57.1.0', 'team_id': self.team.id})
        self.assertTrue(alert.sgi_folio, "El equipo interno del SGI pone folio.")
        self.env.cr.execute(
            "UPDATE quality_alert SET create_date = %s, date_close = %s WHERE id = %s",
            (created, closed, alert.id))
        if cancelled:
            self.env.cr.execute("UPDATE quality_alert SET stage_id = %s WHERE id = %s",
                                (self.stage_cancel.id, alert.id))
        alert.invalidate_recordset()
        return alert

    def _old_cierre_nc(self, date_from, date_to):
        """El cálculo del modo retirado ``cierre_nc`` (sgi_indicator.py hasta
        57.0.0), como referencia."""
        indicator = self.Indicator
        dt_from, dt_to = indicator._sgi_dt_bounds(date_from, date_to)
        detected = self.Alert.search_count([
            ('sgi_folio', '!=', False),
            ('create_date', '>=', dt_from), ('create_date', '<', dt_to)])
        if not detected:
            return None
        closed = self.Alert.search_count([
            ('sgi_folio', '!=', False),
            ('date_close', '>=', dt_from), ('date_close', '<', dt_to)])
        return round(closed / detected * 100.0, 2)

    def _tr01(self):
        indicator = self.Indicator.create({
            'code': 'Z571-TR01', 'name': 'Cierre de NC (prueba)', 'uom': '%',
            'calc_mode': 'configurable'})
        self.Indicator._sgi_cierre_nc_formula([indicator.id])
        return indicator

    # ---- TR-01 ------------------------------------------------------------
    def test_01_formula_mide_lo_mismo_que_el_modo(self):
        dt = datetime.datetime
        self._nc(dt(2044, 3, 5, 10), dt(2044, 3, 20, 10))   # levantada y cerrada en marzo
        self._nc(dt(2044, 3, 10, 10))                        # levantada en marzo, abierta
        self._nc(dt(2044, 2, 15, 10), dt(2044, 3, 2, 10))   # de febrero, cerrada en marzo
        self._nc(dt(2044, 3, 12, 10), dt(2044, 4, 2, 10))   # de marzo, cerrada en abril
        indicator = self._tr01()
        self.assertEqual(indicator.calc_mode, 'configurable')
        self.assertEqual(len(indicator.term_ids), 2)
        old = self._old_cierre_nc(*PERIOD)
        self.assertEqual(old, 66.67, "2 cerradas en marzo ÷ 3 levantadas en marzo.")
        detail = indicator._detail_configurable(*PERIOD)
        self.assertEqual(detail['value'], old, "La fórmula da lo mismo que el modo de código.")
        self.assertEqual((detail['numerator'], detail['denominator']), (2.0, 3.0))
        self.assertEqual(detail['model'], 'quality.alert')
        self.assertEqual(len(detail['ids']), 2, "Guarda las NC cerradas del periodo.")

        # Una cancelada: el modo de código la contaba (2 ÷ 4 = 50 %), la
        # fórmula no (sigue en 2 ÷ 3).
        self._nc(dt(2044, 3, 15, 10), cancelled=True)
        self.assertEqual(self._old_cierre_nc(*PERIOD), 50.0)
        self.assertEqual(indicator._detail_configurable(*PERIOD)['value'], 66.67)

    def test_02_mes_sin_nc_es_sin_dato(self):
        """Agosto de 2026 en producción: todas las NC del mes canceladas.
        El modo de código daba 0 % (0 ÷ 17); la fórmula da «sin dato»."""
        dt = datetime.datetime
        self._nc(dt(2044, 5, 3, 10), cancelled=True)
        self._nc(dt(2044, 5, 4, 10), cancelled=True)
        indicator = self._tr01()
        self.assertEqual(self._old_cierre_nc(date(2044, 5, 1), date(2044, 5, 31)), 0.0)
        vals = indicator._sgi_measure_vals(date(2044, 5, 1), date(2044, 5, 31))
        self.assertEqual(vals['state'], 'sin_dato')

    def test_03_migracion_respeta_lo_de_mast(self):
        legacy = self.Indicator.create({'code': 'Z571-A', 'name': 'Legado', 'uom': '%',
                                        'calc_mode': 'manual'})
        own = self.Indicator.create({'code': 'Z571-B', 'name': 'Con fórmula propia', 'uom': '%',
                                     'calc_mode': 'manual'})
        other = self.Indicator.create({'code': 'Z571-C', 'name': 'Otro modo', 'uom': '%',
                                       'calc_mode': 'manual'})
        model = self.env['ir.model']._get('quality.alert')
        own.write({'term_ids': [(0, 0, {'role': 'numerator', 'model_id': model.id,
                                         'domain': "[('sgi_folio', '!=', False)]",
                                         'date_field': 'create_date'})]})
        own_terms = own.term_ids
        self.env.cr.execute("UPDATE sgi_indicator SET calc_mode = 'cierre_nc' WHERE id IN %s",
                            (tuple((legacy | own).ids),))
        (legacy | own).invalidate_recordset()

        done = self.Indicator._sgi_cierre_nc_formula()
        self.assertEqual(set(done), {legacy.id, own.id})
        self.assertEqual(legacy.calc_mode, 'configurable')
        self.assertEqual(len(legacy.term_ids), 2)
        self.assertEqual(own.calc_mode, 'configurable')
        self.assertEqual(own.term_ids, own_terms, "Los términos que ya tenía se quedan.")
        self.assertEqual(other.calc_mode, 'manual', "Otro modo no se toca.")
        self.assertIn("cierre_nc", legacy.message_ids[:1].body or '',
                      "El modo anterior queda en el chatter.")
        self.assertEqual(self.Indicator._sgi_cierre_nc_formula(), [], "Idempotente.")

    def test_04_tr01_sembrado_es_formula(self):
        tr01 = self.env.ref('quimibond_sgi.sgi_ind_cierre_nc', raise_if_not_found=False)
        if not tr01:
            self.skipTest("Base sin la siembra de TR-01.")
        self.assertEqual(tr01.calc_mode, 'configurable')
        self.assertTrue(tr01.has_formula)
        self.assertNotIn('cierre_nc', dict(tr01._fields['calc_mode'].selection),
                         "El modo de código se retiró.")

    # ---- calc_status / calc_checked ---------------------------------------
    def test_05_ultimo_calculo_desde_la_ultima_medicion(self):
        Measure = self.env['sgi.indicator.measure']
        empty = self.Indicator.create({'code': 'Z571-D', 'name': 'Sin dato', 'uom': '%',
                                       'calc_mode': 'rotacion_rh'})
        good = self.Indicator.create({'code': 'Z571-E', 'name': 'Calcula', 'uom': '%',
                                      'calc_mode': 'rotacion_rh'})
        manual = self.Indicator.create({'code': 'Z571-F', 'name': 'Manual', 'calc_mode': 'manual'})
        known = self.Indicator.create({'code': 'Z571-G', 'name': 'Ya diagnosticado',
                                       'calc_mode': 'rotacion_rh'})
        never = self.Indicator.create({'code': 'Z571-H', 'name': 'Nunca medido',
                                       'calc_mode': 'rotacion_rh'})
        known._sgi_set_calc('error', "falla previa")
        checked = known.calc_checked
        Measure.create({'indicator_id': empty.id, 'period_date': date(2044, 2, 1),
                        'value': 5.0, 'state': 'capturado'})
        Measure.create({'indicator_id': empty.id, 'period_date': date(2044, 3, 1),
                        'value': 0.0, 'state': 'sin_dato', 'note': 'Sin bajas.'})
        Measure.create({'indicator_id': good.id, 'period_date': date(2044, 3, 1),
                        'value': 4.0, 'state': 'capturado'})
        Measure.create({'indicator_id': known.id, 'period_date': date(2044, 3, 1),
                        'value': 4.0, 'state': 'capturado'})
        self.assertFalse(empty.calc_status, "Una medición creada a mano no diagnostica.")

        self.Indicator._sgi_calc_status_backfill()
        self.assertIn(empty.calc_status, ('sin_datos', 'sin_formula'),
                      "Toma la ÚLTIMA medición (marzo, sin dato), no la de febrero.")
        self.assertIn("03/2044", empty.calc_message)
        self.assertTrue(empty.calc_checked)
        self.assertEqual(good.calc_status, 'ok')
        self.assertEqual(manual.calc_status, 'manual')
        self.assertEqual((known.calc_status, known.calc_checked), ('error', checked),
                         "Un indicador ya diagnosticado no cambia.")
        self.assertFalse(never.calc_status, "Sin mediciones lo diagnostica el cron al medir.")

    def test_06_recalcular_la_medicion_deja_el_motivo(self):
        indicator = self.Indicator.create({'code': 'Z571-I', 'name': 'Recalcula',
                                           'uom': '%', 'calc_mode': 'rotacion_rh'})
        measure = self.env['sgi.indicator.measure'].create({
            'indicator_id': indicator.id, 'period_date': date(2044, 3, 1),
            'value': 0.0, 'state': 'pendiente'})
        measure.action_recompute_value()
        self.assertTrue(indicator.calc_status)
        self.assertTrue(indicator.calc_checked)
