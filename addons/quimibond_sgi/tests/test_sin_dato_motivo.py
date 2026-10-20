# -*- coding: utf-8 -*-
"""57.169.0 — «Sin dato» dice por qué (Jose 2026-10-20).

Antes las cuatro causas se veían igual: foto (el pasado no se reconstruye),
falta configurar, la fuente no tiene registros y sin casos en el periodo. La
ficha del indicador ya no lista las de foto y «Dónde se arregla» abre la
pantalla de lo que falta configurar."""
from datetime import date

from odoo.tests import TransactionCase, tagged

from ..models.sgi_indicator_detail import SNAPSHOT_NOTE, sgi_no_data_reason


@tagged('post_install', '-at_install')
class TestSinDatoMotivo(TransactionCase):

    def test_01_causas(self):
        self.assertFalse(sgi_no_data_reason('capturado', SNAPSHOT_NOTE))
        self.assertEqual(sgi_no_data_reason('sin_dato', SNAPSHOT_NOTE + " Valor anterior 5."), 'foto')
        self.assertEqual(sgi_no_data_reason('sin_dato', "Antes de «Medir desde» (01/03/2026): no se mide."),
                         'antes')
        self.assertEqual(sgi_no_data_reason('sin_dato', "Registro vacío: Incidente no tiene ningún registro"),
                         'fuente_vacia')
        self.assertEqual(sgi_no_data_reason('sin_dato', "Configure la capacidad instalada mensual en Ajustes"),
                         'falta_config')
        self.assertEqual(sgi_no_data_reason('sin_dato', False, 'sin_datos', "Ningún puesto tiene «Plantilla»"),
                         'falta_config', "Sin nota, el motivo del indicador decide.")
        self.assertEqual(sgi_no_data_reason('sin_dato', False, 'sin_formula'), 'falta_config')
        self.assertEqual(sgi_no_data_reason('sin_dato', "Sin acuerdos con fecha límite en el periodo."),
                         'sin_casos')

    def test_02_ficha_y_donde_se_arregla(self):
        indicator = self.env['sgi.indicator'].create({
            'code': 'ZSD-01', 'name': 'Producido vs capacidad ZSD', 'calc_mode': 'produccion_vs_capacidad',
            'frequency': 'monthly'})
        Measure = self.env['sgi.indicator.measure'].with_context(sgi_calc_write=True)
        photo = Measure.create({'indicator_id': indicator.id, 'period_date': date(2026, 1, 1),
                                'state': 'sin_dato', 'note': SNAPSHOT_NOTE})
        config = Measure.create({'indicator_id': indicator.id, 'period_date': date(2026, 2, 1),
                                 'state': 'sin_dato', 'note': "Configure la capacidad instalada mensual."})
        self.assertEqual(photo.no_data_reason, 'foto')
        self.assertEqual(config.no_data_reason, 'falta_config')
        self.assertIn(config, indicator.measure_shown_ids)
        self.assertNotIn(photo, indicator.measure_shown_ids, "La ficha no lista las de foto.")
        self.assertIn(photo, indicator.measure_ids, "Siguen existiendo.")
        self.assertFalse(indicator.calc_fix_menu_id)
        indicator._sgi_set_calc('sin_datos', "Configure la capacidad instalada mensual en Ajustes.")
        self.assertEqual(indicator.calc_fix_menu_id, self.env.ref('quimibond_sgi.menu_sgi_config_settings'))
        self.assertIn(indicator.action_open_calc_fix()['type'], ('ir.actions.act_window', 'ir.actions.client'))
