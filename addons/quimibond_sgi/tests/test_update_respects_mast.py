# -*- coding: utf-8 -*-
"""B-001 (auditoría 2026-09): un odoo-update no pisa lo que MAST decidió en
un indicador. Antes, `activate_auto_indicators` corría en cada update y
regresaba a automático cualquier indicador sembrado que MAST hubiera dejado
en manual, y `fix_kpi_seeds` volvía a cambiar objetivo y unidad."""
from odoo.tests import TransactionCase, tagged
from odoo.tools.convert import convert_file

# Los archivos que un update vuelve a cargar y que tocan indicadores.
UPDATE_FILES = (
    'data/sgi_indicators_data.xml',
    'data/sgi_expansion_data.xml',
    'data/sgi_fase7_data.xml',
    'data/sgi_parameters.xml',
)


@tagged('post_install', '-at_install')
class TestUpdateRespectsMast(TransactionCase):

    def _reload(self):
        for filename in UPDATE_FILES:
            convert_file(self.env, 'quimibond_sgi', filename, {}, mode='update', noupdate=False)
        self.env.invalidate_all()

    def test_01_manual_indicators_stay_manual(self):
        seeded = self.env['ir.model.data'].search([
            ('module', '=', 'quimibond_sgi'), ('model', '=', 'sgi.indicator')])
        indicators = self.env['sgi.indicator'].browse(seeded.mapped('res_id')).exists()
        self.assertTrue(indicators, "La base de prueba trae los indicadores sembrados.")
        indicators.write({'calc_mode': 'manual'})
        self._reload()
        flipped = indicators.filtered(lambda ind: ind.calc_mode != 'manual')
        self.assertFalse(flipped.mapped('code'),
                         "El update regresó a automático indicadores que MAST dejó en manual.")

    def test_02_update_does_not_touch_targets_or_uom(self):
        energia = self.env.ref('quimibond_sgi.sgi_ind_consumo_energia', raise_if_not_found=False)
        embarques = self.env.ref('quimibond_sgi.sgi_ind_embarques_sin_error', raise_if_not_found=False)
        if not (energia and embarques):
            self.skipTest("Base sin los indicadores sembrados TR-03 / AL-02.")
        energia.uom = 'kWh'
        embarques.write({'target_objective': 100, 'target_acceptable': 98})
        self._reload()
        self.assertEqual(energia.uom, 'kWh')
        self.assertEqual(embarques.target_objective, 100)

    def test_03_update_runs_only_idempotent_functions(self):
        """El XML que corre en cada update ya no llama a las siembras de un
        solo uso (si alguien las regresa, esta prueba lo dice)."""
        from odoo.tools.misc import file_open
        with file_open('quimibond_sgi/data/sgi_parameters.xml') as handle:
            source = handle.read()
        for name in ('activate_auto_indicators', 'fix_kpi_seeds', 'harden_noupdate',
                     'seed_process_purposes'):
            self.assertNotIn('name="%s"' % name, source)
