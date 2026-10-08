# -*- coding: utf-8 -*-
"""57.133.0 (C1, Jose 5.4): pilotaje (primeros lotes) y estudio de habilidad."""
from odoo.exceptions import UserError
from odoo.tests import TransactionCase, tagged

from ..models.sgi_dev_pilot import PARAM_CPK_MIN, PARAM_LOTS, PARAM_READINGS, capability


@tagged('post_install', '-at_install')
class TestDevPilot(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        m = cls.env.ref('uom.product_uom_meter')
        cls.partner = cls.env['res.partner'].create({'name': 'CLIENTE PILOTAJE PRUEBA', 'is_company': True})
        cls.tela = cls.env['product.product'].create({'name': 'tela pilotaje', 'default_code': 'WJ150Q28JNT174X',
                                                      'type': 'consu', 'is_storable': True, 'tracking': 'lot',
                                                      'uom_id': m.id})
        Param = cls.env['ir.config_parameter'].sudo()
        Param.set_param(PARAM_LOTS, '')
        Param.set_param(PARAM_READINGS, '')
        Param.set_param(PARAM_CPK_MIN, '')
        Car = cls.env['ficha.tecnica.caracteristica']
        cls.car_masa = Car.search([('code', '=', 'masa')], limit=1)
        cls.car_ancho = Car.search([('code', '=', 'ancho')], limit=1)
        cls.dev = cls.env['project.project'].create({'name': 'x', 'sgi_is_ft': True, 'partner_id': cls.partner.id,
                                                     'sgi_dev_product_name': 'Jersey pilotaje',
                                                     'sgi_dev_product_id': cls.tela.id})
        Line = cls.env['sgi.dev.characteristic']
        cls.masa = Line.create({'project_id': cls.dev.id, 'caracteristica_id': cls.car_masa.id, 'spec_nominal': 150.0,
                                'spec_tol_minus': 5.0, 'spec_tol_plus': 5.0, 'spec_tol_pct': True, 'critical': True})
        Line.create({'project_id': cls.dev.id, 'caracteristica_id': cls.car_ancho.id, 'spec_nominal': 1.7,
                     'spec_tol_minus': 0.02, 'spec_tol_plus': 0.02})
        cls.tela.product_tmpl_id.write({'sgi_dev_project_id': cls.dev.id, 'sgi_dev_state': 'pilotaje'})

    def _mo(self, lot_name):
        lot = self.env['stock.lot'].create({'name': lot_name, 'product_id': self.tela.id, 'company_id': self.env.company.id})
        return self.env['mrp.production'].create({'product_id': self.tela.id, 'product_qty': 100.0,
                                                  'product_uom_id': self.tela.uom_id.id, 'lot_producing_id': lot.id})

    def test_01_capability(self):
        stats = capability([148.0, 150.0, 152.0, 149.0, 151.0], 142.5, 157.5)
        self.assertEqual(stats['n'], 5)
        self.assertAlmostEqual(stats['mean'], 150.0)
        self.assertAlmostEqual(stats['sigma'], 1.5811, places=3)
        self.assertAlmostEqual(stats['cp'], 15.0 / (6 * 1.5811), places=3)
        self.assertAlmostEqual(stats['cpk'], 7.5 / (3 * 1.5811), places=3)
        one_sided = capability([10.0, 11.0, 12.0], None, 15.0)
        self.assertIsNone(one_sided['cp'], "Con un solo límite no hay Cp")
        self.assertAlmostEqual(one_sided['cpk'], (15.0 - 11.0) / (3 * 1.0), places=6)
        self.assertIsNone(capability([150.0], 142.5, 157.5)['cpk'], "Con una lectura no hay sigma")

    def test_02_primeros_lotes_entran_solos_y_el_no_conforme_no_cuenta(self):
        Pilot = self.env['sgi.dev.pilot']
        mos = [self._mo('L%d' % i) for i in range(1, 6)]
        pilot = Pilot._sgi_dev_register_production(mos[0])
        self.assertEqual(pilot.project_id, self.dev)
        self.assertEqual(pilot.lots_required, 3, "Sin parámetro, tres (brief)")
        self.assertEqual(pilot.readings_required, 0, "Lecturas por lote sin definir")
        for mo in mos[1:3]:
            Pilot._sgi_dev_register_production(mo)
        self.assertEqual(pilot.lot_count, 3)
        self.assertEqual(Pilot._sgi_dev_register_production(mos[3]), pilot)
        self.assertEqual(pilot.lot_count, 3, "Completo: el cuarto no entra")
        pilot.lot_ids[1].write({'verdict': 'no_conforme'})
        Pilot._sgi_dev_register_production(mos[3])
        self.assertEqual(pilot.lot_count, 4, "El no conforme libera su lugar")
        self.assertEqual(self.dev.sgi_dev_pilot_count, 1)
        # una orden de muestra del proyecto no entra
        sample = self._mo('MUESTRA')
        sample.write({'sgi_dev_project_id': self.dev.id})
        self.assertFalse(Pilot._sgi_dev_register_production(sample))
        self.assertEqual(pilot.lot_count, 4)
        # artículo que no está en pilotaje: nada
        self.tela.product_tmpl_id.write({'sgi_dev_state': 'liberado'})
        self.assertFalse(Pilot._sgi_dev_register_production(mos[4]))

    def test_03_lecturas_dictamen_y_estudio(self):
        Pilot = self.env['sgi.dev.pilot']
        mos = [self._mo('H%d' % i) for i in range(1, 4)]
        pilot = Pilot
        for mo in mos:
            pilot = Pilot._sgi_dev_register_production(mo)
        lots = pilot.lot_ids
        values = {lots[0]: [148.0, 150.0, 152.0], lots[1]: [149.0, 151.0, 150.0], lots[2]: [170.0, 171.0, 169.0]}
        for lot, vals in values.items():
            for i, v in enumerate(vals, start=1):
                self.env['sgi.dev.pilot.reading'].create({'pilot_id': pilot.id, 'lot_id': lot.id,
                                                          'characteristic_id': self.masa.id, 'sequence': i, 'value': v})
        self.assertTrue(pilot.reading_ids.filtered(lambda r: r.lot_id == lots[0]).mapped('within_spec'))
        with self.assertRaises(UserError, msg="Lotes sin dictamen no cierran"):
            pilot.action_close()
        lots.action_evaluate()
        self.assertEqual(lots[0].verdict, 'conforme')
        self.assertEqual(lots[2].verdict, 'no_conforme', "170 g/m² está fuera del ±5 % del cliente")
        with self.assertRaises(UserError, msg="Faltan lotes conformes"):
            pilot.action_close()
        pilot.action_compute_study()
        study = pilot.study_ids
        self.assertEqual(len(study), 1, "Una característica crítica")
        self.assertEqual(study.lots, 2)
        self.assertEqual(study.n, 6, "Solo las lecturas de los lotes conformes")
        self.assertAlmostEqual(study.mean, 150.0)
        self.assertTrue(study.cpk_defined)
        self.assertFalse(study.verdict, "Sin Cpk mínimo no dictamina")
        self.env['ir.config_parameter'].sudo().set_param(PARAM_CPK_MIN, '1.33')
        pilot.invalidate_recordset(['cpk_min'])
        pilot.action_compute_study()
        self.assertEqual(pilot.study_ids.verdict, 'cumple')
        # tercer lote conforme cierra el pilotaje con PDF
        mo4 = self._mo('H4')
        Pilot._sgi_dev_register_production(mo4)
        lot4 = pilot.lot_ids.filtered(lambda l: l.production_id == mo4)
        for i, v in enumerate([150.0, 151.0, 149.0], start=1):
            self.env['sgi.dev.pilot.reading'].create({'pilot_id': pilot.id, 'lot_id': lot4.id,
                                                      'characteristic_id': self.masa.id, 'sequence': i, 'value': v})
        lot4.action_evaluate()
        self.env['ir.config_parameter'].sudo().set_param(PARAM_READINGS, '5')
        pilot.write({'readings_required': 5})
        with self.assertRaises(UserError, msg="Con el parámetro puesto se exigen las lecturas"):
            pilot.action_close()
        pilot.write({'readings_required': 0})
        pilot.action_close()
        self.assertEqual(pilot.state, 'cerrado')
        self.assertEqual(pilot.closed_by_id, self.env.user)
        self.assertTrue(pilot.attachment_id)
        self.assertEqual(pilot.study_ids.n, 9)
        html = self.env['ir.actions.report']._render_qweb_html('quimibond_sgi.report_dev_pilot_document', pilot.ids)[0].decode()
        self.assertIn('Estudio de habilidad', html)
        self.assertIn('H4', html)
        self.assertTrue(pilot.lot_ids[0].date)
