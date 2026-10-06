# -*- coding: utf-8 -*-
"""57.118.0 (C1, bloque 2): proyecto único de desarrollo con ciclo de vida."""
from datetime import timedelta

from odoo import fields
from odoo.exceptions import UserError
from odoo.tests import TransactionCase, tagged


@tagged('post_install', '-at_install')
class TestDevProject(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.Project = cls.env['project.project']
        cls.partner = cls.env['res.partner'].create({'name': 'SHAWMUT PRUEBA', 'is_company': True})
        cls.product = cls.env['product.product'].create({'name': 'Tela línea', 'default_code': 'WJ053Q22JNT160'})

    def _stage(self, key):
        return self.env.ref('quimibond_sgi.sgi_dev_stage_%s' % key)

    def _dev(self, **vals):
        return self.Project.create(dict({'name': 'x', 'sgi_is_ft': True, 'partner_id': self.partner.id,
                                         'sgi_dev_product_name': 'Jersey 53 g'}, **vals))

    def test_01_flag_is_not_the_name(self):
        plain = self.Project.create({'name': 'FT-900-2026 algo'})
        self.assertFalse(plain.sgi_is_ft, "El nombre ya no marca el desarrollo.")
        self.assertFalse(plain.sgi_dev_stage_log_ids)
        dev = self._dev()
        self.assertEqual(dev.stage_id, self._stage('solicitud'))
        self.assertEqual(dev.name, "Análisis Jersey 53 g", "El nombre se arma solo.")
        self.assertEqual(len(dev.sgi_dev_stage_log_ids), 1)
        self.assertEqual(dev.sgi_dev_stage_key, 'solicitud')

    def test_02_folio_sequence_and_name(self):
        dev = self._dev()
        dev.write({'stage_id': self._stage('analisis').id})
        self.assertFalse(dev.sgi_ft_folio, "Antes de la aprobación del cliente no hay folio.")
        dev.write({'stage_id': self._stage('muestra').id})
        year = fields.Date.context_today(dev).year
        self.assertRegex(dev.sgi_ft_folio, r'^FT-\d{3}-%d$' % year)
        self.assertTrue(dev.name.startswith(dev.sgi_ft_folio))
        other = self._dev()
        other.write({'stage_id': self._stage('pilotaje').id})
        self.assertEqual(int(other.sgi_ft_folio[3:6]), int(dev.sgi_ft_folio[3:6]) + 1, "Consecutivo del año.")
        dev.write({'sgi_dev_product_id': self.product.id})
        dev.action_sgi_dev_new_revision()
        self.assertEqual(dev.name, "%s WJ053Q22JNT160 rev. 1" % dev.sgi_ft_folio)
        self.assertEqual(len(dev.sgi_dev_stage_log_ids), 3)
        self.assertEqual(len(dev.sgi_dev_stage_log_ids.filtered(lambda l: not l.date_end)), 1)
        # Folio histórico a mano: se respeta.
        legacy = self._dev(sgi_ft_folio='FT-012-2019')
        legacy.write({'stage_id': self._stage('muestra').id})
        self.assertEqual(legacy.sgi_ft_folio, 'FT-012-2019')

    def test_03_clocks_pause_with_raw_material(self):
        dev = self._dev()
        log = dev.sgi_dev_stage_log_ids
        start = fields.Datetime.now() - timedelta(hours=10)
        log.write({'date_start': start})
        dev.action_sgi_dev_mp_wait_start()
        self.assertTrue(dev.sgi_dev_mp_pending)
        dev.sgi_dev_mp_wait_ids.write({'date_start': start + timedelta(hours=2), 'date_end': start + timedelta(hours=6)})
        dev.invalidate_recordset()
        self.assertAlmostEqual(log.hours_total, 10, delta=0.1)
        self.assertAlmostEqual(log.hours_mp, 4, delta=0.1)
        self.assertAlmostEqual(log.hours_dev, 6, delta=0.1)
        self.assertFalse(dev.sgi_dev_mp_pending)
        self.assertAlmostEqual(dev.sgi_dev_hours_dev, 6, delta=0.1)
        self.assertAlmostEqual(dev.sgi_dev_hours_mp, 4, delta=0.1)

    def test_04_revision_log_after_analysis(self):
        dev = self._dev()
        dev.action_sgi_dev_load_lines()
        masa = dev.sgi_dev_line_ids.filtered(lambda l: l.caracteristica_code == 'masa')
        masa.write({'spec_nominal': 53, 'spec_tol_minus': 3, 'spec_tol_plus': 3})
        self.assertFalse(dev.sgi_dev_revision_ids, "En la solicitud se captura sin bitácora.")
        dev.write({'stage_id': self._stage('cotizacion').id})
        dev.action_sgi_dev_new_revision()
        masa.write({'spec_nominal': 55})
        entries = dev.sgi_dev_revision_ids.filtered('characteristic_id')
        self.assertEqual(len(entries), 1)
        self.assertEqual((entries.revision, entries.old_value, entries.new_value),
                         (1, "53 g/m² ± 3 g/m²", "55 g/m² ± 3 g/m²"))
        masa.write({'note': 'sin cambio de valor'})
        self.assertEqual(len(dev.sgi_dev_revision_ids.filtered('characteristic_id')), 1)

    def test_05_commercial_and_analysis_result(self):
        dev = self._dev(sgi_dev_consumption_annual=12000, sgi_dev_consumption_uom='yd')
        self.assertAlmostEqual(dev.sgi_dev_consumption_monthly, 1000)
        self.assertAlmostEqual(dev.sgi_dev_consumption_annual_m, 10972.8)
        self.assertEqual(dev.sgi_dev_customer_status, 'prospecto')
        with self.assertRaises(UserError):
            dev.action_sgi_dev_close_as_line_product()
        dev.write({'sgi_dev_base_product_id': self.product.id})
        dev.action_sgi_dev_close_as_line_product()
        self.assertEqual((dev.sgi_dev_analysis_result, dev.stage_id), ('linea', self._stage('cerrado_sin_producto')))
        self.assertFalse(dev.sgi_ft_folio, "Un producto de línea no lleva folio FT.")
        reason = self.env['sgi.dev.option'].create({'kind': 'motivo_no_factible', 'name': 'Sin capacidad'})
        other = self._dev()
        with self.assertRaises(UserError):
            other.action_sgi_dev_close_not_feasible()
        other.write({'sgi_dev_not_feasible_reason_id': reason.id})
        other.action_sgi_dev_close_not_feasible()
        self.assertEqual(other.sgi_dev_analysis_result, 'no_factible')

    def test_06_legacy_name_parsing_and_migration(self):
        parse = self.Project._sgi_dev_parse_legacy_name
        self.assertEqual(parse('FT-043/2024 WK140Q48JNT165'), ('FT-043-2024', 'WK140Q48JNT165', 0))
        self.assertEqual(parse('FT-007-2026 WK140R46JNT165 REV3'), ('FT-007-2026', 'WK140R46JNT165', 3))
        self.assertEqual(parse('FT-001-2025  WQ130Q46JNT155'), ('FT-001-2025', 'WQ130Q46JNT155', 0))
        self.assertEqual(parse('FT-195/2019 WR135Q49JNT165'), ('FT-195-2019', 'WR135Q49JNT165', 0))
        self.assertIsNone(parse('Mantenimiento'))
        stage = self.env['project.project.stage'].create({'name': 'SHAWMUT PRUEBA', 'sequence': 999})
        legacy = self.Project.create({'name': 'FT-050/2025 WK300B46JNG165 REV2', 'stage_id': stage.id})
        tpl = self.Project.create({'name': 'PLANTILLA - Diseño y Desarrollo Prueba'})
        self.assertFalse(legacy.sgi_is_ft)
        touched = self.Project._sgi_dev_migrate_legacy()
        self.assertIn(legacy, touched)
        self.assertIn(tpl, touched)
        self.assertEqual((legacy.sgi_is_ft, legacy.sgi_ft_folio, legacy.sgi_dev_product_name, legacy.sgi_dev_revision),
                         (True, 'FT-050-2025', 'WK300B46JNG165', 2))
        self.assertEqual(legacy.partner_id, self.partner, "El cliente sale de la etapa que llevaba su nombre.")
        self.assertEqual(legacy.name, 'FT-050-2025 WK300B46JNG165 rev. 2')
        self.assertTrue(tpl.sgi_is_ft)
        again = self.Project._sgi_dev_migrate_legacy()
        self.assertEqual(legacy.sgi_ft_folio, 'FT-050-2025')
        self.assertNotIn(tpl, again, "Idempotente: la plantilla ya estaba marcada.")

    def test_07_measure_domains_use_the_flag(self):
        deliverable = self.env['sgi.deliverable'].create({
            'name': 'Proyecto abierto (prueba)', 'code': 'C1-PRUEBA-DOM',
            'measure_domain': "[('name', '=like', 'FT-%')]"})
        other = self.env['sgi.deliverable'].create({
            'name': 'Tarea (prueba)', 'code': 'C1-PRUEBA-DOM2',
            'measure_domain': "[('project_id.name', '=like', 'FT-%'), ('state', '=', '1_done')]"})
        n = self.Project._sgi_dev_migrate_measure_domains()
        self.assertGreaterEqual(n, 2)
        self.assertEqual(deliverable.measure_domain, "[('sgi_is_ft', '=', True)]")
        self.assertEqual(other.measure_domain, "[('project_id.sgi_is_ft', '=', True), ('state', '=', '1_done')]")
