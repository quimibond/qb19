# -*- coding: utf-8 -*-
"""57.118.0 (C1, bloque 2): proyecto único de desarrollo con ciclo de vida."""
from datetime import timedelta

from odoo import fields
from odoo.exceptions import UserError
from odoo.tests import TransactionCase, tagged
from odoo.tools.safe_eval import safe_eval


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

    def _approve(self, dev):
        """Revisión única de Ventas (57.120.0): sin ella no se pasa de Análisis a Cotización."""
        dev.write({'sgi_dev_analysis_result': 'nuevo'})
        dev.action_sgi_dev_review_approve()

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
        with self.assertRaises(UserError, msg="Sin la revisión de Ventas no se cotiza (57.120.0)."):
            dev.write({'stage_id': self._stage('cotizacion').id})
        self._approve(dev)
        dev.write({'stage_id': self._stage('muestra').id})
        year = fields.Date.context_today(dev).year
        self.assertRegex(dev.sgi_ft_folio, r'^FT-\d{3}-%d$' % year)
        self.assertTrue(dev.name.startswith(dev.sgi_ft_folio))
        other = self._dev()
        self._approve(other)
        other.write({'stage_id': self._stage('pilotaje').id})
        self.assertEqual(int(other.sgi_ft_folio[3:6]), int(dev.sgi_ft_folio[3:6]) + 1, "Consecutivo del año.")
        dev.write({'sgi_dev_product_id': self.product.id})
        dev.action_sgi_dev_new_revision()
        self.assertEqual(dev.name, "%s WJ053Q22JNT160 rev. 1" % dev.sgi_ft_folio)
        self.assertEqual(len(dev.sgi_dev_stage_log_ids), 3)
        self.assertEqual(len(dev.sgi_dev_stage_log_ids.filtered(lambda l: not l.date_end)), 1)
        # Folio histórico a mano: se respeta.
        legacy = self._dev(sgi_ft_folio='FT-012-2019')
        self._approve(legacy)
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
        self._approve(dev)
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
        # Dos idiomas, como producción (en_US, es_MX, es_ES): el nombre debe quedar igual en todos.
        self.env['res.lang']._activate_lang('es_MX')
        self.env.company.partner_id.lang = 'es_MX'
        Stage = self.env['project.project.stage']
        stage = Stage.create({'name': 'SHAWMUT PRUEBA', 'sequence': 999})
        legacy = self.Project.create({'name': 'FT-050/2025 WK300B46JNG165 REV2', 'stage_id': stage.id})
        tpl = self.Project.create({'name': 'PLANTILLA - Diseño y Desarrollo Prueba', 'is_template': True})
        # Como quedó producción tras 57.118.0: la plantilla renombrada «Análisis» solo en en_US.
        tpl.with_context(lang='en_US').name = 'Análisis'
        self.assertEqual(tpl.with_context(lang='es_MX').name, 'PLANTILLA - Diseño y Desarrollo Prueba')
        analysis = self.Project.create({'name': 'ANALISIS DE PROYECTO PRUEBA', 'stage_id': Stage.create(
            {'name': 'ANALISIS DE PROYECTOS', 'sequence': 995}).id})
        # Regla «uso»: la etapa BOWEN PRUEBA no coincide con ningún nombre, pero
        # dos proyectos de esa etapa ya tienen cliente: ese gana.
        bowen = self.env['res.partner'].create({'name': 'BOWEN NEW MATERIAL PRUEBA', 'is_company': True})
        other = self.env['res.partner'].create({'name': 'OTRO CLIENTE PRUEBA', 'is_company': True})
        bowen_stage = Stage.create({'name': 'BOWEN PRUEBA', 'sequence': 998})
        self.Project.create({'name': 'FT-001-2024 A', 'stage_id': bowen_stage.id, 'partner_id': bowen.id})
        self.Project.create({'name': 'FT-002-2024 B', 'stage_id': bowen_stage.id, 'partner_id': bowen.id})
        self.Project.create({'name': 'FT-003-2024 C', 'stage_id': bowen_stage.id, 'partner_id': other.id})
        bowen_missing = self.Project.create({'name': 'FT-004-2024 D', 'stage_id': bowen_stage.id})
        # QUIMIBOND es origen interno; Cancelada no es cliente.
        quimibond_stage = Stage.create({'name': 'QUIMIBOND', 'sequence': 997})
        internal = self.Project.create({'name': 'FT-005-2024 E', 'stage_id': quimibond_stage.id})
        cancel_stage = Stage.create({'name': 'Cancelada', 'sequence': 996})
        cancelled = self.Project.create({'name': 'FT-006-2024 F', 'stage_id': cancel_stage.id})
        preview = {row['stage']: row for row in self.Project._sgi_dev_migration_preview()}
        self.assertEqual(preview[bowen_stage]['partner'], bowen)
        self.assertIn('uso', preview[bowen_stage]['rule'])
        self.assertEqual(preview[bowen_stage]['projects'], bowen_missing)
        self.assertEqual((preview[bowen_stage]['target'], preview[cancel_stage]['target']), ('muestra', 'cerrado_sin_producto'))
        self.assertEqual(preview[stage]['rule'], 'nombre')
        self.assertEqual(preview[quimibond_stage]['rule'], 'origen interno')
        self.assertEqual(preview[cancel_stage]['rule'], 'no es cliente')
        self.assertFalse(legacy.sgi_is_ft)
        touched = self.Project._sgi_dev_migrate_legacy()
        self.assertIn(legacy, touched)
        self.assertIn(tpl, touched)
        self.assertIn(analysis, touched)
        self.assertEqual((legacy.sgi_is_ft, legacy.sgi_ft_folio, legacy.sgi_dev_product_name, legacy.sgi_dev_revision),
                         (True, 'FT-050-2025', 'WK300B46JNG165', 2))
        self.assertEqual(legacy.partner_id, self.partner, "El cliente sale de la etapa que llevaba su nombre.")
        for lang in ('en_US', 'es_MX'):
            self.assertEqual(legacy.with_context(lang=lang).name, 'FT-050-2025 WK300B46JNG165 rev. 2', lang)
            self.assertEqual(tpl.with_context(lang=lang).name, 'PLANTILLA - Diseño y Desarrollo Prueba',
                             "La plantilla recupera su nombre en %s." % lang)
            self.assertEqual(analysis.with_context(lang=lang).name, 'ANALISIS DE PROYECTO PRUEBA', lang)
        self.assertEqual(bowen_missing.partner_id, bowen, "El cliente que más usa la etapa gana al nombre.")
        self.assertEqual((internal.sgi_dev_origin, internal.partner_id.id), ('interno', False))
        self.assertFalse(cancelled.partner_id, "De «Cancelada» no se adivina cliente.")
        self.assertTrue(cancelled.sgi_is_ft)
        self.assertTrue(tpl.sgi_is_ft)
        self.assertEqual(legacy.stage_id, stage, "La migración de datos no mueve etapas; eso es el paso siguiente.")
        self.assertFalse(legacy.sgi_dev_stage_log_ids, "Con el contexto de migración los ganchos no corren.")
        # Etapas de avance.
        moved, archived = self.Project._sgi_dev_migrate_stages()
        self.assertEqual(legacy.sgi_dev_stage_key, 'muestra')
        self.assertEqual(cancelled.sgi_dev_stage_key, 'cerrado_sin_producto')
        self.assertEqual(analysis.sgi_dev_stage_key, 'analisis')
        self.assertEqual(internal.sgi_dev_stage_key, 'muestra')
        self.assertEqual(tpl.stage_id.id, tpl.stage_id.id)
        self.assertNotIn(tpl, moved, "Las plantillas no se mueven.")
        self.assertEqual(len(legacy.sgi_dev_stage_log_ids), 1, "El reloj arranca en la etapa nueva.")
        self.assertEqual(legacy.sgi_ft_folio, 'FT-050-2025', "Pasar a Muestra no cambia un folio que ya existe.")
        self.assertIn(stage, archived, "La etapa SHAWMUT PRUEBA quedó sin proyectos.")
        self.assertIn(quimibond_stage, archived)
        self.assertNotIn(cancel_stage, archived, "Cancelada no es etapa de cliente.")
        self.assertIn(bowen_stage, archived)
        again = self.Project._sgi_dev_migrate_legacy()
        self.assertEqual(legacy.sgi_ft_folio, 'FT-050-2025')
        self.assertNotIn(tpl, again, "Idempotente: la plantilla ya estaba marcada.")
        moved2, _archived2 = self.Project._sgi_dev_migrate_stages()
        self.assertFalse(moved2, "Idempotente: nada que mover la segunda vez.")

    def test_07_measure_domains_use_the_flag(self):
        deliverable = self.env['sgi.deliverable'].create({
            'name': 'Proyecto abierto (prueba)', 'code': 'C1-PRUEBA-DOM',
            'measure_domain': "[('name', '=like', 'FT-%')]"})
        other = self.env['sgi.deliverable'].create({
            'name': 'Tarea (prueba)', 'code': 'C1-PRUEBA-DOM2',
            'measure_domain': "[('project_id.name', '=like', 'FT-%'), ('state', '=', '1_done')]"})
        already = self.env['sgi.deliverable'].create({
            'name': 'Ya migrado (prueba)', 'code': 'C1-PRUEBA-DOM3',
            'measure_domain': "[('project_id.sgi_is_ft', '=', True), ('state', '=', '1_done')]"})
        n = self.Project._sgi_dev_migrate_measure_domains()
        self.assertGreaterEqual(n, 3)
        self.assertEqual(deliverable.measure_domain, "[('sgi_is_ft', '=', True), ('is_template', '=', False)]")
        self.assertEqual(other.measure_domain,
                         "[('project_id.sgi_is_ft', '=', True), ('project_id.is_template', '=', False), ('state', '=', '1_done')]")
        self.assertEqual(already.measure_domain, other.measure_domain, "Los ya migrados a la bandera excluyen plantillas.")
        self.Project._sgi_dev_migrate_measure_domains()
        self.assertEqual(already.measure_domain.count('is_template'), 1, "Idempotente.")
        tpl = self.Project.create({'name': 'Plantilla medida', 'is_template': True, 'sgi_is_ft': True})
        self.assertNotIn(tpl, self.Project.search(safe_eval(deliverable.measure_domain)))

    def test_08_hooks_skip_under_migration_and_mcp_models(self):
        dev = self._dev()
        name_before = dev.name
        dev.with_context(sgi_dev_migration=True).write({'sgi_dev_product_name': 'Otro producto'})
        self.assertEqual(dev.name, name_before, "Con el contexto de migración no se arma el nombre.")
        dev.write({'sgi_dev_product_name': 'Otro producto'})
        self.assertEqual(dev.name, 'Análisis Otro producto')
        tpl = self.Project.create({'name': 'PLANTILLA - X', 'is_template': True, 'sgi_is_ft': True,
                                   'sgi_ft_folio': 'FT-999-2026'})
        self.assertEqual(tpl.name, 'PLANTILLA - X', "Las plantillas nunca cambian de nombre solas.")
        plain = self._dev(sgi_dev_product_name=False)
        plain.write({'name': 'Mi análisis'})
        self.assertEqual(plain.name, 'Mi análisis', "Sin folio ni producto el nombre lo pone la persona.")
        if 'mcp.enabled.model' in self.env:
            models = self.Project._sgi_dev_enable_mcp_models()
            self.assertIn('sgi.dev.characteristic', models.mapped('model'))
            row = self.env['mcp.enabled.model'].search([('model_name', '=', 'sgi.dev.stage.log')])
            self.assertTrue(row.allow_read)
            self.assertFalse(row.allow_write)
            self.assertFalse(self.Project._sgi_dev_enable_mcp_models(), "Idempotente.")
