# -*- coding: utf-8 -*-
"""57.121.0: las fichas de C1 se miden con el proyecto de desarrollo."""
from odoo import fields
from odoo.tests import TransactionCase, tagged


@tagged('post_install', '-at_install')
class TestDevMeasure(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.Project = cls.env['project.project']
        cls.partner = cls.env['res.partner'].create({'name': 'CLIENTE MEDICIÓN PRUEBA', 'is_company': True})

    def _dev(self, **vals):
        return self.Project.create(dict({'name': 'x', 'sgi_is_ft': True, 'partner_id': self.partner.id,
                                         'sgi_dev_product_name': 'Jersey medición'}, **vals))

    def _stage(self, key):
        return self.env.ref('quimibond_sgi.sgi_dev_stage_%s' % key)

    def test_01_stamps_follow_the_measured_fields(self):
        dev = self._dev(sgi_dev_analysis_result='nuevo')
        self.assertTrue(dev.sgi_dev_analysis_date, "El resultado del análisis lleva fecha.")
        self.assertEqual(dev.sgi_dev_analysis_by_id, self.env.user)
        self.assertFalse(dev.sgi_dev_approved_date)
        dev.write({'sgi_dev_approved_by_id': self.env.user.id})
        self.assertTrue(dev.sgi_dev_approved_date, "«Aprobó» lleva fecha.")
        dev.write({'sgi_dev_analysis_result': False})
        self.assertFalse(dev.sgi_dev_analysis_date, "Sin resultado no hay fecha.")
        before = dev.sgi_dev_approved_date
        dev.with_context(sgi_dev_migration=True).write({'sgi_dev_approved_by_id': False, 'sgi_dev_analysis_result': 'linea'})
        self.assertEqual(dev.sgi_dev_approved_date, before, "Con el contexto de migración no se sella nada.")
        self.assertFalse(dev.sgi_dev_analysis_date)

    def test_02_stage_log_keeps_the_stage_key(self):
        dev = self._dev()
        log = dev.sgi_dev_stage_log_ids.filtered(lambda l: not l.date_end)
        self.assertEqual(log.stage_key, 'solicitud')
        other = self.env['project.project.stage'].create({'name': 'Otra etapa prueba', 'sequence': 990})
        manual = self.env['sgi.dev.stage.log'].create({'project_id': dev.id, 'stage_id': self._stage('pilotaje').id,
                                                       'date_start': fields.Datetime.now()})
        self.assertEqual(manual.stage_key, 'pilotaje')
        manual.write({'stage_id': other.id})
        self.assertFalse(manual.stage_key, "Una etapa que no es de desarrollo no tiene clave.")
        self.assertIn(dev.sgi_dev_stage_log_ids.filtered(lambda l: l.stage_key == 'solicitud'), dev.sgi_dev_stage_log_ids)

    def test_03_c1_deliverables_point_to_the_project(self):
        Process = self.env['sgi.process']
        Deliverable = self.env['sgi.deliverable']
        Activity = self.env['sgi.process.activity']
        process = Process.search([('code', '=', 'C1')], limit=1) or Process.create({'code': 'C1', 'name': 'C1 prueba'})
        task_model = self.env['ir.model']._get('project.task')
        analysis = Deliverable.search([('code', '=', 'C1-ANALISIS')], limit=1) or Deliverable.create({
            'code': 'C1-ANALISIS', 'name': 'Análisis (viejo)', 'odoo_model_id': task_model.id,
            'measure_domain': "[('project_id.sgi_is_ft', '=', True), ('stage_id.name', 'ilike', 'ELEMENTOS')]",
            'measure_date_field': 'write_date', 'measure_user_field': 'write_uid'})
        reply = Deliverable.search([('code', '=', 'C1-RESPUESTA')], limit=1) or Deliverable.create({
            'code': 'C1-RESPUESTA', 'name': 'Respuesta (vieja)', 'odoo_model_id': task_model.id,
            'measure_domain': "[('project_id.sgi_is_ft', '=', True)]"})
        act02 = Activity.search([('number', '=', 'C1.02')], limit=1) or Activity.create({
            'process_id': process.id, 'name': 'Analizar la muestra (prueba)', 'step': 2,
            'measure_method': 'entregable', 'output_deliverable_ids': [(4, analysis.id)],
            'measure_deliverable_id': analysis.id})
        act08 = Activity.search([('number', '=', 'C1.08')], limit=1) or Activity.create({
            'process_id': process.id, 'name': 'Pedir materia prima (prueba)', 'step': 8,
            'measure_method': 'entregable'})
        self.assertEqual(act02.measure_model_id, task_model, "Antes: medía con tareas de la plantilla vieja.")
        report = self.Project._sgi_dev_apply_c1_measures()
        self.assertIn('C1-ANALISIS', report['updated'])
        self.assertIn('C1-MP-ESPERA', report['created'] + report['updated'])
        self.assertEqual(analysis.odoo_model_name, 'project.project')
        self.assertIn("('sgi_dev_analysis_result', '!=', False)", analysis.measure_domain)
        self.assertEqual((analysis.measure_date_field, analysis.measure_user_field),
                         ('sgi_dev_analysis_date', 'sgi_dev_analysis_by_id'))
        self.assertEqual(act02.measure_model_id.model, 'project.project', "La actividad volvió a copiar el entregable.")
        self.assertEqual(act02.measure_date_field, 'sgi_dev_analysis_date')
        self.assertEqual(reply.odoo_model_name, 'sgi.dev.stage.log')
        self.assertIn("('stage_key', '=', 'respuesta_cliente')", reply.measure_domain)
        wait = Deliverable.search([('code', '=', 'C1-MP-ESPERA')], limit=1)
        self.assertEqual(wait.odoo_model_name, 'sgi.dev.mp.wait')
        self.assertIn(wait, act08.output_deliverable_ids)
        self.assertEqual(act08.measure_deliverable_id, wait)
        self.assertEqual(act08.measure_model_id.model, 'sgi.dev.mp.wait')
        again = self.Project._sgi_dev_apply_c1_measures()
        self.assertFalse(again['updated'] + again['created'], "Idempotente: nada que cambiar la segunda vez.")
        # Lo medido existe de verdad: un proyecto con resultado cuenta para C1.02 y un paso por
        # «Respuesta del cliente» cuenta para C1.13.
        dev = self._dev(sgi_dev_analysis_result='nuevo')
        self.assertIn(dev, self.Project.search(eval(analysis.measure_domain)))  # noqa: S307 - dominio propio
        self.env['sgi.dev.stage.log'].create({'project_id': dev.id, 'stage_id': self._stage('respuesta_cliente').id,
                                              'date_start': fields.Datetime.now()})
        self.assertTrue(self.env['sgi.dev.stage.log'].search_count(eval(reply.measure_domain)))  # noqa: S307
