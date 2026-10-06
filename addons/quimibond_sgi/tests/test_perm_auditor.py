# -*- coding: utf-8 -*-
"""PERM-1 (19.0.48.0.0): el Auditor SGI es de solo lectura. Lee todo el SGI
y los registros de los procesos auditados; solo escribe hallazgos."""
from odoo.exceptions import AccessError
from odoo.tests import TransactionCase, tagged, new_test_user

from .common_documents import sgi_hide_real_documents


@tagged('post_install', '-at_install')
class TestPermAuditor(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        sgi_hide_real_documents(cls.env)
        cls.auditor = new_test_user(
            cls.env, login='perm_auditor', groups='base.group_user,quimibond_sgi.group_sgi_auditor')
        cls.process = cls.env['sgi.process'].create({'code': 'XAUD', 'name': 'Proceso auditado'})
        cls.indicator = cls.env['sgi.indicator'].create({
            'code': 'TST-AUD', 'name': 'KPI auditado', 'calc_mode': 'manual',
            'process_id': cls.process.id})
        cls.measure = cls.env['sgi.indicator.measure'].create({
            'indicator_id': cls.indicator.id, 'period_date': '2026-08-01'})
        cls.alert = cls.env['quality.alert'].create({'name': 'NC auditada'})
        cls.audit = cls.env['sgi.audit'].create({
            'name': 'Auditoría de prueba', 'process_ids': [(6, 0, cls.process.ids)]})

    def test_01_auditor_is_not_a_user(self):
        auditor = self.env.ref('quimibond_sgi.group_sgi_auditor')
        self.assertNotIn(self.env.ref('quimibond_sgi.group_sgi_user'), auditor.all_implied_ids,
                         "Auditor ya no implica Usuario SGI.")
        self.assertFalse(self.auditor.has_group('quimibond_sgi.group_sgi_user'))
        # MAST sigue siendo usuario y auditor.
        manager = self.env.ref('quimibond_sgi.group_sgi_manager')
        self.assertIn(self.env.ref('quimibond_sgi.group_sgi_user'), manager.all_implied_ids)
        self.assertIn(auditor, manager.all_implied_ids)

    def test_02_auditor_reads_everything(self):
        # with_user, no env(user=): en Odoo 19 env(user=) conserva el modo
        # superusuario del entorno de pruebas y no revisa permisos.
        env = self.env['res.users'].with_user(self.auditor).env
        self.assertEqual(env['sgi.process'].browse(self.process.id).name, 'Proceso auditado')
        self.assertEqual(env['sgi.indicator.measure'].browse(self.measure.id).indicator_id, self.indicator)
        self.assertEqual(env['quality.alert'].browse(self.alert.id).name, 'NC auditada')
        self.assertTrue(env['sgi.audit'].search([('id', '=', self.audit.id)]))
        for model in ('sgi.risk', 'sgi.policy', 'sgi.management.review', 'sgi.legal.requirement',
                      'documents.document', 'purchase.order', 'stock.picking', 'mrp.production',
                      'maintenance.request', 'helpdesk.ticket'):
            env[model].check_access('read')

    def test_03_auditor_cannot_change_evidence(self):
        # with_user, no env(user=): en Odoo 19 env(user=) conserva el modo
        # superusuario del entorno de pruebas y no revisa permisos.
        env = self.env['res.users'].with_user(self.auditor).env
        with self.assertRaises(AccessError):
            env['sgi.process'].browse(self.process.id).write({'name': 'Cambiado'})
        with self.assertRaises(AccessError):
            env['sgi.indicator.measure'].browse(self.measure.id).write({'value': 1})
        with self.assertRaises(AccessError):
            env['quality.alert'].browse(self.alert.id).write({'name': 'Cambiada'})
        with self.assertRaises(AccessError):
            env['sgi.risk'].create({'name': 'Riesgo', 'process_id': self.process.id})
        for model in ('sgi.action.line', 'purchase.order', 'mrp.production'):
            with self.assertRaises(AccessError):
                env[model].check_access('write')
        # documents.document: en Odoo 19 todo usuario interno tiene la ACL de
        # escritura del modelo y el permiso va por documento (access_internal);
        # el documento controlado lo lee y no lo edita (56.7.0). Se prueba
        # sobre un documento, no sobre el modelo vacío.
        doc = self.env['documents.document'].create({
            'name': 'Evidencia auditada.pdf', 'type': 'binary', 'sgi_is_controlled': True,
            'sgi_doc_type': 'formulario_odoo', 'sgi_state': 'vigente'})
        if 'access_internal' in doc._fields:
            self.assertEqual(doc.access_internal, 'view')
        with self.assertRaises(AccessError):
            env['documents.document'].browse(doc.id).write({'name': 'Cambiada.pdf'})

    def test_04_auditor_writes_findings(self):
        # with_user, no env(user=): en Odoo 19 env(user=) conserva el modo
        # superusuario del entorno de pruebas y no revisa permisos.
        env = self.env['res.users'].with_user(self.auditor).env
        finding = env['sgi.audit.finding'].create({
            'audit_id': self.audit.id, 'description': 'Hallazgo del auditor',
            'process_id': self.process.id})
        self.assertTrue(finding.exists())
        finding.write({'description': 'Hallazgo corregido'})
        self.assertEqual(finding.description, 'Hallazgo corregido')
