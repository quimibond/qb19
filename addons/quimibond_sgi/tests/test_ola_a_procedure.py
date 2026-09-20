# -*- coding: utf-8 -*-
"""OLA A paso 3 — sello de integridad del Procedimiento vivo (G14).

Editar los datos vivos del procedimiento (actividades, responsabilidades, cuerpo
del proceso) sobre un procedimiento con revisión VIGENTE marca el documento como
"pendiente de revisión". Aprobar una nueva revisión limpia la divergencia.
"""
from odoo.tests import TransactionCase, tagged


@tagged('post_install', '-at_install')
class TestOlaAProcedure(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.process = cls.env['sgi.process'].create({
            'code': 'P-OLAA', 'name': 'Proceso OLA A'})
        cls.doc = cls.env['documents.document'].create({
            'name': 'P-V98.pdf', 'type': 'binary',
            'sgi_is_controlled': True, 'sgi_doc_type': 'procedimiento',
            'sgi_code': 'P-V98', 'sgi_revision': '15', 'sgi_state': 'vigente',
            'sgi_process_id': cls.process.id})

    def _add_activity(self):
        return self.env['sgi.process.activity'].create({
            'process_id': self.process.id, 'number': '4.1',
            'name': 'Actividad nueva'})

    def test_01_editing_live_data_flags_document(self):
        self.assertFalse(self.doc.sgi_procedure_dirty)
        self._add_activity()
        self.assertTrue(self.doc.sgi_procedure_dirty,
                        "Editar el procedimiento vivo marca el doc pendiente de revisión.")
        self.assertTrue(self.doc.sgi_procedure_dirty_since)
        self.assertEqual(self.doc.sgi_procedure_dirty_by, self.env.user)

    def test_02_flag_schedules_activity_once(self):
        self.doc.sgi_owner_id = self.env.user
        self._add_activity()
        acts = self.env['mail.activity'].search([
            ('res_model', '=', 'documents.document'),
            ('res_id', '=', self.doc.id)])
        self.assertEqual(len(acts), 1, "Se agenda un aviso.")
        # Un segundo cambio no vuelve a agendar (idempotente por ciclo).
        self.env['sgi.process.activity'].create({
            'process_id': self.process.id, 'number': '4.2', 'name': 'Otra'})
        acts2 = self.env['mail.activity'].search([
            ('res_model', '=', 'documents.document'),
            ('res_id', '=', self.doc.id)])
        self.assertEqual(len(acts2), 1, "No se duplica el aviso mientras siga sucio.")

    def test_03_body_field_flags_document(self):
        self.process.write({'scope': 'Nuevo alcance'})
        self.assertTrue(self.doc.sgi_procedure_dirty)

    def test_04_new_revision_clears_flag(self):
        self._add_activity()
        self.assertTrue(self.doc.sgi_procedure_dirty)
        self.doc.write({'sgi_revision': '16'})
        self.assertFalse(self.doc.sgi_procedure_dirty,
                         "Aprobar una nueva revisión realinea y limpia la divergencia.")
        self.assertFalse(self.doc.sgi_procedure_dirty_since)

    def test_05_no_vigente_doc_no_flag(self):
        # Un proceso sin procedimiento vigente no puede marcar nada.
        process = self.env['sgi.process'].create({
            'code': 'P-OLAA2', 'name': 'Sin doc vigente'})
        self.env['documents.document'].create({
            'name': 'P-V97.pdf', 'type': 'binary',
            'sgi_is_controlled': True, 'sgi_doc_type': 'procedimiento',
            'sgi_code': 'P-V97', 'sgi_state': 'borrador',
            'sgi_process_id': process.id})
        # No debe lanzar ni marcar (no hay revisión vigente de la que diverger).
        self.env['sgi.process.activity'].create({
            'process_id': process.id, 'number': '1.0', 'name': 'act'})
        borrador = self.env['documents.document'].search([
            ('sgi_code', '=', 'P-V97')])
        self.assertFalse(any(borrador.mapped('sgi_procedure_dirty')))

    def test_06_unlink_activity_flags(self):
        act = self._add_activity()
        self.doc.write({'sgi_revision': '16'})  # limpia
        self.assertFalse(self.doc.sgi_procedure_dirty)
        act.unlink()
        self.assertTrue(self.doc.sgi_procedure_dirty,
                        "Borrar una actividad también diverge del PDF aprobado.")
