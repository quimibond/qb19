# -*- coding: utf-8 -*-
"""Estructura del SGI, pasos 3 y 4: ficha de proceso y ficha de actividad."""
from odoo.exceptions import UserError
from odoo.tests import TransactionCase, tagged

from .common_documents import sgi_hide_real_documents


@tagged('post_install', '-at_install')
class TestStructureSgi(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        sgi_hide_real_documents(cls.env)
        cls.job = cls.env['hr.job'].create({'name': 'PUESTO ESTRUCTURA SGI'})
        cls.owner = cls.env['hr.employee'].create({'name': 'Dueño Estructura', 'job_id': cls.job.id})
        cls.process = cls.env['sgi.process'].create({
            'code': 'XS', 'name': 'Proceso estructura', 'owner_id': cls.owner.id})
        cls.activity = cls.env['sgi.process.activity'].create({
            'process_id': cls.process.id, 'name': 'Paso con instructivo',
            'role_ids': [(0, 0, {'role': 'ejecuta', 'job_id': cls.job.id})]})

    def test_01_linea_de_estado_y_nc_abiertas(self):
        self.activity.measure_state = 'rojo'
        self.process.invalidate_recordset()
        status = self.process.structure_status
        self.assertIn('Dueño: Dueño Estructura', status)
        self.assertIn('1 actividad(es) atrasada(s)', status)
        alert = self.env['quality.alert'].create({
            'name': 'NC estructura', 'sgi_process_id': self.process.id})
        self.process.invalidate_recordset(['nc_open_ids'])
        self.assertIn(alert, self.process.nc_open_ids)

    def test_02_pedir_un_cambio_apunta_al_procedimiento(self):
        action = self.process.action_sgi_request_change()
        ctx = action['context']
        self.assertEqual(action['res_model'], 'approval.request')
        self.assertEqual(ctx['default_sgi_change_kind'], 'alta', "Sin procedimiento vigente: alta.")
        doc = self.env['documents.document'].create({
            'name': 'PR-XS', 'type': 'binary', 'sgi_is_controlled': True,
            'sgi_doc_type': 'procedimiento', 'sgi_code': 'PR-XS', 'sgi_state': 'vigente',
            'sgi_process_id': self.process.id})
        self.process.invalidate_recordset()
        if doc in self.process.procedure_ids:
            ctx = self.process.action_sgi_request_change()['context']
            self.assertEqual(ctx['default_sgi_change_kind'], 'modificacion')
            self.assertEqual(ctx['default_sgi_document_id'], doc.id)
        self.assertEqual(ctx['default_sgi_affected_process_ids'], [(6, 0, self.process.ids)])

    def test_03_diagrama_e_instructivo(self):
        action = self.process.action_sgi_view_diagram()
        self.assertEqual(action['res_model'], 'sgi.process.flow')
        self.assertIn(('from_process_id', '=', self.process.id), action['domain'])
        with self.assertRaises(UserError):
            self.activity.action_open_instruction()
        it = self.env['documents.document'].create({
            'name': 'IT estructura', 'type': 'url', 'url': 'https://example.com/it',
            'sgi_is_controlled': True, 'sgi_doc_type': 'instructivo', 'sgi_code': 'IT-XS-01',
            'sgi_state': 'vigente'})
        self.activity.instruction_id = it
        action = self.activity.action_open_instruction()
        self.assertEqual(action['url'], 'https://example.com/it')

    def test_04_registrar_hallazgo(self):
        action = self.process.action_sgi_register_finding()
        self.assertEqual(action['res_model'], 'quality.alert')
        self.assertEqual(action['context']['default_sgi_process_id'], self.process.id)
        action = self.activity.action_sgi_register_finding()
        self.assertIn('Paso con instructivo', action['context']['default_name'])
        self.assertEqual(action['context']['default_sgi_process_id'], self.process.id)
