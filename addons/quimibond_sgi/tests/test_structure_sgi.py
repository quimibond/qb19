# -*- coding: utf-8 -*-
"""Estructura del SGI, pasos 3 y 4: ficha de proceso y ficha de actividad."""
import base64

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
        self.assertEqual(ctx['default_sgi_affected_process_ids'], [(6, 0, self.process.ids)])
        # Con procedimiento vigente: modificación apuntando a ese documento.
        # (Un proceso aparte sin actividades: el candado de medición impide
        # poner vigente el de XS, cuya actividad no tiene método.)
        other = self.env['sgi.process'].create({'code': 'XS2', 'name': 'Proceso estructura 2'})
        doc = self.env['documents.document'].create({
            'name': 'PR-XS2', 'type': 'binary', 'sgi_is_controlled': True,
            'sgi_doc_type': 'procedimiento', 'sgi_code': 'PR-XS2', 'sgi_state': 'vigente',
            'sgi_process_id': other.id})
        other.invalidate_recordset()
        self.assertEqual(other._sgi_procedure_document(), doc)
        ctx = other.action_sgi_request_change()['context']
        self.assertEqual(ctx['default_sgi_change_kind'], 'modificacion')
        self.assertEqual(ctx['default_sgi_document_id'], doc.id)

    def test_03_diagrama_e_instructivo(self):
        # 53.2.0: «Ver en diagrama» es la vista hierarchy de actividades; la
        # lista de flechas sigue en action_sgi_view_flows.
        action = self.process.action_sgi_view_diagram()
        self.assertEqual((action['type'], action['tag']), ('ir.actions.client', 'sgi_diagram'))
        self.assertEqual(action['context']['sgi_diagram_kind'], 'process_flow')
        action = self.process.action_sgi_view_flows()
        self.assertEqual(action['res_model'], 'sgi.process.flow')
        self.assertIn(('from_process_id', '=', self.process.id), action['domain'])
        with self.assertRaises(UserError):
            self.activity.action_open_instruction()
        it = self.env['documents.document'].create({
            'name': 'IT estructura.pdf', 'type': 'binary', 'datas': base64.b64encode(b'%PDF-1.4'),
            'mimetype': 'application/pdf', 'sgi_is_controlled': True,
            'sgi_doc_type': 'instructivo', 'sgi_code': 'IT-XS-01', 'sgi_state': 'vigente',
            'sgi_process_id': self.process.id})
        self.activity.instruction_id = it
        action = self.activity.action_open_instruction()
        self.assertEqual(action['type'], 'ir.actions.act_url')
        self.assertIn('/web/content/%d' % it.attachment_id.id, action['url'])

    def test_04_registrar_hallazgo(self):
        action = self.process.action_sgi_register_finding()
        self.assertEqual(action['res_model'], 'quality.alert')
        self.assertEqual(action['context']['default_sgi_process_id'], self.process.id)
        action = self.activity.action_sgi_register_finding()
        self.assertIn('Paso con instructivo', action['context']['default_name'])
        self.assertEqual(action['context']['default_sgi_process_id'], self.process.id)
