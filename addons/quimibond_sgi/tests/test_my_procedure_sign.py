# -*- coding: utf-8 -*-
"""«Mi procedimiento» firmado en Sign antes de entrar en vigor (56.19.0):
publicar deja la revisión en borrador y la manda a firmar (MAST elabora, el
jefe directo aprueba); al firmarse entra en vigor sola, con acuses."""
import io
from unittest.mock import patch

from odoo.tests import TransactionCase, tagged, new_test_user
from odoo.tools.pdf import PdfFileWriter

from .common_calendar import sgi_test_calendar
from .common_documents import sgi_hide_real_documents


def _blank_pdf():
    writer = PdfFileWriter()
    writer.add_blank_page(612, 792)
    out = io.BytesIO()
    writer.write(out)
    return out.getvalue()


@tagged('post_install', '-at_install')
class TestMyProcedureSign(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        sgi_hide_real_documents(cls.env)
        sgi_test_calendar(cls.env)
        env = cls.env
        env['ir.config_parameter'].sudo().set_param('quimibond_sgi.mp_sign_required', 'True')
        cls.mast = new_test_user(env, login='zs_mps_mast', email='mast.mps@example.com',
                                 groups='base.group_user,quimibond_sgi.group_sgi_manager')
        cls.boss_user = new_test_user(env, login='zs_mps_boss', email='boss.mps@example.com',
                                      groups='base.group_user,quimibond_sgi.group_sgi_user')
        boss = env['hr.employee'].create({'name': 'ZS Jefe', 'user_id': cls.boss_user.id})
        cls.dept = env['hr.department'].create({'name': 'ZS Almacén', 'manager_id': boss.id})
        cls.job = env['hr.job'].create({'name': 'ZS ALMACENISTA', 'department_id': cls.dept.id})
        cls.employee = env['hr.employee'].create({'name': 'ZS Almacenista', 'job_id': cls.job.id})
        process = env['sgi.process'].create({'code': 'ZMP', 'name': 'ZS Almacén'})
        env['sgi.process.activity'].create({
            'process_id': process.id, 'name': 'Recibir material', 'measure_cadence': 'diaria',
            'role_ids': [(0, 0, {'role': 'ejecuta', 'job_id': cls.job.id})]})
        cls.Builder = type(env['sgi.sign.builder'])

    def setUp(self):
        super().setUp()
        patcher = patch.object(self.Builder, '_sgi_render_pdf', lambda self, ref, records: _blank_pdf())
        patcher.start()
        self.addCleanup(patcher.stop)

    def _drafts(self):
        return self.env['documents.document'].search([('sgi_code', '=', self.job._sgi_my_procedure_code()),
                                                      ('sgi_state', '=', 'borrador')])

    def test_01_publicar_manda_a_firmar_y_firmar_pone_en_vigor(self):
        self.job.with_user(self.mast).action_sgi_publish_my_procedure()
        doc = self._drafts()
        self.assertEqual(len(doc), 1, "La revisión nace en borrador.")
        self.assertFalse(self.job._sgi_my_procedure_current_doc(), "Todavía no está vigente.")
        self.assertFalse(doc.sgi_ack_ids, "Sin acuses hasta que entre en vigor.")
        sign = doc.sgi_publish_sign_request_id
        self.assertEqual(sign.request_item_ids.partner_id, (self.mast | self.boss_user).partner_id,
                         "Firman MAST (elaboró) y el jefe directo (aprobó).")
        self.assertEqual(len(sign.template_id.sign_item_ids), 2)
        # Publicar otra vez mientras espera firma no duplica.
        self.job.with_user(self.mast).action_sgi_publish_my_procedure()
        self.assertEqual(len(self._drafts()), 1)
        sign.write({'state': 'signed'})
        self.assertEqual(doc.sgi_state, 'vigente')
        self.assertEqual(self.job._sgi_my_procedure_current_doc(), doc)
        self.assertEqual(doc.sgi_ack_ids.employee_id, self.employee)

    def test_02_sin_correo_no_queda_borrador(self):
        self.boss_user.partner_id.email = False
        result = self.job.with_user(self.mast).action_sgi_publish_my_procedure()
        self.assertFalse(self._drafts(), "Si no se puede mandar a firmar, no queda la revisión a medias.")
        self.assertIn('sin mandar a firma', result['params']['message'])

    def test_03_sin_firma_sigue_como_antes(self):
        self.env['ir.config_parameter'].sudo().set_param('quimibond_sgi.mp_sign_required', 'False')
        self.job.with_user(self.mast).action_sgi_publish_my_procedure()
        self.assertTrue(self.job._sgi_my_procedure_current_doc(), "Sin firma, vigente al publicar.")
