# -*- coding: utf-8 -*-
"""PR 7 del plan (19.0.53.1.0): sustituir un proceso sin dejar nada colgado."""
from odoo.tests import TransactionCase, tagged

from .common_documents import sgi_hide_real_documents


@tagged('post_install', '-at_install')
class TestPr7Replaces(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        sgi_hide_real_documents(cls.env)
        cls.env = cls.env(context=dict(cls.env.context, sgi_skip_role_check=True))
        cls.Process = cls.env['sgi.process'].with_context(active_test=False)

    def _doc(self, code, process, revision=0, state='vigente'):
        return self.env['documents.document'].create({
            'name': code, 'type': 'binary', 'sgi_is_controlled': True,
            'sgi_doc_type': 'procedimiento', 'sgi_code': code, 'sgi_state': state,
            'sgi_revision': revision, 'sgi_process_id': process.id})

    def test_01_replaces_mueve_documentos_y_recuerda_al_sucesor(self):
        old = self.Process.create({'code': 'X-V7', 'name': 'Viejo 7'})
        old_rev = self._doc('P-A95', old, revision=0, state='obsoleto')
        doc = self._doc('P-A95', old, revision=1)
        fmt = self._doc('P-A96', old)
        kpi = self.env['sgi.indicator'].create({
            'code': 'XV7-KPI', 'name': 'KPI 7', 'calc_mode': 'manual', 'process_id': old.id})
        payload = {'processes': [{'code': 'XN7', 'name': 'Nuevo 7', 'replaces': ['X-V7']}]}
        result = self.Process.load_payload(payload)
        self.assertTrue(result['ok'], result['errors'])
        new = self.Process.search([('code', '=', 'XN7')])
        self.assertFalse(old.active)
        self.assertEqual(old.replaced_by_id, new, "El viejo recuerda a su sucesor.")
        for d in (old_rev, doc, fmt):
            self.assertEqual(d.sgi_process_id, new, "Los documentos pasan al nuevo, clave completa.")
        self.assertEqual(doc.sgi_state, 'vigente', "Mover no cambia el estado del documento.")
        self.assertEqual(kpi.process_id, new)
        self.assertEqual(result['summary']['moved'].get('document'), 3)
        self.assertTrue(any(c['kind'] == 'document' and 'P-A95' in c['key'] for c in result['changes']))

    def test_02_una_carga_futura_no_deja_nada_colgado(self):
        old = self.Process.create({'code': 'X-V8', 'name': 'Viejo 8'})
        payload = {'processes': [{'code': 'XN8', 'name': 'Nuevo 8', 'replaces': ['X-V8']}]}
        self.assertTrue(self.Process.load_payload(payload)['ok'])
        new = self.Process.search([('code', '=', 'XN8')])
        # Alguien liga después un indicador, un riesgo y un documento al proceso archivado.
        kpi = self.env['sgi.indicator'].create({
            'code': 'XV8-KPI', 'name': 'KPI colgado', 'calc_mode': 'manual', 'process_id': old.id})
        risk = self.env['sgi.risk'].create({
            'name': 'Riesgo colgado', 'instrument': 'ryo', 'process_id': old.id,
            'eval_probability': '2', 'eval_impact': '2'})
        doc = self._doc('P-A96', old)
        # Cualquier carga posterior (aquí, la misma sin cambios) los mueve al sucesor.
        result = self.Process.load_payload({'processes': [{'code': 'XN8', 'name': 'Nuevo 8'}]})
        self.assertTrue(result['ok'], result['errors'])
        self.assertEqual(kpi.process_id, new)
        self.assertEqual(risk.process_id, new)
        self.assertEqual(doc.sgi_process_id, new)
        self.assertTrue(any('colgado' in c['key'] for c in result['changes'] if c['action'] == 'moved'))
        self.assertFalse(self.env['sgi.indicator'].search([('process_id', '=', old.id)]))
        # Un proceso archivado sin sucesor con un indicador colgado se advierte.
        lonely = self.Process.create({'code': 'X-V9', 'name': 'Viejo 9'})
        self.env['sgi.indicator'].create({
            'code': 'XV9-KPI', 'name': 'KPI huérfano', 'calc_mode': 'manual', 'process_id': lonely.id})
        lonely.active = False
        result = self.Process.load_payload({'processes': [{'code': 'XN8', 'name': 'Nuevo 8'}]})
        self.assertTrue(any(w['kind'] == 'process' and w['key'] == 'X-V9' and 'sin sucesor' in w['message']
                            for w in result['warnings']))
