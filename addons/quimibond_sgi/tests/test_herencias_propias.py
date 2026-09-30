# -*- coding: utf-8 -*-
"""57.22.0+ (entrega 5, `e5-herencias-propias`, A-008): el SGI no hereda sus
propias vistas. Las herencias integradas a su padre no existen tras la
instalación, y el borrado del pre-migrate (``migrations/herencias_propias.py``)
es idempotente y re-apunta hijas ajenas."""
import importlib.util
import os

from odoo.tests import TransactionCase, tagged

_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                     'migrations', 'herencias_propias.py')
_spec = importlib.util.spec_from_file_location('quimibond_sgi_herencias_test', _path)
herencias = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(herencias)

# Una línea por versión que integra herencias (se agregan en cada commit).
INTEGRADAS = (
    'report_nc_document_sgi', 'report_coa_document_sgi', 'report_mgmt_review_document_sgi',
)


@tagged('post_install', '-at_install')
class TestHerenciasPropias(TransactionCase):

    def test_01_integradas_no_existen(self):
        for name in INTEGRADAS:
            self.assertFalse(self.env.ref('quimibond_sgi.%s' % name, raise_if_not_found=False),
                             "%s sigue existiendo: debía integrarse a su padre." % name)

    def test_02_borrado_idempotente_y_reapunta_ajenas(self):
        View = self.env['ir.ui.view']
        parent = self.env.ref('quimibond_sgi.sgi_audit_view_form')
        own = View.create({
            'name': 'herencia propia de prueba', 'model': 'sgi.audit', 'inherit_id': parent.id,
            'arch': '<field name="norm_ids" position="after"><field name="folio"/></field>'})
        self.env['ir.model.data'].create({
            'module': 'quimibond_sgi', 'name': 'zz_test_herencia_propia',
            'model': 'ir.ui.view', 'res_id': own.id})
        foreign = View.create({
            'name': 'herencia ajena de prueba', 'model': 'sgi.audit', 'inherit_id': own.id,
            'arch': '<field name="folio" position="attributes">'
                    '<attribute name="readonly">1</attribute></field>'})
        self.env.flush_all()
        deleted = herencias.borrar_herencias(self.env.cr, 'prueba', ('zz_test_herencia_propia',))
        self.assertEqual(len(deleted), 1)
        self.env.invalidate_all()
        self.assertFalse(own.exists())
        self.assertEqual(foreign.inherit_id, parent, "La ajena se re-apunta al padre.")
        self.assertFalse(self.env['ir.model.data'].search_count([
            ('module', '=', 'quimibond_sgi'), ('name', '=', 'zz_test_herencia_propia')]))
        self.assertEqual(herencias.borrar_herencias(self.env.cr, 'prueba', ('zz_test_herencia_propia',)), [])

    def test_03_pie_de_formato_en_reportes_propios(self):
        """El pie que agregaban las herencias QWeb sigue saliendo."""
        team = self.env.ref('quimibond_sgi.sgi_quality_team_internal')
        alert = self.env['quality.alert'].create({'title': 'NC pie', 'team_id': team.id})
        html = self.env['ir.actions.report']._render_qweb_html(
            'quimibond_sgi.report_nc_document', alert.ids)[0].decode()
        if alert.sudo().sgi_format_info():
            self.assertIn('Formato controlado del SGI', html)
