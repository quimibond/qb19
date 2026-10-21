# -*- coding: utf-8 -*-
"""57.171.0 — Interfaz, entrega 2 (auditoría docs/audit/ui-sgi-2026-10-20.md).

Personas con avatar, vencimientos con «faltan / hace N días», pestañas con
`name`, avisos informativos como `status` y un solo texto para salir de un
asistente sin hacer nada."""
from lxml import etree

from odoo.tests import TransactionCase, tagged


@tagged('post_install', '-at_install')
class TestUiEntrega2(TransactionCase):

    def _views(self):
        return self.env['ir.ui.view'].search([
            ('model_data_id.module', '=', 'quimibond_sgi'), ('type', 'in', ('form', 'list'))])

    def _arch(self, xmlid):
        return etree.fromstring(self.env.ref(xmlid).arch)

    def test_01_personas_con_avatar(self):
        for xmlid, field, widget in (
                ('quimibond_sgi.sgi_work_permit_view_form', 'requester_id', 'many2one_avatar_user'),
                ('quimibond_sgi.sgi_work_permit_view_form', 'executor_ids', 'many2many_avatar_employee'),
                ('quimibond_sgi.sgi_audit_view_form', 'auditor_ids', 'many2many_avatar_user'),
                ('quimibond_sgi.sgi_activity_execution_view_list', 'employee_id', 'many2one_avatar_employee')):
            nodes = self._arch(xmlid).xpath("//field[@name='%s']" % field)
            self.assertTrue(nodes, xmlid)
            self.assertEqual({n.get('widget') for n in nodes}, {widget}, "%s %s" % (xmlid, field))

    def test_02_vencimientos_relativos(self):
        nodes = self._arch('quimibond_sgi.sgi_activity_execution_view_list').xpath("//field[@name='date_due']")
        self.assertEqual(nodes[0].get('widget'), 'remaining_days')
        form = self._arch('quimibond_sgi.sgi_activity_execution_view_form').xpath("//field[@name='date_due']")
        self.assertEqual(len(form), 2, "Abierta: «faltan N días»; cerrada: la fecha.")

    def test_03_pestanas_con_nombre(self):
        for view in self._views():
            for page in etree.fromstring(view.arch).iter('page'):
                self.assertTrue(page.get('name'), "%s: pestaña «%s» sin name" % (view.xml_id, page.get('string')))

    def test_04_avisos_informativos_y_botones_de_salida(self):
        for view in self._views():
            arch = etree.fromstring(view.arch)
            for div in arch.iter('div'):
                if 'alert-info' in (div.get('class') or ''):
                    self.assertNotEqual(div.get('role'), 'alert', "%s: aviso informativo como alerta" % view.xml_id)
            for button in arch.xpath("//button[@special='cancel']"):
                self.assertIn(button.get('string'), ('Descartar', 'Cerrar'), view.xml_id)
