# -*- coding: utf-8 -*-
"""57.156.0 (Dirección General 2026-10-13): tres tipos de NC (auditoría,
proceso, indicador) que salen del origen."""
from lxml import etree

from odoo.tests import TransactionCase, tagged


@tagged('post_install', '-at_install')
class TestNcTipos(TransactionCase):

    def _nc(self, origin):
        return self.env['quality.alert'].create({'title': 'NC tipo %s' % origin, 'sgi_origin_type': origin})

    def test_01_tipo_por_origen(self):
        expected = {
            'auditoria_interna': 'auditoria', 'auditoria_externa': 'auditoria', 'scorecard': 'auditoria',
            'indicador': 'indicador',
            'proceso': 'proceso', 'reclamacion': 'proceso', 'incidente_sst': 'proceso',
            'emergencia': 'proceso', 'recorrido_csh': 'proceso', 'riesgo': 'proceso',
        }
        origins = [value for value, _label in self.env['quality.alert']._fields['sgi_origin_type'].selection]
        self.assertEqual(set(origins), set(expected), "Cada origen tiene su tipo.")
        for origin, nc_type in expected.items():
            self.assertEqual(self._nc(origin).sgi_nc_type, nc_type, origin)

    def test_02_cambia_con_el_origen_y_se_busca(self):
        alert = self._nc('proceso')
        alert.sgi_origin_type = 'auditoria_interna'
        self.assertEqual(alert.sgi_nc_type, 'auditoria')
        self.assertIn(alert, self.env['quality.alert'].search([('sgi_nc_type', '=', 'auditoria')]))
        self.assertNotIn(alert, self.env['quality.alert'].search([('sgi_nc_type', '=', 'proceso')]))

    def test_03_hallazgo_de_auditoria_nace_de_auditoria(self):
        process = self.env['sgi.process'].create({'code': 'ZNT', 'name': 'Proceso tipos NC'})
        audit = self.env['sgi.audit'].create({
            'audit_type': 'interna', 'process_ids': [(6, 0, process.ids)], 'state': 'informe'})
        finding = self.env['sgi.audit.finding'].create({
            'audit_id': audit.id, 'finding_type': 'nc_menor', 'process_id': process.id,
            'description': 'Hallazgo tipos NC'})
        finding.action_generate_nc()
        self.assertEqual(finding.alert_id.sgi_nc_type, 'auditoria')

    def test_04_filtros_y_agrupacion(self):
        search = etree.fromstring(self.env.ref('quimibond_sgi.sgi_nc_view_search').arch)
        for name in ('sgi_type_auditoria', 'sgi_type_proceso', 'sgi_type_indicador', 'group_type'):
            self.assertTrue(search.xpath("//filter[@name='%s']" % name), name)
