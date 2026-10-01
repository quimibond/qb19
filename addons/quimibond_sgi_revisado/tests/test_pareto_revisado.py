# -*- coding: utf-8 -*-
"""4.3.0 (revisión de vistas del SGI, V-M13): el Pareto de defectos del
revisado sale de mayor a menor, con filtro de fecha y ayuda."""
from lxml import etree

from odoo.tests import TransactionCase, tagged


@tagged('post_install', '-at_install')
class TestParetoRevisado(TransactionCase):

    def _arch(self, view_type, ref):
        Log = self.env['mrp.revision.log']
        views = Log.get_views([(self.env.ref('quimibond_sgi_revisado.' + ref).id, view_type)])
        return etree.fromstring(views['views'][view_type]['arch'])

    def test_01_pareto_ordenado_con_fecha_y_ayuda(self):
        self.assertEqual(self._arch('graph', 'mrp_revision_log_view_graph').get('order'), 'desc')
        self.assertEqual(self._arch('pivot', 'mrp_revision_log_view_pivot').get('default_order'),
                         '__count desc')
        search = self._arch('search', 'mrp_revision_log_view_search')
        self.assertTrue(search.xpath("//filter[@name='filter_date'][@date='create_date']"))
        action = self.env.ref('quimibond_sgi_revisado.mrp_revision_log_action_pareto')
        self.assertTrue(action.help)
