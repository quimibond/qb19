# -*- coding: utf-8 -*-
"""57.174.0 — Interfaz, entrega 5 (auditoría docs/audit/ui-sgi-2026-10-20.md).

Mis pendientes con dos botones por renglón (el del tipo y uno para abrir), la
columna «Qué sigue» y los tipos en el panel de búsqueda; filtros y agrupaciones
que faltaban en las demás listas."""
from lxml import etree

from odoo.tests import TransactionCase, tagged
from odoo.tools.safe_eval import safe_eval

KINDS = ('accion', 'nc', 'medicion', 'validacion', 'legal', 'documento', 'aprobacion', 'solicitud',
         'actividad', 'escalamiento', 'acuse', 'firma', 'aviso', 'revision', 'flujo')


@tagged('post_install', '-at_install')
class TestUiEntrega5(TransactionCase):

    def _arch(self, xmlid):
        return etree.fromstring(self.env.ref(xmlid).arch)

    def _visible(self, button, values):
        cond = button.get('invisible')
        return not (cond and safe_eval(cond, values))

    def test_01_dos_botones_por_renglon(self):
        """En cada tipo, agrupado o no, se ven a lo más dos botones, y siempre uno para abrir."""
        arch = self._arch('quimibond_sgi.sgi_my_pending_view_list')
        buttons = arch.xpath('/list/button')
        for kind in KINDS:
            for group_count in (0, 3):
                values = {'kind': kind, 'group_count': group_count, 'can_done': True, 'is_mine': True,
                          'can_mark_exec': True}
                shown = [b for b in buttons if self._visible(b, values)]
                opens = [b for b in shown if b.get('name') == 'action_open']
                self.assertLessEqual(len(shown), 2, "%s/%s: %s" % (kind, group_count, [b.get('string') for b in shown]))
                self.assertEqual(len(opens), 1, "%s/%s: un solo botón para abrir" % (kind, group_count))

    def test_02_que_sigue_y_panel(self):
        Pending = self.env['sgi.my.pending']
        row = Pending.create({'kind': 'acuse', 'name': 'Leer ZU5'})
        self.assertIn('Leído y entendido', row.next_step)
        grouped = Pending.create({'kind': 'medicion', 'name': 'Capturar ZU5', 'group_count': 4})
        self.assertIn('4', grouped.next_step)
        search = self._arch('quimibond_sgi.sgi_my_pending_view_search')
        self.assertTrue(search.xpath("//searchpanel/field[@name='kind']"))
        self.assertFalse(search.xpath("//filter[starts-with(@name, 'kind_')]"),
                         "Los tipos van en el panel, no en filtros sueltos.")

    def test_03_filtros_nuevos(self):
        for xmlid, name in (('quimibond_sgi.sgi_risk_view_search', 'review_due'),
                            ('quimibond_sgi.sgi_indicator_view_search', 'inactive'),
                            ('quimibond_sgi.sgi_process_view_search', 'state_vigente'),
                            ('quimibond_sgi.sgi_audit_view_search', 'group_lead'),
                            ('quimibond_sgi.sgi_nc_view_search', 'group_process'),
                            ('quimibond_sgi.sgi_emergency_plan_view_search', 'drill_due')):
            self.assertTrue(self._arch(xmlid).xpath("//filter[@name='%s']" % name), xmlid)
