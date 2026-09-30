# -*- coding: utf-8 -*-
"""57.21.0 — fichas y búsquedas (D-007, D-008, D-014)."""
from datetime import date, timedelta

from lxml import etree

from odoo.tests import TransactionCase, tagged

from .common_users import sgi_test_user


@tagged('post_install', '-at_install')
class TestFichasBusquedas(TransactionCase):

    # D-014: modelos con menú que no tenían búsqueda propia.
    SEARCH_MODELS = (
        'sgi.audit.program', 'sgi.csh.inspection', 'sgi.management.review', 'sgi.policy',
        'sgi.objective', 'sgi.norm', 'sgi.area', 'sgi.document.type',
        'sgi.checklist.template', 'sgi.risk.category', 'sgi.format.map', 'sgi.job.family',
        'sgi.alert.source', 'sgi.ppap.element.template', 'sgi.interested.party',
        'sgi.legal.requirement', 'sgi.machine.sheet',
    )

    def test_01_search_views_with_archived(self):
        for model in self.SEARCH_MODELS:
            arch = self.env[model].get_view(view_type='search')['arch']
            doc = etree.fromstring(arch)
            self.assertTrue(doc.xpath('//filter'), "%s sin filtros de búsqueda." % model)
            if 'active' in self.env[model]._fields:
                self.assertTrue(doc.xpath("//filter[@name='inactive']"),
                                "%s sin filtro «Archivados»." % model)

    def test_02_state_buttons_confirm(self):
        """D-008: cerrar, obsoletar o regresar a borrador pide confirmación.

        57.66.0: la ficha se pide como Jefe MAST. Los botones llevan
        ``groups`` (MAST, Salud, dueño del proceso) y Odoo quita de la vista
        lo que el usuario no puede ver; el env de la prueba es OdooBot, que en
        la copia de producción no está en esos grupos."""
        manager = sgi_test_user(self.env, login='fichas_confirm_mast')
        checks = {
            'sgi.process': ['action_sgi_set_draft'],
            'sgi.incident': ['action_set_cerrado', 'action_set_reportado'],
            'sgi.audit': ['action_close', 'action_draft'],
            'sgi.management.review': ['action_close', 'action_draft'],
            'sgi.fmea': ['action_set_obsoleto', 'action_set_borrador'],
            'sgi.policy': ['action_set_obsoleta', 'action_set_borrador'],
            'sgi.risk': ['action_set_cerrado', 'action_set_identificado'],
        }
        for model, buttons in checks.items():
            doc = etree.fromstring(self.env[model].with_user(manager).get_view(view_type='form')['arch'])
            for name in buttons:
                nodes = doc.xpath("//button[@name='%s']" % name)
                self.assertTrue(nodes, "%s: falta el botón %s" % (model, name))
                for node in nodes:
                    self.assertTrue(node.get('confirm'), "%s.%s sin confirm" % (model, name))

    def test_03_action_line_history(self):
        """D-007: la acción correctiva tiene historial (mail.thread)."""
        process = self.env['sgi.process'].create({'code': 'XFB', 'name': 'Fichas'})
        risk = self.env['sgi.risk'].create({'name': 'Riesgo fichas', 'instrument': 'ryo',
                                            'process_id': process.id})
        line = self.env['sgi.action.line'].create({
            'risk_id': risk.id, 'name': 'Instalar guarda', 'responsible_id': self.env.user.id,
            'date_commit': date.today() + timedelta(days=10)})
        self._flush_tracking()
        line.invalidate_recordset(['message_ids'])
        before = len(line.message_ids)
        line.action_mark_done()
        # 57.66.0: el seguimiento de mail.thread se escribe en el precommit
        # del cursor (al confirmar), no en el flush: sin correrlo, el conteo
        # dependía de si otra cosa lo había disparado antes.
        self._flush_tracking()
        line.invalidate_recordset(['message_ids'])
        self.assertGreater(len(line.message_ids), before, "Terminar queda en el historial.")
        tracked = line.message_ids.tracking_value_ids.field_id.mapped('name')
        self.assertIn('date_done', tracked, "El historial dice cuándo se terminó.")

    def _flush_tracking(self):
        self.env.flush_all()
        self.env.cr.precommit.run()
