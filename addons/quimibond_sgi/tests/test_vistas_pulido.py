# -*- coding: utf-8 -*-
"""57.40.0+ (pulido de vistas, bloques 1 a 3 de la revisión del 2026-09-30).

Lo que se puede probar sin pantalla: que las vistas nuevas cargan y tienen la
forma prometida, y la lógica de servidor que las acompaña (filtro «Plazo
vencido», solo lectura en cerrado, D-009, validar mediciones)."""
from datetime import datetime, timedelta

from lxml import etree

from odoo.tests import TransactionCase, tagged, new_test_user

from .common_calendar import sgi_test_calendar


def _arch(env, model, view_type, view_ref=None):
    view_id = env.ref(view_ref).id if view_ref else False
    views = env[model].get_views([(view_id, view_type)])
    return etree.fromstring(views['views'][view_type]['arch'])


@tagged('post_install', '-at_install')
class TestNcFichaYLista(TransactionCase):
    """57.40.0 (V-A01, V-A02): una sola ficha de NC y su lista propia."""

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        sgi_test_calendar(cls.env)
        cls.team = cls.env.ref('quimibond_sgi.sgi_quality_team_internal')
        cls.stage_open = cls.env.ref('quimibond_sgi.sgi_nc_int_stage_open')
        cls.user = new_test_user(cls.env, login='vp_nc_user',
                                 groups='base.group_user,quimibond_sgi.group_sgi_user')

    def _nc(self, days_ago=0):
        alert = self.env['quality.alert'].create({
            'title': 'NC pulido', 'team_id': self.team.id, 'stage_id': self.stage_open.id,
            'sgi_responsible_ids': [(6, 0, self.user.ids)]})
        if days_ago:
            old = datetime.now() - timedelta(days=days_ago)
            self.env.cr.execute("UPDATE quality_alert SET create_date = %s WHERE id = %s",
                                (old, alert.id))
            alert.invalidate_recordset()
            alert._sgi_set_deadlines(force=True)
        return alert

    def test_01_un_solo_juego_de_pestanas(self):
        arch = _arch(self.env, 'quality.alert', 'form')
        self.assertEqual(len(arch.xpath('//sheet//notebook')), 1,
                         "La ficha de NC debe tener un solo notebook.")
        pages = [p.get('name') for p in arch.xpath('//sheet/notebook/page')]
        for name in ('sgi_analysis', 'sgi_actions', 'sgi_customer', 'sgi_close',
                     'description', 'sgi_supplier'):
            self.assertIn(name, pages)
        self.assertLess(pages.index('sgi_analysis'), pages.index('description'))
        self.assertNotIn('sgi_claim', pages, "Los metros reclamados van en «Cliente».")
        self.assertTrue(arch.xpath("//page[@name='sgi_customer']//field[@name='sgi_claimed_meters']"))
        # Un solo aviso con el formato controlado adentro.
        self.assertEqual(len(arch.xpath("//field[@name='sgi_format_banner']")), 1)

    def test_02_lista_y_kanban_propios(self):
        list_arch = _arch(self.env, 'quality.alert', 'list', 'quimibond_sgi.sgi_nc_view_list')
        for name in ('sgi_folio', 'sgi_classification', 'sgi_process_id',
                     'sgi_containment_state', 'sgi_root_cause_state', 'sgi_plan_state'):
            self.assertTrue(list_arch.xpath("//field[@name='%s']" % name), name)
        kanban_arch = _arch(self.env, 'quality.alert', 'kanban', 'quimibond_sgi.sgi_nc_view_kanban')
        self.assertTrue(kanban_arch.xpath("//field[@name='sgi_folio']"))
        action = self.env.ref('quimibond_sgi.sgi_nc_board_action').run()
        view_ids = dict((vt, vid) for vid, vt in action['views'])
        self.assertEqual(view_ids['list'], self.env.ref('quimibond_sgi.sgi_nc_view_list').id)
        self.assertEqual(view_ids['kanban'], self.env.ref('quimibond_sgi.sgi_nc_view_kanban').id)
        # Las vistas propias no le ganan a las de Calidad en su app.
        self.assertNotEqual(self.env['quality.alert'].get_views([(False, 'list')])['views']['list']['id'],
                            self.env.ref('quimibond_sgi.sgi_nc_view_list').id)

    def test_03_filtro_plazo_vencido(self):
        late = self._nc(days_ago=40)
        fresh = self._nc()
        self.assertTrue(late.sgi_deadline_overdue)
        self.assertFalse(fresh.sgi_deadline_overdue)
        Alert = self.env['quality.alert']
        found = Alert.search([('sgi_deadline_overdue', '=', True), ('id', 'in', (late | fresh).ids)])
        self.assertEqual(found, late)
        found = Alert.search([('sgi_deadline_overdue', '!=', True), ('id', 'in', (late | fresh).ids)])
        self.assertEqual(found, fresh)
        # Con la contención, la causa raíz y el plan cumplidos deja de estar vencida.
        for kind in ('contencion', 'correctiva'):
            self.env['sgi.action.line'].create({
                'alert_id': late.id, 'action_type': kind, 'name': 'Acción %s' % kind,
                'responsible_id': self.user.id, 'date_commit': datetime.now().date()})
        late.sgi_root_cause = 'Causa'
        self.assertFalse(late.sgi_deadline_overdue)
        self.assertFalse(Alert.search([('sgi_deadline_overdue', '=', True), ('id', '=', late.id)]))
