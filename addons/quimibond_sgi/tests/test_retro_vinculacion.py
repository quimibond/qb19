# -*- coding: utf-8 -*-
from odoo.tests import TransactionCase, tagged

from odoo.addons.quimibond_sgi import post_init_hook


@tagged('post_install', '-at_install')
class TestRetroVinculacion(TransactionCase):

    def test_01_hook_safe_without_teams(self):
        # La BD de prueba no tiene los equipos de piso: el hook no debe romper.
        try:
            post_init_hook(self.env)
        except Exception as exc:  # noqa: BLE001
            self.fail("El post_init_hook no debe romper sin los equipos: %s" % exc)

    def test_02_hook_links_points_by_team(self):
        team = self.env['quality.alert.team'].create({'name': 'Revisado de Tela'})
        point = self.env['quality.point'].create({
            'title': 'Revisado de Tela',
            'team_id': team.id,
            'test_type_id': self.env.ref('quality_control.test_type_measure').id,
            'picking_type_ids': [(4, self.env.ref('stock.picking_type_in').id)],
        })
        self.assertFalse(point.sgi_control_plan_id)
        post_init_hook(self.env)
        plan = self.env.ref('quimibond_sgi.sgi_control_plan_revisado')
        self.assertEqual(point.sgi_control_plan_id, plan,
                         "El punto del equipo «Revisado de Tela» debe quedar ligado al plan.")
