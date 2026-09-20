# -*- coding: utf-8 -*-
from datetime import date

from odoo.tests import TransactionCase, tagged
from odoo.exceptions import UserError


@tagged('post_install', '-at_install')
class TestNcFlow(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.team_int = cls.env.ref('quimibond_sgi.sgi_quality_team_internal')
        cls.team_ext = cls.env.ref('quimibond_sgi.sgi_quality_team_external')
        cls.stage_int_closed = cls.env.ref('quimibond_sgi.sgi_nc_int_stage_closed')
        cls.Alert = cls.env['quality.alert']

    def _new_alert(self, team, **vals):
        base = {'title': 'NC prueba', 'team_id': team.id}
        base.update(vals)
        return self.Alert.create(base)

    def test_00_shared_stage_set(self):
        """Un solo juego de etapas NC compartido por ambos equipos: nada de
        columnas duplicadas ni etapas de cancelación/cierre ambiguas."""
        for xmlid in ('sgi_nc_int_stage_open', 'sgi_nc_int_stage_followup',
                      'sgi_nc_int_stage_closed', 'sgi_nc_int_stage_cancel'):
            stage = self.env.ref('quimibond_sgi.%s' % xmlid)
            self.assertIn(self.team_int, stage.team_ids)
            self.assertIn(self.team_ext, stage.team_ids)
        Stage = self.env['quality.alert.stage']
        self.assertEqual(Stage.search_count(
            [('sgi_is_cancel_stage', '=', True)]), 1)
        self.assertEqual(Stage.search_count(
            [('sgi_is_closing_stage', '=', True)]), 1)
        # Sin nombres repetidos entre las etapas visibles para los equipos NC.
        nc_stages = Stage.search([('team_ids', 'in', (self.team_int | self.team_ext).ids)])
        names = nc_stages.mapped('name')
        self.assertEqual(len(names), len(set(names)),
                         "Etapas duplicadas para los equipos NC: %s" % names)

    def test_01_sequences(self):
        year = date.today().year
        a1 = self._new_alert(self.team_int)
        a2 = self._new_alert(self.team_int)
        e1 = self._new_alert(self.team_ext)
        # No se asumen números absolutos (los datos demo consumen la secuencia):
        # se valida el formato por equipo y el incremento consecutivo.
        self.assertTrue(a1.sgi_folio.startswith('NCI-%s-' % year))
        self.assertTrue(e1.sgi_folio.startswith('NCE-%s-' % year))
        n1 = int(a1.sgi_folio.split('-')[-1])
        n2 = int(a2.sgi_folio.split('-')[-1])
        self.assertEqual(n2, n1 + 1)
        self.assertGreaterEqual(len(a1.sgi_folio.split('-')[-1]), 4)

    def test_02_close_lock(self):
        alert = self._new_alert(self.team_int)
        # Acción abierta y sin causa raíz -> no se puede cerrar
        self.env['sgi.action.line'].create({
            'alert_id': alert.id,
            'name': 'Corregir',
            'responsible_id': self.env.user.id,
            'date_commit': date.today(),
        })
        with self.assertRaises(UserError):
            alert.write({'stage_id': self.stage_int_closed.id})

        # Completar todo
        alert.sgi_action_line_ids.write({'date_done': date.today(), 'progress': '100'})
        alert.write({
            'sgi_root_cause': 'Causa raíz identificada',
            'sgi_effectiveness_note': 'Eficaz',
            'sgi_effectiveness_date': date.today(),
        })
        # Ahora sí cierra
        alert.write({'stage_id': self.stage_int_closed.id})
        self.assertEqual(alert.stage_id, self.stage_int_closed)

    def test_03_force_close_requires_manager(self):
        alert = self._new_alert(self.team_int)
        wizard = self.env['sgi.nc.force.close'].create({
            'alert_id': alert.id,
            'reason': 'Cierre por acuerdo de dirección',
        })
        # Usuario sin grupo manager no puede
        demo_user = self.env['res.users'].create({
            'name': 'Operador SGI',
            'login': 'sgi_op_test',
            'group_ids': [(6, 0, [self.env.ref('quimibond_sgi.group_sgi_user').id])],
        })
        with self.assertRaises(UserError):
            wizard.with_user(demo_user).action_confirm()

        # Manager sí
        self.env.user.group_ids = [(4, self.env.ref('quimibond_sgi.group_sgi_manager').id)]
        wizard.action_confirm()
        self.assertTrue(alert.stage_id.sgi_is_closing_stage)
