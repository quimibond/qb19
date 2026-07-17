# -*- coding: utf-8 -*-
from datetime import date

from odoo.tests import TransactionCase, tagged


@tagged('post_install', '-at_install')
class TestPegamentoNcMayor(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.team_int = cls.env.ref('quimibond_sgi.sgi_quality_team_internal')
        cls.stage_closed = cls.env.ref('quimibond_sgi.sgi_nc_int_stage_closed')
        cls.Alert = cls.env['quality.alert']

    def test_01_nc_mayor_closed_creates_activity(self):
        # Garantiza destinatario (Jefe MAST).
        self.env.user.group_ids = [(4, self.env.ref('quimibond_sgi.group_sgi_manager').id)]
        alert = self.Alert.create({
            'title': 'NC mayor de prueba',
            'team_id': self.team_int.id,
            'sgi_classification': 'mayor',
        })
        # Completa candados de cierre.
        alert.write({
            'sgi_root_cause': 'Causa raíz',
            'sgi_effectiveness_note': 'Eficaz',
            'sgi_effectiveness_date': date.today(),
        })
        alert.write({'stage_id': self.stage_closed.id})
        activities = self.env['mail.activity'].search([
            ('res_model', '=', 'quality.alert'),
            ('res_id', '=', alert.id),
            ('summary', 'ilike', 'actualizar AMEF'),
        ])
        self.assertTrue(
            activities,
            "Cerrar una NC mayor debe agendar la actualización de AMEF/plan de control.")
