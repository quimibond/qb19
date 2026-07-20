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
        # Completa candados de cierre (H5 + refinamiento H1 para NC mayor: causa
        # raíz, 5 porqués, acción correctiva terminada y verificación de eficacia).
        alert.write({
            'sgi_root_cause': 'Causa raíz',
            'sgi_why_1': 'Porqué 1', 'sgi_why_2': 'Porqué 2',
            'sgi_why_3': 'Porqué 3', 'sgi_why_4': 'Porqué 4',
            'sgi_why_5': 'Porqué 5',
            'sgi_effectiveness_note': 'Eficaz',
            'sgi_effectiveness_date': date.today(),
        })
        self.env['sgi.action.line'].create({
            'alert_id': alert.id, 'action_type': 'correctiva',
            'name': 'Corrección de la NC mayor',
            'responsible_id': self.env.user.id,
            'date_commit': date.today(), 'date_done': date.today(),
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
