# -*- coding: utf-8 -*-
from datetime import date

from odoo.tests import TransactionCase, tagged
from odoo.exceptions import UserError, ValidationError


@tagged('post_install', '-at_install')
class TestIncident(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.Incident = cls.env['sgi.incident']

    def _new_incident(self, **vals):
        base = {'name': 'Resbalón en almacén', 'severity': 'leve',
                'incident_type': 'casi_accidente'}
        base.update(vals)
        return self.Incident.create(base)

    def test_01_close_requires_scat(self):
        incident = self._new_incident()
        incident.action_set_investigacion()
        with self.assertRaises(UserError):
            incident.action_set_cerrado()
        # Completa las 3 capas SCAT.
        incident.write({
            'immediate_causes': 'Piso mojado sin señalización',
            'basic_causes': 'Falta de procedimiento de limpieza',
            'lack_of_control': 'No hay inspección planeada de orden y limpieza',
        })
        # El candado ISO (H1, v4.4.17) exige al menos una acción terminada.
        self.env['sgi.action.line'].create({
            'incident_id': incident.id,
            'name': 'Señalizar y establecer inspección de orden y limpieza',
            'responsible_id': self.env.user.id,
            'date_commit': date.today(),
            'date_done': date.today(),
        })
        incident.action_set_cerrado()
        self.assertEqual(incident.state, 'cerrado')

    def test_02_close_blocked_by_open_action(self):
        incident = self._new_incident(
            immediate_causes='a', basic_causes='b', lack_of_control='c')
        self.env['sgi.action.line'].create({
            'incident_id': incident.id,
            'name': 'Colocar señalización',
            'responsible_id': self.env.user.id,
            'date_commit': date.today(),
        })
        with self.assertRaises(UserError):
            incident.action_set_cerrado()
        incident.action_line_ids.write({'date_done': date.today(), 'progress': '100'})
        incident.action_set_cerrado()
        self.assertEqual(incident.state, 'cerrado')

    def test_03_xor_four_links(self):
        incident = self._new_incident()
        risk = self.env['sgi.risk'].create({'name': 'IPER piso', 'instrument': 'iper'})
        # Dos padres a la vez -> error
        with self.assertRaises(ValidationError):
            self.env['sgi.action.line'].create({
                'incident_id': incident.id,
                'risk_id': risk.id,
                'name': 'Acción inválida',
                'responsible_id': self.env.user.id,
                'date_commit': date.today(),
            })
        # Ningún padre -> error
        with self.assertRaises(ValidationError):
            self.env['sgi.action.line'].create({
                'name': 'Acción huérfana',
                'responsible_id': self.env.user.id,
                'date_commit': date.today(),
            })
        # Exactamente uno -> ok
        line = self.env['sgi.action.line'].create({
            'incident_id': incident.id,
            'name': 'Acción válida',
            'responsible_id': self.env.user.id,
            'date_commit': date.today(),
        })
        self.assertTrue(line.id)

    def test_04_serious_notifies_manager(self):
        # Garantiza que exista un destinatario (Jefe MAST).
        self.env.user.group_ids = [(4, self.env.ref('quimibond_sgi.group_sgi_manager').id)]
        incident = self._new_incident(severity='grave')
        activities = self.env['mail.activity'].search([
            ('res_model', '=', 'sgi.incident'),
            ('res_id', '=', incident.id),
        ])
        self.assertTrue(activities, "Un incidente grave debe generar actividad de aviso.")

    def test_05_escalate_severity_forces_nc(self):
        # (a) Leve → write severity grave → NC mayor creada y ligada en ambos sentidos.
        incident = self._new_incident(severity='leve')
        self.assertFalse(incident.sgi_alert_id,
                         "Un incidente leve no fuerza NC.")
        incident.write({'severity': 'grave'})
        self.assertTrue(incident.sgi_alert_id, "Al escalar a grave se fuerza la NC.")
        self.assertEqual(incident.sgi_alert_id.sgi_classification, 'mayor')
        self.assertEqual(incident.sgi_alert_id.sgi_incident_id, incident,
                         "La NC apunta de vuelta al incidente.")

    def test_06_already_serious_write_does_not_duplicate_nc(self):
        # (b) Ya grave con NC → write de otro campo → no se duplica NC.
        incident = self._new_incident(severity='grave')
        alert = incident.sgi_alert_id
        self.assertTrue(alert)
        incident.write({'name': 'Otro título del incidente'})
        self.assertEqual(incident.sgi_alert_id, alert, "No se re-crea la NC.")
        self.assertEqual(self.env['quality.alert'].search_count(
            [('sgi_incident_id', '=', incident.id)]), 1,
            "No se duplica la NC del incidente.")

    def test_07_escalate_leve_to_moderado_no_nc(self):
        # (c) Leve → moderado → sin NC.
        incident = self._new_incident(severity='leve')
        incident.write({'severity': 'moderado'})
        self.assertFalse(incident.sgi_alert_id,
                         "Moderado no es grave/fatal: no fuerza NC.")
