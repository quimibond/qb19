# -*- coding: utf-8 -*-
"""OLA B — Línea dorada sin fugas.

B.1 Incidente grave/fatal FUERZA una NC y exige el IPER ligado para cerrar (G12).
B.2 Cascada: un objetivo nuevo hereda la política vigente (G11, versión suave).
B.3 Cierre de NC mayor exige atestiguar la lección aplicada (G13).
B.4 La salud de política/objetivo recomputa reactivamente (G21).
"""
from datetime import date

from odoo.tests import TransactionCase, tagged
from odoo.exceptions import UserError


@tagged('post_install', '-at_install')
class TestOlaBIncident(TransactionCase):

    def _incident(self, severity, **vals):
        base = {'name': 'Incidente OLA B', 'severity': severity,
                'incident_type': 'lesion',
                'immediate_causes': 'a', 'basic_causes': 'b',
                'lack_of_control': 'c'}
        base.update(vals)
        return self.env['sgi.incident'].create(base)

    def test_01_grave_forces_nc(self):
        inc = self._incident('grave')
        self.assertTrue(inc.sgi_alert_id, "Un incidente grave debe generar una NC.")
        self.assertEqual(inc.sgi_alert_id.sgi_classification, 'mayor')
        self.assertEqual(inc.sgi_alert_id.sgi_incident_id, inc,
                         "La NC apunta de vuelta al incidente (liga bidireccional).")

    def test_02_leve_no_nc(self):
        inc = self._incident('leve')
        self.assertFalse(inc.sgi_alert_id, "Un incidente leve no fuerza NC.")

    def test_03_grave_needs_iper_to_close(self):
        inc = self._incident('grave')
        self.env['sgi.action.line'].create({
            'incident_id': inc.id, 'name': 'acc',
            'responsible_id': self.env.user.id,
            'date_commit': date.today(), 'date_done': date.today()})
        inc.action_set_investigacion()
        # Sin IPER ligado -> no cierra.
        with self.assertRaises(UserError):
            inc.action_set_cerrado()
        # Con IPER -> cierra.
        iper = self.env['sgi.risk'].create({
            'name': 'Peligro X', 'instrument': 'iper',
            'eval_probability': '2', 'eval_impact': '2'})
        inc.risk_id = iper.id
        inc.action_set_cerrado()
        self.assertEqual(inc.state, 'cerrado')

    def test_04_nc_creation_idempotent(self):
        inc = self._incident('fatal')
        first = inc.sgi_alert_id
        inc._sgi_create_alert()
        self.assertEqual(inc.sgi_alert_id, first, "No se duplica la NC del incidente.")


@tagged('post_install', '-at_install')
class TestOlaBLessonLock(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.team = cls.env.ref('quimibond_sgi.sgi_quality_team_internal')
        cls.closed = cls.env.ref('quimibond_sgi.sgi_nc_int_stage_closed')

    def _ready_major_nc(self):
        """NC mayor con TODO listo salvo la lección aplicada."""
        alert = self.env['quality.alert'].create({
            'title': 'NC mayor OLA B', 'team_id': self.team.id,
            'sgi_classification': 'mayor',
            'sgi_why_1': '1', 'sgi_why_2': '2', 'sgi_why_3': '3',
            'sgi_why_4': '4', 'sgi_why_5': '5',
            'sgi_root_cause': 'raíz',
            'sgi_effectiveness_note': 'eficaz',
            'sgi_effectiveness_date': date.today()})
        self.env['sgi.action.line'].create({
            'alert_id': alert.id, 'name': 'correctiva', 'action_type': 'correctiva',
            'responsible_id': self.env.user.id,
            'date_commit': date.today(), 'date_done': date.today()})
        return alert

    def test_01_major_needs_lesson_captured(self):
        alert = self._ready_major_nc()
        self.assertFalse(alert.sgi_lesson_captured)
        with self.assertRaises(UserError):
            alert.write({'stage_id': self.closed.id})
        # Al atestiguar la lección, cierra.
        alert.sgi_lesson_captured = True
        alert.write({'stage_id': self.closed.id})
        self.assertEqual(alert.stage_id, self.closed)

    def test_02_minor_does_not_need_lesson(self):
        alert = self.env['quality.alert'].create({
            'title': 'NC menor OLA B', 'team_id': self.team.id,
            'sgi_classification': 'menor',
            'sgi_root_cause': 'raíz',
            'sgi_effectiveness_note': 'eficaz',
            'sgi_effectiveness_date': date.today()})
        self.env['sgi.action.line'].create({
            'alert_id': alert.id, 'name': 'corr', 'action_type': 'correccion',
            'responsible_id': self.env.user.id,
            'date_commit': date.today(), 'date_done': date.today()})
        alert.write({'stage_id': self.closed.id})
        self.assertEqual(alert.stage_id, self.closed,
                         "Una NC menor no requiere la atestación de lección.")


@tagged('post_install', '-at_install')
class TestOlaBCascade(TransactionCase):

    def test_01_new_objective_inherits_vigente_policy(self):
        policy = self.env['sgi.policy'].create({'name': 'Política OLA B'})
        policy.action_set_vigente()
        self.assertEqual(policy.state, 'vigente')
        objective = self.env['sgi.objective'].create({'name': 'Objetivo nuevo'})
        self.assertEqual(objective.policy_id, policy,
                         "Un objetivo nuevo hereda la política vigente (cascada).")

    def test_02_policy_health_aggregates(self):
        policy = self.env['sgi.policy'].create({'name': 'Política salud'})
        obj = self.env['sgi.objective'].create(
            {'name': 'Obj salud', 'policy_id': policy.id})
        # Sin indicadores/procesos en rojo, la salud agregada es verde.
        self.assertEqual(obj.health, 'verde')
        self.assertEqual(policy.health, 'verde')
