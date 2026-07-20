# -*- coding: utf-8 -*-
"""OLA 1 — Motor de Mejora (ISO 10).

Paso 1: causa raíz antes que acción correctiva/preventiva (H8) y cierre real de
NC mayor (5 porqués + acción correctiva terminada, refinamiento H1).
"""
from datetime import date

from odoo.tests import TransactionCase, tagged
from odoo.exceptions import UserError, ValidationError


@tagged('post_install', '-at_install')
class TestOla1RootCause(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.env.user.group_ids = [
            (4, cls.env.ref('quimibond_sgi.group_sgi_manager').id)]
        cls.team = cls.env.ref('quimibond_sgi.sgi_quality_team_internal')
        cls.stage_closed = cls.env.ref('quimibond_sgi.sgi_nc_int_stage_closed')
        cls.Alert = cls.env['quality.alert']
        cls.Line = cls.env['sgi.action.line']

    def _nc(self, **vals):
        base = {'title': 'NC prueba', 'team_id': self.team.id}
        base.update(vals)
        alert = self.Alert.create(base)
        self.assertTrue(alert.sgi_folio, "Debe recibir folio SGI.")
        return alert

    def _line(self, alert, **vals):
        base = {'alert_id': alert.id, 'name': 'Acción',
                'responsible_id': self.env.user.id, 'date_commit': date.today()}
        base.update(vals)
        return self.Line.create(base)

    # --- H8: causa raíz antes que correctiva/preventiva -------------------
    def test_01_correction_allowed_without_root_cause(self):
        alert = self._nc()
        # La corrección (contención) sí se permite antes de la causa raíz.
        line = self._line(alert, action_type='correccion')
        self.assertTrue(line)

    def test_02_corrective_blocked_without_root_cause(self):
        alert = self._nc()
        with self.assertRaises(ValidationError):
            self._line(alert, action_type='correctiva')

    def test_03_preventive_blocked_without_root_cause(self):
        alert = self._nc()
        with self.assertRaises(ValidationError):
            self._line(alert, action_type='preventiva')

    def test_04_corrective_allowed_with_root_cause(self):
        alert = self._nc(sgi_root_cause='Causa raíz')
        line = self._line(alert, action_type='correctiva')
        self.assertTrue(line)

    def test_05_change_type_to_corrective_blocked(self):
        alert = self._nc()
        line = self._line(alert, action_type='correccion')
        with self.assertRaises(ValidationError):
            line.write({'action_type': 'correctiva'})

    # --- H1: cierre real de NC mayor --------------------------------------
    def _mayor_ready(self):
        alert = self._nc(sgi_classification='mayor', sgi_root_cause='Causa',
                         sgi_effectiveness_note='Eficaz',
                         sgi_effectiveness_date=date.today())
        return alert

    def test_06_mayor_needs_five_whys(self):
        alert = self._mayor_ready()
        self._line(alert, action_type='correctiva', date_done=date.today())
        # Faltan los 5 porqués.
        with self.assertRaises(UserError):
            alert.write({'stage_id': self.stage_closed.id})

    def test_07_mayor_needs_corrective_done(self):
        alert = self._mayor_ready()
        alert.write({'sgi_why_1': '1', 'sgi_why_2': '2', 'sgi_why_3': '3',
                     'sgi_why_4': '4', 'sgi_why_5': '5'})
        # Solo una corrección terminada, sin acción correctiva.
        self._line(alert, action_type='correccion', date_done=date.today())
        with self.assertRaises(UserError):
            alert.write({'stage_id': self.stage_closed.id})

    def test_08_mayor_closes_with_whys_and_corrective(self):
        alert = self._mayor_ready()
        alert.write({'sgi_why_1': '1', 'sgi_why_2': '2', 'sgi_why_3': '3',
                     'sgi_why_4': '4', 'sgi_why_5': '5'})
        self._line(alert, action_type='correctiva', date_done=date.today())
        alert.write({'stage_id': self.stage_closed.id})
        self.assertEqual(alert.stage_id, self.stage_closed)


@tagged('post_install', '-at_install')
class TestOla1Links(TransactionCase):
    """H7: ligas reales NC <-> riesgo <-> AMEF <-> documento + cierre bidireccional."""

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.env.user.group_ids = [
            (4, cls.env.ref('quimibond_sgi.group_sgi_manager').id)]
        cls.team = cls.env.ref('quimibond_sgi.sgi_quality_team_internal')
        cls.stage_closed = cls.env.ref('quimibond_sgi.sgi_nc_int_stage_closed')

    def _mayor(self, **vals):
        base = {'title': 'NC', 'team_id': self.team.id,
                'sgi_classification': 'mayor', 'sgi_root_cause': 'c',
                'sgi_why_1': '1', 'sgi_why_2': '2', 'sgi_why_3': '3',
                'sgi_why_4': '4', 'sgi_why_5': '5',
                'sgi_effectiveness_note': 'e',
                'sgi_effectiveness_date': date.today()}
        base.update(vals)
        alert = self.env['quality.alert'].create(base)
        self.env['sgi.action.line'].create({
            'alert_id': alert.id, 'action_type': 'correctiva', 'name': 'a',
            'responsible_id': self.env.user.id,
            'date_commit': date.today(), 'date_done': date.today()})
        return alert

    def test_01_link_fields_and_inverse_counts(self):
        fmea = self.env['sgi.fmea'].create({'name': 'A', 'fmea_type': 'proceso'})
        risk = self.env['sgi.risk'].create({'name': 'R', 'instrument': 'ryo'})
        alert = self._mayor(sgi_fmea_id=fmea.id, sgi_risk_ids=[(6, 0, [risk.id])])
        self.assertEqual(fmea.sgi_nc_count, 1)
        self.assertEqual(risk.sgi_nc_count, 1)
        self.assertIn(alert, fmea.sgi_nc_ids)
        self.assertIn(alert, risk.sgi_nc_ids)

    def test_02_mayor_close_schedules_on_linked_fmea(self):
        fmea = self.env['sgi.fmea'].create({'name': 'A', 'fmea_type': 'proceso'})
        alert = self._mayor(sgi_fmea_id=fmea.id)
        before = self.env['mail.activity'].search_count(
            [('res_model', '=', 'sgi.fmea'), ('res_id', '=', fmea.id)])
        alert.write({'stage_id': self.stage_closed.id})
        after = self.env['mail.activity'].search_count(
            [('res_model', '=', 'sgi.fmea'), ('res_id', '=', fmea.id)])
        self.assertEqual(after, before + 1,
                         "La actividad de actualizar AMEF va sobre el AMEF ligado.")
        # No debe quedar la genérica sobre la NC.
        self.assertFalse(self.env['mail.activity'].search_count([
            ('res_model', '=', 'quality.alert'), ('res_id', '=', alert.id),
            ('summary', 'ilike', 'actualizar AMEF')]))

    def test_03_mayor_close_generic_without_fmea(self):
        alert = self._mayor()
        alert.write({'stage_id': self.stage_closed.id})
        self.assertTrue(self.env['mail.activity'].search_count([
            ('res_model', '=', 'quality.alert'), ('res_id', '=', alert.id),
            ('summary', 'ilike', 'actualizar AMEF')]))

    def test_04_bidirectional_close_from_chatter(self):
        risk = self.env['sgi.risk'].create({'name': 'R', 'instrument': 'ryo'})
        line = self.env['sgi.action.line'].create({
            'risk_id': risk.id, 'name': 'a', 'responsible_id': self.env.user.id,
            'date_commit': date.today()})
        self.assertTrue(line.activity_id)
        line.activity_id.action_feedback(feedback="hecho desde el chatter")
        line.invalidate_recordset()
        self.assertTrue(line.date_done, "Completar la actividad termina la acción.")
        self.assertEqual(line.state, 'terminada')


@tagged('post_install', '-at_install')
class TestOla1Recurrence(TransactionCase):
    """H2: detector de reincidencia + read-across."""

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.env.user.group_ids = [
            (4, cls.env.ref('quimibond_sgi.group_sgi_manager').id)]
        cls.team = cls.env.ref('quimibond_sgi.sgi_quality_team_internal')
        cls.stage_closed = cls.env.ref('quimibond_sgi.sgi_nc_int_stage_closed')
        cls.proc = cls.env['sgi.process'].search([], limit=1)
        cls.proc2 = cls.env['sgi.process'].search([('id', '!=', cls.proc.id)], limit=1)
        cls.clause = cls.env['sgi.norm.clause'].search([], limit=1)

    def _nc(self, **vals):
        base = {'title': 'NC', 'team_id': self.team.id,
                'sgi_process_id': self.proc.id, 'sgi_classification': 'menor'}
        base.update(vals)
        return self.env['quality.alert'].create(base)

    def test_01_first_nc_not_recurrent(self):
        nc = self._nc()
        self.assertEqual(nc.sgi_recurrence_count, 0)
        self.assertFalse(nc.sgi_is_recurrent)

    def test_02_second_same_process_recurrent(self):
        self._nc()
        nc2 = self._nc()
        self.assertEqual(nc2.sgi_recurrence_count, 1)
        self.assertTrue(nc2.sgi_is_recurrent)

    def test_03_same_clause_weighs_double(self):
        self._nc(sgi_norm_clause_id=self.clause.id)
        nc2 = self._nc(sgi_norm_clause_id=self.clause.id)
        self.assertEqual(nc2.sgi_recurrence_count, 2)

    def test_04_other_process_not_recurrent(self):
        self._nc()
        other = self._nc(sgi_process_id=self.proc2.id)
        self.assertEqual(other.sgi_recurrence_count, 0)

    def test_05_recurrent_close_requires_corrective(self):
        self._nc()
        nc2 = self._nc(sgi_root_cause='c', sgi_effectiveness_note='e',
                       sgi_effectiveness_date=date.today())
        self.env['sgi.action.line'].create({
            'alert_id': nc2.id, 'action_type': 'correccion', 'name': 'x',
            'responsible_id': self.env.user.id,
            'date_commit': date.today(), 'date_done': date.today()})
        with self.assertRaises(UserError):
            nc2.write({'stage_id': self.stage_closed.id})
        self.env['sgi.action.line'].create({
            'alert_id': nc2.id, 'action_type': 'correctiva', 'name': 'cap',
            'responsible_id': self.env.user.id,
            'date_commit': date.today(), 'date_done': date.today()})
        nc2.write({'stage_id': self.stage_closed.id})
        self.assertEqual(nc2.stage_id, self.stage_closed)

    def test_06_read_across_schedules_on_peer_fmeas(self):
        fmea_a = self.env['sgi.fmea'].create({
            'name': 'A', 'fmea_type': 'proceso', 'process_id': self.proc.id})
        fmea_b = self.env['sgi.fmea'].create({
            'name': 'B', 'fmea_type': 'proceso', 'process_id': self.proc.id})
        self._nc()  # primera del proceso
        nc2 = self._nc(sgi_root_cause='c', sgi_effectiveness_note='e',
                       sgi_effectiveness_date=date.today(), sgi_fmea_id=fmea_a.id)
        self.env['sgi.action.line'].create({
            'alert_id': nc2.id, 'action_type': 'correctiva', 'name': 'cap',
            'responsible_id': self.env.user.id,
            'date_commit': date.today(), 'date_done': date.today()})
        before = self.env['mail.activity'].search_count(
            [('res_model', '=', 'sgi.fmea'), ('res_id', '=', fmea_b.id)])
        nc2.write({'stage_id': self.stage_closed.id})
        after = self.env['mail.activity'].search_count(
            [('res_model', '=', 'sgi.fmea'), ('res_id', '=', fmea_b.id)])
        self.assertEqual(after, before + 1,
                         "El read-across agenda revisión en el AMEF par del mismo proceso.")
        # No en el AMEF ligado (origen).
        self.assertFalse(self.env['mail.activity'].search_count([
            ('res_model', '=', 'sgi.fmea'), ('res_id', '=', fmea_a.id),
            ('summary', 'ilike', 'read-across')]))
