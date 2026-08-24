# -*- coding: utf-8 -*-
"""Tests de la auditoría 2026-08: cada uno cubre un hallazgo corregido
(C1-C3, A1, A3-A6 y el helper de robustez de crons de C2). Los negativos usan
with_user() explícito: el env por defecto de TransactionCase es superusuario y
los candados (correctamente) no aplican a código de sistema."""
from datetime import date, timedelta

from odoo import fields
from odoo.exceptions import UserError
from odoo.tests import TransactionCase, tagged


@tagged('post_install', '-at_install')
class TestAuditHardening(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.team_int = cls.env.ref('quimibond_sgi.sgi_quality_team_internal')
        cls.stage_closed = cls.env.ref('quimibond_sgi.sgi_nc_int_stage_closed')
        cls.group_user = cls.env.ref('quimibond_sgi.group_sgi_user')
        cls.group_manager = cls.env.ref('quimibond_sgi.group_sgi_manager')
        cls.sgi_user = cls.env['res.users'].create({
            'name': 'Operador SGI auditoría', 'login': 'sgi_audit_op',
            'group_ids': [(6, 0, [cls.group_user.id])]})
        cls.sgi_manager = cls.env['res.users'].create({
            'name': 'MAST auditoría', 'login': 'sgi_audit_mgr',
            'group_ids': [(6, 0, [cls.group_manager.id])]})

    # ------------------------------------------------------------------
    # C1 — completar una actividad NO debe requerir grupos SGI
    # ------------------------------------------------------------------
    def test_c1_action_done_without_sgi_groups(self):
        """Un usuario interno SIN grupos SGI completa una actividad cualquiera:
        el espejo de sgi.action.line se resuelve con sudo y no truena con
        AccessError (14 de 39 usuarios de producción estaban en este caso)."""
        plain = self.env['res.users'].create({
            'name': 'Empleado sin SGI', 'login': 'sgi_audit_plain',
            'group_ids': [(6, 0, [self.env.ref('base.group_user').id])]})
        request = self.env['maintenance.request'].with_user(plain).create(
            {'name': 'Falla de prueba C1'})
        activity = request.activity_schedule(
            'mail.mail_activity_data_todo', summary="Atender", user_id=plain.id)
        # Antes del fix: AccessError en la search de sgi.action.line.
        activity.with_user(plain).action_feedback(feedback="Listo.")
        self.assertFalse(request.activity_ids.filtered(
            lambda a: a.summary == "Atender"))

    def test_c1_mirror_line_still_closes(self):
        """El cierre bidireccional sigue funcionando: completar la actividad
        espejo termina la acción (date_done)."""
        alert = self.env['quality.alert'].create(
            {'title': 'NC espejo C1', 'team_id': self.team_int.id})
        line = self.env['sgi.action.line'].create({
            'alert_id': alert.id, 'name': 'Corregir',
            'responsible_id': self.sgi_user.id, 'date_commit': date.today()})
        self.assertTrue(line.activity_id)
        line.activity_id.action_feedback(feedback="Hecho")
        self.assertTrue(line.date_done)

    # ------------------------------------------------------------------
    # C2 — el helper de aislamiento procesa el resto aunque uno truene
    # ------------------------------------------------------------------
    def test_c2_for_each_isolates_failures(self):
        areas = self.env['sgi.area'].create([
            {'code': 'ZC2A', 'name': 'Área C2 A'},
            {'code': 'ZC2B', 'name': 'Área C2 B'}])
        touched = []

        def func(record):
            if record.code == 'ZC2A':
                raise ValueError("registro envenenado")
            touched.append(record.code)

        self.env['sgi.cron']._sgi_for_each(areas, func, "prueba C2")
        self.assertEqual(touched, ['ZC2B'])

    # ------------------------------------------------------------------
    # C3 — una medición validada no se reabre ni se edita sin MAST
    # ------------------------------------------------------------------
    def test_c3_validated_measure_lock(self):
        indicator = self.env['sgi.indicator'].create({
            'code': 'ZC3', 'name': 'KPI candado', 'calc_mode': 'manual',
            'responsible_id': self.sgi_user.id})
        measure = self.env['sgi.indicator.measure'].create({
            'indicator_id': indicator.id, 'period_date': date(2026, 7, 1),
            'value': 42.0, 'state': 'capturado'})
        measure.with_user(self.sgi_user).action_validate()
        self.assertEqual(measure.state, 'validado')
        # El responsable (no MAST) ya no puede reabrirla ni editar el valor.
        with self.assertRaises(UserError):
            measure.with_user(self.sgi_user).action_reset()
        with self.assertRaises(UserError):
            measure.with_user(self.sgi_user).write({'value': 99.0})
        # Re-validar la misma medición no truena (no reabre nada).
        measure.with_user(self.sgi_user).action_validate()
        # MAST sí puede reabrir.
        measure.with_user(self.sgi_manager).action_reset()
        self.assertEqual(measure.state, 'pendiente')

    # ------------------------------------------------------------------
    # A1 — «Generar NC» de reclamación es idempotente
    # ------------------------------------------------------------------
    def test_a1_complaint_nc_idempotent(self):
        ticket = self.env['helpdesk.ticket'].create({'name': 'Reclamación A1'})
        action = ticket.action_sgi_generate_nc()
        alert_id = action['res_id']
        self.assertTrue(ticket.sgi_alert_id)
        action2 = ticket.action_sgi_generate_nc()
        self.assertEqual(action2['res_id'], alert_id)
        self.assertEqual(self.env['quality.alert'].search_count(
            [('sgi_complaint_ticket_id', '=', ticket.id)]), 1)

    # ------------------------------------------------------------------
    # A2 — un acuse ajeno no se firma por write directo
    # ------------------------------------------------------------------
    def test_a2_ack_write_lock(self):
        employee = self.env['hr.employee'].create(
            {'name': 'Empleado acuse A2', 'user_id': self.sgi_manager.id})
        doc = self.env['documents.document'].create({
            'name': 'Doc A2', 'type': 'binary', 'sgi_is_controlled': True,
            'sgi_doc_type': 'procedimiento', 'sgi_code': 'P-G09',
            'sgi_state': 'vigente'})
        ack = self.env['sgi.document.ack'].create(
            {'document_id': doc.id, 'employee_id': employee.id})
        with self.assertRaises(UserError):
            ack.with_user(self.sgi_user).write({'state': 'leido'})
        ack.with_user(self.sgi_manager).action_mark_read()
        self.assertEqual(ack.state, 'leido')

    # ------------------------------------------------------------------
    # A3 — el contexto sgi_force_close no basta sin MAST
    # ------------------------------------------------------------------
    def test_a3_force_close_context_gated(self):
        alert = self.env['quality.alert'].create(
            {'title': 'NC contexto A3', 'team_id': self.team_int.id})
        with self.assertRaises(UserError):
            alert.with_user(self.sgi_user).with_context(
                sgi_force_close=True).write({'stage_id': self.stage_closed.id})

    # ------------------------------------------------------------------
    # A4 — el eslabón re-atorado con NC previa CERRADA genera NC nueva
    # ------------------------------------------------------------------
    def test_a4_chain_second_episode(self):
        Process = self.env['sgi.process']
        p_from = Process.create({'code': 'ZA4F', 'name': 'Origen A4'})
        p_to = Process.create({'code': 'ZA4T', 'name': 'Destino A4'})
        Activity = self.env['sgi.process.activity']
        act_from = Activity.create({
            'process_id': p_from.id, 'name': 'Entrega',
            'measure_state': 'verde',
            'measure_last_date': fields.Datetime.now()})
        act_to = Activity.create({
            'process_id': p_to.id, 'name': 'Recibe',
            'measure_state': 'rojo'})
        closed_nc = self.env['quality.alert'].create({
            'title': 'NC episodio 1', 'team_id': self.team_int.id,
            'stage_id': self.stage_closed.id})
        link = self.env['sgi.activity.link'].create({
            'from_activity_id': act_from.id, 'to_activity_id': act_to.id,
            'name': 'Entregable A4'})
        link.write({
            'atorado_since': fields.Datetime.now() - timedelta(days=10),
            'nc_alert_id': closed_nc.id})
        link._sgi_evaluate_chain()
        self.assertTrue(link.nc_alert_id)
        self.assertNotEqual(link.nc_alert_id, closed_nc,
                            "El segundo episodio debe generar su propia NC.")
        self.assertEqual(link.nc_alert_id.sgi_source_id.code, 'eslabon_atorado')

    def test_a4_chain_source_off_no_orphan(self):
        source = self.env.ref('quimibond_sgi.sgi_alert_source_chain_stuck')
        source.enabled = False
        self.addCleanup(setattr, source, 'enabled', True)
        Process = self.env['sgi.process']
        p_from = Process.create({'code': 'ZA4X', 'name': 'Origen A4b'})
        p_to = Process.create({'code': 'ZA4Y', 'name': 'Destino A4b'})
        Activity = self.env['sgi.process.activity']
        act_from = Activity.create({
            'process_id': p_from.id, 'name': 'Entrega',
            'measure_state': 'verde'})
        act_to = Activity.create({
            'process_id': p_to.id, 'name': 'Recibe',
            'measure_state': 'rojo'})
        link = self.env['sgi.activity.link'].create({
            'from_activity_id': act_from.id, 'to_activity_id': act_to.id,
            'name': 'Entregable A4b'})
        link.write(
            {'atorado_since': fields.Datetime.now() - timedelta(days=10)})
        link._sgi_evaluate_chain()  # fuente apagada: no truena y no crea NC
        self.assertFalse(link.nc_alert_id)

    # ------------------------------------------------------------------
    # A5 — un filtro de evidencia inválido da UserError, no traceback
    # ------------------------------------------------------------------
    def test_a5_invalid_measure_domain(self):
        model = self.env['ir.model'].search(
            [('model', '=', 'res.partner')], limit=1)
        process = self.env['sgi.process'].create(
            {'code': 'ZA5', 'name': 'Proceso A5'})
        activity = self.env['sgi.process.activity'].create({
            'process_id': process.id, 'name': 'Paso medible',
            'measure_model_id': model.id,
            'measure_domain': "[('campo_que_no_existe', '=', 1)]"})
        with self.assertRaises(UserError):
            activity.action_view_measure_records()
        activity.measure_domain = "[('is_company', '=', True)]"
        action = activity.action_view_measure_records()
        self.assertEqual(action['res_model'], 'res.partner')

    # ------------------------------------------------------------------
    # A6 — write directo de state no brinca el candado de cierre
    # ------------------------------------------------------------------
    def test_a6_incident_close_via_write(self):
        incident = self.env['sgi.incident'].create(
            {'name': 'Incidente A6', 'incident_type': 'casi_accidente'})
        with self.assertRaises(UserError):
            incident.with_user(self.sgi_user).write({'state': 'cerrado'})
        incident.write({
            'immediate_causes': 'a', 'basic_causes': 'b',
            'lack_of_control': 'c'})
        self.env['sgi.action.line'].create({
            'incident_id': incident.id, 'name': 'Acción',
            'responsible_id': self.sgi_user.id,
            'date_commit': date.today(), 'date_done': date.today()})
        incident.with_user(self.sgi_user).write({'state': 'cerrado'})
        self.assertEqual(incident.state, 'cerrado')
