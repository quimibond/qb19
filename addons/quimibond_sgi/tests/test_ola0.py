# -*- coding: utf-8 -*-
"""OLA 0 — Cimiento técnico del SGI.

Cubre: folio único centralizado en sgi.base.mixin, acciones como actividades
nativas, inmutabilidad de registros cerrados y candado de riesgo alto (H11).
"""
from psycopg2 import IntegrityError

from odoo import fields
from odoo.exceptions import UserError
from odoo.tests import TransactionCase, tagged
from odoo.tools import mute_logger


@tagged('post_install', '-at_install')
class TestOla0Base(TransactionCase):
    """El mixin asigna folio con secuencia y garantiza unicidad."""

    def test_01_folio_auto_assigned(self):
        risk = self.env['sgi.risk'].create({'name': 'R mixin', 'instrument': 'ryo'})
        audit = self.env['sgi.audit'].create({'audit_type': 'interna'})
        self.assertTrue(risk.folio and risk.folio != '/', "El riesgo debe recibir folio.")
        self.assertTrue(audit.folio and audit.folio != '/', "La auditoría debe recibir folio.")

    def test_02_folio_unique_per_model(self):
        self.env['sgi.risk'].create(
            {'name': 'R1', 'instrument': 'ryo', 'folio': 'OLA0-DUP-1'})
        with self.assertRaises(IntegrityError), mute_logger('odoo.sql_db'):
            self.env['sgi.risk'].create(
                {'name': 'R2', 'instrument': 'ryo', 'folio': 'OLA0-DUP-1'})
            self.env.flush_all()

    def test_03_mixin_inherited(self):
        for model in ('sgi.audit', 'sgi.ppap', 'sgi.risk', 'sgi.incident',
                      'sgi.fmea', 'sgi.control.plan', 'sgi.management.review'):
            Model = self.env[model]
            self.assertIn('folio', Model._fields,
                          "%s debe tener folio del cimiento." % model)
            self.assertTrue(hasattr(Model, '_sgi_schedule_activity'),
                            "%s debe tener los helpers del cimiento." % model)
            self.assertTrue(hasattr(Model, '_sgi_locked_records'),
                            "%s debe tener el candado del cimiento." % model)


@tagged('post_install', '-at_install')
class TestOla0Activities(TransactionCase):
    """Las acciones se hacen accionables como actividades nativas."""

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.user = cls.env['res.users'].create(
            {'name': 'Resp OLA0', 'login': 'resp_ola0'})
        cls.risk = cls.env['sgi.risk'].create({'name': 'R', 'instrument': 'ryo'})

    def _make_line(self, **kw):
        vals = {'risk_id': self.risk.id, 'action_type': 'correctiva',
                'name': 'Acción X', 'responsible_id': self.user.id,
                'date_commit': fields.Date.today()}
        vals.update(kw)
        return self.env['sgi.action.line'].create(vals)

    def _count(self):
        return self.env['mail.activity'].search_count(
            [('res_model', '=', 'sgi.risk'), ('res_id', '=', self.risk.id)])

    def test_01_activity_created_on_origin(self):
        line = self._make_line()
        self.assertTrue(line.activity_id, "Debe crearse la actividad.")
        self.assertEqual(line.activity_id.res_model, 'sgi.risk')
        self.assertEqual(line.activity_id.res_id, self.risk.id)
        self.assertEqual(line.activity_id.user_id, self.user)
        self.assertEqual(line.activity_id.date_deadline, fields.Date.today())

    def test_02_reassign_reschedule_no_dup(self):
        line = self._make_line()
        n = self._count()
        u2 = self.env['res.users'].create(
            {'name': 'U2', 'login': 'resp_ola0_2'})
        line.write({'responsible_id': u2.id,
                    'date_commit': fields.Date.to_date('2030-01-01')})
        self.assertEqual(line.activity_id.user_id, u2, "Se reasigna.")
        self.assertEqual(line.activity_id.date_deadline,
                         fields.Date.to_date('2030-01-01'), "Se reagenda.")
        self.assertEqual(self._count(), n, "Sin actividad duplicada.")

    def test_03_done_marks_activity(self):
        line = self._make_line()
        aid = line.activity_id.id
        line.write({'date_done': fields.Date.today()})
        self.assertFalse(line.activity_id, "El enlace se suelta al terminar.")
        self.assertEqual(
            self.env['mail.activity'].search_count([('id', '=', aid)]), 0,
            "La actividad deja de estar pendiente (archivada).")
        self.assertEqual(line.state, 'terminada')

    def test_04_bulk_import_idempotent(self):
        lines = self.env['sgi.action.line'].create([
            {'risk_id': self.risk.id, 'action_type': 'correccion',
             'name': 'A%d' % i, 'responsible_id': self.user.id,
             'date_commit': fields.Date.today()} for i in range(10)])
        acts = lines.mapped('activity_id')
        self.assertEqual(len(acts), 10, "Una actividad por acción, sin duplicar.")
        # Re-sincronizar no crea nuevas.
        before = self._count()
        lines._sgi_sync_activity()
        self.assertEqual(self._count(), before, "La sincronización es idempotente.")


@tagged('post_install', '-at_install')
class TestOla0Lock(TransactionCase):
    """Inmutabilidad de registros cerrados: sólo MAST reabre/edita."""

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        base = cls.env.ref('base.group_user').id
        sgiu = cls.env.ref('quimibond_sgi.group_sgi_user').id
        mgr = cls.env.ref('quimibond_sgi.group_sgi_manager').id
        cls.raso = cls.env['res.users'].create(
            {'name': 'SGI raso', 'login': 'ola0_raso', 'group_ids': [(6, 0, [base, sgiu])]})
        cls.mast = cls.env['res.users'].create(
            {'name': 'SGI MAST', 'login': 'ola0_mast', 'group_ids': [(6, 0, [base, sgiu, mgr])]})

    def _closed_incident(self):
        inc = self.env['sgi.incident'].create({
            'name': 'Incidente cerrado', 'severity': 'leve',
            'incident_type': 'casi_accidente', 'immediate_causes': 'a',
            'basic_causes': 'b', 'lack_of_control': 'c'})
        self.env['sgi.action.line'].create({
            'incident_id': inc.id, 'name': 'acc', 'responsible_id': self.mast.id,
            'date_commit': fields.Date.today(), 'date_done': fields.Date.today()})
        inc.action_set_investigacion()
        inc.action_set_cerrado()
        return inc

    def test_01_raso_cannot_edit_closed(self):
        inc = self._closed_incident()
        self.assertEqual(inc.state, 'cerrado')
        with self.assertRaises(UserError):
            inc.with_user(self.raso).write({'name': 'modificado'})

    def test_02_raso_cannot_reopen_closed(self):
        inc = self._closed_incident()
        with self.assertRaises(UserError):
            inc.with_user(self.raso).action_set_reportado()

    def test_03_mast_can_reopen_and_edit(self):
        inc = self._closed_incident()
        inc.with_user(self.mast).action_set_reportado()
        self.assertEqual(inc.state, 'reportado')
        inc.with_user(self.mast).write({'name': 'corregido por MAST'})
        self.assertEqual(inc.name, 'corregido por MAST')

    def test_04_locked_states_declared(self):
        self.assertEqual(self.env['sgi.ppap']._sgi_locked_states, ('aprobado',))
        self.assertEqual(self.env['sgi.audit']._sgi_locked_states, ('cerrada',))
        self.assertEqual(self.env['sgi.management.review']._sgi_locked_states, ('cerrada',))
        self.assertEqual(self.env['sgi.risk']._sgi_locked_states, ('cerrado',))
        self.assertEqual(self.env['sgi.incident']._sgi_locked_states, ('cerrado',))

    def test_05_lock_does_not_block_related_recompute(self):
        # Cerrar una acción de un riesgo cerrado (recompute indirecto) no debe
        # dispararse contra el candado del riesgo.
        risk = self.env['sgi.risk'].create({
            'name': 'R', 'instrument': 'ryo',
            'eval_probability': '4', 'eval_impact': '4'})
        line = self.env['sgi.action.line'].create({
            'risk_id': risk.id, 'name': 'a', 'responsible_id': self.mast.id,
            'date_commit': fields.Date.today()})
        risk.write({'state': 'cerrado'})
        line.with_user(self.raso).write({'date_done': fields.Date.today()})
        self.assertEqual(line.state, 'terminada')
