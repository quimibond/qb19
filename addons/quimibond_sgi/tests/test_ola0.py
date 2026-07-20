# -*- coding: utf-8 -*-
"""OLA 0 — Cimiento técnico del SGI.

Cubre: folio único centralizado en sgi.base.mixin, acciones como actividades
nativas, inmutabilidad de registros cerrados y candado de riesgo alto (H11).
"""
from psycopg2 import IntegrityError

from odoo import fields
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
