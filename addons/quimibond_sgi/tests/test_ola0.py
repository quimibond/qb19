# -*- coding: utf-8 -*-
"""OLA 0 — Cimiento técnico del SGI.

Cubre: folio único centralizado en sgi.base.mixin, acciones como actividades
nativas, inmutabilidad de registros cerrados y candado de riesgo alto (H11).
"""
from psycopg2 import IntegrityError

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
