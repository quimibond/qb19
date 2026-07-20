# -*- coding: utf-8 -*-
"""OLA 2 — La Línea Dorada (cascada ISO, general → particular)."""
from psycopg2 import IntegrityError

from odoo.tests import TransactionCase, tagged
from odoo.tools import mute_logger


@tagged('post_install', '-at_install')
class TestOla2Policy(TransactionCase):
    """H13: política integral como cabeza de la cascada."""

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.Policy = cls.env['sgi.policy']

    def test_01_policy_folio_and_cascade(self):
        policy = self.Policy.create({'name': 'Política integral 2026'})
        self.assertTrue(policy.folio and policy.folio != '/')
        obj = self.env['sgi.objective'].create(
            {'name': 'Objetivo A', 'policy_id': policy.id})
        self.assertEqual(obj.policy_id, policy)
        self.assertEqual(policy.objective_count, 1)

    def test_02_publish_obsoletes_previous(self):
        p1 = self.Policy.create({'name': 'P1'})
        p1.action_set_vigente()
        self.assertEqual(p1.state, 'vigente')
        p2 = self.Policy.create({'name': 'P2'})
        p2.action_set_vigente()
        self.assertEqual(p2.state, 'vigente')
        self.assertEqual(p1.state, 'obsoleta',
                         "Publicar una nueva obsoleta la anterior.")

    def test_03_only_one_vigente_enforced_in_db(self):
        self.Policy.create({'name': 'V1'}).action_set_vigente()
        p2 = self.Policy.create({'name': 'V2'})
        with self.assertRaises(IntegrityError), self.cr.savepoint(), \
                mute_logger('odoo.sql_db'):
            p2.write({'state': 'vigente'})
            self.env.flush_all()

    def test_04_full_cascade_chain(self):
        policy = self.Policy.create({'name': 'Política cascada'})
        obj = self.env['sgi.objective'].create(
            {'name': 'Obj', 'policy_id': policy.id})
        proc = self.env['sgi.process'].search([], limit=1)
        ind = self.env['sgi.indicator'].create({
            'code': 'CAS-01', 'name': 'KPI cascada', 'calc_mode': 'manual',
            'objective_id': obj.id, 'process_id': proc.id})
        # Política -> Objetivo -> Indicador -> Proceso
        self.assertEqual(ind.objective_id.policy_id, policy)
        self.assertEqual(ind.process_id, proc)
