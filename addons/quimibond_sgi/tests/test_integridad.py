# -*- coding: utf-8 -*-
"""Entrega 3, integridad (auditoría 2026-09).

- C-013: borrar un proceso, un área, una política, un objetivo o una
  cláusula que algo usa ya no deja huérfanos en silencio (``restrict``); los
  procesos se archivan.
- B-008: el «arranque externo» de la especificación sale de la primera etapa
  del proceso, no del campo ``block`` que se retira."""
from psycopg2 import IntegrityError

from odoo.exceptions import UserError
from odoo.tests import TransactionCase, tagged
from odoo.tools import mute_logger


@tagged('post_install', '-at_install')
class TestIntegridad(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.Process = cls.env['sgi.process']
        cls.process = cls.Process.create({'code': 'ZINT', 'name': 'Proceso integridad'})
        cls.indicator = cls.env['sgi.indicator'].create({
            'code': 'ZINT-01', 'name': 'KPI integridad', 'calc_mode': 'manual',
            'process_id': cls.process.id})

    def _assert_restricted(self, record):
        with mute_logger('odoo.sql_db'), self.assertRaises((IntegrityError, UserError)):
            record.unlink()
            self.env.flush_all()
        self.assertTrue(record.exists())

    def test_01_process_with_indicator_cannot_be_deleted(self):
        self._assert_restricted(self.process)
        self.assertEqual(self.indicator.process_id, self.process)

    def test_02_process_is_archived_instead(self):
        self.process.active = False
        self.assertFalse(self.process.active)
        self.assertEqual(self.indicator.process_id, self.process)

    def test_03_unused_process_still_deletes(self):
        loose = self.Process.create({'code': 'ZINT2', 'name': 'Proceso suelto'})
        loose.unlink()
        self.assertFalse(loose.exists())

    def test_04_evidence_chain_policy_objective_indicator(self):
        policy = self.env['sgi.policy'].create({'name': 'Política integridad'})
        objective = self.env['sgi.objective'].create({
            'name': 'Objetivo integridad', 'policy_id': policy.id})
        self.indicator.objective_id = objective
        self._assert_restricted(policy)
        self._assert_restricted(objective)

    def test_05_area_in_use_cannot_be_deleted(self):
        area = self.env['sgi.area'].create({'code': 'ZINTA', 'name': 'Área integridad'})
        self.indicator.sgi_area_id = area
        self._assert_restricted(area)

    def test_06_external_start_comes_from_first_stage(self):
        """B-008: una actividad de la primera etapa cuya entrada no produce
        ninguna actividad no pide plazo; la misma en otra etapa sí."""
        job = self.env['hr.job'].create({'name': 'PUESTO INTEGRIDAD'})
        self.env['hr.employee'].create({'name': 'Emp Integridad', 'job_id': job.id})
        Stage = self.env['sgi.process.stage']
        first = Stage.create({'process_id': self.process.id, 'name': 'A. Entrada', 'sequence': 10})
        second = Stage.create({'process_id': self.process.id, 'name': 'B. Después', 'sequence': 20})
        outside = self.env['sgi.deliverable'].create({'code': 'ZINT-IN', 'name': 'Pedido del cliente'})

        def act(stage, name):
            return self.env['sgi.process.activity'].create({
                'process_id': self.process.id, 'stage_id': stage.id, 'name': name,
                'role_ids': [(0, 0, {'role': 'ejecuta', 'job_id': job.id})],
                'input_ids': [(0, 0, {'deliverable_id': outside.id})],
            })

        starts = act(first, 'Registrar el pedido del cliente')
        later = act(second, 'Revisar el pedido del cliente')
        self.assertNotIn('no_timing', set(starts.spec_gap_ids.mapped('code')))
        self.assertIn('no_timing', set(later.spec_gap_ids.mapped('code')))
