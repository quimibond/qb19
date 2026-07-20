# -*- coding: utf-8 -*-
"""OLA 2 — La Línea Dorada (cascada ISO, general → particular)."""
from datetime import date

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


@tagged('post_install', '-at_install')
class TestOla2Health(TransactionCase):
    """H13: salud por proceso por agregación + cascada de color."""

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.env.user.group_ids = [
            (4, cls.env.ref('quimibond_sgi.group_sgi_manager').id)]
        # Proceso limpio y aislado para no arrastrar datos de demo.
        cls.proc = cls.env['sgi.process'].create(
            {'code': 'HLTH-01', 'name': 'Proceso salud', 'process_type': 'soporte'})
        cls.team = cls.env.ref('quimibond_sgi.sgi_quality_team_internal')

    def _red_kpi(self):
        ind = self.env['sgi.indicator'].create({
            'code': 'RED-01', 'name': 'KPI rojo', 'calc_mode': 'manual',
            'direction': 'higher_better', 'target_objective': 100,
            'target_acceptable': 80, 'process_id': self.proc.id})
        measure = self.env['sgi.indicator.measure'].create({
            'indicator_id': ind.id, 'period_date': date.today().replace(day=1),
            'value': 40.0})
        measure.action_validate()
        self.assertEqual(measure.semaphore, 'rojo')
        return ind

    def test_01_clean_process_is_green(self):
        self.assertEqual(self.proc.health, 'verde')

    def test_02_open_nc_only_is_yellow(self):
        self.env['quality.alert'].create({
            'title': 'NC', 'team_id': self.team.id,
            'sgi_process_id': self.proc.id})
        self.proc.invalidate_recordset()
        self.assertEqual(self.proc.health, 'amarillo')

    def test_03_high_risk_is_red(self):
        self.env['sgi.risk'].create({
            'name': 'Alto', 'instrument': 'ryo', 'eval_probability': '5',
            'eval_impact': '5', 'process_id': self.proc.id})
        self.proc.invalidate_recordset()
        self.assertEqual(self.proc.open_high_risk_count, 1)
        self.assertEqual(self.proc.health, 'rojo')

    def test_04_nc_plus_red_kpi_is_red(self):
        self._red_kpi()
        self.env['quality.alert'].create({
            'title': 'NC', 'team_id': self.team.id,
            'sgi_process_id': self.proc.id})
        self.proc.invalidate_recordset()
        self.assertEqual(self.proc.red_kpi_count, 1)
        self.assertEqual(self.proc.health, 'rojo')

    def test_05_cascade_worst_color_wins(self):
        self.env['sgi.risk'].create({
            'name': 'Alto', 'instrument': 'ryo', 'eval_probability': '5',
            'eval_impact': '5', 'process_id': self.proc.id})
        policy = self.env['sgi.policy'].create({'name': 'Pol'})
        obj = self.env['sgi.objective'].create(
            {'name': 'Obj', 'policy_id': policy.id})
        self.env['sgi.indicator'].create({
            'code': 'AGG-1', 'name': 'k', 'calc_mode': 'manual',
            'objective_id': obj.id, 'process_id': self.proc.id})
        obj.invalidate_recordset()
        policy.invalidate_recordset()
        self.assertEqual(obj.health, 'rojo')
        self.assertEqual(policy.health, 'rojo')


@tagged('post_install', '-at_install')
class TestOla2DocFamily(TransactionCase):
    """H21: familia documental por FK real, no por regex."""

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.Doc = cls.env['documents.document']

    def _doc(self, code, doc_type):
        return self.Doc.create({
            'name': '%s.pdf' % code, 'type': 'binary',
            'sgi_is_controlled': True, 'sgi_doc_type': doc_type,
            'sgi_code': code, 'sgi_state': 'vigente'})

    def test_01_fk_is_stored_and_editable(self):
        parent = self._doc('P-C99', 'procedimiento')
        child = self._doc('F-P-C99-01', 'formato')
        # Editable directamente (sin regex).
        child.sgi_parent_document_id = parent
        self.assertEqual(child.sgi_parent_document_id, parent)
        self.assertIn(child, parent.sgi_family_document_ids)

    def test_02_family_from_fk(self):
        parent = self._doc('P-C98', 'procedimiento')
        f1 = self._doc('F-P-C98-01', 'formato')
        f2 = self._doc('IT-P-C98-01', 'instructivo')
        (f1 | f2).write({'sgi_parent_document_id': parent.id})
        self.assertEqual(set(parent.sgi_family_document_ids.ids), {f1.id, f2.id})
        # Un hijo ve a su hermano.
        self.assertIn(f2, f1.sgi_family_document_ids)
        self.assertFalse(parent.sgi_parent_document_id)

    def test_03_migration_fills_empty_only(self):
        parent = self._doc('P-C97', 'procedimiento')
        child = self._doc('F-P-C97-01', 'formato')
        self.env['sgi.config'].migrate_document_families()
        self.assertEqual(child.sgi_parent_document_id, parent)
        # Respeta un enlace manual a otro padre: no lo pisa.
        other = self._doc('P-C96', 'procedimiento')
        child.sgi_parent_document_id = other
        self.env['sgi.config'].migrate_document_families()
        self.assertEqual(child.sgi_parent_document_id, other,
                         "La migración solo llena vacíos, no pisa enlaces.")

    def test_04_unmatched_stays_empty(self):
        # MIID no pertenece a una familia P-Xnn: queda sin padre.
        miid = self._doc('MIID', 'procedimiento')
        self.env['sgi.config'].migrate_document_families()
        self.assertFalse(miid.sgi_parent_document_id)
