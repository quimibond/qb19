# -*- coding: utf-8 -*-
"""J-013 (auditoría 2026-09): reglas multiempresa del SGI y D-03 (el SGI es
de una sola empresa, ``sgi.config._sgi_company``).

- Un proceso no se crea ni se mueve fuera de la empresa del SGI.
- Un usuario con la empresa A activa no ve procesos ni NC de la empresa B
  (los presupuestos, en quimibond_ventas_presupuesto), aunque tenga B permitida; al activar B sí los ve.
- Los 10 modelos con ``company_id`` que no tenían regla (F-014) la tienen.
- Indicadores, riesgos, auditorías y demás modelos sin ``company_id`` son
  compartidos a propósito (D-03): no se prueban aquí."""
from odoo.exceptions import AccessError, ValidationError
from odoo.tests import TransactionCase, new_test_user, tagged


F014_MODELS = (
    'sgi.health.record', 'sgi.staff.efficiency', 'sgi.checklist.template',
    'sgi.coa.inbox', 'sgi.csh.inspection', 'sgi.inventory.value',
    'sgi.lock.date.log', 'sgi.machine.sheet',
)
# 57.11.0 (A-016): sgi.sales.budget y sgi.sales.budget.line (y test_03) se
# prueban en quimibond_ventas_presupuesto/tests/test_sales_budget_sgi.py.


@tagged('post_install', '-at_install')
class TestMultiCompany(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.company_a = cls.env['sgi.config']._sgi_company()
        cls.company_b = cls.env['res.company'].create({'name': 'ZMC Empresa B'})
        cls.user = new_test_user(
            cls.env, login='zmc_user',
            groups='base.group_user,quimibond_sgi.group_sgi_user,quality.group_quality_user',
            company_id=cls.company_a.id,
            company_ids=[(6, 0, (cls.company_a | cls.company_b).ids)])
        Process = cls.env['sgi.process']
        cls.process_a = Process.create({'code': 'ZMCA', 'name': 'Proceso empresa A'})
        # Un proceso de B ya no se puede crear (D-03): se simula uno de antes
        # de la restricción moviéndolo por SQL.
        cls.process_b = Process.create({'code': 'ZMCB', 'name': 'Proceso empresa B'})
        cls.env.flush_all()
        cls.env.cr.execute("UPDATE sgi_process SET company_id = %s WHERE id = %s",
                           (cls.company_b.id, cls.process_b.id))
        cls.env.invalidate_all()

    def _as_user(self, *companies):
        return self.env(user=self.user, context=dict(
            self.env.context, allowed_company_ids=[c.id for c in companies]))

    # ------------------------------------------------------------------
    def test_01_process_only_in_the_sgi_company(self):
        with self.assertRaises(ValidationError):
            self.env['sgi.process'].create({
                'code': 'ZMCX', 'name': 'Proceso fuera', 'company_id': self.company_b.id})
        with self.assertRaises(ValidationError):
            self.process_a.company_id = self.company_b

    def test_02_processes_of_b_hidden_while_a_is_active(self):
        env_a = self._as_user(self.company_a)
        visible = env_a['sgi.process'].search([('code', 'in', ('ZMCA', 'ZMCB'))])
        self.assertEqual(visible, self.process_a)
        with self.assertRaises(AccessError):
            self.process_b.with_env(env_a).read(['name'])
        env_ab = self._as_user(self.company_a, self.company_b)
        self.assertEqual(
            env_ab['sgi.process'].search([('code', 'in', ('ZMCA', 'ZMCB'))]),
            self.process_a | self.process_b)

    def test_04_nonconformity_of_b_hidden(self):
        team_b = self.env['quality.alert.team'].create(
            {'name': 'ZMC Calidad B', 'company_id': self.company_b.id})
        alert_b = self.env['quality.alert'].with_company(self.company_b).create({
            'title': 'ZMC NC de B', 'team_id': team_b.id, 'company_id': self.company_b.id})
        env_a = self._as_user(self.company_a)
        self.assertFalse(env_a['quality.alert'].search([('id', '=', alert_b.id)]))

    def test_05_f014_models_have_a_company_rule(self):
        Rule = self.env['ir.rule'].sudo()
        for model in F014_MODELS:
            rules = Rule.search([('model_id.model', '=', model), ('global', '=', True),
                                 ('domain_force', 'ilike', 'company_ids')])
            self.assertTrue(rules, "%s sin regla de empresa" % model)
