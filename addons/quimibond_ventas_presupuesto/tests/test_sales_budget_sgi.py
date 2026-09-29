# -*- coding: utf-8 -*-
"""Lo que el SGI probaba del presupuesto de ventas y se mudó con él en
quimibond_sgi 57.11.0 (A-016, J-018):

- test_multicompany test_03 y la regla de empresa de sus dos modelos (F-014);
- test_entrega4 test_02: Dirección no escribe el presupuesto;
- test_avisos_crons: cron_forecast_coverage corre dos veces sin duplicar
  avisos (J-008).
"""
from odoo.exceptions import AccessError
from odoo.tests import TransactionCase, new_test_user, tagged


@tagged('post_install', '-at_install')
class TestSalesBudgetSgi(TransactionCase):

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
        cls.director = new_test_user(cls.env, login='e4_director',
                                     groups='base.group_user,quimibond_sgi.group_sgi_director')

    def _as_user(self, *companies):
        return self.env(user=self.user, context=dict(
            self.env.context, allowed_company_ids=[c.id for c in companies]))

    def test_01_sales_budget_of_b_hidden(self):
        """Antes quimibond_sgi test_multicompany test_03."""
        team = self.env['crm.team'].create({'name': 'ZMC Mercado'})
        budget_b = self.env['sgi.sales.budget'].with_company(self.company_b).create({
            'year': 2049, 'team_id': team.id, 'company_id': self.company_b.id})
        env_a = self._as_user(self.company_a)
        self.assertFalse(env_a['sgi.sales.budget'].search([('id', '=', budget_b.id)]))
        env_ab = self._as_user(self.company_a, self.company_b)
        self.assertEqual(env_ab['sgi.sales.budget'].search([('id', '=', budget_b.id)]), budget_b)

    def test_02_budget_models_have_a_company_rule(self):
        """Antes en F014_MODELS de quimibond_sgi test_multicompany test_05."""
        Rule = self.env['ir.rule'].sudo()
        for model in ('sgi.sales.budget', 'sgi.sales.budget.line'):
            rules = Rule.search([('model_id.model', '=', model), ('global', '=', True),
                                 ('domain_force', 'ilike', 'company_ids')])
            self.assertTrue(rules, "%s sin regla de empresa" % model)

    def test_03_director_does_not_write_the_budget(self):
        """Antes quimibond_sgi test_entrega4 test_02: Dirección ya no escribe el
        presupuesto de ventas (era del Jefe MAST), pero lo aprueba:
        action_approve revisa el grupo y sella con sudo (test_sales_budget)."""
        with self.assertRaises(AccessError):
            self.env['sgi.sales.budget'].with_user(self.director).check_access('write')

    def test_04_forecast_coverage_twice_without_duplicates(self):
        """Antes en AVISO_CRONS de quimibond_sgi test_avisos_crons (J-008)."""
        Activity = self.env['mail.activity'].with_context(active_test=False)
        self.env['sgi.cron'].cron_forecast_coverage()
        first = Activity.search([('active', '=', True)])
        self.env['sgi.cron'].cron_forecast_coverage()
        second = Activity.search([('active', '=', True)])
        self.assertFalse(second - first, "La segunda corrida no crea avisos: %s" % (
            (second - first).mapped('summary')))
