# -*- coding: utf-8 -*-
"""57.19.0 — reclamaciones por marca de equipo (D-006, D-010; decisión 9).

Datos propios: equipos creados en la prueba, nunca los de producción."""
from odoo.exceptions import UserError
from odoo.tests import TransactionCase, tagged


@tagged('post_install', '-at_install')
class TestReclamaciones(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        Team = cls.env['helpdesk.team']
        company = cls.env['sgi.config']._sgi_company()
        cls.team_complaint = Team.create({
            'name': 'Reclamaciones entretelas', 'company_id': company.id})
        cls.team_other = Team.create({
            'name': 'Tickets de Sistemas prueba', 'company_id': company.id})
        cls.partner = cls.env['res.partner'].create({'name': 'Cliente reclamación prueba'})

    def test_01_mark_by_name_idempotent(self):
        """El post-migrate marca por nombre exacto en la empresa del SGI, y
        una segunda corrida no vuelve a escribir nada."""
        marked = self.env['helpdesk.team']._sgi_mark_complaint_teams()
        self.assertIn(self.team_complaint, marked)
        self.assertTrue(self.team_complaint.sgi_is_complaint)
        self.assertFalse(self.team_other.sgi_is_complaint)
        self.assertTrue(self.env.ref('quimibond_sgi.sgi_helpdesk_team_complaints').sgi_is_complaint)
        self.assertFalse(self.env['helpdesk.team']._sgi_mark_complaint_teams())

    def test_02_generate_nc_only_in_complaint_teams(self):
        self.team_complaint.sgi_is_complaint = True
        other = self.env['helpdesk.ticket'].create({
            'name': 'Impresora sin tóner', 'team_id': self.team_other.id})
        self.assertFalse(other.sgi_is_complaint_team)
        with self.assertRaises(UserError):
            other.action_sgi_generate_nc()
        self.assertFalse(other.sgi_alert_id)
        ticket = self.env['helpdesk.ticket'].create({
            'name': 'Rollo manchado', 'team_id': self.team_complaint.id,
            'partner_id': self.partner.id})
        self.assertTrue(ticket.sgi_is_complaint_team)
        ticket.action_sgi_generate_nc()
        self.assertEqual(ticket.sgi_alert_id.sgi_complaint_ticket_id, ticket)

    def test_03_menu_and_kpi_follow_the_mark(self):
        """«Reclamaciones de clientes» y el KPI de reclamaciones leen todos
        los equipos marcados, no el XML ID."""
        self.team_complaint.sgi_is_complaint = True
        ticket = self.env['helpdesk.ticket'].create({
            'name': 'Metraje corto', 'team_id': self.team_complaint.id})
        noise = self.env['helpdesk.ticket'].create({
            'name': 'Cambio de contraseña', 'team_id': self.team_other.id})
        action = self.env.ref('quimibond_sgi.sgi_complaint_action').run()
        found = self.env['helpdesk.ticket'].search(action['domain'])
        self.assertIn(ticket, found)
        self.assertNotIn(noise, found)
        domain = self.env['helpdesk.team']._sgi_complaint_domain()
        self.assertIn(ticket, self.env['helpdesk.ticket'].search(domain))
        self.assertNotIn(noise, self.env['helpdesk.ticket'].search(domain))

    def test_04_mark_finds_name_only_in_spanish(self):
        """57.85.0: el caso de producción. Los equipos 2 y 14 tienen su nombre
        en español en la llave ``es_MX`` del JSONB y otro texto en ``en_US``
        (creados antes de Odoo 16 o renombrados con el usuario en español). La
        migración corre sin ``lang`` y la 57.19.0 los buscaba por ``name in``,
        que compara solo ``en_US``: no los encontró. Ahora se compara el nombre
        en cada idioma instalado, sin distinguir mayúsculas."""
        self.env['res.lang']._activate_lang('es_MX')
        Team = self.env['helpdesk.team']
        company = self.env['sgi.config']._sgi_company()
        team = Team.with_context(lang='en_US').create({
            'name': 'Complaints interlinings', 'company_id': company.id})
        team.with_context(lang='es_MX').name = 'Reclamaciones entretelas'
        team_case = Team.with_context(lang='en_US').create({
            'name': 'Customer care', 'company_id': company.id})
        team_case.with_context(lang='es_MX').name = 'Atencion a Clientes '
        decoy = Team.with_context(lang='en_US').create({
            'name': 'Reclamación Industrial', 'company_id': company.id})
        self.assertEqual(team.with_context(lang='en_US').name, 'Complaints interlinings')
        # Reproduce el fallo de la 57.19.0: la búsqueda sin idioma no lo ve.
        old = Team.with_context(active_test=False, lang=None).search([
            ('name', 'in', list(Team._SGI_COMPLAINT_TEAM_NAMES)),
            ('company_id', '=', company.id)])
        self.assertNotIn(team, old)
        marked = Team.with_context(lang=None)._sgi_mark_complaint_teams()
        self.assertIn(team, marked)
        self.assertIn(team_case, marked)
        self.assertNotIn(decoy, marked)
        self.assertTrue(team.sgi_is_complaint)
        self.assertTrue(team_case.sgi_is_complaint)
        self.assertFalse(decoy.sgi_is_complaint)
        # Solo agrega: un equipo ya marcado no se reescribe y nada se desmarca.
        self.team_other.sgi_is_complaint = True
        self.assertFalse(Team._sgi_mark_complaint_teams())
        self.assertTrue(self.team_other.sgi_is_complaint)
