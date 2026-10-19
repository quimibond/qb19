# -*- coding: utf-8 -*-
"""57.159.0 (Jose, 2026-10-19): escalamientos en pausa mientras arranca el SGI.

Con ``quimibond_sgi.escalations_enabled`` vacío (el default) lo vencido le
llega solo a quien lo tiene que hacer: nada sube al jefe, al dueño del
proceso, a MAST ni a Dirección, y lo ya escalado se cierra con nota."""
from datetime import timedelta

from odoo import fields
from odoo.tests import TransactionCase, new_test_user, tagged

from .common_calendar import sgi_test_calendar
from .common_escalations import sgi_escalations_on
from .common_users import sgi_set_director, sgi_set_mast
from ..models.sgi_calendar import sgi_add_business_days, sgi_today


@tagged('post_install', '-at_install')
class TestEscalamientosPausa(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        sgi_test_calendar(cls.env)
        cls.env['ir.config_parameter'].sudo().set_param('quimibond_sgi.escalations_enabled', '')
        cls.env.user.group_ids = [
            (4, cls.env.ref('quimibond_sgi.group_sgi_manager').id),
            (4, cls.env.ref('quimibond_sgi.group_sgi_director').id)]
        cls.mast = sgi_set_mast(cls.env)
        cls.director = sgi_set_director(cls.env)
        cls.boss = cls.env['res.users'].create({'name': 'Jefe pausa', 'login': 'pausa_jefe'})
        boss_emp = cls.env['hr.employee'].create({'name': 'Jefe pausa emp', 'user_id': cls.boss.id})
        cls.resp = cls.env['res.users'].create({'name': 'Resp pausa', 'login': 'pausa_resp'})
        cls.env['hr.employee'].create(
            {'name': 'Resp pausa emp', 'user_id': cls.resp.id, 'parent_id': boss_emp.id})
        cls.risk = cls.env['sgi.risk'].create({'name': 'R pausa', 'instrument': 'ryo'})
        cls.nc_user = new_test_user(cls.env, login='pausa_nc',
                                    groups='base.group_user,quimibond_sgi.group_sgi_user')
        owner_user = new_test_user(cls.env, login='pausa_owner',
                                   groups='base.group_user,quimibond_sgi.group_sgi_user')
        cls.process = cls.env['sgi.process'].create({
            'code': 'XPAU', 'name': 'Proceso pausa',
            'owner_id': cls.env['hr.employee'].create({'name': 'Dueño pausa', 'user_id': owner_user.id}).id})

    def _risk_summaries(self):
        return self.env['mail.activity'].search(
            [('res_model', '=', 'sgi.risk'), ('res_id', '=', self.risk.id)]).mapped('summary')

    def _overdue_line(self, days):
        return self.env['sgi.action.line'].create({
            'risk_id': self.risk.id, 'name': 'Acción %d' % days, 'responsible_id': self.resp.id,
            'date_commit': sgi_add_business_days(self.env, sgi_today(self.env), -days)})

    def test_01_apagado_por_omision(self):
        self.assertFalse(self.env['sgi.cron']._sgi_escalations_on())
        sgi_escalations_on(self.env)
        self.assertTrue(self.env['sgi.cron']._sgi_escalations_on())

    def test_02_acciones_vencidas_no_escalan(self):
        self._overdue_line(20)
        self.env['sgi.cron'].cron_overdue_actions()
        self.assertFalse(any('escalada' in s for s in self._risk_summaries()))

    def test_03_al_apagar_se_cierra_lo_escalado(self):
        self._overdue_line(20)
        sgi_escalations_on(self.env)
        self.env['sgi.cron'].cron_overdue_actions()
        self.assertTrue(any('escalada' in s for s in self._risk_summaries()))
        self.env['ir.config_parameter'].sudo().set_param('quimibond_sgi.escalations_enabled', '')
        self.assertTrue(self.env['sgi.cron']._sgi_pause_escalations_now())
        self.assertFalse(any('escalada' in s for s in self._risk_summaries()))

    def test_04_nc_vencida_solo_al_responsable(self):
        nc = self.env['quality.alert'].create({
            'title': 'NC pausa', 'team_id': self.env.ref('quimibond_sgi.sgi_quality_team_internal').id,
            'stage_id': self.env.ref('quimibond_sgi.sgi_nc_int_stage_open').id,
            'sgi_process_id': self.process.id, 'sgi_responsible_ids': [(6, 0, self.nc_user.ids)]})
        later = fields.Date.context_today(nc) + timedelta(days=30)
        scheduled = nc._sgi_deadline_escalation(later)
        self.assertTrue(scheduled, "El responsable sigue recibiendo el aviso del plazo.")
        self.assertFalse(any('escalada' in s for s in scheduled))
