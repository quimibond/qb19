# -*- coding: utf-8 -*-
from odoo import models, fields, api


class StockPicking(models.Model):
    _inherit = 'stock.picking'

    sgi_nc_count = fields.Integer(string="# NC", compute='_compute_sgi_nc_count')

    def _compute_sgi_nc_count(self):
        Alert = self.env['quality.alert']
        for picking in self:
            picking.sgi_nc_count = Alert.search_count([('picking_id', '=', picking.id)]) if picking.id else 0

    def action_sgi_open_ncs(self):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'name': "No Conformidades",
            'res_model': 'quality.alert',
            'view_mode': 'list,form',
            'domain': [('picking_id', '=', self.id)],
            'context': {'default_picking_id': self.id, 'default_sgi_origin_type': 'proceso'},
        }


class MrpProduction(models.Model):
    _inherit = 'mrp.production'

    sgi_nc_count = fields.Integer(string="# NC", compute='_compute_sgi_nc_count')

    def _compute_sgi_nc_count(self):
        Alert = self.env['quality.alert']
        for prod in self:
            prod.sgi_nc_count = Alert.search_count([('production_id', '=', prod.id)]) if prod.id else 0

    def action_sgi_open_ncs(self):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'name': "No Conformidades",
            'res_model': 'quality.alert',
            'view_mode': 'list,form',
            'domain': [('production_id', '=', self.id)],
            'context': {'default_production_id': self.id, 'default_sgi_origin_type': 'proceso'},
        }


class HrJob(models.Model):
    _inherit = 'hr.job'

    sgi_document_ids = fields.Many2many('documents.document', 'sgi_document_job_rel',
                                        'job_id', 'document_id', string="Documentos aplicables")


class HrEmployee(models.Model):
    _inherit = 'hr.employee'

    sgi_procedure_count = fields.Integer(string="# Mis procedimientos", compute='_compute_sgi_counts')
    sgi_pending_ack_count = fields.Integer(string="# Acuses pendientes", compute='_compute_sgi_counts')

    def _compute_sgi_counts(self):
        Doc = self.env['documents.document']
        Ack = self.env['sgi.document.ack']
        for emp in self:
            if emp.job_id:
                emp.sgi_procedure_count = Doc.search_count([
                    ('sgi_state', '=', 'vigente'),
                    ('sgi_job_ids', 'in', emp.job_id.id),
                ])
            else:
                emp.sgi_procedure_count = 0
            emp.sgi_pending_ack_count = Ack.search_count([
                ('employee_id', '=', emp.id),
                ('state', '=', 'pendiente'),
            ]) if emp.id else 0

    def action_sgi_my_procedures(self):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'name': "Mis procedimientos",
            'res_model': 'documents.document',
            'view_mode': 'list,form',
            'domain': [('sgi_state', '=', 'vigente'), ('sgi_job_ids', 'in', self.job_id.ids)],
        }

    def action_sgi_pending_acks(self):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'name': "Acuses pendientes",
            'res_model': 'sgi.document.ack',
            'view_mode': 'list,form',
            'domain': [('employee_id', '=', self.id), ('state', '=', 'pendiente')],
        }


class ResPartner(models.Model):
    _inherit = 'res.partner'

    sgi_complaint_count = fields.Integer(string="# Reclamaciones", compute='_compute_sgi_partner_counts')
    sgi_nc_count = fields.Integer(string="# NC", compute='_compute_sgi_partner_counts')

    def _compute_sgi_partner_counts(self):
        Ticket = self.env['helpdesk.ticket']
        Alert = self.env['quality.alert']
        for partner in self:
            if partner.id:
                partner.sgi_complaint_count = Ticket.search_count([
                    ('partner_id', '=', partner.id),
                    ('sgi_alert_id', '!=', False),
                ]) + Ticket.search_count([
                    ('partner_id', '=', partner.id),
                    ('sgi_disposition', '!=', False),
                    ('sgi_alert_id', '=', False),
                ])
                partner.sgi_nc_count = Alert.search_count([('partner_id', '=', partner.id)])
            else:
                partner.sgi_complaint_count = 0
                partner.sgi_nc_count = 0

    def action_sgi_open_complaints(self):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'name': "Reclamaciones",
            'res_model': 'helpdesk.ticket',
            'view_mode': 'list,form',
            'domain': [('partner_id', '=', self.id)],
            'context': {'default_partner_id': self.id},
        }

    def action_sgi_open_ncs(self):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'name': "No Conformidades",
            'res_model': 'quality.alert',
            'view_mode': 'list,form',
            'domain': [('partner_id', '=', self.id)],
            'context': {'default_partner_id': self.id},
        }
