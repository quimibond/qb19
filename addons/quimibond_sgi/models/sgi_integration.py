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


class PurchaseOrder(models.Model):
    _inherit = 'purchase.order'

    sgi_nc_count = fields.Integer(string="# NC proveedor", compute='_compute_sgi_nc_count')

    def _compute_sgi_nc_count(self):
        Alert = self.env['quality.alert']
        for order in self:
            partner = order.partner_id.commercial_partner_id
            order.sgi_nc_count = Alert.search_count(
                [('partner_id', '=', partner.id)]) if partner else 0

    def action_sgi_open_ncs(self):
        self.ensure_one()
        partner = self.partner_id.commercial_partner_id
        return {
            'type': 'ir.actions.act_window',
            'name': "No Conformidades del proveedor",
            'res_model': 'quality.alert',
            'view_mode': 'list,form',
            'domain': [('partner_id', '=', partner.id)],
            'context': {'default_partner_id': partner.id, 'default_sgi_origin_type': 'proceso'},
        }


class MaintenanceRequest(models.Model):
    _inherit = 'maintenance.request'

    sgi_alert_id = fields.Many2one('quality.alert', string="NC generada",
                                   readonly=True, copy=False)

    def action_sgi_raise_nc(self):
        """Levanta una NC (equipo NC Internas) desde una solicitud correctiva,
        pre-llenada con el equipo/máquina y la descripción de la falla."""
        self.ensure_one()
        if self.sgi_alert_id:
            return self._sgi_open_alert()
        team = self.env.ref('quimibond_sgi.sgi_quality_team_internal',
                            raise_if_not_found=False)
        equipment_name = self.equipment_id.name or ''
        vals = {
            'title': "Falla de mantenimiento: %s" % (equipment_name or self.name),
            'sgi_origin_type': 'proceso',
            'sgi_deviation': "Solicitud de mantenimiento «%s» sobre el equipo «%s».\n%s" % (
                self.name or '', equipment_name, self.description or ''),
        }
        if team:
            vals['team_id'] = team.id
        alert = self.env['quality.alert'].sgi_auto_create('mantenimiento_falla', vals)
        self.sgi_alert_id = alert.id
        self.message_post(body="Se levantó la NC <b>%s</b> por esta falla." % (
            alert.sgi_folio or alert.name))
        return self._sgi_open_alert()

    def _sgi_open_alert(self):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'name': "No Conformidad",
            'res_model': 'quality.alert',
            'view_mode': 'form',
            'res_id': self.sgi_alert_id.id,
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
        list_view = self.env.ref('quimibond_sgi.sgi_document_view_list',
                                 raise_if_not_found=False)
        form_view = self.env.ref('quimibond_sgi.sgi_document_view_form',
                                 raise_if_not_found=False)
        return {
            'type': 'ir.actions.act_window',
            'name': "Mis procedimientos",
            'res_model': 'documents.document',
            'view_mode': 'list,form',
            # Fija la ficha SGI (clave/estado/tipo/familia); sin ella Odoo abre
            # el formulario mínimo de captura de URL de la app Documentos.
            'views': [
                (list_view.id if list_view else False, 'list'),
                (form_view.id if form_view else False, 'form'),
            ],
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
