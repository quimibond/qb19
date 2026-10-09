# -*- coding: utf-8 -*-
"""57.137.0 (revisión de Administración de Ventas 2026-10-08, punto 9): **solicitud de modificación del proyecto**.

Cuando la muestra no dio el resultado o el cliente pide cambios, Diseño de Producto llena la solicitud de
modificación: qué cambia, la causa, los **5 porqués**, la solución y las **fases afectadas**; la
firma Dirección de Operaciones. Firmada, abre la revisión siguiente del desarrollo (si la respuesta
del cliente no la abrió ya) y el proyecto vuelve a «Muestra». El brief (C1.13) había sustituido el
formato «Modificación de proyectos» (3966) por la bitácora de revisiones; la bitácora sigue (cada
cambio de especificación queda ahí) y la solicitud es el registro formal del cambio (ISO 9001:2015
8.3.6). La respuesta «pide cambios» del envío deja una solicitud en borrador para que Diseño de Producto la
complete.
"""
import base64

from odoo import api, fields, models
from odoo.exceptions import UserError

ORIGINS = [
    ('cliente', "Cambio que pide el cliente"),
    ('resultado', "No se obtuvo el resultado esperado"),
    ('interno', "Mejora interna"),
]
STATES = [('borrador', "Borrador"), ('firmada', "Firmada"), ('aplicada', "Aplicada")]
STAGE_AFTER_CHANGE = 'muestra'


class SgiDevChangeRequest(models.Model):
    _name = 'sgi.dev.change.request'
    _description = "Solicitud de modificación del proyecto de desarrollo"
    _inherit = ['mail.thread', 'mail.activity.mixin']
    _order = 'id desc'

    name = fields.Char(string="Solicitud", compute='_compute_name', store=True)
    project_id = fields.Many2one('project.project', string="Desarrollo", required=True, ondelete='cascade', index=True,
                                 domain="[('sgi_is_ft', '=', True), ('is_template', '=', False)]")
    partner_id = fields.Many2one(related='project_id.partner_id', string="Cliente")
    company_id = fields.Many2one('res.company', default=lambda self: self.env.company, required=True)
    sequence_number = fields.Integer(string="Número", readonly=True, copy=False,
                                     help="Consecutivo dentro del desarrollo.")
    date = fields.Date(string="Fecha", default=fields.Date.context_today, required=True)
    origin = fields.Selection(ORIGINS, string="Motivo", required=True, default='resultado', tracking=True)
    shipment_id = fields.Many2one('sgi.dev.shipment', string="Respuesta del cliente que la origina",
                                  ondelete='set null', readonly=True, copy=False)
    revision = fields.Integer(string="Revisión que abre", readonly=True, copy=False,
                              help="Revisión del desarrollo que queda abierta con esta modificación.")
    description = fields.Text(string="Qué se modifica", help="El cambio al producto o al proceso, en palabras del cliente o del resultado.")
    cause = fields.Text(string="Por qué no se obtuvo el resultado", help="Causa observada. Los 5 porqués llevan a la causa raíz.")
    why_1 = fields.Char(string="¿Por qué? 1")
    why_2 = fields.Char(string="¿Por qué? 2")
    why_3 = fields.Char(string="¿Por qué? 3")
    why_4 = fields.Char(string="¿Por qué? 4")
    why_5 = fields.Char(string="¿Por qué? 5")
    root_cause = fields.Text(string="Causa raíz")
    solution = fields.Text(string="Solución", help="Qué se va a cambiar para obtener el resultado.")
    stage_ids = fields.Many2many('project.project.stage', 'sgi_dev_change_request_stage_rel', 'request_id', 'stage_id',
                                 string="Fases afectadas", domain="[('id', 'in', dev_stage_ids)]")
    dev_stage_ids = fields.Many2many('project.project.stage', compute='_compute_dev_stage_ids')
    requested_by_id = fields.Many2one('res.users', string="Elaboró (Diseño de Producto)", default=lambda self: self.env.user,
                                      readonly=True)
    state = fields.Selection(STATES, string="Estado", default='borrador', required=True, tracking=True, index=True)
    approved_by_id = fields.Many2one('res.users', string="Aprobó (Dirección de Operaciones)", readonly=True, copy=False)
    approved_at = fields.Datetime(string="Firmada el", readonly=True, copy=False)
    applied_by_id = fields.Many2one('res.users', string="Aplicó los cambios", readonly=True, copy=False)
    applied_at = fields.Datetime(string="Aplicada el", readonly=True, copy=False)
    attachment_id = fields.Many2one('ir.attachment', string="PDF firmado", readonly=True, copy=False)

    @api.depends('project_id.sgi_ft_folio', 'project_id.name', 'sequence_number')
    def _compute_name(self):
        for req in self:
            req.name = "Modificación %d · %s" % (req.sequence_number or 0,
                                                 req.project_id.sgi_ft_folio or req.project_id.name or '')

    def _compute_dev_stage_ids(self):
        ids = list(self.env['project.project']._sgi_dev_stage_keys())
        for req in self:
            req.dev_stage_ids = [(6, 0, ids)]

    @api.model_create_multi
    def create(self, vals_list):
        for vals in vals_list:
            if vals.get('project_id') and not vals.get('sequence_number'):
                last = self.search([('project_id', '=', vals['project_id'])], order='sequence_number desc', limit=1)
                vals['sequence_number'] = (last.sequence_number or 0) + 1
        return super().create(vals_list)

    def unlink(self):
        if any(r.state != 'borrador' for r in self):
            raise UserError("Una solicitud firmada no se borra.")
        return super().unlink()

    def _whys(self):
        self.ensure_one()
        return [w for w in (self.why_1, self.why_2, self.why_3, self.why_4, self.why_5) if (w or '').strip()]

    def _check_before_sign(self):
        for req in self:
            faltan = []
            if not (req.description or '').strip():
                faltan.append("qué se modifica")
            if req.origin != 'cliente' and not (req.cause or '').strip():
                faltan.append("por qué no se obtuvo el resultado")
            if req.origin != 'cliente' and not req._whys():
                faltan.append("al menos un porqué")
            if not (req.solution or '').strip():
                faltan.append("la solución")
            if not req.stage_ids:
                faltan.append("las fases afectadas")
            if faltan:
                raise UserError("%s: antes de firmar falta %s." % (req.name, ", ".join(faltan)))
        return True

    def _emit_pdf(self):
        self.ensure_one()
        report = self.env.ref('quimibond_sgi.action_report_dev_change_request')
        pdf, _kind = self.env['ir.actions.report'].sudo()._render_qweb_pdf(report.report_name, res_ids=self.ids)
        return self.env['ir.attachment'].sudo().create({
            'name': "%s.pdf" % self.name, 'type': 'binary', 'datas': base64.b64encode(pdf),
            'mimetype': 'application/pdf', 'res_model': self._name, 'res_id': self.id,
        })

    def action_approve(self):
        """Firma de Dirección de Operaciones: abre la revisión del desarrollo si la respuesta del
        cliente no la abrió ya, regresa el proyecto a «Muestra» y guarda el PDF."""
        Project = self.env['project.project']
        for req in self:
            if req.state != 'borrador':
                raise UserError("%s ya está firmada." % req.name)
            req._check_before_sign()
            project = req.project_id
            if not req.revision:
                project.action_sgi_dev_new_revision()
                req.revision = project.sgi_dev_revision
            log = project.sgi_dev_revision_ids.filtered(lambda r: r.revision == req.revision).sorted('id')[:1]
            if log:
                log.write({'note': "%s · %s: %s" % (log.note or '', req.name, (req.description or '').strip()[:200]),
                           'requested_by': 'cliente' if req.origin == 'cliente' else 'interno'})
            muestra = Project._sgi_dev_stage(STAGE_AFTER_CHANGE)
            if (muestra and project.sgi_dev_stage_key not in ('cerrado_sin_producto', 'liberado')
                    and project.sgi_dev_stage_seq > Project._sgi_dev_stage_order(STAGE_AFTER_CHANGE)):
                project.write({'stage_id': muestra.id})
            req.write({'state': 'firmada', 'approved_by_id': self.env.uid, 'approved_at': fields.Datetime.now()})
            req.write({'attachment_id': req._emit_pdf().id})
            project.message_post(body="Solicitud de modificación firmada por %s: %s (revisión %d; fases: %s)." % (
                self.env.user.name, (req.description or '').strip()[:200], req.revision,
                ", ".join(req.stage_ids.mapped('name')) or '—'), attachment_ids=req.attachment_id.ids)
        return True

    def action_apply(self):
        for req in self:
            if req.state != 'firmada':
                raise UserError("Solo una solicitud firmada se marca aplicada.")
            req.write({'state': 'aplicada', 'applied_by_id': self.env.uid, 'applied_at': fields.Datetime.now()})
            req.project_id.message_post(body="Cambios de %s aplicados." % req.name)
        return True

    def action_back_to_draft(self):
        for req in self:
            if req.state != 'firmada':
                raise UserError("Solo una solicitud firmada vuelve a borrador.")
            req.write({'state': 'borrador', 'approved_by_id': False, 'approved_at': False})
        return True

    def action_print(self):
        self.ensure_one()
        return self.env.ref('quimibond_sgi.action_report_dev_change_request').report_action(self)


class ProjectProjectDevChange(models.Model):
    _inherit = 'project.project'

    sgi_dev_change_request_ids = fields.One2many('sgi.dev.change.request', 'project_id', string="Solicitudes de modificación")
    sgi_dev_change_request_count = fields.Integer(compute='_compute_sgi_dev_change_request_count')

    @api.depends('sgi_dev_change_request_ids')
    def _compute_sgi_dev_change_request_count(self):
        for project in self:
            project.sgi_dev_change_request_count = len(project.sgi_dev_change_request_ids)

    def action_sgi_dev_new_change_request(self):
        self.ensure_one()
        return {'type': 'ir.actions.act_window', 'res_model': 'sgi.dev.change.request', 'view_mode': 'form',
                'target': 'current', 'context': {'default_project_id': self.id}}

    def action_sgi_dev_change_requests(self):
        self.ensure_one()
        action = self.env['ir.actions.act_window']._for_xml_id('quimibond_sgi.sgi_dev_change_request_action')
        action['domain'] = [('project_id', '=', self.id)]
        action['context'] = {'default_project_id': self.id, 'search_default_project_id': self.id}
        return action


class SgiDevShipmentChange(models.Model):
    _inherit = 'sgi.dev.shipment'

    def action_register_response(self):
        """La respuesta «pide cambios» deja una solicitud de modificación en borrador con la revisión
        que acaba de abrir, para que Diseño de Producto la complete y Dirección la firme."""
        res = super().action_register_response()
        Request = self.env['sgi.dev.change.request']
        for ship in self.filtered(lambda s: s.response == 'cambios' and s.project_id):
            if Request.search_count([('shipment_id', '=', ship.id)]):
                continue
            Request.create({
                'project_id': ship.project_id.id, 'origin': 'cliente', 'shipment_id': ship.id,
                'revision': ship.project_id.sgi_dev_revision, 'description': ship.response_note or '',
                'date': ship.response_date or fields.Date.context_today(self),
            })
        return res
