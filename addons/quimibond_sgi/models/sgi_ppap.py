# -*- coding: utf-8 -*-
from odoo import models, fields, api
from odoo.exceptions import UserError

# Tabla S/R de AIAG (PPAP 4a edición) por elemento (secuencia 1-18) y nivel:
# las 3 letras son los niveles 1, 2 y 3 (S = presentar al cliente,
# R = retener en planta a disposición). El nivel 4 lo define el cliente
# (se parte del mapa del nivel 3 y se ajusta por elemento) y el nivel 5
# retiene todo (revisión en las instalaciones del proveedor).
AIAG_SUBMISSION_MAP = {
    1: 'RSS', 2: 'RSS', 3: 'RRS', 4: 'RRS', 5: 'RRS', 6: 'RRS',
    7: 'RRS', 8: 'RRS', 9: 'RSS', 10: 'RSS', 11: 'RRS', 12: 'RSS',
    13: 'SSS', 14: 'RSS', 15: 'RRR', 16: 'RRR', 17: 'RRS', 18: 'SSS',
}


class SgiPpapElementTemplate(models.Model):
    _name = 'sgi.ppap.element.template'
    _description = "Elemento PPAP (catálogo AIAG)"
    _order = 'sequence, id'

    sequence = fields.Integer(string="N°", required=True)
    name = fields.Char(string="Elemento", required=True, translate=True)
    is_psw = fields.Boolean(string="Es PSW (elemento 18)")
    active = fields.Boolean(default=True)


class SgiPpap(models.Model):
    _name = 'sgi.ppap'
    _description = "PPAP - Proceso de Aprobación de Partes de Producción (P-C15)"
    _inherit = ['sgi.base.mixin']
    _order = 'folio desc'
    _sgi_sequence_code = 'sgi.ppap'
    _sgi_locked_states = ('aprobado',)

    _folio_uniq = models.Constraint(
        'unique(folio)',
        "Ya existe un PPAP con ese folio.",
    )

    partner_id = fields.Many2one('res.partner', string="Cliente", required=True, tracking=True,
                                 domain="[('is_company', '=', True)]")
    product_tmpl_id = fields.Many2one('product.template', string="Producto", required=True,
                                      tracking=True)
    level = fields.Selection([
        ('1', "Nivel 1"),
        ('2', "Nivel 2"),
        ('3', "Nivel 3"),
        ('4', "Nivel 4"),
        ('5', "Nivel 5"),
    ], string="Nivel", default='3', required=True)
    reason = fields.Selection([
        ('nuevo_producto', "Nuevo producto"),
        ('cambio_ingenieria', "Cambio de ingeniería"),
        ('cambio_proceso', "Cambio de proceso"),
        ('recertificacion', "Recertificación"),
        ('solicitud_cliente', "Solicitud del cliente"),
    ], string="Motivo", default='nuevo_producto', required=True)
    state = fields.Selection([
        ('preparacion', "Preparación"),
        ('enviado', "Enviado"),
        ('aprobado', "Aprobado"),
        ('interino', "Interino"),
        ('rechazado', "Rechazado"),
    ], string="Estado", default='preparacion', required=True, tracking=True)
    date_submitted = fields.Date(string="Fecha de envío", readonly=True)
    date_decision = fields.Date(string="Fecha de decisión", readonly=True)
    element_ids = fields.One2many('sgi.ppap.element', 'ppap_id', string="Elementos")
    notes = fields.Text(string="Notas")

    @api.model_create_multi
    def create(self, vals_list):
        ppaps = super().create(vals_list)
        for ppap in ppaps:
            ppap._sgi_generate_elements()
        return ppaps

    @api.model
    def _sgi_submission_for(self, sequence, level):
        """S/R del elemento `sequence` para el nivel AIAG del expediente."""
        lvl = int(level or 3)
        if lvl == 5:
            return 'retain'
        code = AIAG_SUBMISSION_MAP.get(sequence, 'SSS')[min(lvl, 3) - 1]
        return 'submit' if code == 'S' else 'retain'

    def _sgi_generate_elements(self):
        """Genera (idempotente) los 18 elementos AIAG desde el catálogo."""
        Template = self.env['sgi.ppap.element.template']
        Element = self.env['sgi.ppap.element']
        for ppap in self:
            existing = ppap.element_ids.mapped('template_id')
            for tmpl in Template.search([]):
                if tmpl not in existing:
                    Element.create({
                        'ppap_id': ppap.id,
                        'template_id': tmpl.id,
                        'sequence': tmpl.sequence,
                        'name': tmpl.name,
                        'state': 'pendiente',
                        'submission': self._sgi_submission_for(
                            tmpl.sequence, ppap.level),
                    })
        return True

    def write(self, vals):
        res = super().write(vals)
        # Cambiar el nivel en preparación re-aplica la tabla AIAG a los
        # elementos del catálogo (los ajustes manuales se rehacen después,
        # elemento por elemento, si el cliente pide algo distinto).
        if 'level' in vals:
            for ppap in self.filtered(lambda p: p.state == 'preparacion'):
                for element in ppap.element_ids.filtered('template_id'):
                    element.submission = self._sgi_submission_for(
                        element.template_id.sequence, ppap.level)
        return res

    def action_mark_enviado(self):
        for ppap in self:
            pending = ppap.element_ids.filtered(
                lambda e: e.state == 'pendiente' and e.submission == 'submit')
            if pending:
                raise UserError(
                    "No se puede marcar como Enviado el PPAP %s: hay %d elemento(s) "
                    "A PRESENTAR (S) en estado Pendiente para el nivel %s.\n\n"
                    "Los elementos a retener (R) no bloquean el envío, pero deben "
                    "quedar disponibles en planta." % (
                        ppap.folio, len(pending), ppap.level))
            ppap.write({
                'state': 'enviado',
                'date_submitted': fields.Date.context_today(ppap),
            })
        return True

    def action_approve(self):
        for ppap in self:
            psw = ppap.element_ids.filtered(lambda e: e.template_id.is_psw)
            if not psw or any(e.state not in ('listo', 'aprobado') for e in psw):
                raise UserError(
                    "No se puede aprobar el PPAP %s: el elemento 18 (PSW / Garantía de "
                    "presentación de la parte) debe estar en Listo o Aprobado." % ppap.folio)
            ppap.write({
                'state': 'aprobado',
                'date_decision': fields.Date.context_today(ppap),
            })
        return True

    def action_set_interino(self):
        self.write({'state': 'interino', 'date_decision': fields.Date.context_today(self)})
        return True

    def action_reject(self):
        self.write({'state': 'rechazado', 'date_decision': fields.Date.context_today(self)})
        return True

    def action_reset(self):
        # Limpia las fechas del ciclo anterior: un PPAP regresado a preparación
        # no debe conservar el sello de un envío/decisión que ya no aplica.
        self.write({'state': 'preparacion',
                    'date_submitted': False, 'date_decision': False})
        return True

    @api.depends('folio', 'product_tmpl_id')
    def _compute_display_name(self):
        for ppap in self:
            ppap.display_name = "%s - %s" % (
                ppap.folio, ppap.product_tmpl_id.name or '') if ppap.folio else (ppap.folio or '')


class ResPartnerPpap(models.Model):
    _inherit = 'res.partner'

    sgi_ppap_count = fields.Integer(string="PPAP", compute='_compute_sgi_ppap_count')

    def _compute_sgi_ppap_count(self):
        data = self.env['sgi.ppap']._read_group(
            [('partner_id', 'in', self.ids)], ['partner_id'], ['__count'])
        mapped = {partner.id: count for partner, count in data}
        for partner in self:
            partner.sgi_ppap_count = mapped.get(partner.id, 0)

    def action_view_sgi_ppap(self):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'name': "PPAP",
            'res_model': 'sgi.ppap',
            'view_mode': 'list,form',
            'domain': [('partner_id', '=', self.id)],
            'context': {'default_partner_id': self.id},
        }


class ProductTemplatePpap(models.Model):
    _inherit = 'product.template'

    sgi_ppap_count = fields.Integer(string="PPAP", compute='_compute_sgi_ppap_count')

    def _compute_sgi_ppap_count(self):
        data = self.env['sgi.ppap']._read_group(
            [('product_tmpl_id', 'in', self.ids)], ['product_tmpl_id'], ['__count'])
        mapped = {tmpl.id: count for tmpl, count in data}
        for tmpl in self:
            tmpl.sgi_ppap_count = mapped.get(tmpl.id, 0)

    def action_view_sgi_ppap(self):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'name': "PPAP",
            'res_model': 'sgi.ppap',
            'view_mode': 'list,form',
            'domain': [('product_tmpl_id', '=', self.id)],
            'context': {'default_product_tmpl_id': self.id},
        }


class ProductProductPpap(models.Model):
    """El formulario de variante hereda la vista de la plantilla, así que el
    smart button de PPAP también debe resolver en product.product (sin esto,
    Odoo invalida las vistas de variante que lo incluyen)."""
    _inherit = 'product.product'

    sgi_ppap_count = fields.Integer(string="PPAP", compute='_compute_sgi_ppap_count')

    def _compute_sgi_ppap_count(self):
        for product in self:
            product.sgi_ppap_count = product.product_tmpl_id.sgi_ppap_count

    def action_view_sgi_ppap(self):
        self.ensure_one()
        return self.product_tmpl_id.action_view_sgi_ppap()


class SgiPpapElement(models.Model):
    _name = 'sgi.ppap.element'
    _description = "Elemento de un PPAP"
    _order = 'ppap_id, sequence, id'

    ppap_id = fields.Many2one('sgi.ppap', string="PPAP", required=True, ondelete='cascade')
    template_id = fields.Many2one('sgi.ppap.element.template', string="Elemento (catálogo)")
    sequence = fields.Integer(string="N°", default=10)
    name = fields.Char(string="Elemento", required=True)
    state = fields.Selection([
        ('na', "N/A"),
        ('pendiente', "Pendiente"),
        ('listo', "Listo"),
        ('aprobado', "Aprobado"),
    ], string="Estado", default='pendiente', required=True)
    submission = fields.Selection([
        ('submit', "Presentar (S)"),
        ('retain', "Retener (R)"),
    ], string="S/R", default='submit', required=True,
        help="Según el nivel AIAG del expediente: S se presenta al cliente y "
             "bloquea el envío si está pendiente; R se retiene en planta a "
             "disposición. Editable por elemento (p. ej. nivel 4).")
    fmea_id = fields.Many2one('sgi.fmea', string="AMEF")
    control_plan_id = fields.Many2one('sgi.control.plan', string="Plan de control")
    document_id = fields.Many2one('documents.document', string="Documento")
    notes = fields.Text(string="Notas")

    def unlink(self):
        # Un PPAP aprobado es evidencia presentada al cliente: sus elementos no
        # se borran (salvo MAST). En preparación se editan libremente.
        if not self.env.su and not self.env.user.has_group(
                'quimibond_sgi.group_sgi_manager'):
            locked = self.filtered(lambda e: e.ppap_id.state == 'aprobado')
            if locked:
                raise UserError(
                    "No se puede borrar un elemento de un PPAP aprobado (es "
                    "evidencia presentada al cliente). Pide al Jefe de MAST "
                    "reabrirlo.\n\nPPAP: %s" % ", ".join(
                        locked.mapped('ppap_id.display_name')))
        return super().unlink()
