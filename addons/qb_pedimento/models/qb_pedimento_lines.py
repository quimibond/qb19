# -*- coding: utf-8 -*-
"""Renglones del expediente: documentos del checklist y partidas."""
from odoo import api, fields, models


class QbPedimentoDocumento(models.Model):
    _name = 'qb.pedimento.documento'
    _description = 'Documento del expediente de pedimento'
    _order = 'pedimento_id, sequence, id'

    pedimento_id = fields.Many2one('qb.pedimento', required=True, ondelete='cascade', index=True)
    tipo_id = fields.Many2one('qb.pedimento.documento.tipo', 'Documento', required=True, ondelete='restrict')
    sequence = fields.Integer(related='tipo_id.sequence', store=True)
    required = fields.Boolean(related='tipo_id.required', store=True, string='Obligatorio')
    attachment_ids = fields.Many2many(
        'ir.attachment', 'qb_pedimento_documento_attachment_rel', 'documento_id', 'attachment_id', 'Archivos')
    no_aplica = fields.Boolean('No aplica')
    referencia = fields.Char('Folio / nota', help='Folio del documento (COVE, DODA, Entry) o comentario.')
    cumplido = fields.Boolean(compute='_compute_cumplido', store=True)

    _tipo_uniq = models.Constraint('unique(pedimento_id, tipo_id)', 'Ese documento ya está en el expediente.')

    @api.depends('attachment_ids', 'no_aplica')
    def _compute_cumplido(self):
        for rec in self:
            rec.cumplido = bool(rec.attachment_ids) or rec.no_aplica


class QbPedimentoPartida(models.Model):
    _name = 'qb.pedimento.partida'
    _description = 'Partida de pedimento'
    _order = 'pedimento_id, sequence, id'

    sequence = fields.Integer(default=10)
    pedimento_id = fields.Many2one('qb.pedimento', required=True, ondelete='cascade', index=True)
    company_id = fields.Many2one(related='pedimento_id.company_id', store=True)
    currency_id = fields.Many2one(related='pedimento_id.currency_id')
    operacion = fields.Selection(related='pedimento_id.operacion', store=True)
    fecha_pago = fields.Date(related='pedimento_id.fecha_pago', store=True)
    partner_id = fields.Many2one(related='pedimento_id.partner_id', store=True)
    product_id = fields.Many2one('product.product', 'Producto')
    descripcion = fields.Char('Descripción')
    fraccion = fields.Char(
        'Fracción arancelaria', compute='_compute_fraccion', store=True, readonly=False, index=True,
        help='8 dígitos de la TIGIE (10 con NICO). Por omisión, la del producto.')
    cantidad = fields.Float('Cantidad (UMC)', digits='Product Unit')
    uom_id = fields.Many2one('uom.uom', 'Unidad')
    valor_aduana = fields.Monetary('Valor en aduana')
    tasa_igi = fields.Float('Tasa IGI (%)', digits=(5, 2))
    igi = fields.Monetary('IGI', compute='_compute_igi', store=True, readonly=False)

    @api.depends('product_id')
    def _compute_fraccion(self):
        for rec in self:
            fraccion = rec.product_id.product_tmpl_id._fields.get('l10n_mx_edi_tariff_fraction_id') \
                and rec.product_id.l10n_mx_edi_tariff_fraction_id
            if fraccion and not rec.fraccion:
                rec.fraccion = fraccion.code
            elif not rec.fraccion:
                rec.fraccion = False

    @api.depends('valor_aduana', 'tasa_igi')
    def _compute_igi(self):
        for rec in self:
            rec.igi = rec.currency_id.round(rec.valor_aduana * rec.tasa_igi / 100.0) if rec.currency_id else 0.0

    @api.onchange('product_id')
    def _onchange_product_id(self):
        if self.product_id:
            self.descripcion = self.descripcion or self.product_id.display_name
            self.uom_id = self.uom_id or self.product_id.uom_id
