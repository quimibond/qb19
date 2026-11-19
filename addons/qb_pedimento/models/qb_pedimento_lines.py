# -*- coding: utf-8 -*-
"""Renglones del expediente: documentos del checklist y partidas."""
from odoo import _, api, fields, models
from odoo.exceptions import ValidationError


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
    pide_vigencia = fields.Boolean(related='tipo_id.pide_vigencia')
    fecha_vigencia = fields.Date(
        'Vigente hasta', help='Fin de la vigencia del documento (encargo conferido, certificación de origen). '
                              'Si venció antes del pago del pedimento, el documento no cuenta.')
    vencido = fields.Boolean(
        compute='_compute_cumplido', store=True,
        help='El documento tiene vigencia y venció antes de la fecha del pedimento (pago; si no hay, arribo; '
             'si no, alta del expediente).')
    cumplido = fields.Boolean(compute='_compute_cumplido', store=True)

    _tipo_uniq = models.Constraint('unique(pedimento_id, tipo_id)', 'Ese documento ya está en el expediente.')

    @api.depends('attachment_ids', 'no_aplica', 'fecha_vigencia', 'tipo_id.pide_vigencia',
                 'pedimento_id.fecha_pago', 'pedimento_id.fecha_arribo')
    def _compute_cumplido(self):
        for rec in self:
            ped = rec.pedimento_id
            fecha_ref = ped.fecha_pago or ped.fecha_arribo or (ped.create_date and ped.create_date.date())
            rec.vencido = bool(rec.tipo_id.pide_vigencia and rec.fecha_vigencia and fecha_ref
                               and rec.fecha_vigencia < fecha_ref)
            con_archivo = bool(rec.attachment_ids) and not rec.vencido and (
                not rec.tipo_id.pide_vigencia or bool(rec.fecha_vigencia))
            rec.cumplido = rec.no_aplica or con_archivo

    @api.constrains('no_aplica', 'referencia')
    def _check_no_aplica_con_motivo(self):
        for rec in self:
            if rec.no_aplica and rec.required and not (rec.referencia or '').strip():
                raise ValidationError(_(
                    '«%(doc)s» es obligatorio: para marcarlo «No aplica» escriba el motivo en «Folio / nota».',
                    doc=rec.tipo_id.name))


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
