# -*- coding: utf-8 -*-
"""Reporte de conformidad (CoA) impreso desde la tabla (57.130.0; brief §5.1 y anexo; Jose 2026-10-08, 3.4).

El Reporte de conformidad F-P-C07-01 deja de armarse fuera de Odoo: es un
registro ``sgi.dev.coa`` por lote (o por envío si el rollo no lleva lote) que
toma de la tabla del proyecto los renglones marcados **«En certificado»** y
los imprime **contra la especificación del cliente**; el control interno
nunca sale (brief §5.1). Cada renglón del certificado guarda el valor
obtenido en ese lote (precargado con el promedio de la corrida; en los lotes
de pilotaje, bloque 3.6, lo captura el laboratorio) y si cumple la
especificación del cliente.

«Emitir» sella quién y cuándo, genera el PDF y lo deja: en el certificado,
en el envío de la muestra (viaja en el correo al cliente, bloque 3.3) y, si
el envío tiene baja de almacén, como CoA de esa salida (el circuito de CoA
de las entregas, ``sgi_coa.py``). El pie lleva la clave del formato del lote
(``format_map_stock_lot``, F-P-C07-01).
"""
import base64

from odoo import api, fields, models
from odoo.exceptions import UserError

COA_RESULTS = [('cumple', "Cumple"), ('no_conforme', "No conforme")]


class SgiDevCoa(models.Model):
    _name = 'sgi.dev.coa'
    _description = "Reporte de conformidad (CoA) del desarrollo"
    _inherit = ['mail.thread']
    _order = 'id desc'

    name = fields.Char(string="Certificado", compute='_compute_name', store=True)
    project_id = fields.Many2one('project.project', string="Desarrollo", required=True, ondelete='cascade', index=True,
                                 domain="[('sgi_is_ft', '=', True), ('is_template', '=', False)]",
                                 help="Proyecto cuya tabla de características alimenta el certificado.")
    partner_id = fields.Many2one(related='project_id.partner_id', string="Cliente")
    company_id = fields.Many2one('res.company', default=lambda self: self.env.company, required=True)
    shipment_id = fields.Many2one('sgi.dev.shipment', string="Envío de muestra", ondelete='set null', index=True,
                                  domain="[('project_id', '=', project_id)]",
                                  help="Envío al que acompaña el certificado; el PDF se adjunta ahí al emitir.")
    production_id = fields.Many2one('mrp.production', string="Orden de muestra", ondelete='set null',
                                    domain="[('sgi_dev_project_id', '=', project_id)]")
    lot_id = fields.Many2one('stock.lot', string="Lote", ondelete='set null', index=True,
                             domain="[('product_id', '=', product_id)]",
                             help="Lote certificado. Vacío si los rollos del envío no llevan lote.")
    product_id = fields.Many2one('product.product', string="Artículo", compute='_compute_product_id', store=True,
                                 readonly=False)
    date = fields.Date(string="Fecha", default=fields.Date.context_today, required=True)
    quantity_m = fields.Float(string="Metros certificados", digits=(16, 2),
                              help="Metros del lote que ampara el certificado (de los rollos del envío).")
    roll_count = fields.Integer(string="Rollos")
    state = fields.Selection([('borrador', "Borrador"), ('emitido', "Emitido")], string="Estado", default='borrador',
                             required=True, tracking=True, copy=False)
    line_ids = fields.One2many('sgi.dev.coa.line', 'coa_id', string="Renglones")
    # 57.131.1: almacenados, para filtrar y ordenar por ellos en la lista (el
    # filtro «Con no conformes» tumbó el build de main, 2026-10-08 07:17 UTC).
    line_count = fields.Integer(compute='_compute_counts', store=True)
    pending_count = fields.Integer(string="Sin resultado", compute='_compute_counts', store=True,
                                   help="Renglones del certificado sin valor obtenido.")
    nonconforming_count = fields.Integer(string="No conformes", compute='_compute_counts', store=True)
    issued_by_id = fields.Many2one('res.users', string="Emitió (Calidad)", readonly=True, copy=False)
    issued_at = fields.Datetime(string="Emitido el", readonly=True, copy=False)
    attachment_id = fields.Many2one('ir.attachment', string="PDF emitido", readonly=True, copy=False)

    @api.depends('lot_id.name', 'project_id.sgi_ft_folio', 'project_id.name', 'product_id.default_code')
    def _compute_name(self):
        for coa in self:
            coa.name = "CoA %s · %s" % (coa.lot_id.name or coa.product_id.default_code or coa.project_id.name or '',
                                        coa.project_id.sgi_ft_folio or coa.project_id.name or '')

    @api.depends('lot_id', 'production_id', 'shipment_id', 'project_id.sgi_dev_product_id')
    def _compute_product_id(self):
        for coa in self:
            coa.product_id = (coa.lot_id.product_id or coa.production_id.product_id or coa.shipment_id.product_id
                              or coa.project_id.sgi_dev_product_id)

    @api.depends('line_ids', 'line_ids.has_value', 'line_ids.result')
    def _compute_counts(self):
        for coa in self:
            coa.line_count = len(coa.line_ids)
            coa.pending_count = len(coa.line_ids.filtered(lambda l: not l.has_value))
            coa.nonconforming_count = len(coa.line_ids.filtered(lambda l: l.result == 'no_conforme'))

    @api.model_create_multi
    def create(self, vals_list):
        coas = super().create(vals_list)
        for coa in coas:
            if not coa.line_ids and not self.env.context.get('sgi_dev_coa_no_lines'):
                coa.action_load_lines()
        return coas

    def unlink(self):
        if any(c.state == 'emitido' for c in self):
            raise UserError("Un certificado emitido no se borra.")
        return super().unlink()

    # ------------------------------------------------------------------------
    # Renglones desde la tabla del proyecto
    # ------------------------------------------------------------------------
    def action_load_lines(self):
        """Carga los renglones «En certificado» de la tabla que aún no están, con el valor de la corrida."""
        Line = self.env['sgi.dev.coa.line']
        for coa in self:
            if coa.state != 'borrador':
                raise UserError("El certificado ya está emitido.")
            present = coa.line_ids.mapped('characteristic_id')
            for seq, char in enumerate(coa.project_id.sgi_dev_line_ids.filtered('in_coa').sorted('sequence'), start=1):
                if char in present:
                    continue
                vals = {'coa_id': coa.id, 'sequence': seq * 10, 'characteristic_id': char.id}
                if char.kind == 'num' and char.run_count:
                    vals['value'] = char.run_avg
                elif char.kind != 'num' and char.run_text:
                    vals['text'] = char.run_text
                Line.create(vals)
        return True

    # ------------------------------------------------------------------------
    # Emisión
    # ------------------------------------------------------------------------
    def _check_emit(self):
        for coa in self:
            if coa.state != 'borrador':
                raise UserError("El certificado ya está emitido.")
            if not coa.line_ids:
                raise UserError("El certificado no tiene renglones: marque «En certificado» en la tabla del "
                                "proyecto y cargue los renglones.")
            pending = coa.line_ids.filtered(lambda l: not l.has_value)
            if pending:
                raise UserError("Falta el valor obtenido en: %s." % ", ".join(pending.mapped('name')))
        return True

    def _render_pdf(self):
        self.ensure_one()
        report = self.env.ref('quimibond_sgi.action_report_dev_coa')
        pdf, _kind = self.env['ir.actions.report'].sudo()._render_qweb_pdf(report.report_name, res_ids=self.ids)
        return pdf

    def action_emit(self):
        for coa in self:
            coa._check_emit()
            coa.write({'state': 'emitido', 'issued_by_id': self.env.uid, 'issued_at': fields.Datetime.now()})
            pdf = coa._render_pdf()
            fname = "CoA %s %s.pdf" % (coa.product_id.default_code or coa.project_id.sgi_ft_folio or 'desarrollo',
                                       coa.lot_id.name or coa.date)
            att = self.env['ir.attachment'].create({
                'name': fname, 'datas': base64.b64encode(pdf), 'mimetype': 'application/pdf',
                'res_model': coa._name, 'res_id': coa.id})
            coa.write({'attachment_id': att.id})
            coa.message_post(body="Certificado emitido por %s: %d renglón(es), %d no conforme(s)." % (
                self.env.user.name, coa.line_count, coa.nonconforming_count), attachment_ids=att.ids)
            if coa.shipment_id:
                ship_att = att.copy({'res_model': 'sgi.dev.shipment', 'res_id': coa.shipment_id.id})
                coa.shipment_id.write({'attachment_ids': [(4, ship_att.id)]})
                picking = coa.shipment_id.picking_id
                if picking:
                    picking._sgi_coa_register(att.copy({'res_model': 'stock.picking', 'res_id': picking.id}))
            coa.project_id.message_post(body="Reporte de conformidad %s emitido%s." % (
                coa.name, (" (lote %s)" % coa.lot_id.name) if coa.lot_id else ''))
        return True

    def action_print(self):
        self.ensure_one()
        return self.env.ref('quimibond_sgi.action_report_dev_coa').report_action(self)

    def action_open_lines(self):
        self.ensure_one()
        return {'type': 'ir.actions.act_window', 'res_model': 'sgi.dev.coa.line', 'view_mode': 'list',
                'domain': [('coa_id', '=', self.id)], 'target': 'current', 'name': self.name}


class SgiDevCoaLine(models.Model):
    _name = 'sgi.dev.coa.line'
    _description = "Renglón del reporte de conformidad"
    _order = 'coa_id, sequence, id'

    coa_id = fields.Many2one('sgi.dev.coa', string="Certificado", required=True, ondelete='cascade', index=True)
    sequence = fields.Integer(default=10)
    characteristic_id = fields.Many2one('sgi.dev.characteristic', string="Renglón de la tabla", required=True,
                                        ondelete='restrict',
                                        domain="[('project_id', '=', parent.project_id), ('in_coa', '=', True)]")
    name = fields.Char(related='characteristic_id.name', string="Característica")
    name_en = fields.Char(related='characteristic_id.caracteristica_id.name_en', string="Characteristic")
    kind = fields.Selection(related='characteristic_id.kind')
    unit = fields.Char(related='characteristic_id.unit')
    method = fields.Char(related='characteristic_id.method', string="Método / norma")
    direction = fields.Selection(related='characteristic_id.direction')
    position = fields.Selection(related='characteristic_id.position')
    spec_label = fields.Char(related='characteristic_id.spec_label', string="Especificación del cliente")
    value = fields.Float(string="Obtenido", digits=(16, 3), help="Valor obtenido en el lote.")
    text = fields.Char(string="Obtenido (texto)", help="Para características cualitativas.")
    has_value = fields.Boolean(compute='_compute_result', store=True)
    result = fields.Selection(COA_RESULTS, string="Resultado", compute='_compute_result', store=True,
                              help="Contra la especificación del cliente; el control interno no cuenta aquí.")

    @api.depends('value', 'text', 'characteristic_id.kind', 'characteristic_id.spec_nominal',
                 'characteristic_id.spec_limit', 'characteristic_id.spec_tol_minus', 'characteristic_id.spec_tol_plus',
                 'characteristic_id.spec_tol_pct', 'characteristic_id.spec_text', 'characteristic_id.spec_bool')
    def _compute_result(self):
        for line in self:
            char = line.characteristic_id
            if char.kind == 'num':
                line.has_value = bool(line.value)
                if line.value and char._has_spec():
                    line.result = 'cumple' if char._within(line.value, char._limits('spec')) else 'no_conforme'
                else:
                    line.result = False
            else:
                line.has_value = bool(line.text)
                line.result = False

    def _value_label(self):
        self.ensure_one()
        if self.kind == 'num':
            return ('%.3f' % self.value).rstrip('0').rstrip('.') + ((' %s' % self.unit) if self.unit else '')
        return self.text or ''


class ProjectProjectDevCoa(models.Model):
    _inherit = 'project.project'

    sgi_dev_coa_ids = fields.One2many('sgi.dev.coa', 'project_id', string="Reportes de conformidad")
    sgi_dev_coa_count = fields.Integer(compute='_compute_sgi_dev_coa_count')

    @api.depends('sgi_dev_coa_ids')
    def _compute_sgi_dev_coa_count(self):
        for project in self:
            project.sgi_dev_coa_count = len(project.sgi_dev_coa_ids)

    def action_sgi_dev_coas(self):
        self.ensure_one()
        action = self.env['ir.actions.act_window']._for_xml_id('quimibond_sgi.sgi_dev_coa_action')
        action['domain'] = [('project_id', '=', self.id)]
        action['context'] = {'default_project_id': self.id, 'search_default_project_id': self.id}
        return action

    def action_sgi_dev_new_coa(self):
        self.ensure_one()
        if not self.sgi_dev_line_ids.filtered('in_coa'):
            raise UserError("Ningún renglón de la tabla está marcado «En certificado».")
        coa = self.env['sgi.dev.coa'].create({'project_id': self.id})
        return {'type': 'ir.actions.act_window', 'res_model': 'sgi.dev.coa', 'res_id': coa.id,
                'view_mode': 'form', 'target': 'current'}


class SgiDevShipmentCoa(models.Model):
    _inherit = 'sgi.dev.shipment'

    coa_ids = fields.One2many('sgi.dev.coa', 'shipment_id', string="Reportes de conformidad")
    coa_count = fields.Integer(compute='_compute_coa_count')

    @api.depends('coa_ids')
    def _compute_coa_count(self):
        for ship in self:
            ship.coa_count = len(ship.coa_ids)

    def action_sgi_dev_coa(self):
        """Un certificado por lote de los rollos avisados (uno solo si no llevan lote), con los metros
        y rollos de ese lote; abre el que falta por emitir o la lista."""
        self.ensure_one()
        if not self.project_id.sgi_dev_line_ids.filtered('in_coa'):
            raise UserError("Ningún renglón de la tabla del proyecto está marcado «En certificado».")
        Coa = self.env['sgi.dev.coa']
        lots = self.roll_ids.mapped('lot_id')
        groups = [(lot, self.roll_ids.filtered(lambda r: r.lot_id == lot)) for lot in lots] or [(lots, self.roll_ids)]
        if lots and self.roll_ids.filtered(lambda r: not r.lot_id):
            groups.append((self.env['stock.lot'], self.roll_ids.filtered(lambda r: not r.lot_id)))
        created = Coa
        for lot, rolls in groups:
            existing = self.coa_ids.filtered(lambda c: c.lot_id == lot)
            if existing:
                continue
            created |= Coa.create({
                'project_id': self.project_id.id, 'shipment_id': self.id, 'lot_id': lot.id,
                'production_id': self.production_id.id, 'date': self.date_shipped or fields.Date.context_today(self),
                'quantity_m': sum(rolls.mapped('length_m')), 'roll_count': len(rolls)})
        coas = self.coa_ids
        if len(coas) == 1:
            return {'type': 'ir.actions.act_window', 'res_model': 'sgi.dev.coa', 'res_id': coas.id,
                    'view_mode': 'form', 'target': 'current'}
        action = self.env['ir.actions.act_window']._for_xml_id('quimibond_sgi.sgi_dev_coa_action')
        action['domain'] = [('shipment_id', '=', self.id)]
        action['context'] = {'default_project_id': self.project_id.id, 'default_shipment_id': self.id}
        return action
