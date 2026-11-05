# -*- coding: utf-8 -*-
"""Traer renglones de otro presupuesto del mismo año (19.0.3.5.3).

Para armar una revisión con los cinco mercados a partir de documentos que se
capturaron por separado (en 2026, Industrial aprobado y Confección y Especiales
en borrador, de antes de 3.5.0). Copia cada renglón con su mercado, cliente,
producto o proyecto, mes, unidad, cantidad y precio. Un renglón que ya existe
(mismo producto o proyecto, cliente y mes) o que el presupuesto no acepta
(anti-doble-conteo, unidad) no se copia: queda en el resultado y en el chatter.
"""
from markupsafe import Markup

from odoo import fields, models
from odoo.exceptions import UserError, ValidationError

COPY_FIELDS = ('mercado', 'partner_id', 'product_id', 'proyecto_descripcion', 'proyecto_gramaje',
               'proyecto_ancho_cm', 'date', 'uom_id', 'qty_budget', 'price_input', 'price_currency_id',
               'price_basis', 'price_origen')


class QbSalesBudgetCopy(models.TransientModel):
    _name = 'qb.sales.budget.copy'
    _description = "Traer renglones de otro presupuesto"

    budget_id = fields.Many2one('sgi.sales.budget', string="Presupuesto destino", required=True,
                                default=lambda self: self.env.context.get('active_id'))
    year = fields.Integer(related='budget_id.year')
    source_ids = fields.Many2many(
        'sgi.sales.budget', string="Traer de",
        domain="[('kind', '=', 'presupuesto'), ('year', '=', year), ('id', '!=', budget_id)]",
        help="Presupuestos del mismo año (de cualquier estado) cuyos renglones se copian al destino.")
    result = fields.Text(string="Resultado", readonly=True)

    def action_copy(self):
        self.ensure_one()
        if not self.source_ids:
            raise UserError("Elija de qué presupuestos traer los renglones.")
        copied, skipped = self.budget_id._qb_copy_lines_from(self.source_ids)
        self.result = "%d renglón(es) copiado(s).%s" % (
            copied, ("\nNo se copiaron:\n- " + "\n- ".join(skipped)) if skipped else "")
        return {'type': 'ir.actions.act_window', 'res_model': self._name, 'res_id': self.id,
                'view_mode': 'form', 'target': 'new'}


class SgiSalesBudgetCopy(models.Model):
    _inherit = 'sgi.sales.budget'

    def action_qb_open_copy(self):
        self.ensure_one()
        if self.state != 'borrador':
            raise UserError("Los renglones se traen a un presupuesto en borrador.")
        return {'type': 'ir.actions.act_window', 'name': "Traer renglones de otro presupuesto",
                'res_model': 'qb.sales.budget.copy', 'view_mode': 'form', 'target': 'new',
                'context': {'active_id': self.id, 'default_budget_id': self.id}}

    def action_qb_copy_lines_from(self, source_ids):
        """Copia los renglones de los presupuestos ``source_ids`` (ids) a este; para MCP y
        el asistente. Devuelve (copiados, lista de los que no se copiaron)."""
        self.ensure_one()
        return self._qb_copy_lines_from(self.browse(source_ids))

    def _qb_copy_lines_from(self, sources):
        self.ensure_one()
        if self.state != 'borrador':
            raise UserError("Los renglones se traen a un presupuesto en borrador.")
        sources = sources.filtered(lambda b: b != self and b.kind == 'presupuesto')
        wrong_year = sources.filtered(lambda b: b.year != self.year or b.company_id != self.company_id)
        if wrong_year:
            raise UserError("Solo se traen renglones de presupuestos del mismo año y compañía (%s)." % (
                ", ".join(wrong_year.mapped('name'))))
        Line = self.env['sgi.sales.budget.line']
        existing = {(l.product_id.id, l.partner_id.id, l.date, l.proyecto_descripcion or False)
                    for l in self.line_ids}
        copied, skipped = 0, []
        for line in sources.line_ids.sorted(lambda l: (l.mercado or '', l.date, l.id)):
            key = (line.product_id.id, line.partner_id.id, line.date, line.proyecto_descripcion or False)
            where = "%s · %s · %s · %s" % (line.budget_id.folio or line.budget_id.name, line._qb_label(),
                                          line.partner_id.name or "sin cliente", line.date.strftime('%m/%Y'))
            if key in existing:
                skipped.append("%s: ya existe en el destino." % where)
                continue
            vals = {name: (line[name].id if line._fields[name].type == 'many2one' else line[name])
                    for name in COPY_FIELDS}
            vals['budget_id'] = self.id
            try:
                with self.env.cr.savepoint():
                    Line.create(vals)
            except (ValidationError, UserError) as exc:
                skipped.append("%s: %s" % (where, exc.args[0] if exc.args else exc))
                continue
            existing.add(key)
            copied += 1
        body = Markup("Renglones traídos de %s: %d copiado(s), %d no copiado(s).") % (
            ", ".join(sources.mapped(lambda b: b.folio or b.name)), copied, len(skipped))
        if skipped:
            body += Markup("<br/>- ") + Markup("<br/>- ").join(skipped[:50])
            if len(skipped) > 50:
                body += Markup("<br/>…y %d más.") % (len(skipped) - 50)
        self.message_post(body=body)
        return copied, skipped
