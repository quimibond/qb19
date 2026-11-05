# -*- coding: utf-8 -*-
"""«Pegar semanas desde Excel» (diseño v3, sección 2.3; 19.0.3.1.0).

Para los clientes que no tienen lector de release: Jessica copia en Excel el
bloque de la hoja del cliente (código, parte y semanas) y lo pega aquí. El
texto lo lee ``release/forecast_paste.py`` (Python puro) y este asistente lo
escribe en ``qb.forecast.line`` del cliente y la planta que se eligen.

Reglas (las mismas de la carga del forecast de Excel):
- lo anterior a la semana en curso no se toca;
- lo que escribió un release no se pisa (se reporta);
- una semana vacía del bloque quita lo que había en esa semana para ese
  producto, si «Las semanas vacías quitan lo que había» está prendido;
- el producto se busca por su código y, si no, por la parte del cliente
  confirmada; si no se encuentra, se reporta y no se carga.
"""
from collections import defaultdict
from datetime import date

from markupsafe import Markup

from odoo import api, fields, models
from odoo.exceptions import UserError

from ..release import forecast_paste
from .qb_forecast import UNITS, monday_of

PROTECTED_ORIGINS = ('release',)


class QbForecastPaste(models.TransientModel):
    _name = 'qb.forecast.paste'
    _description = "Pegar semanas del pronóstico desde Excel"

    partner_id = fields.Many2one('res.partner', string="Cliente", required=True,
                                 domain="[('is_company', '=', True)]")
    ship_to_id = fields.Many2one('res.partner', string="Dirección de entrega",
                                 domain="[('commercial_partner_id', '=', partner_id)]")
    plant = fields.Char(string="Planta", help="Como la escribe el forecast (Silao, Tennessee). Déjela vacía si el "
                                             "cliente tiene una sola planta.")
    uom_origen = fields.Selection(UNITS, string="Unidad del bloque",
                                  help="Vacía = la unidad de venta de cada producto.")
    year = fields.Integer(string="Año de la primera semana", required=True,
                          default=lambda self: fields.Date.context_today(self).isocalendar()[0])
    first_week = fields.Integer(string="Primera semana del bloque",
                                default=lambda self: fields.Date.context_today(self).isocalendar()[1],
                                help="Solo si el bloque no trae el renglón con los números de semana.")
    text = fields.Text(string="Bloque copiado de Excel", required=True,
                       help="En Excel seleccione el código, la parte y las semanas (con el renglón de "
                            "los números de semana si puede), copie con Ctrl+C y pegue aquí con Ctrl+V.")
    replace_blanks = fields.Boolean(string="Las semanas vacías quitan lo que había", default=True,
                                    help="Dentro de las semanas del bloque y de los productos pegados.")
    result = fields.Html(string="Resultado", readonly=True, sanitize=False)
    state = fields.Selection([('draft', "Por pegar"), ('done', "Pegado")], default='draft')

    @api.onchange('partner_id')
    def _onchange_partner_id(self):
        """Unidad por omisión: la de la hoja del forecast del cliente, si está ligada."""
        if self.partner_id and not self.uom_origen:
            sheet = self.env['qb.forecast.sheet'].search(
                [('partner_id', '=', self.partner_id.commercial_partner_id.id), ('default_uom', '!=', False)],
                limit=1)
            self.uom_origen = sheet.default_uom or False

    # --- Productos -------------------------------------------------------------
    def _qb_products(self, rows, company):
        """{código normalizado: producto} y {parte del cliente: producto} para los ids pegados."""
        ids = {forecast_paste.norm_code(i) for row in rows for i in row['ids']}
        by_code = {}
        for product in self.env['product.product'].search([
                ('default_code', '!=', False), ('company_id', 'in', (False, company.id))]):
            code = forecast_paste.norm_code(product.default_code)
            if code in ids:
                current = by_code.get(code)
                if not current or (current.qb_conv_origen == 'sin_dato' and product.qb_conv_origen != 'sin_dato'):
                    by_code[code] = product
        parts = self.env['qb.customer.part'].search([
            ('partner_id', '=', self.partner_id.commercial_partner_id.id), ('state', '=', 'confirmado'),
            ('product_id', '!=', False)])
        by_part = {forecast_paste.norm_code(p.customer_part): p for p in parts}
        return by_code, by_part

    @staticmethod
    def _qb_monday(year, offset, week):
        try:
            return date.fromisocalendar(year + offset, week, 1)
        except ValueError:
            return None

    # --- Pegar -----------------------------------------------------------------
    def action_apply(self):
        self.ensure_one()
        company = self.env.company
        Line = self.env['qb.forecast.line']
        parsed = forecast_paste.parse_paste(self.text, self.first_week)
        if not parsed['rows']:
            raise UserError("\n".join(parsed['warnings']) or "El bloque no trae renglones.")
        partner = self.partner_id.commercial_partner_id
        ship_to = self.ship_to_id
        plant = (self.plant or '').strip() or False
        if ship_to:
            plant = False
        current = monday_of(fields.Date.context_today(self))
        by_code, by_part = self._qb_products(parsed['rows'], company)
        warnings = list(parsed['warnings'])
        wanted = {}
        skipped_past = 0
        for row in parsed['rows']:
            product = part_record = None
            part_text = False
            for ident in row['ids']:
                code = forecast_paste.norm_code(ident)
                if not product and code in by_code:
                    product = by_code[code]
                elif code in by_part:
                    part_record = by_part[code]
                    part_text = ident
                    product = product or part_record.product_id
                elif product and not part_text:
                    part_text = ident
            if not product:
                warnings.append("Renglón %d: «%s» no es un producto de Odoo ni una parte confirmada del "
                                "cliente; no se cargó." % (row['line'], row['ids'][0]))
                continue
            for (offset, week), qty in row['qty'].items():
                monday = self._qb_monday(self.year, offset, week)
                if not monday:
                    warnings.append("La semana %d de %d no existe." % (week, self.year + offset))
                    continue
                if monday < current:
                    skipped_past += 1
                    continue
                key = (product.id, monday)
                vals = wanted.setdefault(key, {'qty': None, 'part': part_text, 'part_id': part_record})
                if qty is not None:
                    vals['qty'] = (vals['qty'] or 0.0) + qty
        if not wanted:
            raise UserError("Nada que pegar desde la semana en curso.\n" + "\n".join(warnings))
        products = list({k[0] for k in wanted})
        weeks = [k[1] for k in wanted]
        existing = Line.search([
            ('company_id', '=', company.id), ('partner_id', '=', partner.id),
            ('ship_to_id', '=', ship_to.id or False), ('plant', '=', plant),
            ('product_id', 'in', products), ('week', '>=', min(weeks)), ('week', '<=', max(weeks))])
        by_key = defaultdict(lambda: Line)
        for line in existing:
            by_key[(line.product_id.id, line.week)] |= line
        stats = defaultdict(int)
        to_create = []
        for (product_id, monday), vals in wanted.items():
            lines = by_key.get((product_id, monday), Line)
            if lines.filtered(lambda l: l.origen in PROTECTED_ORIGINS):
                stats['release'] += 1
                continue
            qty = vals['qty']
            if not qty:
                if lines and (qty == 0 or self.replace_blanks):
                    stats['quitadas'] += len(lines)
                    lines.unlink()
                continue
            line_vals = {'qty_origen': qty, 'uom_origen': self.uom_origen or False, 'origen': 'excel'}
            if vals['part']:
                line_vals['customer_part'] = vals['part']
            if vals['part_id']:
                line_vals['customer_part_id'] = vals['part_id'].id
            if lines:
                first, extra = lines[0], lines[1:]
                changes = {k: v for k, v in line_vals.items() if first[k] != v}
                if changes:
                    first.write(changes)
                    stats['cambiadas'] += 1
                if extra:
                    extra.unlink()
            else:
                line_vals.update(company_id=company.id, partner_id=partner.id, ship_to_id=ship_to.id or False,
                                 plant=plant, product_id=product_id, week=monday)
                to_create.append(line_vals)
        created = Line.create(to_create)
        stats['nuevas'] = len(created)
        Line.search([('company_id', '=', company.id), ('partner_id', '=', partner.id),
                     ('week', '>=', min(weeks)), ('week', '<=', max(weeks))]).action_qb_refresh()
        self.env['qb.demand.line'].action_qb_rebuild()
        self.write({'result': self._qb_result(stats, skipped_past, warnings), 'state': 'done'})
        return {'type': 'ir.actions.act_window', 'res_model': self._name, 'res_id': self.id,
                'view_mode': 'form', 'target': 'new'}

    @staticmethod
    def _qb_result(stats, skipped_past, warnings):
        items = Markup('').join(Markup('<li>%s</li>') % w for w in warnings[:50])
        return Markup(
            '<p>Semanas nuevas: %d; cambiadas: %d; quitadas: %d; de release (sin tocar): %d; '
            'anteriores a esta semana (sin tocar): %d.</p>%s') % (
            stats['nuevas'], stats['cambiadas'], stats['quitadas'], stats['release'], skipped_past,
            Markup('<p>Avisos:</p><ul>%s</ul>') % items if items else Markup(''))
