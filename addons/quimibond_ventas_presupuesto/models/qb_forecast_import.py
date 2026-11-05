# -*- coding: utf-8 -*-
"""Cargar el forecast de Excel (forecast.xlsx, F-P-A28-13) al pronóstico vigente.

Sirve para la carga inicial y para la transición: mientras un cliente no tenga
lector de release, Jessica puede seguir llenando su hoja y cargarla aquí. Lee
las hojas de cliente con ``release/forecast_xlsx.py`` (Python puro) y escribe
``qb.forecast.line`` con origen «Forecast de Excel».

Reglas:
- solo desde la semana «Desde» (la en curso por omisión): lo pasado no se toca;
- una hoja (y planta) se liga a su cliente en «Hojas del forecast»; la
  primera vez se sugiere por nombre y se anota para la siguiente;
- de los clientes que trae el archivo, las semanas desde «Desde» que el
  archivo ya no trae se quitan, salvo las que escribió un release;
- el producto se busca por su código; si no existe, se reporta y no se carga.
"""
import base64
import io
from collections import defaultdict

from markupsafe import Markup

from odoo import api, fields, models
from odoo.exceptions import UserError

from ..release import forecast_xlsx
from .qb_forecast import monday_of

PROTECTED_ORIGINS = ('release',)


class QbForecastImport(models.TransientModel):
    _name = 'qb.forecast.import'
    _description = "Cargar el forecast de Excel"

    file = fields.Binary(string="Archivo forecast.xlsx", required=True)
    filename = fields.Char(string="Nombre del archivo")
    date_from = fields.Date(string="Desde la semana", required=True,
                            default=lambda self: monday_of(fields.Date.context_today(self)),
                            help="Lo anterior a esta semana no se toca.")
    years = fields.Char(string="Años a leer", required=True,
                        default=lambda self: "%d,%d" % (fields.Date.context_today(self).year,
                                                        fields.Date.context_today(self).year + 1),
                        help="Años de los bloques de cada hoja, separados por coma.")
    replace = fields.Boolean(string="Quitar las semanas que el archivo ya no trae", default=True,
                             help="De los clientes del archivo, desde la semana «Desde». Nunca quita lo que "
                                  "escribió un release.")
    result = fields.Html(string="Resultado", readonly=True, sanitize=False)
    state = fields.Selection([('draft', "Por cargar"), ('done', "Cargado")], default='draft')

    # --- Ligas hoja → cliente ------------------------------------------------
    @api.model
    def _qb_guess_partner(self, sheet, plant):
        """Cliente que sugiere el nombre de la hoja: la empresa cliente cuyo nombre empieza igual
        (TQ-1 → TQ-1 DE MEXICO, SHAWMUT → SHAWMUT LLC). Solo si hay una."""
        Partner = self.env['res.partner']
        words = forecast_xlsx.norm(sheet).split()
        for size in range(len(words), 0, -1):
            prefix = ' '.join(words[:size])
            if len(prefix) < 3:
                break
            found = Partner.search([('is_company', '=', True), ('customer_rank', '>', 0),
                                    ('name', '=ilike', prefix + '%')], limit=2)
            if len(found) == 1:
                return found
        return Partner

    @api.model
    def _qb_sheet_map(self, keys, company):
        """{(hoja, planta): qb.forecast.sheet}; crea las que falten con el cliente sugerido."""
        Sheet = self.env['qb.forecast.sheet']
        existing = Sheet.with_context(active_test=False).search([('company_id', '=', company.id)])
        by_key = {(s.name, s.plant or False): s for s in existing}
        for sheet, plant in keys:
            key = (sheet, plant or False)
            if key in by_key:
                continue
            # La planta hereda el cliente de la hoja si ya está ligada.
            parent = by_key.get((sheet, False))
            partner = parent.partner_id if parent else self._qb_guess_partner(sheet, plant)
            by_key[key] = Sheet.create({'name': sheet, 'plant': plant or False, 'partner_id': partner.id,
                                        'company_id': company.id})
        return by_key

    # --- Carga ---------------------------------------------------------------
    def _qb_parse(self):
        try:
            import openpyxl
        except ImportError as exc:
            raise UserError("Falta openpyxl en el servidor para leer el Excel.") from exc
        try:
            workbook = openpyxl.load_workbook(io.BytesIO(base64.b64decode(self.file)), data_only=True,
                                              read_only=True)
        except Exception as exc:  # noqa: BLE001 - el mensaje va al usuario
            raise UserError("No se pudo abrir el archivo como Excel (.xlsx): %s" % exc) from exc
        try:
            years = {int(y) for y in (self.years or '').replace(' ', '').split(',') if y}
        except ValueError as exc:
            raise UserError("«Años a leer» debe ser una lista como 2026,2027.") from exc
        return forecast_xlsx.parse_workbook(workbook, years=years)

    def action_import(self):
        self.ensure_one()
        company = self.env.company
        Line = self.env['qb.forecast.line']
        parsed = self._qb_parse()
        rows = [r for r in parsed['rows'] if r['monday'] >= self.date_from]
        sheets = self._qb_sheet_map({(r['sheet'], r['plant']) for r in rows}, company)
        codes = {forecast_xlsx.norm(r['code']).replace(' ', '') for r in rows}
        products = {}
        for product in self.env['product.product'].with_context(active_test=False).search(
                [('default_code', 'in', list({r['code'] for r in rows}))]):
            code = forecast_xlsx.norm(product.default_code).replace(' ', '')
            current = products.get(code)
            # Activo antes que archivado; entre dos activos con el mismo código (pasa), el que sí
            # tiene conversión.
            if (not current or (product.active and not current.active)
                    or (product.active == current.active and current.qb_conv_origen == 'sin_dato'
                        and product.qb_conv_origen != 'sin_dato')):
                products[code] = product
        stats = defaultdict(lambda: defaultdict(float))
        problems = defaultdict(set)
        wanted = {}
        # Un cliente con varias hojas (FXI CD JUAREZ y FXI SANTA TERESA; Saltillo y León): cada hoja
        # es una planta. Sin esto las dos plantas se sumarían en un solo renglón (2.0.3).
        sheets_by_partner = defaultdict(set)
        for (sheet_name, _plant), mapping in sheets.items():
            if mapping.partner_id:
                sheets_by_partner[mapping.partner_id.commercial_partner_id.id].add(sheet_name)
        for row in rows:
            sheet_key = (row['sheet'], row['plant'] or False)
            mapping = sheets[sheet_key]
            label = "%s%s" % (row['sheet'], " / %s" % row['plant'] if row['plant'] else "")
            if not mapping.partner_id:
                problems[label].add("sin cliente ligado (Hojas del forecast)")
                stats[label]['omitidas'] += 1
                continue
            code = forecast_xlsx.norm(row['code']).replace(' ', '')
            product = products.get(code)
            if not product:
                problems[label].add("el producto %s no existe en Odoo" % row['code'])
                stats[label]['omitidas'] += 1
                continue
            if not product.active:
                problems[label].add("el producto %s está archivado (se cargó igual)" % row['code'])
            unit = row['unit'] or mapping.default_uom or False
            partner_id = mapping.partner_id.commercial_partner_id.id
            plant = row['plant'] or (row['sheet'] if len(sheets_by_partner[partner_id]) > 1 else False)
            key = (partner_id, mapping.ship_to_id.id or False,
                   (plant or False) if not mapping.ship_to_id else False, product.id, row['monday'])
            vals = wanted.setdefault(key, {
                'company_id': company.id, 'partner_id': key[0], 'ship_to_id': key[1], 'plant': key[2],
                'product_id': product.id, 'week': row['monday'], 'qty_origen': 0.0,
                'uom_origen': unit, 'origen': 'excel', 'delivery_date': row['delivery'],
                'customer_part': row['part'] or False, 'customer_po': row['po'] or False,
                'tag': row['tag'] or False,
            })
            vals['qty_origen'] += row['qty']
            stats[label]['renglones'] += 1
            stats[label]['cantidad'] += row['qty']
        partners = {k[0] for k in wanted}
        existing = Line.search([('company_id', '=', company.id), ('partner_id', 'in', list(partners)),
                                ('week', '>=', self.date_from)]) if partners else Line
        by_key = {(l.partner_id.id, l.ship_to_id.id or False, l.plant or False, l.product_id.id, l.week): l
                  for l in existing}
        to_create, updated, kept = [], 0, 0
        for key, vals in wanted.items():
            line = by_key.pop(key, None)
            if line and line.origen in PROTECTED_ORIGINS:
                kept += 1
                continue
            if line:
                changes = {k: v for k, v in vals.items() if k in (
                    'qty_origen', 'uom_origen', 'origen', 'delivery_date', 'customer_part', 'customer_po',
                    'tag') and line[k] != v}
                if changes:
                    line.write(changes)
                    updated += 1
            else:
                to_create.append(vals)
        Line.create(to_create)
        removed = 0
        if self.replace:
            stale = Line.browse([l.id for l in by_key.values() if l.origen not in PROTECTED_ORIGINS])
            removed = len(stale)
            stale.unlink()
        Line.search([('company_id', '=', company.id), ('week', '>=', self.date_from)]).action_qb_refresh()
        self.env['qb.demand.line'].action_qb_rebuild()
        self.write({'result': self._qb_result(parsed, stats, problems, len(to_create), updated, removed, kept,
                                              len(codes)),
                    'state': 'done'})
        return {'type': 'ir.actions.act_window', 'res_model': self._name, 'res_id': self.id,
                'view_mode': 'form', 'target': 'new'}

    @staticmethod
    def _qb_result(parsed, stats, problems, created, updated, removed, kept, n_codes):
        rows = Markup('').join(
            Markup('<tr><td>%s</td><td class="text-end">%d</td><td class="text-end">%s</td>'
                   '<td class="text-end">%d</td><td>%s</td></tr>') % (
                label, int(data['renglones']), '{:,.0f}'.format(data['cantidad']), int(data['omitidas']),
                "; ".join(sorted(problems.get(label, ()))))
            for label, data in sorted(stats.items()))
        warnings = Markup('').join(Markup('<li>%s</li>') % w for w in parsed['warnings'][:50])
        return Markup(
            '<p>Hojas leídas: %d. Semanas nuevas: %d; cambiadas: %d; quitadas: %d; de release (sin tocar): %d. '
            'Productos distintos: %d.</p>'
            '<table class="table table-sm"><thead><tr><th>Hoja</th><th class="text-end">Semanas</th>'
            '<th class="text-end">Cantidad (unidad del cliente)</th><th class="text-end">Omitidas</th>'
            '<th>Problemas</th></tr></thead><tbody>%s</tbody></table>%s') % (
            len(parsed['sheets']), created, updated, removed, kept, n_codes, rows,
            Markup('<p>Avisos del archivo:</p><ul>%s</ul>') % warnings if warnings else Markup(''))
