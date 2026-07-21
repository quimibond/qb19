# -*- coding: utf-8 -*-
"""Asistente de importación del F-P-A28-18 desde el Excel real.

Lee la matriz producto × meses (pares "<mes> m" / "<mes> $") con openpyxl y crea
las líneas del presupuesto. Tolerante a mayúsculas/acentos/espacios en los
encabezados. Todo-o-nada por hoja para errores ESTRUCTURALES; los productos sin
match NO son error: se reportan y la importación sigue.

Convive con la importación plana estándar (base_import) de la lista de líneas.
"""
import base64
import io
import unicodedata
from datetime import date

from odoo import models, fields, api
from odoo.exceptions import UserError, ValidationError

_MONTHS = {
    'enero': 1, 'febrero': 2, 'marzo': 3, 'abril': 4, 'mayo': 5, 'junio': 6,
    'julio': 7, 'agosto': 8, 'septiembre': 9, 'setiembre': 9, 'octubre': 10,
    'noviembre': 11, 'diciembre': 12,
}
_QTY_TOKENS = {'m', 'cant', 'cantidad', 'unidades', 'cantidades'}
_AMOUNT_TOKENS = {'$', 'importe', 'pesos', 'importes', 'mxn'}


def _norm(value):
    """Normaliza un encabezado: sin acentos, minúsculas, sin espacios extra."""
    if value is None:
        return ''
    text = str(value).strip().lower()
    return ''.join(c for c in unicodedata.normalize('NFD', text)
                   if unicodedata.category(c) != 'Mn')


class SgiSalesBudgetImport(models.TransientModel):
    _name = 'sgi.sales.budget.import'
    _description = "Importar presupuesto desde Excel (F-P-A28-18)"

    budget_id = fields.Many2one(
        'sgi.sales.budget', string="Presupuesto", required=True,
        default=lambda self: self.env.context.get('active_id'))
    file = fields.Binary(string="Archivo .xlsx", required=True)
    filename = fields.Char(string="Nombre del archivo")
    available_sheets = fields.Char(string="Hojas del archivo", readonly=True)
    sheet_name = fields.Char(
        string="Hoja a importar",
        help="Vacío = la primera hoja del libro.")
    conflict_mode = fields.Selection([
        ('replace', "Reemplazar líneas existentes del producto"),
        ('add', "Sumar a las existentes"),
    ], string="Si el producto ya tiene líneas", default='replace', required=True)
    result = fields.Text(string="Resultado", readonly=True)

    @api.onchange('file')
    def _onchange_file(self):
        self.available_sheets = False
        if not self.file:
            return
        try:
            wb = self._load_workbook()
        except UserError:
            self.available_sheets = "No se pudo leer el archivo (¿es .xlsx?)."
            return
        self.available_sheets = ", ".join(wb.sheetnames)
        if not self.sheet_name and wb.sheetnames:
            self.sheet_name = wb.sheetnames[0]

    def _load_workbook(self):
        try:
            import openpyxl
        except ImportError:
            raise UserError("openpyxl no está disponible en este servidor.")
        try:
            # read_only=False: se necesita acceso aleatorio ws.cell(fila, col).
            return openpyxl.load_workbook(
                io.BytesIO(base64.b64decode(self.file)), data_only=True)
        except Exception as exc:
            raise UserError("No se pudo abrir el Excel: %s" % exc)

    # --- Parseo del layout ---------------------------------------------------
    def _parse_header(self, ws):
        """Ubica la fila de encabezados (busca 'PRODUCTO') y mapea columnas."""
        header_row = prod_col = None
        max_r = min(ws.max_row or 0, 30)
        max_c = min(ws.max_column or 0, 80)
        for r in range(1, max_r + 1):
            for c in range(1, max_c + 1):
                if 'producto' in _norm(ws.cell(r, c).value):
                    header_row, prod_col = r, c
                    break
            if header_row:
                break
        if not header_row:
            raise UserError(
                "No se encontró la fila de encabezados (debe haber una celda "
                "'PRODUCTO'). Revisa la hoja seleccionada.")
        unit_col = client_col = None
        month_cols = {}
        for c in range(1, (ws.max_column or 0) + 1):
            if c == prod_col:
                continue
            text = _norm(ws.cell(header_row, c).value)
            if not text:
                continue
            if text == 'unidad':
                unit_col = c
                continue
            if text == 'cliente':
                client_col = c
                continue
            tokens = text.replace('$', ' $ ').split()
            month = next((_MONTHS[t] for t in tokens if t in _MONTHS), None)
            if not month:
                continue
            if _AMOUNT_TOKENS & set(tokens):
                month_cols[(month, 'amount')] = c
            elif _QTY_TOKENS & set(tokens):
                month_cols[(month, 'qty')] = c
        if not month_cols:
            raise UserError(
                "No se reconoció ninguna columna de mes (pares '<mes> m' / "
                "'<mes> $'). Revisa los encabezados.")
        return header_row, prod_col, unit_col, client_col, month_cols

    def _match_product(self, ref):
        ref = (ref or '').strip()
        if not ref:
            return self.env['product.product']
        Product = self.env['product.product']
        product = Product.search([('default_code', '=', ref)], limit=1)
        if product:
            return product
        product = Product.search([('name', '=', ref)], limit=1)
        if product:
            return product
        product = Product.search([('name', 'ilike', ref)], limit=2)
        return product if len(product) == 1 else Product

    def _resolve_uom(self, unit_text, product, warnings):
        """Unidad de la columna UNIDAD (validando categoría) o la de venta."""
        if not unit_text:
            return product.uom_id
        uom = self.env['uom.uom'].search([('name', '=', unit_text.strip())], limit=1)
        if not uom:
            uom = self.env['uom.uom'].search([('name', 'ilike', unit_text.strip())], limit=1)
        if not uom:
            warnings.append("Unidad '%s' no reconocida para %s: se usó %s." % (
                unit_text, product.display_name, product.uom_id.name))
            return product.uom_id
        if not product.uom_id._has_common_reference(uom):
            warnings.append("Unidad '%s' de otra categoría para %s: se usó %s." % (
                unit_text, product.display_name, product.uom_id.name))
            return product.uom_id
        return uom

    # --- Importación ---------------------------------------------------------
    def action_import(self):
        self.ensure_one()
        budget = self.budget_id
        if budget.state != 'borrador':
            raise UserError(
                "Solo se importa sobre un presupuesto en borrador.")
        wb = self._load_workbook()
        name = self.sheet_name or (wb.sheetnames[0] if wb.sheetnames else None)
        if not name or name not in wb.sheetnames:
            raise UserError("La hoja '%s' no existe en el archivo." % (name or ''))
        ws = wb[name]
        # Errores ESTRUCTURALES abortan todo (dentro del savepoint).
        with self.env.cr.savepoint():
            header_row, prod_col, unit_col, client_col, month_cols = \
                self._parse_header(ws)
            imported = 0
            unmatched = []
            warnings = []
            cleared = set()
            Line = self.env['sgi.sales.budget.line']
            for r in range(header_row + 1, (ws.max_row or header_row) + 1):
                ref = ws.cell(r, prod_col).value
                if ref is None or str(ref).strip() == '':
                    continue
                product = self._match_product(ref)
                if not product:
                    unmatched.append(str(ref).strip())
                    continue
                unit_text = ws.cell(r, unit_col).value if unit_col else None
                uom = self._resolve_uom(unit_text, product, warnings)
                partner = False
                if client_col:
                    ctext = ws.cell(r, client_col).value
                    if ctext and str(ctext).strip():
                        partner = self.env['res.partner'].search(
                            [('name', 'ilike', str(ctext).strip())], limit=1)
                if self.conflict_mode == 'replace' and product.id not in cleared:
                    budget.line_ids.filtered(
                        lambda l: l.product_id == product).unlink()
                    cleared.add(product.id)
                months = {}
                for (month, kind), col in month_cols.items():
                    val = ws.cell(r, col).value
                    try:
                        val = float(val) if val not in (None, '') else 0.0
                    except (TypeError, ValueError):
                        val = 0.0
                    if val > 0:
                        months.setdefault(month, {})[kind] = val
                for month, kv in months.items():
                    qty = kv.get('qty', 0.0)
                    amount = kv.get('amount', 0.0)
                    ok = self._upsert_line(
                        budget, product, month, uom, partner and partner.id,
                        qty, amount, warnings)
                    if ok:
                        imported += 1
            budget.message_post(body=self._result_body(
                imported, unmatched, warnings))
        self.result = self._result_body(imported, unmatched, warnings, html=False)
        return {
            'type': 'ir.actions.act_window',
            'res_model': 'sgi.sales.budget.import',
            'res_id': self.id,
            'view_mode': 'form',
            'target': 'new',
        }

    def _upsert_line(self, budget, product, month, uom, partner_id, qty, amount, warnings):
        """Crea/suma una línea (producto, mes, cliente). Errores de datos (unidad,
        esquema mixto) se reportan y NO abortan la hoja (savepoint por línea)."""
        line_date = date(budget.year, month, 1)
        Line = self.env['sgi.sales.budget.line']
        domain = [('budget_id', '=', budget.id), ('product_id', '=', product.id),
                  ('date', '=', line_date),
                  ('partner_id', '=', partner_id or False)]
        try:
            with self.env.cr.savepoint():
                existing = Line.search(domain, limit=1)
                if existing and self.conflict_mode == 'add':
                    existing.qty_budget += qty
                    if amount:
                        existing.amount_budget = existing.amount_budget + amount
                    return True
                vals = {
                    'budget_id': budget.id, 'product_id': product.id,
                    'date': line_date, 'uom_id': uom.id,
                    'partner_id': partner_id or False, 'qty_budget': qty,
                }
                if amount:
                    vals['amount_budget'] = amount
                line = Line.create(vals)
                # m sin $: sugiere el importe con el precio de lista.
                if qty and not amount:
                    price, source = line._sgi_suggest_price()
                    if price:
                        line.price_unit_budget = price
                        line.price_source = source
                return True
        except (ValidationError, UserError) as exc:
            warnings.append("%s %02d/%s: %s" % (
                product.display_name, month, budget.year,
                exc.args[0] if exc.args else exc))
            return False

    def _result_body(self, imported, unmatched, warnings, html=True):
        parts = ["%d línea(s) importada(s)." % imported]
        if unmatched:
            parts.append("%d producto(s) sin match: %s" % (
                len(unmatched), ", ".join(unmatched)))
        if warnings:
            parts.append("Avisos:\n- " + "\n- ".join(warnings))
        text = "\n".join(parts)
        if html:
            return text.replace("\n", "<br/>")
        return text
