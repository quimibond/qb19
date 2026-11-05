# -*- coding: utf-8 -*-
"""PDF y Excel del presupuesto y del pronóstico vigente (19.0.3.6.0).

- Presupuesto: resumen por mercado y mes, y detalle por mercado (cliente,
  producto o proyecto, cantidades por mes, precio presupuestal e importe). El
  PDF es el F-P-A28-17/18 (menú Imprimir); el Excel trae las mismas tablas.
- Pronóstico vigente: el asistente «Imprimir o descargar el pronóstico» arma la
  matriz cliente y planta × producto × semana en la unidad elegida (metros,
  kilos o la de venta), para un rango de semanas y, si se quiere, solo algunos
  clientes.
"""
import base64
import io
from collections import OrderedDict, defaultdict
from datetime import timedelta

from odoo import api, fields, models
from odoo.exceptions import UserError

from .qb_forecast_week import week_label

MONTHS = ['Ene', 'Feb', 'Mar', 'Abr', 'May', 'Jun', 'Jul', 'Ago', 'Sep', 'Oct', 'Nov', 'Dic']
UNITS = [('m', "Metros"), ('kg', "Kilos"), ('venta', "Unidad de venta")]
QTY_FIELD = {'m': 'qty_m', 'kg': 'qty_kg', 'venta': 'qty_venta'}


def xlsx_download(env, filename, sheets, res_model, res_id):
    """Libro con ``sheets`` = [(título, encabezado, renglones)] como adjunto descargable."""
    try:
        import openpyxl
        from openpyxl.styles import Font
    except ImportError as exc:  # pragma: no cover - en Odoo siempre está
        raise UserError("Falta openpyxl en el servidor para generar el Excel.") from exc
    wb = openpyxl.Workbook()
    wb.remove(wb.active)
    for title, header, rows in sheets:
        ws = wb.create_sheet(title[:31])
        ws.append(header)
        for cell in ws[1]:
            cell.font = Font(bold=True)
        for row in rows:
            ws.append(row)
        ws.freeze_panes = 'A2'
        for idx, name in enumerate(header, start=1):
            ws.column_dimensions[openpyxl.utils.get_column_letter(idx)].width = max(10, min(45, len(str(name)) + 4))
    buf = io.BytesIO()
    wb.save(buf)
    attachment = env['ir.attachment'].create({
        'name': filename, 'datas': base64.b64encode(buf.getvalue()),
        'res_model': res_model, 'res_id': res_id,
        'mimetype': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    })
    return {'type': 'ir.actions.act_url', 'url': '/web/content/%d?download=true' % attachment.id,
            'target': 'self'}


class SgiSalesBudgetReports(models.Model):
    _inherit = 'sgi.sales.budget'

    def _qb_report_data(self):
        """Resumen por mercado × mes y detalle por mercado para el PDF y el Excel."""
        self.ensure_one()
        from .sgi_sales_budget import MERCADOS
        labels = dict(MERCADOS)
        order = [k for k, _l in MERCADOS]
        summary = OrderedDict()
        sections = OrderedDict()
        for key in order:
            summary[key] = {'label': labels[key], 'cells': defaultdict(float), 'total': 0.0}
            sections[key] = {'label': labels[key], 'rows': OrderedDict(), 'cells': defaultdict(float), 'total': 0.0}
        for line in self.line_ids.sorted(lambda l: (l.partner_id.name or '', l._qb_label(), l.date)):
            key = line.mercado if line.mercado in summary else order[0]
            month = line.date.month
            summary[key]['cells'][month] += line.amount_budget
            summary[key]['total'] += line.amount_budget
            section = sections[key]
            section['cells'][month] += line.amount_budget
            section['total'] += line.amount_budget
            rkey = (line.partner_id.id, line.product_id.id or line.proyecto_descripcion, line.uom_id.id)
            row = section['rows'].get(rkey)
            if not row:
                row = section['rows'][rkey] = {
                    'client': line.partner_id.name or '', 'product': line._qb_label(),
                    'uom': line.uom_id.name or '', 'cells': defaultdict(float), 'qty_total': 0.0,
                    'amount_cells': defaultdict(float), 'amount_total': 0.0,
                    'price': line.price_input, 'currency': line.price_currency_id.name or '',
                    'basis': dict(line._fields['price_basis'].selection).get(line.price_basis, ''),
                }
            row['cells'][month] += line.qty_budget
            row['qty_total'] += line.qty_budget
            row['amount_cells'][month] += line.amount_budget
            row['amount_total'] += line.amount_budget
        total_cells = defaultdict(float)
        for item in summary.values():
            for month, amount in item['cells'].items():
                total_cells[month] += amount
        return {
            'months': list(range(1, 13)), 'month_names': MONTHS,
            'summary': [v for v in summary.values() if v['total']],
            'total_cells': total_cells, 'total': sum(v['total'] for v in summary.values()),
            'sections': [dict(v, rows=list(v['rows'].values())) for v in sections.values() if v['rows']],
        }

    def action_qb_download_xlsx(self):
        self.ensure_one()
        data = self._qb_report_data()
        months = list(range(1, 13))
        resumen = [[s['label']] + [round(s['cells'].get(m, 0.0), 2) for m in months] + [round(s['total'], 2)]
                   for s in data['summary']]
        resumen.append(["Total"] + [round(data['total_cells'].get(m, 0.0), 2) for m in months]
                       + [round(data['total'], 2)])
        cantidades, importes = [], []
        for section in data['sections']:
            for row in section['rows']:
                cantidades.append([section['label'], row['client'], row['product'], row['uom']]
                                  + [row['cells'].get(m, 0.0) for m in months]
                                  + [row['qty_total'], row['price'], row['currency'], row['basis'],
                                     round(row['amount_total'], 2)])
                importes.append([section['label'], row['client'], row['product']]
                                + [round(row['amount_cells'].get(m, 0.0), 2) for m in months]
                                + [round(row['amount_total'], 2)])
        return xlsx_download(self.env, '%s.xlsx' % (self.folio or self.name), [
            ("Resumen", ["Mercado"] + MONTHS + ["Total"], resumen),
            ("Cantidades", ["Mercado", "Cliente", "Producto o proyecto", "Unidad"] + MONTHS
             + ["Total", "Precio", "Moneda", "Base del precio", "Importe MXN"], cantidades),
            ("Importes MXN", ["Mercado", "Cliente", "Producto o proyecto"] + MONTHS + ["Total"], importes),
        ], self._name, self.id)


class QbForecastPrint(models.TransientModel):
    _name = 'qb.forecast.print'
    _description = "Imprimir o descargar el pronóstico vigente"

    def _default_monday(self):
        today = fields.Date.context_today(self)
        return today - timedelta(days=today.weekday())

    company_id = fields.Many2one('res.company', string="Compañía", required=True,
                                 default=lambda self: self.env.company)
    date_from = fields.Date(string="Desde la semana del", required=True, default=_default_monday)
    weeks = fields.Integer(string="Semanas", required=True, default=13)
    partner_ids = fields.Many2many('res.partner', string="Clientes",
                                   help="Vacío = todos los clientes con pronóstico en esas semanas.")
    unit = fields.Selection(UNITS, string="Unidad", required=True, default='m')

    def _qb_mondays(self):
        self.ensure_one()
        if not 1 <= self.weeks <= 60:
            raise UserError("Elija entre 1 y 60 semanas.")
        start = self.date_from - timedelta(days=self.date_from.weekday())
        return [start + timedelta(weeks=i) for i in range(self.weeks)]

    def _qb_matrix(self):
        """Cliente y planta → producto × semana en la unidad elegida."""
        self.ensure_one()
        mondays = self._qb_mondays()
        domain = [('company_id', '=', self.company_id.id), ('week', '>=', mondays[0]), ('week', '<=', mondays[-1])]
        if self.partner_ids:
            domain.append(('partner_id', 'child_of', self.partner_ids.commercial_partner_id.ids))
        field = QTY_FIELD[self.unit]
        groups = OrderedDict()
        for line in self.env['qb.forecast.line'].search(domain).sorted(
                lambda l: (l.partner_id.commercial_partner_id.name or '', l.plant or '',
                           l.product_id.default_code or l.product_id.name or '', l.week)):
            partner = line.partner_id.commercial_partner_id
            gkey = (partner.id, line.plant or '')
            group = groups.get(gkey)
            if not group:
                group = groups[gkey] = {'client': partner.name or '', 'plant': line.plant or '',
                                        'rows': OrderedDict(), 'cells': defaultdict(float), 'total': 0.0}
            row = group['rows'].get(line.product_id.id)
            if not row:
                row = group['rows'][line.product_id.id] = {
                    'product': line.product_id.default_code or line.product_id.name or '',
                    'uom': line.uom_venta if self.unit == 'venta' else dict(UNITS)[self.unit].lower(),
                    'cells': defaultdict(float), 'total': 0.0}
            monday = line.week - timedelta(days=line.week.weekday())
            qty = line[field] or 0.0
            row['cells'][monday] += qty
            row['total'] += qty
            group['cells'][monday] += qty
            group['total'] += qty
        return {
            'mondays': mondays, 'labels': [week_label(m) for m in mondays],
            'unit': dict(UNITS)[self.unit], 'sum_groups': self.unit != 'venta',
            'groups': [dict(g, rows=list(g['rows'].values())) for g in groups.values()],
        }

    def action_print(self):
        self.ensure_one()
        self._qb_mondays()
        return self.env.ref('quimibond_ventas_presupuesto.action_report_forecast').report_action(self)

    def action_xlsx(self):
        self.ensure_one()
        data = self._qb_matrix()
        rows = []
        for group in data['groups']:
            for row in group['rows']:
                rows.append([group['client'], group['plant'], row['product'], row['uom']]
                            + [round(row['cells'].get(m, 0.0), 2) for m in data['mondays']]
                            + [round(row['total'], 2)])
        return xlsx_download(self.env, 'Pronostico %s.xlsx' % data['labels'][0].split(' ')[0], [
            ("Pronóstico", ["Cliente", "Planta", "Producto", "Unidad"] + data['labels'] + ["Total"], rows),
        ], self._name, self.id)

    @api.model
    def action_open(self):
        return {'type': 'ir.actions.act_window', 'name': "Imprimir o descargar el pronóstico",
                'res_model': self._name, 'view_mode': 'form', 'target': 'new'}
