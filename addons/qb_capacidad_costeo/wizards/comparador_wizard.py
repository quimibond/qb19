# -*- coding: utf-8 -*-
"""Comparador de productos: pon 2–6 productos lado a lado y compara su costo
por capa (MP, energía, fabricación, operación), su precio real y sus márgenes.

Toma los números del reporte de costos (qb.costo.producto) del período; para
un producto sin ventas ese mes, calcula el costo en vivo con el mismo motor
del cotizador (precio = precio de mercado 12m). Todo en la moneda de la
compañía (MXN)."""
from odoo import api, fields, models


class QbComparadorWizard(models.TransientModel):
    _name = 'qb.comparador.wizard'
    _description = 'Comparar costos de productos'

    product_ids = fields.Many2many(
        'product.product', string='Productos a comparar',
        domain="[('sale_ok', '=', True)]",
        help='Elige de 2 a 6 productos. Se comparan lado a lado por costo, '
             'precio real y margen.')
    period = fields.Date(
        string='Período', required=True,
        help='Mes del que se toman las ventas y factores. Por defecto el '
             'último calculado.')
    margen_objetivo = fields.Float(
        string='Margen objetivo %',
        help='Si lo pones (p.ej. 25), la fila "Precio sugerido" muestra a '
             'cuánto vender CADA producto para dejar ese margen neto. Vacío = '
             'usa el margen meta global de Configuración.')
    company_currency_id = fields.Many2one(
        'res.currency', default=lambda self: self.env.company.currency_id)
    comparativa_html = fields.Html(
        compute='_compute_comparativa', string='Comparativa', sanitize=False)

    @api.model
    def default_get(self, fields_list):
        res = super().default_get(fields_list)
        ctx = self.env.context
        Costo = self.env['qb.costo.producto']
        # Lanzado desde la selección del reporte de costos → precarga productos
        if ctx.get('active_model') == 'qb.costo.producto' and ctx.get('active_ids'):
            recs = Costo.browse(ctx['active_ids']).exists()
            res['product_ids'] = [(6, 0, recs.mapped('product_id').ids)]
            periods = [p for p in recs.mapped('period') if p]
            if periods:
                res['period'] = max(periods)
        if not res.get('period'):
            latest = Costo.search([], order='period desc', limit=1)
            res['period'] = (latest.period if latest
                             else fields.Date.today().replace(day=1))
        return res

    # ------------------------------------------------------------------
    def _sugerido(self, variable, fab, op_pct, piso_lleno):
        """Precio para el margen objetivo del wizard (o el meta global), sin
        piso de mercado (aquí queremos la guía cost-plus pura). Devuelve
        (precio_sugerido, margen_neto_% a ese precio)."""
        Costo = self.env['qb.costo.producto']
        target = (self.margen_objetivo / 100.0) if self.margen_objetivo else None
        sug = Costo._precio_sugerido(variable, fab, op_pct, piso_lleno, 0.0,
                                     target=target)
        neto = (100.0 * (sug - variable - fab - op_pct * sug) / sug
                if sug else 0.0)
        return sug, neto

    def _metrics(self, product):
        """Métricas de UN producto para el período: primero del reporte
        guardado; si no hay fila, se calculan en vivo (precio = mercado)."""
        Costo = self.env['qb.costo.producto']
        rec = Costo.search([('period', '=', self.period),
                            ('product_id', '=', product.id),
                            ('company_id', '=', self.env.company.id)], limit=1)
        if rec:
            op_pct = (rec.factores_id.op_pct if rec.factores_id
                      else (rec.op_unit / rec.precio_prom
                            if rec.precio_prom else 0.0))
            piso_lleno = ((rec.costo_variable + rec.fab_unit) / (1.0 - op_pct)
                          if op_pct < 1 else 0.0)
            sug, sug_neto = self._sugerido(
                rec.costo_variable, rec.fab_unit, op_pct, piso_lleno)
            return {
                'ref': rec.default_code or product.name,
                'uom': rec.uom_name or '',
                'divisa': rec.divisa_venta or '',
                'precio': rec.precio_prom,
                'mp': rec.mp_unit, 'energia': rec.energia_unit,
                'variable': rec.costo_variable, 'fab': rec.fab_unit,
                'op': rec.op_unit, 'absorbido': rec.costo_absorbido,
                'piso_lleno': piso_lleno, 'sugerido': sug, 'sug_neto': sug_neto,
                'contrib': rec.margen_contribucion,
                'contrib_pct': rec.margen_contribucion_pct,
                'bruto_pct': rec.margen_bruto_pct,
                'abs_pct': rec.margen_absorbido_pct,
                'vendido': bool(rec.qty_vendida),
                'alerta': rec.alerta,
            }
        # Sin fila del período: costo en vivo con el mismo motor
        factores = self.env['qb.costo.factores'].search(
            [], order='period DESC', limit=1)
        if not factores:
            return None
        q = Costo.quote_product(product, factores)
        precio = q.get('precio_mercado', 0.0)
        variable = q['variable']
        absorbido = variable + q['fab'] + q['op_pct'] * precio
        contrib = precio - variable if precio else 0.0
        sug, sug_neto = self._sugerido(
            variable, q['fab'], q['op_pct'], q['piso_lleno'])
        return {
            'ref': product.default_code or product.name,
            'uom': q.get('uom_name', product.uom_id.name or ''),
            'divisa': '',
            'precio': precio,
            'mp': q['mp'], 'energia': q['energia'],
            'variable': variable, 'fab': q['fab'],
            'op': q['op_pct'] * precio, 'absorbido': absorbido,
            'piso_lleno': q['piso_lleno'], 'sugerido': sug, 'sug_neto': sug_neto,
            'contrib': contrib,
            'contrib_pct': 100.0 * contrib / precio if precio else 0.0,
            'bruto_pct': (100.0 * (precio - variable - q['fab']) / precio
                          if precio else 0.0),
            'abs_pct': 100.0 * (precio - absorbido) / precio if precio else 0.0,
            'vendido': False,
            'alerta': '',
        }

    @api.depends('product_ids', 'period', 'margen_objetivo')
    def _compute_comparativa(self):
        for wiz in self:
            prods = wiz.product_ids[:6]
            if len(prods) < 2:
                wiz.comparativa_html = (
                    '<p class="text-muted">Elige al menos 2 productos para '
                    'compararlos.</p>')
                continue
            cols = [(p, wiz._metrics(p)) for p in prods]
            cols = [(p, m) for p, m in cols if m]
            wiz.comparativa_html = wiz._render(cols)

    def _render(self, cols):
        sym = self.company_currency_id.symbol or '$'

        def money(v):
            return '%s%s' % (sym, '{:,.2f}'.format(v or 0.0))

        def pct(v):
            return '{:,.1f}%'.format(v or 0.0)

        sug_label = ('⭐ Precio p/ margen %g%% / u' % self.margen_objetivo
                     if self.margen_objetivo else '⭐ Precio sugerido / u')

        # Filas: (etiqueta, key, formato, ¿resaltar?)
        filas = [
            ('Unidad', 'uom', 'raw', False),
            ('Facturado en', 'divisa', 'raw', False),
            ('Precio real / u', 'precio', 'money', True),
            ('Materia prima', 'mp', 'money', False),
            ('+ Energía', 'energia', 'money', False),
            ('= Costo variable', 'variable', 'money', True),
            ('+ Fabricación', 'fab', 'money', False),
            ('+ Operación', 'op', 'money', False),
            ('= Costo absorbido', 'absorbido', 'money', True),
            ('Contribución / u', 'contrib', 'money', False),
            ('Contribución %', 'contrib_pct', 'pct', True),
            ('Margen bruto %', 'bruto_pct', 'pct', True),
            ('Margen neto %', 'abs_pct', 'pct', True),
            ('Piso a planta llena / u', 'piso_lleno', 'money', False),
            (sug_label, 'sugerido', 'money', True),
            ('Margen al sugerido %', 'sug_neto', 'pct', False),
        ]
        th = ''.join(
            '<th class="text-end" style="padding:6px 10px;">%s%s</th>'
            % (m['ref'], '' if m['vendido'] else
               ' <span style="font-weight:normal;color:#888;">(sin venta '
               'del mes)</span>')
            for _p, m in cols)
        body = ''
        for etq, key, fmt, hl in filas:
            # Fila divisa: solo si algún producto se facturó en otra moneda
            if key == 'divisa' and not any(m.get('divisa') for _p, m in cols):
                continue
            tds = ''
            for _p, m in cols:
                v = m.get(key)
                if fmt == 'money':
                    txt = money(v)
                elif fmt == 'pct':
                    txt = pct(v)
                else:
                    txt = v or '—'
                color = ''
                if key in ('contrib_pct', 'bruto_pct', 'abs_pct'):
                    color = ('color:#c0392b;' if (v or 0) < 0
                             else 'color:#27ae60;')
                tds += ('<td class="text-end" style="padding:5px 10px;%s%s">%s</td>'
                        % ('font-weight:bold;' if hl else '', color, txt))
            body += ('<tr style="%s"><td style="padding:5px 10px;%s">%s</td>%s</tr>'
                     % ('background:#f7f9fa;' if hl else '',
                        'font-weight:bold;' if hl else '', etq, tds))
        return (
            '<div style="overflow-x:auto;">'
            '<table style="border-collapse:collapse;min-width:100%%;font-size:13px;">'
            '<thead><tr style="border-bottom:2px solid #34495e;">'
            '<th style="padding:6px 10px;text-align:left;">Concepto</th>%s</tr></thead>'
            '<tbody>%s</tbody></table>'
            '<p style="font-size:11px;color:#888;margin-top:8px;">Todo en %s '
            '(moneda de la compañía). Los productos facturados en otra divisa '
            'ya están convertidos al TC de la factura. "Costo variable" = MP + '
            'energía (lo que sale de la bolsa por unidad extra). "Costo '
            'absorbido" = variable + fabricación + operación (costo completo). '
            'Márgenes: contribución = precio − variable (aporte a fijos, aún '
            'no es utilidad); bruto = precio − (variable + fabricación); '
            'neto = después de TODO, incluida operación.'
            '</p></div>'
            % (th, body, self.company_currency_id.name or 'MXN'))
