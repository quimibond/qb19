# -*- coding: utf-8 -*-
"""Costo por producto y período: la fórmula única del costeo v2.

    mp_u           = receta vigente explotada a precio de hoja al corte
    fabricacion_u  = Σ_centro horas_u × tarifa_centro          (fija + variable)
    energia_u      = Σ_centro horas_u × tarifa_variable_centro (la parte variable)
    variable_u     = mp_u + energia_u
    produccion_u   = mp_u + fabricacion_u
    vendible_u     = produccion_u ÷ rendimiento
    op_u           = op_pct × precio
    total_u        = vendible_u + op_u
    piso_ocioso    = variable_u ÷ rendimiento       (nunca vender abajo)
    piso_lleno     = vendible_u ÷ (1 − op_pct)      (precio de equilibrio)

Las horas salen de `qb.producto.horas` (con su fuente), las tarifas de
`qb.tarifa` del período, la MP de `qb.producto.mp` al corte, el rendimiento
de `qb.producto.rendimiento` y el peso de `qb.producto.kg`. `calidad` es la
peor de las entradas: la cotización y el cierre la leen.

Los totales del período son costo unitario × cantidad vendida. La traza por
lotes de lo que Odoo capitalizó (spec §5.5) llega con la conciliación.
"""
from odoo import api, fields, models


QTY_DEDUP_SQL = """
    SELECT move_id, product_id, currency_id, quantity, move_type
    FROM (
        SELECT l.*,
               COUNT(*) OVER (
                   PARTITION BY l.move_id, l.product_id, ABS(l.quantity)) AS n,
               ROW_NUMBER() OVER (
                   PARTITION BY l.move_id, l.product_id, ABS(l.quantity)
                   ORDER BY l.move_id) AS rn
        FROM lines l
    ) g
    WHERE g.n < 3 OR g.rn = 1
"""

CALIDAD_SEL = [
    ('alta', 'Alta: medido o estándar'),
    ('media', 'Media: hermano o galga'),
    ('baja', 'Baja: estimado'),
    ('dudosa', 'Dudosa: validaciones abiertas'),
    ('ninguna', 'Sin dato'),
]


class QbCostoProducto(models.Model):
    _name = 'qb.costo.unitario'
    _description = 'Costo por producto y período (v2)'
    _order = 'period desc, ventas_total desc'
    _rec_name = 'product_id'

    periodo_id = fields.Many2one('qb.periodo', required=True, index=True,
                                 ondelete='cascade')
    period = fields.Date(related='periodo_id.period', store=True)
    state = fields.Selection(related='periodo_id.state', store=True)
    product_id = fields.Many2one('product.product', required=True, index=True,
                                 ondelete='cascade')
    default_code = fields.Char(related='product_id.default_code', store=True)
    company_id = fields.Many2one('res.company', required=True)
    uom_name = fields.Char(string='UdM')
    kg_por_unidad = fields.Float(digits=(16, 6))
    peso_fuente = fields.Char()

    # Ventas del mes (hecho contable)
    qty_vendida = fields.Float(digits=(16, 2))
    ventas_total = fields.Float(
        digits=(16, 2), help='Σ −balance de las líneas de ingreso de las '
        'facturas del mes, en moneda de la compañía (las notas restan).')
    precio_prom = fields.Float(digits=(16, 4), string='Precio promedio')
    divisa_id = fields.Many2one('res.currency', string='Divisa')
    qty_divisa = fields.Float(digits=(16, 2))
    ventas_total_divisa = fields.Float(digits=(16, 2))
    precio_prom_divisa = fields.Float(digits=(16, 4))
    tc_prom = fields.Float(digits=(16, 4), string='TC efectivo')

    # Capas por unidad
    mp_unit = fields.Float(digits=(16, 4), string='MP $/u')
    energia_unit = fields.Float(digits=(16, 4), string='Energía $/u',
                                help='Σ horas × tarifa variable del centro.')
    fabricacion_unit = fields.Float(
        digits=(16, 4), string='Fabricación $/u',
        help='Σ horas × tarifa del centro (fija + variable).')
    costo_variable = fields.Float(digits=(16, 4), string='Variable $/u')
    costo_produccion = fields.Float(digits=(16, 4), string='Producción $/u')
    rendimiento = fields.Float(digits=(6, 4))
    rendimiento_fuente = fields.Char()
    costo_vendible = fields.Float(digits=(16, 4), string='Vendible $/u')
    op_pct = fields.Float(digits=(16, 6))
    op_unit = fields.Float(digits=(16, 4), string='Operación $/u')
    costo_total = fields.Float(digits=(16, 4), string='Total $/u')
    piso_ocioso = fields.Float(digits=(16, 4))
    piso_lleno = fields.Float(digits=(16, 4))
    margen_contribucion_pct = fields.Float(digits=(6, 2))
    margen_bruto_pct = fields.Float(digits=(6, 2))
    margen_neto_pct = fields.Float(digits=(6, 2))
    calidad = fields.Selection(CALIDAD_SEL, default='ninguna')
    calidad_detalle = fields.Char()
    semaforo = fields.Selection([
        ('rojo', 'Abajo del piso con capacidad ociosa'),
        ('ambar', 'Entre pisos'),
        ('verde', 'Arriba del piso lleno'),
        ('sin_precio', 'Sin ventas')], default='sin_precio')

    # Totales del período (unitario × qty vendida)
    mp_total = fields.Float(digits=(16, 2))
    energia_total = fields.Float(digits=(16, 2))
    fabricacion_total = fields.Float(digits=(16, 2))
    op_total = fields.Float(digits=(16, 2))
    costo_total_periodo = fields.Float(digits=(16, 2), string='Costo total $')
    margen_neto_total = fields.Float(digits=(16, 2))
    linea_ids = fields.One2many('qb.costo.unitario.centro', 'costo_id',
                                string='Por centro')

    _periodo_product_uniq = models.Constraint(
        'unique(periodo_id, product_id)',
        'Ya existe el costo de ese producto en ese período.')

    # ------------------------------------------------------------------
    # Ventas
    # ------------------------------------------------------------------
    @api.model
    def ventas_por_producto(self, date_from, date_to):
        """{product_id: dict} con qty (dedup del triplete lista/descuento/
        neta), revenue en moneda de la compañía y la divisa dominante."""
        company = self.env.company
        self.env.flush_all()
        self.env.cr.execute("""
            WITH lines AS (
                SELECT aml.move_id, aml.product_id, aml.quantity, aml.balance,
                       aml.amount_currency, am.move_type, am.currency_id
                FROM account_move_line aml
                JOIN account_move am ON am.id = aml.move_id
                JOIN account_account aa ON aa.id = aml.account_id
                WHERE am.move_type IN ('out_invoice', 'out_refund')
                  AND am.state = 'posted'
                  AND aml.display_type = 'product'
                  AND aml.product_id IS NOT NULL
                  AND aa.account_type = 'income'
                  AND am.invoice_date >= %s AND am.invoice_date < %s
                  AND aml.company_id = %s
            ),
            qty_dedup AS (""" + QTY_DEDUP_SQL + """),
            qty_agg AS (
                SELECT product_id, currency_id,
                       SUM(CASE WHEN move_type = 'out_refund'
                                THEN -quantity ELSE quantity END) AS qty
                FROM qty_dedup GROUP BY 1, 2
            ),
            rev_agg AS (
                SELECT product_id, currency_id, SUM(-balance) AS revenue,
                       SUM(-amount_currency) AS revenue_cur
                FROM lines GROUP BY 1, 2
            )
            SELECT r.product_id, r.currency_id, COALESCE(q.qty, 0),
                   r.revenue, r.revenue_cur
            FROM rev_agg r
            LEFT JOIN qty_agg q ON q.product_id = r.product_id
                 AND q.currency_id IS NOT DISTINCT FROM r.currency_id
        """, (date_from, date_to, company.id))
        out, foreign = {}, {}
        for pid, cur_id, qty, rev, rev_cur in self.env.cr.fetchall():
            row = out.setdefault(pid, {'qty': 0.0, 'revenue': 0.0,
                                       'divisa_id': None, 'qty_divisa': 0.0,
                                       'revenue_divisa': 0.0, 'revenue_mxn_divisa': 0.0})
            row['qty'] += qty or 0.0
            row['revenue'] += rev or 0.0
            if cur_id and cur_id != company.currency_id.id:
                foreign.setdefault(pid, {})[cur_id] = (qty or 0.0, rev or 0.0,
                                                       rev_cur or 0.0)
        for pid, by_cur in foreign.items():
            cur_id = max(by_cur, key=lambda c: abs(by_cur[c][1]))
            qty, rev_mxn, rev_cur = by_cur[cur_id]
            out[pid].update(divisa_id=cur_id, qty_divisa=qty,
                            revenue_divisa=rev_cur, revenue_mxn_divisa=rev_mxn)
        return out

    # ------------------------------------------------------------------
    # Cálculo
    # ------------------------------------------------------------------
    @api.model
    def calcular_periodo(self, periodo, products=None):
        """Recalcula el costo por producto del período. Usa las tarifas ya
        calculadas; las horas, la MP, el rendimiento y el peso almacenados
        (recalculados antes si hace falta)."""
        company = periodo.company_id
        self = self.with_company(company)
        Horas = self.env['qb.producto.horas']
        Mp = self.env['qb.producto.mp']
        Rend = self.env['qb.producto.rendimiento']
        Peso = self.env['qb.producto.kg']
        tarifas = {t.centro_id.id: t for t in periodo.tarifa_ids}
        ventas = self.ventas_por_producto(periodo.date_from, periodo.date_to)
        Prod = self.env['product.product']
        if products is None:
            products = Horas._productos_a_calcular() | Prod.browse(list(ventas))
            products = products.filtered(lambda p: p.active or p.id in ventas)
        horas = {}
        for h in Horas.search([('product_id', 'in', products.ids),
                               ('company_id', '=', company.id)]):
            horas.setdefault(h.product_id.id, {})[h.centro_id.id] = h
        rend_rows = {r.product_id.id: r for r in Rend.search(
            [('product_id', 'in', products.ids), ('company_id', '=', company.id)])}
        planta = Rend.planta(periodo.date_to)
        mp_ctx = {'memo': {}, 'precios': {}, 'pila': set(), 'dudosos': set()}
        cutoff = periodo.date_to
        self.env.cr.execute(
            'SELECT DISTINCT product_id FROM mrp_bom_line WHERE product_id IS NOT NULL')
        hojas_ids = [r[0] for r in self.env.cr.fetchall()]
        mp_ctx['precios'] = Mp.precios_compra(hojas_ids, cutoff)
        dudosos = set(self.env['qb.producto.validacion'].search(
            [('estado', '=', 'abierta'), ('company_id', '=', company.id)])
            .mapped('product_id').ids)
        existentes = {r.product_id.id: r for r in self.search(
            [('periodo_id', '=', periodo.id)])}
        Linea = self.env['qb.costo.unitario.centro']
        centros_directos = self.env['qb.centro'].search(
            [('company_id', '=', company.id), ('nature', '=', 'directo')])
        filas = self.browse()
        for p in products:
            mp, hojas = Mp.explotar(p, cutoff, mp_ctx)
            hs = horas.get(p.id, {})
            if mp is None and not hs and p.id not in ventas:
                continue
            mp = mp or 0.0
            fab = energia = 0.0
            lineas = []
            rango = 0
            for cid, h in hs.items():
                t = tarifas.get(cid)
                if not t or not h.horas_unidad:
                    continue
                costo = h.horas_unidad * t.tarifa
                fab += costo
                energia += h.horas_unidad * t.tarifa_variable
                lineas.append((0, 0, {
                    'centro_id': cid, 'horas_unidad': h.horas_unidad,
                    'tarifa': t.tarifa, 'tarifa_variable': t.tarifa_variable,
                    'costo_unit': costo, 'fuente': h.fuente,
                    'calidad': h.calidad}))
                rango = max(rango, {'alta': 1, 'media': 2, 'baja': 3,
                                    'ninguna': 4}.get(h.calidad, 4))
            rr = rend_rows.get(p.id)
            if rr and rr.fuente != 'ninguna':
                rend, rend_fuente = rr.rendimiento or 1.0, rr.fuente
            else:
                rend, rend_fuente = planta, 'planta'
            rend = min(max(rend or 1.0, 0.05), 1.0)
            kg, peso_fuente = Peso.kg_de(p)
            venta = ventas.get(p.id, {})
            qty = venta.get('qty', 0.0)
            revenue = venta.get('revenue', 0.0)
            precio = revenue / qty if qty > 0 else 0.0
            qty_ef = qty if precio else 0.0
            variable = mp + energia
            produccion = mp + fab
            vendible = produccion / rend
            op_pct = periodo.op_pct or 0.0
            op = op_pct * precio
            total = vendible + op
            piso_ocioso = variable / rend
            piso_lleno = vendible / (1 - op_pct) if op_pct < 1 else vendible
            dud = p.id in dudosos or any(lid in dudosos for lid in hojas)
            # Centros por los que pasa el producto según su código y que no
            # tienen horas: el costo está incompleto, aunque lo que sí hay
            # sea medido.
            faltan = [c.name for c in Horas.centros_esperados(p, centros_directos, cutoff)
                      if not (hs.get(c.id) and hs[c.id].horas_unidad)] if hs else []
            if faltan:
                rango = max(rango, 3)
            if dud:
                calidad = 'dudosa'
            elif rango >= 4 and not hs:
                calidad = 'ninguna' if not mp else 'baja'
            else:
                calidad = {1: 'alta', 2: 'media', 3: 'baja', 4: 'ninguna'}[rango or 1]
            det = []
            if hs:
                det.append('horas: ' + ', '.join(sorted({
                    h.fuente if h.horas_propias else 'heredadas'
                    for h in hs.values()})))
            if faltan:
                det.append('sin horas en ' + ', '.join(faltan))
            det.append('rendimiento: %s' % rend_fuente)
            det.append('peso: %s' % peso_fuente)
            if dud:
                det.append('validaciones abiertas')
            if not precio:
                semaforo = 'sin_precio'
            elif precio < piso_ocioso:
                semaforo = 'rojo'
            elif precio < piso_lleno:
                semaforo = 'ambar'
            else:
                semaforo = 'verde'
            rev_div = venta.get('revenue_divisa', 0.0)
            qty_div = venta.get('qty_divisa', 0.0)
            vals = {
                'uom_name': p.uom_id.name, 'kg_por_unidad': kg,
                'peso_fuente': peso_fuente,
                'qty_vendida': qty, 'ventas_total': revenue, 'precio_prom': precio,
                'divisa_id': venta.get('divisa_id') or False,
                'qty_divisa': qty_div, 'ventas_total_divisa': rev_div,
                'precio_prom_divisa': rev_div / qty_div if qty_div else 0.0,
                'tc_prom': (venta.get('revenue_mxn_divisa', 0.0) / rev_div
                            if rev_div else 0.0),
                'mp_unit': mp, 'energia_unit': energia, 'fabricacion_unit': fab,
                'costo_variable': variable, 'costo_produccion': produccion,
                'rendimiento': rend, 'rendimiento_fuente': rend_fuente,
                'costo_vendible': vendible, 'op_pct': op_pct, 'op_unit': op,
                'costo_total': total, 'piso_ocioso': piso_ocioso,
                'piso_lleno': piso_lleno,
                'margen_contribucion_pct': 100 * (precio - variable) / precio if precio else 0.0,
                'margen_bruto_pct': 100 * (precio - produccion) / precio if precio else 0.0,
                'margen_neto_pct': 100 * (precio - total) / precio if precio else 0.0,
                'calidad': calidad, 'calidad_detalle': ' · '.join(det),
                'semaforo': semaforo,
                'mp_total': mp * qty_ef, 'energia_total': energia * qty_ef,
                'fabricacion_total': fab * qty_ef, 'op_total': op * qty_ef,
                'costo_total_periodo': total * qty_ef,
                'margen_neto_total': revenue - total * qty_ef if precio else 0.0,
                'linea_ids': [(5, 0, 0)] + lineas,
            }
            rec = existentes.pop(p.id, None)
            if rec:
                rec.write(vals)
            else:
                rec = self.create(dict(vals, periodo_id=periodo.id,
                                       product_id=p.id, company_id=company.id))
            filas |= rec
        for rec in existentes.values():
            rec.unlink()
        _ = Linea
        return filas


class QbCostoProductoCentro(models.Model):
    _name = 'qb.costo.unitario.centro'
    _description = 'Costo por centro de una unidad de producto'
    _order = 'costo_id, centro_id'

    costo_id = fields.Many2one('qb.costo.unitario', required=True,
                               ondelete='cascade', index=True)
    centro_id = fields.Many2one('qb.centro', required=True)
    horas_unidad = fields.Float(digits=(16, 6))
    tarifa = fields.Float(digits=(16, 4))
    tarifa_variable = fields.Float(digits=(16, 4))
    costo_unit = fields.Float(digits=(16, 4), string='$/u')
    fuente = fields.Char()
    calidad = fields.Char()
