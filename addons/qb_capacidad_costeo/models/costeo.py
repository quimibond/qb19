# -*- coding: utf-8 -*-
"""Motor de costo real por producto.

Capas (validadas con dirección; mismas reglas que el modelo de silver):

1. MP  = explosión recursiva de BOM al ÚLTIMO costo de compra (fallback
         costo promedio). Importados (' I') = landed cost (avg de Odoo, que
         ya incluye flete/aduana vía AVCO); sin costo propio → gemelo
         nacional. Subproductos (SALDO/DESPERDICIO) = $0 (su MP ya está en
         la receta del producto principal).
2. Energía variable = $/kg × peso (luz/gas/agua; importados sin energía).
3. Fabricación absorbida = pool GL fijo de fábrica (MOD + overhead +
         depreciación + arrendamiento de maquinaria) repartido HÍBRIDO:
         fab_weight_share por peso (tejido+tintorería) y el resto por largo
         (acabado). Entretelas usan su propio factor $/m (su MOD + renta
         contractual ÷ sus metros). Importados y subproductos NO cargan
         fabricación.
4. Operación = op_pct × precio (op_pct = Σ cuentas operación ÷ Σ ventas,
         suavizado; o override).

costo_variable = MP + energía        (para margen de contribución)
costo_absorbido = variable + fab + op (para P&L / precio piso lleno)

Los factores del período se guardan en qb.costo.factores para que cada
número sea auditable (pool, denominadores, ventana usada).
"""
import logging
from datetime import date

from dateutil.relativedelta import relativedelta

from odoo import api, fields, models
from odoo.tools import html_escape

from .cuenta_map import CUENTA_MAP_SQL, mo_qty_sql, wo_qty_sql

_logger = logging.getLogger(__name__)

KG_UOM_NAMES = ('kg', 'kgs', 'kilogramo', 'kilogramos')
FAB_BUCKETS = ('mod', 'overhead_fab', 'depreciacion', 'arrend_maquinaria')


class QbCostoFactores(models.Model):
    _name = 'qb.costo.factores'
    _description = 'Factores de costeo por período (trazabilidad)'
    _order = 'period DESC'
    _rec_name = 'period'

    period = fields.Date(required=True, index=True,
                         help='Primer día del mes calculado.')
    company_id = fields.Many2one(
        'res.company', default=lambda self: self.env.company, required=True)
    window_months = fields.Integer(string='Ventana de suavizado (meses)')
    fab_pool_month = fields.Float(
        string='Pool fabricación/mes',
        help='MOD + overhead + depreciación + arrendamiento maquinaria, '
             'promedio suavizado, sin la parte de entretelas.')
    energia_pool_month = fields.Float(string='Pool energía/mes')
    op_pool_month = fields.Float(string='Pool operación/mes')
    ventas_pool_month = fields.Float(string='Ventas/mes (promedio)')
    entretela_pool_month = fields.Float(string='Pool entretelas/mes')
    kg_denom_month = fields.Float(string='Denominador kg/mes')
    m_denom_month = fields.Float(string='Denominador m/mes')
    entretela_m_denom_month = fields.Float(string='Metros entretela/mes')
    fab_weight_share = fields.Float(string='Share peso')
    factor_fab_kg = fields.Float(string='Factor fabricación $/kg')
    factor_fab_m = fields.Float(string='Factor fabricación $/m')
    energia_por_kg = fields.Float(string='Energía $/kg')
    op_pct = fields.Float(string='Operación % sobre ventas')
    entretela_factor_m = fields.Float(string='Factor entretela $/m')
    cobertura_fab_pct = fields.Float(
        string='Cobertura del pool %',
        help='Σ fabricación absorbida en lo vendido ÷ pool. ~90% es sano '
             '(el resto queda en inventario / no-vendibles). Mucho menos = '
             'revisar denominadores o clasificación de cuentas.')
    notes = fields.Text()

    _sql_constraints = [
        ('period_company_uniq', 'unique(period, company_id)',
         'Ya existen factores para ese período.'),
    ]


class QbCostoProducto(models.Model):
    _name = 'qb.costo.producto'
    _description = 'Costo real por producto y período'
    _order = 'period DESC, qty_vendida DESC'
    _rec_name = 'product_id'

    period = fields.Date(required=True, index=True)
    product_id = fields.Many2one(
        'product.product', required=True, index=True, ondelete='cascade')
    default_code = fields.Char(
        related='product_id.default_code', store=True, string='Referencia')
    company_id = fields.Many2one(
        'res.company', default=lambda self: self.env.company, required=True)
    product_bucket = fields.Char(string='Clasificación')
    uom_name = fields.Char(string='UoM')
    kg_per_unit = fields.Float(digits=(16, 6))
    m_per_kg = fields.Float(digits=(16, 6))
    qty_vendida = fields.Float(string='Qty vendida (período)')
    precio_prom = fields.Float(
        string='Precio promedio',
        help='Revenue ÷ qty deduplicada por el triplete lista/descuento/neta '
             '(DISTINCT ON move, product, qty) — sin el dedup el precio '
             'saldría ~1/3 en productos con triplete.')
    mp_unit = fields.Float(string='MP $/u', digits=(16, 4))
    energia_unit = fields.Float(string='Energía $/u', digits=(16, 4))
    costo_variable = fields.Float(string='Costo variable $/u', digits=(16, 4))
    fab_unit = fields.Float(string='Fabricación $/u', digits=(16, 4))
    op_unit = fields.Float(string='Operación $/u', digits=(16, 4))
    costo_absorbido = fields.Float(string='Costo absorbido $/u', digits=(16, 4))
    margen_contribucion = fields.Float(string='Contribución $/u', digits=(16, 4))
    margen_contribucion_pct = fields.Float(string='Contribución %')
    margen_absorbido = fields.Float(string='Margen absorbido $/u', digits=(16, 4))
    margen_absorbido_pct = fields.Float(string='Margen absorbido %')
    contrib_hora_maquina = fields.Float(
        string='Contribución $/hora-máquina',
        help='Margen de contribución ÷ horas-máquina por unidad en el centro '
             'más lento de su ruta. Para rankear productos cuando la '
             'capacidad es el límite.')
    contrib_total = fields.Float(
        string='Contribución total (período)',
        help='Contribución unitaria × qty vendida: cuánto aportó a fijos '
             'este producto en el mes.')
    alerta = fields.Selection([
        ('bajo_variable', 'Vendido bajo costo variable'),
        ('bajo_absorbido', 'No cubre costo absorbido'),
        ('sin_peso', 'Sin peso resuelto'),
        ('ok', 'OK'),
    ], string='Alerta',
        help='bajo_variable = destruye valor (rojo); bajo_absorbido = aporta '
             'a fijos pero no cubre todo (ámbar); sin_peso = falta el peso '
             'kg/u, la fabricación no se puede repartir.')
    centro_route = fields.Char(string='Ruta (centros)')
    factores_id = fields.Many2one('qb.costo.factores', string='Factores usados')

    _sql_constraints = [
        ('period_product_uniq', 'unique(period, product_id, company_id)',
         'Ya existe el costo de ese producto para ese período.'),
    ]

    # ------------------------------------------------------------------
    # Pools GL
    # ------------------------------------------------------------------
    @api.model
    def _pool_by_month(self, buckets, date_from, date_to,
                       es_variable=None, centro_id=None, sign=1.0):
        """Σ balance de las cuentas clasificadas en `buckets`, por mes.

        Devuelve {date_mes: monto}. sign=-1 para ingresos (saldo acreedor).
        """
        query = """
            WITH cuenta_map AS (%s)
            SELECT date_trunc('month', aml.date)::date AS mes,
                   SUM(aml.balance * m.allocation_pct / 100.0) AS monto
            FROM account_move_line aml
            JOIN cuenta_map m ON m.account_id = aml.account_id
            WHERE m.bucket IN %%s
              AND aml.parent_state = 'posted'
              AND aml.date >= %%s AND aml.date < %%s
              AND aml.company_id = %%s
        """ % CUENTA_MAP_SQL
        params = [tuple(buckets), date_from, date_to, self.env.company.id]
        if es_variable is not None:
            query += ' AND COALESCE(m.es_variable, FALSE) = %s'
            params.append(es_variable)
        if centro_id is not None:
            query += ' AND m.centro_id = %s'
            params.append(centro_id)
        query += ' GROUP BY 1'
        self.env.cr.execute(query, tuple(params))
        return {row[0]: sign * row[1] for row in self.env.cr.fetchall()}

    @api.model
    def _smooth(self, by_month, exclude_nonpositive=True):
        """Promedio de meses válidos (guard pool>0: excluye reversos de
        cierre anual que meterían meses negativos/cero a la media)."""
        values = [v for v in by_month.values()
                  if not exclude_nonpositive or v > 0]
        return sum(values) / len(values) if values else 0.0

    # ------------------------------------------------------------------
    # Denominadores de producción (kg / m)
    # ------------------------------------------------------------------
    @api.model
    def _production_month_avg(self, centros, date_from, date_to):
        """Producción promedio mensual de un conjunto de centros: vía sus
        workcenters (mrp.workorder) o su mo_name_pattern (mrp.production)."""
        if not centros:
            return 0.0
        # Promediar SOLO los meses con producción: si los workcenters
        # arrancaron a media ventana (tejido: mayo 2026), dividir entre la
        # ventana completa subestima el denominador ×4 e infla energía y
        # factores de fabricación en la misma proporción.
        by_month = {}
        wc_ids = centros.mapped('workcenter_ids').ids
        if wc_ids:
            self.env.cr.execute("""
                SELECT date_trunc('month', wo.date_finished)::date,
                       COALESCE(SUM(%s), 0)
                FROM mrp_workorder wo
                WHERE wo.workcenter_id IN %%s AND wo.state = 'done'
                  AND wo.date_finished >= %%s AND wo.date_finished < %%s
                GROUP BY 1
            """ % wo_qty_sql(self.env), (tuple(wc_ids), date_from, date_to))
            for mes, qty in self.env.cr.fetchall():
                by_month[mes] = by_month.get(mes, 0.0) + (qty or 0.0)
        for centro in centros.filtered(
                lambda c: c.mo_name_pattern and not c.workcenter_ids):
            self.env.cr.execute("""
                SELECT date_trunc('month', mp.date_finished)::date,
                       COALESCE(SUM(%s), 0)
                FROM mrp_production mp
                WHERE mp.name LIKE %%s AND mp.state = 'done'
                  AND mp.date_finished >= %%s AND mp.date_finished < %%s
                GROUP BY 1
            """ % mo_qty_sql(self.env),
                (centro.mo_name_pattern, date_from, date_to))
            for mes, qty in self.env.cr.fetchall():
                by_month[mes] = by_month.get(mes, 0.0) + (qty or 0.0)
        activos = [q for q in by_month.values() if q > 0]
        return sum(activos) / len(activos) if activos else 0.0

    # ------------------------------------------------------------------
    # Factores del período
    # ------------------------------------------------------------------
    @api.model
    def _compute_factores(self, period):
        Config = self.env['qb.costeo.factor.config']
        Centro = self.env['qb.costeo.centro']
        window = int(Config.get_param('smoothing_months', 12)) or 12
        date_to = period + relativedelta(months=1)
        date_from = date_to - relativedelta(months=window)

        fab_by_month = self._pool_by_month(FAB_BUCKETS, date_from, date_to,
                                           es_variable=False)
        energia_by_month = self._pool_by_month(('energia',), date_from, date_to)
        op_by_month = self._pool_by_month(('operacion',), date_from, date_to)
        ventas_by_month = self._pool_by_month(('ventas',), date_from, date_to,
                                              sign=-1.0)

        # Entretelas: su MOD del GL + renta contractual + extra configurable.
        # Se RESTA del pool de tela (split quirúrgico) y forma su factor $/m.
        ent_centros = Centro.search([('driver_principal', '=', 'largo'),
                                     ('nature', '=', 'fabril_directo'),
                                     ('code', 'ilike', 'ENTRETELA')])
        entretela_pool = 0.0
        entretela_m = 0.0
        if ent_centros:
            ent_mod = self._smooth(self._pool_by_month(
                ('mod',), date_from, date_to, centro_id=ent_centros[0].id))
            entretela_pool = (
                ent_mod
                + sum(ent_centros.mapped('renta_contractual_mxn'))
                + Config.get_param('entretela_overhead_extra_mxn', 0.0))
            entretela_m = self._production_month_avg(ent_centros, date_from, date_to)

        fab_pool = max(self._smooth(fab_by_month) - entretela_pool, 0.0)
        energia_pool = self._smooth(energia_by_month)
        op_pool = self._smooth(op_by_month)
        ventas_pool = self._smooth(ventas_by_month)

        kg_centros = Centro.search([('es_denominador_kg', '=', True)])
        m_centros = Centro.search([('es_denominador_m', '=', True)])
        kg_denom = (Config.get_param('denominador_kg_override', 0.0)
                    or self._production_month_avg(kg_centros, date_from, date_to))
        m_denom = (Config.get_param('denominador_m_override', 0.0)
                   or self._production_month_avg(m_centros, date_from, date_to))

        ws = Config.get_param('fab_weight_share', 0.67)
        factor_fab_kg = ws * fab_pool / kg_denom if kg_denom else 0.0
        factor_fab_m = (1 - ws) * fab_pool / m_denom if m_denom else 0.0
        energia_por_kg = (Config.get_param('energia_por_kg', 0.0)
                          or (energia_pool / kg_denom if kg_denom else 0.0))
        op_pct = (Config.get_param('op_pct_override', 0.0)
                  or (op_pool / ventas_pool if ventas_pool else 0.0))
        entretela_factor = entretela_pool / entretela_m if entretela_m else 0.0

        Factores = self.env['qb.costo.factores']
        vals = {
            'period': period,
            'window_months': window,
            'fab_pool_month': fab_pool,
            'energia_pool_month': energia_pool,
            'op_pool_month': op_pool,
            'ventas_pool_month': ventas_pool,
            'entretela_pool_month': entretela_pool,
            'kg_denom_month': kg_denom,
            'm_denom_month': m_denom,
            'entretela_m_denom_month': entretela_m,
            'fab_weight_share': ws,
            'factor_fab_kg': factor_fab_kg,
            'factor_fab_m': factor_fab_m,
            'energia_por_kg': energia_por_kg,
            'op_pct': op_pct,
            'entretela_factor_m': entretela_factor,
        }
        existing = Factores.search([
            ('period', '=', period),
            ('company_id', '=', self.env.company.id)], limit=1)
        if existing:
            existing.write(vals)
            return existing
        return Factores.create(vals)

    # ------------------------------------------------------------------
    # MP: último costo de compra, explosión recursiva
    # ------------------------------------------------------------------
    @api.model
    def _last_purchase_line_map(self, product_ids):
        """{product_id: purchase_order_line_id} de la última compra confirmada,
        resuelto en UN query (DISTINCT ON). Un search por hoja de BOM no
        escala: con 3k SKUs × BOMs recursivas son decenas de miles de queries.
        """
        if not product_ids:
            return {}
        # state/date_order viven en purchase_order (en Odoo 19 ya no son
        # columnas de la línea) — joinear la orden.
        self.env.cr.execute("""
            SELECT DISTINCT ON (pol.product_id) pol.product_id, pol.id
            FROM purchase_order_line pol
            JOIN purchase_order po ON po.id = pol.order_id
            WHERE po.state IN ('purchase', 'done')
              AND pol.price_unit > 0
              AND pol.product_id = ANY(%s)
            ORDER BY pol.product_id, po.date_order DESC, pol.id DESC
        """, (list(product_ids),))
        return dict(self.env.cr.fetchall())

    @api.model
    def _last_purchase_cost(self, product, pol_map=None):
        """Último precio de compra confirmado, en moneda de la compañía.

        Con `pol_map` (de _last_purchase_line_map) no hace ningún search;
        sin él (cotizador, llamadas sueltas) busca la línea individual.
        """
        if pol_map is not None:
            pol_id = pol_map.get(product.id)
            pol = self.env['purchase.order.line'].browse(pol_id) if pol_id \
                else self.env['purchase.order.line']
        else:
            # order_id.state en el domain y orden por id (proxy de recencia):
            # state/date_order de la línea no son columnas propias en Odoo 19.
            pol = self.env['purchase.order.line'].search([
                ('product_id', '=', product.id),
                ('order_id.state', 'in', ('purchase', 'done')),
                ('price_unit', '>', 0),
            ], order='id desc', limit=1)
        if not pol:
            return product.standard_price or 0.0
        price = pol.price_unit * (1 - (pol.discount or 0.0) / 100.0)
        company = self.env.company
        # Moneda y fecha desde la orden: en Odoo 19 no son columnas de la línea
        currency = pol.order_id.currency_id
        date_order = pol.order_id.date_order
        if currency and currency != company.currency_id:
            price = currency._convert(
                price, company.currency_id, company,
                date_order and date_order.date() or fields.Date.today())
        # Normalizar a la UoM del producto (campo renombrado en Odoo 17+)
        pol_uom = pol.product_uom_id if 'product_uom_id' in pol._fields \
            else pol.product_uom
        if pol_uom and pol_uom != product.uom_id:
            qty_in_product_uom = pol_uom._compute_quantity(
                1.0, product.uom_id, round=False)
            if qty_in_product_uom:
                price = price / qty_in_product_uom
        return price

    @api.model
    def _engine_ctx(self, product_ids=None):
        """Contexto de una corrida del motor: todo lo que se resuelve UNA vez
        y se comparte en el loop (reglas de ruteo, mapa de últimas compras,
        cachés de peso y MP). Mantiene el motor O(productos), no O(queries).
        """
        leaf_ids = set(product_ids or [])
        # Todas las hojas posibles de las recetas, en un query
        self.env.cr.execute(
            'SELECT DISTINCT product_id FROM mrp_bom_line WHERE product_id IS NOT NULL')
        leaf_ids.update(r[0] for r in self.env.cr.fetchall())
        pol_map = self._last_purchase_line_map(leaf_ids)
        # Warm-up del cache ORM: un solo fetch para las líneas y sus órdenes
        if pol_map:
            pols = self.env['purchase.order.line'].browse(list(pol_map.values()))
            pols.read(['price_unit', 'discount', 'order_id'])
            pols.order_id.read(['currency_id', 'date_order'])
        return {
            'rules': self.env['qb.producto.ruteo'].search([]),
            'pol_map': pol_map,
            'multi_bom_ids': self._multi_bom_ids_set(),
            'peso_cache': {},
            'mp_cache': {},
        }

    @api.model
    def _mp_cost_unit(self, product, cache=None, seen=None, ctx=None):
        """Costo primo MP por unidad: BOM recursiva a último costo.

        Reglas: subproducto → $0 (su MP ya está en la receta del principal);
        importado → landed (avg de Odoo), sin costo propio → gemelo nacional;
        receta AMBIGUA (>1 BOM activa) con AVCO válido → costo AVCO de Odoo
        (evita colapsar el costo eligiendo una receta al azar);
        hoja sin BOM → último costo de compra (fallback avg).

        `ctx` (de _engine_ctx) comparte reglas/pol_map/cachés en loops
        grandes; sin él (cotizador, tests) resuelve todo al vuelo.
        """
        cache = cache if cache is not None \
            else (ctx['mp_cache'] if ctx else {})
        seen = seen if seen is not None else set()
        if product.id in cache:
            return cache[product.id]
        if product.id in seen:  # ciclo en la receta: cortar con avg
            return product.standard_price or 0.0
        seen = seen | {product.id}
        rules = ctx['rules'] if ctx else None
        pol_map = ctx['pol_map'] if ctx else None

        bucket, _centros = self.env['qb.producto.ruteo'].resolve(product, rules)
        ref = product.default_code or ''
        cost = 0.0
        if bucket == 'subproducto':
            cost = 0.0
        elif bucket == 'importado' or ref.endswith(' I'):
            cost = product.standard_price \
                or self._last_purchase_cost(product, pol_map)
            if not cost and ref.endswith(' I'):
                twin = self.env['product.product'].search(
                    [('default_code', '=', ref[:-2].strip())], limit=1)
                if twin:
                    cost = self._mp_cost_unit(twin, cache, seen, ctx)
        else:
            std = product.standard_price or 0.0
            if std > 0.0 and self._has_multiple_boms(product, ctx):
                # Receta AMBIGUA: el producto tiene VARIAS BOMs activas (típico
                # de semiterminados genéricos "MUESTRA PILOTO", que llegan a
                # tener 26 recetas). _bom_find elegiría una al azar y el costo
                # se colapsa (bug WD080: MP $0.13 en vez de ~$12). En vez de
                # explotar una receta arbitraria, usa el AVCO que Odoo ya
                # mantiene para el intermedio: es nativo y confiable.
                cost = std
            else:
                bom = self.env['mrp.bom']._bom_find(product).get(product)
                if bom:
                    total = 0.0
                    for line in bom.bom_line_ids:
                        comp = line.product_id
                        qty = line.product_uom_id._compute_quantity(
                            line.product_qty, comp.uom_id, round=False)
                        total += qty * self._mp_cost_unit(comp, cache, seen, ctx)
                    bom_qty = bom.product_uom_id._compute_quantity(
                        bom.product_qty, product.uom_id, round=False) or 1.0
                    cost = total / bom_qty
                else:
                    cost = self._last_purchase_cost(product, pol_map)
        cache[product.id] = cost
        return cost

    @api.model
    def _multi_bom_ids_set(self):
        """product.product.id con MÁS DE UNA BOM activa aplicable (receta
        ambigua). Una sola query para todo el motor; se cachea en el ctx."""
        self.env.cr.execute("""
            SELECT p.id
            FROM product_product p
            JOIN mrp_bom b
              ON b.active
             AND (b.product_id = p.id
                  OR (b.product_id IS NULL
                      AND b.product_tmpl_id = p.product_tmpl_id))
            GROUP BY p.id
            HAVING count(*) > 1
        """)
        return {r[0] for r in self.env.cr.fetchall()}

    @api.model
    def _has_multiple_boms(self, product, ctx=None):
        """¿El producto tiene receta ambigua (>1 BOM activa)? Con ctx usa el
        set precomputado (O(1)); sin él (cotizador) hace un search_count."""
        if ctx is not None and 'multi_bom_ids' in ctx:
            return product.id in ctx['multi_bom_ids']
        return self.env['mrp.bom'].search_count([
            '|', ('product_id', '=', product.id),
            '&', ('product_id', '=', False),
                 ('product_tmpl_id', '=', product.product_tmpl_id.id),
        ]) > 1

    # ------------------------------------------------------------------
    # Ventas del período (qty deduplicada + precio promedio)
    # ------------------------------------------------------------------
    @api.model
    def _sales_by_product(self, period):
        """{product_id: (qty, revenue)} del período, con dedup del triplete
        (lista+/descuento−/neta+) para qty; revenue suma las 3 líneas
        (cancelan aritméticamente). out_refund resta."""
        date_to = period + relativedelta(months=1)
        self.env.cr.execute("""
            WITH lines AS (
                SELECT aml.move_id, aml.product_id, aml.quantity,
                       aml.price_subtotal, am.move_type
                FROM account_move_line aml
                JOIN account_move am ON am.id = aml.move_id
                WHERE am.move_type IN ('out_invoice', 'out_refund')
                  AND am.state = 'posted'
                  AND aml.display_type = 'product'
                  AND aml.product_id IS NOT NULL
                  AND am.invoice_date >= %s AND am.invoice_date < %s
                  AND aml.company_id = %s
            ),
            qty_dedup AS (
                SELECT DISTINCT ON (move_id, product_id, ABS(quantity))
                       move_id, product_id, quantity, move_type
                FROM lines
                ORDER BY move_id, product_id, ABS(quantity)
            )
            SELECT l.product_id,
                   COALESCE((SELECT SUM(CASE WHEN q.move_type = 'out_refund'
                                             THEN -q.quantity ELSE q.quantity END)
                             FROM qty_dedup q WHERE q.product_id = l.product_id), 0) AS qty,
                   SUM(CASE WHEN l.move_type = 'out_refund'
                            THEN -l.price_subtotal ELSE l.price_subtotal END) AS revenue
            FROM lines l
            GROUP BY l.product_id
        """, (period, date_to, self.env.company.id))
        return {row[0]: (row[1] or 0.0, row[2] or 0.0)
                for row in self.env.cr.fetchall()}

    # ------------------------------------------------------------------
    # Recompute
    # ------------------------------------------------------------------
    @api.model
    def action_recompute_period(self, period=None):
        """Recalcula factores + costo por producto para un período (mes).

        Default: mes anterior completo. Idempotente: upsert por
        (period, product).
        """
        if not period:
            today = fields.Date.today()
            period = date(today.year, today.month, 1) - relativedelta(months=1)
        elif isinstance(period, str):
            period = fields.Date.from_string(period)
        period = date(period.year, period.month, 1)

        factores = self._compute_factores(period)
        sales = self._sales_by_product(period)

        Product = self.env['product.product']
        product_ids = set(sales.keys())
        # También productos vendibles con BOM (aunque no se vendieran el mes)
        boms = self.env['mrp.bom'].search([('active', '=', True)])
        for bom in boms:
            products = (bom.product_id
                        or bom.product_tmpl_id.product_variant_ids)
            product_ids.update(products.filtered('sale_ok').ids)

        Ruteo = self.env['qb.producto.ruteo']
        Peso = self.env['qb.producto.peso']
        ctx = self._engine_ctx(product_ids)
        existing = {r.product_id.id: r for r in self.search(
            [('period', '=', period),
             ('company_id', '=', self.env.company.id)])}
        fab_absorbida_total = 0.0
        to_create = []
        errores = 0

        products = Product.browse(list(product_ids)).exists()
        # Prefetch en bloque: evita un fetch por producto dentro del loop
        products.read(['default_code', 'standard_price', 'weight'])
        for product in products:
            try:
                vals, fab_x_qty = self._compute_product_vals(
                    product, period, factores, sales, ctx, Ruteo, Peso)
            except Exception:
                # Una BOM rota (ciclo raro, UoM inconsistente) no debe tumbar
                # el cálculo mensual completo: se loggea y se sigue.
                errores += 1
                _logger.exception(
                    'qb.costo.producto: error costeando %s — se omite',
                    product.display_name)
                continue
            fab_absorbida_total += fab_x_qty
            rec = existing.get(product.id)
            if rec:
                rec.write(vals)
            else:
                to_create.append(vals)
        if to_create:
            self.create(to_create)

        if factores.fab_pool_month:
            factores.cobertura_fab_pct = (
                100.0 * fab_absorbida_total
                / (factores.fab_pool_month + factores.entretela_pool_month))
        _logger.info(
            'qb.costo.producto: %s productos para %s (%s nuevos, %s errores, '
            'cobertura fab %.1f%%)',
            len(products), period, len(to_create), errores,
            factores.cobertura_fab_pct)
        return True

    @api.model
    def _compute_product_vals(self, product, period, factores, sales, ctx,
                              Ruteo, Peso):
        """Vals de qb.costo.producto para UN producto. Devuelve
        (vals, fab_absorbida × qty) para el acumulado de cobertura."""
        bucket, centros = Ruteo.resolve(product, ctx['rules'])
        kg = Peso.resolve_kg_per_unit(product, ctx['peso_cache'])
        m_per_kg = Peso.resolve_m_per_kg(product, ctx['peso_cache'])
        uom_name = (product.uom_id.name or '').lower()
        is_kg = uom_name in KG_UOM_NAMES
        qty, revenue = sales.get(product.id, (0.0, 0.0))
        precio = revenue / qty if qty else 0.0

        mp = self._mp_cost_unit(product, ctx=ctx)
        energia = 0.0 if bucket in ('importado', 'subproducto') \
            else factores.energia_por_kg * kg
        fab = self._fab_unit(bucket, is_kg, kg, m_per_kg, factores)
        op = factores.op_pct * precio
        variable = mp + energia
        absorbido = variable + fab + op
        contrib = precio - variable
        hours_per_unit = self._hours_per_unit(centros, is_kg, kg, m_per_kg)

        if qty and precio and precio < variable:
            alerta = 'bajo_variable'
        elif qty and precio and precio < absorbido:
            alerta = 'bajo_absorbido'
        elif not kg and not is_kg and bucket in (
                'tela', 'entretela_tejida', 'entretela_carda'):
            alerta = 'sin_peso'
        else:
            alerta = 'ok'

        vals = {
            'period': period,
            'product_id': product.id,
            'product_bucket': bucket,
            'uom_name': product.uom_id.name,
            'kg_per_unit': kg,
            'm_per_kg': m_per_kg,
            'qty_vendida': qty,
            'precio_prom': precio,
            'mp_unit': mp,
            'energia_unit': energia,
            'costo_variable': variable,
            'fab_unit': fab,
            'op_unit': op,
            'costo_absorbido': absorbido,
            'margen_contribucion': contrib if precio else 0.0,
            'margen_contribucion_pct':
                100.0 * contrib / precio if precio else 0.0,
            'margen_absorbido': precio - absorbido if precio else 0.0,
            'margen_absorbido_pct':
                100.0 * (precio - absorbido) / precio if precio else 0.0,
            'contrib_hora_maquina':
                contrib / hours_per_unit if hours_per_unit and precio else 0.0,
            'contrib_total': contrib * qty if precio else 0.0,
            'alerta': alerta,
            'centro_route': ', '.join(centros.mapped('code')),
            'factores_id': factores.id,
        }
        return vals, fab * qty

    @api.model
    def _fab_unit(self, bucket, is_kg, kg, m_per_kg, factores):
        """Fabricación por unidad según familia y unidad de venta."""
        f_kg = factores.factor_fab_kg
        f_m = factores.factor_fab_m
        f_ent = factores.entretela_factor_m
        if bucket == 'tela':
            if is_kg:
                return f_kg + m_per_kg * f_m
            return kg * f_kg + f_m
        if bucket == 'entretela_tejida':
            # Pasa por tejido+tintorería (peso) + su proceso de puntos ($/m)
            if is_kg:
                return f_kg + m_per_kg * f_ent
            return kg * f_kg + f_ent
        if bucket == 'entretela_carda':
            return m_per_kg * f_ent if is_kg else f_ent
        # importado / subproducto / servicio: no cargan fabricación
        return 0.0

    @api.model
    def _hours_per_unit(self, centros, is_kg, kg, m_per_kg):
        """Horas-máquina por unidad en el centro MÁS LENTO de la ruta
        (la etapa que amarra). Para contribución por hora-máquina."""
        worst = 0.0
        for centro in centros:
            std = centro.std_output_per_hour
            if not std:
                continue
            if centro.driver_principal == 'peso':
                units_kg = 1.0 if is_kg else kg
                hours = units_kg / std
            else:
                units_m = m_per_kg if is_kg else 1.0
                hours = units_m / std
            worst = max(worst, hours)
        return worst

    # ------------------------------------------------------------------
    # Cotización puntual (una sola fuente de matemáticas para todos los
    # wizards: individual y por orden completa)
    # ------------------------------------------------------------------
    @api.model
    def quote_product(self, product, factores=None):
        """Costo por capa y precios de UN producto con los factores vigentes.

        Fórmulas (op% va SOBRE VENTA, por eso divide):
          variable        = MP + energía
          piso_ocioso     = variable
          piso_lleno      = (variable + fab) / (1 − op)
          precio_mercado  = promedio real facturado 12m (todos los clientes)

        Sin "margen meta": el ancla para cotizar no es una aspiración — son
        los pisos (debajo de qué no bajar) y el mercado (qué se está
        logrando de verdad).
        """
        Peso = self.env['qb.producto.peso']
        if factores is None:
            factores = self.env['qb.costo.factores'].search(
                [], order='period DESC', limit=1)
        if not factores:
            return None
        bucket, centros = self.env['qb.producto.ruteo'].resolve(product)
        kg = Peso.resolve_kg_per_unit(product)
        m_per_kg = Peso.resolve_m_per_kg(product)
        is_kg = (product.uom_id.name or '').lower() in KG_UOM_NAMES
        mp = self._mp_cost_unit(product)
        energia = 0.0 if bucket in ('importado', 'subproducto') \
            else factores.energia_por_kg * kg
        fab = self._fab_unit(bucket, is_kg, kg, m_per_kg, factores)
        variable = mp + energia
        op = factores.op_pct
        piso_lleno = (variable + fab) / (1.0 - op) if op < 1 else 0.0
        hours = self._hours_per_unit(centros, is_kg, kg, m_per_kg)
        return {
            'bucket': bucket, 'centros': centros, 'kg': kg,
            'm_per_kg': m_per_kg, 'is_kg': is_kg,
            'mp': mp, 'energia': energia, 'fab': fab, 'variable': variable,
            'op_pct': op,
            'piso_ocioso': variable, 'piso_lleno': piso_lleno,
            'precio_mercado': self.market_price(product),
            'hours_per_unit': hours,
            'factores': factores,
        }

    @api.model
    def market_price(self, product, months=12):
        """Precio promedio REAL al que se facturó el producto en la ventana
        (todos los clientes, MXN, dedup del triplete). 0 = sin ventas."""
        rows = self.sales_by_customer(product, months)
        qty = sum(r['qty'] for r in rows)
        rev = sum(r['revenue_mxn'] for r in rows)
        return rev / qty if qty else 0.0

    @api.model
    def monthly_sales_volume(self, product, partner=None, months=12):
        """Volumen mensual histórico del producto (opcionalmente por cliente).

        Devuelve (qty_promedio_mensual, meses_con_compra, months). El
        promedio es sobre los meses CON compra — para un producto de línea
        o proyecto en curso, eso es el run-rate real del cliente. Dedup del
        triplete (DISTINCT ON move/product/|qty|) como en todo el módulo.
        """
        date_to = fields.Date.today().replace(day=1)
        date_from = date_to - relativedelta(months=months)
        params = [product.id, date_from, date_to, self.env.company.id]
        partner_clause = ''
        if partner:
            partner_clause = 'AND am.commercial_partner_id = %s'
            params.append(partner.commercial_partner_id.id)
        self.env.cr.execute("""
            WITH lines AS (
                SELECT DISTINCT ON (aml.move_id, aml.product_id, ABS(aml.quantity))
                       aml.move_id, aml.quantity, am.move_type, am.invoice_date
                FROM account_move_line aml
                JOIN account_move am ON am.id = aml.move_id
                WHERE am.move_type IN ('out_invoice', 'out_refund')
                  AND am.state = 'posted'
                  AND aml.display_type = 'product'
                  AND aml.product_id = %%s
                  AND am.invoice_date >= %%s AND am.invoice_date < %%s
                  AND aml.company_id = %%s
                  %s
                ORDER BY aml.move_id, aml.product_id, ABS(aml.quantity)
            )
            SELECT date_trunc('month', invoice_date)::date,
                   SUM(CASE WHEN move_type = 'out_refund'
                            THEN -quantity ELSE quantity END)
            FROM lines GROUP BY 1
        """ % partner_clause, tuple(params))
        by_month = [q for _mes, q in self.env.cr.fetchall() if q and q > 0]
        if not by_month:
            return 0.0, 0, months
        return sum(by_month) / len(by_month), len(by_month), months

    @api.model
    def sales_by_customer(self, product, months=12):
        """¿A cuánto se vende HOY este producto, cliente por cliente?

        Devuelve filas ordenadas por venta:
        {partner (res.partner), qty, qty_mes, meses, ultima, revenue_mxn,
         precio_mxn, currency (nombre si facturó en divisa), precio_divisa}

        Precio MXN desde aml.balance (moneda de la compañía): un cliente
        facturado en USD sale con su precio real en pesos, no con el número
        en dólares crudo. Dedup del triplete como en todo el módulo.
        """
        date_from = fields.Date.today() - relativedelta(months=months)
        self.env.cr.execute("""
            WITH lines AS (
                SELECT aml.move_id, aml.quantity, aml.price_subtotal,
                       aml.balance, am.move_type, am.invoice_date,
                       am.commercial_partner_id, am.currency_id
                FROM account_move_line aml
                JOIN account_move am ON am.id = aml.move_id
                WHERE am.move_type IN ('out_invoice', 'out_refund')
                  AND am.state = 'posted'
                  AND aml.display_type = 'product'
                  AND aml.product_id = %s
                  AND am.invoice_date >= %s
                  AND aml.company_id = %s
            ),
            qty_dedup AS (
                SELECT DISTINCT ON (move_id, ABS(quantity))
                       move_id, commercial_partner_id, invoice_date,
                       CASE WHEN move_type = 'out_refund'
                            THEN -quantity ELSE quantity END AS qty
                FROM lines
                ORDER BY move_id, ABS(quantity)
            ),
            qty_agg AS (
                SELECT commercial_partner_id,
                       SUM(qty) AS qty,
                       COUNT(DISTINCT date_trunc('month', invoice_date)) AS meses,
                       MAX(invoice_date) AS ultima
                FROM qty_dedup GROUP BY 1
            ),
            rev AS (
                SELECT commercial_partner_id,
                       SUM(-balance) AS revenue_mxn,
                       SUM(CASE WHEN move_type = 'out_refund'
                                THEN -price_subtotal
                                ELSE price_subtotal END) AS revenue_doc,
                       MIN(currency_id) AS cur_min,
                       MAX(currency_id) AS cur_max
                FROM lines GROUP BY 1
            )
            SELECT q.commercial_partner_id, q.qty, q.meses, q.ultima,
                   r.revenue_mxn, r.revenue_doc, r.cur_min, r.cur_max
            FROM qty_agg q
            JOIN rev r USING (commercial_partner_id)
            ORDER BY r.revenue_mxn DESC
        """, (product.id, date_from, self.env.company.id))
        company_cur = self.env.company.currency_id
        rows = []
        for pid, qty, meses, ultima, rev_mxn, rev_doc, cmin, cmax in \
                self.env.cr.fetchall():
            qty = qty or 0.0
            currency = None
            precio_divisa = 0.0
            if cmin and cmin == cmax and cmin != company_cur.id:
                currency = self.env['res.currency'].browse(cmin).name
                precio_divisa = (rev_doc or 0.0) / qty if qty else 0.0
            rows.append({
                'partner': self.env['res.partner'].browse(pid),
                'qty': qty,
                'qty_mes': qty / meses if meses else 0.0,
                'meses': meses or 0,
                'ultima': ultima,
                'revenue_mxn': rev_mxn or 0.0,
                'precio_mxn': (rev_mxn or 0.0) / qty if qty else 0.0,
                'currency': currency,
                'precio_divisa': precio_divisa,
            })
        return rows

    @api.model
    def related_presentations(self, product):
        """Otras presentaciones/variantes del MISMO artículo, por nomenclatura:

        - Prefijo 'I' = el mismo tejido vendido en KILOS
          (WJ038Q22JNT160 ↔ IWJ038Q22JNT160).
        - Sufijo ' I' = versión IMPORTADA del mismo artículo.

        Devuelve [(product, etiqueta)]. Solo empareja si la referencia
        hermana existe tal cual — sin adivinar.
        """
        Product = self.env['product.product']
        ref = (product.default_code or '').strip()
        if not ref:
            return []
        seen = {product.id}
        out = []

        def add(code, label):
            if not code:
                return
            for p in Product.search([('default_code', '=', code),
                                     ('id', 'not in', list(seen))]):
                seen.add(p.id)
                out.append((p, label))

        if ref.endswith(' I'):
            base = ref[:-2].strip()
            add(base, 'Gemelo nacional (mismo artículo, fabricado aquí)')
        else:
            base = ref
            add(base + ' I', 'Versión importada del mismo artículo')
        if base.startswith('I'):
            add(base[1:], 'El mismo artículo vendido en METROS')
        else:
            add('I' + base, 'El mismo artículo vendido en KILOS')
        return out

    @api.model
    def comparativa_html(self, product, factores=None, partner=None,
                         max_clientes=10):
        """La comparativa que pide dirección al cotizar: ¿a cuánto vendo YA
        este producto a otros clientes, y a cuánto sus otras presentaciones
        (metros vs kilos, nacional vs importado) — y qué margen deja cada
        una a su precio de venta actual?

        Márgenes evaluados con el costo VIGENTE (factores de hoy), no el del
        mes en que se facturó: la pregunta es "si hoy vendo a ese precio,
        ¿qué gano?". `partner` (commercial) resalta al cliente cotizado.
        """
        if factores is None:
            factores = self.env['qb.costo.factores'].search(
                [], order='period DESC', limit=1)
        if not factores:
            return False
        emoji = {'rojo': '🔴', 'ambar': '🟡', 'verde': '🟢', False: ''}

        def margenes(q, precio):
            """(contribución %, neto %, semáforo) al precio dado, MXN."""
            if not precio:
                return 0.0, 0.0, False
            contrib = 100.0 * (precio - q['variable']) / precio
            neto = (100.0 * (precio - q['variable'] - q['fab']) / precio
                    - 100.0 * q['op_pct'])
            return contrib, neto, self.semaforo_for(
                precio, q['piso_ocioso'], q['piso_lleno'])

        q_prod = self.quote_product(product, factores)
        uom = product.uom_id.name or 'u'

        # ---- A. Por cliente ----
        rows = self.sales_by_customer(product)
        html = (
            '<h5>💲 ¿A cuánto vendo HOY este producto? '
            '<span style="font-weight:normal;">(últimos 12 meses — precios '
            'promedio realmente facturados, en MXN)</span></h5>')
        if not rows:
            html += ('<p style="font-size:12px;">Sin ventas de este producto '
                     'en los últimos 12 meses: no hay precio de referencia '
                     'de otros clientes.</p>')
        else:
            html += (
                '<p style="font-size:12px;" class="text-muted">Márgenes '
                'evaluados con el costo VIGENTE: «si hoy le vendo a ese '
                'precio, ¿qué gano?». Contribución = (precio − costo '
                'variable) ÷ precio; neto = después de fabricación y '
                'operación.</p>'
                '<table class="table table-sm" style="font-size:12px;">'
                '<thead><tr><th>Cliente</th>'
                '<th class="text-end">Meses c/compra</th>'
                '<th class="text-end">Vol. prom/mes</th>'
                '<th class="text-end">Precio prom $/%s MXN</th>'
                '<th class="text-end">Contribución %%</th>'
                '<th class="text-end">Margen neto %%</th>'
                '<th>🚦</th></tr></thead><tbody>' % html_escape(uom))
            partner_id = partner.id if partner else None
            resto = rows[max_clientes:]
            for r in rows[:max_clientes]:
                contrib, neto, sem = margenes(q_prod, r['precio_mxn'])
                es_actual = r['partner'].id == partner_id
                divisa = (' <span class="text-muted">(facturado en %s: '
                          '%.2f %s/%s)</span>'
                          % (r['currency'], r['precio_divisa'],
                             r['currency'], html_escape(uom))
                          if r['currency'] else '')
                html += (
                    '<tr%s><td>%s%s</td><td class="text-end">%s</td>'
                    '<td class="text-end">%s</td>'
                    '<td class="text-end">$%.2f%s</td>'
                    '<td class="text-end">%.1f%%</td>'
                    '<td class="text-end">%.1f%%</td><td>%s</td></tr>'
                    % (' style="background:#fff3cd;font-weight:bold;"'
                       if es_actual else '',
                       html_escape(r['partner'].name or '?'),
                       ' ← este cliente' if es_actual else '',
                       r['meses'], f"{r['qty_mes']:,.0f}",
                       r['precio_mxn'], divisa, contrib, neto,
                       emoji.get(sem, '')))
            if resto:
                qty_r = sum(r['qty'] for r in resto)
                rev_r = sum(r['revenue_mxn'] for r in resto)
                precio_r = rev_r / qty_r if qty_r else 0.0
                contrib, neto, sem = margenes(q_prod, precio_r)
                html += (
                    '<tr class="text-muted"><td>Otros (%s clientes)</td>'
                    '<td class="text-end">—</td><td class="text-end">%s</td>'
                    '<td class="text-end">$%.2f</td>'
                    '<td class="text-end">%.1f%%</td>'
                    '<td class="text-end">%.1f%%</td><td>%s</td></tr>'
                    % (len(resto), f'{qty_r / 12.0:,.0f}', precio_r,
                       contrib, neto, emoji.get(sem, '')))
            html += '</tbody></table>'

        # ---- B. Otras presentaciones del mismo artículo ----
        variantes = self.related_presentations(product)
        if variantes:
            Peso = self.env['qb.producto.peso']
            html += (
                '<h5>⚖ El mismo artículo en otras presentaciones</h5>'
                '<p style="font-size:12px;" class="text-muted">La versión '
                'con prefijo «I» es el MISMO tejido vendido por peso (kg); '
                'la de sufijo « I» es la versión importada. Para comparar '
                'peras con peras, el precio por kg se muestra también como '
                'su equivalente por metro. Cada margen está evaluado al '
                'precio de venta actual de ESA presentación.</p>'
                '<table class="table table-sm" style="font-size:12px;">'
                '<thead><tr><th>Referencia</th><th>Relación</th>'
                '<th>Se vende en</th>'
                '<th class="text-end">Precio prom 12m (MXN)</th>'
                '<th class="text-end">Equivalente</th>'
                '<th class="text-end">Contribución %</th>'
                '<th class="text-end">Margen neto %</th>'
                '<th>🚦</th></tr></thead><tbody>')
            for p, label in [(product, 'La que estás cotizando')] + variantes:
                qq = q_prod if p == product \
                    else self.quote_product(p, factores)
                r_all = self.sales_by_customer(p)
                qty_t = sum(r['qty'] for r in r_all)
                rev_t = sum(r['revenue_mxn'] for r in r_all)
                precio = rev_t / qty_t if qty_t else 0.0
                p_uom = (p.uom_id.name or 'u')
                equiv = '—'
                if precio and p_uom.lower() in KG_UOM_NAMES:
                    m_per_kg = Peso.resolve_m_per_kg(p)
                    if m_per_kg:
                        equiv = ('≈ $%.2f/m (1 kg ≈ %.1f m)'
                                 % (precio / m_per_kg, m_per_kg))
                if precio:
                    contrib, neto, sem = margenes(qq, precio)
                    celdas = (
                        '<td class="text-end">$%.2f / %s</td>'
                        '<td class="text-end">%s</td>'
                        '<td class="text-end">%.1f%%</td>'
                        '<td class="text-end">%.1f%%</td><td>%s</td>'
                        % (precio, html_escape(p_uom), equiv, contrib,
                           neto, emoji.get(sem, '')))
                else:
                    celdas = (
                        '<td class="text-end text-muted">sin ventas 12m '
                        '(piso lleno: $%.2f)</td>'
                        '<td class="text-end">—</td>'
                        '<td class="text-end">—</td>'
                        '<td class="text-end">—</td><td></td>'
                        % (qq['piso_lleno'] if qq else 0.0))
                html += (
                    '<tr%s><td>%s</td><td style="font-size:11px;">%s</td>'
                    '<td>%s</td>%s</tr>'
                    % (' class="fw-bold"' if p == product else '',
                       html_escape(p.default_code or p.name),
                       html_escape(label), html_escape(p_uom), celdas))
            html += '</tbody></table>'
        else:
            html += ('<p style="font-size:12px;" class="text-muted">Sin '
                     'otras presentaciones detectadas (kg/metros o '
                     'importado) para esta referencia.</p>')
        return html

    @api.model
    def to_mxn_rate(self, currency, date=None):
        """MXN por 1 unidad de `currency`, con el tipo de cambio de Odoo
        (res.currency.rate) a la fecha dada."""
        company = self.env.company
        if not currency or currency == company.currency_id:
            return 1.0
        return currency._convert(1.0, company.currency_id, company,
                                 date or fields.Date.today(), round=False)

    @api.model
    def mp_breakdown(self, product, qty=1.0, _depth=0):
        """Explosión de la BOM en HOJAS, con la FUENTE de cada costo.

        Devuelve filas {name, ref, qty, uom, unit_cost, total, fuente}:
        de dónde viene cada peso de la MP — última compra (proveedor y
        fecha), costo promedio, landed de importado o subproducto en $0.
        La suma de `total` reproduce exactamente _mp_cost_unit.
        """
        if _depth > 10:  # guard de recetas circulares
            return []
        bucket, _centros = self.env['qb.producto.ruteo'].resolve(product)
        ref = product.default_code or ''
        base = {'name': product.name, 'ref': ref, 'qty': qty,
                'uom': product.uom_id.name}
        if bucket == 'subproducto':
            return [dict(base, unit_cost=0.0, total=0.0,
                         fuente='Subproducto: su MP ya está en la receta '
                                'del producto principal → $0')]
        if bucket == 'importado' or ref.endswith(' I'):
            cost = product.standard_price or self._last_purchase_cost(product)
            return [dict(base, unit_cost=cost, total=cost * qty,
                         fuente='Importado: landed cost (promedio Odoo, '
                                'incluye flete/aduana)')]
        std = product.standard_price or 0.0
        if std > 0.0 and self._has_multiple_boms(product):
            # Receta ambigua (>1 BOM): mismo criterio que _mp_cost_unit —
            # usa el AVCO del intermedio en vez de explotar una receta al azar.
            return [dict(base, unit_cost=std, total=std * qty,
                         fuente='Semiterminado con receta ambigua (varias '
                                'BOMs) → costo AVCO de Odoo')]
        bom = self.env['mrp.bom']._bom_find(product).get(product)
        if bom:
            rows = []
            bom_qty = bom.product_uom_id._compute_quantity(
                bom.product_qty, product.uom_id, round=False) or 1.0
            for line in bom.bom_line_ids:
                comp = line.product_id
                qty_comp = line.product_uom_id._compute_quantity(
                    line.product_qty, comp.uom_id, round=False)
                rows.extend(self.mp_breakdown(
                    comp, qty * qty_comp / bom_qty, _depth + 1))
            return rows
        # Hoja comprada: última compra real + tendencia de las últimas 5
        pols = self.env['purchase.order.line'].search([
            ('product_id', '=', product.id),
            ('order_id.state', 'in', ('purchase', 'done')),
            ('price_unit', '>', 0),
        ], order='id desc', limit=5)
        cost = self._last_purchase_cost(product)
        if pols:
            pol = pols[0]
            fuente = 'Última compra: %s a %s (%s, %s %s)' % (
                pol.order_id.name, pol.order_id.partner_id.name,
                pol.order_id.date_order.date() if pol.order_id.date_order
                else 's/f',
                pol.price_unit, pol.order_id.currency_id.name or 'MXN')
            if len(pols) > 1:
                # ¿Cotizas con el costo en subida o en bajada? Con la MP
                # dominando el variable, este es el mayor riesgo de cotizar.
                company = self.env.company
                mxn = [p.order_id.currency_id._convert(
                    p.price_unit, company.currency_id, company,
                    p.order_id.date_order.date() if p.order_id.date_order
                    else fields.Date.today(), round=False) for p in pols]
                serie = list(reversed(mxn))  # vieja → nueva
                if serie[-1] > serie[0] * 1.02:
                    arrow = '📈 SUBIENDO'
                elif serie[-1] < serie[0] * 0.98:
                    arrow = '📉 bajando'
                else:
                    arrow = '➡️ estable'
                fuente += ' · Tendencia %s compras: %s MXN %s' % (
                    len(serie),
                    ' → '.join('%.2f' % v for v in serie), arrow)
        else:
            fuente = 'Sin compras registradas: costo promedio de Odoo'
        return [dict(base, unit_cost=cost, total=cost * qty, fuente=fuente)]

    @api.model
    def explain_quote_html(self, product, factores):
        """El costo completo EXPLICADO: cada capa con su fórmula, sus
        números reales y la fuente de cada dato — para cotizar entendiendo,
        no confiando a ciegas."""
        Peso = self.env['qb.producto.peso']
        q = self.quote_product(product, factores)
        if not q:
            return '<p>Sin factores calculados.</p>'
        kg = q['kg']
        peso_rec = Peso.search([('product_id', '=', product.id)], limit=1)
        peso_fuente = dict(Peso._fields['source'].selection).get(
            peso_rec.source) if peso_rec else \
            'gramaje del ref / peso de Odoo (sin registro en el maestro)'

        # ---- 1. Materia prima ----
        rows = self.mp_breakdown(product)
        mp_total = sum(r['total'] for r in rows)
        mp_rows = ''.join(
            '<tr><td>%s <span class="text-muted">%s</span></td>'
            '<td class="text-end">%.4f %s</td>'
            '<td class="text-end">$%.2f</td>'
            '<td class="text-end">$%.4f</td>'
            '<td style="font-size:11px;">%s</td></tr>'
            % (r['ref'] or r['name'],
               r['name'] if r['ref'] else '',
               r['qty'], r['uom'] or '', r['unit_cost'], r['total'],
               r['fuente'])
            for r in rows)
        html = (
            '<h5>1. Materia prima — $%.2f/u (explosión de la receta)</h5>'
            '<table class="table table-sm" style="font-size:12px;">'
            '<thead><tr><th>Componente</th><th class="text-end">Cant/u</th>'
            '<th class="text-end">$ unit</th><th class="text-end">Total</th>'
            '<th>De dónde viene</th></tr></thead>'
            '<tbody>%s</tbody></table>' % (mp_total, mp_rows))

        # ---- 2. Energía ----
        html += (
            '<h5>2. Energía variable — $%.2f/u</h5>'
            '<p style="font-size:12px;">$%.2f/kg × %.4f kg/u. El $/kg = '
            'pool de luz+gas+agua ($%s/mes, cuentas clasificadas como '
            'variables) ÷ %s kg producidos/mes. Peso del producto: '
            '<b>%.4f kg/u</b> (fuente: %s).</p>'
            % (q['energia'], factores.energia_por_kg, kg,
               f'{factores.energia_pool_month:,.0f}',
               f'{factores.kg_denom_month:,.0f}', kg, peso_fuente))

        # ---- 3. Fabricación ----
        ws = factores.fab_weight_share
        html += (
            '<h5>3. Fabricación absorbida — $%.2f/u</h5>'
            '<p style="font-size:12px;">Pool fijo de fábrica (MOD + '
            'overhead + depreciación + arrendamiento de maquinaria) = '
            '<b>$%s/mes</b> (GL suavizado %s meses, período %s). Se '
            'reparte híbrido: %.0f%% por PESO (tejido+tintorería: '
            '$%.2f/kg) y %.0f%% por LARGO (acabado: $%.2f/m). Este '
            'producto: %.4f kg × $%.2f + $%.2f = <b>$%.2f</b>. '
            'Familia: %s (importados y subproductos no cargan '
            'fabricación).</p>'
            % (q['fab'], f'{factores.fab_pool_month:,.0f}',
               factores.window_months, factores.period,
               ws * 100, factores.factor_fab_kg,
               (1 - ws) * 100, factores.factor_fab_m,
               kg, factores.factor_fab_kg, factores.factor_fab_m,
               q['fab'], q['bucket']))

        # ---- 4. Operación ----
        html += (
            '<h5>4. Operación — %.1f%% sobre el precio de venta</h5>'
            '<p style="font-size:12px;">Gastos de admin y ventas (cuentas '
            '6xx: $%s/mes) ÷ ventas ($%s/mes) = %.1f%%. Se cobra sobre '
            'el precio porque escala con cuánto vendes, no con lo que '
            'produces.</p>'
            % (q['op_pct'] * 100, f'{factores.op_pool_month:,.0f}',
               f'{factores.ventas_pool_month:,.0f}', q['op_pct'] * 100))

        # ---- Resumen ----
        mercado = q.get('precio_mercado', 0.0)
        html += (
            '<h5>= Costo completo</h5>'
            '<p style="font-size:12px;"><b>Variable</b> (MP + energía) = '
            '$%.2f → piso absoluto. <b>+ Fabricación</b> = $%.2f. '
            '<b>+ Operación</b> → piso a planta llena <b>$%.2f MXN</b> '
            '(margen cero cubriendo todo).%s</p>'
            % (q['variable'], q['variable'] + q['fab'], q['piso_lleno'],
               (' Referencia de mercado: hoy se vende en promedio a '
                '<b>$%.2f MXN</b> (12m, todos los clientes).' % mercado)
               if mercado else ''))
        return html

    @api.model
    def semaforo_for(self, precio, piso_ocioso, piso_lleno):
        """rojo = debajo del variable; ámbar = entre pisos; verde = cubre todo."""
        if not precio:
            return False
        if precio < piso_ocioso:
            return 'rojo'
        if piso_lleno and precio < piso_lleno:
            return 'ambar'
        return 'verde'

    @api.model
    def cron_recompute_monthly(self):
        """Cron: día 2 de cada mes, recalcula el mes anterior."""
        self.action_recompute_period()
