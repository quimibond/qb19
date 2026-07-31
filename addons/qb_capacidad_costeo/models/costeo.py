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
        months = max(
            (date_to.year - date_from.year) * 12 + date_to.month - date_from.month, 1)
        total = 0.0
        wc_ids = centros.mapped('workcenter_ids').ids
        if wc_ids:
            self.env.cr.execute("""
                SELECT COALESCE(SUM(%s), 0)
                FROM mrp_workorder wo
                WHERE wo.workcenter_id IN %%s AND wo.state = 'done'
                  AND wo.date_finished >= %%s AND wo.date_finished < %%s
            """ % wo_qty_sql(self.env), (tuple(wc_ids), date_from, date_to))
            total += self.env.cr.fetchone()[0] or 0.0
        for centro in centros.filtered(
                lambda c: c.mo_name_pattern and not c.workcenter_ids):
            self.env.cr.execute("""
                SELECT COALESCE(SUM(%s), 0)
                FROM mrp_production mp
                WHERE mp.name LIKE %%s AND mp.state = 'done'
                  AND mp.date_finished >= %%s AND mp.date_finished < %%s
            """ % mo_qty_sql(self.env), (centro.mo_name_pattern, date_from, date_to))
            total += self.env.cr.fetchone()[0] or 0.0
        return total / months

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
            'peso_cache': {},
            'mp_cache': {},
        }

    @api.model
    def _mp_cost_unit(self, product, cache=None, seen=None, ctx=None):
        """Costo primo MP por unidad: BOM recursiva a último costo.

        Reglas: subproducto → $0 (su MP ya está en la receta del principal);
        importado → landed (avg de Odoo), sin costo propio → gemelo nacional;
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
    def quote_product(self, product, factores=None, target=None):
        """Costo por capa y precios de UN producto con los factores vigentes.

        Fórmulas (op% y margen meta van SOBRE VENTA, por eso dividen):
          variable        = MP + energía
          piso_ocioso     = variable
          piso_lleno      = (variable + fab) / (1 − op)
          precio_sugerido = (variable + fab) / (1 − op − margen_meta)
        """
        Peso = self.env['qb.producto.peso']
        Config = self.env['qb.costeo.factor.config']
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
        if target is None:
            target = Config.get_param('target_margin', 0.30)
        piso_lleno = (variable + fab) / (1.0 - op) if op < 1 else 0.0
        denom = 1.0 - op - target
        sugerido = (variable + fab) / denom if denom > 0 else 0.0
        hours = self._hours_per_unit(centros, is_kg, kg, m_per_kg)
        return {
            'bucket': bucket, 'centros': centros, 'kg': kg,
            'm_per_kg': m_per_kg, 'is_kg': is_kg,
            'mp': mp, 'energia': energia, 'fab': fab, 'variable': variable,
            'op_pct': op, 'target': target,
            'piso_ocioso': variable, 'piso_lleno': piso_lleno,
            'precio_sugerido': sugerido, 'hours_per_unit': hours,
            'factores': factores,
        }

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
