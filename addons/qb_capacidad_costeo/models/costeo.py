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

costo_variable = MP + energía          (para margen de contribución)
costo_produccion = variable + fab      (para margen BRUTO)
costo_absorbido = producción + op      (para margen NETO / precio piso lleno)

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
    mp_gl_month = fields.Float(
        string='MP consumida/mes (mayor)',
        help='Costo primo más ajustes de inventario del mayor: la materia '
             'prima que la planta REALMENTE consumió. Sale de las cuentas '
             'clasificadas en el bucket «Materia prima».')
    mp_modelada_month = fields.Float(
        string='MP modelada/mes (receta)',
        help='Σ (MP de receta × cantidad vendida) de producto nacional, en la '
             'misma ventana. Es lo que el motor le cobra a los productos '
             'antes del ajuste.')
    mp_ajuste = fields.Float(
        string='Ajuste de MP', digits=(16, 6), default=1.0,
        help='MP consumida ÷ MP modelada. Acerca la receta al último precio '
             'de compra (un costo de reposición teórico, sin merma ni '
             'rendimiento real) a lo que de verdad se consumió. 1.0 = no hay '
             'nada que conciliar (bucket «mp» vacío) o ya cuadran.')
    importacion_pool_month = fields.Float(
        string='Pool importación/mes',
        help='Gastos e impuestos de importación (IGI, DTA, PRV, agente '
             'aduanal, flete). No se prorratean sobre las ventas: se cargan '
             'al valor de lo importado, que es lo que los causa.')
    importacion_base_month = fields.Float(
        string='Compras importadas/mes',
        help='Valor de compra mensual promedio de los productos importados, '
             'en moneda de la compañía. Es la base del factor.')
    factor_importacion = fields.Float(
        string='Factor importación', digits=(16, 6),
        help='Pool ÷ base: cuánto se suma al costo de un importado por cada '
             'peso de valor de compra. 0.15 = 15% sobre el valor importado.')
    ventas_pool_month = fields.Float(string='Ventas/mes (promedio)')
    entretela_pool_month = fields.Float(string='Pool entretelas/mes')
    renta_contractual_pool = fields.Float(
        string='Renta contractual/mes',
        help='Σ de la renta contractual de los centros fabriles (sin '
             'entretelas, que tienen la suya en su propio pool). Sustituye a '
             'las cuentas de renta del GL, que se pagan a saltos.')
    renta_gl_sustituida = fields.Float(
        string='Renta del GL sustituida/mes',
        help='Lo que las cuentas marcadas «es renta de inmueble» aportaban al '
             'pool fabril y que se saca para no contar la renta dos veces. Si '
             'sale en 0 con renta contractual > 0, revisa que las cuentas de '
             'renta estén marcadas — puede haber doble conteo.')
    kg_denom_month = fields.Float(
        string='Denominador kg/mes',
        help='Capacidad NORMAL en kg de los centros que definen el peso '
             '(IAS 2), o su producción real si no hay capacidad derivable.')
    m_denom_month = fields.Float(
        string='Denominador m/mes',
        help='Capacidad NORMAL en metros, misma regla que el de kg.')
    kg_produccion_month = fields.Float(string='Producción kg/mes (real)')
    m_produccion_month = fields.Float(string='Producción m/mes (real)')
    utilizacion_kg_pct = fields.Float(
        string='Utilización kg %',
        help='Producción real ÷ capacidad normal. Si sale absurdamente baja, '
             'revisa el throughput nominal y los calendarios: el denominador '
             'estará inflado y el costo unitario saldrá bajo.')
    utilizacion_m_pct = fields.Float(string='Utilización m %')
    fab_ocioso_month = fields.Float(
        string='Fabricación no absorbida/mes',
        help='La parte del pool fijo que la producción real no alcanza a '
             'absorber contra la capacidad normal: el costo de la capacidad '
             'ociosa. Bajo IAS 2 va al resultado del período, NO al costo del '
             'producto — por eso el modelo reparte menos que el gasto total, '
             'y esa diferencia es deliberada.')
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

    _period_company_uniq = models.Constraint(
        'unique(period, company_id)',
        "Ya existen factores para ese período.",
    )


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
    company_currency_id = fields.Many2one(
        'res.currency', related='company_id.currency_id', store=True,
        string='Moneda', help='Moneda de la compañía. Todos los montos del '
        'reporte (precio, costos, márgenes) están en ESTA moneda, aunque la '
        'factura original haya sido en otra divisa.')
    divisa_venta = fields.Char(
        string='Facturado en',
        help='Divisa(s) distintas a la de la compañía en que se facturó el '
             'producto en el período (p.ej. USD). El precio mostrado es la '
             'conversión a la moneda de la compañía al tipo de cambio de la '
             'factura — no el número crudo en dólares. Vacío = solo moneda '
             'local.')
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
    ventas_total = fields.Float(
        string='Ventas $ (período)',
        help='Lo REALMENTE facturado del producto en el mes, en moneda de la '
             'compañía: Σ de aml.balance de las líneas de factura contra '
             'cuentas de ingreso (las notas de crédito restan). Una factura '
             'en divisa entra con su valor real en pesos, al TC de la '
             'factura. Es un hecho contable, no un cálculo: cuadra contra el '
             'estado de resultados.\n\n'
             'Nota: si en el mes las devoluciones superaron a las ventas '
             '(qty neta ≤ 0) este monto se muestra igual, pero los costos y '
             'márgenes totales quedan en 0 — no hay precio unitario válido '
             'que costear.')
    divisa_id = fields.Many2one(
        'res.currency', string='Divisa',
        help='Divisa distinta a la de la compañía con MÁS facturación del '
             'producto en el mes (p.ej. USD). Vacío = solo se vendió en '
             'moneda local.')
    qty_divisa = fields.Float(
        string='Qty vendida en divisa',
        help='Unidades facturadas en esa divisa (subconjunto de la qty '
             'total del período).')
    ventas_total_divisa = fields.Monetary(
        string='Ventas en divisa', currency_field='divisa_id',
        help='Facturado en la moneda ORIGINAL del documento '
             '(Σ amount_currency), sin convertir. El número que ve el '
             'cliente en su factura.')
    precio_prom_divisa = fields.Monetary(
        string='Precio en divisa', currency_field='divisa_id',
        help='Ventas en divisa ÷ qty en divisa: el precio unitario tal cual '
             'se cotizó y facturó en esa moneda.')
    tc_prom = fields.Float(
        string='TC promedio', digits=(16, 4),
        help='Tipo de cambio implícito de lo facturado en divisa: pesos '
             'reconocidos ÷ importe en divisa. Es el TC efectivo de las '
             'facturas del mes, no el del día.')
    mp_unit = fields.Float(string='MP $/u', digits=(16, 4))
    importacion_unit = fields.Float(
        string='de eso, importación $/u', digits=(16, 4),
        help='Cuánto de la MP de arriba son gastos e impuestos de aduana '
             '(IGI, DTA, PRV, agente aduanal, flete) repartidos sobre el '
             'valor importado. NO es una capa aparte: ya está dentro de la '
             'MP, se muestra para poder auditarla.\n\n'
             'Solo aplica al producto importado en sí; si un producto '
             'nacional lleva componentes importados, la aduana de esos '
             'componentes va dentro de su MP y no se ve en este renglón.')
    energia_unit = fields.Float(string='Energía $/u', digits=(16, 4))
    costo_variable = fields.Float(
        string='Costo variable $/u', digits=(16, 4),
        help='MP + energía: lo que sale de la bolsa por producir UNA unidad '
             'más. Base del margen de contribución.')
    fab_unit = fields.Float(string='Fabricación $/u', digits=(16, 4))
    costo_produccion = fields.Float(
        string='Costo de producción $/u', digits=(16, 4),
        help='Variable + fabricación absorbida: lo que cuesta FABRICAR la '
             'unidad. Base del margen bruto.')
    op_unit = fields.Float(string='Operación $/u', digits=(16, 4))
    costo_absorbido = fields.Float(
        string='Costo absorbido $/u', digits=(16, 4),
        help='Producción + operación (admin y ventas como % del precio): el '
             'costo COMPLETO. Base del margen neto.')
    # --- Totales del período ($ del mes, no $/unidad) -------------------
    # Aditivos: se pueden sumar entre productos y entre meses en el pivote.
    # Todos son costo_unitario × qty vendida — o sea, costo de lo VENDIDO
    # (COGS del modelo), no el gasto del mes ni el costo de lo producido.
    mp_total = fields.Float(
        string='MP $ (período)',
        help='MP unitaria × qty vendida: cuánta materia prima cargó lo '
             'vendido este mes.')
    importacion_total = fields.Float(
        string='de eso, importación $ (período)',
        help='La parte de la MP del período que es aduana. Ya está dentro de '
             '«MP $ (período)» — no se suma aparte.')
    energia_total = fields.Float(string='Energía $ (período)')
    fab_total = fields.Float(string='Fabricación $ (período)')
    op_total = fields.Float(string='Operación $ (período)')
    costo_variable_total = fields.Float(
        string='Costo variable $ (período)',
        help='(MP + energía) × qty vendida.')
    costo_produccion_total = fields.Float(
        string='Costo de producción $ (período)',
        help='(variable + fabricación) × qty vendida. Ventas − esto = margen '
             'bruto total.')
    costo_absorbido_total = fields.Float(
        string='Costo total $ (período)',
        help='Costo absorbido × qty vendida: el costo COMPLETO de lo vendido. '
             'Ventas − esto = margen neto total.')
    margen_contribucion = fields.Float(
        string='Contribución $/u', digits=(16, 4),
        help='Precio − costo VARIABLE (MP + energía). Lo que cada unidad '
             'vendida aporta para pagar los costos fijos (que se pagan igual, '
             'se venda o no). Todavía NO es utilidad: de aquí salen la '
             'fabricación y la operación.')
    margen_contribucion_pct = fields.Float(string='Contribución %')
    margen_bruto = fields.Float(
        string='Margen bruto $/u', digits=(16, 4),
        help='Precio − costo de producción (MP + energía + fabricación). '
             'Utilidad de fabricar y vender la unidad, ANTES de gastos de '
             'administración y ventas.')
    margen_bruto_pct = fields.Float(string='Margen bruto %')
    margen_absorbido = fields.Float(
        string='Margen neto $/u', digits=(16, 4),
        help='Precio − costo absorbido (producción + operación). Lo que queda '
             'de verdad después de TODOS los costos asignables. También '
             'llamado "margen absorbido" en costeo.')
    margen_absorbido_pct = fields.Float(string='Margen neto %')
    margen_bruto_total = fields.Float(
        string='Margen bruto total (período)',
        help='Margen bruto unitario × qty vendida. Aditivo: se puede sumar '
             'entre meses/productos en el pivote.')
    margen_neto_total = fields.Float(
        string='Margen neto total (período)',
        help='Margen neto unitario × qty vendida. Aditivo: se puede sumar '
             'entre meses/productos en el pivote.')
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
        ('peso_estimado', 'Peso ESTIMADO — verificar'),
        ('ok', 'OK'),
    ], string='Alerta',
        help='bajo_variable = destruye valor (rojo); bajo_absorbido = aporta '
             'a fijos pero no cubre todo (ámbar); sin_peso = falta el peso '
             'kg/u, la fabricación no se puede repartir; peso_estimado = el '
             'peso se adivinó del código o del campo weight de Odoo (no es '
             'medido) → energía y fabricación pueden estar mal, hay que '
             'capturar el peso real.')
    peso_source = fields.Char(
        string='Fuente del peso',
        help='De dónde salió el kg/u: manual/cvu/kg_native/record/import_twin '
             '= confiable; ref_gramaje/odoo_weight = estimado.')
    centro_route = fields.Char(string='Ruta (centros)')
    factores_id = fields.Many2one('qb.costo.factores', string='Factores usados')

    _period_product_uniq = models.Constraint(
        'unique(period, product_id, company_id)',
        "Ya existe el costo de ese producto para ese período.",
    )

    # ------------------------------------------------------------------
    # Pools GL
    # ------------------------------------------------------------------
    @api.model
    def _pool_by_month(self, buckets, date_from, date_to,
                       es_variable=None, centro_id=None, sign=1.0,
                       es_renta=None):
        """Σ balance de las cuentas clasificadas en `buckets`, por mes.

        Devuelve {date_mes: monto}. sign=-1 para ingresos (saldo acreedor).
        `es_renta=True` aísla las cuentas de renta de inmueble (para poder
        sustituirlas por la renta contractual sin contarlas dos veces).
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
        if es_renta is not None:
            query += ' AND COALESCE(m.es_renta, FALSE) = %s'
            params.append(es_renta)
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
    def _production_month_avg(self, centros, date_from, date_to,
                              restar_by_month=None):
        """Producción promedio mensual de un conjunto de centros: vía sus
        workcenters (mrp.workorder) o su mo_name_pattern (mrp.production).

        `restar_by_month` ajusta metros que la orden reportó pero que ya no
        coinciden al momento de vender (hoy: encogimiento menos estiramiento).
        Un valor negativo suma metros en vez de restarlos, que es el caso del
        estiramiento. Sin ese ajuste el pool se reparte entre metros que se
        evaporaron y el costo unitario queda subvaluado."""
        if not centros:
            return 0.0
        # Promediar SOLO los meses con producción: si los workcenters
        # arrancaron a media ventana (tejido: mayo 2026), dividir entre la
        # ventana completa subestima el denominador ×4 e infla energía y
        # factores de fabricación en la misma proporción.
        by_month = {}
        # Producción a nivel ORDEN (mrp.production) es la fuente confiable:
        # la cantidad por workorder está mal registrada (p.ej. tejido abril
        # colapsa a ~1/5 de lo real). Por eso el patrón de orden MANDA; el
        # conteo por workorder queda sólo como fallback para centros con
        # workcenters pero SIN patrón de orden.
        pattern_centros = centros.filtered('mo_name_pattern')
        for centro in pattern_centros:
            # mo_name_pattern admite varios patrones separados por coma
            # (p.ej. acabado = 'TL/OP-ACA%,TL/OP-V10%'): matchea cualquiera.
            self.env.cr.execute("""
                SELECT date_trunc('month', mp.date_finished)::date,
                       COALESCE(SUM(%s), 0)
                FROM mrp_production mp
                WHERE mp.name LIKE ANY(string_to_array(%%s, ','))
                  AND mp.state = 'done'
                  AND mp.company_id = %%s
                  AND mp.date_finished >= %%s AND mp.date_finished < %%s
                GROUP BY 1
            """ % mo_qty_sql(self.env),
                (centro.mo_name_pattern, self.env.company.id,
                 date_from, date_to))
            for mes, qty in self.env.cr.fetchall():
                by_month[mes] = by_month.get(mes, 0.0) + (qty or 0.0)
        wc_ids = centros.filtered(
            lambda c: c.workcenter_ids and not c.mo_name_pattern
        ).mapped('workcenter_ids').ids
        if wc_ids:
            # El pool de gasto se lee SOLO de la compañía activa; el
            # denominador de producción tiene que leerse igual o el factor
            # $/kg queda dividido entre la producción de todo el grupo.
            self.env.cr.execute("""
                SELECT date_trunc('month', wo.date_finished)::date,
                       COALESCE(SUM(%s), 0)
                FROM mrp_workorder wo
                JOIN mrp_production mp ON mp.id = wo.production_id
                WHERE wo.workcenter_id IN %%s AND wo.state = 'done'
                  AND mp.company_id = %%s
                  AND wo.date_finished >= %%s AND wo.date_finished < %%s
                GROUP BY 1
            """ % wo_qty_sql(self.env),
                (tuple(wc_ids), self.env.company.id, date_from, date_to))
            for mes, qty in self.env.cr.fetchall():
                by_month[mes] = by_month.get(mes, 0.0) + (qty or 0.0)
        for mes, qty in (restar_by_month or {}).items():
            if mes in by_month:
                by_month[mes] -= qty
        activos = [q for q in by_month.values() if q > 0]
        return sum(activos) / len(activos) if activos else 0.0

    @api.model
    def _capacidad_normal_map(self, centros):
        """{centro_id: capacidad normal/mes} desde `qb.ociosidad`.

        Se resuelve UNA vez por corrida y se pasa a los denominadores: la
        vista de ociosidad arma calendarios, pools del GL y producción, y
        `action_recompute_year` llamaría a ese query veinticuatro veces.
        """
        if not centros:
            return {}
        return {o.centro_id.id: o.capacity_month_units
                for o in self.env['qb.ociosidad'].search(
                    [('centro_id', 'in', centros.ids)])}

    def _denominador_capacidad(self, centros, date_from, date_to,
                               restar_by_month=None, caps=None):
        """Denominador del factor de fabricación: capacidad NORMAL del centro,
        no su producción real (costeo normal, IAS 2).

        Dividir el pool fijo entre la producción del mes le carga la ociosidad
        al producto: un mes flojo lo encarece, y el modelo entonces recomienda
        subir el precio justo cuando lo que hace falta es vender más. Bajo
        IAS 2 el costo no absorbido por la capacidad ociosa va al resultado
        del período, no al inventario.

        La capacidad normal sale de `qb.ociosidad`, que ya la deriva igual que
        el campo promete: `capacidad_normal` capturada, o calendario real ×
        throughput nominal. Usar la misma fuente que la vista de ociosidad es
        lo que hace que las dos mitades del módulo digan lo mismo — antes el
        motor dividía entre producción real y la vista entre capacidad normal.

        Un centro sin capacidad derivable (sin throughput nominal, sin
        workcenters y sin turnos) cae a su producción real: degradar con
        gracia es preferible a dejar el denominador en cero.
        """
        if not centros:
            return 0.0
        Config = self.env['qb.costeo.factor.config']
        if not Config.get_param('denominador_capacidad_normal', 1.0):
            return self._production_month_avg(
                centros, date_from, date_to, restar_by_month)
        if caps is None:
            caps = self._capacidad_normal_map(centros)
        con_normal = centros.filtered(lambda c: caps.get(c.id, 0.0) > 0)
        sin_normal = centros - con_normal
        total = sum(caps[c.id] for c in con_normal)
        if sin_normal:
            # El ajuste de metros (encogimiento/estiramiento) solo aplica al
            # lado que se mide con producción: la capacidad normal es un techo
            # teórico y no encoge.
            total += self._production_month_avg(
                sin_normal, date_from, date_to, restar_by_month)
        return total

    def _ajuste_metros_by_month(self, date_from, date_to):
        """Metros que la orden reportó y que ya no coinciden al vender.

        Ni el encogimiento ni el estiramiento destruyen o crean material: la
        misma tela mide menos o mide más. Ninguno de los dos pasa por la orden
        de fabricación —ocurren después, en Inspección, con su propio tipo de
        operación— así que el denominador los ignora por completo.

        Se mide como salida NETA de inventario, de modo que el signo sale
        solo: el encogimiento saca metros y da positivo (hay que restarlos del
        denominador); el estiramiento mete metros y da negativo (al restarlo,
        los devuelve). La misma fórmula cubre además los dos regímenes del
        encogimiento: el viejo (movimiento a scrap, que sacaba los metros y
        los mandaba a resultados) y el nuevo (orden que consume 100 y produce
        95 sin tocar resultados).

        Los tipos se identifican por sequence_code porque el nombre es
        traducible y no se puede filtrar en SQL. Se usa 'OP-EST' y no 'EST' a
        propósito: 'EST' también matchea 'DEST-' (DESTRUCCIÓN), que es
        material realmente perdido y no un cambio de medida.
        """
        self.env.cr.execute("""
            SELECT date_trunc('month', sm.date)::date,
                   COALESCE(SUM(
                       CASE WHEN ld.usage != 'internal' THEN sm.quantity
                            ELSE 0 END
                     - CASE WHEN ls.usage != 'internal' THEN sm.quantity
                            ELSE 0 END), 0)
            FROM stock_move sm
            JOIN stock_picking_type spt ON spt.id = sm.picking_type_id
            JOIN stock_location ls ON ls.id = sm.location_id
            JOIN stock_location ld ON ld.id = sm.location_dest_id
            WHERE sm.state = 'done'
              AND sm.company_id = %s
              AND (spt.sequence_code LIKE '%%ENC%%'
                   OR spt.sequence_code LIKE '%%OP-EST%%')
              AND sm.date >= %s AND sm.date < %s
            GROUP BY 1
        """, (self.env.company.id, date_from, date_to))
        return {mes: (qty or 0.0) for mes, qty in self.env.cr.fetchall()}

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
            # MOD sumado sobre TODOS los centros de entretela (antes tomaba
            # sólo el primero mientras la renta sí sumaba todos → asimétrico).
            ent_mod = sum(
                self._smooth(self._pool_by_month(
                    ('mod',), date_from, date_to, centro_id=c.id))
                for c in ent_centros)
            entretela_pool = (
                ent_mod
                + sum(ent_centros.mapped('renta_contractual_mxn'))
                + Config.get_param('entretela_overhead_extra_mxn', 0.0))
            entretela_m = self._production_month_avg(ent_centros, date_from, date_to)

        # Renta: el GL se paga a saltos, así que las cuentas marcadas
        # `es_renta` SALEN del pool y en su lugar entra la renta contractual
        # de los centros fabriles. Antes la contractual solo se aplicaba a
        # entretelas — tejido, tintorería y acabado tenían su renta capturada
        # y nunca llegaba al costo del producto (y la que sí llegaba, por una
        # cuenta de renta clasificada en un bucket fabril, se contaba doble
        # en cuanto se activaba el contrato).
        # La resta va MES A MES, no contra el promedio: la renta se postea en
        # unos meses sí y otros no, así que promediarla sobre sus propios
        # meses de pago la escalaría distinto que al pool y sobre-restaría.
        renta_by_month = self._pool_by_month(
            FAB_BUCKETS, date_from, date_to, es_variable=False, es_renta=True)
        fab_sin_renta = {mes: monto - renta_by_month.get(mes, 0.0)
                         for mes, monto in fab_by_month.items()}
        renta_gl = sum(renta_by_month.values()) / window if window else 0.0
        renta_centros = Centro.search([
            ('nature', 'in', ('fabril_directo', 'fabril_indirecto')),
            ('id', 'not in', ent_centros.ids),
        ])
        renta_contractual = sum(renta_centros.mapped('renta_contractual_mxn'))

        fab_pool = max(self._smooth(fab_sin_renta) - entretela_pool
                       + renta_contractual, 0.0)
        energia_pool = self._smooth(energia_by_month)
        op_pool = self._smooth(op_by_month)
        ventas_pool = self._smooth(ventas_by_month)

        kg_centros = Centro.search([('es_denominador_kg', '=', True)])
        m_centros = Centro.search([('es_denominador_m', '=', True)])
        ajuste_m = self._ajuste_metros_by_month(date_from, date_to)
        caps = self._capacidad_normal_map(kg_centros | m_centros)
        # Denominador = capacidad NORMAL (IAS 2). La producción real se sigue
        # midiendo aparte para saber cuánto del pool NO se absorbió: ese es el
        # costo de la ociosidad, y va al resultado del período, no al producto.
        kg_denom = (Config.get_param('denominador_kg_override', 0.0)
                    or self._denominador_capacidad(
                        kg_centros, date_from, date_to, caps=caps))
        m_denom = (Config.get_param('denominador_m_override', 0.0)
                   or self._denominador_capacidad(
                       m_centros, date_from, date_to,
                       restar_by_month=ajuste_m, caps=caps))
        kg_real = self._production_month_avg(kg_centros, date_from, date_to)
        m_real = self._production_month_avg(
            m_centros, date_from, date_to, restar_by_month=ajuste_m)

        # Importación: los gastos e impuestos de aduana se cargan al valor de
        # lo importado (el IGI se calcula sobre el valor en aduana; flete y
        # agente escalan con el valor embarcado). Antes caían en `no_costeo` y
        # ningún producto los pagaba, o peor, en `operacion` y se prorrateaban
        # sobre TODAS las ventas — incluidas las de producto nacional.
        importacion_pool = self._smooth(
            self._pool_by_month(('importacion',), date_from, date_to))
        # Sin pool no hay factor: la base cuesta un escaneo de 12 meses de
        # compras y una resolución de ruteo por producto comprado, y
        # action_recompute_year llama a esto doce veces.
        importacion_base = (self._import_purchase_base(date_from, date_to)
                            if importacion_pool else 0.0)
        factor_importacion = (
            Config.get_param('importacion_factor_override', 0.0)
            or (importacion_pool / importacion_base if importacion_base else 0.0))
        # Guarda contra clasificación errónea: una cuenta grande mal puesta en
        # el bucket dispararía el costo de TODOS los importados sin aviso.
        factor_max = Config.get_param('importacion_factor_max', 1.0) or 1.0
        if factor_importacion > factor_max:
            _logger.warning(
                'qb.costo.factores %s: factor de importación %.3f supera el '
                'máximo %.3f (pool %.2f ÷ base %.2f). Se recorta — revisa qué '
                'cuentas están en el bucket «importacion».',
                period, factor_importacion, factor_max,
                importacion_pool, importacion_base)
            factor_importacion = factor_max

        ws = Config.get_param('fab_weight_share', 0.67)
        factor_fab_kg = ws * fab_pool / kg_denom if kg_denom else 0.0
        factor_fab_m = (1 - ws) * fab_pool / m_denom if m_denom else 0.0
        # La energía es VARIABLE: su $/kg se divide entre los kilos que de
        # verdad se produjeron, no entre la capacidad normal. Con capacidad
        # normal en el denominador, un mes al 60% de utilización daría una
        # energía por kilo 40% baja — justo al revés de la realidad física.
        # (El override manual sigue mandando sobre los dos.)
        kg_energia = Config.get_param('denominador_kg_override', 0.0) or kg_real
        energia_por_kg = (Config.get_param('energia_por_kg', 0.0)
                          or (energia_pool / kg_energia if kg_energia else 0.0))
        op_pct = (Config.get_param('op_pct_override', 0.0)
                  or (op_pool / ventas_pool if ventas_pool else 0.0))
        entretela_factor = entretela_pool / entretela_m if entretela_m else 0.0

        # Costo de la capacidad ociosa: la parte del pool fijo que la
        # producción real NO alcanza a absorber contra la capacidad normal.
        # Con denominador = producción real esto da 0 por construcción, que es
        # justo el problema que el costeo normal evita.
        util_kg = kg_real / kg_denom if kg_denom else 0.0
        util_m = m_real / m_denom if m_denom else 0.0
        fab_absorbible = fab_pool * (ws * util_kg + (1 - ws) * util_m)
        fab_ocioso = max(fab_pool - fab_absorbible, 0.0)

        Factores = self.env['qb.costo.factores']
        vals = {
            'period': period,
            'window_months': window,
            'fab_pool_month': fab_pool,
            'energia_pool_month': energia_pool,
            'op_pool_month': op_pool,
            'importacion_pool_month': importacion_pool,
            'importacion_base_month': importacion_base,
            'factor_importacion': factor_importacion,
            'ventas_pool_month': ventas_pool,
            'entretela_pool_month': entretela_pool,
            'renta_contractual_pool': renta_contractual,
            'renta_gl_sustituida': renta_gl,
            'kg_denom_month': kg_denom,
            'm_denom_month': m_denom,
            'kg_produccion_month': kg_real,
            'm_produccion_month': m_real,
            'utilizacion_kg_pct': 100.0 * util_kg,
            'utilizacion_m_pct': 100.0 * util_m,
            'fab_ocioso_month': fab_ocioso,
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
    # Ajuste de MP: receta teórica vs. materia prima realmente consumida
    # ------------------------------------------------------------------
    @api.model
    def _sales_qty_by_month(self, date_from, date_to):
        """{(mes, product_id): qty} vendida, con el mismo dedup del triplete
        que usa `_sales_by_product`.

        Una sola query para TODA la ventana: el ajuste de MP necesita doce
        meses, y hacer doce consultas por recálculo no escala — un
        `action_recompute_year` haría ciento cuarenta y cuatro.
        """
        self.env.cr.execute("""
            WITH lines AS (
                SELECT aml.move_id, aml.product_id, aml.quantity,
                       am.move_type,
                       date_trunc('month', am.invoice_date)::date AS mes
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
            dedup AS (
                SELECT DISTINCT ON (move_id, product_id, ABS(quantity))
                       mes, product_id, quantity, move_type
                FROM lines
                ORDER BY move_id, product_id, ABS(quantity)
            )
            SELECT mes, product_id,
                   SUM(CASE WHEN move_type = 'out_refund'
                            THEN -quantity ELSE quantity END)
            FROM dedup
            GROUP BY 1, 2
        """, (date_from, date_to, self.env.company.id))
        return {(mes, pid): qty or 0.0
                for mes, pid, qty in self.env.cr.fetchall()}

    @api.model
    def _mp_ajuste(self, date_from, date_to, ctx):
        """Factor que acerca la MP de receta a la MP realmente consumida.

        Devuelve `(gl_mes, modelada_mes, factor)`.

        La MP del motor es la receta explotada al ÚLTIMO precio de compra: un
        costo de reposición teórico. No lleva merma, ni rendimiento real, ni
        la variación entre ese último precio y lo que de verdad se pagó. La
        contabilidad sí sabe cuánta materia prima se consumió — es el costo
        primo, más los ajustes de inventario. El cociente entre las dos es el
        factor.

        Ambos lados se suman sobre la MISMA ventana antes de dividir, no se
        promedian por separado: una ventana con meses de venta desigual daría
        un factor sesgado hacia los meses flojos.

        El factor NO se aplica a los importados: su MP es el precio de compra
        más aduana, no materia prima que la planta consuma, y meterla al
        cociente contaminaría las dos partes.

        Factor 1.0 (inerte) si no hay cuentas clasificadas en el bucket
        `mp` — el ajuste solo existe cuando hay contra qué conciliar.

        Aproximación conocida: la MP modelada usa la receta y el último precio
        de compra de HOY sobre las ventas de los doce meses, mientras el GL es
        histórico. Es la misma convención que usa todo el motor (el costo se
        expresa a precios de reposición), y por eso el factor se lee como
        "cuánto se desvía la receta del consumo real", no como una
        reexpresión contable de cada mes.
        """
        Config = self.env['qb.costeo.factor.config']
        gl_by_month = self._pool_by_month(('mp',), date_from, date_to)
        if not gl_by_month:
            return 0.0, 0.0, 1.0

        qty_by_month = self._sales_qty_by_month(date_from, date_to)
        Ruteo = self.env['qb.producto.ruteo']
        rules = ctx['rules']
        Product = self.env['product.product']
        products = {p.id: p for p in Product.browse(
            list({pid for _mes, pid in qty_by_month})).exists()}
        modelada_by_month = {}
        nacional = {}
        for (mes, pid), qty in qty_by_month.items():
            if qty <= 0:
                continue
            product = products.get(pid)
            if product is None:
                continue
            es_nac = nacional.get(pid)
            if es_nac is None:
                bucket, _centros = Ruteo.resolve(product, rules)
                es_nac = not self._es_importado(product, bucket) \
                    and bucket != 'subproducto'
                nacional[pid] = es_nac
            if not es_nac:
                continue
            mp = self._mp_cost_unit(product, ctx=ctx)
            modelada_by_month[mes] = modelada_by_month.get(mes, 0.0) + mp * qty

        # Solo los meses con dato de los DOS lados: un mes sin póliza de costo
        # primo (o sin ventas) sesgaría el cociente.
        meses = [m for m in gl_by_month
                 if gl_by_month[m] > 0 and modelada_by_month.get(m, 0.0) > 0]
        if not meses:
            return 0.0, 0.0, 1.0
        gl = sum(gl_by_month[m] for m in meses)
        modelada = sum(modelada_by_month[m] for m in meses)
        factor = (Config.get_param('mp_ajuste_override', 0.0)
                  or (gl / modelada if modelada else 1.0))

        # Banda de cordura: un factor disparado casi siempre significa una
        # cuenta mal clasificada en el bucket `mp`, no que la receta esté
        # equivocada por 3×. Se recorta y se loggea en vez de reescribir todos
        # los costos en silencio.
        f_min = Config.get_param('mp_ajuste_min', 0.5) or 0.5
        f_max = Config.get_param('mp_ajuste_max', 1.5) or 1.5
        if not f_min <= factor <= f_max:
            _logger.warning(
                'qb.costo.factores: ajuste de MP %.4f fuera de la banda '
                '[%.2f, %.2f] (GL %.2f ÷ modelada %.2f en %s meses). Se '
                'recorta — revisa qué cuentas están en el bucket «mp».',
                factor, f_min, f_max, gl, modelada, len(meses))
            factor = min(max(factor, f_min), f_max)
        return gl / len(meses), modelada / len(meses), factor

    # ------------------------------------------------------------------
    # Base de importación: valor comprado de producto importado
    # ------------------------------------------------------------------
    @api.model
    def _es_importado(self, product, bucket=None):
        """¿El producto se compra importado? Mismo criterio que usa la MP:
        la familia de ruteo, o el sufijo ' I' de la nomenclatura."""
        if bucket is None:
            bucket, _c = self.env['qb.producto.ruteo'].resolve(product)
        return bucket == 'importado' or (product.default_code or '').endswith(' I')

    @api.model
    def _import_purchase_base(self, date_from, date_to, rules=None):
        """Valor de compra mensual promedio de los productos IMPORTADOS, en
        moneda de la compañía.

        Es la base sobre la que se reparten los gastos e impuestos de aduana:
        el IGI se calcula sobre el valor en aduana, y flete y agente escalan
        con el valor embarcado. Se promedian solo los meses CON compras — si
        se importa cada dos meses, dividir entre la ventana completa partiría
        el factor a la mitad.
        """
        self.env.cr.execute("""
            SELECT date_trunc('month', po.date_order)::date AS mes,
                   pol.product_id, po.currency_id,
                   SUM(pol.price_unit * pol.product_qty
                       * (1 - COALESCE(pol.discount, 0) / 100.0)) AS monto
            FROM purchase_order_line pol
            JOIN purchase_order po ON po.id = pol.order_id
            WHERE po.state IN ('purchase', 'done')
              AND po.company_id = %s
              AND po.date_order >= %s AND po.date_order < %s
              AND pol.product_id IS NOT NULL
            GROUP BY 1, 2, 3
        """, (self.env.company.id, date_from, date_to))
        rows = self.env.cr.fetchall()
        if not rows:
            return 0.0

        Ruteo = self.env['qb.producto.ruteo']
        rules = rules if rules is not None else Ruteo.search([])
        products = self.env['product.product'].browse(
            list({r[1] for r in rows})).exists()
        products.read(['default_code'])
        importados = set()
        for product in products:
            bucket, _centros = Ruteo.resolve(product, rules)
            if self._es_importado(product, bucket):
                importados.add(product.id)
        if not importados:
            return 0.0

        company = self.env.company
        currencies = {c.id: c for c in self.env['res.currency'].browse(
            list({r[2] for r in rows if r[2]})).exists()}
        by_month = {}
        for mes, product_id, currency_id, monto in rows:
            if product_id not in importados:
                continue
            monto = monto or 0.0
            currency = currencies.get(currency_id)
            if currency and currency != company.currency_id:
                monto = currency._convert(
                    monto, company.currency_id, company, mes)
            by_month[mes] = by_month.get(mes, 0.0) + monto
        activos = [v for v in by_month.values() if v > 0]
        return sum(activos) / len(activos) if activos else 0.0

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
            # raise_if_failure=False: si la compra y el producto están en
            # categorías de UoM distintas (kg vs m en textil), Odoo NO truena;
            # deja el precio sin convertir en vez de tumbar el costeo.
            qty_in_product_uom = pol_uom._compute_quantity(
                1.0, product.uom_id, round=False, raise_if_failure=False)
            if qty_in_product_uom:
                price = price / qty_in_product_uom
        return price

    @api.model
    def _engine_ctx(self, product_ids=None, factores=None):
        """Contexto de una corrida del motor: todo lo que se resuelve UNA vez
        y se comparte en el loop (reglas de ruteo, mapa de últimas compras,
        factor de importación, cachés de peso y MP). Mantiene el motor
        O(productos), no O(queries).
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
            # El caché de MP guarda el costo YA con aduana, así que el factor
            # tiene que vivir en el contexto de la corrida: mezclar dos
            # factores en el mismo caché daría costos incoherentes.
            'import_factor': factores.factor_importacion if factores else 0.0,
            'peso_cache': {},
            'mp_cache': {},
        }

    @api.model
    def _mp_cost_unit(self, product, cache=None, seen=None, ctx=None,
                      import_factor=None):
        """Costo primo MP por unidad: BOM recursiva a último costo.

        Reglas: subproducto → $0 (su MP ya está en la receta del principal);
        importado → costo de compra MÁS gastos e impuestos de aduana
        (`import_factor`); sin costo propio → gemelo nacional;
        receta AMBIGUA (>1 BOM activa) con AVCO válido → costo AVCO de Odoo
        (evita colapsar el costo eligiendo una receta al azar);
        hoja sin BOM → último costo de compra (fallback avg).

        `ctx` (de _engine_ctx) comparte reglas/pol_map/cachés en loops
        grandes; sin él (cotizador, tests) resuelve todo al vuelo.
        """
        # El landed va DENTRO de la MP y no como capa aparte: así lo recoge
        # también la receta que consume el importado como componente, y la
        # cascada del cotizador y del PDF sigue cuadrando sin cambios.
        if import_factor is None:
            import_factor = (ctx or {}).get('import_factor', 0.0)
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
                    cost = self._mp_cost_unit(twin, cache, seen, ctx,
                                              import_factor)
            # El AVCO de Odoo NO trae la aduana: el IGI, el DTA y el agente
            # aduanal se postean directo a resultados, así que nunca entraron
            # al costo del producto. Aquí se le suman.
            cost *= 1.0 + import_factor
        else:
            std = product.standard_price or 0.0
            is_multi = self._has_multiple_boms(product, ctx)
            if std > 0.0 and is_multi:
                # Receta AMBIGUA: el producto tiene VARIAS BOMs activas (típico
                # de semiterminados genéricos "MUESTRA PILOTO", que llegan a
                # tener 26 recetas). _bom_find elegiría una al azar y el costo
                # se colapsa (bug WD080: MP $0.13 en vez de ~$12). En vez de
                # explotar una receta arbitraria, usa el AVCO que Odoo ya
                # mantiene para el intermedio: es nativo y confiable.
                cost = std
            elif is_multi:
                # Varias recetas y SIN AVCO confiable: no hay una "correcta".
                # En vez de explotar una al azar (que colapsaría el costo),
                # explota TODAS y toma la MÁS CARA — conservador para cotizar.
                boms = self._applicable_boms(product)
                costs = [self._explode_bom(b, product, cache, seen, ctx,
                                           import_factor)
                         for b in boms]
                cost = max(costs) if costs else \
                    self._last_purchase_cost(product, pol_map)
            else:
                bom = self.env['mrp.bom']._bom_find(product).get(product)
                cost = self._explode_bom(bom, product, cache, seen, ctx,
                                         import_factor) \
                    if bom else self._last_purchase_cost(product, pol_map)
        cache[product.id] = cost
        return cost

    @api.model
    def _applicable_boms(self, product):
        """BOMs activas aplicables a un producto (por variante o por
        plantilla). Se usa para detectar/resolver recetas ambiguas."""
        return self.env['mrp.bom'].search([
            '|', ('product_id', '=', product.id),
            '&', ('product_id', '=', False),
                 ('product_tmpl_id', '=', product.product_tmpl_id.id),
        ])

    @api.model
    def _explode_bom(self, bom, product, cache, seen, ctx,
                     import_factor=0.0):
        """Costo MP/unidad explotando UNA receta: Σ(qty × costo_hoja) ÷ salida.
        raise_if_failure=False evita que una conversión entre categorías de
        UoM (kg vs m) tumbe el costeo — deja la cantidad sin convertir."""
        total = 0.0
        for line in bom.bom_line_ids:
            comp = line.product_id
            qty = line.product_uom_id._compute_quantity(
                line.product_qty, comp.uom_id, round=False,
                raise_if_failure=False)
            total += qty * self._mp_cost_unit(comp, cache, seen, ctx,
                                              import_factor)
        bom_qty = bom.product_uom_id._compute_quantity(
            bom.product_qty, product.uom_id, round=False,
            raise_if_failure=False) or 1.0
        return total / bom_qty

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
        """Ventas facturadas del período, por producto.

        Devuelve ``{product_id: dict}`` con:

        ==================  ====================================================
        ``qty``             unidades netas (dedup del triplete lista/desc/neta)
        ``revenue``         facturado en MONEDA DE LA COMPAÑÍA (Σ ``-balance``)
        ``divisas``         texto con las divisas distintas a la local
        ``divisa_id``       la divisa extranjera con MÁS facturación (o None)
        ``qty_divisa``      unidades facturadas en esa divisa
        ``revenue_divisa``  facturado en esa divisa, sin convertir
                            (Σ ``-amount_currency``)
        ``revenue_mxn_divisa``  esa misma facturación, en moneda local
                            (el par de los dos da el TC efectivo)
        ==================  ====================================================

        Dedup del triplete (lista+/descuento−/neta+) para qty; el revenue suma
        las 3 líneas (cancelan aritméticamente). ``out_refund`` resta.

        El revenue en moneda local sale de ``aml.balance``, NO de
        ``price_subtotal`` (moneda del documento): así una factura en USD entra
        con su valor real en pesos y no mezcla dólares con pesos. Para una línea
        de ingreso, balance es crédito (negativo) en venta y débito (positivo)
        en nota de crédito → ``SUM(-balance)`` suma ventas y resta devoluciones
        sin necesitar el CASE por move_type. ``amount_currency`` lleva el mismo
        signo, así que el importe en divisa se obtiene igual; el par de los dos
        da el TC efectivo de las facturas del mes.

        Solo cuentas de tipo 'income' (401.x ventas, 402.x rebajas/dev):
        una factura contra 'utilidad en venta de activo fijo' (income_other,
        p.ej. la venta de una rama Icomatex por $11.3M en 2026-03) o contra
        anticipos de clientes (liability) NO es venta de producto y antes
        contaminaba precio y contribución del mes."""
        date_to = period + relativedelta(months=1)
        company_currency = self.env.company.currency_id
        self.env.cr.execute("""
            WITH lines AS (
                SELECT aml.move_id, aml.product_id, aml.quantity,
                       aml.balance, aml.amount_currency,
                       am.move_type, am.currency_id
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
            qty_dedup AS (
                SELECT DISTINCT ON (move_id, product_id, ABS(quantity))
                       move_id, product_id, currency_id, quantity, move_type
                FROM lines
                ORDER BY move_id, product_id, ABS(quantity)
            ),
            qty_agg AS (
                SELECT product_id, currency_id,
                       SUM(CASE WHEN move_type = 'out_refund'
                                THEN -quantity ELSE quantity END) AS qty
                FROM qty_dedup
                GROUP BY 1, 2
            ),
            rev_agg AS (
                SELECT product_id, currency_id,
                       SUM(-balance) AS revenue,
                       SUM(-amount_currency) AS revenue_cur
                FROM lines
                GROUP BY 1, 2
            )
            SELECT r.product_id, r.currency_id, cur.name,
                   COALESCE(q.qty, 0), r.revenue, r.revenue_cur
            FROM rev_agg r
            LEFT JOIN qty_agg q
                   ON q.product_id = r.product_id
                  AND q.currency_id IS NOT DISTINCT FROM r.currency_id
            LEFT JOIN res_currency cur ON cur.id = r.currency_id
        """, (period, date_to, self.env.company.id))

        result = {}
        # {product_id: {currency_id: (qty, revenue_mxn, revenue_cur)}} de las
        # divisas EXTRANJERAS, para elegir después la dominante por facturación.
        foreign = {}
        for pid, cur_id, cur_name, qty, revenue, revenue_cur in \
                self.env.cr.fetchall():
            row = result.setdefault(pid, {
                'qty': 0.0, 'revenue': 0.0, 'divisas': [],
                'divisa_id': None, 'qty_divisa': 0.0, 'revenue_divisa': 0.0,
                'revenue_mxn_divisa': 0.0,
            })
            row['qty'] += qty or 0.0
            row['revenue'] += revenue or 0.0
            if cur_id and cur_id != company_currency.id:
                if cur_name and cur_name not in row['divisas']:
                    row['divisas'].append(cur_name)
                foreign.setdefault(pid, {})[cur_id] = (
                    qty or 0.0, revenue or 0.0, revenue_cur or 0.0)
        for pid, by_cur in foreign.items():
            # La divisa dominante = la de mayor facturación (en pesos, para
            # que sean comparables entre sí monedas distintas).
            cur_id = max(by_cur, key=lambda c: abs(by_cur[c][1]))
            qty, rev_mxn, rev_cur = by_cur[cur_id]
            result[pid].update({
                'divisa_id': cur_id,
                'qty_divisa': qty,
                'revenue_divisa': rev_cur,
                'revenue_mxn_divisa': rev_mxn,
            })
        for row in result.values():
            row['divisas'] = ', '.join(sorted(row['divisas']))
        return result

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
        ctx = self._engine_ctx(product_ids, factores)

        # El ajuste de MP necesita el caché de costos ya caliente, así que se
        # resuelve DESPUÉS del contexto y antes del loop. Compara la receta
        # contra el costo primo del mayor sobre la misma ventana de suavizado.
        window = factores.window_months or 12
        win_to = period + relativedelta(months=1)
        win_from = win_to - relativedelta(months=window)
        mp_gl, mp_modelada, mp_ajuste = self._mp_ajuste(win_from, win_to, ctx)
        factores.write({'mp_gl_month': mp_gl,
                        'mp_modelada_month': mp_modelada,
                        'mp_ajuste': mp_ajuste})

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

        # Filas huérfanas: quedaron de una corrida anterior y su producto ya
        # no está en el alcance del período (sin venta válida y sin BOM
        # vendible). Pasa cuando una regla nueva excluye ventas — el filtro
        # de cuentas 'income' sacó la venta de la rama Icomatex ($11.3M,
        # 2026-03) pero el upsert nunca tocaba su fila vieja y el monto
        # sobrevivía cada recálculo.
        stale = self.browse([
            rec.id for pid, rec in existing.items() if pid not in product_ids])
        if stale:
            _logger.info('qb.costo.producto: %s filas huérfanas eliminadas '
                         'para %s', len(stale), period)
            stale.unlink()

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
    def cron_recompute_current_month(self):
        """Mantiene fresco el mes EN CURSO sin esperar al cierre. El cron
        mensual (día 1) cierra el mes anterior; éste refresca el actual, para
        que el reporte no requiera 'Recalcular' a mano entre cierres."""
        today = fields.Date.today()
        self.action_recompute_period(date(today.year, today.month, 1))
        return True

    @api.model
    def action_recompute_year(self, year=None):
        """Recalcula el costo por producto de TODOS los meses del año, de enero
        al mes en curso (o a diciembre para un año pasado). Corre
        action_recompute_period por cada mes; idempotente. Útil para ver el
        reporte del año completo (luego el pivote suma por producto)."""
        today = fields.Date.today()
        year = int(year) if year else today.year
        last_month = today.month if year == today.year else 12
        for m in range(1, last_month + 1):
            self.action_recompute_period(date(year, m, 1))
        _logger.info('qb.costo.producto: recalculado el año %s (meses 1-%s)',
                     year, last_month)
        return True

    @api.model
    def _compute_product_vals(self, product, period, factores, sales, ctx,
                              Ruteo, Peso):
        """Vals de qb.costo.producto para UN producto. Devuelve
        (vals, fab_absorbida × qty) para el acumulado de cobertura."""
        bucket, centros = Ruteo.resolve(product, ctx['rules'])
        kg = Peso.resolve_kg_per_unit(product, ctx['peso_cache'])
        peso_source = Peso.resolve_kg_source(product, ctx['peso_cache'])
        m_per_kg = Peso.resolve_m_per_kg(product, ctx['peso_cache'])
        uom_name = (product.uom_id.name or '').lower()
        is_kg = uom_name in KG_UOM_NAMES
        venta = sales.get(product.id) or {}
        qty = venta.get('qty', 0.0)
        revenue = venta.get('revenue', 0.0)
        divisa = venta.get('divisas', '')
        # qty neta ≤ 0 (devoluciones > ventas en el período) daría un precio
        # negativo que envenena márgenes y alertas: se trata como sin ventas.
        precio = revenue / qty if qty > 0 else 0.0
        # Los totales del período se calculan sobre la qty EFECTIVA: sin
        # precio válido no hay nada que costear y todo total queda en 0
        # (ventas_total sí conserva el hecho contable).
        qty_efectiva = qty if precio else 0.0
        qty_divisa = venta.get('qty_divisa', 0.0)
        revenue_divisa = venta.get('revenue_divisa', 0.0)

        mp = self._mp_cost_unit(product, ctx=ctx)
        es_importado = self._es_importado(product, bucket)
        # Ajuste de MP: acerca la receta teórica a la materia prima que de
        # verdad se consumió. NO aplica a importados — su MP es precio de
        # compra más aduana, no materia prima que la planta consuma.
        if not es_importado and bucket != 'subproducto':
            mp *= factores.mp_ajuste or 1.0
        # Parte de la MP que es aduana (informativa: ya está dentro de mp).
        f_imp = ctx.get('import_factor', 0.0)
        importacion = (mp * f_imp / (1.0 + f_imp)
                       if f_imp and es_importado else 0.0)
        energia = 0.0 if bucket in ('importado', 'subproducto') \
            else factores.energia_por_kg * kg
        fab = self._fab_unit(bucket, is_kg, kg, m_per_kg, factores)
        op = factores.op_pct * precio
        variable = mp + energia
        produccion = variable + fab
        absorbido = produccion + op
        contrib = precio - variable
        bruto = precio - produccion
        hours_per_unit = self._hours_per_unit(centros, is_kg, kg, m_per_kg)

        peso_relevante = not is_kg and bucket in (
            'tela', 'entretela_tejida', 'entretela_carda')
        if qty and precio and precio < variable:
            alerta = 'bajo_variable'
        elif qty and precio and precio < absorbido:
            alerta = 'bajo_absorbido'
        elif not kg and peso_relevante:
            alerta = 'sin_peso'
        elif peso_relevante and peso_source in Peso.PESO_SOURCES_ESTIMADAS:
            # El peso es una adivinanza (código o weight de Odoo), no medido:
            # energía/fabricación pueden estar mal → hay que verificarlo.
            alerta = 'peso_estimado'
        else:
            alerta = 'ok'

        vals = {
            'period': period,
            'product_id': product.id,
            'product_bucket': bucket,
            'uom_name': product.uom_id.name,
            'kg_per_unit': kg,
            'peso_source': peso_source,
            'm_per_kg': m_per_kg,
            'qty_vendida': qty,
            'precio_prom': precio,
            'ventas_total': revenue,
            'divisa_venta': divisa or False,
            'divisa_id': venta.get('divisa_id') or False,
            'qty_divisa': qty_divisa,
            'ventas_total_divisa': revenue_divisa,
            'precio_prom_divisa':
                revenue_divisa / qty_divisa if qty_divisa > 0 else 0.0,
            'tc_prom': (venta.get('revenue_mxn_divisa', 0.0)
                        / revenue_divisa) if revenue_divisa else 0.0,
            'mp_unit': mp,
            'importacion_unit': importacion,
            'energia_unit': energia,
            'costo_variable': variable,
            'fab_unit': fab,
            'costo_produccion': produccion,
            'op_unit': op,
            'costo_absorbido': absorbido,
            'mp_total': mp * qty_efectiva,
            'importacion_total': importacion * qty_efectiva,
            'energia_total': energia * qty_efectiva,
            'fab_total': fab * qty_efectiva,
            'op_total': op * qty_efectiva,
            'costo_variable_total': variable * qty_efectiva,
            'costo_produccion_total': produccion * qty_efectiva,
            'costo_absorbido_total': absorbido * qty_efectiva,
            'margen_contribucion': contrib if precio else 0.0,
            'margen_contribucion_pct':
                100.0 * contrib / precio if precio else 0.0,
            'margen_bruto': bruto if precio else 0.0,
            'margen_bruto_pct':
                100.0 * bruto / precio if precio else 0.0,
            'margen_bruto_total': bruto * qty if precio else 0.0,
            'margen_absorbido': precio - absorbido if precio else 0.0,
            'margen_absorbido_pct':
                100.0 * (precio - absorbido) / precio if precio else 0.0,
            'margen_neto_total': (precio - absorbido) * qty if precio else 0.0,
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
        peso_source = Peso.resolve_kg_source(product)
        m_per_kg = Peso.resolve_m_per_kg(product)
        is_kg = (product.uom_id.name or '').lower() in KG_UOM_NAMES
        mp = self._mp_cost_unit(
            product, import_factor=factores.factor_importacion)
        if not self._es_importado(product, bucket) and bucket != 'subproducto':
            mp *= factores.mp_ajuste or 1.0
        energia = 0.0 if bucket in ('importado', 'subproducto') \
            else factores.energia_por_kg * kg
        fab = self._fab_unit(bucket, is_kg, kg, m_per_kg, factores)
        variable = mp + energia
        op = factores.op_pct
        piso_lleno = (variable + fab) / (1.0 - op) if op < 1 else 0.0
        hours = self._hours_per_unit(centros, is_kg, kg, m_per_kg)
        mercado = self.market_price(product)
        # ¿el peso es estimado (adivinanza) y relevante para el costo?
        peso_estimado = (not is_kg
                         and bucket in ('tela', 'entretela_tejida',
                                        'entretela_carda')
                         and peso_source in Peso.PESO_SOURCES_ESTIMADAS)
        return {
            'bucket': bucket, 'centros': centros, 'kg': kg,
            'peso_source': peso_source, 'peso_estimado': peso_estimado,
            'm_per_kg': m_per_kg, 'is_kg': is_kg,
            'mp': mp, 'energia': energia, 'fab': fab, 'variable': variable,
            'op_pct': op,
            'piso_ocioso': variable, 'piso_lleno': piso_lleno,
            'precio_mercado': mercado,
            'precio_sugerido': self._precio_sugerido(
                variable, fab, op, piso_lleno, mercado),
            'target_margin': self.env['qb.costeo.factor.config'].get_param(
                'target_margin', 0.0),
            'hours_per_unit': hours,
            'factores': factores,
        }

    @api.model
    def _precio_sugerido(self, variable, fab, op, piso_lleno, mercado,
                         target=None):
        """Precio que da el margen NETO meta y nunca queda por debajo del piso
        lleno ni del mercado:  costo_producción ÷ (1 − op% − margen_meta),
        con piso en el piso lleno y en el precio de mercado real.

        La operación va en el denominador porque es % sobre venta (depende del
        precio). Con margen meta 0 el sugerido colapsa al piso lleno.

        `target` (fracción, p.ej. 0.30 = 30%) permite pedir un margen puntual
        al cotizar; si es None se usa el margen meta global de Configuración.
        """
        if target is None:
            target = self.env['qb.costeo.factor.config'].get_param(
                'target_margin', 0.0) or 0.0
        denom = 1.0 - op - target
        base = (variable + fab) / denom if denom > 0 else piso_lleno
        return max(base, piso_lleno, mercado)

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
                JOIN account_account aa ON aa.id = aml.account_id
                WHERE am.move_type IN ('out_invoice', 'out_refund')
                  AND am.state = 'posted'
                  AND aml.display_type = 'product'
                  AND aa.account_type = 'income'
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
                JOIN account_account aa ON aa.id = aml.account_id
                WHERE am.move_type IN ('out_invoice', 'out_refund')
                  AND am.state = 'posted'
                  AND aml.display_type = 'product'
                  AND aa.account_type = 'income'
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
                bom.product_qty, product.uom_id, round=False,
                raise_if_failure=False) or 1.0
            for line in bom.bom_line_ids:
                comp = line.product_id
                qty_comp = line.product_uom_id._compute_quantity(
                    line.product_qty, comp.uom_id, round=False,
                    raise_if_failure=False)
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
