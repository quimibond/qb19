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
import re
from datetime import date

from dateutil.relativedelta import relativedelta

from odoo import api, fields, models
from odoo.exceptions import UserError
from odoo.tools import html_escape

from .cuenta_map import (CUENTA_MAP_SQL, excluir_refs_sql,
                         mo_qty_sql, wo_qty_sql)

_logger = logging.getLogger(__name__)

KG_UOM_NAMES = ('kg', 'kgs', 'kilogramo', 'kilogramos')

# Dedup del "triplete" de facturación (línea de lista +, de descuento −, y
# neta +): las tres repiten la MISMA cantidad y sin colapsarlas la qty saldría
# al triple. El revenue no necesita dedup — las tres suman el neto.
#
# La regla mira el TAMAÑO del grupo, no solo la cantidad repetida: un triplete
# son exactamente tres líneas, mientras que dos rollos iguales en la misma
# factura son dos. Antes se colapsaba cualquier repetición, así que dos rollos
# de 100 m se contaban como uno y el precio promedio salía al doble. Medido
# sobre ene–ago 2026 (2,116 líneas de factura) ningún grupo llega a tres: hoy
# el dedup no descarta nada y solo estaba el riesgo.
QTY_DEDUP_SQL = """
    SELECT move_id, product_id, currency_id, quantity, move_type, mes
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
FAB_BUCKETS = ('mod', 'overhead_fab', 'depreciacion', 'arrend_maquinaria')

# Categorías cuyo pedimento NO puede viajar al costo de un producto: un
# activo fijo se deprecia, no se vende. Se quedan en la BASE del factor de
# importación —su pedimento existe y lo diluye correctamente— pero nunca
# reciben el recargo, así que esa parte del pool se queda en resultados.
ACTIVO_FIJO_RE = re.compile(r'ACTIVO FIJO')


class QbCostoFactores(models.Model):
    _name = 'qb.costo.factores'
    _description = 'Factores de costeo por período (trazabilidad)'
    _inherit = ['mail.thread']
    _order = 'period DESC'
    _rec_name = 'period'

    period = fields.Date(required=True, index=True,
                         help='Primer día del mes calculado.')
    state = fields.Selection([
        ('borrador', 'Borrador'),
        ('cerrado', 'Cerrado'),
    ], default='borrador', required=True, tracking=True, string='Estado',
        help='CERRADO congela el período: ni el cron ni un recálculo manual '
             'pueden volver a tocar sus factores ni sus costos por producto. '
             'Sin esto, el número que presentaste el mes pasado cambia solo '
             'la próxima vez que alguien recalcula, y no hay forma de '
             'defenderlo.')
    cerrado_por = fields.Many2one('res.users', string='Cerrado por',
                                  readonly=True, tracking=True)
    cerrado_el = fields.Datetime(string='Cerrado el', readonly=True,
                                 tracking=True)
    reaperturas = fields.Integer(
        string='Reaperturas', readonly=True, default=0, tracking=True,
        help='Cuántas veces se reabrió un período ya cerrado. Cualquier '
             'número distinto de cero es una señal para el auditor.')
    motivo_reapertura = fields.Text(
        string='Motivo de reapertura', tracking=True,
        help='Obligatorio para reabrir. Queda en el historial del período.')
    company_id = fields.Many2one(
        'res.company', default=lambda self: self.env.company, required=True)
    window_months = fields.Integer(string='Ventana de suavizado (meses)')
    fab_ventana_desde = fields.Date(
        string='Ventana del pool fabril desde',
        help='Normalmente el inicio de la ventana de suavizado. Cuando un '
             'centro migra a absorción por workcenter, la ventana del pool '
             'fabril arranca en esa fecha de corte: promediar meses del '
             'régimen viejo con meses del nuevo mezclaría dos cosas distintas '
             'y el factor del mes describiría a un mes que ya no existe.')
    fab_ventana_meses = fields.Integer(
        string='Meses en la ventana fabril',
        help='Con pocos meses el pool sale ruidoso — es el precio de que el '
             'régimen acabe de cambiar. Se estabiliza solo al acumular meses.')
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
             'en moneda de la compañía. Es la base del factor. Incluye el '
             'activo fijo y los servicios: su pedimento existe y diluye el '
             'factor correctamente, aunque nunca reciban el recargo.')
    importacion_base_costeable = fields.Float(
        string='Compras importadas costeables/mes',
        help='La parte de la base que SÍ puede recibir el recargo: materia '
             'prima y producto, sin activo fijo ni servicios. La diferencia '
             'contra la base total es aduana que se queda en resultados a '
             'propósito — la de una máquina no la paga el hilo.')
    factor_importacion = fields.Float(
        string='Factor importación', digits=(16, 6),
        help='Pool ÷ base: cuánto se suma al costo de un importado por cada '
             'peso de valor de compra. 0.15 = 15% sobre el valor importado.')
    nomina_a_operacion_month = fields.Float(
        string='Nómina movida a operación/mes',
        help='Nómina que cobra por cuentas de fábrica (501.06) pero cuyo '
             'trabajo es gasto del período, detectada por la referencia de '
             'la póliza (config «nomina_operacion_refs», hoy DISEÑO): '
             'desarrollar producto no es costo de fabricar lo que ya se '
             'vende. Se resta del pool fabril y se suma a operación — la '
             'pagan TODOS los productos. La administración de la planta se '
             'queda en fabril: administrar el sitio productivo sí es '
             'overhead (IAS 2).')
    inspeccion_pool_month = fields.Float(
        string='Inspección de importados/mes',
        help='La parte del centro Inspección y Empaque que trabaja para la '
             'reventa importada (OPs TL/CONV de productos \' I\'). Se saca '
             'del pool fabril — las telas la estaban pagando — y se cobra a '
             'los importados por metro.')
    inspeccion_share = fields.Float(
        digits=(16, 4), string='Share headcount inspección',
        help='Fracción de la plantilla fabril que trabaja en Inspección y '
             'Empaque (headcount vivo de RH vía el mapa centro→'
             'departamentos). Proxy de nómina: la 501.06 no distingue '
             'departamentos.')
    inspeccion_m_month = fields.Float(
        string='Metros importados inspeccionados/mes',
        help='Metros de producto \' I\' convertidos en OPs TL/CONV, '
             'promedio mensual de la ventana fabril.')
    factor_inspeccion_m = fields.Float(
        string='Inspección $/m', digits=(16, 4),
        help='Costo de inspección y reempaque por metro. Lo cargan los '
             'importados como su capa de fabricación (no fabrican, pero sí '
             'se inspeccionan y reempacan — «todo lo importado se '
             'inspecciona»).')
    ventas_pool_month = fields.Float(string='Ventas/mes (promedio)')
    entretela_pool_month = fields.Float(string='Pool entretelas/mes')
    absorcion_bruta_month = fields.Float(
        string='Absorbido por Odoo/mes (bruto)',
        help='Saldo acreedor de la cuenta de costos fabriles aplicados a '
             'producción: lo que los workcenters capitalizaron al AVCO del '
             'producto vía tarifa por hora. Es el hecho contable, no un '
             'parámetro: si la tarifa absorbe de más o de menos, esto se '
             'mueve solo.')
    absorcion_ya_fuera_month = fields.Float(
        string='Absorbido ya excluido/mes',
        help='La parte de lo absorbido que el pool YA no traía: las cuentas '
             'etiquetadas al centro absorbido (que salieron de los buckets '
             'fabriles) más su renta contractual (que salió de la renta de '
             'centros). Restar el bruto completo las quitaría dos veces.')
    absorcion_pool_month = fields.Float(
        string='Absorbido por Odoo/mes (neto)',
        help='Bruto − ya excluido: el remanente que el centro absorbido '
             'aportaba al pool por cuentas SIN etiquetar (nómina, indirectos '
             'genéricos, depreciación). Es lo único que falta restar, porque '
             'ese costo ya viaja dentro del inventario y la venta lo libera '
             'solo. Si sale en 0 con bruto > 0, la tarifa absorbe menos que '
             'lo que el centro ya tenía etiquetado: revisa la tarifa o la '
             'clasificación de sus cuentas.')
    centros_absorbidos = fields.Char(
        string='Centros absorbidos por Odoo',
        help='Qué centros estaban en absorción por workcenter en ESTE '
             'período. Queda guardado para que un mes viejo se pueda leer '
             'con el régimen que de verdad tuvo.')
    centros_capa = fields.Char(
        string='Centros en capa',
        help='Qué centros repartió el módulo con sus factores en este período.')
    renta_contractual_pool = fields.Float(
        string='Renta contractual/mes',
        help='Σ de la renta contractual de TODOS los centros fabriles. '
             'Sustituye a las cuentas de renta del GL, que se pagan a saltos. '
             'La parte de entretelas entra aquí y sale otra vez con su pool '
             'propio, para que el pool de tela no pague una renta ajena.')
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
    utilizacion_pond_pct = fields.Float(
        string='Utilización ponderada %',
        help='Las dos utilizaciones pesadas por el share peso/largo, que es '
             'como el pool se reparte de verdad. Cuando un lado se queda sin '
             'centros en capa su share es 0, así que esta cifra no se cae '
             'sola por eso — a diferencia de mirar solo la de kg.')
    confiabilidad = fields.Selection(
        [('ok', 'Comparable'),
         ('parcial', 'Producción baja en la ventana'),
         ('mala', 'Ventana casi sin producción')],
        string='Confiabilidad', default='ok',
        help='Si la producción de la ventana quedó muy por debajo de la '
             'capacidad normal, el costo unitario de este período NO es '
             'comparable con uno normal: la energía es variable y se divide '
             'entre los kilos REALES, así que su $/kg se infla en la misma '
             'proporción. Puede ser ociosidad de verdad o producción que '
             'todavía no se registraba en Odoo — en los dos casos el número '
             'no sirve para comparar contra otro mes.')
    confiabilidad_detalle = fields.Text(
        string='Por qué', help='Qué se midió y en cuánto queda inflado.')
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
    op_pct = fields.Float(
        string='Operación % sobre ventas',
        help='Σ cuentas de operación ÷ Σ ventas. Es lo correcto para COTIZAR '
             '(el piso a planta llena resuelve qué precio deja cubierta una '
             'operación que es % de la venta), pero no para reportar costo.')
    op_rate = fields.Float(
        string='Operación sobre costo de producción', digits=(16, 6),
        help='Pool de operación ÷ costo de producción de lo vendido. Es el '
             'driver del costo REPORTADO: repartir la operación sobre el '
             'precio hacía que vender con descuento «abaratara» el producto. '
             '0 = driver legacy sobre ventas (parámetro `op_driver`).')
    entretela_factor_m = fields.Float(string='Factor entretela $/m')
    fab_pool_con_centro_pct = fields.Float(
        string='Pool fabril asignado a un centro %',
        help='Qué parte del pool de fabricación está clasificada CON centro '
             'de costo. Es el prerrequisito para costear por ruta real: '
             'mientras la mayoría del gasto no tenga centro, la fabricación '
             'solo se puede repartir a nivel planta (el split peso/largo) y '
             'un producto que se vende crudo termina pagando acabado.')
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

    # ------------------------------------------------------------------
    # Cierre de período: el costo deja de ser un número que se mueve solo
    # ------------------------------------------------------------------
    @api.model
    def periodo_cerrado(self, period, company=None):
        """¿El período está cerrado? Lo consultan el motor y el cron antes
        de escribir nada."""
        return bool(self.search_count([
            ('period', '=', period),
            ('company_id', '=', (company or self.env.company).id),
            ('state', '=', 'cerrado'),
        ]))

    def action_cerrar(self):
        for rec in self:
            if rec.state == 'cerrado':
                continue
            rec.write({
                'state': 'cerrado',
                'cerrado_por': self.env.user.id,
                'cerrado_el': fields.Datetime.now(),
            })
            rec.message_post(body='Período cerrado. Sus factores y sus costos '
                                  'por producto quedan congelados.')
        return True

    def action_reabrir(self):
        for rec in self:
            if rec.state != 'cerrado':
                continue
            if not (rec.motivo_reapertura or '').strip():
                raise UserError(
                    'Escribe el motivo de reapertura antes de reabrir %s. '
                    'Reabrir un período cerrado cambia números que ya se '
                    'reportaron: tiene que quedar por qué.' % rec.period)
            rec.write({'state': 'borrador',
                       'reaperturas': rec.reaperturas + 1})
            rec.message_post(
                body='Período REABIERTO (%s vez/veces). Motivo: %s'
                     % (rec.reaperturas, rec.motivo_reapertura))
        return True


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
             '— sin el dedup el precio saldría ~1/3 en productos con '
             'triplete. La regla mira el tamaño del grupo (un triplete son '
             'tres líneas), así que dos rollos iguales en la misma factura '
             'siguen contando como dos.')
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
             '(IGI, DTA, PRV, agente aduanal, flete). NO es una capa aparte: '
             'ya está dentro de la MP, se muestra para poder auditarla.\n\n'
             'Solo el recargo de ESTE producto, cuando él mismo se compra '
             'importado. La aduana de un componente importado (el hilo, por '
             'ejemplo) vive dentro de la MP del componente y llega a la tela '
             'por la receta, sin aparecer en este renglón.\n\n'
             'Sale en 0 con el driver «landed» (el default), que es el que no '
             'prorratea: ahí la aduana se captura con el landed cost de Odoo '
             'sobre cada recepción.')
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

    def _bloquear_si_cerrado(self):
        """Un período cerrado es un snapshot: sus filas no se escriben ni se
        borran. El guard del recálculo ya lo evita por el camino normal; esto
        cierra la puerta también a un write suelto desde la UI o un script.

        `qb_periodo_verificado` lo pone el propio recálculo cuando YA comprobó
        el estado: sin eso, el loop haría una consulta de períodos cerrados por
        cada uno de los ~1,250 productos.
        """
        ctx = self.env.context
        if ctx.get('qb_periodo_verificado') or \
                ctx.get('qb_forzar_periodo_cerrado'):
            return
        Factores = self.env['qb.costo.factores']
        cerrados = {
            (f.period, f.company_id.id)
            for f in Factores.search([('state', '=', 'cerrado')])}
        for rec in self:
            if (rec.period, rec.company_id.id) in cerrados:
                raise UserError(
                    'El período %s está cerrado: su costo por producto es un '
                    'snapshot y no se modifica. Reábrelo con motivo si de '
                    'verdad hay que moverlo.' % rec.period)

    def write(self, vals):
        self._bloquear_si_cerrado()
        return super().write(vals)

    def unlink(self):
        self._bloquear_si_cerrado()
        return super().unlink()

    # ------------------------------------------------------------------
    # Pools GL
    # ------------------------------------------------------------------
    @api.model
    def _pool_by_month(self, buckets, date_from, date_to,
                       es_variable=None, centro_id=None, sign=1.0,
                       es_renta=None, con_centro=None, excluir_centros=None,
                       incluir_centros=None):
        """Σ balance de las cuentas clasificadas en `buckets`, por mes.

        Devuelve {date_mes: monto}. sign=-1 para ingresos (saldo acreedor).
        `es_renta=True` aísla las cuentas de renta de inmueble (para poder
        sustituirlas por la renta contractual sin contarlas dos veces).
        `excluir_centros` deja fuera las cuentas etiquetadas a esos centros
        (conservando las sin centro) e `incluir_centros` hace lo contrario:
        SOLO las de esos centros. Son complementarias a propósito — con las
        dos se puede medir cuánto de un pool aporta un centro, que es lo que
        permite restar lo absorbido sin restarlo dos veces.
        """
        query = """
            WITH cuenta_map AS (%s)
            SELECT date_trunc('month', aml.date)::date AS mes,
                   SUM(aml.balance * m.allocation_pct / 100.0) AS monto
            FROM account_move_line aml
            JOIN account_move am ON am.id = aml.move_id
            JOIN cuenta_map m ON m.account_id = aml.account_id
            WHERE m.bucket IN %%s
              AND aml.parent_state = 'posted'
              AND aml.date >= %%s AND aml.date < %%s
              AND aml.company_id = %%s
              AND {CIERRE}
              -- Filtro de LÍNEA: una cuenta que mezcla naturalezas deja
              -- pasar solo las líneas cuyo concepto lo diga. Hoy:
              -- `501.01.02` solo cuenta con `SP/`, que es la merma real;
              -- los embarques y el encogimiento son ajustes, no costo.
              AND (m.filtro_etiqueta = ''
                   OR position(m.filtro_etiqueta in
                               coalesce(aml.name, '')) > 0)
        """.replace('{CIERRE}', excluir_refs_sql(self.env)) % CUENTA_MAP_SQL
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
        if con_centro is not None:
            query += (' AND m.centro_id IS NOT NULL' if con_centro
                      else ' AND m.centro_id IS NULL')
        if excluir_centros:
            query += ' AND (m.centro_id IS NULL OR m.centro_id != ALL(%s))'
            params.append(list(excluir_centros))
        if incluir_centros:
            query += ' AND m.centro_id = ANY(%s)'
            params.append(list(incluir_centros))
        query += ' GROUP BY 1'
        self.env.cr.execute(query, tuple(params))
        return {row[0]: sign * row[1] for row in self.env.cr.fetchall()}

    @api.model
    def _meses_con_actividad(self, date_from, date_to):
        """Meses de la ventana con pólizas posteadas.

        Es el denominador correcto de cualquier pool: un gasto que se registra
        al PAGARSE aparece en unos meses sí y otros no, y dividir entre los
        meses en que apareció da el cargo por factura, no el costo mensual.
        Con energía en 53k / 65k / 173k según cuándo llegó el recibo, dividir
        entre tres da $97k y dividir entre los siete meses de la ventana da
        $112,678 — el segundo es el que se parece al consumo real.
        """
        self.env.cr.execute("""
            SELECT COUNT(DISTINCT date_trunc('month', aml.date))
            FROM account_move_line aml
            WHERE aml.parent_state = 'posted'
              AND aml.company_id = %s
              AND aml.date >= %s AND aml.date < %s
        """, (self.env.company.id, date_from, date_to))
        return self.env.cr.fetchone()[0] or 0

    @api.model
    def _smooth(self, by_month, meses=None, exclude_negative=True):
        """Promedio mensual de un pool.

        `meses` es el número de meses de la VENTANA, no de los meses en que la
        cuenta tuvo movimiento: ver `_meses_con_actividad`. Sin él se conserva
        el comportamiento viejo (dividir entre los meses con dato), que sirve
        para llamadas sueltas donde la ventana no se conoce.

        Los meses NEGATIVOS se descartan de los dos lados de la división: son
        los reversos del cierre anual (diciembre 2025 metió +$163M de débito a
        cuentas de ingreso), y dejarlos en el denominador subvaluaría el
        promedio tanto como dejarlos en el numerador lo hundiría.
        """
        valores = [v for v in by_month.values()
                   if not exclude_negative or v >= 0]
        if meses is None:
            positivos = [v for v in valores if v > 0]
            return sum(positivos) / len(positivos) if positivos else 0.0
        descartados = len([v for v in by_month.values() if v < 0]) \
            if exclude_negative else 0
        n = max(meses - descartados, 1)
        return sum(valores) / n

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

    @api.model
    def _meses_sin_estiramiento(self, date_from, date_to):
        """Meses de la ventana con encogimiento pero SIN estiramiento.

        El ajuste de metros resta el encogimiento y suma el estiramiento, que
        se compensan. Si el estiramiento se detiene y el encogimiento no, la
        resta queda sin su contrapeso y el denominador se sobrecorrige: el
        costo por metro sube por una operación que dejó de hacerse, no porque
        la planta gaste más. Se detectó parado desde el 30-jun-2026.
        """
        self.env.cr.execute("""
            SELECT date_trunc('month', sm.date)::date AS mes,
                   COUNT(*) FILTER (WHERE spt.sequence_code LIKE '%%ENC%%')
                       AS encogimiento,
                   COUNT(*) FILTER (WHERE spt.sequence_code LIKE '%%OP-EST%%')
                       AS estiramiento
            FROM stock_move sm
            JOIN stock_picking_type spt ON spt.id = sm.picking_type_id
            WHERE sm.state = 'done'
              AND sm.company_id = %s
              AND (spt.sequence_code LIKE '%%ENC%%'
                   OR spt.sequence_code LIKE '%%OP-EST%%')
              AND sm.date >= %s AND sm.date < %s
            GROUP BY 1
            ORDER BY 1
        """, (self.env.company.id, date_from, date_to))
        return [mes for mes, enc, est in self.env.cr.fetchall()
                if enc and not est]

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

        # Régimen híbrido. Un centro cuyos workcenters ya capitalizan (tarifa
        # por hora + cuenta de costos aplicados) NO puede seguir en el pool:
        # Odoo mete su costo al AVCO del producto y la venta lo libera. Si el
        # módulo lo repartiera además con sus factores, el mismo peso se
        # cobraría dos veces.
        absorbidos = Centro.absorbidos_en(period)
        excluir = absorbidos.ids

        # Durante una migración, promediar el pool a doce meses MEZCLA
        # REGÍMENES: los meses anteriores al corte llevan el gasto del centro
        # completo y los posteriores no. El factor de septiembre tiene que
        # describir a septiembre, así que la ventana del pool fabril arranca
        # en el corte más reciente. Sale ruidosa el primer mes y se estabiliza
        # sola conforme se acumulan meses del régimen nuevo.
        fab_from = date_from
        if absorbidos:
            fab_from = max([date_from] + absorbidos.mapped('fecha_absorcion'))
        fab_meses = self._meses_con_actividad(fab_from, date_to)
        meses = self._meses_con_actividad(date_from, date_to)

        fab_by_month = self._pool_by_month(FAB_BUCKETS, fab_from, date_to,
                                           es_variable=False,
                                           excluir_centros=excluir)
        energia_by_month = self._pool_by_month(('energia',), date_from, date_to)
        op_by_month = self._pool_by_month(('operacion',), date_from, date_to)
        ventas_by_month = self._pool_by_month(('ventas',), date_from, date_to,
                                              sign=-1.0)

        # Entretelas: su MOD del GL + renta contractual + extra configurable.
        # Se RESTA del pool de tela (split quirúrgico) y forma su factor $/m.
        ent_centros = Centro.search([('driver_principal', '=', 'largo'),
                                     ('nature', '=', 'fabril_directo'),
                                     ('code', 'ilike', 'ENTRETELA'),
                                     ('id', 'not in', excluir)])
        entretela_pool = 0.0
        entretela_en_pool = 0.0
        entretela_m = 0.0
        if ent_centros:
            # MOD sumado sobre TODOS los centros de entretela (antes tomaba
            # sólo el primero mientras la renta sí sumaba todos → asimétrico).
            ent_mod = sum(
                self._smooth(self._pool_by_month(
                    ('mod',), fab_from, date_to, centro_id=c.id),
                    meses=fab_meses)
                for c in ent_centros)
            # Dos cifras distintas a propósito:
            #  · `entretela_en_pool` es lo que entretelas toma de la bolsa
            #    común (su MOD del GL y su renta contractual, que sí se sumó
            #    al total). Eso es lo único que se le puede RESTAR a tela.
            #  · `entretela_pool` financia además su overhead extra
            #    capturado a mano, que nunca estuvo en la bolsa común: restarlo
            #    de tela le quitaría dinero que tela nunca tuvo.
            entretela_en_pool = (
                ent_mod + sum(ent_centros.mapped('renta_contractual_mxn')))
            entretela_pool = (
                entretela_en_pool
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
            FAB_BUCKETS, fab_from, date_to, es_variable=False, es_renta=True,
            excluir_centros=excluir)
        fab_sin_renta = {mes: monto - renta_by_month.get(mes, 0.0)
                         for mes, monto in fab_by_month.items()}
        renta_gl = (sum(renta_by_month.values()) / fab_meses
                    if fab_meses else 0.0)
        # La renta contractual entra al total de TODOS los centros fabriles,
        # entretelas incluidas; lo que entretelas se lleva sale después con
        # `entretela_en_pool`. Sumar solo las no-entretela y restar igual la de
        # entretelas le quitaría a tela una renta que nunca se le sumó.
        renta_centros = Centro.search([
            ('nature', 'in', ('fabril_directo', 'fabril_indirecto')),
            ('id', 'not in', excluir),
        ])
        renta_contractual = sum(renta_centros.mapped('renta_contractual_mxn'))

        # Lo que Odoo capitalizó de verdad: el saldo ACREEDOR de la cuenta de
        # costos fabriles aplicados a producción. No es un parámetro que haya
        # que mantener al día — es el hecho contable, y se autocorrige si la
        # tarifa por hora absorbe de más o de menos.
        absorcion_bruta = self._smooth(
            self._pool_by_month(('absorcion_odoo',), fab_from, date_to,
                                sign=-1.0), meses=fab_meses)

        # ...pero el pool del que se resta YA NO TRAE al centro completo. Dos
        # exclusiones anteriores le quitaron su parte:
        #  · `excluir_centros` sacó de `fab_by_month` las cuentas etiquetadas
        #    al centro absorbido (en TEJIDO: energéticos y agujados, ~179k/mes)
        #  · `renta_centros` dejó fuera su renta contractual (284,269/mes)
        # La tarifa por hora, en cambio, capitaliza el costo COMPLETO del
        # centro — renta y cuentas etiquetadas incluidas. Restar el abono
        # entero quitaría esas dos partidas por segunda vez y subvaluaría el
        # factor de los centros que siguen en capa (~463k/mes con la tarifa de
        # sep-2026, ~12% del pool). Así que se resta solo el REMANENTE: lo que
        # el centro absorbido aportaba al pool por cuentas SIN etiquetar
        # (nómina de 501.06, indirectos genéricos de 504.01, depreciación),
        # que es lo único que las exclusiones no pudieron quitar.
        absorcion_ya_fuera = 0.0
        if absorbidos:
            absorcion_ya_fuera = (
                self._smooth(self._pool_by_month(
                    FAB_BUCKETS, fab_from, date_to, es_variable=False,
                    es_renta=False, incluir_centros=excluir), meses=fab_meses)
                # Sólo la renta que `renta_centros` habría sumado: la de un
                # centro admin nunca estuvo en el pool, así que descontarla
                # aquí haría lo contrario de lo que este bloque arregla.
                + sum(absorbidos.filtered(
                    lambda c: c.nature in ('fabril_directo',
                                           'fabril_indirecto')
                ).mapped('renta_contractual_mxn')))
        absorcion_pool = max(absorcion_bruta - absorcion_ya_fuera, 0.0)

        fab_pool = max(self._smooth(fab_sin_renta, meses=fab_meses)
                       + renta_contractual
                       - entretela_en_pool - absorcion_pool, 0.0)
        energia_pool = self._smooth(energia_by_month, meses=meses)
        op_pool = self._smooth(op_by_month, meses=meses)
        ventas_pool = self._smooth(ventas_by_month, meses=meses)

        kg_centros = Centro.search([('es_denominador_kg', '=', True),
                                    ('id', 'not in', excluir)])
        m_centros = Centro.search([('es_denominador_m', '=', True),
                                   ('id', 'not in', excluir)])
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

        # Inspección y empaque de importados: TODO lo importado pasa por una
        # OP TL/CONV, y la gente que la trabaja (centro INSP_EMPAQUE) cobra
        # por la 501.06 — que entra completa al pool fabril que solo
        # absorben los FABRICADOS. Las telas estaban pagando la inspección
        # de la reventa. La tasa por metro sale del costo del centro (share
        # de headcount sobre la MOD) entre TODOS los metros que atiende
        # (lo producido + lo importado convertido); del pool fabril se resta
        # solo la parte importada, que es la que ahora cobran los ' I'.
        # Nómina que cobra por la 501.06 pero no fabrica: las pólizas de
        # DISEÑO («QNAL TOLUCA, DISEÑO») son desarrollo de producto —
        # gasto del período, no costo de fabricar lo que ya se vende. Se
        # detectan por la referencia de la póliza y se mueven del pool
        # fabril a operación. La administración de la PLANTA (misma
        # cuenta) se queda en fabril: administrar el sitio productivo sí
        # es overhead (IAS 2).
        refs_op = [r.strip().upper() for r in Config.get_param_text(
            'nomina_operacion_refs', 'DISEÑO').split(',') if r.strip()]
        nomina_a_op = sum(
            self._nomina_por_ref(r, fab_from, date_to, fab_meses)
            for r in refs_op)
        fab_pool = max(fab_pool - nomina_a_op, 0.0)
        op_pool += nomina_a_op

        insp_share = self._inspeccion_headcount_share()
        mod_pool = self._smooth(self._pool_by_month(
            ('mod',), fab_from, date_to), meses=fab_meses) - nomina_a_op
        insp_m = self._conv_import_m_avg(fab_from, date_to, fab_meses)
        insp_base_m = (m_denom or 0.0) + insp_m
        factor_inspeccion_m = (mod_pool * insp_share / insp_base_m
                               if insp_base_m else 0.0)
        inspeccion_pool = factor_inspeccion_m * insp_m
        fab_pool = max(fab_pool - inspeccion_pool, 0.0)

        # Importación: los gastos e impuestos de aduana se cargan al valor de
        # lo importado (el IGI se calcula sobre el valor en aduana; flete y
        # agente escalan con el valor embarcado). Antes caían en `no_costeo` y
        # ningún producto los pagaba, o peor, en `operacion` y se prorrateaban
        # sobre TODAS las ventas — incluidas las de producto nacional.
        importacion_pool = self._smooth(
            self._pool_by_month(('importacion',), date_from, date_to),
            meses=meses)
        # Driver por default: `landed`. La aduana NO se prorratea con una
        # fórmula, porque el pedimento ya dice a qué embarque pertenece: se
        # captura con el landed cost de Odoo sobre la recepción y cae en los
        # productos que de verdad lo causaron. Prorratear sobre una base
        # promedio le cobra al hilo el pedimento de una máquina y viceversa.
        #
        # `compras` habilita el prorrateo como aproximación explícita, para
        # quien no vaya a capturar los pedimentos. Sigue siendo un proxy.
        driver = Config.get_param_text('importacion_driver', 'landed')
        importacion_base = 0.0
        importacion_base_costeable = 0.0
        import_ids = set()
        if importacion_pool and driver == 'compras':
            (importacion_base, import_ids,
             importacion_base_costeable) = self._import_purchase_base(
                date_from, date_to)
        factor_importacion = (
            Config.get_param('importacion_factor_override', 0.0)
            or (importacion_pool / importacion_base if importacion_base else 0.0))
        # Guarda contra base mal medida o cuenta mal clasificada: cualquiera
        # de las dos dispararía el costo de TODOS los importados sin aviso.
        factor_max = Config.get_param('importacion_factor_max', 1.0) or 1.0
        if factor_importacion > factor_max:
            _logger.warning(
                'qb.costo.factores %s: factor de importación %.3f supera el '
                'máximo %.3f (pool %.2f ÷ base %.2f). Se recorta — casi '
                'siempre significa que la base está incompleta (proveedores '
                'sin país capturado) o que hay una cuenta ajena en el bucket '
                '«importacion».',
                period, factor_importacion, factor_max,
                importacion_pool, importacion_base)
            factor_importacion = factor_max

        ws = Config.get_param('fab_weight_share', 0.67)
        # El share se calibró con TODOS los centros dentro. Cuando un lado se
        # queda sin centros en capa, repartirle una fracción del pool dejaría
        # dinero sin absorber en un factor que ya no tiene denominador.
        if not kg_centros and m_centros:
            ws = 0.0
        elif not m_centros and kg_centros:
            ws = 1.0
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

        # Qué tanto del pool fabril tiene centro asignado. No cambia ningún
        # número: mide el camino que falta para poder costear por ruta real.
        fab_con_centro = self._smooth(self._pool_by_month(
            FAB_BUCKETS, fab_from, date_to, es_variable=False,
            con_centro=True, excluir_centros=excluir), meses=fab_meses)
        fab_gl_total = self._smooth(fab_by_month, meses=fab_meses)
        fab_con_centro_pct = (100.0 * fab_con_centro / fab_gl_total
                              if fab_gl_total else 0.0)

        # Costo de la capacidad ociosa: la parte del pool fijo que la
        # producción real NO alcanza a absorber contra la capacidad normal.
        # Con denominador = producción real esto da 0 por construcción, que es
        # justo el problema que el costeo normal evita.
        util_kg = kg_real / kg_denom if kg_denom else 0.0
        util_m = m_real / m_denom if m_denom else 0.0
        fab_absorbible = fab_pool * (ws * util_kg + (1 - ws) * util_m)
        fab_ocioso = max(fab_pool - fab_absorbible, 0.0)

        # ¿El costo unitario de este período se puede comparar con otro?
        #
        # La fabricación se divide entre capacidad normal, así que no se mueve
        # con la producción. La ENERGÍA sí: es variable y se divide entre los
        # kilos REALES, que es lo correcto físicamente. Pero cuando los kilos
        # de la ventana están muy por debajo de la capacidad, su $/kg se infla
        # en esa misma proporción y el producto sale caro por una razón que no
        # es su costo.
        #
        # En producción se vio crudo: enero-2024 salió con energía a $34.22/kg
        # y diciembre-2024 a $11.09/kg —3.1×— porque la ventana de los
        # primeros meses cae en 2023, cuando la producción todavía no se
        # registraba en Odoo (372 órdenes en todo 2023 contra 4,715 en 2024).
        # El margen de esos meses salía negativo por eso, no por el negocio.
        #
        # Da igual si es subregistro o paro real: en los dos casos el unitario
        # no compara contra un mes normal, y quien lea el reporte tiene que
        # saberlo sin ir a investigar.
        util_pond = ws * util_kg + (1 - ws) * util_m
        conf_parcial = Config.get_param('utilizacion_min_comparable', 0.70)
        conf_mala = Config.get_param('utilizacion_min_utilizable', 0.40)
        confiabilidad, conf_detalle = 'ok', False
        if util_pond and util_pond < conf_mala:
            confiabilidad = 'mala'
        elif util_pond and util_pond < conf_parcial:
            confiabilidad = 'parcial'
        if confiabilidad != 'ok':
            conf_detalle = (
                'La producción de la ventana quedó en %.1f%% de la capacidad '
                'normal. La energía se divide entre kilos reales, así que su '
                '$/kg está inflado alrededor de %.1f veces contra un período '
                'a capacidad, y con él el costo unitario. Revisa si la planta '
                'de verdad corrió así de bajo o si las órdenes de producción '
                'de esos meses no se estaban registrando.'
                % (100.0 * util_pond, 1.0 / util_pond))

        Factores = self.env['qb.costo.factores']
        vals = {
            'period': period,
            'window_months': window,
            'fab_ventana_desde': fab_from,
            'fab_ventana_meses': fab_meses,
            'fab_pool_month': fab_pool,
            'energia_pool_month': energia_pool,
            'op_pool_month': op_pool,
            'importacion_pool_month': importacion_pool,
            'importacion_base_month': importacion_base,
            'importacion_base_costeable': importacion_base_costeable,
            'factor_importacion': factor_importacion,
            'nomina_a_operacion_month': nomina_a_op,
            'inspeccion_pool_month': inspeccion_pool,
            'inspeccion_share': insp_share,
            'inspeccion_m_month': insp_m,
            'factor_inspeccion_m': factor_inspeccion_m,
            'ventas_pool_month': ventas_pool,
            'entretela_pool_month': entretela_pool,
            'renta_contractual_pool': renta_contractual,
            'absorcion_pool_month': absorcion_pool,
            'absorcion_bruta_month': absorcion_bruta,
            'absorcion_ya_fuera_month': absorcion_ya_fuera,
            'centros_absorbidos': ', '.join(absorbidos.mapped('code')),
            'centros_capa': ', '.join(Centro.search([
                ('nature', '!=', 'admin'),
                ('id', 'not in', excluir)]).mapped('code')),
            'renta_gl_sustituida': renta_gl,
            'kg_denom_month': kg_denom,
            'm_denom_month': m_denom,
            'kg_produccion_month': kg_real,
            'm_produccion_month': m_real,
            'utilizacion_kg_pct': 100.0 * util_kg,
            'utilizacion_m_pct': 100.0 * util_m,
            'utilizacion_pond_pct': 100.0 * util_pond,
            'confiabilidad': confiabilidad,
            'confiabilidad_detalle': conf_detalle,
            'fab_ocioso_month': fab_ocioso,
            'fab_pool_con_centro_pct': fab_con_centro_pct,
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
            if existing.state == 'cerrado' and not self.env.context.get(
                    'qb_forzar_periodo_cerrado'):
                # Puerta trasera: alguien llamando _compute_factores directo,
                # sin pasar por el guard de action_recompute_period.
                _logger.info('qb.costo.factores: %s está CERRADO — se '
                             'devuelven los factores congelados.', period)
                return existing
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
                       am.move_type, am.currency_id,
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
                {QTY_DEDUP}
            )
            SELECT mes, product_id,
                   SUM(CASE WHEN move_type = 'out_refund'
                            THEN -quantity ELSE quantity END)
            FROM dedup
            GROUP BY 1, 2
        """.format(QTY_DEDUP=QTY_DEDUP_SQL),
            (date_from, date_to, self.env.company.id))
        return {(mes, pid): qty or 0.0
                for mes, pid, qty in self.env.cr.fetchall()}

    @api.model
    def _mp_ajuste(self, date_from, date_to, ctx, qty_by_month=None):
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

        if qty_by_month is None:
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
    def _es_importado_costeable(self, product):
        """¿El pedimento de este producto puede viajar al costo de otro?

        No, si es un servicio (seguro, flete, licencia: no hay inventario que
        cargar) ni si vive en una categoría de activo fijo (una máquina se
        deprecia, no se vende). Su aduana se queda en resultados.
        """
        if product.type == 'service' or not product.is_storable:
            return False
        categ = product.categ_id
        nombre = (categ.complete_name or categ.name or '').upper()
        return not ACTIVO_FIJO_RE.search(nombre)

    @api.model
    def _import_purchase_base(self, date_from, date_to, rules=None):
        """Valor de compra mensual promedio de lo IMPORTADO, y qué productos
        lo son. Devuelve `(base_mensual, {product_id})`.

        Es la base sobre la que se reparten los gastos e impuestos de aduana:
        el IGI se calcula sobre el valor en aduana, y flete y agente escalan
        con el valor embarcado. Se promedian solo los meses CON compras — si
        se importa cada dos meses, dividir entre la ventana completa partiría
        el factor a la mitad.

        **Qué cuenta como importado.** El país del proveedor, no la moneda de
        la orden. Comprarle en dólares a un proveedor mexicano (ALPEK POLYESTER
        MEXICO, HILADOS DE ALTA CALIDAD) NO es una importación y no causa
        pedimento; comprarle a NINGBO MH INDUSTRY sí, se facture en la moneda
        que se facture.

        **La base es TODO lo importado, no solo lo que se revende.** Medido
        sobre sep 2025 – ago 2026, el valor importado se reparte ~83% materia
        prima (hilo, fibra, resina), ~9% producto de reventa y ~6% activo fijo.
        Tomar como base solo la familia de reventa multiplicaba el factor por
        once: el pedimento del hilo lo causa el hilo.

        El activo fijo se queda en la base a propósito —su pedimento existe y
        diluye el factor correctamente— pero nunca recibe el recargo, porque
        una máquina no pasa por el costo del producto. Esa parte del pool
        queda sin absorber, que es justo lo que debe pasar.

        Eso último no basta con documentarlo: el conjunto que devuelve esta
        función es QUIÉN recibe el recargo, así que se filtra aquí. En la
        ventana sep-2025/ago-2026 la base traía una ROPE OPENER AND SLITTING
        LINE de €95,000 y una decena de seguros, fletes y licencias — todos
        habrían recibido recargo de haber entrado al costeo. Su pedimento se
        queda en resultados: la aduana de una máquina no la paga el hilo.

        Devuelve `(base_mensual, {product_id costeables}, base_costeable)`.
        """
        company = self.env.company
        # El país del PROVEEDOR es el discriminante. Se compara contra el país
        # de la compañía: sin país capturado, la compra no se cuenta como
        # importación (mejor dejar dinero fuera del reparto que inventarlo).
        self.env.cr.execute("""
            SELECT date_trunc('month', po.date_order)::date AS mes,
                   pol.product_id, po.currency_id,
                   SUM(pol.price_unit * pol.product_qty
                       * (1 - COALESCE(pol.discount, 0) / 100.0)) AS monto
            FROM purchase_order_line pol
            JOIN purchase_order po ON po.id = pol.order_id
            JOIN res_partner prov ON prov.id = po.partner_id
            JOIN res_company cia ON cia.id = po.company_id
            JOIN res_partner cia_p ON cia_p.id = cia.partner_id
            WHERE po.state IN ('purchase', 'done')
              AND po.company_id = %s
              AND po.date_order >= %s AND po.date_order < %s
              AND pol.product_id IS NOT NULL
              AND prov.country_id IS NOT NULL
              AND prov.country_id IS DISTINCT FROM cia_p.country_id
            GROUP BY 1, 2, 3
        """, (company.id, date_from, date_to))
        rows = self.env.cr.fetchall()
        if not rows:
            return 0.0, set(), 0.0

        currencies = {c.id: c for c in self.env['res.currency'].browse(
            list({r[2] for r in rows if r[2]})).exists()}
        productos = self.env['product.product'].browse(
            list({r[1] for r in rows})).exists()
        costeables = {p.id for p in productos
                      if self._es_importado_costeable(p)}
        by_month = {}
        by_month_costeable = {}
        importados = set()
        for mes, product_id, currency_id, monto in rows:
            monto = monto or 0.0
            currency = currencies.get(currency_id)
            if currency and currency != company.currency_id:
                monto = currency._convert(
                    monto, company.currency_id, company, mes)
            by_month[mes] = by_month.get(mes, 0.0) + monto
            if product_id in costeables:
                by_month_costeable[mes] = (
                    by_month_costeable.get(mes, 0.0) + monto)
                importados.add(product_id)
        activos = [v for v in by_month.values() if v > 0]
        base = sum(activos) / len(activos) if activos else 0.0
        # El promedio de la parte costeable se divide entre los MISMOS meses
        # que el total: si se dividiera entre sus propios meses con compra,
        # las dos cifras no serían comparables y el porcentaje mentiría.
        base_costeable = (
            sum(by_month_costeable.get(m, 0.0) for m in by_month
                if by_month[m] > 0) / len(activos) if activos else 0.0)
        return base, importados, base_costeable

    # ------------------------------------------------------------------
    # MP: último costo de compra, explosión recursiva
    # ------------------------------------------------------------------
    @api.model
    def _last_purchase_line_map(self, product_ids, cutoff=None):
        """{product_id: purchase_order_line_id} de la última compra confirmada,
        resuelto en UN query (DISTINCT ON). Un search por hoja de BOM no
        escala: con 3k SKUs × BOMs recursivas son decenas de miles de queries.

        Con `cutoff` (fin del período costeado) toma la última compra
        CONOCIDA A ESE CORTE: la foto histórica usa el precio de su época,
        no el de hoy — si no, recalcular marzo con el hilo de agosto pinta
        márgenes que nunca existieron. Producto sin compras previas al
        corte (compró por primera vez después): su PRIMERA compra conocida,
        que es el precio más cercano a la época — nunca el de hoy.
        """
        if not product_ids:
            return {}
        # state/date_order viven en purchase_order (en Odoo 19 ya no son
        # columnas de la línea) — joinear la orden.
        date_filter = 'AND po.date_order < %s' if cutoff else ''
        params = [list(product_ids)] + ([cutoff] if cutoff else [])
        self.env.cr.execute("""
            SELECT DISTINCT ON (pol.product_id) pol.product_id, pol.id
            FROM purchase_order_line pol
            JOIN purchase_order po ON po.id = pol.order_id
            WHERE po.state IN ('purchase', 'done')
              AND pol.price_unit > 0
              AND pol.product_id = ANY(%s)
              """ + date_filter + """
            ORDER BY pol.product_id, po.date_order DESC, pol.id DESC
        """, params)
        result = dict(self.env.cr.fetchall())
        if cutoff:
            faltan = set(product_ids) - set(result)
            if faltan:
                self.env.cr.execute("""
                    SELECT DISTINCT ON (pol.product_id)
                           pol.product_id, pol.id
                    FROM purchase_order_line pol
                    JOIN purchase_order po ON po.id = pol.order_id
                    WHERE po.state IN ('purchase', 'done')
                      AND pol.price_unit > 0
                      AND pol.product_id = ANY(%s)
                    ORDER BY pol.product_id, po.date_order ASC, pol.id ASC
                """, (list(faltan),))
                result.update(self.env.cr.fetchall())
        return result

    @api.model
    def _last_purchase_pol(self, product, pol_map=None, cutoff=None):
        """La LÍNEA de la última compra confirmada (recordset, vacío si no
        hay). Con `pol_map` (de _last_purchase_line_map) no hace ningún
        search; sin él (cotizador, llamadas sueltas) busca la línea.
        `cutoff` acota la búsqueda suelta al precio de la época del período
        (el mapa ya viene acotado desde _engine_ctx)."""
        if pol_map is not None:
            pol_id = pol_map.get(product.id)
            return self.env['purchase.order.line'].browse(pol_id) if pol_id \
                else self.env['purchase.order.line']
        # order_id.state en el domain y orden por id (proxy de recencia):
        # state/date_order de la línea no son columnas propias en Odoo 19.
        domain = [
            ('product_id', '=', product.id),
            ('order_id.state', 'in', ('purchase', 'done')),
            ('price_unit', '>', 0),
        ]
        corte = [('order_id.date_order', '<', cutoff)] if cutoff else []
        pol = self.env['purchase.order.line'].search(
            domain + corte, order='id desc', limit=1)
        if not pol and cutoff:
            # Sin compras previas al corte: la PRIMERA conocida es el
            # precio más cercano a la época, nunca el de hoy.
            pol = self.env['purchase.order.line'].search(
                domain, order='id asc', limit=1)
        return pol

    @api.model
    def _pol_es_importada(self, pol):
        """¿La compra usada para el costo es una IMPORTACIÓN? El país del
        PROVEEDOR de ESA orden decide — mismo criterio que la base del
        factor. El mismo hilo comprado a un comerciante nacional (GRUPO
        FILAFIL a $65 MXN) ya trae el arancel dentro del precio: recargarle
        además el factor lo contaba dos veces. La prueba de mercado: la
        compra extranjera del mismo hilo ($2.82 USD ≈ $48.8) × 1.32 ≈ $64.5,
        justo el precio del nacional. Sin país capturado no es importación
        (consistente con la base: mejor dejar dinero fuera que inventarlo).
        """
        if not pol:
            return False
        pais_prov = pol.order_id.partner_id.country_id
        pais_cia = self.env.company.partner_id.country_id
        return bool(pais_prov) and pais_prov != pais_cia

    @api.model
    def _last_purchase_cost(self, product, pol_map=None, cutoff=None):
        """Último precio de compra confirmado, en moneda de la compañía."""
        pol = self._last_purchase_pol(product, pol_map, cutoff)
        if not pol:
            # AVCO negativo (herida de valuación de inventario, caso
            # PESFCHMO1.5X2.0 en -0.30/kg) no es un costo: acotarlo a 0
            # para que ninguna MP salga negativa.
            return max(product.standard_price or 0.0, 0.0)
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
        # Con factores (recálculo de un período) la MP se acota al precio
        # de la ÉPOCA: última compra conocida al fin de ese período. Sin
        # factores (cotizador) no hay corte — cotizar es a reposición de hoy.
        cutoff = factores.period + relativedelta(months=1) if factores \
            else None
        pol_map = self._last_purchase_line_map(leaf_ids, cutoff)
        # Warm-up del cache ORM: un solo fetch para las líneas y sus órdenes
        if pol_map:
            pols = self.env['purchase.order.line'].browse(list(pol_map.values()))
            pols.read(['price_unit', 'discount', 'order_id'])
            pols.order_id.read(['currency_id', 'date_order'])
        # Qué productos se COMPRAN importados: son los que cargan el recargo
        # de aduana. Incluye la materia prima importada (el hilo es el 83% del
        # valor importado), no solo la familia de reventa — y por eso el
        # recargo llega a la tela nacional por la receta, sin tratarla como
        # importada.
        import_ids = set()
        if factores and factores.factor_importacion:
            win_to = factores.period + relativedelta(months=1)
            win_from = win_to - relativedelta(
                months=factores.window_months or 12)
            _base, import_ids, _cost = self._import_purchase_base(
                win_from, win_to)
        multi_bom_ids = self._multi_bom_ids_set()
        return {
            'rules': self.env['qb.producto.ruteo'].search([]),
            'pol_map': pol_map,
            'multi_bom_ids': multi_bom_ids,
            # Receta ambigua → la BOM con la que se fabricó de verdad la
            # última vez; el mapa se resuelve una vez por corrida.
            'last_mo_bom': self._last_mo_bom_map(multi_bom_ids),
            # El caché de MP guarda el costo YA con aduana, así que el factor
            # tiene que vivir en el contexto de la corrida: mezclar dos
            # factores en el mismo caché daría costos incoherentes.
            'import_factor': factores.factor_importacion if factores else 0.0,
            'import_ids': import_ids,
            'peso_cache': {},
            'mp_cache': {},
        }

    @api.model
    def _mp_cost_unit(self, product, cache=None, seen=None, ctx=None,
                      import_factor=None):
        """Costo primo MP por unidad: BOM recursiva a último costo.

        Reglas: subproducto → $0 (su MP ya está en la receta del principal);
        importado → costo de compra MÁS gastos e impuestos de aduana
        (`import_factor`); sin costo propio → gemelo ' IT' de la BOM de
        conversión, luego gemelo nacional; receta AMBIGUA (>1 BOM activa) →
        la BOM de la ÚLTIMA OP terminada (cómo se fabrica hoy) y, sin OPs,
        explota TODAS y toma la MÁS CARA (nunca el AVCO de un fabricado:
        trae conversión de MOs, no solo materiales); hoja sin BOM → último
        costo de compra (fallback avg, acotado a ≥0).

        `ctx` (de _engine_ctx) comparte reglas/pol_map/cachés en loops
        grandes; sin él (cotizador, tests) resuelve todo al vuelo.
        """
        # El landed va DENTRO de la MP y no como capa aparte: así lo recoge
        # también la receta que consume el importado como componente, y la
        # cascada del cotizador y del PDF sigue cuadrando sin cambios.
        if import_factor is None:
            import_factor = (ctx or {}).get('import_factor', 0.0)
        # El recargo de aduana se aplica SOLO donde el costo viene de una
        # compra importada. Nunca después de explotar una receta: los
        # componentes importados ya lo traen dentro, y volver a aplicarlo
        # sobre el total lo contaría dos veces.
        import_ids = (ctx or {}).get('import_ids') or set()
        cache = cache if cache is not None \
            else (ctx['mp_cache'] if ctx else {})
        seen = seen if seen is not None else set()
        if product.id in cache:
            return cache[product.id]
        if product.id in seen:  # ciclo en la receta: cortar con avg
            return max(product.standard_price or 0.0, 0.0)
        seen = seen | {product.id}
        rules = ctx['rules'] if ctx else None
        pol_map = ctx['pol_map'] if ctx else None

        bucket, _centros = self.env['qb.producto.ruteo'].resolve(product, rules)
        ref = product.default_code or ''
        cost = 0.0
        if bucket == 'subproducto':
            cost = 0.0
        elif bucket == 'importado' or ref.endswith(' I'):
            # El AVCO del ' I' es la compra del ' IT' más los gastos de la
            # OP de conversión (landed real cuando el leg de gastos corre);
            # negativo no es un costo — se acota a 0 y caen los fallbacks.
            cost = max(product.standard_price or 0.0, 0.0) \
                or self._last_purchase_cost(product, pol_map)
            if not cost and ref.endswith(' I'):
                # Sin AVCO ni compra propia: el gemelo ' IT' REAL es el
                # componente de su OP de conversión — el código puede diferir
                # del prefijo (KP2032T11GO152 I se produce del
                # KP4032T11GO152 IT), así que primero la BOM y luego el ref.
                it = self._it_twin(product)
                if it:
                    cost = self._last_purchase_cost(it, pol_map)
            if not cost and ref.endswith(' I'):
                twin = self.env['product.product'].search(
                    [('default_code', '=', ref[:-2].strip())], limit=1)
                if twin:
                    # El gemelo nacional ya viene con su propio tratamiento;
                    # no se le vuelve a aplicar el recargo encima.
                    cost = self._mp_cost_unit(twin, cache, seen, ctx,
                                              import_factor)
                    import_factor = 0.0
            cost *= 1.0 + import_factor
        else:
            is_multi = self._has_multiple_boms(product, ctx)
            if is_multi:
                # Receta AMBIGUA: el producto tiene VARIAS BOMs activas.
                # NUNCA usar el AVCO de un fabricado: ese promedio trae las
                # capas de conversión de las órdenes de producción (horas ×
                # tarifa de workcenter), no solo materiales, y el modelo ya
                # cobra la conversión vía fab_unit — usarlo aquí la cobraba
                # DOS veces (caso CONTITECH: la cruda de WC090 con AVCO de
                # $107/kg cuando el hilo cuesta ~$40/kg → todo el segmento
                # industrial salía en rojo). Tampoco explotar UNA al azar
                # (_bom_find, colapsaba el costo — bug WD080). El costo debe
                # seguir a CÓMO SE FABRICA HOY: la BOM de la última OP
                # terminada — sin eso, los genéricos de prueba colgados de
                # BOMs activas inflan la MP (TJ085Q22JNT157 salía a 11.30/m
                # por un camino "MUESTRA PILOTO" cuando su receta real, la de
                # 53 de sus 55 OPs, da ~6.2). Sin OPs: explota TODAS y toma
                # la MÁS CARA, conservador para cotizar.
                boms = self._applicable_boms(product)
                bom_op = self._bom_de_ultima_op(product, boms, ctx)
                cost = self._explode_bom(bom_op, product, cache, seen, ctx,
                                         import_factor) if bom_op else 0.0
                if cost <= 0.0:
                    costs = [self._explode_bom(b, product, cache, seen, ctx,
                                               import_factor)
                             for b in boms]
                    cost = max(costs) if costs else 0.0
                if cost <= 0.0:
                    cost = self._costo_de_compra(
                        product, pol_map, import_factor, import_ids)
                std = product.standard_price or 0.0
                if std > 0.0 and cost < std * 0.05:
                    # Todas las recetas explotan a casi nada frente al AVCO
                    # (típico de genéricos "MUESTRA PILOTO" con recetas de
                    # relleno): el costo queda el explotado igual — el AVCO
                    # de un fabricado no es MP — pero se avisa para que
                    # alguien arregle las BOMs.
                    _logger.warning(
                        'qb_capacidad_costeo: receta ambigua de [%s] %s '
                        'explota a %.4f frente a AVCO %.2f — las BOMs '
                        'parecen degeneradas, revisarlas.',
                        ref, product.name, cost, std)
            else:
                bom = self.env['mrp.bom']._bom_find(product).get(product)
                cost = self._explode_bom(bom, product, cache, seen, ctx,
                                         import_factor) \
                    if bom else self._costo_de_compra(
                        product, pol_map, import_factor, import_ids)
        cache[product.id] = cost
        return cost

    @api.model
    def _costo_de_compra(self, product, pol_map, import_factor, import_ids):
        """Último costo de compra de una hoja, con el recargo de aduana si
        ESA compra fue importada.

        Aquí es donde el pedimento del hilo llega al costo de la tela: el
        hilo importado carga su aduana en SU costo, y la receta lo arrastra
        a cada tela que lo consume. La tela no se trata como importada — no
        lo es.

        El recargo sigue a la COMPRA usada, no al producto: `import_ids`
        dice qué productos PUEDEN llevarlo (se han comprado importados en
        la ventana), pero si la última compra es de un proveedor nacional,
        su precio ya trae el arancel adentro y no se recarga de nuevo
        (caso HP65P35A22/1: FILAFIL MX a $65 vs IG TEXTILE US a $48.8+32%).
        """
        cost = self._last_purchase_cost(product, pol_map)
        if import_factor and product.id in import_ids \
                and self._pol_es_importada(
                    self._last_purchase_pol(product, pol_map)):
            cost *= 1.0 + import_factor
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
    def _bom_de_ultima_op(self, product, boms, ctx=None):
        """La BOM de la última OP terminada del producto, si sigue entre las
        activas aplicables. Vacío cuando el producto nunca se ha fabricado
        (o su última receta ya no está activa): ahí decide el criterio
        conservador de explotar todas."""
        if ctx is not None and 'last_mo_bom' in ctx:
            bom_id = ctx['last_mo_bom'].get(product.id)
            if bom_id and bom_id in set(boms.ids):
                return self.env['mrp.bom'].browse(bom_id)
            return self.env['mrp.bom']
        mo = self.env['mrp.production'].search(
            [('product_id', '=', product.id), ('state', '=', 'done'),
             ('bom_id', 'in', boms.ids)],
            order='date_finished desc, id desc', limit=1)
        return mo.bom_id

    @api.model
    def _last_mo_bom_map(self, product_ids):
        """{product_id: bom_id} de la última OP terminada cuya BOM sigue
        activa — un query para todo el motor (solo hace falta para las
        recetas ambiguas)."""
        if not product_ids:
            return {}
        self.env.cr.execute("""
            SELECT DISTINCT ON (mp.product_id) mp.product_id, mp.bom_id
            FROM mrp_production mp
            JOIN mrp_bom b ON b.id = mp.bom_id AND b.active
            WHERE mp.state = 'done'
              AND mp.product_id = ANY(%s)
            ORDER BY mp.product_id,
                     mp.date_finished DESC NULLS LAST, mp.id DESC
        """, (list(product_ids),))
        return dict(self.env.cr.fetchall())

    @api.model
    def _it_twin(self, product):
        """Gemelo ' IT' de un producto de importación ' I': el componente de
        su BOM de conversión cuyo código termina en ' IT' — el código puede
        NO compartir prefijo con el ' I' (KP2032T11GO152 I se produce del
        KP4032T11GO152 IT). Fallback: mismo código + 'T'."""
        for bom in self._applicable_boms(product):
            for line in bom.bom_line_ids:
                if (line.product_id.default_code or '').endswith(' IT'):
                    return line.product_id
        ref = product.default_code or ''
        return self.env['product.product'].search(
            [('default_code', '=', ref + 'T')], limit=1)

    @api.model
    def _explode_bom(self, bom, product, cache, seen, ctx,
                     import_factor=0.0):
        """Costo MP/unidad explotando UNA receta: Σ(qty × costo_hoja) ÷ salida.
        raise_if_failure=False evita que una conversión entre categorías de
        UoM (kg vs m) tumbe el costeo — deja la cantidad sin convertir."""
        total = 0.0
        for line in bom.bom_line_ids:
            # Una receta con atributos trae líneas que solo aplican a ciertas
            # variantes. Sin este filtro, el producto cargaba componentes que
            # NO consume — su MP salía inflada por todo lo de sus hermanas.
            if line._skip_bom_line(product):
                continue
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
        El dedup colapsa solo grupos de TRES o más líneas con la misma
        cantidad: dos rollos iguales en una factura son dos ventas, no un
        triplete (ver ``QTY_DEDUP_SQL``).

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
                       am.move_type, am.currency_id,
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
            qty_dedup AS (
                {QTY_DEDUP}
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
        """.format(QTY_DEDUP=QTY_DEDUP_SQL),
            (period, date_to, self.env.company.id))

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

        Factores = self.env['qb.costo.factores']
        if Factores.periodo_cerrado(period) and not self.env.context.get(
                'qb_forzar_periodo_cerrado'):
            _logger.info(
                'qb.costo.producto: %s está CERRADO — no se recalcula. '
                'Reábrelo con motivo si de verdad hay que moverlo.', period)
            return False
        self = self.with_context(qb_periodo_verificado=True)

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
        # Las cantidades de la ventana las comparten los dos factores: una
        # sola query en vez de dos idénticas por recálculo.
        qty_ventana = self._sales_qty_by_month(win_from, win_to)
        mp_gl, mp_modelada, mp_ajuste = self._mp_ajuste(
            win_from, win_to, ctx, qty_by_month=qty_ventana)
        factores.write({'mp_gl_month': mp_gl,
                        'mp_modelada_month': mp_modelada,
                        'mp_ajuste': mp_ajuste})
        # La tasa de operación se reparte sobre el costo de producción, que ya
        # incorpora el ajuste de MP recién escrito — de ahí el orden.
        factores.op_rate = self._op_rate(
            win_from, win_to, factores, ctx, Ruteo, Peso,
            qty_by_month=qty_ventana)

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
    def cron_recompute_pendientes(self):
        """Recalcula por LOTES los períodos que una migración dejó diferidos.

        Una migración que recalcula TODOS los períodos guardados bloquea el
        build lo que tarden: con 8 períodos eran ~80 s, pero al cargar 2024 y
        2025 son 32 y el mismo build pasó a 5-6 minutos. La migración ahora
        recalcula síncrono solo el año corriente —lo que se usa para decidir—
        y deja los históricos en el parámetro `recalculo_pendiente`; este
        cron los va vaciando por lotes y se APAGA solo al terminar.

        Idempotente: un período cerrado se salta (el guard de
        `action_recompute_period` ya lo hace) y un lote interrumpido se
        repite sin daño."""
        Config = self.env['qb.costeo.factor.config']
        crudo = Config.get_param_text('recalculo_pendiente', '')
        pendientes = [p.strip() for p in (crudo or '').split(',')
                      if p.strip()]
        lote, resto = pendientes[:6], pendientes[6:]
        for iso in lote:
            self.action_recompute_period(fields.Date.from_string(iso))
        rec = Config.search([('key', '=', 'recalculo_pendiente')], limit=1)
        if rec:
            rec.value_text = ','.join(resto)
        if not resto:
            cron = self.env.ref(
                'qb_capacidad_costeo.cron_recalculo_pendientes',
                raise_if_not_found=False)
            if cron:
                cron.active = False
        _logger.info(
            'qb.costo.producto: lote diferido de %s períodos recalculado '
            '(%s); quedan %s.', len(lote), ', '.join(lote) or 'ninguno',
            len(resto))
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
        bucket, centros, kg, m_per_kg, is_kg, mp, energia, fab = \
            self._capas_produccion(product, factores, ctx, Ruteo, Peso)
        peso_source = Peso.resolve_kg_source(product, ctx['peso_cache'])
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

        # Parte de la MP que es aduana (informativa: ya está dentro de mp).
        # Solo la de ESTE producto si él mismo se compra importado; la aduana
        # de un componente importado vive dentro de la MP del componente.
        f_imp = ctx.get('import_factor', 0.0)
        es_compra_importada = (
            self._es_importado(product, bucket)
            or (product.id in (ctx.get('import_ids') or set())
                and self._pol_es_importada(self._last_purchase_pol(
                    product, ctx.get('pol_map')))))
        importacion = (mp * f_imp / (1.0 + f_imp)
                       if f_imp and es_compra_importada else 0.0)
        variable = mp + energia
        produccion = variable + fab
        # Operación sobre el costo de producción, no sobre el precio: si el
        # costo depende del precio, vender con descuento «abarata» el
        # producto y su margen se ve sano. Con op_rate en 0 (driver legacy
        # «ventas») se mantiene el reparto sobre el precio.
        op = (factores.op_rate * produccion if factores.op_rate
              else factores.op_pct * precio)
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
            # Los totales de margen se derivan del INGRESO, no de
            # `precio × qty`. Para una fila normal da exactamente lo mismo
            # (revenue = precio × qty), pero cuando la cantidad neta del
            # período es ≤ 0 —devoluciones mayores que ventas— la fila se
            # trata como «sin ventas» para no generar un precio negativo, y
            # sin embargo `ventas_total` sí conserva el ingreso negativo.
            # Calculando desde `precio` el margen salía 0 contra un ingreso
            # que no era 0, y la identidad ventas − costo = margen se rompía
            # justo ahí: 11 filas metieron $561,866 de residuo en la
            # conciliación entre enero y julio de 2026, sin causa real
            # detrás. Derivado del ingreso, la identidad se cumple por
            # construcción en toda fila.
            'margen_bruto_total': revenue - produccion * qty_efectiva,
            'margen_absorbido': precio - absorbido if precio else 0.0,
            'margen_absorbido_pct':
                100.0 * (precio - absorbido) / precio if precio else 0.0,
            'margen_neto_total': revenue - absorbido * qty_efectiva,
            'contrib_hora_maquina':
                contrib / hours_per_unit if hours_per_unit and precio else 0.0,
            'contrib_total': revenue - variable * qty_efectiva,
            'alerta': alerta,
            'centro_route': ', '.join(centros.mapped('code')),
            'factores_id': factores.id,
        }
        return vals, fab * qty

    @api.model
    def _capas_produccion(self, product, factores, ctx, Ruteo, Peso):
        """Las capas de costo de FABRICAR una unidad, en un solo lugar.

        Devuelve `(bucket, centros, kg, m_per_kg, is_kg, mp, energia, fab)`.

        Vive aparte porque lo consumen dos caminos que NO pueden divergir: el
        costo por producto del reporte, y la base sobre la que se reparte la
        operación. Si cada uno lo calculara por su cuenta, la tasa de
        operación dejaría de cuadrar contra el costo al que se aplica.
        """
        bucket, centros = Ruteo.resolve(product, ctx['rules'])
        kg = Peso.resolve_kg_per_unit(product, ctx['peso_cache'])
        m_per_kg = Peso.resolve_m_per_kg(product, ctx['peso_cache'])
        is_kg = (product.uom_id.name or '').lower() in KG_UOM_NAMES
        mp = self._mp_cost_unit(product, ctx=ctx)
        # Reventa/servicio: ni ajuste de merma ni energía — no pasa por
        # planta (caso PES1.4NG1.5: fibra revendida que cargaba $66/kg de
        # proceso que no lleva).
        if not self._es_importado(product, bucket) \
                and bucket not in ('subproducto', 'servicio'):
            mp *= factores.mp_ajuste or 1.0
        energia = 0.0 if bucket in ('importado', 'subproducto', 'servicio') \
            else factores.energia_por_kg * kg
        fab = self._fab_unit(bucket, is_kg, kg, m_per_kg, factores)
        return bucket, centros, kg, m_per_kg, is_kg, mp, energia, fab

    @api.model
    def _op_rate(self, date_from, date_to, factores, ctx, Ruteo, Peso,
                 qty_by_month=None):
        """Tasa de operación sobre el COSTO DE PRODUCCIÓN.

        El modelo cobraba la operación como porcentaje del precio de venta
        (`op = op_pct × precio`). Eso hace que el costo dependa del precio:
        si vendes más barato, el modelo te dice que costó menos, y el
        producto vendido con descuento se ve artificialmente sano. Peor aún,
        un producto SIN ventas en el mes tenía precio 0 y por lo tanto
        operación 0 — justo los productos que hay que evaluar para decidir si
        vale la pena empujarlos.

        Con driver de producción la operación se reparte sobre lo que cuesta
        fabricar, que no se mueve con el descuento del vendedor.

        OJO: esto cambia el costo REPORTADO, no el piso de precio. Para
        cotizar, `op_pct` sobre el precio sigue siendo lo correcto — el piso a
        planta llena resuelve «qué precio deja cubierta una operación que es
        % de la venta», y ahí la circularidad es la fórmula, no un error.
        """
        Config = self.env['qb.costeo.factor.config']
        if Config.get_param_text('op_driver', 'produccion') != 'produccion':
            return 0.0
        if qty_by_month is None:
            qty_by_month = self._sales_qty_by_month(date_from, date_to)
        if not qty_by_month:
            return 0.0
        Product = self.env['product.product']
        products = {p.id: p for p in Product.browse(
            list({pid for _mes, pid in qty_by_month})).exists()}
        unitario = {}
        base = 0.0
        meses = set()
        for (mes, pid), qty in qty_by_month.items():
            if qty <= 0:
                continue
            product = products.get(pid)
            if product is None:
                continue
            costo = unitario.get(pid)
            if costo is None:
                _b, _c, _kg, _mkg, _ik, mp, energia, fab = \
                    self._capas_produccion(product, factores, ctx, Ruteo, Peso)
                costo = mp + energia + fab
                unitario[pid] = costo
            base += costo * qty
            meses.add(mes)
        base_mes = base / len(meses) if meses else 0.0
        return factores.op_pool_month / base_mes if base_mes else 0.0

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
        if bucket == 'importado':
            # No fabrican, pero TODO lo importado se inspecciona y reempaca
            # (OPs TL/CONV): carga el centro de inspección por metro. Esa
            # parte ya se restó del pool fabril de los fabricados.
            f_insp = factores.factor_inspeccion_m
            return m_per_kg * f_insp if is_kg else f_insp
        # subproducto / servicio: no cargan fabricación
        return 0.0

    @api.model
    def _inspeccion_headcount_share(self):
        """Fracción de la plantilla fabril que trabaja en Inspección y
        Empaque, con el mapa centro→departamentos y el headcount vivo de RH.
        Proxy de nómina: la 501.06 entra al pool completa y no distingue
        departamentos."""
        Centro = self.env['qb.costeo.centro']
        insp = Centro.search([('code', '=', 'INSP_EMPAQUE')], limit=1)
        fabriles = Centro.search([
            ('nature', 'in', ('fabril_directo', 'fabril_indirecto'))])
        depts_insp = insp.department_ids.ids
        depts_fab = fabriles.mapped('department_ids').ids
        if not depts_insp or not depts_fab:
            return 0.0
        Emp = self.env['hr.employee']
        n_insp = Emp.search_count([('department_id', 'in', depts_insp)])
        n_fab = Emp.search_count([('department_id', 'in', depts_fab)])
        return n_insp / n_fab if n_fab else 0.0

    @api.model
    def _nomina_por_ref(self, texto, date_from, date_to, meses):
        """Nómina del bucket MOD cuyas pólizas traen `texto` en la
        referencia (p. ej. «QNAL TOLUCA, DISEÑO»): promedio mensual.
        Las pólizas de nómina se postean por departamento y la referencia
        es el único lugar donde el departamento queda escrito — el
        concepto de la línea dice «Sueldos y salarios» en todas."""
        self.env.cr.execute("""
            SELECT COALESCE(SUM(aml.balance), 0)
            FROM account_move_line aml
            JOIN account_move am ON am.id = aml.move_id
            JOIN (%s) m ON m.account_id = aml.account_id
            WHERE m.bucket = 'mod'
              AND am.state = 'posted'
              AND aml.date >= %%s AND aml.date < %%s
              AND aml.company_id = %%s
              AND position(%%s in upper(coalesce(am.ref, ''))) > 0
        """ % CUENTA_MAP_SQL, (date_from, date_to, self.env.company.id,
                               (texto or '').upper()))
        total = self.env.cr.fetchone()[0] or 0.0
        return total / max(meses or 1, 1)

    @api.model
    def _conv_import_m_avg(self, date_from, date_to, meses):
        """Metros de producto importado (' I') convertidos/inspeccionados
        por mes: OPs TL/CONV terminadas en la ventana, entre sus meses."""
        self.env.cr.execute("""
            SELECT COALESCE(SUM(mp.product_qty), 0)
            FROM mrp_production mp
            JOIN product_product pp ON pp.id = mp.product_id
            WHERE mp.state = 'done'
              AND mp.name LIKE %s
              AND pp.default_code LIKE %s
              AND mp.date_finished >= %s
              AND mp.date_finished < %s
        """, ('TL/CONV%', '% I', date_from, date_to))
        total = self.env.cr.fetchone()[0] or 0.0
        return total / max(meses or 1, 1)

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
        # Reventa/servicio: ni ajuste de merma ni energía — no pasa por
        # planta (caso PES1.4NG1.5: fibra revendida que cargaba $66/kg de
        # proceso que no lleva).
        if not self._es_importado(product, bucket) \
                and bucket not in ('subproducto', 'servicio'):
            mp *= factores.mp_ajuste or 1.0
        energia = 0.0 if bucket in ('importado', 'subproducto', 'servicio') \
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
        if self._has_multiple_boms(product):
            # Receta ambigua (>1 BOM): mismo criterio que _mp_cost_unit —
            # explota todas y sigue la MÁS CARA (nunca el AVCO de un
            # fabricado: trae conversión de MOs, no solo materiales).
            boms = self._applicable_boms(product)
            bom = max(
                boms,
                key=lambda b: self._explode_bom(b, product, {}, set(), None),
            ) if boms else self.env['mrp.bom']
        else:
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
            'Familia: %s (importados cargan solo inspección/reempaque '
            'por metro; subproductos nada).</p>'
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
