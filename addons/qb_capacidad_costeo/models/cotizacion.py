# -*- coding: utf-8 -*-
"""Cotizaciones guardadas: resultado del cotizador con sus supuestos.

Cada cotización queda con los factores usados (FX, ventana de gastos,
denominadores) para trazabilidad y comparación antes/después.
"""
from odoo import api, fields, models
from odoo.exceptions import UserError

from .glosario import GLOSARIO_HTML

# Un centro sin throughput ni turnos configurados no se puede validar. Eso no
# es «cabe» ni «no cabe»: es «no sé», y un booleano no sabe decirlo. Mientras
# la respuesta vivió solo en `capacity_ok`, el «no sé» se contaba como sí y la
# cotización afirmaba que el volumen cabía en la planta habiendo medido un
# solo centro de los tres de la ruta.
CAPACITY_STATUS = [
    ('ok', 'Cabe: todos los centros de la ruta validados'),
    ('parcial', 'Parcial: lo medido cabe, pero faltan datos de algún centro'),
    ('sin_datos', 'Sin datos: no se pudo validar ningún centro'),
    ('no_cabe', 'No cabe: un centro medido no da abasto'),
    ('sin_ruta', 'Sin ruta de fabricación: no consume capacidad'),
    ('sin_volumen', 'Falta capturar el volumen para poder validar'),
]


class QbCotizacion(models.Model):
    _name = 'qb.cotizacion'
    _description = 'Cotización de capacidad y costo'
    _inherit = ['mail.thread', 'mail.activity.mixin']
    _order = 'create_date DESC'

    name = fields.Char(required=True, default='Nueva cotización')
    company_id = fields.Many2one(
        'res.company', default=lambda self: self.env.company, required=True)
    partner_id = fields.Many2one('res.partner', string='Cliente')
    product_id = fields.Many2one(
        'product.product', string='Producto existente')
    spec_descripcion = fields.Char(
        string='Especificación nueva',
        help='Para producto que aún no existe: descripción libre.')
    spec_gramaje = fields.Float(string='Gramaje (g/m²)')
    spec_ancho = fields.Float(string='Ancho (m)')
    spec_galga = fields.Char(string='Galga')
    volumen = fields.Float(string='Volumen (unidades/mes)')
    uom_name = fields.Char(string='Unidad')
    currency_id = fields.Many2one(
        'res.currency', string='Moneda de la cotización',
        help='La moneda en la que se capturó y se presenta el precio al '
             'cliente. Los montos guardados están en MXN; el TC los traduce.')
    fx_rate = fields.Float(
        string='TC usado (MXN por 1 divisa)',
        help='Tipo de cambio de Odoo el día que se cotizó: pesos por 1 '
             'unidad de la divisa (ej. 18.50 = 1 USD costaba $18.50 MXN). '
             '1.0 o vacío = la cotización fue en MXN. Informativo para la '
             'MP: ya viene convertida a MXN al TC de cada compra.')

    # Desglose de costo por capa ($/unidad, siempre MXN)
    mp_unit = fields.Float(
        string='Materia prima $/u MXN', digits=(16, 4),
        help='Receta (BOM) explotada al ÚLTIMO costo de compra de cada '
             'componente, convertido a MXN.')
    energia_unit = fields.Float(
        string='Energía $/u MXN', digits=(16, 4),
        help='Luz/gas/agua variables: $/kg del período × peso de la unidad.')
    fab_unit = fields.Float(
        string='Fabricación $/u MXN', digits=(16, 4),
        help='Parte del gasto FIJO de fábrica (sueldos de planta, renta, '
             'depreciación, arrendamiento de maquinaria) que absorbe cada '
             'unidad, repartida por peso y por metros.')
    op_pct = fields.Float(
        string='Operación % s/venta',
        help='Gastos de administración y ventas (6xx) como % de las ventas. '
             'Se cobra como % del precio.')
    costo_variable = fields.Float(
        string='Costo variable $/u MXN', digits=(16, 4),
        help='MP + energía: lo que sale de la bolsa por producir UNA unidad '
             'más. Piso absoluto de cualquier precio.')
    costo_absorbido_sin_op = fields.Float(
        string='Costo de producción $/u MXN', digits=(16, 4),
        help='Costo variable + fabricación absorbida (aún sin operación).')

    # Precios (guardados SIEMPRE en MXN; el espejo en divisa usa el TC)
    precio_objetivo = fields.Float(
        string='Precio objetivo $/u MXN',
        help='El precio que se propuso o que pidió el cliente. Se capturó '
             'en la moneda de la cotización y aquí está YA CONVERTIDO a MXN '
             'con el TC guardado.')
    precio_mercado = fields.Float(
        string='Precio de mercado $/u MXN',
        help='Promedio REAL facturado de este producto en los 12 meses '
             'previos a cotizar (todos los clientes). El ancla realista: '
             'los pisos dicen debajo de qué no bajar; el mercado dice qué '
             'se está logrando. 0 = sin ventas en la ventana.')
    piso_ocioso = fields.Float(
        string='Piso con capacidad ociosa $/u MXN',
        help='= costo variable. Con capacidad ociosa, todo precio arriba de '
             'esto APORTA a fijos (aunque el absorbido salga negativo). '
             'Nunca vender debajo.')
    piso_lleno = fields.Float(
        string='Piso a planta llena $/u MXN',
        help='= (variable + fab) ÷ (1 − op%): margen cero absorbiendo todo. '
             'Con la planta llena no aceptar debajo de esto.')
    # Los márgenes y el semáforo se COMPUTAN del precio y del snapshot de
    # costos, no se guardan sueltos. Antes eran floats que el cotizador
    # escribía una vez: al editar después el precio objetivo sobre la
    # cotización, el margen se quedaba con el del precio anterior. Pasó tres
    # veces en producción — la peor, una cotización a $16.00 presumiendo 5.0%
    # de margen cuando a ese precio el real era 1.5% (el 5.0% correspondía a
    # $16.72, el precio de antes de la rebaja).
    #
    # Los COSTOS sí son snapshot a propósito: son la foto de los factores del
    # día en que se cotizó, y recalcularlos al vuelo cambiaría una cotización
    # ya presentada. Lo que no puede quedarse viejo es la aritmética entre
    # el precio vigente y esa foto.
    margen_contribucion = fields.Float(
        compute='_compute_margenes', store=True,
        string='Contribución $/u MXN', digits=(16, 4),
        help='Precio − costo variable: lo que cada unidad aporta para pagar '
             'los costos fijos. Calculado siempre del precio vigente.')
    margen_contribucion_pct = fields.Float(
        compute='_compute_margenes', store=True, string='Contribución %')
    margen_bruto_pct = fields.Float(
        compute='_compute_margenes', store=True,
        string='Margen bruto %',
        help='(precio − costo de producción) ÷ precio, al precio cotizado. '
             'Utilidad después de fabricar, ANTES de admin/ventas.')
    margen_neto_pct = fields.Float(
        compute='_compute_margenes', store=True,
        string='Margen neto %',
        help='Margen bruto − %operación: lo que queda después de TODO. '
             'Calculado siempre del precio vigente — editar el precio '
             'objetivo lo actualiza solo.')

    @api.depends('precio_objetivo', 'precio_mercado', 'piso_lleno',
                 'piso_ocioso', 'costo_variable', 'costo_absorbido_sin_op',
                 'op_pct')
    def _compute_margenes(self):
        for rec in self:
            # Mismo fallback que el precio evaluado: objetivo → mercado →
            # piso lleno. No se usa precio_evaluado directo porque este
            # compute es almacenado y aquél no.
            precio = (rec.precio_objetivo or rec.precio_mercado
                      or rec.piso_lleno)
            if not precio:
                rec.margen_contribucion = 0.0
                rec.margen_contribucion_pct = 0.0
                rec.margen_bruto_pct = 0.0
                rec.margen_neto_pct = 0.0
                rec.semaforo = False
                continue
            contrib = precio - rec.costo_variable
            bruto = 100.0 * (precio - rec.costo_absorbido_sin_op) / precio
            rec.margen_contribucion = contrib
            rec.margen_contribucion_pct = 100.0 * contrib / precio
            rec.margen_bruto_pct = bruto
            # op_pct está guardado en puntos porcentuales (14.85, no 0.1485)
            rec.margen_neto_pct = bruto - rec.op_pct
            rec.semaforo = self.env['qb.costo.producto'].semaforo_for(
                precio, rec.piso_ocioso, rec.piso_lleno)

    # Espejo en divisa (desde el TC guardado al cotizar)
    precio_mercado_divisa = fields.Float(
        compute='_compute_divisa', string='Mercado (divisa)', digits=(16, 4))
    piso_ocioso_divisa = fields.Float(
        compute='_compute_divisa', string='Piso ocioso (divisa)', digits=(16, 4))
    piso_lleno_divisa = fields.Float(
        compute='_compute_divisa', string='Piso lleno (divisa)', digits=(16, 4))
    es_divisa = fields.Boolean(compute='_compute_divisa')

    @api.depends('fx_rate', 'precio_mercado', 'piso_ocioso', 'piso_lleno')
    def _compute_divisa(self):
        for rec in self:
            fx = rec.fx_rate if rec.fx_rate and rec.fx_rate != 1.0 else 0.0
            rec.es_divisa = bool(fx)
            rec.precio_mercado_divisa = rec.precio_mercado / fx if fx else 0.0
            rec.piso_ocioso_divisa = rec.piso_ocioso / fx if fx else 0.0
            rec.piso_lleno_divisa = rec.piso_lleno / fx if fx else 0.0

    # El precio EVALUADO (semáforo, márgenes, PDF cliente): el objetivo si
    # se capturó; si no, el de mercado; sin ventas, el piso a planta llena.
    precio_evaluado = fields.Float(
        compute='_compute_precio_evaluado', digits=(16, 2),
        string='Precio evaluado $/u MXN',
        help='Objetivo → mercado → piso lleno. Sobre este precio están '
             'calculados el semáforo y los márgenes.')
    evaluado_fuente = fields.Char(
        compute='_compute_precio_evaluado', string='Fuente del precio')
    precio_cliente_mxn = fields.Float(
        compute='_compute_precio_evaluado', digits=(16, 2),
        string='Precio al cliente $/u MXN')
    precio_cliente_divisa = fields.Float(
        compute='_compute_precio_evaluado', digits=(16, 4),
        string='Precio al cliente (divisa)')

    @api.depends('precio_objetivo', 'precio_mercado', 'piso_lleno', 'fx_rate')
    def _compute_precio_evaluado(self):
        for rec in self:
            if rec.precio_objetivo:
                rec.precio_evaluado = rec.precio_objetivo
                rec.evaluado_fuente = 'precio objetivo'
            elif rec.precio_mercado:
                rec.precio_evaluado = rec.precio_mercado
                rec.evaluado_fuente = 'precio de mercado (prom. 12m)'
            else:
                rec.precio_evaluado = rec.piso_lleno
                rec.evaluado_fuente = 'piso a planta llena'
            rec.precio_cliente_mxn = rec.precio_evaluado
            fx = rec.fx_rate if rec.fx_rate and rec.fx_rate != 1.0 else 0.0
            rec.precio_cliente_divisa = \
                rec.precio_cliente_mxn / fx if fx else 0.0
    contrib_hora_maquina = fields.Float(
        string='Contribución $/hora-máquina',
        help='Para rankear contra otros productos cuando hay cuello de botella.')

    semaforo = fields.Selection([
        ('rojo', 'Debajo del costo variable'),
        ('ambar', 'Aporta a fijos (no absorbe todo)'),
        ('verde', 'Cubre costo total + operación'),
    ], compute='_compute_margenes', store=True, string='Semáforo de precio',
        help='Precio evaluado contra los pisos: rojo = destruye valor; '
             'ámbar = con capacidad ociosa conviene (aporta a fijos); '
             'verde = cubre el costo absorbido completo. Se recalcula solo '
             'al cambiar el precio.')
    sale_order_id = fields.Many2one(
        'sale.order', string='Orden de venta', readonly=True,
        help='Orden desde la que se generó la cotización (si aplica).')

    # Chequeo de capacidad
    capacity_ok = fields.Boolean(
        string='Sin impedimento de capacidad conocido',
        help='Solo es False cuando un centro que SÍ se pudo medir no da '
             'abasto. Un centro sin datos no lo baja: de esta bandera cuelgan '
             'los tramos que se le ofrecen al cliente, y hacerla False por '
             'falta de configuración borraría la escalera de volumen del PDF. '
             'La respuesta completa está en «¿Cabe en capacidad?».')
    capacity_status = fields.Selection(
        CAPACITY_STATUS, string='¿Cabe en capacidad?',
        help='Distingue «cabe» de «no pude validarlo». Vacío en las '
             'cotizaciones anteriores a que existiera el campo.')
    capacity_detail = fields.Text(
        string='Detalle de capacidad',
        help='Horas-máquina requeridas vs libres por centro de la ruta; '
             'cuántas máquinas/turnos faltan si no cabe.')

    desglose_html = fields.Html(
        string='Desglose explicado', sanitize=False,
        help='Foto del desglose de costos al momento de cotizar: BOM hoja '
             'por hoja con su última compra, peso, factores y fórmulas.')
    tramo_ids = fields.One2many(
        'qb.cotizacion.tramo', 'cotizacion_id',
        string='Escalera de volumen',
        help='Precios estandarizados por tramo de volumen: descuento fijo '
             'por cada duplicación, nunca debajo del piso a planta llena y '
             'con contribución total que nunca baja.')
    comparativa_html = fields.Html(
        string='Comparativa de precios', sanitize=False,
        help='Foto al cotizar: a cuánto se vendía este producto a otros '
             'clientes (últimos 12 meses) y a cuánto sus otras '
             'presentaciones (metros/kilos, nacional/importado), con el '
             'margen de cada una a su precio de venta.')
    glosario_html = fields.Html(
        compute='_compute_glosario', sanitize=False, string='Glosario',
        help='Definición de cada término del cotizador (precio objetivo, '
             'TC, márgenes, pisos, capacidad, ociosidad...).')

    def _compute_glosario(self):
        for rec in self:
            rec.glosario_html = GLOSARIO_HTML

    # Supuestos (trazabilidad)
    factores_id = fields.Many2one('qb.costo.factores', string='Factores usados')
    supuestos = fields.Text(
        string='Supuestos',
        help='Ventana de gastos, denominadores de producción, FX, fuente de '
             'peso — para reproducir el número después.')
    state = fields.Selection([
        ('draft', 'Borrador'),
        ('done', 'Presentada'),
        ('won', 'Ganada'),
        ('lost', 'Perdida'),
        ('superseded', 'Reemplazada'),
    ], default='draft', tracking=True)

    # ------------------------------------------------------------------
    # Historial de revisiones: cotizar el MISMO producto al MISMO cliente
    # otra vez crea la revisión siguiente y reemplaza a la anterior.
    # Sin esto, cada recotización quedaba como borrador suelto y nadie
    # sabía cuál era la vigente ni qué precio se había ofrecido antes.
    # ------------------------------------------------------------------
    revision = fields.Integer(
        string='Revisión', default=1, readonly=True, copy=False,
        help='Número de revisión dentro de la cadena cliente+producto. '
             'Sube solo al cotizar de nuevo el mismo producto al mismo '
             'cliente.')
    revision_anterior_id = fields.Many2one(
        'qb.cotizacion', string='Sustituye a', readonly=True, copy=False,
        help='La cotización que esta revisión reemplaza. La anterior pasa a '
             '«Reemplazada» automáticamente si estaba en borrador o '
             'presentada; una ganada o perdida conserva su estado (es '
             'historia del trato, no una oferta viva).')
    revision_siguiente_ids = fields.One2many(
        'qb.cotizacion', 'revision_anterior_id', string='Sustituida por')
    historial_count = fields.Integer(compute='_compute_historial_count')

    @api.depends('partner_id', 'product_id')
    def _compute_historial_count(self):
        for rec in self:
            if rec.product_id and rec.partner_id:
                rec.historial_count = self.search_count([
                    ('product_id', '=', rec.product_id.id),
                    ('partner_id', '=', rec.partner_id.id)])
            else:
                rec.historial_count = 1

    @api.model_create_multi
    def create(self, vals_list):
        records = super().create(vals_list)
        for rec in records:
            if not (rec.product_id and rec.partner_id):
                continue
            anterior = self.search([
                ('product_id', '=', rec.product_id.id),
                ('partner_id', '=', rec.partner_id.id),
                ('id', 'not in', records.ids),
            ], order='revision desc, create_date desc, id desc', limit=1)
            if anterior:
                rec.write({'revision': anterior.revision + 1,
                           'revision_anterior_id': anterior.id})
                # Solo una oferta VIVA se reemplaza; ganada/perdida son
                # historia del trato y conservan su estado.
                if anterior.state in ('draft', 'done'):
                    anterior.state = 'superseded'
        return records

    def action_ver_historial(self):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'name': 'Historial: %s · %s' % (
                self.partner_id.name or 'sin cliente',
                self.product_id.default_code or self.name),
            'res_model': 'qb.cotizacion',
            'view_mode': 'list,form',
            'domain': [('product_id', '=', self.product_id.id),
                       ('partner_id', '=', self.partner_id.id)],
            'context': {'search_default_order_revision': 1},
        }

    # ------------------------------------------------------------------
    # Campos del PDF para cliente (formato FPA2812, clave F-P-A28-12).
    # Todo editable por cotización; los defaults son las condiciones
    # estándar del formato viejo.
    # ------------------------------------------------------------------
    atencion_a = fields.Char(
        string='En atención a',
        help='Nombre y puesto del contacto del cliente, como debe salir en '
             'el PDF. Vacío = el nombre del cliente.')
    lote_minimo = fields.Char(
        string='Lote mínimo', default='5,000 m',
        help='Cantidad mínima por lote / Minimum lot quantity.')
    presentacion_rollos = fields.Char(
        string='Presentación de rollos', default='500 m ± 100 m',
        help='Presentación estándar de los rollos / Roll presentation.')
    lugar_entrega = fields.Char(
        string='Lugar y condición de entrega',
        default='Toluca, Estado de México (EXWORKS)',
        help='Incoterm y plaza donde aplica el precio.')
    tiempo_entrega = fields.Char(
        string='Tiempo de entrega',
        default='4 semanas a partir de la liberación o forecast / '
                '4 weeks after release or forecast')
    muestra_leyenda = fields.Char(
        string='Leyenda de muestra',
        default='Muestra menor a 50 m sin costo / Sample less than 50 '
                'meters free of charge')
    tc_coa = fields.Boolean(string='CoA al 100%', default=True)
    tc_ppap = fields.Boolean(string='PPAP', default=True)
    tc_inspeccion_total = fields.Boolean(
        string='Inspección total / 100% inspection', default=True)
    tc_cpk = fields.Boolean(string='CPK 3 sigma')
    tc_pscr = fields.Boolean(string='PSCR')
    tc_pruebas_lab = fields.Boolean(
        string='Pruebas especiales de laboratorio')
    tc_apqp = fields.Boolean(string='APQP')
    tc_ctpat = fields.Boolean(string='Evidencia de carga C-TPAT')
    tc_lta = fields.Boolean(string='LTA')

    folio = fields.Char(compute='_compute_folio', string='Folio')

    @api.depends('create_date')
    def _compute_folio(self):
        # Mismo estilo que el folio del formato viejo (año + consecutivo:
        # «20260096»). El consecutivo es el id, que no se recicla.
        for rec in self:
            year = (rec.create_date or fields.Datetime.now()).year
            rec.folio = '%s%04d' % (year, rec.id or 0)
    precio_vs_piso_pct = fields.Float(
        compute='_compute_precio_vs_piso', store=True,
        string='% sobre el piso lleno',
        help='(precio evaluado ÷ piso a planta llena) − 1. El insumo del '
             'análisis win/loss: ¿a qué % sobre el piso ganamos y a cuál '
             'perdemos?')

    @api.depends('precio_objetivo', 'precio_mercado', 'piso_lleno')
    def _compute_precio_vs_piso(self):
        for rec in self:
            base = rec.precio_objetivo or rec.precio_mercado
            rec.precio_vs_piso_pct = (
                100.0 * (base / rec.piso_lleno - 1.0)
                if base and rec.piso_lleno else 0.0)
    validez_hasta = fields.Date(
        string='Válida hasta',
        help='Después de esta fecha los supuestos (TC, último costo de MP) '
             'pueden haber cambiado: re-cotizar antes de comprometer.')

    # ------------------------------------------------------------------
    # Post-mortem: qué pasó DE VERDAD después de cotizar
    # (mejor práctica: cerrar el ciclo cotizado → real; sin esto las
    # cotizaciones nunca aprenden)
    # ------------------------------------------------------------------
    real_precio_prom = fields.Float(
        compute='_compute_real', string='Precio real $/u MXN', digits=(16, 2),
        help='Precio promedio al que este producto realmente se vendió en el '
             'último período costeado DESPUÉS de la cotización.')
    real_qty = fields.Float(
        compute='_compute_real', string='Qty real vendida/mes', digits=(16, 0))
    real_margen_pct = fields.Float(
        compute='_compute_real', string='Contribución real %')
    delta_precio_pct = fields.Float(
        compute='_compute_real', string='Δ precio real vs cotizado %',
        help='Positivo = se vendió más caro que lo cotizado; negativo = el '
             'precio real quedó por debajo de lo que se cotizó.')

    def _compute_real(self):
        Costo = self.env['qb.costo.producto']
        for rec in self:
            rec.real_precio_prom = rec.real_qty = 0.0
            rec.real_margen_pct = rec.delta_precio_pct = 0.0
            if not rec.product_id:
                continue
            real = Costo.search([
                ('product_id', '=', rec.product_id.id),
                ('period', '>=', (rec.create_date or fields.Datetime.now())
                 .date().replace(day=1)),
                ('qty_vendida', '>', 0),
            ], order='period DESC', limit=1)
            if not real:
                continue
            rec.real_precio_prom = real.precio_prom
            rec.real_qty = real.qty_vendida
            rec.real_margen_pct = real.margen_contribucion_pct
            base = rec.precio_objetivo or rec.precio_mercado
            if base:
                rec.delta_precio_pct = \
                    100.0 * (real.precio_prom - base) / base

    @api.onchange('product_id')
    def _onchange_product(self):
        if self.product_id:
            self.uom_name = self.product_id.uom_id.name

    # ------------------------------------------------------------------
    # Re-cotizar / duplicar escenario / enviar por correo
    # ------------------------------------------------------------------
    def action_recotizar(self):
        """Abre la calculadora precargada con esta cotización — para
        refrescarla con factores/TC de hoy o probar otro precio como nuevo
        escenario. Guardar crea una cotización NUEVA (la original queda
        intacta como histórico)."""
        self.ensure_one()
        Costo = self.env['qb.costo.producto']
        pricelist = getattr(self.partner_id, 'property_product_pricelist', None)
        currency = (pricelist.currency_id if pricelist and pricelist.currency_id
                    else self.env.company.currency_id)
        rate = Costo.to_mxn_rate(currency)
        ctx = {
            'default_partner_id': self.partner_id.id,
            'default_product_id': self.product_id.id,
            'default_currency_id': currency.id,
            'default_volumen': self.volumen,
            # El objetivo guardado es MXN → a la moneda de la cotización
            'default_precio_objetivo':
                self.precio_objetivo / rate if self.precio_objetivo else 0.0,
        }
        if not self.product_id:
            ctx.update({
                'default_spec_mode': True,
                'default_spec_descripcion': self.spec_descripcion,
                'default_spec_gramaje': self.spec_gramaje,
                'default_spec_ancho': self.spec_ancho,
                'default_spec_galga': self.spec_galga,
            })
        return {
            'type': 'ir.actions.act_window',
            'name': 'Re-cotizar / nuevo escenario',
            'res_model': 'qb.cotizador.wizard',
            'view_mode': 'form',
            'target': 'new',
            'context': ctx,
        }

    def recotizar_ahora(self):
        """Recotiza sin pasar por la interfaz: corre el cotizador completo con
        los factores de hoy y devuelve las cotizaciones nuevas.

        `action_recotizar` solo abre la calculadora, así que el pipeline
        completo vivía únicamente en la UI: quien necesitaba recotizar desde
        la shell, un cron o un script terminaba escribiendo el `qb.cotizacion`
        a mano. Eso ya salió caro — las revisiones del 28-ago se armaron así y
        quedaron sin validez, sin escalera de volumen, sin contribución por
        hora-máquina y sin especificación, que es justo lo que solo llena
        `_save_cotizacion`. Con esto, recotizar es una llamada y sale
        completo por construcción.

        El precio al cliente se conserva EN SU MONEDA: se reconstruye con el
        TC que quedó guardado al cotizar, no con el de hoy. Recotizar refresca
        los costos; moverle el precio al cliente por detrás es otra decisión,
        y no la toma un recálculo.

        Solo aplica a cotizaciones de un producto existente: de una
        especificación nueva el registro guarda gramaje, ancho y galga, pero
        NO la MP estimada, la familia ni la ruta que se capturaron en la
        calculadora. Recalcularla desde aquí las tomaría en cero y saldría un
        costo bajísimo con toda la pinta de estar bien, así que mejor falla.
        """
        Wizard = self.env['qb.cotizador.wizard']
        Costo = self.env['qb.costo.producto']
        nuevas = self.env['qb.cotizacion']
        for rec in self:
            if not rec.product_id:
                raise UserError(
                    'La cotización %s es de una especificación nueva: el '
                    'registro no guarda la MP estimada ni la ruta que se '
                    'capturaron, y recalcularla aquí las daría por cero. '
                    'Ábrela con «↻ Re-cotizar» y captúralas.'
                    % (rec.folio or rec.name))
            currency = rec.currency_id or self.env.company.currency_id
            rate = rec.fx_rate or Costo.to_mxn_rate(currency)
            vals = {
                'partner_id': rec.partner_id.id,
                'product_id': rec.product_id.id,
                'volumen': rec.volumen,
                'currency_id': currency.id,
                'precio_objetivo': (rec.precio_objetivo / rate
                                    if rec.precio_objetivo and rate else 0.0),
                # Para un producto existente las specs no entran al cálculo
                # (el peso sale del maestro), pero sí salen en el PDF: si no
                # se arrastran, cada revisión las pierde y el cliente recibe
                # la cotización sin gramaje ni ancho.
                'spec_gramaje': rec.spec_gramaje,
                'spec_ancho': rec.spec_ancho,
                'spec_galga': rec.spec_galga,
                # La revisión sigue colgando del pedido que la originó.
                'sale_order_id': rec.sale_order_id.id,
            }
            nuevas |= Wizard.create(vals)._save_cotizacion()
        return nuevas

    def action_recotizar_ahora(self):
        """Botón: recotiza con los factores de hoy y abre la revisión nueva.

        La original queda intacta como histórico (`create` la encadena y la
        marca «Reemplazada»).
        """
        self.ensure_one()
        nueva = self.recotizar_ahora()
        return {
            'type': 'ir.actions.act_window',
            'name': 'Cotización recalculada',
            'res_model': 'qb.cotizacion',
            'res_id': nueva.id,
            'view_mode': 'form',
            'target': 'current',
        }

    def action_enviar_correo(self):
        """Composer de correo al cliente con el PDF COMERCIAL adjunto
        (solo producto, precio y condiciones). La hoja interna de costo
        NUNCA se manda por aquí."""
        self.ensure_one()
        # Sin fallback a la plantilla vieja: aquella adjuntaba la hoja
        # INTERNA con costos y márgenes — jamás debe llegar al cliente.
        template = self.env.ref(
            'qb_capacidad_costeo.mail_template_cotizacion_cliente',
            raise_if_not_found=False)
        return {
            'type': 'ir.actions.act_window',
            'res_model': 'mail.compose.message',
            'view_mode': 'form',
            'target': 'new',
            'context': {
                'default_model': 'qb.cotizacion',
                'default_res_ids': self.ids,
                'default_template_id': template.id if template else False,
                'default_composition_mode': 'comment',
            },
        }

    # ------------------------------------------------------------------
    # Ciclo de vida (botones explícitos; el chatter registra cada paso)
    # ------------------------------------------------------------------
    def action_marcar_presentada(self):
        self.filtered(lambda c: c.state == 'draft').write({'state': 'done'})

    def action_marcar_ganada(self):
        self.write({'state': 'won'})

    def action_marcar_perdida(self):
        self.write({'state': 'lost'})

    def action_reabrir(self):
        self.write({'state': 'draft'})


class QbCotizacionTramo(models.Model):
    _name = 'qb.cotizacion.tramo'
    _description = 'Tramo de la escalera de volumen de una cotización'
    _order = 'volumen'

    cotizacion_id = fields.Many2one(
        'qb.cotizacion', required=True, ondelete='cascade', index=True)
    multiplo = fields.Float(
        string='× volumen cotizado',
        help='0.5 = la mitad del volumen cotizado; 2 = el doble.')
    volumen = fields.Float(string='Volumen/mes', digits=(16, 0))
    es_base = fields.Boolean(
        string='Cotizado',
        help='El tramo del volumen realmente cotizado (múltiplo 1×).')
    precio_mxn = fields.Float(string='Precio $/u MXN', digits=(16, 2))
    precio_divisa = fields.Float(string='Precio (divisa)', digits=(16, 4))
    margen_neto_pct = fields.Float(string='Margen neto %', digits=(16, 1))
    contrib_total_mes = fields.Float(
        string='Contribución $/mes MXN', digits=(16, 0),
        help='(precio − costo variable) × volumen: el cheque total que ese '
             'tramo aporta a los fijos cada mes. La regla de la escalera es '
             'que NUNCA baje al crecer el volumen — si un descuento lo '
             'bajara, el precio del tramo se ajusta hacia arriba.')
    semaforo = fields.Selection([
        ('rojo', 'Debajo del costo variable'),
        ('ambar', 'Aporta a fijos'),
        ('verde', 'Cubre costo total'),
    ], string='Semáforo')
    capacity_ok = fields.Boolean(
        string='¿Cabe?',
        help='Si ese volumen cabe en las horas libres de la planta. Al '
             'cliente solo se le ofrecen tramos que sí caben. False solo '
             'cuando un centro medido no da abasto; ver «Estado» para '
             'distinguirlo de un centro sin datos.')
    capacity_status = fields.Selection(
        CAPACITY_STATUS, string='Estado de capacidad')
