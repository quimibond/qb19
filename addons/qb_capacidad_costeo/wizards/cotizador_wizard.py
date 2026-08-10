# -*- coding: utf-8 -*-
"""Cotizador: calculadora viva de capacidad y costo.

Los resultados (costo por capa, pisos, contribución, capacidad) son campos
computados que se recalculan EN VIVO al cambiar producto, volumen, precio o
margen — sensación de hoja de cálculo, pero con datos que entran solos de
Odoo (último costo de BOM, factores del GL, horas-máquina libres). El botón
solo GUARDA el escenario elegido como qb.cotizacion con sus supuestos.
"""
from dateutil.relativedelta import relativedelta

from odoo import api, fields, models
from odoo.exceptions import UserError

from ..models.glosario import GLOSARIO_HTML

KG_UOM_NAMES = ('kg', 'kgs', 'kilogramo', 'kilogramos')


class QbCotizadorWizard(models.TransientModel):
    _name = 'qb.cotizador.wizard'
    _description = 'Cotizador de capacidad y costo (calculadora viva)'

    # ------------------------------------------------------------------
    # Inputs
    # ------------------------------------------------------------------
    partner_id = fields.Many2one('res.partner', string='Cliente')
    product_id = fields.Many2one(
        'product.product', string='Producto existente',
        domain=[('sale_ok', '=', True)])
    spec_mode = fields.Boolean(
        string='Especificación nueva',
        help='Cotizar un producto que aún no existe en Odoo, con gramaje/'
             'ancho/receta tentativa.')
    spec_descripcion = fields.Char(string='Descripción')
    spec_gramaje = fields.Float(string='Gramaje (g/m²)')
    spec_ancho = fields.Float(string='Ancho (m)', default=1.5)
    spec_galga = fields.Char(string='Galga')
    spec_bucket = fields.Selection([
        ('tela', 'Tela'),
        ('entretela_tejida', 'Entretela tejida'),
        ('entretela_carda', 'Entretela carda'),
        ('importado', 'Importado'),
    ], string='Familia', default='tela')
    spec_mp_unit = fields.Float(
        string='MP estimada $/u',
        help='Costo de MP por unidad de la receta tentativa (a último costo).')
    spec_centro_ids = fields.Many2many(
        'qb.costeo.centro', string='Ruta (centros)',
        help='Centros por los que pasaría. Vacío = según familia.')
    volumen = fields.Float(string='Volumen (unidades/mes)')
    currency_id = fields.Many2one(
        'res.currency', string='Moneda de la cotización',
        default=lambda self: self.env.company.currency_id,
        help='La moneda en la que CAPTURAS el precio objetivo y en la que '
             'se muestran/aplican los precios. Se precarga con la moneda '
             'del pedido. Los COSTOS del modelo siempre son MXN.')
    precio_objetivo = fields.Float(
        string='Precio objetivo',
        help='El precio que TÚ propones o que el cliente pide, EN LA MONEDA '
             'DE LA COTIZACIÓN (campo de arriba). El modelo lo convierte a '
             'MXN con el TC de Odoo y sobre él evalúa semáforo y márgenes. '
             'Vacío = se evalúa el precio sugerido.')
    precio_objetivo_mxn = fields.Float(
        compute='_compute_cotizacion', string='= Precio objetivo en MXN',
        digits=(16, 2),
        help='El precio objetivo convertido a pesos con el TC de hoy — este '
             'es el número que se compara contra los costos y pisos (que '
             'siempre son MXN).')
    target_margin = fields.Float(
        string='Margen meta %',
        help='El % de utilidad NETA (después de operación) que quieres '
             'ganar sobre el precio de venta. 0 = usar el margen meta de '
             'configuración.')
    fx_rate = fields.Float(
        string='TC (MXN por 1 de la moneda)', digits=(16, 4), readonly=True,
        compute='_compute_fx_rate',
        help='Tipo de cambio de Odoo a hoy para la moneda elegida.')
    moneda_info = fields.Char(compute='_compute_fx_rate')
    es_mxn = fields.Boolean(compute='_compute_fx_rate')

    @api.depends('currency_id')
    def _compute_fx_rate(self):
        Costo = self.env['qb.costo.producto']
        for wiz in self:
            rate = Costo.to_mxn_rate(wiz.currency_id)
            wiz.fx_rate = rate
            wiz.es_mxn = rate == 1.0
            if rate == 1.0:
                wiz.moneda_info = 'Todo en MXN (moneda de la compañía).'
            else:
                wiz.moneda_info = (
                    'Capturas el precio en %s · TC Odoo de hoy: 1 %s = '
                    '$%.4f MXN. Los costos del modelo son MXN; los precios '
                    'se muestran en ambas monedas.'
                    % (wiz.currency_id.name, wiz.currency_id.name, rate))
    regularidad_info = fields.Char(
        compute='_compute_regularidad', string='Histórico del cliente')

    # Integración con la orden de venta (cuando se lanza desde una)
    sale_order_id = fields.Many2one('sale.order', string='Orden de venta',
                                    readonly=True)
    sale_line_id = fields.Many2one(
        'sale.order.line', string='Línea a cotizar',
        domain="[('order_id', '=', sale_order_id), ('product_id', '!=', False)]",
        help='Al guardar con "Aplicar a la línea", el precio calculado se '
             'escribe en esta línea del pedido.')

    # ------------------------------------------------------------------
    # Resultados EN VIVO (computados, se refrescan al teclear)
    # ------------------------------------------------------------------
    factores_id = fields.Many2one(
        'qb.costo.factores', compute='_compute_cotizacion',
        string='Factores usados')
    factores_info = fields.Char(
        compute='_compute_cotizacion', string='Base de cálculo')
    product_bucket = fields.Char(
        compute='_compute_cotizacion', string='Familia detectada')
    kg_per_unit = fields.Float(
        compute='_compute_cotizacion', string='Peso (kg/u)', digits=(16, 4))
    mp_unit = fields.Float(
        compute='_compute_cotizacion', string='MP $/u', digits=(16, 4))
    energia_unit = fields.Float(
        compute='_compute_cotizacion', string='Energía $/u', digits=(16, 4))
    fab_unit = fields.Float(
        compute='_compute_cotizacion', string='Fabricación $/u', digits=(16, 4))
    costo_variable = fields.Float(
        compute='_compute_cotizacion', string='Costo variable $/u', digits=(16, 4))
    op_pct_display = fields.Float(
        compute='_compute_cotizacion', string='Operación % s/venta')
    precio_sugerido = fields.Float(
        compute='_compute_cotizacion', string='Precio sugerido $/u', digits=(16, 4))
    piso_ocioso = fields.Float(
        compute='_compute_cotizacion', string='Piso con capacidad ociosa $/u',
        digits=(16, 4),
        help='= costo variable. Con capacidad ociosa, todo precio arriba de '
             'esto APORTA a fijos.')
    piso_lleno = fields.Float(
        compute='_compute_cotizacion', string='Piso a planta llena $/u',
        digits=(16, 4),
        help='= (variable + fab) ÷ (1 − op%): margen cero absorbiendo todo.')
    margen_contribucion = fields.Float(
        compute='_compute_cotizacion', string='Contribución $/u', digits=(16, 4))
    margen_contribucion_pct = fields.Float(
        compute='_compute_cotizacion', string='Contribución %')
    contrib_hora_maquina = fields.Float(
        compute='_compute_cotizacion', string='Contribución $/hora-máquina')
    capacity_ok = fields.Boolean(
        compute='_compute_cotizacion', string='¿Cabe en capacidad?')
    capacity_detail = fields.Text(
        compute='_compute_cotizacion', string='Detalle de capacidad')
    explicacion_html = fields.Html(
        compute='_compute_explicacion', sanitize=False,
        string='¿De dónde viene cada costo?')
    moneda_alerta = fields.Char(
        compute='_compute_cotizacion', string='Alerta de moneda',
        help='Guardián anti-error de captura: precio sospechosamente chico '
             '(¿USD tecleado con moneda MXN?) o sospechosamente grande '
             '(¿MXN tecleado con moneda USD?).')

    @api.depends('product_id', 'target_margin')
    def _compute_explicacion(self):
        """El desglose completo con fuentes: BOM hoja por hoja con su última
        compra, peso con su fuente, factores con la fórmula y los números
        del período. Solo depende del producto (no recalcula al teclear
        precio/volumen — eso lo hace el compute ligero)."""
        Costo = self.env['qb.costo.producto']
        for wiz in self:
            if not wiz.product_id or wiz.spec_mode:
                wiz.explicacion_html = False
                continue
            factores = self.env['qb.costo.factores'].search(
                [], order='period DESC', limit=1)
            if not factores:
                wiz.explicacion_html = False
                continue
            try:
                wiz.explicacion_html = Costo.explain_quote_html(
                    wiz.product_id, factores)
            except Exception as exc:
                wiz.explicacion_html = '<p>Error al explicar: %s</p>' % exc

    comparativa_html = fields.Html(
        compute='_compute_comparativa', sanitize=False,
        string='¿A cuánto lo vendo hoy?',
        help='Precios reales (12 meses) de este producto a otros clientes y '
             'de sus otras presentaciones (metros/kilos, importado), con el '
             'margen de cada una a su precio de venta actual.')
    glosario_html = fields.Html(
        compute='_compute_glosario', sanitize=False, string='Glosario')

    def _compute_glosario(self):
        for wiz in self:
            wiz.glosario_html = GLOSARIO_HTML

    @api.depends('product_id', 'partner_id', 'spec_mode')
    def _compute_comparativa(self):
        Costo = self.env['qb.costo.producto']
        for wiz in self:
            if not wiz.product_id or wiz.spec_mode:
                wiz.comparativa_html = False
                continue
            factores = self.env['qb.costo.factores'].search(
                [], order='period DESC', limit=1)
            if not factores:
                wiz.comparativa_html = False
                continue
            try:
                wiz.comparativa_html = Costo.comparativa_html(
                    wiz.product_id, factores,
                    wiz.partner_id.commercial_partner_id
                    if wiz.partner_id else None)
            except Exception as exc:
                wiz.comparativa_html = '<p>Error al comparar: %s</p>' % exc
    semaforo = fields.Selection([
        ('rojo', 'Debajo del costo variable'),
        ('ambar', 'Aporta a fijos (no absorbe todo)'),
        ('verde', 'Cubre costo total + operación'),
    ], compute='_compute_cotizacion', string='Semáforo de precio',
        help='Evalúa el precio objetivo (o el sugerido) contra los pisos: '
             'rojo = destruye valor; ámbar = con capacidad ociosa conviene, '
             'aporta a fijos; verde = cubre todo el costo absorbido.')
    # Espejo en la moneda elegida (visibles cuando no es MXN)
    precio_sugerido_divisa = fields.Float(
        compute='_compute_cotizacion', string='Precio sugerido (divisa)',
        digits=(16, 4))
    sugerido_colchon_divisa = fields.Float(
        compute='_compute_cotizacion',
        string='Sugerido + colchón FX (divisa)', digits=(16, 4),
        help='Precio sugerido con el colchón cambiario de configuración '
             '(fx_buffer_pct): cotizar exportación al TC de HOY sin colchón '
             'deja el margen expuesto a la depreciación del peso durante la '
             'vigencia de la cotización.')
    margen_bruto_pct = fields.Float(
        compute='_compute_cotizacion', string='Margen bruto %',
        help='(precio − costo de producción [MP + energía + fabricación]) '
             '÷ precio. Evaluado al precio objetivo (o al sugerido).')
    margen_neto_pct = fields.Float(
        compute='_compute_cotizacion', string='Margen neto %',
        help='(precio − costo de producción − operación) ÷ precio: lo que '
             'queda después de TODO. Al precio sugerido, es exactamente el '
             'margen meta.')
    piso_ocioso_divisa = fields.Float(
        compute='_compute_cotizacion', string='Piso ocioso (divisa)',
        digits=(16, 4))
    piso_lleno_divisa = fields.Float(
        compute='_compute_cotizacion', string='Piso lleno (divisa)',
        digits=(16, 4))

    @api.model
    def default_get(self, fields_list):
        """Prefill desde el contexto: lanzado desde una orden de venta toma
        cliente, la primera línea con producto, su precio y su cantidad."""
        res = super().default_get(fields_list)
        Costo = self.env['qb.costo.producto']
        # Lanzado desde un PRODUCTO (menú Acción en la ficha/lista): cotizar
        # sin pasar por un pedido — volumen = run-rate global del producto
        active_model = self.env.context.get('active_model')
        if active_model in ('product.product', 'product.template') \
                and self.env.context.get('active_id'):
            record = self.env[active_model].browse(
                self.env.context['active_id']).exists()
            product = (record if active_model == 'product.product'
                       else record.product_variant_id) if record else None
            if product:
                res.setdefault('product_id', product.id)
                vol, meses, _v = Costo.monthly_sales_volume(product)
                if meses >= 3:
                    res.setdefault('volumen', vol)
        order = None
        order_id = res.get('sale_order_id')
        if (self.env.context.get('active_model') == 'sale.order'
                and self.env.context.get('active_id')):
            order_id = self.env.context['active_id']
        if order_id:
            order = self.env['sale.order'].browse(order_id).exists()
        if order:
            res.setdefault('partner_id', order.partner_id.id)
            res.setdefault('sale_order_id', order.id)
            # La moneda de la cotización ES la del pedido: el precio se
            # captura y aplica en ella, sin conversiones mentales
            res['currency_id'] = order.currency_id.id
            line = (self.env['sale.order.line'].browse(
                res.get('sale_line_id')).exists()
                or order.order_line.filtered('product_id')[:1])
            if line:
                res.setdefault('sale_line_id', line.id)
                res.setdefault('product_id', line.product_id.id)
                res['precio_objetivo'] = line.price_unit  # moneda del pedido
                # Volumen: si el cliente compra REGULAR este producto, el
                # run-rate mensual histórico manda sobre la qty del pedido
                vol, meses, _v = Costo.monthly_sales_volume(
                    line.product_id, order.partner_id)
                res['volumen'] = vol if meses >= 3 else line.product_uom_qty
        # Sin pedido pero con cliente en contexto: la moneda de su lista
        # de precios (Contitech en USD → cotización en USD, sola)
        if not res.get('currency_id') and res.get('partner_id'):
            partner = self.env['res.partner'].browse(res['partner_id']).exists()
            pricelist = getattr(partner, 'property_product_pricelist', None) \
                if partner else None
            if pricelist and pricelist.currency_id:
                res['currency_id'] = pricelist.currency_id.id
        return res

    @api.onchange('partner_id')
    def _onchange_partner(self):
        """La moneda sigue al CLIENTE aunque no vengas de un pedido: si su
        lista de precios está en USD/EUR, la cotización arranca en esa
        moneda — sin acordarse de cambiarla a mano."""
        if self.partner_id and not self.sale_order_id:
            pricelist = getattr(self.partner_id,
                                'property_product_pricelist', None)
            if pricelist and pricelist.currency_id:
                self.currency_id = pricelist.currency_id

    @api.onchange('sale_line_id')
    def _onchange_sale_line(self):
        """Cambiar de línea re-precarga producto, precio (en la moneda del
        pedido) y volumen (run-rate histórico si el cliente es regular)."""
        if self.sale_line_id:
            Costo = self.env['qb.costo.producto']
            self.product_id = self.sale_line_id.product_id
            self.currency_id = self.sale_line_id.order_id.currency_id
            self.precio_objetivo = self.sale_line_id.price_unit
            vol, meses, _v = Costo.monthly_sales_volume(
                self.sale_line_id.product_id,
                self.sale_line_id.order_id.partner_id)
            self.volumen = vol if meses >= 3 \
                else self.sale_line_id.product_uom_qty

    @api.depends('product_id', 'partner_id')
    def _compute_regularidad(self):
        Costo = self.env['qb.costo.producto']
        for wiz in self:
            if not wiz.product_id:
                wiz.regularidad_info = False
                continue
            vol, meses, ventana = Costo.monthly_sales_volume(
                wiz.product_id, wiz.partner_id)
            if not meses:
                wiz.regularidad_info = (
                    'Sin histórico de este producto%s en los últimos 12 meses.'
                    % (' con este cliente' if wiz.partner_id else ''))
            else:
                wiz.regularidad_info = (
                    '%s: %s de %s meses con compra, promedio %s %s/mes%s.'
                    % ('Cliente REGULAR' if meses >= 3 else 'Compra ocasional',
                       meses, ventana, f'{vol:,.0f}',
                       wiz.product_id.uom_id.name or 'u',
                       ' (usado como volumen)' if meses >= 3 else ''))

    # ------------------------------------------------------------------
    # Cálculo (compartido entre el compute vivo y el guardado)
    # ------------------------------------------------------------------
    def _calc(self):
        """Corre el motor para los inputs actuales. Devuelve dict o None si
        aún no hay qué calcular. NO escribe nada (seguro para compute)."""
        self.ensure_one()
        if not self.product_id and not self.spec_mode:
            return None
        Costo = self.env['qb.costo.producto']
        Config = self.env['qb.costeo.factor.config']
        factores = self.env['qb.costo.factores'].search(
            [], order='period DESC', limit=1)
        if not factores:
            return {'error': 'Aún no hay factores calculados: corre '
                             '"Recalcular costeo (mes anterior)" en '
                             'Configuración una primera vez.'}

        target = self.target_margin / 100.0 if self.target_margin else None
        if self.product_id and not self.spec_mode:
            product = self.product_id
            q = Costo.quote_product(product, factores, target)
            uom_name = product.uom_id.name
            name = 'COT %s' % (product.default_code or product.name)
        else:
            # Especificación nueva: no existe el producto; misma matemática
            # que quote_product con los datos capturados (se cotiza por metro)
            bucket = self.spec_bucket
            kg = (self.spec_gramaje / 1000.0) * (self.spec_ancho or 1.5)
            m_per_kg = 1.0 / kg if kg else Config.get_param('m_per_kg_default', 8.0)
            mp = self.spec_mp_unit
            energia = 0.0 if bucket in ('importado', 'subproducto') \
                else factores.energia_por_kg * kg
            fab = Costo._fab_unit(bucket, False, kg, m_per_kg, factores)
            variable = mp + energia
            op = factores.op_pct
            t = target if target is not None \
                else Config.get_param('target_margin', 0.30)
            denom = 1.0 - op - t
            centros = self.spec_centro_ids
            q = {
                'bucket': bucket, 'centros': centros, 'kg': kg,
                'm_per_kg': m_per_kg, 'is_kg': False,
                'mp': mp, 'energia': energia, 'fab': fab, 'variable': variable,
                'op_pct': op, 'target': t,
                'piso_ocioso': variable,
                'piso_lleno': (variable + fab) / (1.0 - op) if op < 1 else 0.0,
                'precio_sugerido': (variable + fab) / denom if denom > 0 else 0.0,
                'hours_per_unit': 0.0,
                'factores': factores,
            }
            uom_name = 'm'
            name = 'COT %s' % (self.spec_descripcion or 'especificación nueva')
        centros = q['centros'] or self._default_centros(q['bucket'])
        if not q['hours_per_unit']:
            q['hours_per_unit'] = Costo._hours_per_unit(
                centros, q['is_kg'], q['kg'], q['m_per_kg'])

        # El precio objetivo viene EN LA MONEDA elegida → a MXN para comparar
        fx = self.env['qb.costo.producto'].to_mxn_rate(self.currency_id)
        precio_ref = (self.precio_objetivo * fx) if self.precio_objetivo \
            else q['precio_sugerido']
        contrib = precio_ref - q['variable']
        contrib_hora = contrib / q['hours_per_unit'] \
            if q['hours_per_unit'] else 0.0
        capacity_ok, capacity_detail = self._check_capacity(
            centros, q['is_kg'], q['kg'], q['m_per_kg'], self.volumen)
        semaforo = Costo.semaforo_for(
            precio_ref, q['piso_ocioso'], q['piso_lleno'])

        return {
            'semaforo': semaforo, 'fx': fx,
            'name': name, 'bucket': q['bucket'], 'centros': centros,
            'factores': factores, 'kg': q['kg'], 'm_per_kg': q['m_per_kg'],
            'uom_name': uom_name, 'mp': q['mp'], 'energia': q['energia'],
            'fab': q['fab'], 'variable': q['variable'],
            'op_pct': q['op_pct'], 'target': q['target'],
            'precio_sugerido': q['precio_sugerido'],
            'piso_ocioso': q['piso_ocioso'], 'piso_lleno': q['piso_lleno'],
            'precio_ref': precio_ref,
            'contrib': contrib, 'contrib_hora': contrib_hora,
            'capacity_ok': capacity_ok, 'capacity_detail': capacity_detail,
        }

    @api.depends('product_id', 'spec_mode', 'spec_gramaje', 'spec_ancho',
                 'spec_bucket', 'spec_mp_unit', 'spec_centro_ids',
                 'volumen', 'precio_objetivo', 'target_margin')
    def _compute_cotizacion(self):
        for wiz in self:
            zero = dict.fromkeys([
                'kg_per_unit', 'mp_unit', 'energia_unit', 'fab_unit',
                'costo_variable', 'op_pct_display', 'precio_sugerido',
                'piso_ocioso', 'piso_lleno', 'margen_contribucion',
                'margen_contribucion_pct', 'contrib_hora_maquina',
                'precio_sugerido_divisa', 'piso_ocioso_divisa',
                'piso_lleno_divisa', 'sugerido_colchon_divisa',
                'margen_bruto_pct', 'margen_neto_pct',
                'precio_objetivo_mxn'], 0.0)
            zero['semaforo'] = False
            zero['moneda_alerta'] = False
            try:
                res = wiz._calc()
            except Exception as exc:  # un dato roto no debe romper el form
                wiz.update(dict(zero, factores_id=False, product_bucket=False,
                                factores_info=False, capacity_ok=False,
                                capacity_detail='Error al calcular: %s' % exc))
                continue
            if not res:
                wiz.update(dict(zero, factores_id=False, product_bucket=False,
                                factores_info=False, capacity_ok=False,
                                capacity_detail=False))
                continue
            if res.get('error'):
                wiz.update(dict(zero, factores_id=False, product_bucket=False,
                                factores_info=res['error'], capacity_ok=False,
                                capacity_detail=res['error']))
                continue
            factores = res['factores']
            precio_ref = res['precio_ref']
            if precio_ref:
                # bruto = tras costo de producción; neto = bruto − op%
                bruto = 100.0 * (precio_ref - res['variable']
                                 - res['fab']) / precio_ref
                neto = bruto - 100.0 * res['op_pct']
            else:
                bruto = neto = 0.0
            # Guardián anti-error de moneda: el precio capturado quedó
            # ABSURDO contra los pisos → casi siempre es la moneda equivocada
            moneda_alerta = False
            piso = res['piso_ocioso']
            if wiz.precio_objetivo and piso:
                if wiz.es_mxn and precio_ref < piso * 0.25:
                    moneda_alerta = (
                        'El precio capturado ($%.2f MXN) es menos de ¼ del '
                        'piso mínimo ($%.2f MXN). ¿Lo tecleaste en dólares? '
                        'Cambia "Moneda de la cotización" a USD.'
                        % (precio_ref, piso))
                elif not wiz.es_mxn and res['piso_lleno'] \
                        and precio_ref > res['piso_lleno'] * 5:
                    moneda_alerta = (
                        'El precio capturado (%.2f %s = $%.2f MXN) es más de '
                        '5× el piso lleno. ¿Lo tecleaste en pesos? Cambia '
                        '"Moneda de la cotización" a MXN.'
                        % (wiz.precio_objetivo, wiz.currency_id.name,
                           precio_ref))
            wiz.update({
                'moneda_alerta': moneda_alerta,
                'precio_objetivo_mxn':
                    wiz.precio_objetivo * res['fx']
                    if wiz.precio_objetivo else 0.0,
                'factores_id': factores.id,
                'factores_info': 'Factores %s (ventana %sm) · fab $%.2f/kg + '
                                 '$%.2f/m · energía $%.2f/kg · op %.1f%%' % (
                                     factores.period, factores.window_months,
                                     factores.factor_fab_kg,
                                     factores.factor_fab_m,
                                     factores.energia_por_kg,
                                     res['op_pct'] * 100.0),
                'product_bucket': res['bucket'],
                'kg_per_unit': res['kg'],
                'mp_unit': res['mp'],
                'energia_unit': res['energia'],
                'fab_unit': res['fab'],
                'costo_variable': res['variable'],
                'op_pct_display': res['op_pct'] * 100.0,
                'precio_sugerido': res['precio_sugerido'],
                'piso_ocioso': res['piso_ocioso'],
                'piso_lleno': res['piso_lleno'],
                'margen_contribucion': res['contrib'],
                'margen_contribucion_pct':
                    100.0 * res['contrib'] / precio_ref if precio_ref else 0.0,
                'contrib_hora_maquina': res['contrib_hora'],
                'capacity_ok': res['capacity_ok'],
                'capacity_detail': res['capacity_detail'],
                'semaforo': res['semaforo'],
                'precio_sugerido_divisa': res['precio_sugerido'] / res['fx'],
                'piso_ocioso_divisa': res['piso_ocioso'] / res['fx'],
                'piso_lleno_divisa': res['piso_lleno'] / res['fx'],
                'sugerido_colchon_divisa':
                    res['precio_sugerido'] / res['fx']
                    * (1.0 + self.env['qb.costeo.factor.config'].get_param(
                        'fx_buffer_pct', 0.03)),
                'margen_bruto_pct': bruto,
                'margen_neto_pct': neto,
            })

    # ------------------------------------------------------------------
    # Guardar el escenario
    # ------------------------------------------------------------------
    def _save_cotizacion(self):
        """Congela el escenario actual como qb.cotizacion (con supuestos)."""
        self.ensure_one()
        if not self.volumen:
            raise UserError('Captura el volumen mensual para guardar la cotización.')
        res = self._calc()
        if not res:
            raise UserError('Elige un producto existente o captura una '
                            'especificación nueva.')
        if res.get('error'):
            raise UserError(res['error'])

        factores = res['factores']
        supuestos = (
            'Factores del período %s (ventana %s meses).\n'
            'Pool fabricación $%s/mes; denominadores: %s kg/mes, %s m/mes.\n'
            'Factor peso $%.2f/kg; factor largo $%.2f/m; energía $%.2f/kg; '
            'operación %.1f%% sobre venta.\n'
            'Peso usado: %.4f kg/u (%s). FX supuesto: %s.'
        ) % (factores.period, factores.window_months,
             f'{factores.fab_pool_month:,.0f}',
             f'{factores.kg_denom_month:,.0f}',
             f'{factores.m_denom_month:,.0f}',
             factores.factor_fab_kg, factores.factor_fab_m,
             factores.energia_por_kg, res['op_pct'] * 100.0,
             res['kg'], res['bucket'], self.fx_rate or 'FX de cada compra')

        cotizacion = self.env['qb.cotizacion'].create({
            'name': res['name'],
            'partner_id': self.partner_id.id,
            'product_id': self.product_id.id,
            'spec_descripcion': self.spec_descripcion,
            'spec_gramaje': self.spec_gramaje,
            'spec_ancho': self.spec_ancho,
            'spec_galga': self.spec_galga,
            'volumen': self.volumen,
            'uom_name': res['uom_name'],
            'fx_rate': self.fx_rate,
            'mp_unit': res['mp'],
            'energia_unit': res['energia'],
            'fab_unit': res['fab'],
            'op_pct': res['op_pct'] * 100.0,
            'costo_variable': res['variable'],
            'costo_absorbido_sin_op': res['variable'] + res['fab'],
            'target_margin': res['target'] * 100.0,
            # Todo lo guardado es MXN (consistencia histórica); el TC y la
            # moneda capturada quedan en fx_rate/supuestos
            'precio_objetivo': self.precio_objetivo * res['fx']
                               if self.precio_objetivo else 0.0,
            'precio_sugerido': res['precio_sugerido'],
            'piso_ocioso': res['piso_ocioso'],
            'piso_lleno': res['piso_lleno'],
            'margen_contribucion': res['contrib'],
            'margen_contribucion_pct':
                100.0 * res['contrib'] / res['precio_ref']
                if res['precio_ref'] else 0.0,
            'margen_bruto_pct': self.margen_bruto_pct,
            'margen_neto_pct': self.margen_neto_pct,
            'contrib_hora_maquina': res['contrib_hora'],
            'capacity_ok': res['capacity_ok'],
            'capacity_detail': res['capacity_detail'],
            'semaforo': res['semaforo'],
            'sale_order_id': self.sale_order_id.id,
            'factores_id': factores.id,
            'supuestos': supuestos,
            'desglose_html': self.explicacion_html or False,
            'comparativa_html': self.comparativa_html or False,
            'validez_hasta': fields.Date.today() + relativedelta(
                days=int(self.env['qb.costeo.factor.config'].get_param(
                    'quote_validity_days', 15))),
        })
        return cotizacion

    def action_cotizar(self):
        cotizacion = self._save_cotizacion()
        return {
            'type': 'ir.actions.act_window',
            'res_model': 'qb.cotizacion',
            'res_id': cotizacion.id,
            'view_mode': 'form',
            'target': 'current',
        }

    def action_aplicar_precio(self):
        """Guarda la cotización Y escribe el precio calculado en la línea
        del pedido (precio objetivo si lo capturaste; si no, el sugerido).
        Regresa a la orden de venta."""
        self.ensure_one()
        if not self.sale_line_id:
            raise UserError('Elige la línea del pedido a la que aplicar el precio.')
        cotizacion = self._save_cotizacion()
        Costo = self.env['qb.costo.producto']
        # precio_objetivo está en la moneda de la cotización → a MXN → a la
        # moneda del pedido (normalmente son la misma y esto es identidad)
        fx_wiz = Costo.to_mxn_rate(self.currency_id)
        precio_mxn = (self.precio_objetivo * fx_wiz) if self.precio_objetivo \
            else cotizacion.precio_sugerido
        rate = Costo.to_mxn_rate(self.sale_order_id.currency_id)
        precio_divisa = precio_mxn / rate if rate else precio_mxn
        self.sale_line_id.price_unit = precio_divisa
        divisa = self.sale_order_id.currency_id.name or 'MXN'
        fx = (' (TC %.4f)' % rate) if rate != 1.0 else ''
        self.sale_order_id.message_post(
            body='Precio de %s actualizado a %s %.2f%s ($%.2f MXN) por el '
                 'cotizador de costos (%s, semáforo: %s).'
                 % (self.sale_line_id.product_id.display_name, divisa,
                    precio_divisa, fx, precio_mxn, cotizacion.name,
                    dict(cotizacion._fields['semaforo'].selection).get(
                        cotizacion.semaforo, 'n/d')))
        return {
            'type': 'ir.actions.act_window',
            'res_model': 'sale.order',
            'res_id': self.sale_order_id.id,
            'view_mode': 'form',
            'target': 'current',
        }

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _default_centros(self, bucket):
        Centro = self.env['qb.costeo.centro']
        if bucket in ('importado', 'subproducto', 'servicio'):
            return Centro.browse()
        if bucket == 'entretela_carda':
            return Centro.search([('code', 'ilike', 'ENTRETELA')])
        return Centro.search([('nature', '=', 'fabril_directo')])

    def _check_capacity(self, centros, is_kg, kg, m_per_kg, volumen):
        """Horas requeridas vs libres por centro; máquinas/turnos faltantes."""
        if not centros:
            return True, ('Sin ruta de fabricación (importado/servicio): '
                          'no consume capacidad.')
        if not volumen:
            return True, 'Captura el volumen mensual para validar capacidad.'
        lines = []
        ok = True
        capacidad = self.env['qb.capacidad'].search(
            [('centro_id', 'in', centros.ids)])
        free_by_centro = {}
        hours_wc_by_centro = {}
        for cap in capacidad:
            free_by_centro[cap.centro_id.id] = \
                free_by_centro.get(cap.centro_id.id, 0.0) + cap.free_hours_month
            hours_wc_by_centro[cap.centro_id.id] = \
                hours_wc_by_centro.get(cap.centro_id.id, 0.0) + cap.hours_month_available
        for centro in centros:
            std = centro.std_output_per_hour
            if not std:
                lines.append('%s: sin throughput nominal configurado — no se '
                             'puede validar capacidad.' % centro.code)
                continue
            if centro.driver_principal == 'peso':
                units = volumen * (1.0 if is_kg else kg)
            else:
                units = volumen * (m_per_kg if is_kg else 1.0)
            hours_needed = units / std
            free = free_by_centro.get(centro.id)
            if free is None:
                # Centro sin workcenters: capacidad desde turnos config
                turnos = self.env['qb.turno.config'].search(
                    [('centro_id', '=', centro.id)])
                total_hours = sum(t.hours_per_month() for t in turnos)
                balance = self.env['qb.balance'].search(
                    [('centro_id', '=', centro.id)], limit=1)
                used_pct = balance.utilization_pct if balance else 0.0
                free = total_hours * (1.0 - used_pct / 100.0)
            if hours_needed <= free:
                lines.append('%s: requiere %.0f h/mes, libres %.0f h/mes — OK.'
                             % (centro.code, hours_needed, free))
            else:
                ok = False
                deficit = hours_needed - free
                n_wc = len(centro.workcenter_ids) or 1
                hours_per_machine = (
                    hours_wc_by_centro.get(centro.id, 0.0) / n_wc
                ) or 200.0
                machines_needed = deficit / hours_per_machine
                lines.append(
                    '%s: requiere %.0f h/mes, libres %.0f h/mes — FALTAN '
                    '%.0f h (≈ %.1f máquinas o turnos equivalentes).'
                    % (centro.code, hours_needed, free, deficit, machines_needed))
        return ok, '\n'.join(lines)
