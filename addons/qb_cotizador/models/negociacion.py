# -*- coding: utf-8 -*-
"""Negociaciones de precio con clientes existentes (1.8.0).

Renovaciones anuales y peticiones de rebaja (el programa Blockbuster de ContiTech): una negociación por cliente,
moneda y vigencia, con una línea por producto de uno de tres tipos:

- ``precio_nuevo``: precio fijo nuevo.
- ``rebaja_pct``: % sobre el precio actual de su tarifa.
- ``escalera_incremental``: precio base + tramos por volumen mensual; el descuento de cada tramo aplica solo a
  las unidades que caen dentro del tramo (como el ISR por tramos), medido por mes, trimestre (umbrales × 3) o
  año (× 12).

Sigue el mismo ciclo y la misma aprobación por puesto que la cotización (``qb.cotizador.aprobacion``). Al ganar
con la aprobación del cliente, cada línea cierra el precio vigente de la tarifa propia del cliente un día antes
de la vigencia y abre el nuevo (no se borra nada: queda el historial); la escalera además crea un
``qb.cotizador.acuerdo.volumen`` que se liquida por periodo con una nota de crédito en borrador.

Esta escalera NO es ``qb.cotizador.tramo`` (escalera de precio único ½×, 1×, 2×, 4× del PDF de la cotización).
"""
from datetime import date, datetime, time, timedelta

from dateutil.relativedelta import relativedelta

from odoo import api, fields, models
from odoo.exceptions import UserError, ValidationError

from .aprobacion import APROBADAS, SEMAFOROS

TIPOS = [
    ('precio_nuevo', 'Precio fijo nuevo'),
    ('rebaja_pct', 'Rebaja % sobre el precio actual'),
    ('escalera_incremental', 'Escalera incremental por volumen'),
]
PERIODOS = [('mensual', 'Mensual'), ('trimestral', 'Trimestral'), ('anual', 'Anual')]
MESES_PERIODO = {'mensual': 1, 'trimestral': 3, 'anual': 12}
ESCENARIOS = [
    ('actual', 'Volumen actual (12 meses)'),
    ('pronostico', 'Pronóstico'),
    ('menos', 'Pronóstico − variación'),
    ('mas', 'Pronóstico + variación'),
    ('libre', 'Volumen libre'),
]
ORDEN_SEMAFORO = {'rojo': 0, 'ambar': 1, 'verde': 2}
INFINITO = float('inf')


def descuento_tramos(unidades, tramos, precio, meses=1):
    """Descuento de una escalera incremental.

    ``tramos``: iterable de (desde, hasta, pct) en unidades **por mes** (hasta 0 o None = sin límite). Los
    umbrales se multiplican por ``meses`` (trimestre = × 3) y el % de cada tramo aplica solo a las unidades que
    caen dentro de él, sobre ``precio``. Devuelve (descuento, [(desde, hasta, pct, unidades del tramo,
    descuento del tramo)]) con los umbrales ya multiplicados.

    ContiTech, trimestre con 263,750 m a US$1.880 y tramos 0–50k 0 %, 50–75k 10 %, 75–95k 12 %, >95k 15 %:
    (225,000 − 150,000) × 1.88 × 10 % + (263,750 − 225,000) × 1.88 × 12 % = 14,100 + 8,742 = US$22,842.
    """
    total, detalle = 0.0, []
    for desde, hasta, pct in sorted(tramos, key=lambda t: t[0] or 0.0):
        lo = (desde or 0.0) * meses
        hi = (hasta * meses) if hasta else INFINITO
        en_tramo = max(0.0, min(unidades, hi) - lo)
        desc = en_tramo * precio * (pct or 0.0) / 100.0
        total += desc
        detalle.append((lo, hi if hi != INFINITO else 0.0, pct or 0.0, en_tramo, desc))
    return total, detalle


def _peor(semaforos):
    vals = [s for s in semaforos if s]
    return min(vals, key=lambda s: ORDEN_SEMAFORO[s]) if vals else False


def _semaforo(precio, costo_variable, costo_completo):
    if not precio or not costo_completo:
        return False
    if costo_variable and precio < costo_variable:
        return 'rojo'
    if precio < costo_completo:
        return 'ambar'
    return 'verde'


class QbCotizadorNegociacion(models.Model):
    _name = 'qb.cotizador.negociacion'
    _description = 'Negociación de precio con un cliente'
    _inherit = ['mail.thread', 'mail.activity.mixin', 'qb.cotizador.aprobacion']
    _order = 'id desc'
    _ciclo_nombre = 'negociación'

    name = fields.Char(compute='_compute_name', store=True)
    folio = fields.Char(readonly=True, copy=False, index=True)
    active = fields.Boolean(default=True)
    company_id = fields.Many2one('res.company', required=True, default=lambda self: self.env.company)
    user_id = fields.Many2one('res.users', string='Vendedor', default=lambda self: self.env.user, tracking=True)
    partner_id = fields.Many2one('res.partner', string='Cliente', required=True, tracking=True,
                                 help='La empresa (entidad comercial): un contacto se cambia por su empresa.')
    atencion_a = fields.Char(string='En atención a',
                             help='Nombre y puesto del contacto como sale en la carta. Vacío: el nombre del cliente.')
    contacto_id = fields.Many2one('res.partner', string='Contacto de la carta',
                                  domain="[('commercial_partner_id', '=', partner_id)]",
                                  help='Su idioma decide si la carta al cliente sale en español o en inglés. Vacío: '
                                       'el idioma del cliente.')
    currency_id = fields.Many2one('res.currency', string='Moneda', required=True,
                                  default=lambda self: self.env.company.currency_id)
    fx_rate = fields.Float(string='TC (MXN por 1 divisa)', digits=(16, 4), default=1.0,
                           help='Pesos por 1 unidad de la divisa, para pasar el costo (MXN) a la moneda de la '
                                'negociación. Se toma del tipo de cambio de Odoo al crear y con «Actualizar datos»; '
                                'se puede capturar a mano en borrador.')
    es_divisa = fields.Boolean(compute='_compute_es_divisa')
    fecha_inicio = fields.Date(string='Vigencia desde', required=True, tracking=True,
                               default=lambda self: date(fields.Date.context_today(self).year + 1, 1, 1))
    fecha_fin = fields.Date(string='Vigencia hasta', required=True, tracking=True,
                            default=lambda self: date(fields.Date.context_today(self).year + 1, 12, 31))
    notas_cliente = fields.Text(string='Contrapropuesta del cliente',
                                help='Lo que pidió el cliente, con sus palabras (correo, programa, fecha).')
    contra_rebaja_pct = fields.Float(string='Rebaja pareja que pide el cliente (%)', digits=(6, 2),
                                     help='La rebaja sobre el precio actual que pide el cliente para todos sus '
                                          'productos. Con sus tramos (en cada línea), el simulador la compara lado a '
                                          'lado con la propuesta.')
    sim_variacion_pct = fields.Float(string='Variación del simulador (%)', default=15.0, digits=(6, 2),
                                     help='El simulador corre el pronóstico más y menos este porcentaje.')
    compra_12m = fields.Float(string='Compra total del cliente 12 meses', digits=(16, 2), readonly=True,
                              help='Lo facturado al cliente (todas las monedas, neto de notas de crédito) en los '
                                   '12 meses anteriores, en la moneda de la negociación.')
    datos_al = fields.Datetime(string='Datos del cliente al', readonly=True, copy=False)
    linea_ids = fields.One2many('qb.cotizador.negociacion.linea', 'negociacion_id', string='Productos', copy=True)
    simulacion_ids = fields.One2many('qb.cotizador.negociacion.simulacion', 'negociacion_id',
                                     string='Simulador por producto', readonly=True,
                                     domain=[('linea_id', '!=', False)])
    simulacion_global_ids = fields.One2many('qb.cotizador.negociacion.simulacion', 'negociacion_id',
                                            string='Simulador global', readonly=True,
                                            domain=[('linea_id', '=', False)])
    acuerdo_ids = fields.One2many('qb.cotizador.acuerdo.volumen', 'negociacion_id', string='Acuerdos de volumen',
                                  readonly=True)
    acuerdo_count = fields.Integer(compute='_compute_acuerdo_count')
    semaforo = fields.Selection(SEMAFOROS, string='Semáforo', compute='_compute_semaforo', store=True,
                                help='El peor de sus productos: rojo si algún precio o tramo queda debajo del costo '
                                     'variable; ámbar si queda debajo del costo completo (piso lleno).')
    descuento_pronostico = fields.Float(string='Descuento al año (pronóstico)', digits=(16, 2), readonly=True,
                                        copy=False)
    descuento_pct_pronostico = fields.Float(string='% efectivo (pronóstico)', digits=(6, 2), readonly=True,
                                            copy=False)
    descuento_pct_compra = fields.Float(string='% de su compra total', digits=(6, 2), readonly=True, copy=False)
    simulado_el = fields.Datetime(string='Simulada el', readonly=True, copy=False)
    aplicada_el = fields.Datetime(string='Aplicada a la tarifa el', readonly=True, copy=False)

    # ==================================================================
    # Cómputos
    # ==================================================================
    @api.depends('folio', 'partner_id.name', 'fecha_inicio', 'fecha_fin')
    def _compute_name(self):
        for rec in self:
            vig = '%s a %s' % (rec.fecha_inicio, rec.fecha_fin) if rec.fecha_inicio and rec.fecha_fin else ''
            rec.name = ' · '.join(p for p in (rec.folio, rec.partner_id.name, vig) if p)

    @api.depends('currency_id', 'company_id')
    def _compute_es_divisa(self):
        for rec in self:
            rec.es_divisa = rec.currency_id != rec.company_id.currency_id

    @api.depends('linea_ids.semaforo')
    def _compute_semaforo(self):
        for rec in self:
            rec.semaforo = _peor(rec.linea_ids.mapped('semaforo'))

    def _compute_acuerdo_count(self):
        for rec in self:
            rec.acuerdo_count = len(rec.acuerdo_ids)

    @api.constrains('fecha_inicio', 'fecha_fin')
    def _check_fechas(self):
        for rec in self:
            if rec.fecha_inicio and rec.fecha_fin and rec.fecha_fin < rec.fecha_inicio:
                raise ValidationError('La vigencia termina antes de empezar.')

    # ==================================================================
    # Alta y datos del cliente
    # ==================================================================
    @api.model_create_multi
    def create(self, vals_list):
        for vals in vals_list:
            if not vals.get('folio'):
                vals['folio'] = self.env['ir.sequence'].next_by_code('qb.cotizador.negociacion')
            if vals.get('partner_id'):
                vals['partner_id'] = self.env['res.partner'].browse(vals['partner_id']).commercial_partner_id.id
            # El TC va antes de crear: las líneas que vienen en el mismo create convierten su costo con él.
            if vals.get('currency_id') and (vals.get('fx_rate') or 0.0) <= 1.0:
                company = self.env['res.company'].browse(vals.get('company_id')) or self.env.company
                moneda = self.env['res.currency'].browse(vals['currency_id'])
                if moneda != company.currency_id:
                    vals['fx_rate'] = moneda._convert(1.0, company.currency_id, company,
                                                      fields.Date.context_today(self), round=False)
        recs = super().create(vals_list)
        for rec in recs:
            if rec.es_divisa and (rec.fx_rate or 0.0) <= 1.0:
                rec.fx_rate = rec._fx_odoo()
            if not rec.compra_12m:
                rec.compra_12m = rec._compra_12m()
                rec.datos_al = fields.Datetime.now()
        return recs

    def write(self, vals):
        if vals.get('partner_id'):
            vals['partner_id'] = self.env['res.partner'].browse(vals['partner_id']).commercial_partner_id.id
        return super().write(vals)

    @api.onchange('partner_id')
    def _onchange_partner_id(self):
        if self.partner_id:
            comercial = self.partner_id.commercial_partner_id
            if comercial != self.partner_id:
                self.partner_id = comercial
            tarifa = comercial.with_company(self.company_id or self.env.company).property_product_pricelist
            if tarifa.currency_id:
                self.currency_id = tarifa.currency_id

    @api.onchange('currency_id')
    def _onchange_currency_id(self):
        self.fx_rate = self._fx_odoo()

    def _fx_odoo(self):
        """Pesos por 1 unidad de la moneda de la negociación al día de hoy (1.0 en la moneda de la compañía)."""
        self.ensure_one()
        company = self.company_id or self.env.company
        if not self.currency_id or self.currency_id == company.currency_id:
            return 1.0
        return self.currency_id._convert(1.0, company.currency_id, company, fields.Date.context_today(self),
                                         round=False)

    def _a_moneda(self, monto_mxn):
        """Un monto en pesos (moneda de la compañía) en la moneda de la negociación, con su TC."""
        self.ensure_one()
        return monto_mxn / (self.fx_rate or 1.0) if self.es_divisa else monto_mxn

    def _ventana_12m(self):
        hoy = fields.Date.context_today(self)
        return hoy - relativedelta(months=12), hoy - timedelta(days=1)

    def _compra_12m(self):
        """Lo facturado al cliente en los 12 meses anteriores (neto de notas de crédito), en la moneda de la
        negociación. Las notas de crédito de los acuerdos de volumen no cuentan."""
        self.ensure_one()
        if not self.partner_id:
            return 0.0
        desde, hasta = self._ventana_12m()
        self.env.flush_all()
        self.env.cr.execute("""
            SELECT COALESCE(SUM(-aml.balance), 0)
              FROM account_move_line aml
              JOIN account_move am ON am.id = aml.move_id
              JOIN account_account aa ON aa.id = aml.account_id
             WHERE am.move_type IN ('out_invoice', 'out_refund')
               AND am.state = 'posted'
               AND aml.display_type = 'product'
               AND aa.account_type = 'income'
               AND aml.company_id = %s
               AND am.commercial_partner_id = %s
               AND am.qb_liquidacion_id IS NULL
               AND am.invoice_date BETWEEN %s AND %s
        """, (self.company_id.id, self.partner_id.commercial_partner_id.id, desde, hasta))
        return self._a_moneda(self.env.cr.fetchone()[0] or 0.0)

    def action_actualizar_datos(self):
        """Vuelve a leer TC, compra del cliente y, por línea, precio vigente, volumen y costo."""
        for rec in self:
            if rec.state != 'borrador':
                raise UserError('Los datos del cliente solo se actualizan en borrador.')
            if rec.es_divisa:
                rec.fx_rate = rec._fx_odoo()
            rec.write({'compra_12m': rec._compra_12m(), 'datos_al': fields.Datetime.now()})
            for linea in rec.linea_ids:
                linea.write(linea._valores_prellenado(conservar_pronostico=True))
            rec.action_simular()
        return True

    # ==================================================================
    # Simulador
    # ==================================================================
    def action_simular(self):
        """Corre cada línea y el total a los volúmenes del simulador y deja las filas en la ficha."""
        Sim = self.env['qb.cotizador.negociacion.simulacion']
        for rec in self:
            Sim.search([('negociacion_id', '=', rec.id)]).unlink()
            filas = []
            for linea in rec.linea_ids:
                filas += linea._filas_simulacion()
            globales = []
            for esc, _nombre in ESCENARIOS:
                if esc == 'libre':
                    continue
                del_esc = [f for f in filas if f['escenario'] == esc]
                if not del_esc:
                    continue
                suma = {k: sum(f[k] for f in del_esc) for k in (
                    'volumen_anual', 'venta_actual', 'descuento', 'venta_neta', 'contrib_variable',
                    'contrib_completa', 'cliente_descuento', 'cliente_venta_neta', 'cliente_contrib_completa')}
                suma.update({
                    'negociacion_id': rec.id, 'linea_id': False, 'escenario': esc, 'sequence': 0,
                    'con_contrapropuesta': any(f['con_contrapropuesta'] for f in del_esc),
                    'pct_efectivo': 100.0 * suma['descuento'] / suma['venta_actual'] if suma['venta_actual'] else 0.0,
                    'pct_compra_total': 100.0 * suma['descuento'] / rec.compra_12m if rec.compra_12m else 0.0,
                    'semaforo': _peor(f['semaforo'] for f in del_esc),
                    'diferencia_cliente': suma['cliente_descuento'] - suma['descuento'],
                })
                globales.append(suma)
            Sim.create(globales + filas)
            prono = [g for g in globales if g['escenario'] == 'pronostico']
            prono = prono[0] if prono else {}
            rec.write({'simulado_el': fields.Datetime.now(),
                       'descuento_pronostico': prono.get('descuento', 0.0),
                       'descuento_pct_pronostico': prono.get('pct_efectivo', 0.0),
                       'descuento_pct_compra': prono.get('pct_compra_total', 0.0)})
        return True

    # ==================================================================
    # Ganchos del ciclo
    # ==================================================================
    def _validar_para_aprobacion(self):
        self.ensure_one()
        faltan = []
        if not self.linea_ids:
            faltan.append('al menos un producto')
        if self.es_divisa and (self.fx_rate or 0.0) <= 1.0:
            faltan.append('el tipo de cambio (la negociación es en %s y el TC es %.4f)' % (
                self.currency_id.name, self.fx_rate or 0.0))
        for linea in self.linea_ids:
            ref = linea.product_id.default_code or linea.product_id.display_name or 'una línea'
            if not linea.product_id:
                faltan.append('el producto de una línea')
            if linea.precio_resultante <= 0:
                faltan.append('el precio de %s' % ref)
            if linea.tipo == 'escalera_incremental' and not linea.tramo_ids:
                faltan.append('los tramos de %s' % ref)
            if not linea.costo_completo:
                faltan.append('el costo de %s (sin costo en el período, «Actualizar datos»)' % ref)
            if linea.precio_alerta:
                faltan.append(linea.precio_alerta)
        if faltan:
            raise UserError('Antes de pedir aprobación falta: %s.' % ', '.join(faltan))
        self.action_simular()

    def _aviso_aprobacion(self):
        if self.semaforo == 'ambar':
            return ' Aviso: algún precio o tramo queda debajo del costo completo (piso lleno).'
        return ''

    def _nota_aprobacion(self):
        return ('%d producto(s), vigencia %s a %s. Descuento al año con el pronóstico: %s %.2f (%.2f %% efectivo, '
                '%.2f %% de su compra total). Semáforo: %s.' % (
                    len(self.linea_ids), self.fecha_inicio, self.fecha_fin, self.currency_id.name,
                    self.descuento_pronostico, self.descuento_pct_pronostico, self.descuento_pct_compra,
                    dict(SEMAFOROS).get(self.semaforo, 'sin costo')))

    def _al_ganar(self):
        return self._aplicar_en_tarifa()

    @api.model
    def _cron_ciclo(self):
        return super()._cron_ciclo()

    # ==================================================================
    # Al ganar: tarifa y acuerdos
    # ==================================================================
    @api.model
    def _inicio_dt(self, dia):
        return datetime.combine(dia, time.min)

    @api.model
    def _fin_dt(self, dia):
        return datetime.combine(dia, time(23, 59, 59))

    def _aplicar_en_tarifa(self):
        """Ganada + cliente aprobó ⇒ cada línea cierra el precio vigente de la tarifa propia del cliente con
        ``date_end`` = inicio − 1 día y crea el nuevo con la vigencia de la negociación; la escalera además crea
        su acuerdo de volumen. Una línea ya aplicada no se vuelve a aplicar."""
        for rec in self:
            if rec.state != 'ganada' or not rec.cliente_aprobo:
                continue
            pendientes = rec.linea_ids.filtered(lambda l: not l.pricelist_item_id)
            if not pendientes:
                continue
            tarifa = rec._tarifa_del_cliente()
            textos = []
            for linea in pendientes:
                textos.append(linea._aplicar_en_tarifa(tarifa))
                if linea.tipo == 'escalera_incremental' and not linea.acuerdo_id:
                    acuerdo = self.env['qb.cotizador.acuerdo.volumen'].sudo()._crear_de_linea(linea)
                    textos.append('Acuerdo de volumen %s creado.' % acuerdo.name)
            rec.write({'aplicada_el': fields.Datetime.now()})
            rec.message_post(body='Precios puestos en la tarifa %s:<br/>%s' % (tarifa.name, '<br/>'.join(textos)))
        return True

    # ==================================================================
    # PDF y navegación
    # ==================================================================
    def action_print_cliente(self):
        for rec in self:
            if rec.state not in APROBADAS:
                raise UserError('La carta al cliente solo se imprime con la negociación aprobada (%s).'
                                % rec._quien_aprueba_texto())
        return self.env.ref('qb_cotizador.action_report_negociacion_cliente').report_action(self)

    def action_print_interna(self):
        self.filtered(lambda r: not r.simulado_el and r.linea_ids).action_simular()
        return self.env.ref('qb_cotizador.action_report_negociacion_interna').report_action(self)

    def action_ver_acuerdos(self):
        self.ensure_one()
        return {'type': 'ir.actions.act_window', 'name': 'Acuerdos de volumen',
                'res_model': 'qb.cotizador.acuerdo.volumen', 'view_mode': 'list,form',
                'domain': [('negociacion_id', '=', self.id)]}

    def _idioma_carta(self):
        """'es' o 'en' según el idioma del contacto de la carta (o del cliente)."""
        self.ensure_one()
        lang = (self.contacto_id or self.partner_id).lang or ''
        return 'es' if lang.startswith('es') else 'en'


class QbCotizadorNegociacionLinea(models.Model):
    _name = 'qb.cotizador.negociacion.linea'
    _description = 'Producto de una negociación de precio'
    _order = 'negociacion_id, sequence, id'

    negociacion_id = fields.Many2one('qb.cotizador.negociacion', required=True, ondelete='cascade', index=True)
    sequence = fields.Integer(default=10)
    estado = fields.Selection(related='negociacion_id.state', string='Estado de la negociación')
    currency_id = fields.Many2one(related='negociacion_id.currency_id')
    product_id = fields.Many2one('product.product', string='Producto', required=True)
    uom_name = fields.Char(related='product_id.uom_id.name', string='Unidad')
    tipo = fields.Selection(TIPOS, string='Tipo', required=True, default='precio_nuevo')
    precio_actual = fields.Float(string='Precio actual', digits=(16, 4),
                                 help='El vigente en la tarifa del cliente, en la moneda de la negociación. Se llena '
                                      'solo al elegir el producto; se puede corregir.')
    precio_actual_fuente = fields.Char(string='De dónde sale el precio actual', readonly=True)
    precio_nuevo = fields.Float(string='Precio nuevo', digits=(16, 4))
    rebaja_pct = fields.Float(string='Rebaja (%)', digits=(6, 3))
    precio_base = fields.Float(string='Precio base de la escalera', digits=(16, 4),
                               help='El precio que va a la tarifa; los tramos se descuentan sobre él con nota de '
                                    'crédito al cierre de cada periodo.')
    precio_resultante = fields.Float(string='Precio en tarifa', digits=(16, 4), compute='_compute_precio',
                                     store=True)
    precio_alerta = fields.Char(compute='_compute_precio_alerta')
    periodo = fields.Selection(PERIODOS, string='Periodo de medición', default='trimestral',
                               help='Los tramos se capturan por mes; trimestral multiplica los umbrales × 3 y anual '
                                    '× 12.')
    solo_pagadas_en_plazo = fields.Boolean(string='Solo facturas pagadas en plazo',
                                           help='En la liquidación del acuerdo solo cuentan las facturas pagadas a '
                                                'más tardar en su fecha de vencimiento.')
    tramo_ids = fields.One2many('qb.cotizador.negociacion.tramo', 'linea_id', string='Tramos',
                                domain=[('de_cliente', '=', False)], copy=True)
    cliente_tramo_ids = fields.One2many('qb.cotizador.negociacion.tramo', 'linea_id',
                                        string='Tramos que pide el cliente', domain=[('de_cliente', '=', True)],
                                        copy=True, context={'default_de_cliente': True})
    volumen_12m = fields.Float(string='Volumen 12 meses', digits=(16, 0), readonly=True,
                               help='Unidades facturadas al cliente en los 12 meses anteriores (facturas menos notas '
                                    'de crédito; la nota de una refacturación cuenta en la fecha de la factura que '
                                    'corrige).')
    volumen_mensual = fields.Float(string='Promedio mensual', digits=(16, 0), compute='_compute_volumen_mensual',
                                   store=True)
    volumen_pronostico = fields.Float(string='Pronóstico anual', digits=(16, 0),
                                      help='Unidades al año que se esperan (p. ej. el forecast anual del cliente). Por '
                                           'omisión, el volumen de los últimos 12 meses.')
    sim_volumenes = fields.Char(string='Volúmenes libres (al año)',
                                help='Otros volúmenes anuales a simular, separados por coma (p. ej. 700000, 1200000).')
    periodo_costo_id = fields.Many2one('qb.periodo', string='Período del costo', readonly=True)
    costo_id = fields.Many2one('qb.costo.unitario', string='Costo usado', readonly=True)
    costo_variable = fields.Float(string='Costo variable', digits=(16, 4), readonly=True,
                                  help='Piso con capacidad ociosa (costo variable ÷ rendimiento) del último período, '
                                       'en la moneda de la negociación.')
    costo_completo = fields.Float(string='Costo completo', digits=(16, 4), readonly=True,
                                  help='Piso a planta llena (costo vendible ÷ (1 − operación)) del último período, en '
                                       'la moneda de la negociación.')
    semaforo = fields.Selection(SEMAFOROS, string='Semáforo', compute='_compute_semaforo', store=True,
                                help='El peor de sus precios: el de tarifa y, en la escalera, el de cada tramo.')
    precio_minimo = fields.Float(string='Precio más bajo', digits=(16, 4), compute='_compute_semaforo', store=True,
                                 help='En la escalera, el del último tramo.')
    pricelist_item_id = fields.Many2one('product.pricelist.item', string='Precio nuevo en tarifa', readonly=True,
                                        copy=False)
    item_cerrado_ids = fields.Many2many('product.pricelist.item', 'qb_cot_neg_linea_item_cerrado_rel',
                                        'linea_id', 'item_id', string='Precios cerrados', readonly=True, copy=False)
    acuerdo_id = fields.Many2one('qb.cotizador.acuerdo.volumen', string='Acuerdo de volumen', readonly=True,
                                 copy=False)

    @api.depends('tipo', 'precio_actual', 'precio_nuevo', 'rebaja_pct', 'precio_base')
    def _compute_precio(self):
        for rec in self:
            if rec.tipo == 'precio_nuevo':
                precio = rec.precio_nuevo
            elif rec.tipo == 'rebaja_pct':
                precio = rec.precio_actual * (1 - (rec.rebaja_pct or 0.0) / 100.0)
            else:
                precio = rec.precio_base
            rec.precio_resultante = round(precio, 6)

    @api.depends('tipo', 'precio_actual', 'product_id')
    def _compute_precio_alerta(self):
        for rec in self:
            rec.precio_alerta = '%s no tiene precio actual: la rebaja no tiene sobre qué calcularse' % (
                rec.product_id.default_code or rec.product_id.display_name) \
                if rec.tipo == 'rebaja_pct' and not rec.precio_actual else ''

    @api.depends('volumen_12m')
    def _compute_volumen_mensual(self):
        for rec in self:
            rec.volumen_mensual = rec.volumen_12m / 12.0

    @api.depends('tipo', 'precio_resultante', 'costo_variable', 'costo_completo', 'tramo_ids.descuento_pct')
    def _compute_semaforo(self):
        for rec in self:
            precios = [rec.precio_resultante]
            if rec.tipo == 'escalera_incremental':
                precios += [rec.precio_resultante * (1 - t.descuento_pct / 100.0) for t in rec.tramo_ids]
            minimo = min(precios) if precios else 0.0
            rec.precio_minimo = minimo
            rec.semaforo = _semaforo(minimo, rec.costo_variable, rec.costo_completo)

    # ------------------------------------------------------------------
    # Prellenado
    # ------------------------------------------------------------------
    @api.model_create_multi
    def create(self, vals_list):
        recs = super().create(vals_list)
        # Lo que se lee de la base (volumen, costo, fuente) siempre; lo capturable, solo si no vino.
        siempre = ('volumen_12m', 'precio_actual_fuente', 'periodo_costo_id', 'costo_id', 'costo_variable',
                   'costo_completo')
        for rec, vals in zip(recs, vals_list):
            pre = rec._valores_prellenado()
            upd = {k: v for k, v in pre.items() if k in siempre or k not in vals}
            if 'precio_actual' in vals and abs(vals['precio_actual'] - pre.get('precio_actual', 0.0)) > 1e-6:
                upd['precio_actual_fuente'] = 'capturado a mano'
                for campo in ('precio_base', 'precio_nuevo'):
                    if campo in upd:
                        upd[campo] = vals['precio_actual']
            if upd:
                rec.write(upd)
        return recs

    @api.onchange('product_id')
    def _onchange_product_id(self):
        if self.product_id:
            self.update(self._valores_prellenado())

    @api.onchange('tipo', 'precio_actual')
    def _onchange_tipo(self):
        if self.tipo == 'escalera_incremental' and not self.precio_base:
            self.precio_base = self.precio_actual
        if self.tipo == 'precio_nuevo' and not self.precio_nuevo:
            self.precio_nuevo = self.precio_actual

    def _valores_prellenado(self, conservar_pronostico=False):
        """Precio vigente de la tarifa del cliente, volumen de 12 meses y costo del último período."""
        self.ensure_one()
        neg = self.negociacion_id
        vals = {}
        if not self.product_id or not neg.partner_id:
            return vals
        precio, fuente = self._precio_vigente()
        vals.update({'precio_actual': precio, 'precio_actual_fuente': fuente})
        desde, hasta = neg._ventana_12m()
        Acuerdo = self.env['qb.cotizador.acuerdo.volumen']
        movs = Acuerdo._unidades_por_movimiento(neg.partner_id, self.product_id, desde, hasta, neg.company_id)
        vol = sum(movs.values())
        vals['volumen_12m'] = vol
        if not (conservar_pronostico and self.volumen_pronostico):
            vals['volumen_pronostico'] = vol
        vals.update(self._valores_costo())
        if self.tipo == 'escalera_incremental' and not self.precio_base:
            vals['precio_base'] = precio
        if self.tipo == 'precio_nuevo' and not self.precio_nuevo:
            vals['precio_nuevo'] = precio
        return vals

    def _precio_vigente(self):
        """(precio, fuente) de la regla vigente hoy de la tarifa del cliente, en la moneda de la negociación."""
        self.ensure_one()
        neg = self.negociacion_id
        company = neg.company_id or self.env.company
        tarifa = neg.partner_id.commercial_partner_id.with_company(company).property_product_pricelist
        if not tarifa:
            return 0.0, 'el cliente no tiene tarifa'
        hoy = fields.Date.context_today(self)
        tarifa = tarifa.sudo()
        regla = self.env['product.pricelist.item'].sudo().browse(
            tarifa._get_product_rule(self.product_id, 1.0, date=hoy))
        if not regla:
            return 0.0, 'la tarifa %s no tiene precio para el producto' % tarifa.name
        precio = tarifa._get_product_price(self.product_id, 1.0, date=hoy)
        if tarifa.currency_id != neg.currency_id:
            precio = tarifa.currency_id._convert(precio, neg.currency_id, company, hoy, round=False)
        desde = regla.date_start.date() if regla.date_start else None
        return precio, 'tarifa %s (regla %d%s)' % (tarifa.name, regla.id, (', desde %s' % desde) if desde else '')

    def _valores_costo(self):
        """Pisos del último período cerrado de qb_costeo (sin cerrado, el último con costos) en la moneda de la
        negociación."""
        self.ensure_one()
        neg = self.negociacion_id
        company = neg.company_id or self.env.company
        Periodo = self.env['qb.periodo'].sudo()
        periodo = Periodo.para_cotizar(company) or Periodo.search(
            [('company_id', '=', company.id), ('costo_ids', '!=', False)], order='period desc', limit=1)
        costo = self.env['qb.costo.unitario'].sudo().search(
            [('periodo_id', '=', periodo.id), ('product_id', '=', self.product_id.id)], limit=1) if periodo else None
        if not costo:
            return {'periodo_costo_id': periodo.id if periodo else False, 'costo_id': False,
                    'costo_variable': 0.0, 'costo_completo': 0.0}
        return {'periodo_costo_id': periodo.id, 'costo_id': costo.id,
                'costo_variable': neg._a_moneda(costo.piso_ocioso),
                'costo_completo': neg._a_moneda(costo.piso_lleno)}

    def action_actualizar(self):
        for rec in self:
            if rec.estado != 'borrador':
                raise UserError('Los datos solo se actualizan con la negociación en borrador.')
            rec.write(rec._valores_prellenado(conservar_pronostico=True))
        return True

    # ------------------------------------------------------------------
    # Cálculo
    # ------------------------------------------------------------------
    def _meses(self):
        return MESES_PERIODO.get(self.periodo or 'mensual', 1)

    def _tramos_tuplas(self, de_cliente=False):
        tramos = self.cliente_tramo_ids if de_cliente else self.tramo_ids
        return [(t.desde, t.hasta, t.descuento_pct) for t in tramos]

    def _descuento_anual(self, volumen_anual):
        """(descuento al año contra el precio actual, precio marginal más bajo alcanzado)."""
        self.ensure_one()
        actual = self.precio_actual or self.precio_resultante
        if self.tipo != 'escalera_incremental':
            return (actual - self.precio_resultante) * volumen_anual, self.precio_resultante
        meses = self._meses()
        periodos = 12 // meses
        por_periodo = volumen_anual / periodos
        desc, detalle = descuento_tramos(por_periodo, self._tramos_tuplas(), self.precio_base, meses)
        alcanzados = [pct for (_lo, _hi, pct, u, _d) in detalle if u > 0]
        marginal = self.precio_base * (1 - max(alcanzados) / 100.0) if alcanzados else self.precio_base
        return (actual - self.precio_base) * volumen_anual + desc * periodos, marginal

    def _descuento_cliente(self, volumen_anual):
        """Lo que pide el cliente: su rebaja pareja sobre el precio actual y, encima, sus tramos."""
        self.ensure_one()
        actual = self.precio_actual or self.precio_resultante
        pct = self.negociacion_id.contra_rebaja_pct or 0.0
        desc = actual * volumen_anual * pct / 100.0
        if self.cliente_tramo_ids:
            meses = self._meses()
            periodos = 12 // meses
            tramos, _det = descuento_tramos(volumen_anual / periodos, self._tramos_tuplas(de_cliente=True),
                                            actual * (1 - pct / 100.0), meses)
            desc += tramos * periodos
        return desc

    def _volumenes_simulacion(self):
        self.ensure_one()
        var = (self.negociacion_id.sim_variacion_pct or 0.0) / 100.0
        prono = self.volumen_pronostico or self.volumen_12m
        vols = [('actual', self.volumen_12m), ('pronostico', prono),
                ('menos', prono * (1 - var)), ('mas', prono * (1 + var))]
        for parte in (self.sim_volumenes or '').replace(';', ',').split(','):
            parte = parte.strip().replace(' ', '').replace('_', '')
            if not parte:
                continue
            try:
                vols.append(('libre', float(parte)))
            except ValueError:
                raise UserError('Volumen libre inválido en %s: «%s». Use números separados por coma.' % (
                    self.product_id.display_name, parte))
        return vols

    def _filas_simulacion(self):
        self.ensure_one()
        neg = self.negociacion_id
        actual = self.precio_actual or self.precio_resultante
        con_contra = bool(neg.contra_rebaja_pct or self.cliente_tramo_ids)
        filas = []
        for orden, (esc, vol) in enumerate(self._volumenes_simulacion()):
            venta_actual = actual * vol
            desc, marginal = self._descuento_anual(vol)
            neta = venta_actual - desc
            c_desc = self._descuento_cliente(vol) if con_contra else 0.0
            c_neta = venta_actual - c_desc if con_contra else 0.0
            filas.append({
                'negociacion_id': neg.id, 'linea_id': self.id, 'escenario': esc, 'sequence': orden + 1,
                'volumen_anual': vol, 'venta_actual': venta_actual, 'descuento': desc,
                'pct_efectivo': 100.0 * desc / venta_actual if venta_actual else 0.0,
                'pct_compra_total': 100.0 * desc / neg.compra_12m if neg.compra_12m else 0.0,
                'venta_neta': neta,
                'contrib_variable': neta - vol * self.costo_variable if self.costo_completo else 0.0,
                'contrib_completa': neta - vol * self.costo_completo if self.costo_completo else 0.0,
                'precio_marginal': marginal,
                'semaforo': _semaforo(marginal, self.costo_variable, self.costo_completo),
                'con_contrapropuesta': con_contra,
                'cliente_descuento': c_desc, 'cliente_venta_neta': c_neta,
                'cliente_contrib_completa': (c_neta - vol * self.costo_completo) if con_contra and self.costo_completo
                else 0.0,
                'diferencia_cliente': (c_desc - desc) if con_contra else 0.0,
            })
        return filas

    # ------------------------------------------------------------------
    # Tarifa
    # ------------------------------------------------------------------
    def _aplicar_en_tarifa(self, tarifa):
        """Cierra los precios vigentes del producto en ``tarifa`` al día anterior al inicio y crea el nuevo."""
        self.ensure_one()
        neg = self.negociacion_id
        Item = self.env['product.pricelist.item'].sudo()
        producto = self.product_id
        inicio = neg._inicio_dt(neg.fecha_inicio)
        cierre = neg._fin_dt(neg.fecha_inicio - timedelta(days=1))
        vigentes = Item.search([
            ('pricelist_id', '=', tarifa.id),
            ('applied_on', 'in', ('1_product', '0_product_variant')),
            '|', ('product_id', '=', producto.id),
            '&', ('product_id', '=', False), ('product_tmpl_id', '=', producto.product_tmpl_id.id),
            '|', ('date_start', '=', False), ('date_start', '<', inicio),
            '|', ('date_end', '=', False), ('date_end', '>=', inicio),
        ])
        vigentes.write({'date_end': cierre})
        item = Item.create({
            'pricelist_id': tarifa.id, 'applied_on': '1_product',
            'product_tmpl_id': producto.product_tmpl_id.id,
            'compute_price': 'fixed', 'fixed_price': self.precio_resultante, 'min_quantity': 0,
            'date_start': inicio, 'date_end': neg._fin_dt(neg.fecha_fin),
        })
        self.write({'pricelist_item_id': item.id, 'item_cerrado_ids': [(4, i.id) for i in vigentes]})
        return '%s: %s %s del %s al %s%s.' % (
            producto.default_code or producto.display_name, self.precio_resultante, neg.currency_id.name,
            neg.fecha_inicio, neg.fecha_fin,
            ('; se cerró al %s el precio anterior (%s)' % (cierre.date(), ', '.join(
                '%s' % v.fixed_price for v in vigentes))) if vigentes else '')


class QbCotizadorNegociacionTramo(models.Model):
    _name = 'qb.cotizador.negociacion.tramo'
    _description = 'Tramo de una escalera incremental por volumen'
    _order = 'desde, id'

    linea_id = fields.Many2one('qb.cotizador.negociacion.linea', ondelete='cascade', index=True)
    acuerdo_id = fields.Many2one('qb.cotizador.acuerdo.volumen', ondelete='cascade', index=True)
    de_cliente = fields.Boolean(string='Lo pide el cliente')
    desde = fields.Float(string='Desde (u/mes)', digits=(16, 0), required=True)
    hasta = fields.Float(string='Hasta (u/mes)', digits=(16, 0), help='0 = sin límite.')
    descuento_pct = fields.Float(string='Descuento (%)', digits=(6, 3))
    precio = fields.Float(string='Precio en el tramo', digits=(16, 4), compute='_compute_precio')
    semaforo = fields.Selection(SEMAFOROS, string='Semáforo', compute='_compute_precio')

    @api.depends('descuento_pct', 'linea_id.precio_resultante', 'linea_id.precio_actual', 'acuerdo_id.precio_base',
                 'linea_id.costo_variable', 'linea_id.costo_completo')
    def _compute_precio(self):
        for rec in self:
            if rec.acuerdo_id:
                base, var, completo = rec.acuerdo_id.precio_base, 0.0, 0.0
            else:
                linea = rec.linea_id
                base = linea.precio_actual if rec.de_cliente else linea.precio_resultante
                var, completo = linea.costo_variable, linea.costo_completo
            rec.precio = base * (1 - (rec.descuento_pct or 0.0) / 100.0)
            rec.semaforo = _semaforo(rec.precio, var, completo)

    @api.constrains('desde', 'hasta', 'descuento_pct', 'linea_id', 'acuerdo_id', 'de_cliente')
    def _check_escalera(self):
        for rec in self:
            hermanos = self.search([('linea_id', '=', rec.linea_id.id), ('acuerdo_id', '=', rec.acuerdo_id.id),
                                    ('de_cliente', '=', rec.de_cliente)])
            hermanos._validar_escalera()

    def _validar_escalera(self):
        """Tramos sin huecos raros: desde < hasta, sin traslapes y solo el último sin límite."""
        tramos = self.sorted('desde')
        for i, t in enumerate(tramos):
            if t.desde < 0 or (t.hasta and t.hasta <= t.desde):
                raise ValidationError('Tramo %s–%s: «hasta» debe ser mayor que «desde».' % (t.desde, t.hasta or '∞'))
            if not t.hasta and i < len(tramos) - 1:
                raise ValidationError('Solo el último tramo puede ir sin límite («hasta» 0).')
            if i and tramos[i - 1].hasta and t.desde < tramos[i - 1].hasta:
                raise ValidationError('Los tramos %s–%s y %s–%s se traslapan.' % (
                    tramos[i - 1].desde, tramos[i - 1].hasta, t.desde, t.hasta or '∞'))
            if not 0 <= (t.descuento_pct or 0.0) < 100:
                raise ValidationError('El descuento de un tramo va de 0 a menos de 100 %.')


class QbCotizadorNegociacionSimulacion(models.Model):
    _name = 'qb.cotizador.negociacion.simulacion'
    _description = 'Escenario del simulador de una negociación'
    _order = 'negociacion_id, linea_id, sequence, id'

    negociacion_id = fields.Many2one('qb.cotizador.negociacion', required=True, ondelete='cascade', index=True)
    linea_id = fields.Many2one('qb.cotizador.negociacion.linea', string='Producto (línea)', ondelete='cascade')
    product_id = fields.Many2one(related='linea_id.product_id', string='Producto')
    sequence = fields.Integer()
    escenario = fields.Selection(ESCENARIOS, string='Escenario', required=True)
    volumen_anual = fields.Float(string='Volumen al año', digits=(16, 0))
    venta_actual = fields.Float(string='Venta a precio actual', digits=(16, 2))
    descuento = fields.Float(string='Descuento al año', digits=(16, 2))
    pct_efectivo = fields.Float(string='% efectivo en el producto', digits=(6, 2))
    pct_compra_total = fields.Float(string='% de su compra total', digits=(6, 2))
    venta_neta = fields.Float(string='Venta neta', digits=(16, 2))
    contrib_variable = fields.Float(string='Contribución sobre costo variable', digits=(16, 2))
    contrib_completa = fields.Float(string='Contribución sobre costo completo', digits=(16, 2))
    precio_marginal = fields.Float(string='Precio del último tramo alcanzado', digits=(16, 4))
    semaforo = fields.Selection(SEMAFOROS, string='Semáforo')
    con_contrapropuesta = fields.Boolean(string='Con contrapropuesta')
    cliente_descuento = fields.Float(string='Descuento que pide el cliente', digits=(16, 2))
    cliente_venta_neta = fields.Float(string='Venta neta (cliente)', digits=(16, 2))
    cliente_contrib_completa = fields.Float(string='Contribución sobre costo completo (cliente)', digits=(16, 2))
    diferencia_cliente = fields.Float(string='Cliente − propuesta', digits=(16, 2),
                                      help='Cuánto más descuento pide el cliente que la propuesta (negativo: menos).')
