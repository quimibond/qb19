# -*- coding: utf-8 -*-
"""Cotización de costo (spec §6 y §6.1; brief C1 §6.8).

Lo que guarda es una **foto del costo** (del último período cerrado de
`qb_costeo`, de un producto hermano o capturada a mano, siempre con su
fuente) y el **precio vigente**; pisos, márgenes y semáforo se calculan del
precio sobre esa foto, así que editar el precio los actualiza y recalcular
el costo es una acción explícita que deja rastro.

Ciclo (procedimiento C1):

    borrador → por_aprobar → presentada → ganada | perdida | vencida
                 ↓ regreso con motivo                     ↓ renovar
               borrador                              reemplazada (la vieja)

- Solo el **puesto** configurado (por omisión 183, Director de Finanzas y
  Administración) o su suplente (parámetro vacío) aprueba; una cotización
  en rojo solo la aprueba el grupo «Autoriza precio bajo piso» (CEO).
- Sin aprobación no se imprime el PDF comercial.
- Al presentar: validez, seguimiento a Ventas a los N días hábiles, y al
  vencer pasa a «Vencida» y exige decidir (renovar, ganada, perdida).
- Al ganar con el cliente aprobando la muestra: precio en la tarifa del
  cliente con precio, moneda y vigencia de la cotización.
- Borradores sin movimiento N días se archivan.
"""
import logging
import math
from datetime import timedelta

from odoo import api, fields, models
from odoo.exceptions import UserError

from .settings import (
    APROBADOR_JOB_NAME, PARAM_APROBADOR, PARAM_ARCHIVAR_DIAS, PARAM_ESCALERA_PCT,
    PARAM_MARGEN_MINIMO, PARAM_SEGUIMIENTO_DIAS, PARAM_SUPLENTE, PARAM_VALIDEZ_DIAS,
    PARAM_VENTAS, VENTAS_JOB_NAME,
)

_logger = logging.getLogger(__name__)

STATES = [
    ('borrador', 'Borrador'),
    ('por_aprobar', 'Por aprobar'),
    ('presentada', 'Presentada'),
    ('ganada', 'Ganada'),
    ('perdida', 'Perdida'),
    ('vencida', 'Vencida'),
    ('reemplazada', 'Reemplazada'),
]
VIVAS = ('borrador', 'por_aprobar', 'presentada', 'vencida')
APROBADAS = ('presentada', 'ganada', 'perdida', 'vencida')
COSTO_FUENTES = [
    ('periodo', 'Costo del producto en el período cerrado'),
    ('hermano', 'Costo de un producto hermano'),
    ('manual', 'Capturado a mano (desarrollo sin receta)'),
    ('legado', 'Importado del cotizador anterior'),
]
SEMAFOROS = [
    ('rojo', 'Debajo del costo variable'),
    ('ambar', 'Entre pisos: aporta a fijos'),
    ('verde', 'Cubre todo y deja margen'),
]
CLIENTE_MEDIOS = [
    ('correo', 'Correo'), ('oc', 'Orden de compra'), ('whatsapp', 'WhatsApp'),
    ('cotizacion_firmada', 'Cotización firmada'),
]
VOLUMEN_UOMS = [('m', 'm / mes'), ('kg', 'kg / mes')]
ESCALERA_MULTIPLOS = (0.5, 1.0, 2.0, 4.0)
# Estados del cotizador anterior → de este.
LEGACY_STATE = {'draft': 'borrador', 'done': 'presentada', 'won': 'ganada',
                'lost': 'perdida', 'superseded': 'reemplazada'}


class QbCotizadorCotizacion(models.Model):
    _name = 'qb.cotizador.cotizacion'
    _description = 'Cotización de costo'
    _inherit = ['mail.thread', 'mail.activity.mixin']
    _order = 'id desc'

    # ------------------------------------------------------------------
    # Identidad
    # ------------------------------------------------------------------
    name = fields.Char(compute='_compute_name', store=True)
    folio = fields.Char(readonly=True, copy=False, index=True)
    active = fields.Boolean(default=True)
    company_id = fields.Many2one('res.company', required=True,
                                 default=lambda self: self.env.company)
    user_id = fields.Many2one('res.users', string='Vendedor',
                              default=lambda self: self.env.user, tracking=True)
    partner_id = fields.Many2one('res.partner', string='Cliente', tracking=True)
    atencion_a = fields.Char(string='En atención a',
                             help='Nombre y puesto del contacto como sale en el PDF. '
                                  'Vacío: el nombre del cliente.')
    product_id = fields.Many2one('product.product', string='Producto existente', tracking=True)
    spec_descripcion = fields.Char(string='Especificación nueva',
                                   help='Producto que aún no existe: descripción corta.')
    spec_gramaje = fields.Float(string='Gramaje (g/m²)')
    spec_ancho = fields.Float(string='Ancho (m)')
    spec_galga = fields.Char(string='Galga')
    volumen = fields.Float(string='Volumen mensual', tracking=True)
    volumen_uom = fields.Selection(VOLUMEN_UOMS, string='Unidad del volumen', default='m')
    currency_id = fields.Many2one('res.currency', string='Moneda', required=True,
                                  default=lambda self: self.env.company.currency_id)
    fx_rate = fields.Float(string='TC (MXN por 1 divisa)', digits=(16, 4), default=1.0,
                           help='Tipo de cambio de Odoo el día del cálculo; queda guardado.')
    es_divisa = fields.Boolean(compute='_compute_es_divisa')

    # ------------------------------------------------------------------
    # Foto del costo (siempre en moneda de la compañía, por unidad vendible)
    # ------------------------------------------------------------------
    costo_fuente = fields.Selection(COSTO_FUENTES, string='Fuente del costo', default='periodo',
                                    tracking=True)
    hermano_product_id = fields.Many2one('product.product', string='Producto hermano',
                                         help='Producto existente cuyo costo se usa como '
                                              'referencia para una especificación nueva.')
    periodo_id = fields.Many2one('qb.periodo', string='Período del costo', readonly=True,
                                 copy=False)
    costo_id = fields.Many2one('qb.costo.unitario', string='Costo usado', readonly=True,
                               copy=False)
    calculado_el = fields.Datetime(readonly=True, copy=False)
    mp_unit = fields.Float(string='Materia prima $/u', digits=(16, 4))
    energia_unit = fields.Float(string='Energía $/u', digits=(16, 4))
    fabricacion_unit = fields.Float(string='Fabricación $/u', digits=(16, 4),
                                    help='Horas × tarifa del centro (fija + variable): la '
                                         'energía va adentro.')
    costo_variable = fields.Float(string='Costo variable $/u', digits=(16, 4),
                                  help='MP + energía por unidad producida.')
    costo_produccion = fields.Float(string='Costo de producción $/u', digits=(16, 4),
                                    help='MP + fabricación por unidad producida.')
    rendimiento = fields.Float(string='Rendimiento vendible', digits=(6, 4), default=1.0)
    op_pct = fields.Float(string='Operación (fracción de la venta)', digits=(16, 6))
    costo_vendible = fields.Float(string='Costo vendible $/u', digits=(16, 4),
                                  compute='_compute_pisos', store=True)
    piso_ocioso = fields.Float(string='Piso con capacidad ociosa $/u', digits=(16, 4),
                               compute='_compute_pisos', store=True)
    piso_lleno = fields.Float(string='Piso a planta llena $/u', digits=(16, 4),
                              compute='_compute_pisos', store=True)
    calidad = fields.Selection([
        ('alta', 'Alta'), ('media', 'Media'), ('baja', 'Baja'),
        ('dudosa', 'Dudosa'), ('ninguna', 'Sin dato')], string='Calidad del costo',
        default='ninguna', readonly=True, copy=False)
    calidad_detalle = fields.Char(readonly=True, copy=False)
    precio_mercado = fields.Float(string='Precio de mercado $/u MXN', digits=(16, 4),
                                  readonly=True, copy=False,
                                  help='Precio promedio facturado del producto en el período '
                                       'del costo (todos los clientes).')
    costo_muestra = fields.Float(string='Costo de la muestra $ MXN', digits=(16, 2),
                                 tracking=True,
                                 help='Lo que cuesta correr la muestra (una muestra con teñido '
                                      'cuesta el baño completo). Visible antes de aprobar.')
    costo_muestra_nota = fields.Char(string='Cómo se calculó la muestra')

    # ------------------------------------------------------------------
    # Precio y lo que deja
    # ------------------------------------------------------------------
    precio_objetivo = fields.Monetary(string='Precio al cliente', currency_field='currency_id',
                                      tracking=True,
                                      help='En la moneda de la cotización; se convierte a MXN '
                                           'con el TC guardado para medirlo contra los pisos.')
    precio_mxn = fields.Float(string='Precio $/u MXN', digits=(16, 4),
                              compute='_compute_margenes', store=True)
    margen_contribucion_pct = fields.Float(string='Contribución %', digits=(6, 2),
                                           compute='_compute_margenes', store=True)
    margen_bruto_pct = fields.Float(string='Margen bruto %', digits=(6, 2),
                                    compute='_compute_margenes', store=True)
    margen_neto_pct = fields.Float(string='Margen neto %', digits=(6, 2),
                                   compute='_compute_margenes', store=True)
    semaforo = fields.Selection(SEMAFOROS, compute='_compute_margenes', store=True)
    precio_vs_piso_pct = fields.Float(string='% sobre el piso lleno', digits=(6, 2),
                                      compute='_compute_margenes', store=True)
    margen_minimo_pct = fields.Float(compute='_compute_margen_minimo')
    bajo_margen_minimo = fields.Boolean(compute='_compute_margen_minimo')
    piso_ocioso_divisa = fields.Float(compute='_compute_divisa', digits=(16, 4))
    piso_lleno_divisa = fields.Float(compute='_compute_divisa', digits=(16, 4))
    precio_mercado_divisa = fields.Float(compute='_compute_divisa', digits=(16, 4))
    con_escalera = fields.Boolean(string='Ofrecer precios por volumen',
                                  help='El PDF lleva la escalera (½×, 1×, 2×, 4×) si el '
                                       'parámetro de descuento tiene valor.')
    tramo_ids = fields.One2many('qb.cotizador.tramo', 'cotizacion_id', string='Escalera de volumen',
                                copy=True)

    # ------------------------------------------------------------------
    # Ciclo
    # ------------------------------------------------------------------
    state = fields.Selection(STATES, default='borrador', tracking=True, copy=False, index=True)
    aprobador_job_id = fields.Many2one('hr.job', compute='_compute_aprobacion')
    puede_aprobar = fields.Boolean(compute='_compute_aprobacion')
    solicitada_por_id = fields.Many2one('res.users', readonly=True, copy=False)
    solicitada_el = fields.Datetime(readonly=True, copy=False)
    approved_by_id = fields.Many2one('res.users', string='Aprobó', readonly=True, copy=False)
    approved_date = fields.Datetime(string='Aprobada el', readonly=True, copy=False)
    regreso_motivo_id = fields.Many2one('qb.cotizador.motivo', string='Último motivo de regreso',
                                        readonly=True, copy=False)
    regreso_count = fields.Integer(string='Veces regresada', readonly=True, copy=False)
    presented_by_id = fields.Many2one('res.users', string='Presentó', readonly=True, copy=False)
    presented_date = fields.Datetime(string='Presentada el', readonly=True, copy=False)
    validez_hasta = fields.Date(string='Válida hasta', tracking=True, copy=False)
    seguimiento_fecha = fields.Date(string='Seguimiento el', readonly=True, copy=False)
    seguimiento_hecho = fields.Boolean(readonly=True, copy=False)
    cliente_aprobo = fields.Boolean(string='El cliente aprobó la muestra', tracking=True, copy=False)
    cliente_medio = fields.Selection(CLIENTE_MEDIOS, string='Medio de aprobación', copy=False)
    cliente_fecha = fields.Date(string='Fecha de aprobación del cliente', copy=False)
    cliente_evidencia = fields.Binary(string='Evidencia', attachment=True, copy=False)
    cliente_evidencia_nombre = fields.Char(copy=False)
    ganada_date = fields.Datetime(string='Ganada el', readonly=True, copy=False)
    sale_order_id = fields.Many2one('sale.order', string='Pedido', readonly=True, copy=False)
    pricelist_item_id = fields.Many2one('product.pricelist.item', string='Precio en tarifa',
                                        readonly=True, copy=False)
    perdida_motivo_id = fields.Many2one('qb.cotizador.motivo', string='Motivo de pérdida',
                                        readonly=True, copy=False)
    perdida_nota = fields.Char(readonly=True, copy=False)
    revision = fields.Integer(default=1, readonly=True, copy=False)
    revision_anterior_id = fields.Many2one('qb.cotizador.cotizacion', string='Sustituye a',
                                           readonly=True, copy=False)
    revision_siguiente_ids = fields.One2many('qb.cotizador.cotizacion', 'revision_anterior_id',
                                             string='Sustituida por')
    legacy_id = fields.Integer(string='Id en el cotizador anterior', readonly=True, copy=False,
                               index=True)

    # ------------------------------------------------------------------
    # Condiciones del PDF comercial (F-P-A28-12)
    # ------------------------------------------------------------------
    lote_minimo = fields.Char(string='Lote mínimo', default='5,000 m')
    presentacion_rollos = fields.Char(string='Presentación de rollos', default='500 m ± 100 m')
    lugar_entrega = fields.Char(string='Lugar y condición de entrega',
                                default='Toluca, Estado de México (EXWORKS)')
    tiempo_entrega = fields.Char(string='Tiempo de entrega',
                                 default='4 semanas a partir de la liberación o forecast / '
                                         '4 weeks after release or forecast')
    muestra_leyenda = fields.Char(string='Leyenda de muestra',
                                  default='Muestra menor a 50 m sin costo / Sample less than 50 '
                                          'meters free of charge')
    tc_coa = fields.Boolean(string='CoA al 100 %', default=True)
    tc_pruebas_lab = fields.Boolean(string='Pruebas especiales de laboratorio')
    tc_lta = fields.Boolean(string='LTA')
    tc_ppap = fields.Boolean(string='PPAP', default=True)
    tc_inspeccion_total = fields.Boolean(string='Inspección total', default=True)
    tc_cpk = fields.Boolean(string='CPK 3 sigma')
    tc_apqp = fields.Boolean(string='APQP')
    tc_pscr = fields.Boolean(string='PSCR')
    tc_ctpat = fields.Boolean(string='Evidencia de carga C-TPAT')
    tc_hoja_seguridad = fields.Boolean(string='Hoja de seguridad')
    tc_imds = fields.Boolean(string='IMDS')
    tc_iso = fields.Boolean(string='Certificación ISO')
    supuestos = fields.Text(readonly=True, copy=False)

    # ==================================================================
    # Cómputos
    # ==================================================================
    @api.depends('folio', 'product_id.default_code', 'product_id.name', 'spec_descripcion',
                 'partner_id.name')
    def _compute_name(self):
        for rec in self:
            que = rec.product_id.default_code or rec.product_id.name or rec.spec_descripcion or ''
            rec.name = ' · '.join(p for p in (rec.folio, que, rec.partner_id.name) if p)

    @api.depends('currency_id', 'company_id')
    def _compute_es_divisa(self):
        for rec in self:
            rec.es_divisa = rec.currency_id != rec.company_id.currency_id

    @api.depends('costo_variable', 'costo_produccion', 'rendimiento', 'op_pct')
    def _compute_pisos(self):
        for rec in self:
            rend = min(max(rec.rendimiento or 1.0, 0.05), 1.0)
            rec.costo_vendible = rec.costo_produccion / rend
            rec.piso_ocioso = rec.costo_variable / rend
            op = rec.op_pct or 0.0
            rec.piso_lleno = rec.costo_vendible / (1 - op) if op < 1 else rec.costo_vendible

    @api.depends('precio_objetivo', 'fx_rate', 'piso_ocioso', 'piso_lleno', 'costo_vendible',
                 'op_pct')
    def _compute_margenes(self):
        for rec in self:
            fx = rec.fx_rate or 1.0
            precio = (rec.precio_objetivo or 0.0) * fx
            rec.precio_mxn = precio
            if not precio:
                rec.margen_contribucion_pct = rec.margen_bruto_pct = 0.0
                rec.margen_neto_pct = rec.precio_vs_piso_pct = 0.0
                rec.semaforo = False
                continue
            rec.margen_contribucion_pct = 100.0 * (precio - rec.piso_ocioso) / precio
            bruto = 100.0 * (precio - rec.costo_vendible) / precio
            rec.margen_bruto_pct = bruto
            rec.margen_neto_pct = bruto - 100.0 * (rec.op_pct or 0.0)
            rec.precio_vs_piso_pct = (100.0 * (precio / rec.piso_lleno - 1.0)
                                      if rec.piso_lleno else 0.0)
            rec.semaforo = self._semaforo_for(precio, rec.piso_ocioso, rec.piso_lleno)

    @api.model
    def _semaforo_for(self, precio, piso_ocioso, piso_lleno):
        if not precio:
            return False
        if precio < piso_ocioso:
            return 'rojo'
        if piso_lleno and precio < piso_lleno:
            return 'ambar'
        return 'verde'

    @api.depends('margen_neto_pct')
    def _compute_margen_minimo(self):
        minimo = self._param_float(PARAM_MARGEN_MINIMO)
        for rec in self:
            rec.margen_minimo_pct = minimo
            rec.bajo_margen_minimo = bool(minimo) and bool(rec.precio_mxn) and \
                rec.margen_neto_pct < minimo

    @api.depends('fx_rate', 'piso_ocioso', 'piso_lleno', 'precio_mercado')
    def _compute_divisa(self):
        for rec in self:
            fx = rec.fx_rate or 1.0
            rec.piso_ocioso_divisa = rec.piso_ocioso / fx
            rec.piso_lleno_divisa = rec.piso_lleno / fx
            rec.precio_mercado_divisa = rec.precio_mercado / fx

    @api.depends_context('uid')
    @api.depends('state', 'semaforo')
    def _compute_aprobacion(self):
        job = self._job_from_param(PARAM_APROBADOR)
        for rec in self:
            rec.aprobador_job_id = job
            rec.puede_aprobar = rec._puede_aprobar(self.env.user)

    # ==================================================================
    # Parámetros y puestos
    # ==================================================================
    @api.model
    def _param_float(self, key, default=0.0):
        val = self.env['ir.config_parameter'].sudo().get_param(key)
        try:
            return float(val) if val not in (None, False, '') else default
        except ValueError:
            return default

    @api.model
    def _param_int(self, key, default=0):
        return int(self._param_float(key, default))

    @api.model
    def _job_from_param(self, key):
        val = self.env['ir.config_parameter'].sudo().get_param(key)
        try:
            job = self.env['hr.job'].sudo().browse(int(val)) if val else self.env['hr.job']
        except ValueError:
            return self.env['hr.job']
        return job if job.exists() else self.env['hr.job']

    @api.model
    def _users_of_job(self, job):
        """Usuarios de las personas con ese puesto (todas las compañías)."""
        if not job:
            return self.env['res.users']
        employees = self.env['hr.employee'].sudo().search([('job_id', '=', job.id)])
        return employees.mapped('user_id').filtered('active')

    @api.model
    def _buscar_puesto(self, nombre):
        """`hr.job` por nombre en cualquier idioma instalado (el nombre es
        traducible y una búsqueda sin idioma solo ve en_US)."""
        Job = self.env['hr.job'].sudo()
        for code, _name in self.env['res.lang'].get_installed():
            job = Job.with_context(lang=code).search([('name', '=ilike', nombre)], limit=1)
            if job:
                return job
        return Job

    @api.model
    def _configurar_puestos_por_omision(self):
        Param = self.env['ir.config_parameter'].sudo()
        for key, nombre in ((PARAM_APROBADOR, APROBADOR_JOB_NAME),
                            (PARAM_VENTAS, VENTAS_JOB_NAME)):
            if Param.get_param(key):
                continue
            job = self._buscar_puesto(nombre)
            if job:
                Param.set_param(key, str(job.id))
        if not Param.get_param(PARAM_VALIDEZ_DIAS):
            Param.set_param(PARAM_VALIDEZ_DIAS, '15')
        # 1.1.0 (Jose 2026-10-08, punto 2): los días de seguimiento y de
        # archivo de borradores no están definidos: se quedan vacíos y sin
        # valor no hay seguimiento automático ni se archivan borradores.

    def _puede_aprobar(self, user):
        self.ensure_one()
        if user.has_group('qb_cotizador.group_autoriza_bajo_piso'):
            return True
        if self.semaforo == 'rojo':
            return False
        titulares = self._users_of_job(self._job_from_param(PARAM_APROBADOR))
        suplentes = self._users_of_job(self._job_from_param(PARAM_SUPLENTE))
        return user in (titulares | suplentes)

    def _usuarios_ventas(self):
        users = self._users_of_job(self._job_from_param(PARAM_VENTAS))
        return users or self.user_id or self.create_uid

    # ==================================================================
    # Alta
    # ==================================================================
    @api.model_create_multi
    def create(self, vals_list):
        for vals in vals_list:
            if not vals.get('folio'):
                vals['folio'] = self.env['ir.sequence'].next_by_code('qb.cotizador.cotizacion')
        return super().create(vals_list)

    @api.onchange('product_id')
    def _onchange_product_id(self):
        if self.product_id:
            self.volumen_uom = 'kg' if self.product_id.uom_id.name.lower().startswith('k') else 'm'

    @api.onchange('partner_id')
    def _onchange_partner_id(self):
        if self.partner_id and self.partner_id.property_product_pricelist.currency_id:
            self.currency_id = self.partner_id.property_product_pricelist.currency_id

    # ==================================================================
    # Cálculo del costo
    # ==================================================================
    def _producto_del_costo(self):
        self.ensure_one()
        if self.costo_fuente == 'hermano':
            return self.hermano_product_id
        return self.product_id

    def _fx_hoy(self):
        self.ensure_one()
        company = self.company_id
        if not self.currency_id or self.currency_id == company.currency_id:
            return 1.0
        return self.currency_id._convert(1.0, company.currency_id, company,
                                         fields.Date.context_today(self), round=False)

    def action_calcular(self):
        """Toma el costo del producto (o del hermano) en el último período
        cerrado y lo guarda como foto; con fuente manual solo refresca TC,
        escalera y supuestos sobre lo capturado."""
        # El vendedor no tiene permisos sobre el costeo: la lectura va con sudo.
        Periodo = self.env['qb.periodo'].sudo()
        Costo = self.env['qb.costo.unitario'].sudo()
        for rec in self:
            if rec.state not in ('borrador', 'por_aprobar', 'presentada'):
                raise UserError('Solo se recalcula una cotización viva (borrador, por aprobar o '
                                'presentada). Para una vencida usa «Renovar».')
            vals = {'fx_rate': rec._fx_hoy(), 'calculado_el': fields.Datetime.now()}
            if rec.costo_fuente in ('periodo', 'hermano'):
                producto = rec._producto_del_costo()
                if not producto:
                    raise UserError('Elige el producto existente (o el hermano) del que se toma '
                                    'el costo, o cambia la fuente a «Capturado a mano».')
                periodo = Periodo.para_cotizar(rec.company_id)
                if not periodo:
                    raise UserError('No hay un período de costeo cerrado en %s: cierra uno en '
                                    'Manufactura → Costos → Períodos.' % rec.company_id.name)
                costo = Costo.search([('periodo_id', '=', periodo.id),
                                      ('product_id', '=', producto.id)], limit=1)
                if not costo:
                    raise UserError('%s no tiene costo calculado en el período %s. Calcula los '
                                    'costos del período, elige otro producto hermano o captura '
                                    'el costo a mano.' % (producto.display_name, periodo.period))
                vals.update({
                    'periodo_id': periodo.id, 'costo_id': costo.id,
                    'mp_unit': costo.mp_unit, 'energia_unit': costo.energia_unit,
                    'fabricacion_unit': costo.fabricacion_unit,
                    'costo_variable': costo.costo_variable,
                    'costo_produccion': costo.costo_produccion,
                    'rendimiento': costo.rendimiento or 1.0, 'op_pct': costo.op_pct,
                    'calidad': costo.calidad, 'calidad_detalle': costo.calidad_detalle,
                    'precio_mercado': costo.precio_prom,
                })
            elif rec.costo_fuente == 'manual':
                periodo = Periodo.para_cotizar(rec.company_id)
                vals.update({
                    'periodo_id': periodo.id if periodo else False,
                    'costo_variable': rec.mp_unit + rec.energia_unit,
                    'costo_produccion': rec.mp_unit + rec.fabricacion_unit,
                    'op_pct': rec.op_pct or (periodo.op_pct if periodo else 0.0),
                    'calidad': 'baja', 'calidad_detalle': 'capturado a mano',
                })
            else:
                raise UserError('Una cotización importada del cotizador anterior no se recalcula: '
                                'crea una revisión nueva.')
            rec.write(vals)
            rec._calcular_tramos()
            rec.supuestos = rec._texto_supuestos()
            rec.message_post(body='Costo recalculado (%s): piso lleno $%.4f, piso ocioso $%.4f, '
                                  'calidad %s, TC %.4f.' % (
                                      dict(COSTO_FUENTES).get(rec.costo_fuente), rec.piso_lleno,
                                      rec.piso_ocioso, rec.calidad, rec.fx_rate))
        return True

    def _texto_supuestos(self):
        self.ensure_one()
        partes = []
        if self.periodo_id:
            partes.append('Período de costo %s (%s).' % (self.periodo_id.period,
                                                        self.periodo_id.state))
        partes.append('Fuente: %s.' % dict(COSTO_FUENTES).get(self.costo_fuente, ''))
        if self.costo_fuente == 'hermano' and self.hermano_product_id:
            partes.append('Hermano: %s.' % self.hermano_product_id.display_name)
        partes.append('Rendimiento vendible %.1f %%; operación %.2f %% de la venta.'
                      % (100 * (self.rendimiento or 1.0), 100 * (self.op_pct or 0.0)))
        if self.es_divisa:
            partes.append('TC %.4f MXN por %s.' % (self.fx_rate, self.currency_id.name))
        if self.calidad_detalle:
            partes.append('Calidad: %s.' % self.calidad_detalle)
        return ' '.join(partes)

    def _calcular_tramos(self):
        """Escalera de volumen (½×, 1×, 2×, 4×): descuento fijo por cada
        duplicación, nunca debajo del piso lleno y con contribución mensual
        que no baja al crecer el tramo. Sin parámetro, sin escalera."""
        desc = self._param_float(PARAM_ESCALERA_PCT) / 100.0
        Tramo = self.env['qb.cotizador.tramo']
        for rec in self:
            rec.tramo_ids.unlink()
            if not desc or not rec.precio_mxn or not rec.volumen:
                continue
            prev_contrib = None
            for mult in ESCALERA_MULTIPLOS:
                precio = rec.precio_mxn * (1 - desc) ** math.log2(mult)
                precio = max(precio, rec.piso_lleno)
                vol = rec.volumen * mult
                contrib = (precio - rec.piso_ocioso) * vol
                if prev_contrib is not None and contrib < prev_contrib:
                    precio = rec.piso_ocioso + prev_contrib / vol
                    contrib = prev_contrib
                prev_contrib = contrib
                Tramo.create({
                    'cotizacion_id': rec.id, 'multiplo': mult, 'volumen': vol,
                    'es_base': mult == 1.0, 'precio_mxn': precio,
                    'precio_divisa': precio / (rec.fx_rate or 1.0),
                    'margen_neto_pct': (100.0 * (precio - rec.costo_vendible) / precio
                                        - 100.0 * (rec.op_pct or 0.0)) if precio else 0.0,
                    'contrib_total_mes': contrib,
                    'semaforo': self._semaforo_for(precio, rec.piso_ocioso, rec.piso_lleno),
                })

    # ==================================================================
    # Ciclo: aprobación
    # ==================================================================
    def _validar_para_aprobacion(self):
        self.ensure_one()
        faltan = []
        if not self.partner_id:
            faltan.append('el cliente')
        if not (self.product_id or self.spec_descripcion):
            faltan.append('el producto o la especificación')
        if not self.volumen:
            faltan.append('el volumen mensual')
        if not self.precio_objetivo:
            faltan.append('el precio al cliente')
        if not self.calculado_el:
            faltan.append('el cálculo del costo (botón «Calcular costo»)')
        if faltan:
            raise UserError('Antes de pedir aprobación falta: %s.' % ', '.join(faltan))

    def action_enviar_aprobacion(self):
        for rec in self:
            if rec.state != 'borrador':
                raise UserError('Solo un borrador se manda a aprobar.')
            rec._validar_para_aprobacion()
            rec.write({'state': 'por_aprobar', 'solicitada_por_id': self.env.uid,
                       'solicitada_el': fields.Datetime.now()})
            aviso = ''
            if rec.bajo_margen_minimo:
                aviso = ' Aviso: margen neto %.1f %% debajo del mínimo %.1f %%.' % (
                    rec.margen_neto_pct, rec.margen_minimo_pct)
            if rec.semaforo == 'rojo':
                aviso += ' Precio debajo del costo variable: solo la autoriza «Autoriza precio bajo piso».'
            rec.message_post(body='Enviada a aprobación.%s' % aviso)
            job = rec._job_from_param(PARAM_APROBADOR)
            users = rec._users_of_job(job) | rec._users_of_job(rec._job_from_param(PARAM_SUPLENTE))
            for user in users:
                rec.activity_schedule(
                    'mail.mail_activity_data_todo', user_id=user.id,
                    summary='Aprobar cotización %s' % rec.folio,
                    note='Piso lleno $%.4f MXN, precio $%.4f MXN (%s). Costo de la muestra $%.2f.'
                         % (rec.piso_lleno, rec.precio_mxn,
                            dict(SEMAFOROS).get(rec.semaforo, 'sin precio'), rec.costo_muestra))
        return True

    def _quien_aprueba_texto(self):
        job = self._job_from_param(PARAM_APROBADOR)
        sup = self._job_from_param(PARAM_SUPLENTE)
        partes = [job.name if job else 'el puesto configurado en Ajustes → Ventas → Cotizador']
        if sup:
            partes.append('su suplente (%s)' % sup.name)
        return ' o '.join(partes)

    def action_aprobar(self):
        for rec in self:
            if rec.state != 'por_aprobar':
                raise UserError('Solo una cotización «Por aprobar» se aprueba.')
            if not rec._puede_aprobar(self.env.user):
                if rec.semaforo == 'rojo':
                    raise UserError('El precio está debajo del costo variable: solo el grupo '
                                    '«Autoriza precio bajo piso» la puede aprobar.')
                raise UserError('Esta cotización la aprueba %s; tu usuario no tiene ese puesto.'
                                % rec._quien_aprueba_texto())
            rec.write({'approved_by_id': self.env.uid, 'approved_date': fields.Datetime.now()})
            rec.activity_unlink(['mail.mail_activity_data_todo'])
            rec._presentar()
        return True

    def action_regresar(self):
        self.ensure_one()
        if self.state != 'por_aprobar':
            raise UserError('Solo una cotización «Por aprobar» se regresa.')
        if not self._puede_aprobar(self.env.user) and self.semaforo != 'rojo':
            raise UserError('Esta cotización la regresa %s.' % self._quien_aprueba_texto())
        return {
            'type': 'ir.actions.act_window', 'name': 'Regresar a recotizar',
            'res_model': 'qb.cotizador.decision.wizard', 'view_mode': 'form', 'target': 'new',
            'context': {'default_cotizacion_id': self.id, 'default_tipo': 'regreso'},
        }

    def _regresar(self, motivo, nota=None):
        for rec in self:
            rec.write({'state': 'borrador', 'regreso_motivo_id': motivo.id,
                       'regreso_count': rec.regreso_count + 1})
            rec.activity_unlink(['mail.mail_activity_data_todo'])
            rec.message_post(body='Regresada a recotizar. Motivo: %s.%s'
                             % (motivo.name, (' ' + nota) if nota else ''))
            destino = rec.solicitada_por_id or rec.user_id
            if destino:
                rec.activity_schedule('mail.mail_activity_data_todo', user_id=destino.id,
                                      summary='Recotizar %s: %s' % (rec.folio, motivo.name),
                                      note=nota or '')
        return True

    # ==================================================================
    # Ciclo: presentada, seguimiento, vencimiento
    # ==================================================================
    def _fecha_habil(self, dias, desde=None):
        """`dias` hábiles después de `desde` según el calendario de la
        compañía; sin calendario, días naturales."""
        self.ensure_one()
        desde = desde or fields.Datetime.now()
        cal = self.company_id.resource_calendar_id
        if cal and dias:
            try:
                return cal.plan_days(dias, desde, compute_leaves=True).date()
            except Exception:  # noqa: BLE001 - calendario sin horarios: días naturales
                pass
        return (desde + timedelta(days=dias)).date()

    def _presentar(self):
        hoy = fields.Date.context_today(self)
        for rec in self:
            validez = self._param_int(PARAM_VALIDEZ_DIAS, 15)
            # 1.1.0: sin días de seguimiento en el parámetro no hay seguimiento automático.
            dias_seg = self._param_int(PARAM_SEGUIMIENTO_DIAS, 0)
            vals = {'state': 'presentada', 'presented_by_id': self.env.uid,
                    'presented_date': fields.Datetime.now(), 'seguimiento_hecho': dias_seg <= 0,
                    'seguimiento_fecha': rec._fecha_habil(dias_seg) if dias_seg > 0 else False}
            if not rec.validez_hasta or rec.validez_hasta < hoy:
                vals['validez_hasta'] = hoy + timedelta(days=validez)
            rec.write(vals)
            rec.message_post(body='Aprobada por %s y presentada. Válida hasta %s%s.' % (
                self.env.user.name, vals.get('validez_hasta') or rec.validez_hasta,
                ('; seguimiento el %s' % vals['seguimiento_fecha']) if vals['seguimiento_fecha']
                else '; sin seguimiento automático (días no definidos)'))
        return True

    def _vencer(self):
        for rec in self:
            rec.write({'state': 'vencida'})
            rec.message_post(body='Venció el %s sin pedido: decidir renovar, ganada o perdida.'
                             % rec.validez_hasta)
            for user in rec._usuarios_ventas():
                rec.activity_schedule('mail.mail_activity_data_todo', user_id=user.id,
                                      summary='Cotización %s vencida: renovar, ganada o perdida'
                                              % rec.folio)
        return True

    def _seguimiento(self):
        for rec in self:
            rec.write({'seguimiento_hecho': True})
            for user in rec._usuarios_ventas():
                rec.activity_schedule('mail.mail_activity_data_todo', user_id=user.id,
                                      summary='Seguimiento a %s (%s)' % (rec.folio,
                                                                        rec.partner_id.name),
                                      note='Presentada el %s sin respuesta del cliente.'
                                           % (rec.presented_date and rec.presented_date.date()))
        return True

    @api.model
    def _cron_diario(self):
        hoy = fields.Date.context_today(self)
        self.search([('state', '=', 'presentada'), ('validez_hasta', '<', hoy)])._vencer()
        self.search([('state', '=', 'presentada'), ('seguimiento_hecho', '=', False),
                     ('seguimiento_fecha', '<=', hoy)])._seguimiento()
        dias = self._param_int(PARAM_ARCHIVAR_DIAS, 0)  # 1.1.0: vacío = no se archivan borradores
        if dias > 0:
            limite = fields.Datetime.now() - timedelta(days=dias)
            viejas = self.search([('state', '=', 'borrador'), ('write_date', '<', limite)])
            for rec in viejas:
                rec.message_post(body='Archivada: borrador sin movimiento %d días.' % dias)
            viejas.write({'active': False})
        return True

    # ==================================================================
    # Ciclo: ganada, perdida, renovar
    # ==================================================================
    def action_ganar(self):
        for rec in self:
            if rec.state not in ('presentada', 'vencida'):
                raise UserError('Solo una cotización presentada o vencida se marca ganada.')
            rec._marcar_ganada()
        return True

    def _marcar_ganada(self, sale_order=None, automatico=False):
        for rec in self:
            vals = {'state': 'ganada', 'ganada_date': fields.Datetime.now()}
            if sale_order:
                vals['sale_order_id'] = sale_order.id
            rec.write(vals)
            rec.activity_unlink(['mail.mail_activity_data_todo'])
            rec.message_post(body='Ganada%s%s.' % (
                ' automáticamente' if automatico else '',
                (' con el pedido %s' % sale_order.name) if sale_order else ''))
            rec._sincronizar_tarifa()
        return True

    def action_perder(self):
        self.ensure_one()
        if self.state not in ('presentada', 'vencida', 'por_aprobar'):
            raise UserError('Solo una cotización presentada, vencida o por aprobar se da por perdida.')
        return {
            'type': 'ir.actions.act_window', 'name': 'Dar por perdida',
            'res_model': 'qb.cotizador.decision.wizard', 'view_mode': 'form', 'target': 'new',
            'context': {'default_cotizacion_id': self.id, 'default_tipo': 'perdida'},
        }

    def _marcar_perdida(self, motivo, nota=None):
        for rec in self:
            rec.write({'state': 'perdida', 'perdida_motivo_id': motivo.id, 'perdida_nota': nota})
            rec.activity_unlink(['mail.mail_activity_data_todo'])
            rec.message_post(body='Perdida. Motivo: %s.%s' % (motivo.name,
                                                               (' ' + nota) if nota else ''))
        return True

    def action_renovar(self):
        """Revisión nueva en borrador a partir de esta (vencida o presentada);
        esta pasa a «Reemplazada»."""
        self.ensure_one()
        if self.state not in ('vencida', 'presentada'):
            raise UserError('Solo una cotización presentada o vencida se renueva.')
        nueva = self._nueva_revision()
        return {'type': 'ir.actions.act_window', 'res_model': self._name, 'res_id': nueva.id,
                'view_mode': 'form', 'target': 'current'}

    def _nueva_revision(self):
        self.ensure_one()
        nueva = self.copy({'revision': self.revision + 1, 'revision_anterior_id': self.id,
                           'state': 'borrador', 'validez_hasta': False})
        self.write({'state': 'reemplazada'})
        self.activity_unlink(['mail.mail_activity_data_todo'])
        self.message_post(body='Reemplazada por la revisión %d (%s).' % (nueva.revision, nueva.folio))
        nueva.message_post(body='Revisión %d de %s.' % (nueva.revision, self.folio))
        return nueva

    def action_volver_a_borrador(self):
        for rec in self:
            if rec.state != 'por_aprobar' or rec.solicitada_por_id != self.env.user:
                raise UserError('Solo quien la mandó a aprobar puede retirarla antes de la aprobación.')
            rec.write({'state': 'borrador'})
            rec.activity_unlink(['mail.mail_activity_data_todo'])
        return True

    # ==================================================================
    # Aprobación del cliente y tarifa
    # ==================================================================
    def write(self, vals):
        res = super().write(vals)
        if vals.get('cliente_aprobo'):
            for rec in self:
                if not (rec.cliente_medio and rec.cliente_fecha):
                    raise UserError('La aprobación del cliente lleva medio y fecha.')
                if rec.state == 'ganada':
                    rec._sincronizar_tarifa()
        return res

    def _tarifa_del_cliente(self):
        """Tarifa propia del cliente; si usa una compartida, se le crea una."""
        self.ensure_one()
        partner = self.partner_id.commercial_partner_id
        Pricelist = self.env['product.pricelist'].sudo().with_company(self.company_id)
        actual = partner.with_company(self.company_id).property_product_pricelist
        # Convención de la base: la tarifa propia de un cliente se llama
        # «TARIFA <cliente>»; cualquier otra (pública, predeterminada) es
        # compartida y no se le mete un precio de un solo cliente.
        propia = bool(actual) and actual.name.upper().startswith('TARIFA ')
        if propia and actual.currency_id == self.currency_id:
            return actual
        tarifa = Pricelist.create({'name': 'TARIFA %s' % partner.name,
                                   'currency_id': self.currency_id.id,
                                   'company_id': self.company_id.id})
        partner.sudo().with_company(self.company_id).write({'property_product_pricelist': tarifa.id})
        self.message_post(body='Tarifa %s creada y asignada al cliente.' % tarifa.name)
        return tarifa

    def _sincronizar_tarifa(self):
        """Ganada + cliente aprobó la muestra ⇒ precio en la tarifa del
        cliente con precio, moneda y vigencia de la cotización."""
        for rec in self:
            if rec.state != 'ganada' or not rec.cliente_aprobo or rec.pricelist_item_id:
                continue
            if not rec.product_id:
                rec.message_post(body='Sin artículo ligado: el precio no se puede poner en la '
                                      'tarifa. Liga el artículo y vuelve a marcar la aprobación.')
                continue
            tarifa = rec._tarifa_del_cliente()
            item = self.env['product.pricelist.item'].sudo().create({
                'pricelist_id': tarifa.id, 'applied_on': '1_product',
                'product_tmpl_id': rec.product_id.product_tmpl_id.id,
                'compute_price': 'fixed', 'fixed_price': rec.precio_objetivo,
                'min_quantity': 0,
                'date_start': rec.ganada_date or fields.Datetime.now(),
                'date_end': fields.Datetime.to_datetime(rec.validez_hasta) if rec.validez_hasta
                and rec.validez_hasta > fields.Date.context_today(rec) else False,
            })
            rec.write({'pricelist_item_id': item.id})
            rec.message_post(body='Precio %s %s puesto en la tarifa %s (vigencia %s).' % (
                rec.precio_objetivo, rec.currency_id.name, tarifa.name,
                rec.validez_hasta or 'sin fin'))
        return True

    # ==================================================================
    # PDF
    # ==================================================================
    def action_print_cliente(self):
        for rec in self:
            if rec.state not in APROBADAS:
                raise UserError('El PDF comercial solo se imprime con la cotización aprobada '
                                '(%s).' % rec._quien_aprueba_texto())
        return self.env.ref('qb_cotizador.action_report_cotizacion_cliente').report_action(self)

    def action_print_interna(self):
        return self.env.ref('qb_cotizador.action_report_cotizacion_interna').report_action(self)

    def action_ver_revisiones(self):
        self.ensure_one()
        raiz = self
        while raiz.revision_anterior_id:
            raiz = raiz.revision_anterior_id
        ids = [raiz.id]
        pend = raiz
        while pend:
            sig = pend.revision_siguiente_ids
            ids += sig.ids
            pend = sig
        return {'type': 'ir.actions.act_window', 'name': 'Revisiones de %s' % raiz.folio,
                'res_model': self._name, 'view_mode': 'list,form',
                'domain': [('id', 'in', ids), ('active', 'in', (True, False))]}

    # ==================================================================
    # Importación del cotizador anterior (solo lectura de qb.cotizacion)
    # ==================================================================
    @api.model
    @api.model
    def _vals_desde_legado(self, d, hoy=None):
        """Valores de una cotización nueva a partir de un diccionario con la
        forma de `qb.cotizacion` (campos del cotizador anterior; many2one como
        id). Lo usan la importación y la calculadora viva (1.1.0, Jose 3):
        la misma conversión en los dos caminos. `tramo_ids` viene como lista
        de diccionarios de `qb.cotizacion.tramo`."""
        hoy = hoy or fields.Date.context_today(self)
        fx = d.get('fx_rate') if d.get('fx_rate') and d.get('fx_rate') != 1.0 else 1.0
        rendimiento = d.get('rendimiento') or 1.0
        state = LEGACY_STATE.get(d.get('state') or 'draft', 'borrador')
        validez = d.get('validez_hasta')
        if state == 'presentada' and validez and validez < hoy:
            state = 'vencida'
        company_id = d.get('company_id') or self.env.company.id
        currency_id = d.get('currency_id') or self.env['res.company'].browse(company_id).currency_id.id
        energia = d.get('energia_unit') or 0.0
        vals = {
            'company_id': company_id, 'partner_id': d.get('partner_id'), 'atencion_a': d.get('atencion_a'),
            'product_id': d.get('product_id'), 'spec_descripcion': d.get('spec_descripcion'),
            'spec_gramaje': d.get('spec_gramaje'), 'spec_ancho': d.get('spec_ancho'),
            'spec_galga': d.get('spec_galga'), 'volumen': d.get('volumen'),
            'volumen_uom': 'kg' if (d.get('uom_name') or '').lower().startswith('k') else 'm',
            'currency_id': currency_id, 'fx_rate': fx, 'costo_fuente': 'legado',
            # En v2 «fabricación» = horas × tarifa (fija + variable), con la
            # energía adentro; la conversión absorbida del viejo entra ahí.
            'mp_unit': d.get('mp_unit') or 0.0, 'energia_unit': energia + (d.get('conv_var_unit') or 0.0),
            'fabricacion_unit': energia + (d.get('fab_unit') or 0.0) + (d.get('conv_unit') or 0.0),
            # El viejo guardaba variable y producción ya por unidad vendible.
            'costo_variable': (d.get('costo_variable') or 0.0) * rendimiento,
            'costo_produccion': (d.get('costo_absorbido_sin_op') or 0.0) * rendimiento,
            'rendimiento': rendimiento, 'op_pct': (d.get('op_pct') or 0.0) / 100.0,
            'calidad': 'ninguna', 'precio_mercado': d.get('precio_mercado') or 0.0,
            # El viejo guardaba el precio objetivo en MXN; aquí va en la moneda de la cotización.
            'precio_objetivo': ((d.get('precio_objetivo') or 0.0) / fx) if d.get('precio_objetivo') else 0.0,
            'con_escalera': d.get('con_escalera') or False, 'state': state,
            'validez_hasta': validez, 'supuestos': d.get('supuestos'),
            'revision': d.get('revision') or 1, 'sale_order_id': d.get('sale_order_id'),
            'lote_minimo': d.get('lote_minimo'), 'presentacion_rollos': d.get('presentacion_rollos'),
            'lugar_entrega': d.get('lugar_entrega'), 'tiempo_entrega': d.get('tiempo_entrega'),
            'muestra_leyenda': d.get('muestra_leyenda'),
            'tramo_ids': [(0, 0, {
                'multiplo': t.get('multiplo'), 'volumen': t.get('volumen'), 'es_base': t.get('es_base'),
                'precio_mxn': t.get('precio_mxn'), 'precio_divisa': t.get('precio_divisa'),
                'margen_neto_pct': t.get('margen_neto_pct'),
                'contrib_total_mes': t.get('contrib_total_mes'), 'semaforo': t.get('semaforo'),
            }) for t in (d.get('tramo_ids') or [])],
        }
        for tc in ('tc_coa', 'tc_ppap', 'tc_inspeccion_total', 'tc_cpk', 'tc_pscr',
                   'tc_pruebas_lab', 'tc_apqp', 'tc_ctpat', 'tc_lta'):
            if tc in d:
                vals[tc] = d[tc]
        return vals

    @api.model
    def crear_desde_calculadora(self, d):
        """1.1.0 (Jose 2026-10-08, punto 3): la calculadora viva de capacidad y
        costo (`qb.cotizador.wizard`) guarda aquí, no en `qb.cotizacion`, para
        que toda cotización pase por la aprobación del puesto. Entra como
        borrador con el costo que calculó la calculadora (fuente «cotizador
        anterior») y su folio propio."""
        vals = self._vals_desde_legado(dict(d, state='draft', validez_hasta=False))
        vals.update({'state': 'borrador', 'user_id': self.env.uid, 'calculado_el': fields.Datetime.now(),
                     'calidad_detalle': 'calculado en la calculadora de capacidad y costo (%s)'
                                        % (d.get('name') or '')})
        return self.create(vals)

    def importar_legadas(self):
        """Copia las cotizaciones de `qb.cotizacion` que aún no están aquí
        (por `legacy_id`), con su estado, revisión, foto de costo y escalera.
        Las presentadas con validez vencida entran como «Vencida» para que
        Ventas decida. Nunca escribe en el modelo viejo."""
        if 'qb.cotizacion' not in self.env:
            return 0
        Old = self.env['qb.cotizacion'].sudo()
        hoy = fields.Date.context_today(self)
        hechas = {r.legacy_id: r for r in self.sudo().with_context(active_test=False).search(
            [('legacy_id', '!=', 0)])}
        n = 0
        simple = ('atencion_a', 'spec_descripcion', 'spec_gramaje', 'spec_ancho', 'spec_galga', 'volumen',
                  'uom_name', 'fx_rate', 'mp_unit', 'energia_unit', 'conv_var_unit', 'fab_unit', 'conv_unit',
                  'costo_variable', 'costo_absorbido_sin_op', 'rendimiento', 'op_pct', 'precio_mercado',
                  'precio_objetivo', 'con_escalera', 'state', 'validez_hasta', 'supuestos', 'revision',
                  'lote_minimo', 'presentacion_rollos', 'lugar_entrega', 'tiempo_entrega', 'muestra_leyenda',
                  'tc_coa', 'tc_ppap', 'tc_inspeccion_total', 'tc_cpk', 'tc_pscr', 'tc_pruebas_lab',
                  'tc_apqp', 'tc_ctpat', 'tc_lta')
        for old in Old.search([], order='id asc'):
            if old.id in hechas:
                continue
            anterior = hechas.get(old.revision_anterior_id.id) if old.revision_anterior_id else None
            d = {k: old[k] for k in simple}
            d.update({'company_id': old.company_id.id, 'partner_id': old.partner_id.id,
                      'product_id': old.product_id.id, 'currency_id': old.currency_id.id,
                      'sale_order_id': old.sale_order_id.id,
                      'tramo_ids': [{k: t[k] for k in ('multiplo', 'volumen', 'es_base', 'precio_mxn',
                                                       'precio_divisa', 'margen_neto_pct',
                                                       'contrib_total_mes', 'semaforo')}
                                    for t in old.tramo_ids]})
            vals = self._vals_desde_legado(d, hoy)
            state = vals['state']
            vals.update({
                'legacy_id': old.id, 'folio': old.folio, 'user_id': old.create_uid.id,
                'calculado_el': old.create_date,
                'calidad_detalle': 'importada de qb.cotizacion #%d (%s)' % (old.id, old.folio),
                'revision_anterior_id': anterior.id if anterior else False,
            })
            if state in ('presentada', 'vencida', 'ganada', 'perdida'):
                vals.update({'presented_date': old.create_date, 'presented_by_id': old.create_uid.id,
                             'approved_date': old.create_date, 'approved_by_id': old.create_uid.id,
                             'seguimiento_hecho': True})
            if state == 'ganada':
                vals['ganada_date'] = old.write_date
            nueva = self.sudo().with_context(mail_create_nolog=True, tracking_disable=True,
                                             mail_notrack=True).create(vals)
            self.env.cr.execute('UPDATE qb_cotizador_cotizacion SET create_date=%s, write_date=%s '
                                'WHERE id=%s', (old.create_date, old.write_date, nueva.id))
            nueva.invalidate_recordset(['create_date', 'write_date'])
            nueva.message_post(body='Importada del cotizador anterior (folio %s, estado «%s»).'
                               % (old.folio, dict(Old._fields['state'].selection).get(old.state)))
            hechas[old.id] = nueva
            n += 1
        if n:
            _logger.info('qb_cotizador: %d cotizaciones importadas del cotizador anterior', n)
        return n


class QbCotizadorTramo(models.Model):
    _name = 'qb.cotizador.tramo'
    _description = 'Tramo de la escalera de volumen'
    _order = 'volumen'

    cotizacion_id = fields.Many2one('qb.cotizador.cotizacion', required=True,
                                    ondelete='cascade', index=True)
    multiplo = fields.Float(string='× volumen')
    volumen = fields.Float(string='Volumen/mes', digits=(16, 0))
    es_base = fields.Boolean(string='Cotizado')
    precio_mxn = fields.Float(string='Precio $/u MXN', digits=(16, 4))
    precio_divisa = fields.Float(string='Precio (divisa)', digits=(16, 4))
    margen_neto_pct = fields.Float(string='Margen neto %', digits=(16, 1))
    contrib_total_mes = fields.Float(string='Contribución $/mes', digits=(16, 0))
    semaforo = fields.Selection(SEMAFOROS)
