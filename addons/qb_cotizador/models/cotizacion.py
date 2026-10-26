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
from types import SimpleNamespace

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
FX_FUENTES = [('odoo', 'Tipo de cambio de Odoo'), ('manual', 'Capturado a mano')]
COSTO_FUENTES = [
    ('periodo', 'Costo del producto en el período cerrado'),
    ('hermano', 'Costo de un producto hermano'),
    # 1.7.0: el artículo nuevo de un desarrollo, costeado con su lista de materiales (ver _costo_receta).
    ('receta', 'Receta del desarrollo'),
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
    ('direccion', 'Dirección (desarrollo interno)'),  # 1.2.0: mismo catálogo que la aprobación del SGI
]
VOLUMEN_UOMS = [('m', 'm / mes'), ('kg', 'kg / mes')]
# 1.6.0: unidad en que está expresada la foto del costo (la del volumen al calcular).
COSTO_UOMS = [('m', 'por m'), ('kg', 'por kg')]
# 1.6.1: unidades del costo de origen que se saben convertir; cualquier otra (pieza, servicio…) no se cotiza.
COSTO_UOMS_ORIGEN = COSTO_UOMS + [('m2', 'por m²')]
UOM_ALIAS = {'m': 'm', 'mt': 'm', 'mts': 'm', 'metro': 'm', 'metros': 'm',
             'kg': 'kg', 'kgs': 'kg', 'kilo': 'kg', 'kilos': 'kg', 'kilogramo': 'kg', 'kilogramos': 'kg',
             'm2': 'm2', 'm²': 'm2', 'mt2': 'm2', 'metro cuadrado': 'm2', 'metros cuadrados': 'm2'}
KG_FUENTES = [('costo', 'Peso del costo (kg por unidad del producto)'),
              ('especificacion', 'Gramaje × ancho de la cotización')]
# 1.6.1: diferencia máxima entre el peso del costo y el que da el código del artículo antes de avisar.
PESO_TOLERANCIA = 0.10
# 1.6.1: rango creíble del peso de una tela (kg por m lineal / kg por m²) cuando el código no se puede leer.
PESO_RANGO = {'m': (0.02, 0.8), 'm2': (0.01, 0.5)}

# 1.6.0: unidades en que se puede imprimir el precio al cliente (además de la de la cotización).
YARDA_M = 0.9144  # 1 yd = 0.9144 m, exacto
PDF_UNIDADES = (('m', 'imprimir_m', 'm'), ('yd', 'imprimir_yd', 'yd'), ('kg', 'imprimir_kg', 'kg'))
# Campos de la foto que van por unidad (se convierten de m a kg o al revés); rendimiento y operación no.
COSTO_CAMPOS_UNIDAD = ('mp_unit', 'energia_unit', 'fabricacion_unit', 'costo_variable', 'costo_produccion',
                       'precio_mercado')
ESCALERA_MULTIPLOS = (0.5, 1.0, 2.0, 4.0)
# 1.7.0: lo que «manual» deja en blanco al elegirlo (antes conservaba los números de la fuente anterior).
CAMPOS_FOTO_MANUAL = ('mp_unit', 'energia_unit', 'fabricacion_unit', 'costo_variable', 'costo_produccion',
                      'rendimiento', 'op_pct', 'precio_mercado')
# 1.7.0: diferencia máxima entre el consumo de tela por metro de la receta nueva y el del hermano ajustado.
CONSUMO_TOLERANCIA = 0.10


def _num(valor):
    return ('%.4f' % valor).rstrip('0').rstrip('.')


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
                           help='Pesos por 1 unidad de la divisa. Se toma solo del tipo de cambio de Odoo al '
                                'elegir la moneda, con «TC de hoy» y al calcular el costo; se puede capturar a '
                                'mano mientras la cotización está en borrador o por aprobar (queda como '
                                '«capturado a mano» y el cálculo del costo ya no lo pisa).')
    fx_fuente = fields.Selection(FX_FUENTES, string='De dónde sale el TC', default='odoo', copy=False)
    fx_fecha = fields.Date(string='Fecha del TC', copy=False,
                           help='Fecha del tipo de cambio de Odoo que se usó, o del día en que se capturó a mano.')
    es_divisa = fields.Boolean(compute='_compute_es_divisa')
    moneda_alerta = fields.Char(compute='_compute_moneda_alerta')
    sin_costo = fields.Boolean(compute='_compute_margenes', store=True,
                               help='Sin piso calculado no hay semáforo ni márgenes.')

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
    # 1.6.0 (Dirección General 2026-10-15): la foto va en la unidad de la cotización. Un costo por m con
    # la cotización en kg (o al revés) se convierte al calcular con los kg por metro; si después cambia
    # la unidad del volumen, la cotización avisa y no se aprueba hasta recalcular.
    costo_uom = fields.Selection(COSTO_UOMS_ORIGEN, string='Unidad del costo', readonly=True, copy=False,
                                 help='Unidad en que están los costos y pisos de la foto: la del volumen al '
                                      'calcular el costo.')
    costo_uom_origen = fields.Selection(COSTO_UOMS_ORIGEN, string='Unidad del costo de origen', readonly=True,
                                        copy=False, help='Unidad del costo del período (la del producto).')
    costo_kg_por_m = fields.Float(string='kg por metro (conversión)', digits=(16, 6), readonly=True,
                                  copy=False, help='Con qué se convirtió el costo de m a kg o al revés; '
                                                   'vacío si no hubo conversión.')
    costo_kg_fuente = fields.Selection(KG_FUENTES, string='De dónde salen los kg por metro', readonly=True,
                                       copy=False)
    unidad_alerta = fields.Char(compute='_compute_unidad_alerta')
    costo_conversion = fields.Char(string='Conversión del costo', readonly=True, copy=False,
                                   help='Cómo se pasó el costo de origen a la unidad de la cotización.')
    # 1.6.1: el peso del costo contra el que da el código del artículo (gramaje × ancho, DAT P-D02-01).
    peso_alerta = fields.Char(compute='_compute_peso_alerta')
    peso_confirmado = fields.Boolean(string='Peso revisado', copy=False, tracking=True,
                                     help='Marcar cuando se revisó que el peso del costo es el bueno aunque no '
                                          'cuadre con el código del artículo (por ejemplo, rollos pesados). '
                                          'Quita el bloqueo del aviso de peso.')
    # 1.7.0: receta del desarrollo. El desglose y el consumo de tela por unidad contra el del hermano ajustado
    # por ancho y gramaje (aviso si se aleja más de CONSUMO_TOLERANCIA).
    receta_detalle = fields.Text(string='Desglose de la receta', readonly=True, copy=False)
    consumo_kg = fields.Float(string='Consumo de tela de la receta (kg/u)', digits=(16, 6), readonly=True,
                              copy=False, help='kg de tela (componentes con código de la nomenclatura) por unidad '
                                               'del artículo nuevo, según su lista de materiales.')
    consumo_hermano_kg = fields.Float(string='Consumo del hermano ajustado (kg/u)', digits=(16, 6), readonly=True,
                                      copy=False, help='El del hermano por su lista de materiales, ajustado por ancho '
                                                       'y gramaje al artículo nuevo.')
    consumo_ajuste = fields.Char(string='Cómo se ajustó el hermano', readonly=True, copy=False)
    consumo_alerta = fields.Char(compute='_compute_consumo_alerta')
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
    # 1.2.0 (Jose 2026-10-08, 5.6): cuándo y quién puso el precio en la tarifa; con esto se mide C1.17.
    tarifa_fecha = fields.Datetime(string='Precio en tarifa el', readonly=True, copy=False)
    tarifa_user_id = fields.Many2one('res.users', string='Puso el precio en tarifa', readonly=True, copy=False)
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
    # 1.6.0 (Dirección General 2026-10-15): en qué unidades sale el precio en el PDF. La cotización se
    # calcula y se aprueba en su unidad; las demás se convierten solo para imprimir (yd = 0.9144 m; m ↔ kg
    # con los kg por metro). Sin ninguna marcada, la de la cotización. El default sale del cliente.
    imprimir_m = fields.Boolean(string='Precio por m', compute='_compute_imprimir', store=True,
                                readonly=False, precompute=True)
    imprimir_yd = fields.Boolean(string='Precio por yd', compute='_compute_imprimir', store=True,
                                 readonly=False, precompute=True)
    imprimir_kg = fields.Boolean(string='Precio por kg', compute='_compute_imprimir', store=True,
                                 readonly=False, precompute=True)
    pdf_kg_por_m = fields.Float(string='kg por metro (para imprimir)', digits=(16, 6),
                                compute='_compute_pdf_kg_por_m',
                                help='Con qué se pasa de m a kg en el PDF: los del costo o el gramaje × ancho.')
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

    @api.depends('costo_uom', 'volumen_uom', 'calculado_el', 'costo_fuente')
    def _compute_unidad_alerta(self):
        for rec in self:
            alerta = ''
            if rec.calculado_el and rec.costo_fuente != 'legado' and rec.costo_uom \
                    and rec.costo_uom != rec.volumen_uom:
                alerta = ('El costo está %s y la cotización %s: los márgenes y el semáforo comparan unidades '
                          'distintas. Recalcula el costo («Calcular costo»).' % (
                              dict(COSTO_UOMS_ORIGEN)[rec.costo_uom], dict(COSTO_UOMS)[rec.volumen_uom]))
            rec.unidad_alerta = alerta

    @api.depends('partner_id', 'volumen_uom')
    def _compute_imprimir(self):
        for rec in self:
            p = rec.partner_id.commercial_partner_id if rec.partner_id else rec.partner_id
            if p and (p.qb_cot_pdf_m or p.qb_cot_pdf_yd or p.qb_cot_pdf_kg):
                rec.imprimir_m, rec.imprimir_yd, rec.imprimir_kg = p.qb_cot_pdf_m, p.qb_cot_pdf_yd, p.qb_cot_pdf_kg
            else:
                uom = rec.volumen_uom or 'm'
                rec.imprimir_m, rec.imprimir_yd, rec.imprimir_kg = uom == 'm', False, uom == 'kg'

    @api.depends('costo_kg_por_m', 'costo_uom_origen', 'costo_id.kg_por_unidad', 'spec_gramaje', 'spec_ancho')
    def _compute_pdf_kg_por_m(self):
        for rec in self:
            kg_m = rec.costo_kg_por_m
            if not kg_m and rec.costo_uom_origen == 'm' and rec.costo_id:
                kg_m = rec.costo_id.sudo().kg_por_unidad
            if not kg_m and rec.spec_gramaje > 0 and rec.spec_ancho > 0:
                kg_m = rec.spec_gramaje * rec.spec_ancho / 1000.0
            rec.pdf_kg_por_m = kg_m or 0.0

    def _pdf_unidades(self):
        """Unidades en que se imprime el precio, en orden m, yd, kg; la de la cotización si no hay marcadas."""
        self.ensure_one()
        unidades = [u for u, campo, _l in PDF_UNIDADES if self[campo]]
        return unidades or [self.volumen_uom or 'm']

    def _pdf_factor(self, unidad):
        """Cuántas unidades de la cotización caben en una ``unidad`` (m, yd o kg): precio por ``unidad`` =
        precio × factor; volumen en ``unidad`` = volumen ÷ factor. None si no se puede convertir."""
        self.ensure_one()
        base = self.volumen_uom or 'm'
        if unidad == base:
            return 1.0
        kg_m = self.pdf_kg_por_m
        # Primero se pasa a metros: 1 m son kg_m kg; 1 yd son 0.9144 m.
        m_por_unidad = {'m': 1.0, 'yd': YARDA_M, 'kg': (1.0 / kg_m) if kg_m else None}[unidad]
        base_por_m = 1.0 if base == 'm' else kg_m  # unidades de la cotización en 1 m
        if m_por_unidad is None or not base_por_m:
            return None
        return m_por_unidad * base_por_m

    def _pdf_precios(self, precio):
        """[(unidad, precio)] para el PDF a partir de un precio por unidad de la cotización."""
        self.ensure_one()
        return [(u, precio * self._pdf_factor(u)) for u in self._pdf_unidades() if self._pdf_factor(u)]

    def _pdf_volumenes(self, volumen):
        """[(unidad, volumen)] en las unidades marcadas que no son la de la cotización."""
        self.ensure_one()
        base = self.volumen_uom or 'm'
        return [(u, volumen / self._pdf_factor(u)) for u in self._pdf_unidades()
                if u != base and self._pdf_factor(u)]

    def _pdf_validar_unidades(self):
        for rec in self:
            faltan = [u for u in rec._pdf_unidades() if not rec._pdf_factor(u)]
            if faltan:
                raise UserError('%s: para imprimir el precio por %s hacen falta los kg por metro (el costo no los '
                                'trae y la cotización no tiene gramaje y ancho). Captura gramaje y ancho o '
                                'desmarca esa unidad.' % (rec.folio or rec.display_name, ', '.join(faltan)))

    @api.model
    def _unidad_de(self, nombre):
        """1.6.1: 'm', 'kg' o 'm2' a partir del nombre de la unidad de medida; None si es otra (pieza…)."""
        return UOM_ALIAS.get(' '.join((nombre or '').strip().lower().split()))

    @api.model
    def _codigo_partes(self, ref):
        """1.6.1: (gramaje g/m², ancho m) del código del artículo con la regla del DAT P-D02-01 (la misma del
        costeo, ``qb_costeo.models.peso.NOMEN_RE``); 0 en lo que no se lee o no es creíble."""
        from odoo.addons.qb_costeo.models.peso import NOMEN_RE
        m = NOMEN_RE.match((ref or '').replace(' ', ''))
        if not m:
            return 0.0, 0.0
        ancho = int(m.group(8)) / 100.0
        return float(int(m.group(3))), (ancho if 0.3 <= ancho <= 3.5 else 0.0)

    @api.model
    def _peso_de_codigo(self, ref, por_m2=False):
        """1.6.1: kg por metro (o por m² con ``por_m2``) que da el código del artículo; 0 si no se lee."""
        gramaje, ancho = self._codigo_partes(ref)
        if por_m2:
            return gramaje / 1000.0
        return gramaje * ancho / 1000.0

    def _costo_conversion(self, costo):
        """(factor, unidad de origen, kg por metro, fuente de los kg, texto) para pasar la foto del costo a
        la unidad del volumen (m o kg). El factor multiplica los campos por unidad (``COSTO_CAMPOS_UNIDAD``).

        1.6.0: m ↔ kg con los kg por metro del costo (si el costo va por m) o el gramaje × ancho de la
        cotización. 1.6.1: también desde m² (por m = por m² × ancho; por kg = por m² ÷ kg por m²); una unidad
        que no es m, kg ni m² no se convierte. Sin el dato que hace falta, no se calcula y lo dice."""
        self.ensure_one()
        nombre = costo.uom_name or costo.product_id.uom_id.name or ''
        origen = self._unidad_de(nombre)
        destino = self.volumen_uom or 'm'
        producto = costo.product_id.display_name
        if not origen:
            raise UserError('El costo de %s está por «%s»: el cotizador solo convierte costos por m, kg o m². '
                            'Elige otro producto (o hermano) o captura el costo a mano.' % (producto, nombre))
        peso_costo = costo.kg_por_unidad if costo.kg_por_unidad > 0 else 0.0
        spec_kg_m = self.spec_gramaje * self.spec_ancho / 1000.0 if self.spec_gramaje > 0 and self.spec_ancho > 0 \
            else 0.0
        ancho = self.spec_ancho if self.spec_ancho > 0 else 0.0
        if not ancho and origen == 'm2':
            ancho = self._codigo_partes(costo.product_id.default_code)[1]
        falta = None
        if origen == 'm':
            kg_m, fuente = (peso_costo, 'costo') if peso_costo else (spec_kg_m, 'especificacion' if spec_kg_m else False)
            if destino == 'm':
                return 1.0, origen, kg_m, fuente, ''
            if not kg_m:
                falta = 'los kg por metro (el costo no los trae y la cotización no tiene gramaje y ancho)'
            else:
                return 1.0 / kg_m, origen, kg_m, fuente, '÷ %s kg/m' % _num(kg_m)
        elif origen == 'kg':
            kg_m, fuente = spec_kg_m, ('especificacion' if spec_kg_m else False)
            if destino == 'kg':
                return 1.0, origen, kg_m, fuente, ''
            if not kg_m:
                falta = 'el gramaje y el ancho de la cotización (para saber cuántos kg pesa un metro)'
            else:
                return kg_m, origen, kg_m, fuente, '× %s kg/m' % _num(kg_m)
        else:  # m2
            kg_m2, fuente = (peso_costo, 'costo') if peso_costo else (
                (self.spec_gramaje / 1000.0, 'especificacion') if self.spec_gramaje > 0 else (0.0, False))
            kg_m = kg_m2 * ancho if kg_m2 and ancho else 0.0
            if destino == 'm':
                if not ancho:
                    falta = 'el ancho (para saber cuántos m² tiene un metro)'
                else:
                    return ancho, origen, kg_m, fuente, '× %s m de ancho' % _num(ancho)
            elif not kg_m2:
                falta = 'los kg por m² (el costo no los trae y la cotización no tiene gramaje)'
            else:
                return 1.0 / kg_m2, origen, kg_m, fuente, '÷ %s kg/m²' % _num(kg_m2)
        raise UserError('El costo de %s está %s y la cotización por %s; para convertirlo falta %s. Captúralo '
                        'o cambia la unidad del volumen.' % (producto, dict(COSTO_UOMS_ORIGEN)[origen], destino,
                                                             falta))

    @api.depends('costo_id', 'costo_kg_fuente', 'costo_uom_origen', 'peso_confirmado')
    def _compute_peso_alerta(self):
        for rec in self:
            alerta = ''
            costo = rec.costo_id.sudo()
            if costo and rec.costo_kg_fuente == 'costo' and rec.costo_uom_origen in ('m', 'm2') \
                    and rec.costo_uom_origen != rec.costo_uom and costo.kg_por_unidad > 0:
                por_m2 = rec.costo_uom_origen == 'm2'
                esperado = rec._peso_de_codigo(costo.product_id.default_code, por_m2=por_m2)
                u = 'kg/m²' if por_m2 else 'kg/m'
                bajo, alto = PESO_RANGO[rec.costo_uom_origen]
                w = costo.kg_por_unidad
                if esperado and abs(w / esperado - 1.0) > PESO_TOLERANCIA:
                    motivo = 'no cuadra con su código (%s %s)' % (_num(esperado), u)
                elif not esperado and not bajo <= w <= alto:
                    motivo = 'está fuera de lo creíble para una tela (%s a %s %s) y su código no se puede leer' % (
                        _num(bajo), _num(alto), u)
                else:
                    motivo = ''
                if motivo:
                    alerta = ('El peso del costo de %s (%s %s, %s) %s: la conversión a %s puede estar mal. Corrige '
                              'el peso en Costos o, si ya se revisó, marca «Peso revisado».' % (
                                  costo.product_id.display_name, _num(w), u, costo.peso_fuente or 'sin fuente',
                                  motivo, rec.costo_uom))
            rec.peso_alerta = alerta

    @api.depends('consumo_kg', 'consumo_hermano_kg', 'consumo_ajuste', 'costo_fuente', 'hermano_product_id')
    def _compute_consumo_alerta(self):
        for rec in self:
            alerta = ''
            if rec.costo_fuente == 'receta' and rec.consumo_kg > 0 and rec.consumo_hermano_kg > 0:
                dif = rec.consumo_kg / rec.consumo_hermano_kg - 1.0
                if abs(dif) > CONSUMO_TOLERANCIA:
                    u = rec.product_id.uom_id.name or 'u'
                    alerta = ('El consumo de tela de la receta nueva (%s kg/%s) se aleja %+.1f %% del hermano %s '
                              'ajustado por ancho y gramaje (%s kg/%s; %s). Revise la lista de materiales del '
                              'artículo nuevo antes de aprobar.' % (
                                  _num(rec.consumo_kg), u, 100 * dif, rec.hermano_product_id.default_code or '',
                                  _num(rec.consumo_hermano_kg), u, rec.consumo_ajuste or 'sin ajuste'))
            rec.consumo_alerta = alerta

    @api.depends('currency_id', 'company_id', 'fx_rate', 'costo_fuente')
    def _compute_moneda_alerta(self):
        for rec in self:
            alerta = ''
            if rec.es_divisa and (rec.fx_rate or 0.0) <= 1.0:
                alerta = ('La cotización es en %s pero el tipo de cambio es %.4f: el precio se está leyendo '
                          'como si fuera MXN. Captura el TC o toma el de hoy.' % (rec.currency_id.name, rec.fx_rate or 0.0))
            elif not rec.es_divisa and rec.fx_rate and rec.fx_rate != 1.0 and rec.costo_fuente != 'legado':
                alerta = 'La cotización es en %s: el tipo de cambio debe ser 1.' % rec.currency_id.name
            rec.moneda_alerta = alerta

    @api.depends('precio_objetivo', 'fx_rate', 'piso_ocioso', 'piso_lleno', 'costo_vendible',
                 'op_pct')
    def _compute_margenes(self):
        for rec in self:
            fx = rec.fx_rate or 1.0
            precio = (rec.precio_objetivo or 0.0) * fx
            rec.precio_mxn = precio
            # 1.3.0: sin piso calculado no hay nada contra qué medir el precio. Antes el semáforo
            # salía verde con piso 0 («cubre todo») y el margen 100 %.
            rec.sin_costo = not rec.piso_lleno
            if not precio or rec.sin_costo:
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

    def _pidio_la_aprobacion(self, user):
        """1.5.0 (Dirección General 2026-10-09): nadie aprueba lo que él mismo
        pidió. Quien mandó la cotización a aprobar no la aprueba, tenga el
        puesto titular, el suplente o el grupo bajo piso."""
        self.ensure_one()
        return bool(self.solicitada_por_id) and user == self.solicitada_por_id

    def _puede_aprobar(self, user):
        self.ensure_one()
        if self._pidio_la_aprobacion(user):
            return False
        if user.has_group('qb_cotizador.group_autoriza_bajo_piso'):
            return True
        if self.semaforo == 'rojo':
            return False
        titulares = self._users_of_job(self._job_from_param(PARAM_APROBADOR))
        suplentes = self._users_of_job(self._job_from_param(PARAM_SUPLENTE))
        return user in (titulares | suplentes)

    def _aprobadores_salvo(self, user):
        """Titular y suplente que no son ``user`` (a quién le toca cuando quien
        pide es aprobador)."""
        self.ensure_one()
        users = self._users_of_job(self._job_from_param(PARAM_APROBADOR)) \
            | self._users_of_job(self._job_from_param(PARAM_SUPLENTE))
        return users - user

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
        recs = super().create(vals_list)
        # 1.3.0: una cotización en divisa nace con el TC de Odoo (antes se quedaba en 1.0 hasta
        # calcular el costo y el precio se leía como MXN). Las importadas y las capturadas a mano no.
        for rec in recs.filtered(lambda r: r.es_divisa and r.costo_fuente != 'legado'
                                 and r.fx_fuente != 'manual' and (r.fx_rate or 0.0) <= 1.0):
            rec._fx_desde_odoo()
        return recs

    @api.onchange('product_id')
    def _onchange_product_id(self):
        if self.product_id:
            self.volumen_uom = 'kg' if self.product_id.uom_id.name.lower().startswith('k') else 'm'

    @api.onchange('costo_fuente')
    def _onchange_costo_fuente(self):
        """1.7.0: al elegir «manual» la foto queda en blanco para capturarla (antes se quedaban los números del
        hermano o del período y parecía un costo capturado)."""
        if self.costo_fuente == 'manual' and self._origin.costo_fuente != 'manual':
            self.update(self._vals_foto_en_blanco())

    @api.model
    def _vals_foto_en_blanco(self):
        vals = dict.fromkeys(CAMPOS_FOTO_MANUAL, 0.0)
        vals.update({'costo_id': False, 'calidad': 'ninguna', 'calidad_detalle': False, 'receta_detalle': False,
                     'consumo_kg': 0.0, 'consumo_hermano_kg': 0.0, 'consumo_ajuste': False})
        return vals

    @api.onchange('partner_id')
    def _onchange_partner_id(self):
        if self.partner_id and self.partner_id.property_product_pricelist.currency_id:
            self.currency_id = self.partner_id.property_product_pricelist.currency_id
            self._onchange_currency_id()

    @api.onchange('currency_id')
    def _onchange_currency_id(self):
        """1.3.0: al elegir la moneda el TC se llena con el de Odoo (1.0 en la moneda de la compañía)."""
        rate, fecha = self._fx_odoo()
        self.fx_rate, self.fx_fecha, self.fx_fuente = rate, fecha, 'odoo'

    @api.onchange('fx_rate')
    def _onchange_fx_rate(self):
        """1.3.0: un TC distinto al de Odoo queda como capturado a mano (y el cálculo no lo pisa)."""
        rate, fecha = self._fx_odoo()
        if abs((self.fx_rate or 0.0) - rate) < 1e-6:
            self.fx_fuente, self.fx_fecha = 'odoo', fecha
        else:
            self.fx_fuente, self.fx_fecha = 'manual', fields.Date.context_today(self)

    # ==================================================================
    # Cálculo del costo
    # ==================================================================
    def _producto_del_costo(self):
        self.ensure_one()
        if self.costo_fuente == 'hermano':
            return self.hermano_product_id
        return self.product_id

    def _fx_odoo(self):
        """(TC, fecha) de Odoo para la moneda de la cotización: pesos por 1 unidad de la divisa con
        el último `res.currency.rate` hasta hoy; (1.0, hoy) en la moneda de la compañía."""
        self.ensure_one()
        company = self.company_id or self.env.company
        hoy = fields.Date.context_today(self)
        if not self.currency_id or self.currency_id == company.currency_id:
            return 1.0, hoy
        rate = self.currency_id._convert(1.0, company.currency_id, company, hoy, round=False)
        fila = self.env['res.currency.rate'].sudo().search(
            [('currency_id', '=', self.currency_id.id), ('name', '<=', hoy),
             ('company_id', 'in', [company.id, False])], order='name desc, company_id desc', limit=1)
        return rate, (fila.name if fila else hoy)

    def _fx_hoy(self):
        return self._fx_odoo()[0]

    def _fx_desde_odoo(self):
        """Escribe el TC de Odoo (salvo que esté capturado a mano). Devuelve los valores escritos."""
        vals = {}
        for rec in self:
            if rec.fx_fuente == 'manual' and rec.es_divisa:
                continue
            rate, fecha = rec._fx_odoo()
            vals = {'fx_rate': rate, 'fx_fecha': fecha, 'fx_fuente': 'odoo'}
            rec.write(vals)
        return vals

    def action_tc_hoy(self):
        """Botón «TC de hoy»: vuelve al tipo de cambio de Odoo aunque estuviera capturado a mano."""
        for rec in self:
            if rec.state not in ('borrador', 'por_aprobar', 'presentada'):
                raise UserError('El tipo de cambio solo se cambia en una cotización viva.')
            rate, fecha = rec._fx_odoo()
            rec.write({'fx_rate': rate, 'fx_fecha': fecha, 'fx_fuente': 'odoo'})
            rec._calcular_tramos()
            rec.message_post(body='Tipo de cambio de Odoo del %s: %.4f MXN por %s.' % (
                fecha, rate, rec.currency_id.name))
        return True

    def _periodo_para_cotizar(self):
        """El período con el que se cotiza: el último cerrado. 1.3.0: si no hay ninguno cerrado,
        el último que ya tenga costos calculados aunque siga en borrador; en ese caso el costo
        queda marcado provisional (calidad baja) y la hoja interna lo dice. Devuelve (período,
        provisional)."""
        self.ensure_one()
        Periodo = self.env['qb.periodo'].sudo()
        periodo = Periodo.para_cotizar(self.company_id)
        if periodo:
            return periodo, False
        periodo = Periodo.search([('company_id', '=', self.company_id.id), ('costo_ids', '!=', False)],
                                 order='period desc', limit=1)
        return periodo, bool(periodo)

    def action_calcular(self):
        """Toma el costo del producto (o del hermano) en el último período
        cerrado y lo guarda como foto; con fuente manual solo refresca TC,
        escalera y supuestos sobre lo capturado."""
        # El vendedor no tiene permisos sobre el costeo: la lectura va con sudo.
        Costo = self.env['qb.costo.unitario'].sudo()
        for rec in self:
            if rec.state not in ('borrador', 'por_aprobar', 'presentada'):
                raise UserError('Solo se recalcula una cotización viva (borrador, por aprobar o '
                                'presentada). Para una vencida usa «Renovar».')
            vals = {'calculado_el': fields.Datetime.now(), 'receta_detalle': False, 'consumo_kg': 0.0,
                    'consumo_hermano_kg': 0.0, 'consumo_ajuste': False}
            if rec.es_divisa and rec.fx_fuente == 'manual':
                pass  # 1.3.0: el TC capturado a mano se respeta
            else:
                rate, fecha = rec._fx_odoo()
                vals.update({'fx_rate': rate, 'fx_fecha': fecha, 'fx_fuente': 'odoo'})
            if rec.costo_fuente in ('periodo', 'hermano'):
                producto = rec._producto_del_costo()
                if not producto:
                    raise UserError('Elige el producto existente (o el hermano) del que se toma '
                                    'el costo, o cambia la fuente a «Capturado a mano».')
                periodo, provisional = rec._periodo_para_cotizar()
                if not periodo:
                    raise UserError('No hay un período de costeo con costos calculados en %s: calcula y '
                                    'cierra uno en Manufactura → Costos → Períodos.' % rec.company_id.name)
                costo = Costo.search([('periodo_id', '=', periodo.id),
                                      ('product_id', '=', producto.id)], limit=1)
                if not costo:
                    raise UserError('%s no tiene costo calculado en el período %s. Calcula los '
                                    'costos del período, elige otro producto hermano, «Receta del '
                                    'desarrollo» si el artículo tiene lista de materiales, o captura '
                                    'el costo a mano.' % (producto.display_name, periodo.period))
                factor, origen, kg_m, kg_fuente, conversion = rec._costo_conversion(costo)
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
                for campo in COSTO_CAMPOS_UNIDAD:
                    vals[campo] = vals[campo] * factor
                vals.update({'costo_uom': rec.volumen_uom or 'm', 'costo_uom_origen': origen,
                             'costo_kg_por_m': kg_m, 'costo_kg_fuente': kg_fuente,
                             'costo_conversion': conversion})
                if provisional:
                    vals.update({'calidad': 'baja', 'calidad_detalle': 'período %s sin cerrar: costo provisional%s' % (
                        periodo.period, (' (%s)' % costo.calidad_detalle) if costo.calidad_detalle else '')})
            elif rec.costo_fuente == 'receta':
                vals.update(rec._costo_receta())
            elif rec.costo_fuente == 'manual':
                periodo, _provisional = rec._periodo_para_cotizar()
                vals.update({
                    'periodo_id': periodo.id if periodo else False,
                    # 1.6.0: lo capturado a mano está en la unidad de la cotización.
                    'costo_uom': rec.volumen_uom or 'm', 'costo_uom_origen': rec.volumen_uom or 'm',
                    'costo_kg_por_m': 0.0, 'costo_kg_fuente': False, 'costo_conversion': '',
                    'costo_variable': rec.mp_unit + rec.energia_unit,
                    'costo_produccion': rec.mp_unit + rec.fabricacion_unit,
                    'op_pct': rec.op_pct or (periodo.op_pct if periodo else 0.0),
                    'calidad': 'baja', 'calidad_detalle': 'capturado a mano',
                })
            else:
                raise UserError('Una cotización importada del cotizador anterior no se recalcula: '
                                'crea una revisión nueva.')
            if rec.costo_fuente == 'manual':
                vals.update({'costo_id': False, 'precio_mercado': 0.0})
            rec.write(vals)
            rec._calcular_tramos()
            rec.supuestos = rec._texto_supuestos()
            rec.message_post(body='Costo recalculado (%s): piso lleno $%.4f, piso ocioso $%.4f, '
                                  'calidad %s%s, TC %.4f (%s%s).' % (
                                      dict(COSTO_FUENTES).get(rec.costo_fuente), rec.piso_lleno,
                                      rec.piso_ocioso, rec.calidad,
                                      (' · ' + rec.calidad_detalle) if rec.calidad_detalle else '',
                                      rec.fx_rate, dict(FX_FUENTES).get(rec.fx_fuente, ''),
                                      (' del %s' % rec.fx_fecha) if rec.fx_fecha else ''))
        return True

    # ------------------------------------------------------------------
    # 1.7.0: receta del desarrollo
    # ------------------------------------------------------------------
    @api.model
    def _consumo_tela(self, product, cutoff=None):
        """(kg de tela por unidad de ``product``, componentes) según su receta vigente: los renglones cuyo
        componente lleva código de la nomenclatura DAT P-D02-01 (la tela que entra: crudo, teñido…), en kg.
        (0, vacío) si la receta no lleva tela o no se puede pasar a kg."""
        from odoo.addons.qb_costeo.models.horas import NOMEN_RE
        Horas = self.env['qb.producto.horas'].sudo()
        bom = Horas._bom_vigente(product, cutoff)
        kg = self.env.ref('uom.product_uom_kgm', raise_if_not_found=False)
        if not bom or not kg:
            return 0.0, self.env['product.product']
        total, comps = 0.0, self.env['product.product']
        for line in bom.bom_line_ids:
            comp = line.product_id
            if line._skip_bom_line(product) or not NOMEN_RE.match((comp.default_code or '').replace(' ', '')):
                continue
            if not line.product_uom_id._has_common_reference(kg):
                continue
            total += line.product_uom_id._compute_quantity(line.product_qty, kg, round=False)
            comps |= comp
        return total / Horas._qty_bom_en_uom_producto(bom, product), comps

    def _consumo_hermano_ajustado(self, hermano, cutoff=None):
        """(consumo del hermano ajustado, texto): kg de tela por unidad del hermano × ancho y gramaje del
        artículo nuevo (de la cotización o de su código) ÷ los del hermano (de su código)."""
        self.ensure_one()
        base, _comps = self._consumo_tela(hermano, cutoff)
        if not base:
            return 0.0, ''
        g_h, a_h = self._codigo_partes(hermano.default_code)
        g_n, a_n = self._codigo_partes(self.product_id.default_code)
        g_n = self.spec_gramaje or g_n
        a_n = self.spec_ancho or a_n
        factor, partes = 1.0, ['%s kg' % _num(base)]
        if a_h and a_n:
            factor *= a_n / a_h
            partes.append('× %s/%s de ancho' % (_num(a_n * 100), _num(a_h * 100)))
        if g_h and g_n:
            factor *= g_n / g_h
            partes.append('× %s/%s g/m²' % (_num(g_n), _num(g_h)))
        if len(partes) == 1:
            partes.append('sin ajuste: no se leen ancho ni gramaje')
        return base * factor, ' '.join(partes)

    def _costo_receta(self):
        """Foto del costo del artículo nuevo con su lista de materiales, para el período con que se cotiza
        (1.7.0, Dirección General 2026-10-26; caso COT-2026-0015, proyecto 630).

        - La receta se explota hacia abajo (acabado → teñido → crudo, y una fórmula como VERDE HOJA 001 → sus
          químicos). Un componente que ya tiene costo en el período (el crudo que sí se produce, el engomado) entra
          con ese costo: su MP, su fabricación y su energía. Un componente sin costo con lista de materiales se
          explota (una fórmula con ``standard_price`` 0 se costea con su lista). Uno comprado, a la última compra
          antes del cierre del período; sin compras, a su ``standard_price``, y los supuestos lo dicen.
        - Conversión (fabricación y energía) por cada centro de la ruta con horas × la tarifa del período. Un
          centro que el hermano sí trae y que ni la ruta ni los componentes con costo cubren hereda la conversión
          por kg de tela del hermano en ese centro, por los kg de tela de la receta nueva.
        - Rendimiento del hermano (sin hermano, el de planta); operación del período.
        - Calidad media, con lo heredado en el detalle (baja sin hermano o con el período sin cerrar)."""
        self.ensure_one()
        product = self.product_id
        if not product:
            raise UserError('«Receta del desarrollo» costea el artículo nuevo: liga el producto (el artículo que '
                            'generó el desarrollo) o elige otra fuente.')
        periodo, provisional = self._periodo_para_cotizar()
        if not periodo:
            raise UserError('No hay un período de costeo con costos calculados en %s: calcula y cierra uno en '
                            'Manufactura → Costos → Períodos.' % self.company_id.name)
        env = self.env(su=True)
        Horas = env['qb.producto.horas'].with_company(self.company_id)
        Mp = env['qb.producto.mp'].with_company(self.company_id)
        cutoff = periodo.date_to
        if not Horas._bom_vigente(product, cutoff):
            raise UserError('%s no tiene lista de materiales: captúrala (C1.04) o elige otra fuente del costo.'
                            % product.display_name)
        Costo = env['qb.costo.unitario']
        costos = {c.product_id.id: c for c in Costo.search([('periodo_id', '=', periodo.id)])}
        tarifas = {t.centro_id.id: t for t in periodo.sudo().tarifa_ids}
        centros = env['qb.centro'].search([('company_id', '=', self.company_id.id), ('nature', '=', 'directo')])
        notas = {'periodo': [], 'compra': [], 'standard': [], 'sin_precio': [], 'ruta': []}
        precios = {}
        pila = set()

        def nodo(prod, raiz=False):
            """(mp, fabricación, energía, centros cubiertos) por unidad de ``prod``."""
            costo = costos.get(prod.id)
            if not raiz and costo and costo.costo_produccion > 0:
                notas['periodo'].append('%s $%s' % (prod.default_code or prod.name, _num(costo.costo_produccion)))
                cubiertos = {l.centro_id.id for l in costo.linea_ids if l.costo_unit}
                return costo.mp_unit, costo.fabricacion_unit, costo.energia_unit, cubiertos
            bom = Horas._bom_vigente(prod, cutoff) if prod.id not in pila else False
            if bom and bom.bom_line_ids:
                pila.add(prod.id)
                qty_bom = Horas._qty_bom_en_uom_producto(bom, prod)
                mp = fab = energia = 0.0
                cubiertos = set()
                for line in bom.bom_line_ids:
                    comp = line.product_id
                    if line._skip_bom_line(prod) or comp.type == 'service':
                        continue
                    qty = line.product_uom_id._compute_quantity(line.product_qty, comp.uom_id, round=False,
                                                                raise_if_failure=False) / qty_bom
                    c_mp, c_fab, c_en, c_cub = nodo(comp)
                    mp += qty * c_mp
                    fab += qty * c_fab
                    energia += qty * c_en
                    cubiertos |= c_cub
                for centro in centros:
                    horas = Horas._estandar(prod, bom, centro)
                    tarifa = tarifas.get(centro.id)
                    if horas and tarifa:
                        fab += horas * tarifa.tarifa
                        energia += horas * tarifa.tarifa_variable
                        cubiertos.add(centro.id)
                        notas['ruta'].append('%s %s h × $%s' % (centro.name, _num(horas), _num(tarifa.tarifa)))
                pila.discard(prod.id)
                return mp, fab, energia, cubiertos
            if prod.id not in precios:
                precios.update(Mp.precios_compra([prod.id], cutoff))
            ref = prod.default_code or prod.name
            if prod.id in precios:
                precio = precios[prod.id]
                notas['compra'].append('%s $%s' % (ref, _num(precio)))
            elif prod.standard_price > 0:
                precio = prod.standard_price
                notas['standard'].append('%s $%s' % (ref, _num(precio)))
            else:
                precio = 0.0
                notas['sin_precio'].append(ref)
            return precio, 0.0, 0.0, set()

        mp, fab, energia, cubiertos = nodo(product, raiz=True)
        consumo, telas = self._consumo_tela(product, cutoff)
        hermano = self.hermano_product_id
        costo_h = costos.get(hermano.id) if hermano else None
        heredado, centros_heredados = [], []
        if costo_h:
            base_h = self._consumo_tela(hermano, cutoff)[0] or costo_h.kg_por_unidad or 1.0
            base_n = consumo or (1.0 if self._unidad_de(product.uom_id.name) == 'kg' else 0.0)
            for linea in costo_h.linea_ids.filtered(lambda l: l.costo_unit and l.centro_id.id not in cubiertos):
                if not base_n:
                    heredado.append('%s (sin kg de tela en la receta nueva: no se hereda)' % linea.centro_id.name)
                    continue
                por_kg = linea.costo_unit / base_h
                en_kg = linea.horas_unidad * linea.tarifa_variable / base_h
                fab += por_kg * base_n
                energia += en_kg * base_n
                cubiertos.add(linea.centro_id.id)
                centros_heredados.append(linea.centro_id.name)
                heredado.append('%s $%s/kg × %s kg' % (linea.centro_id.name, _num(round(por_kg, 4)), _num(base_n)))
        faltan = [c.name for c in Horas.centros_esperados(product, centros, cutoff) if c.id not in cubiertos]
        if costo_h:
            rend, rend_fuente = costo_h.rendimiento or 1.0, 'del hermano %s' % hermano.default_code
        else:
            rend, rend_fuente = env['qb.producto.rendimiento'].with_company(self.company_id).planta(cutoff), 'de planta'
        rend = min(max(rend or 1.0, 0.05), 1.0)
        consumo_h, ajuste = self._consumo_hermano_ajustado(hermano, cutoff) if hermano else (0.0, '')

        detalle = ['Receta de %s (período %s):' % (product.display_name, periodo.period)]
        if notas['periodo']:
            detalle.append('- Con su costo del período: %s.' % ', '.join(notas['periodo']))
        if notas['compra']:
            detalle.append('- Comprados, al precio de compra del período: %s.' % ', '.join(notas['compra']))
        if notas['standard']:
            detalle.append('- Sin compras, a su costo estándar: %s.' % ', '.join(notas['standard']))
        if notas['sin_precio']:
            detalle.append('- Sin compras ni costo estándar ($0): %s.' % ', '.join(notas['sin_precio']))
        if notas['ruta']:
            detalle.append('- Conversión de la ruta: %s.' % ', '.join(notas['ruta']))
        if heredado:
            detalle.append('- Conversión heredada del hermano %s (la ruta no tiene horas): %s.' % (
                hermano.default_code, ', '.join(heredado)))
        if faltan:
            detalle.append('- Sin conversión (ni horas en la ruta ni hermano): %s.' % ', '.join(faltan))
        detalle.append('- Rendimiento %.2f %% %s.' % (100 * rend, rend_fuente))
        if consumo:
            detalle.append('- Tela: %s kg por %s (%s).' % (_num(consumo), product.uom_id.name,
                                                          ', '.join(telas.mapped('default_code'))))

        lo_heredado = ['rendimiento'] if costo_h else []
        if centros_heredados:
            lo_heredado.append('conversión en %s' % ', '.join(centros_heredados))
        det = ['receta de %s' % (product.default_code or product.name)]
        if lo_heredado:
            det.append('heredado del hermano %s: %s' % (hermano.default_code, ', '.join(lo_heredado)))
        else:
            det.append('sin hermano con costo: rendimiento de planta')
        if notas['standard']:
            det.append('costo estándar en %d componente(s)' % len(notas['standard']))
        if notas['sin_precio']:
            det.append('sin precio: %s' % ', '.join(notas['sin_precio']))
        if faltan:
            det.append('sin conversión en %s' % ', '.join(faltan))
        calidad = 'media' if costo_h and not faltan else 'baja'
        if provisional:
            calidad = 'baja'
            det.insert(0, 'período %s sin cerrar: costo provisional' % periodo.period)

        Peso = env['qb.producto.kg'].with_company(self.company_id)
        kg_u = Peso.kg_de(product)[0] if self._unidad_de(product.uom_id.name) != 'kg' else 1.0
        origen = SimpleNamespace(uom_name=product.uom_id.name, product_id=product, kg_por_unidad=kg_u or 0.0)
        factor, uom_origen, kg_m, kg_fuente, conversion = self._costo_conversion(origen)
        vals = {
            'periodo_id': periodo.id, 'costo_id': False,
            'mp_unit': mp, 'energia_unit': energia, 'fabricacion_unit': fab,
            'costo_variable': mp + energia, 'costo_produccion': mp + fab,
            'rendimiento': rend, 'op_pct': periodo.op_pct,
            'calidad': calidad, 'calidad_detalle': ' · '.join(det), 'precio_mercado': 0.0,
            'receta_detalle': '\n'.join(detalle),
            'consumo_kg': consumo, 'consumo_hermano_kg': consumo_h, 'consumo_ajuste': ajuste,
        }
        for campo in COSTO_CAMPOS_UNIDAD:
            vals[campo] = vals[campo] * factor
        vals.update({'costo_uom': self.volumen_uom or 'm', 'costo_uom_origen': uom_origen, 'costo_kg_por_m': kg_m,
                     'costo_kg_fuente': kg_fuente, 'costo_conversion': conversion})
        return vals

    def _texto_supuestos(self):
        self.ensure_one()
        partes = []
        if self.periodo_id:
            partes.append('Período de costo %s (%s).' % (self.periodo_id.period,
                                                        self.periodo_id.state))
        partes.append('Fuente: %s.' % dict(COSTO_FUENTES).get(self.costo_fuente, ''))
        if self.costo_fuente in ('hermano', 'receta') and self.hermano_product_id:
            partes.append('Hermano: %s.' % self.hermano_product_id.display_name)
        if self.costo_uom:
            if self.costo_conversion and self.costo_uom_origen and self.costo_uom_origen != self.costo_uom:
                partes.append('Costo de origen %s convertido a %s (%s; %s).' % (
                    dict(COSTO_UOMS_ORIGEN)[self.costo_uom_origen], self.costo_uom, self.costo_conversion,
                    dict(KG_FUENTES).get(self.costo_kg_fuente, '').lower() or 'ancho de la cotización'))
            else:
                partes.append('Costos y precio %s.' % dict(COSTO_UOMS_ORIGEN)[self.costo_uom])
        partes.append('Rendimiento vendible %.1f %%; operación %.2f %% de la venta.'
                      % (100 * (self.rendimiento or 1.0), 100 * (self.op_pct or 0.0)))
        if self.es_divisa:
            partes.append('TC %.4f MXN por %s (%s%s).' % (
                self.fx_rate, self.currency_id.name, dict(FX_FUENTES).get(self.fx_fuente, ''),
                (' del %s' % self.fx_fecha) if self.fx_fecha else ''))
        if self.calidad_detalle:
            partes.append('Calidad: %s.' % self.calidad_detalle)
        texto = ' '.join(partes)
        # 1.7.0: el desglose de la receta y el aviso de consumo van a la hoja interna.
        if self.costo_fuente == 'receta' and self.receta_detalle:
            texto += '\n' + self.receta_detalle
            if self.consumo_alerta:
                texto += '\nAviso: ' + self.consumo_alerta
        return texto

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
        elif self.sin_costo:
            faltan.append('un piso de costo mayor a cero (el semáforo no se puede leer)')
        if self.es_divisa and (self.fx_rate or 0.0) <= 1.0:
            faltan.append('el tipo de cambio (la cotización es en %s y el TC es %.4f)' % (
                self.currency_id.name, self.fx_rate or 0.0))
        if self.unidad_alerta:
            faltan.append('recalcular el costo en la unidad de la cotización (%s)' % self.unidad_alerta)
        if self.peso_alerta and not self.peso_confirmado:
            faltan.append('revisar el peso (%s)' % self.peso_alerta)
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
            # 1.5.0: el aviso de aprobar no le llega a quien la pidió (no la aprueba).
            users = rec._aprobadores_salvo(self.env.user)
            if not users and rec._pidio_la_aprobacion(self.env.user) and rec.semaforo != 'rojo':
                rec.message_post(body='Usted pidió la aprobación y es quien aprueba: nadie más puede '
                                      'aprobarla hasta que Dirección nombre el suplente en Ajustes → '
                                      'Ventas → Cotizador, o que la mande otra persona.')
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
                if rec._pidio_la_aprobacion(self.env.user):
                    otros = rec._aprobadores_salvo(self.env.user)
                    raise UserError('Usted mandó esta cotización a aprobar: quien pide no la aprueba. %s' % (
                        ('Le toca a %s.' % ', '.join(otros.mapped('name'))) if otros else
                        'No hay suplente nombrado (Ajustes → Ventas → Cotizador): nadie más puede aprobarla.'))
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
            if rec.unidad_alerta:
                # 1.6.0: tampoco se presenta (aprobación aquí o en Aprobaciones) con unidades cruzadas.
                raise UserError('%s no se puede presentar: %s' % (rec.folio or rec.display_name, rec.unidad_alerta))
            if rec.peso_alerta and not rec.peso_confirmado:
                raise UserError('%s no se puede presentar: %s' % (rec.folio or rec.display_name, rec.peso_alerta))
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
        if vals.get('costo_fuente') == 'manual':
            # 1.7.0: pasar a «manual» deja en blanco lo que no se captura en el mismo cambio.
            cambian = self.filtered(lambda r: r.costo_fuente != 'manual')
            if cambian and cambian != self:
                return (self - cambian).write(vals) and cambian.write(vals)
            if cambian:
                vals = dict(self._vals_foto_en_blanco(), **vals)
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

    def _producto_para_tarifa(self):
        """Artículo cuyo precio va a la tarifa. Gancho: el puente con el SGI
        pone el artículo generado por el desarrollo cuando la cotización se
        hizo antes de que existiera."""
        self.ensure_one()
        return self.product_id

    def _sincronizar_tarifa(self):
        """Ganada + cliente aprobó la muestra ⇒ precio en la tarifa del
        cliente con precio, moneda y vigencia de la cotización. 1.2.0: sella
        ``tarifa_fecha`` / ``tarifa_user_id`` (medición de C1.17)."""
        for rec in self:
            if rec.state != 'ganada' or not rec.cliente_aprobo or rec.pricelist_item_id:
                continue
            product = rec._producto_para_tarifa()
            if not product:
                rec.message_post(body='Sin artículo ligado: el precio no se puede poner en la '
                                      'tarifa. Liga el artículo y vuelve a marcar la aprobación.')
                continue
            if rec.product_id != product:
                rec.write({'product_id': product.id})
            tarifa = rec._tarifa_del_cliente()
            item = self.env['product.pricelist.item'].sudo().create({
                'pricelist_id': tarifa.id, 'applied_on': '1_product',
                'product_tmpl_id': product.product_tmpl_id.id,
                'compute_price': 'fixed', 'fixed_price': rec.precio_objetivo,
                'min_quantity': 0,
                'date_start': rec.ganada_date or fields.Datetime.now(),
                'date_end': fields.Datetime.to_datetime(rec.validez_hasta) if rec.validez_hasta
                and rec.validez_hasta > fields.Date.context_today(rec) else False,
            })
            rec.write({'pricelist_item_id': item.id, 'tarifa_fecha': fields.Datetime.now(),
                       'tarifa_user_id': self.env.uid})
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
        self._pdf_validar_unidades()
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
