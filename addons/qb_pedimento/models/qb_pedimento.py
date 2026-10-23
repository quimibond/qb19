# -*- coding: utf-8 -*-
"""El expediente de un pedimento. Ver docs/superpowers/specs/2026-10-23-pedimentos-design.md.

Un registro = un pedimento con su cuenta de gastos: es la fila de los Excel
F-P-A-16-05 IMPORTACIONES y F-P-A-16-06 EXPORTACIONES que llevaba Logística.
Se liga muchos a muchos con compras, recepciones o entregas y facturas (un
pedimento cubrió tres recepciones en LC/2026/0254; un V1 cubre las facturas
del mes). No crea asientos: el costo en destino nace en borrador.
"""
import re
from datetime import timedelta

from odoo import _, api, fields, models
from odoo.exceptions import UserError, ValidationError

from .qb_pedimento_catalog import OPERACIONES

# AA AD PPPP NNNNNNN: año de validación, aduana de despacho, patente, consecutivo.
NUMERO_RE = re.compile(r'^(\d{2}) (\d{2}) (\d{4}) (\d{7})$')

STATES = [
    ('preparacion', 'Preparación'),
    ('transito', 'En tránsito'),
    ('aduana', 'En aduana'),
    ('despachado', 'Despachado'),
    ('entregado', 'Entregado / Cruzado'),
    ('cerrado', 'Cerrado'),
    ('cancelado', 'Cancelado'),
]
# Estados a partir de los cuales el pedimento ya está pagado y tiene número.
CON_NUMERO = ('despachado', 'entregado', 'cerrado')


def normalizar_numero(valor):
    """'26-16-3774-6000206', '26  16  3774  6000206' o '261637746000206' →
    '26 16 3774 6000206' (como lo escribe Logística). Lo que no tenga 15
    dígitos se devuelve tal cual para que la validación lo rechace con su
    mensaje."""
    if not valor:
        return False
    digitos = re.sub(r'\D', '', valor)
    if len(digitos) != 15:
        return valor.strip()
    return '%s %s %s %s' % (digitos[:2], digitos[2:4], digitos[4:8], digitos[8:])


def formato_nativo(numero):
    """Formato de la localización mexicana (l10n_mx_edi): dos espacios entre
    bloques, p.ej. '26  16  3774  6000417' (LC/2026/0254 en producción)."""
    return numero.replace(' ', '  ') if numero else False


def dias_habiles_entre(desde, hasta):
    """Días hábiles (lunes a viernes) transcurridos de `desde` a `hasta`, sin
    contar `desde`. No descuenta feriados: el aviso puede llegar un día antes."""
    if not desde or desde >= hasta:
        return 0
    dias, d = 0, desde
    while d < hasta:
        d += timedelta(days=1)
        if d.weekday() < 5:
            dias += 1
    return dias


class QbPedimento(models.Model):
    _name = 'qb.pedimento'
    _description = 'Pedimento'
    _inherit = ['mail.thread', 'mail.activity.mixin']
    _order = 'fecha_pago desc, id desc'

    # ── identidad ────────────────────────────────────────────────────
    name = fields.Char('Expediente', compute='_compute_name', store=True, index=True)
    numero = fields.Char(
        'Número de pedimento', tracking=True, copy=False, index=True,
        help='15 dígitos: año (2), aduana (2), patente (4) y consecutivo (7), p.ej. 26 16 3774 6000206. '
             'El primer dígito del consecutivo es el último dígito del año. Obligatorio desde «Despachado».')
    referencia_agencia = fields.Char(
        'Referencia de la agencia', tracking=True,
        help='Referencia o número de cuenta de gastos con que la agencia identifica la operación '
             '(83272, SIN-0661/26). Identifica el expediente antes de que exista el número.')
    operacion = fields.Selection(OPERACIONES, required=True, default='importacion', tracking=True)
    clave_id = fields.Many2one(
        'qb.pedimento.clave', 'Clave', tracking=True,
        domain="[('operacion', 'in', (operacion, 'ambas'))]")
    virtual = fields.Boolean(related='clave_id.virtual')
    rectificacion = fields.Boolean(related='clave_id.rectificacion')
    rectifica_id = fields.Many2one(
        'qb.pedimento', 'Rectifica a', index=True,
        domain="[('id', '!=', id), ('company_id', '=', company_id)]",
        help='Pedimento original que esta rectificación (R1) corrige.')
    rectificacion_ids = fields.One2many('qb.pedimento', 'rectifica_id', 'Rectificaciones')
    aduana_id = fields.Many2one('qb.pedimento.aduana', 'Aduana', readonly=True,
                                help='Sale del número de pedimento (dígitos 3 y 4).')
    patente = fields.Char(compute='_compute_patente', store=True, index=True)
    state = fields.Selection(STATES, 'Etapa', default='preparacion', required=True, tracking=True,
                             group_expand=True, copy=False)
    company_id = fields.Many2one('res.company', required=True, default=lambda s: s.env.company, index=True)
    user_id = fields.Many2one(
        'res.users', 'Responsable', tracking=True,
        default=lambda s: s.env.company.qb_pedimento_responsable_id or s.env.user,
        help='Recibe los avisos del expediente (actividades).')

    # ── contrapartes ─────────────────────────────────────────────────
    partner_id = fields.Many2one(
        'res.partner', 'Proveedor / Cliente', tracking=True, index=True,
        help='Proveedor extranjero en importación; cliente en exportación.')
    agencia_id = fields.Many2one(
        'res.partner', 'Agencia aduanal', tracking=True, index=True,
        help='Quien despacha y factura la cuenta de gastos. Una agencia opera con varias patentes; '
             'la patente sale del número.')
    transportista_id = fields.Many2one('res.partner', 'Naviera / transportista')

    # ── documentos ligados ───────────────────────────────────────────
    purchase_ids = fields.Many2many(
        'purchase.order', 'qb_pedimento_purchase_rel', 'pedimento_id', 'purchase_id', 'Compras')
    sale_ids = fields.Many2many(
        'sale.order', 'qb_pedimento_sale_rel', 'pedimento_id', 'sale_id', 'Ventas')
    picking_ids = fields.Many2many(
        'stock.picking', 'qb_pedimento_picking_rel', 'pedimento_id', 'picking_id', 'Recepciones / entregas')
    invoice_ids = fields.Many2many(
        'account.move', 'qb_pedimento_move_rel', 'pedimento_id', 'move_id', 'Facturas de la mercancía',
        domain="[('move_type', 'in', ('in_invoice', 'in_refund', 'out_invoice', 'out_refund'))]",
        help='Factura del proveedor extranjero (importación) o facturas de venta que ampara (exportación; '
             'en un V1 mensual, todas las del mes).')
    gasto_move_ids = fields.Many2many(
        'account.move', 'qb_pedimento_gasto_move_rel', 'pedimento_id', 'move_id', 'Facturas de la agencia',
        domain="[('move_type', 'in', ('in_invoice', 'in_refund'))]",
        help='Cuenta de gastos, honorarios, naviera, fletes y maniobras de este pedimento.')
    landed_cost_ids = fields.One2many('stock.landed.cost', 'qb_pedimento_id', 'Costos en destino')
    purchase_count = fields.Integer(compute='_compute_counts')
    picking_count = fields.Integer(compute='_compute_counts')
    invoice_count = fields.Integer(compute='_compute_counts')
    landed_cost_count = fields.Integer(compute='_compute_counts')

    # ── embarque ─────────────────────────────────────────────────────
    incoterm_id = fields.Many2one('account.incoterms', 'Incoterm')
    factura_proveedor = fields.Char('Factura comercial', help='Folio de la factura comercial (25MH322448).')
    bl = fields.Char('BL / guía', help='Conocimiento de embarque, guía aérea o de courier.')
    booking = fields.Char('Booking')
    contenedores = fields.Char('Contenedores', help='Separados por coma: NYKU0807898, CMAU…')
    cove = fields.Char('COVE')
    doda = fields.Char('DODA')
    entry = fields.Char('Entry (EUA)', help='Número de entry del lado americano en exportaciones.')
    origen_country_id = fields.Many2one('res.country', 'País de origen / destino')

    # ── fechas ───────────────────────────────────────────────────────
    fecha_factura = fields.Date('Fecha factura')
    fecha_arribo = fields.Date('Arribo / ETA', tracking=True)
    fecha_pago = fields.Date('Pago del pedimento', tracking=True)
    fecha_despacho = fields.Datetime('Liberación / despacho', tracking=True, copy=False)
    fecha_entrega = fields.Datetime(
        'Entrega en planta / cruce', tracking=True, copy=False,
        help='Importación: llegada a planta. Exportación: cruce confirmado por la agencia (C2.31).')
    fecha_cierre = fields.Date('Cierre del expediente', tracking=True, copy=False)
    dias_cierre = fields.Integer(
        'Días hábiles a cierre', compute='_compute_dias_cierre', store=True, aggregator='avg',
        help='Días hábiles de la entrega o cruce al cierre del expediente (indicador C2-05: meta 5).')

    # ── valores y contribuciones (MXN) ───────────────────────────────
    currency_id = fields.Many2one(related='company_id.currency_id')
    factura_currency_id = fields.Many2one('res.currency', 'Moneda de la factura')
    valor_factura = fields.Monetary('Valor factura (moneda)', currency_field='factura_currency_id')
    tipo_cambio = fields.Float('Tipo de cambio', digits=(12, 4))
    valor_comercial = fields.Monetary('Valor comercial')
    valor_aduana = fields.Monetary('Valor en aduana')
    dta = fields.Monetary('DTA')
    igi = fields.Monetary('IGI / IGE')
    iva = fields.Monetary('IVA')
    prv = fields.Monetary('PRV')
    iva_prv = fields.Monetary('IVA/PRV')
    multas = fields.Monetary('Multas y recargos')
    total_contribuciones = fields.Monetary(
        compute='_compute_totales', store=True,
        help='Lo pagado en efectivo en el pedimento. Lo depositado en cuenta aduanera de garantía (forma de '
             'pago «otros», precios estimados) va como garantía en la pestaña de cuenta de gastos.')
    partida_ids = fields.One2many('qb.pedimento.partida', 'pedimento_id', 'Partidas', copy=True)

    # ── cuenta de gastos ─────────────────────────────────────────────
    cuenta_gastos = fields.Char('Cuenta de gastos', help='Folio de la cuenta de gastos de la agencia.')
    gastos_sin_iva = fields.Monetary(
        'Gastos al costo (sin IVA)',
        help='Honorarios, servicios, fletes, maniobras y almacenajes sin IVA: lo que se suma al costo.')
    cuenta_gastos_total = fields.Monetary(
        'Total cuenta de gastos', tracking=True,
        help='Todo lo que la agencia pagó por cuenta de Quimibond, contribuciones incluidas.')
    anticipo = fields.Monetary('Anticipo', tracking=True)
    fecha_anticipo = fields.Date('Fecha del anticipo')
    saldo = fields.Monetary(
        compute='_compute_totales', store=True,
        help='Total de la cuenta de gastos menos el anticipo, como en el Excel: negativo = saldo a favor '
             'de Quimibond (la agencia lo devuelve); positivo = falta pagarle a la agencia.')
    saldo_liquidado = fields.Boolean(
        'Saldo liquidado', tracking=True, copy=False,
        help='La agencia devolvió el saldo a favor o ya se le pagó la diferencia.')
    garantia_ids = fields.One2many('qb.pedimento.garantia', 'pedimento_id', 'Garantías')
    garantia_pendiente = fields.Monetary(compute='_compute_garantia_pendiente')

    # ── expediente ───────────────────────────────────────────────────
    documento_ids = fields.One2many('qb.pedimento.documento', 'pedimento_id', 'Documentos', copy=False)
    documentos_faltantes = fields.Integer(compute='_compute_documentos', store=True)
    documentos_completos = fields.Boolean(compute='_compute_documentos', store=True)
    notas = fields.Html('Observaciones')

    _numero_uniq = models.Constraint(
        'unique(company_id, numero)', 'Ya existe un expediente con ese número de pedimento.')

    # ── cálculos ─────────────────────────────────────────────────────
    @api.depends('numero', 'referencia_agencia')
    def _compute_name(self):
        for rec in self:
            if rec.numero:
                rec.name = rec.numero
            elif rec.referencia_agencia:
                rec.name = _('Ref. %s', rec.referencia_agencia)
            else:
                rec.name = _('Nuevo')

    @api.depends('numero')
    def _compute_patente(self):
        for rec in self:
            match = NUMERO_RE.match(rec.numero or '')
            rec.patente = match.group(3) if match else False

    def _vals_numero(self, vals):
        """Normaliza el número y pone la aduana que trae (se da de alta si
        no está en el catálogo). Se hace al escribir, no en un cálculo, para
        no crear registros dentro de un compute."""
        if 'numero' not in vals:
            return vals
        vals['numero'] = normalizar_numero(vals['numero'])
        match = NUMERO_RE.match(vals['numero'] or '')
        vals['aduana_id'] = self.env['qb.pedimento.aduana']._get_or_create(match.group(2)).id if match else False
        return vals

    def _compute_counts(self):
        for rec in self:
            rec.purchase_count = len(rec.purchase_ids)
            rec.picking_count = len(rec.picking_ids)
            rec.invoice_count = len(rec.invoice_ids | rec.gasto_move_ids)
            rec.landed_cost_count = len(rec.landed_cost_ids)

    @api.depends('dta', 'igi', 'iva', 'prv', 'iva_prv', 'multas', 'cuenta_gastos_total', 'anticipo')
    def _compute_totales(self):
        for rec in self:
            rec.total_contribuciones = rec.dta + rec.igi + rec.iva + rec.prv + rec.iva_prv + rec.multas
            rec.saldo = rec.cuenta_gastos_total - rec.anticipo if rec.cuenta_gastos_total else 0.0

    @api.depends('garantia_ids.pendiente')
    def _compute_garantia_pendiente(self):
        for rec in self:
            rec.garantia_pendiente = sum(rec.garantia_ids.mapped('pendiente'))

    @api.depends('documento_ids.cumplido', 'documento_ids.required')
    def _compute_documentos(self):
        for rec in self:
            faltan = rec.documento_ids.filtered(lambda d: d.required and not d.cumplido)
            rec.documentos_faltantes = len(faltan)
            rec.documentos_completos = bool(rec.documento_ids) and not faltan

    @api.depends('fecha_entrega', 'fecha_cierre')
    def _compute_dias_cierre(self):
        for rec in self:
            if rec.fecha_entrega and rec.fecha_cierre:
                rec.dias_cierre = dias_habiles_entre(rec.fecha_entrega.date(), rec.fecha_cierre)
            else:
                rec.dias_cierre = 0

    # ── validaciones ─────────────────────────────────────────────────
    @api.constrains('numero', 'clave_id')
    def _check_numero(self):
        for rec in self.filtered('numero'):
            match = NUMERO_RE.match(rec.numero)
            if not match:
                raise ValidationError(_(
                    'El número de pedimento «%s» debe tener 15 dígitos: año, aduana, patente y consecutivo '
                    '(p.ej. 26 16 3774 6000206).', rec.numero))
            anio, consecutivo = match.group(1), match.group(4)
            # El consecutivo empieza con el último dígito del año, salvo en una
            # rectificación de aduana consolidada del año anterior.
            if consecutivo[0] != anio[1] and not rec.rectificacion:
                raise ValidationError(_(
                    'En el pedimento %(n)s el consecutivo debe empezar con %(d)s (último dígito del año 20%(a)s).',
                    n=rec.numero, d=anio[1], a=anio))

    @api.constrains('rectifica_id', 'clave_id')
    def _check_rectificacion(self):
        for rec in self:
            if rec.rectifica_id == rec:
                raise ValidationError(_('Un pedimento no puede rectificarse a sí mismo.'))

    # ── alta y escritura ─────────────────────────────────────────────
    @api.model_create_multi
    def create(self, vals_list):
        records = super().create([self._vals_numero(vals) for vals in vals_list])
        records._sync_documentos()
        records._sync_nativo()
        return records

    def write(self, vals):
        res = super().write(self._vals_numero(vals))
        if {'operacion', 'clave_id'} & set(vals):
            self._sync_documentos()
        if {'numero', 'picking_ids'} & set(vals):
            self._sync_nativo()
        return res

    @api.onchange('purchase_ids')
    def _onchange_purchase_ids(self):
        """La primera compra ligada propone proveedor, moneda e incoterm."""
        po = self.purchase_ids[:1]
        if po:
            self.partner_id = self.partner_id or po.partner_id
            self.factura_currency_id = self.factura_currency_id or po.currency_id
            self.incoterm_id = self.incoterm_id or po.incoterm_id

    @api.onchange('sale_ids')
    def _onchange_sale_ids(self):
        so = self.sale_ids[:1]
        if so:
            self.partner_id = self.partner_id or so.partner_id
            self.incoterm_id = self.incoterm_id or so.incoterm

    def _sync_documentos(self):
        """Agrega al checklist los tipos de documento que pide la operación
        y quita los que ya no aplican y siguen vacíos (no borra archivos)."""
        Tipo = self.env['qb.pedimento.documento.tipo']
        tipos = Tipo.search([])
        for rec in self:
            aplican = tipos._applies_to(rec.operacion, rec.virtual)
            rec.documento_ids.filtered(
                lambda d: d.tipo_id not in aplican and not d.attachment_ids and not d.no_aplica).unlink()
            faltan = aplican - rec.documento_ids.tipo_id
            if faltan:
                self.env['qb.pedimento.documento'].create(
                    [{'pedimento_id': rec.id, 'tipo_id': t.id} for t in faltan])

    def _sync_nativo(self):
        """Copia el número a los campos de la localización mexicana: la
        entrega o recepción (C2.29) y los costos en destino, de donde Odoo lo
        lleva a los lotes y al CFDI. Sin l10n_mx_edi instalado no hace nada."""
        Picking = self.env['stock.picking']
        Landed = self.env['stock.landed.cost']
        for rec in self.filtered('numero'):
            nativo = formato_nativo(rec.numero)
            if 'l10n_mx_edi_pedimento_number' in Picking._fields:
                rec.picking_ids.filtered(
                    lambda p: p.l10n_mx_edi_pedimento_number != nativo
                ).write({'l10n_mx_edi_pedimento_number': nativo})
            if 'l10n_mx_edi_customs_number' in Landed._fields:
                rec.landed_cost_ids.filtered(
                    lambda lc: lc.state == 'draft' and lc.l10n_mx_edi_customs_number != nativo
                ).write({'l10n_mx_edi_customs_number': nativo})

    # ── etapas ───────────────────────────────────────────────────────
    def _set_state(self, state, **extra):
        self.write(dict(state=state, **extra))

    def action_transito(self):
        self._set_state('transito')

    def action_aduana(self):
        self._set_state('aduana')

    def action_despachado(self):
        for rec in self:
            if not rec.numero:
                raise UserError(_('Capture el número de pedimento antes de marcarlo como despachado.'))
            rec._set_state('despachado', fecha_despacho=rec.fecha_despacho or fields.Datetime.now(),
                           fecha_pago=rec.fecha_pago or fields.Date.context_today(rec))

    def action_entregado(self):
        for rec in self:
            if rec.state not in CON_NUMERO:
                raise UserError(_('El pedimento %s aún no está despachado.', rec.name))
            rec._set_state('entregado', fecha_entrega=rec.fecha_entrega or fields.Datetime.now())

    def _motivos_no_cierre(self):
        self.ensure_one()
        motivos = []
        if not self.numero:
            motivos.append(_('falta el número de pedimento'))
        if not self.fecha_entrega:
            motivos.append(_('falta la fecha de entrega en planta o de cruce'))
        faltan = self.documento_ids.filtered(lambda d: d.required and not d.cumplido)
        if faltan:
            motivos.append(_('faltan documentos: %s', ', '.join(faltan.tipo_id.mapped('name'))))
        if not self.cuenta_gastos_total and not self.gasto_move_ids:
            motivos.append(_('falta la cuenta de gastos (total o factura de la agencia)'))
        return motivos

    def action_cerrar(self):
        for rec in self:
            motivos = rec._motivos_no_cierre()
            if motivos:
                raise UserError(_('No se puede cerrar %(n)s: %(m)s.', n=rec.name, m='; '.join(motivos)))
            rec._set_state('cerrado', fecha_cierre=rec.fecha_cierre or fields.Date.context_today(rec))
            rec.activity_ids.filtered(lambda a: a.summary and a.summary.startswith('[Pedimento]')).action_feedback(
                feedback=_('Expediente cerrado.'))

    def action_cancelar(self):
        self._set_state('cancelado')

    def action_reabrir(self):
        self._set_state('preparacion', fecha_cierre=False)

    # ── costo en destino ─────────────────────────────────────────────
    def action_crear_costo_destino(self):
        """Costo en destino en BORRADOR sobre las recepciones ligadas: IGI,
        DTA, PRV y los gastos al costo. El IVA no entra (es acreditable).
        Contabilidad revisa y valida; el módulo no crea asientos."""
        self.ensure_one()
        if self.operacion != 'importacion':
            raise UserError(_('El costo en destino aplica solo a importaciones.'))
        recepciones = self.picking_ids.filtered(lambda p: p.picking_type_code == 'incoming' and p.state == 'done')
        if not recepciones:
            raise UserError(_('Ligue al menos una recepción validada antes de crear el costo en destino.'))
        company = self.company_id
        conceptos = [
            (company.qb_pedimento_product_igi_id, self.igi, _('IGI')),
            (company.qb_pedimento_product_dta_id, self.dta, _('DTA')),
            (company.qb_pedimento_product_prv_id, self.prv, _('PRV')),
            (company.qb_pedimento_product_gastos_id, self.gastos_sin_iva, _('Gastos de importación')),
        ]
        lines = []
        for product, amount, label in conceptos:
            if not amount:
                continue
            if not product:
                raise UserError(_(
                    'Configure el producto de costo en destino para «%s» en Ajustes → Pedimentos.', label))
            lines.append(fields.Command.create({
                'product_id': product.id,
                'name': '%s %s' % (label, self.name),
                'price_unit': amount,
                'split_method': product.split_method_landed_cost or 'by_current_cost_price',
                'account_id': (product.property_account_expense_id
                               or product.categ_id.property_account_expense_categ_id).id,
            }))
        if not lines:
            raise UserError(_('Capture IGI, DTA, PRV o los gastos al costo antes de crear el costo en destino.'))
        vals = {
            'picking_ids': [fields.Command.set(recepciones.ids)],
            'cost_lines': lines,
            'qb_pedimento_id': self.id,
            'company_id': company.id,
        }
        if 'l10n_mx_edi_customs_number' in self.env['stock.landed.cost']._fields and self.numero:
            vals['l10n_mx_edi_customs_number'] = formato_nativo(self.numero)
        landed = self.env['stock.landed.cost'].create(vals)
        return {
            'type': 'ir.actions.act_window',
            'res_model': 'stock.landed.cost',
            'res_id': landed.id,
            'view_mode': 'form',
        }

    # ── botones inteligentes ─────────────────────────────────────────
    def _action_open(self, model, records, name):
        action = {'type': 'ir.actions.act_window', 'res_model': model, 'name': name,
                  'domain': [('id', 'in', records.ids)], 'view_mode': 'list,form'}
        if len(records) == 1:
            action.update(res_id=records.id, view_mode='form')
        return action

    def action_view_purchases(self):
        return self._action_open('purchase.order', self.purchase_ids, _('Compras'))

    def action_view_pickings(self):
        return self._action_open('stock.picking', self.picking_ids, _('Recepciones / entregas'))

    def action_view_invoices(self):
        return self._action_open('account.move', self.invoice_ids | self.gasto_move_ids, _('Facturas'))

    def action_view_landed_costs(self):
        return self._action_open('stock.landed.cost', self.landed_cost_ids, _('Costos en destino'))

    # ── avisos (cron) ────────────────────────────────────────────────
    def _avisar(self, user, clave, texto):
        """Una actividad por aviso y expediente: si ya hay una abierta con la
        misma clave no se repite."""
        self.ensure_one()
        resumen = '[Pedimento] %s' % clave
        if not user or self.activity_ids.filtered(lambda a: a.summary == resumen):
            return False
        self.activity_schedule('mail.mail_activity_data_todo', summary=resumen, note=texto, user_id=user.id)
        return True

    @api.model
    def _cron_avisos(self, hoy=None):
        """C2.31: despachado y sin cruce o llegada en N días hábiles.
        C2.32: entregado y sin cerrar en N días hábiles. Garantías sin
        devolver en N días naturales. Idempotente."""
        hoy = hoy or fields.Date.context_today(self)
        for company in self.env['res.company'].search([]):
            abiertos = self.search([('company_id', '=', company.id), ('state', 'in', ('despachado', 'entregado'))])
            for rec in abiertos:
                user = rec.user_id or company.qb_pedimento_responsable_id
                if rec.state == 'despachado' and rec.fecha_despacho:
                    dias = dias_habiles_entre(rec.fecha_despacho.date(), hoy)
                    if dias >= company.qb_pedimento_dias_cruce:
                        rec._avisar(user, _('Sin cruce o llegada'), _(
                            'Despachado hace %(d)s días hábiles y sin entrega en planta ni cruce confirmado. '
                            'Confirmar con la agencia y avisar hoy (C2.31).', d=dias))
                elif rec.state == 'entregado' and rec.fecha_entrega:
                    dias = dias_habiles_entre(rec.fecha_entrega.date(), hoy)
                    if dias >= company.qb_pedimento_dias_cierre:
                        motivos = rec._motivos_no_cierre()
                        rec._avisar(user, _('Expediente sin cerrar'), _(
                            'Entregado hace %(d)s días hábiles. Pendiente: %(m)s. Pedir a la agencia el pedimento '
                            'pagado y la cuenta de gastos y avisar al Contador general (C2.32).',
                            d=dias, m='; '.join(motivos) or _('cerrarlo')))
            limite = hoy - timedelta(days=company.qb_pedimento_dias_garantia)
            garantias = self.env['qb.pedimento.garantia'].search([
                ('company_id', '=', company.id), ('state', 'in', ('pendiente', 'parcial')),
                ('fecha_pago', '<=', limite)])
            for garantia in garantias:
                garantia._avisar(company)
