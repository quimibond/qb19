# -*- coding: utf-8 -*-
"""Condiciones de entrega por cliente y planta (19.0.3.19.0, Jose 19-nov-2026).

Un lugar para la presentación de entrega de cada cliente, tenga o no release: cómo y cuándo se le
embarca (día de recolección, días de adelanto, colchón, semanas congeladas, embarques cada N semanas)
y, por producto, la presentación del rollo (largo, redondeo, ancho, tubo, empalmes, peso, empaque,
etiqueta). Al aplicar un release, las reglas salen de aquí (``release/reglas.py``); «Proponer pedidos»
toma de aquí la fecha de entrega. Nada está escrito por cliente en el código.
"""
from datetime import timedelta

from odoo import api, fields, models

WEEKDAYS = [('0', "Lunes"), ('1', "Martes"), ('2', "Miércoles"), ('3', "Jueves"), ('4', "Viernes"),
            ('5', "Sábado"), ('6', "Domingo")]
ROLL_UNITS = [('m', "m"), ('yd', "yd"), ('kg', "kg"), ('m2', "m²")]


class QbDeliveryTerms(models.Model):
    _name = 'qb.delivery.terms'
    _description = "Condiciones de entrega del cliente"
    _inherit = ['mail.thread']
    _order = 'partner_id, plant, ship_to_id'

    name = fields.Char(string="Nombre", compute='_compute_name', store=True)
    partner_id = fields.Many2one('res.partner', string="Cliente", required=True, index=True,
                                 domain="[('is_company', '=', True)]", tracking=True)
    ship_to_id = fields.Many2one('res.partner', string="Planta / dirección de entrega", tracking=True,
                                 domain="[('id', 'child_of', partner_id)]")
    plant = fields.Char(string="Planta (pronóstico)", tracking=True,
                        help="Como la escribe el pronóstico mientras no exista como dirección de entrega. "
                             "Vacía y sin dirección = todas las plantas del cliente.")
    company_id = fields.Many2one('res.company', required=True, default=lambda self: self.env.company)
    active = fields.Boolean(default=True)

    # Cuándo se embarca
    pickup_weekday = fields.Selection(
        WEEKDAYS, string="Día de recolección", tracking=True,
        help="Día de la semana en que se embarca o el cliente recoge. Es la fecha de entrega del pedido "
             "dentro de la semana de embarque.")
    lead_days = fields.Integer(
        string="Días hábiles de adelanto", tracking=True,
        help="Días hábiles (sin sábados ni domingos) antes de la fecha en que el cliente necesita el "
             "material en su planta que se embarca (una semana = 5). Lo usa el release cuando sus fechas "
             "son de entrega en planta y el perfil no trae días de tránsito.")
    buffer_days = fields.Integer(
        string="Colchón (días)", default=1, tracking=True,
        help="Sin día de recolección, la fecha de entrega del pedido es la del cliente menos estos días: "
             "un atraso de un día es interno y no un cambio con el cliente.")
    frozen_weeks = fields.Integer(
        string="Semanas congeladas", default=3, tracking=True,
        help="Semanas desde la semana en curso que Planeación ya programó: al aplicar el release se quedan "
             "como estaban y lo que el cliente sube o baja pasa a la semana siguiente (la 4 con 3); una baja "
             "que no cabe sigue a la 5 y las demás. 0 = sin congelar.")
    group_weeks = fields.Integer(
        string="Semanas por embarque", default=1, tracking=True,
        help="Embarques cada N semanas: la demanda de cada bloque se suma en su primera semana. 1 = cada "
             "semana.")
    group_start = fields.Date(
        string="Primer embarque de los bloques", tracking=True,
        help="Lunes de una semana con embarque; los bloques se cuentan desde aquí para que no se recorran. "
             "Vacío = la primera semana después de las congeladas.")
    closed_qty = fields.Boolean(
        string="Manda cantidades cerradas", tracking=True,
        help="El cliente pide en múltiplos de su presentación: no se redondea.")
    capacity_m = fields.Float(
        string="Capacidad asignada por semana (m)", digits=(16, 0),
        help="Opcional: tope de metros por semana para este cliente (informativo).")

    # Presentación por omisión
    roll_qty = fields.Float(string="Rollo", tracking=True,
                            help="Largo del rollo para todos sus productos (un renglón del producto gana). "
                                 "Las cantidades del release se suben al múltiplo del rollo.")
    roll_uom = fields.Selection(ROLL_UNITS, string="Unidad del rollo", default='yd')
    max_splices = fields.Integer(string="Empalmes máximos por rollo",
                                 help="Para todos sus productos; un renglón del producto gana. -1 = sin límite.",
                                 default=-1)
    required_docs = fields.Text(string="Documentos del embarque",
                                help="Certificado de calidad, packing list, factura en el embarque…")
    label_notes = fields.Text(string="Etiquetado")
    notes = fields.Html(string="Notas")
    line_ids = fields.One2many('qb.delivery.terms.line', 'terms_id', string="Presentación por producto")

    _terms_uniq = models.Constraint(
        'unique nulls not distinct (company_id, partner_id, ship_to_id, plant)',
        "Ya hay condiciones de entrega para ese cliente y planta.")

    @api.depends('partner_id', 'ship_to_id', 'plant')
    def _compute_name(self):
        for terms in self:
            where = terms.ship_to_id.name or terms.plant
            terms.name = "%s — %s" % (terms.partner_id.name, where) if where else (terms.partner_id.name or "")

    @api.model
    def _qb_for(self, partner, ship_to=None, plant=None, company=None):
        """Las condiciones de ese cliente y planta; si no hay, las del cliente sin planta."""
        if not partner:
            return self.browse()
        commercial = partner.commercial_partner_id
        company = company or self.env.company
        base = [('partner_id', '=', commercial.id), ('company_id', '=', company.id)]
        terms = self.browse()
        if ship_to:
            terms = self.search(base + [('ship_to_id', '=', ship_to.id)], limit=1)
        if not terms and plant:
            terms = self.search(base + [('ship_to_id', '=', False), ('plant', '=ilike', plant)], limit=1)
        if not terms:
            terms = self.search(base + [('ship_to_id', '=', False), ('plant', '=', False)], limit=1)
        return terms

    def _qb_line_for(self, product):
        """Renglón del producto (o de su artículo: la misma tela en metros o kilos)."""
        self.ensure_one()
        articulo = product.qb_articulo_id or product
        exact = self.line_ids.filtered(lambda l: l.product_id == product)
        return exact[:1] or self.line_ids.filtered(
            lambda l: (l.product_id.qb_articulo_id or l.product_id) == articulo)[:1]

    def _qb_roll_for(self, product):
        """(rollo, unidad) para redondear ese producto; (0, False) si no se redondea."""
        if not self or self.closed_qty:
            return 0.0, False
        line = self._qb_line_for(product)
        if line and not line.round_up:
            return 0.0, False
        if line and line.roll_qty > 0:
            return line.roll_qty, line.roll_uom or 'yd'
        return (self.roll_qty, self.roll_uom or 'yd') if self.roll_qty > 0 else (0.0, False)

    def _qb_delivery_date(self, customer_date, week):
        """Fecha de entrega del pedido: el día de recolección de la semana de embarque o, sin él, la fecha
        del cliente menos el colchón. Sin condiciones, la del cliente (o el lunes)."""
        if not self:
            return customer_date or week
        if self.pickup_weekday and week:
            return week + timedelta(days=int(self.pickup_weekday))
        if customer_date:
            return customer_date - timedelta(days=max(self.buffer_days or 0, 0))
        return week


class QbDeliveryTermsLine(models.Model):
    _name = 'qb.delivery.terms.line'
    _description = "Presentación de entrega por producto"
    _order = 'terms_id, product_id'

    terms_id = fields.Many2one('qb.delivery.terms', required=True, ondelete='cascade', index=True)
    partner_id = fields.Many2one(related='terms_id.partner_id', store=True, string="Cliente")
    product_id = fields.Many2one('product.product', string="Producto", required=True, index=True)
    customer_part = fields.Char(string="Parte del cliente")
    roll_qty = fields.Float(string="Largo del rollo", help="0 = el del cliente.")
    roll_uom = fields.Selection(ROLL_UNITS, string="Unidad", default='yd')
    round_up = fields.Boolean(string="Redondear a rollos", default=True,
                              help="Sube cada semana del release al múltiplo del rollo (505 → 1000).")
    width_m = fields.Float(string="Ancho (m)", digits=(16, 3))
    core = fields.Char(string="Tubo / core", help="Diámetro y material: 3\" cartón, 6\" PVC…")
    max_splices = fields.Integer(string="Empalmes máx.", default=-1, help="-1 = el del cliente.")
    min_piece = fields.Float(string="Tramo mínimo entre empalmes",
                             help="En la unidad del rollo.")
    max_weight_kg = fields.Float(string="Peso máx. por rollo (kg)")
    rolls_per_pallet = fields.Integer(string="Rollos por tarima")
    packaging = fields.Char(string="Empaque", help="Emplayado, bolsa, tarima, esquineros…")
    label = fields.Char(string="Etiqueta")
    notes = fields.Char(string="Notas")
    company_id = fields.Many2one(related='terms_id.company_id', store=True)

    _line_uniq = models.Constraint('unique (terms_id, product_id)',
                                   "Ese producto ya tiene su presentación en estas condiciones.")
