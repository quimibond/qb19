# -*- coding: utf-8 -*-
"""Condiciones de entrega por cliente y planta (19.0.3.19.0, Jose 19-nov-2026).

Un lugar para la presentación de entrega de cada cliente, tenga o no release: cómo y cuándo se le
embarca (día de recolección, días de adelanto, colchón, semanas congeladas, embarques cada N semanas)
y, por producto, la presentación del rollo (largo, redondeo, ancho, tubo, empalmes, peso, empaque,
etiqueta). Al aplicar un release, las reglas salen de aquí (``release/reglas.py``); «Proponer pedidos»
toma de aquí la fecha de entrega. Nada está escrito por cliente en el código.
"""
import math
from datetime import timedelta

from markupsafe import Markup

from odoo import api, fields, models
from odoo.exceptions import ValidationError

WEEKDAYS = [('0', "Lunes"), ('1', "Martes"), ('2', "Miércoles"), ('3', "Jueves"), ('4', "Viernes"),
            ('5', "Sábado"), ('6', "Domingo")]
ROLL_UNITS = [('m', "m"), ('yd', "yd"), ('kg', "kg"), ('m2', "m²")]
# Cómo se llama en Odoo la unidad del rollo (para repartir una semana entre días por rollos completos).
ROLL_UOM_NAMES = {'m': {'m', 'mt', 'mts', 'metro', 'metros'}, 'yd': {'yd', 'yds', 'yarda', 'yardas'},
                  'kg': {'kg', 'kgs', 'kilo', 'kilos', 'kilogramo', 'kilogramos'}, 'm2': {'m2', 'm²'}}


class QbWeekday(models.Model):
    """Día de la semana (3.25.0): un cliente puede recoger varios días (Lear: martes y viernes)."""
    _name = 'qb.weekday'
    _description = "Día de la semana"
    _order = 'code'

    name = fields.Char(string="Día", required=True)
    code = fields.Integer(string="Número", required=True, help="0 = lunes … 6 = domingo.")

    _code_uniq = models.Constraint('unique (code)', "Ese día ya existe.")


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
    pickup_mode = fields.Selection(
        [('libre', "Sin día fijo"), ('uno', "Un día a la semana"), ('varios', "Varios días a la semana")],
        string="Recolección", required=True, default='libre', tracking=True,
        help="Sin día fijo: la fecha de entrega del pedido es la que pide el cliente menos el colchón. Un día: "
             "ese día de la semana de embarque. Varios (Lear: martes y viernes): la semana se reparte por "
             "partes iguales y sale un pedido por día.")
    pickup_day_ids = fields.Many2many(
        'qb.weekday', string="Días de recolección", tracking=True,
        help="Días de la semana en que se embarca o el cliente recoge.")
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

    @api.constrains('pickup_mode', 'pickup_day_ids')
    def _check_pickup_days(self):
        for terms in self:
            n = len(terms.pickup_day_ids)
            if terms.pickup_mode == 'uno' and n != 1:
                raise ValidationError("«Un día a la semana» lleva exactamente un día de recolección.")
            if terms.pickup_mode == 'varios' and n < 2:
                raise ValidationError("«Varios días a la semana» lleva al menos dos días de recolección.")

    @api.onchange('pickup_mode')
    def _onchange_pickup_mode(self):
        if self.pickup_mode == 'libre':
            self.pickup_day_ids = False

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

    def _qb_open_po_for(self, product, week):
        """(OC abierta del producto, motivo si venció) — 3.24.0: un cliente con una OC abierta por
        producto (Zwisstex). ('', '') si el producto no tiene OC propia: manda la regla del cliente."""
        line = self._qb_line_for(product) if self else self.browse()
        po = (line.open_po or '').strip() if line else ''
        if not po:
            return '', ''
        if line.open_po_valid_to and week and week > line.open_po_valid_to:
            return '', "La OC abierta %s de %s venció el %s" % (
                po, product.default_code or product.display_name, line.open_po_valid_to)
        return po, ''

    def _qb_roll_for(self, product):
        """(rollo, unidad) para redondear ese producto; (0, False) si no se redondea."""
        if not self or self.closed_qty:
            return 0.0, False
        line = self._qb_line_for(product)
        if line and not line.round_up:
            return 0.0, False
        if line and line.roll_qty > 0:
            return line.roll_qty, line.roll_uom or 'yd'
        if self.roll_qty > 0:
            return self.roll_qty, self.roll_uom or 'yd'
        # 3.26.0: sin rollo del cliente, el estándar de la ficha de acabado.
        ficha = self.env['ficha.tecnica.acabado']._qb_for_product(product)
        if ficha.qb_pres_roll_qty > 0:
            return ficha.qb_pres_roll_qty, ficha.qb_pres_roll_uom or 'm'
        return 0.0, False

    def _qb_pickup_days(self):
        """Días de recolección como números (0 = lunes), en orden."""
        if not self or self.pickup_mode == 'libre':
            return []
        return sorted(self.pickup_day_ids.mapped('code'))

    def _qb_split(self, product, qty, parts):
        """3.25.0: reparte ``qty`` entre ``parts`` días por partes iguales; cada parte baja a rollos
        completos si el rollo está en la unidad del producto, si no a unidades enteras. Lo que sobra va
        al primer día."""
        if parts < 2:
            return [qty]
        roll, unit = self._qb_roll_for(product)
        uom = (product.uom_id.name or '').strip().lower()
        step = roll if roll and uom in ROLL_UOM_NAMES.get(unit, ()) else 1.0
        each = math.floor(qty / parts / step + 1e-9) * step
        split = [each] * parts
        split[0] = round(qty - each * (parts - 1), 2)
        return split

    def _qb_delivery_date(self, customer_date, week):
        """Fecha de entrega del pedido: el (primer) día de recolección de la semana de embarque o, sin él,
        la fecha del cliente menos el colchón. Sin condiciones, la del cliente (o el lunes)."""
        if not self:
            return customer_date or week
        days = self._qb_pickup_days()
        if days and week:
            return week + timedelta(days=days[0])
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
    # 3.26.0: lo que el cliente pide distinto de la ficha de acabado. Vacío (0, -1) = el del cliente
    # (encabezado) y, si tampoco, el estándar de la ficha.
    roll_qty = fields.Float(string="Largo del rollo", help="0 = el del cliente o, si no, el de la ficha.")
    roll_uom = fields.Selection(ROLL_UNITS, string="Unidad", default='yd')
    round_up = fields.Boolean(string="Redondear a rollos", default=True,
                              help="Sube cada semana del release al múltiplo del rollo (505 → 1000).")
    core = fields.Char(string="Tubo / core", help="Vacío = el de la ficha. 3\" cartón, 6\" PVC…")
    max_splices = fields.Integer(string="Empalmes máx.", default=-1,
                                 help="-1 = el del cliente o, si no, el de la ficha.")
    min_piece = fields.Float(string="Tramo mínimo entre empalmes",
                             help="En la unidad del rollo. 0 = el de la ficha.")
    max_weight_kg = fields.Float(string="Peso máx. por rollo (kg)", help="0 = el de la ficha.")
    rolls_per_pallet = fields.Integer(string="Rollos por tarima", help="0 = el de la ficha.")
    packaging = fields.Char(string="Empaque", help="Vacío = el de la ficha. Emplayado, bolsa, tarima…")
    spec_text = fields.Char(
        string="Especificación (ficha)", compute='_compute_ficha_text',
        help="Ficha de acabado del producto: ancho de venta, peso y espesor con sus tolerancias. Se cambia en la "
             "ficha, no aquí.")
    ficha_pres_text = fields.Char(
        string="Presentación de la ficha", compute='_compute_ficha_text',
        help="La presentación estándar de la ficha de acabado: lo que se entrega si el cliente no pide otra cosa.")
    own_tolerance = fields.Boolean(
        string="Tolerancia del cliente",
        help="El cliente exige una tolerancia distinta de la ficha (más cerrada, por ejemplo en automotriz).")
    width_min_cm = fields.Float(string="Ancho mín. (cm)")
    width_max_cm = fields.Float(string="Ancho máx. (cm)")
    weight_min = fields.Float(string="Peso mín. (g/m²)")
    weight_max = fields.Float(string="Peso máx. (g/m²)")
    label = fields.Char(string="Etiqueta")
    notes = fields.Char(string="Notas")
    open_po = fields.Char(
        string="OC abierta",
        help="Para el cliente que tiene una OC abierta por producto (Zwisstex): los pedidos de este "
             "producto llevan esta OC. Vacía = la OC de la regla del cliente o de la planta.")
    open_po_valid_to = fields.Date(string="Vigencia de la OC",
                                   help="Vacía = sin fecha de fin. Vencida, el producto ya no se pide.")
    company_id = fields.Many2one(related='terms_id.company_id', store=True)

    _line_uniq = models.Constraint('unique (terms_id, product_id)',
                                   "Ese producto ya tiene su presentación en estas condiciones.")

    @staticmethod
    def _qb_fmt(value):
        return ('%.3f' % value).rstrip('0').rstrip('.')

    @api.depends('product_id')
    def _compute_ficha_text(self):
        Ficha = self.env['ficha.tecnica.acabado']
        fmt = self._qb_fmt
        for line in self:
            ficha = Ficha._qb_for_product(line.product_id)
            spec = []
            for label, nominal, plus, minus, unit in (
                    ("Ancho", 'ancho_acabado', 'ancho_acabado_tol', 'ancho_acabado_tol_minus', 'ancho_acabado_tol_unit'),
                    ("Peso", 'peso_acabado', 'peso_acabado_tol', 'peso_acabado_tol_minus', 'peso_acabado_tol_unit'),
                    ("Espesor", 'espesor_acabado', 'espesor_acabado_tol', 'espesor_acabado_tol_minus',
                     'espesor_acabado_tol_unit')):
                value = ficha[nominal] if ficha else 0.0
                if not value:
                    continue
                up, down = ficha[plus], ficha[minus]
                tol = (" ±%s" % fmt(up) if up == down else " +%s/−%s" % (fmt(up), fmt(down))) if (up or down) else ""
                spec.append("%s %s%s %s" % (label, fmt(value), tol, ficha[unit] or ""))
            line.spec_text = " · ".join(s.strip() for s in spec) or (False if ficha else "Sin ficha de acabado")
            pres = []
            if ficha:
                if ficha.qb_pres_roll_qty:
                    pres.append("Rollo %s %s" % (fmt(ficha.qb_pres_roll_qty), ficha.qb_pres_roll_uom or ''))
                if ficha.qb_pres_core:
                    pres.append("tubo %s" % ficha.qb_pres_core)
                if ficha.qb_pres_max_splices >= 0:
                    pres.append("%s empalmes" % ficha.qb_pres_max_splices)
                if ficha.qb_pres_min_piece:
                    pres.append("tramo mín. %s" % fmt(ficha.qb_pres_min_piece))
                if ficha.qb_pres_max_weight_kg:
                    pres.append("máx. %s kg" % fmt(ficha.qb_pres_max_weight_kg))
                if ficha.qb_pres_rolls_per_pallet:
                    pres.append("%s por tarima" % ficha.qb_pres_rolls_per_pallet)
                if ficha.qb_pres_packaging:
                    pres.append(ficha.qb_pres_packaging)
            line.ficha_pres_text = " · ".join(pres) or False

    _PO_FIELDS = ('open_po', 'open_po_valid_to')

    @api.model_create_multi
    def create(self, vals_list):
        lines = super().create(vals_list)
        for line in lines.filtered('open_po'):
            line._qb_post_po(False, False)
        return lines

    def write(self, vals):
        before = {line.id: (line.open_po, line.open_po_valid_to) for line in self} \
            if any(f in vals for f in self._PO_FIELDS) else {}
        res = super().write(vals)
        for line in self.filtered(lambda l: l.id in before):
            old_po, old_to = before[line.id]
            if (old_po or False, old_to or False) != (line.open_po or False, line.open_po_valid_to or False):
                line._qb_post_po(old_po, old_to)
        return res

    def _qb_post_po(self, old_po, old_to):
        """El cambio de OC queda en el historial de las condiciones (el renglón no tiene chatter)."""
        def show(po, to):
            return "%s%s" % (po or "—", " (vigente al %s)" % to if to else "")
        self.terms_id.message_post(body=Markup("OC abierta de %s: %s → %s.") % (
            self.product_id.display_name, show(old_po, old_to), show(self.open_po, self.open_po_valid_to)))
