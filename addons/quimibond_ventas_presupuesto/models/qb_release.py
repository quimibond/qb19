# -*- coding: utf-8 -*-
"""Releases de clientes: perfil por cliente, el release (una versión por
documento recibido), sus partes y sus semanas, y su aplicación al pronóstico.

Flujo (E1a): se sube el archivo → «Leer» (lector determinístico del perfil)
→ Ventas revisa → «Aplicar»: reescribe el pronóstico del cliente en las
semanas que cubre el release (repartido por año) y recalcula el MPS. El
release anterior del mismo perfil queda «reemplazado» (comparable). Un
release se lee completo o queda «por revisar» con el motivo; se aplica
completo o no se aplica (una transacción).

El buzón, el acuse, las diferencias y el CUM son E1b.
"""
import base64
from collections import defaultdict
from datetime import timedelta

from odoo import api, fields, models
from odoo.exceptions import UserError

from ..release import fxi_sum, lear_aiag

_READERS = [
    ('lear_aiag', "Lear — texto AIAG (Supplier Schedule / Material Release)"),
    ('fxi_sum', "FXI — Excel SUM (reporte SAP)"),
]


def _monday(day):
    return day - timedelta(days=day.weekday())


class QbReleaseProfile(models.Model):
    _name = 'qb.release.profile'
    _description = "Perfil de release por cliente"
    _inherit = ['mail.thread']
    _order = 'partner_id, ship_to_id'

    name = fields.Char(string="Nombre", compute='_compute_name', store=True)
    partner_id = fields.Many2one('res.partner', string="Cliente", required=True,
                                 domain="[('is_company', '=', True)]", tracking=True)
    ship_to_id = fields.Many2one('res.partner', string="Planta / ship-to",
                                 domain="[('id', 'child_of', partner_id)]", tracking=True)
    supplier_code = fields.Char(string="Nuestro número de proveedor", tracking=True,
                                help="Como nos identifica el cliente (Lear 6PIN0010, FXI 105925).")
    team_id = fields.Many2one('crm.team', string="Mercado", required=True, tracking=True,
                              help="Equipo del pronóstico que se crea al aplicar.")
    reader = fields.Selection(_READERS, string="Lector", required=True, tracking=True)
    date_basis = fields.Selection([
        ('embarque', "Fecha de embarque"),
        ('entrega', "Fecha de entrega en planta del cliente"),
    ], string="Las fechas del release son", required=True, default='embarque',
        tracking=True)
    transit_days = fields.Integer(string="Días de tránsito", tracking=True,
                                  help="Si el release trae fechas de entrega, se "
                                       "restan para obtener la semana de embarque.")
    firm_rule = fields.Selection([
        ('semanas', "N semanas desde la fecha del release"),
        ('fab_auth', "Hasta la autorización de producto terminado (Fab)"),
    ], string="Zona firme", required=True, default='semanas', tracking=True)
    firm_weeks = fields.Integer(string="Semanas firmes", default=3, tracking=True)
    so_weeks = fields.Integer(
        string="Semanas a pedido", default=4, tracking=True,
        help="Semanas de la zona firme que se proponen como pedido de venta (E2). "
             "El resto queda como pronóstico / autorización.")
    ack_required = fields.Boolean(string="Exige acuse", tracking=True)
    ack_hours = fields.Integer(string="Plazo del acuse (h)", default=24)
    cum_managed = fields.Boolean(string="Maneja acumulados (CUM)", tracking=True)
    cum_reset_date = fields.Date(string="Reinicio del CUM", tracking=True)
    sales_user_id = fields.Many2one('res.users', string="Responsable en Ventas",
                                    tracking=True)
    logistics_user_id = fields.Many2one('res.users', string="Responsable en Logística")
    sender_domains = fields.Char(
        string="Dominios del remitente",
        help="Separados por coma (lear.com, fxi.com): así se reconoce el correo (E1b).")
    company_id = fields.Many2one('res.company', required=True,
                                 default=lambda self: self.env.company)
    active = fields.Boolean(default=True)
    release_ids = fields.One2many('qb.release', 'profile_id', string="Releases")

    _profile_uniq = models.Constraint(
        'unique nulls not distinct (company_id, partner_id, ship_to_id)',
        "Ya hay un perfil de release para ese cliente y planta.")

    @api.depends('partner_id', 'ship_to_id')
    def _compute_name(self):
        for profile in self:
            name = profile.partner_id.name or ''
            if profile.ship_to_id:
                name = "%s — %s" % (name, profile.ship_to_id.name)
            profile.name = name


class QbRelease(models.Model):
    _name = 'qb.release'
    _description = "Release de cliente"
    _inherit = ['mail.thread', 'mail.activity.mixin']
    _order = 'release_date desc, id desc'

    name = fields.Char(string="Release", compute='_compute_name', store=True)
    profile_id = fields.Many2one('qb.release.profile', string="Perfil", required=True,
                                 index=True, tracking=True)
    partner_id = fields.Many2one(related='profile_id.partner_id', store=True,
                                 string="Cliente")
    company_id = fields.Many2one(related='profile_id.company_id', store=True)
    release_ref = fields.Char(string="Release ID", tracking=True)
    release_date = fields.Date(string="Fecha del release", tracking=True)
    version = fields.Integer(string="Versión", readonly=True, copy=False)
    previous_id = fields.Many2one('qb.release', string="Release anterior",
                                  readonly=True, copy=False)
    file = fields.Binary(string="Archivo", attachment=True, copy=False)
    filename = fields.Char(string="Nombre del archivo")
    state = fields.Selection([
        ('recibido', "Recibido"),
        ('por_revisar', "Por revisar"),
        ('leido', "Leído"),
        ('revisado', "Revisado por Ventas"),
        ('aplicado', "Aplicado"),
        ('reemplazado', "Reemplazado"),
        ('descartado', "Descartado"),
    ], string="Estado", default='recibido', required=True, tracking=True, copy=False)
    error_reason = fields.Text(string="Motivo", readonly=True, copy=False)
    part_ids = fields.One2many('qb.release.part', 'release_id', string="Partes",
                               copy=False)
    line_ids = fields.One2many('qb.release.line', 'release_id', string="Semanas",
                               copy=False)
    forecast_ids = fields.Many2many('sgi.sales.budget', string="Pronósticos escritos",
                                    readonly=True, copy=False)
    applied_date = fields.Datetime(string="Aplicado el", readonly=True, copy=False)
    applied_by_id = fields.Many2one('res.users', string="Aplicó", readonly=True,
                                    copy=False)
    unmapped_count = fields.Integer(string="Partes sin producto confirmado",
                                    compute='_compute_unmapped_count')

    @api.depends('profile_id', 'release_ref', 'release_date')
    def _compute_name(self):
        for release in self:
            parts = [release.profile_id.partner_id.name or "Release"]
            if release.release_ref:
                parts.append(release.release_ref)
            if release.release_date:
                parts.append(fields.Date.to_string(release.release_date))
            release.name = " · ".join(parts)

    @api.depends('part_ids.catalog_part_id.state')
    def _compute_unmapped_count(self):
        for release in self:
            release.unmapped_count = len(release.part_ids.filtered(
                lambda p: p.catalog_part_id.state != 'confirmado'))

    # --- Lectura ----------------------------------------------------------------
    def _qb_parse_file(self):
        """Archivo → lista de partes normalizadas (mismo formato para todos los
        lectores). Levanta ValueError (LearReleaseError / FxiReleaseError) si no
        se puede leer."""
        self.ensure_one()
        if not self.file:
            raise ValueError("El release no tiene archivo.")
        content = base64.b64decode(self.file)
        reader = self.profile_id.reader
        if reader == 'lear_aiag':
            text = content.decode('utf-8', errors='replace')
            text = self._qb_eml_body(text)
            data = lear_aiag.parse(text)
            return [{
                'release_ref': p['release_ref'], 'release_date': p['release_date'],
                'customer_part': p['customer_part'], 'description': p.get('description'),
                'customer_po': p.get('customer_po'), 'uom': p['uom'],
                'cum_received': p.get('cum_received', 0.0),
                'in_transit_qty': p.get('in_transit_qty', 0.0),
                'last_receipt_date': p.get('last_receipt_date'),
                'last_receipt_qty': p.get('last_receipt_qty', 0.0),
                'packing_slip': p.get('packing_slip'),
                'fab_auth_qty': p.get('fab_auth_qty', 0.0),
                'fab_auth_date': p.get('fab_auth_date'),
                'raw_auth_qty': p.get('raw_auth_qty', 0.0),
                'raw_auth_date': p.get('raw_auth_date'),
                'lines': [{'date': ln['date'], 'qty': ln['req_qty'],
                           'line_type': ln['qty_type'], 'cum_req': ln['cum_req_qty'],
                           'net_req': ln['net_req_qty']} for ln in p['lines']],
            } for p in data['parts']]
        if reader == 'fxi_sum':
            data = fxi_sum.parse_xlsx(content)
            return [{
                'release_ref': p['release_ref'], 'release_date': p['release_date'],
                'customer_part': p['customer_part'], 'description': p.get('description'),
                'customer_po': p.get('agreement'), 'uom': p['uom'],
                'cum_received': p.get('raw_accum_rcvd', 0.0),
                'vendor_auth_qty': p.get('vendor_auth_qty', 0.0),
                'lines': [{'date': ln['date'], 'qty': ln['gross_need'],
                           'line_type': 'P', 'cum_req': ln['cum'], 'net_req': 0.0}
                          for ln in p['lines']],
            } for p in data['parts']]
        raise ValueError("El perfil no tiene un lector conocido.")

    @staticmethod
    def _qb_eml_body(text):
        """Si el archivo es un .eml, devuelve su cuerpo de texto."""
        if 'MATERIAL RELEASE' in text.upper() and 'Content-Type' not in text[:2000]:
            return text
        import email
        from email import policy
        message = email.message_from_string(text, policy=policy.default)
        body = message.get_body(preferencelist=('plain',))
        return body.get_content() if body else text

    def action_read(self):
        """Lee el archivo con el lector del perfil. Todo o nada: si algo falla, el
        release queda «por revisar» con el motivo y sin partes."""
        for release in self:
            if release.state not in ('recibido', 'por_revisar', 'leido'):
                raise UserError("Solo se lee un release recibido, por revisar o leído.")
            release.part_ids.unlink()
            try:
                parts = release._qb_parse_file()
            except (ValueError, UnicodeDecodeError) as exc:
                release.write({'state': 'por_revisar', 'error_reason': str(exc)})
                release.message_post(body="No se pudo leer: %s" % exc)
                continue
            refs = {p['release_ref'] for p in parts}
            dates = {p['release_date'] for p in parts if p['release_date']}
            release._qb_create_parts(parts)
            release.write({
                'state': 'leido', 'error_reason': False,
                'release_ref': ", ".join(sorted(r for r in refs if r)),
                'release_date': max(dates) if dates else False,
            })
            release._qb_link_previous()
            if release.unmapped_count:
                release.message_post(
                    body="Leído. %d parte(s) sin producto confirmado en el catálogo: "
                         "confírmalas en Ventas → Presupuesto y pronóstico → Partes del "
                         "cliente antes de aplicar." % release.unmapped_count)
        return True

    def _qb_create_parts(self, parts):
        self.ensure_one()
        Part = self.env['qb.release.part']
        for data in parts:
            catalog = self._qb_catalog_part(data)
            part = Part.create({
                'release_id': self.id,
                'catalog_part_id': catalog.id,
                'customer_part': data['customer_part'],
                'description': data.get('description'),
                'customer_po': data.get('customer_po'),
                'customer_uom': data['uom'],
                'cum_received': data.get('cum_received', 0.0),
                'in_transit_qty': data.get('in_transit_qty', 0.0),
                'last_receipt_date': data.get('last_receipt_date'),
                'last_receipt_qty': data.get('last_receipt_qty', 0.0),
                'packing_slip': data.get('packing_slip'),
                'fab_auth_qty': data.get('fab_auth_qty', 0.0),
                'fab_auth_date': data.get('fab_auth_date'),
                'raw_auth_qty': data.get('raw_auth_qty', 0.0),
                'raw_auth_date': data.get('raw_auth_date'),
                'vendor_auth_qty': data.get('vendor_auth_qty', 0.0),
            })
            self.env['qb.release.line'].create([{
                'release_part_id': part.id,
                'date_customer': line['date'],
                'qty_customer': line['qty'],
                'line_type': line.get('line_type') or 'P',
                'cum_req': line.get('cum_req', 0.0),
                'net_req': line.get('net_req', 0.0),
            } for line in data['lines']])

    def _qb_catalog_part(self, data):
        """Parte del catálogo para (cliente, planta, parte). Si no existe se crea
        y se le sugiere producto; nunca se confirma sola."""
        self.ensure_one()
        profile = self.profile_id
        Catalog = self.env['qb.customer.part']
        domain = [('partner_id', '=', profile.partner_id.id),
                  ('customer_part', '=', data['customer_part']),
                  ('company_id', '=', profile.company_id.id)]
        catalog = Catalog.search(domain + [('ship_to_id', '=', profile.ship_to_id.id)],
                                 limit=1) \
            or Catalog.search(domain + [('ship_to_id', '=', False)], limit=1)
        if not catalog:
            catalog = Catalog.create({
                'partner_id': profile.partner_id.id,
                'ship_to_id': profile.ship_to_id.id,
                'customer_part': data['customer_part'],
                'customer_description': data.get('description'),
                'customer_po': data.get('customer_po'),
                'customer_uom': data['uom'],
                'company_id': profile.company_id.id,
            })
            catalog.action_suggest_product()
        return catalog

    def _qb_link_previous(self):
        """Versión y release anterior (el último aplicado del mismo perfil)."""
        for release in self:
            previous = self.search([
                ('profile_id', '=', release.profile_id.id),
                ('id', '!=', release.id),
                ('state', 'in', ('aplicado', 'reemplazado')),
            ], order='release_date desc, id desc', limit=1)
            last_version = max(release.profile_id.release_ids.mapped('version') or [0])
            vals = {'previous_id': previous.id}
            if not release.version:
                vals['version'] = last_version + 1
            release.write(vals)

    # --- Revisión y aplicación ----------------------------------------------------
    def action_mark_reviewed(self):
        for release in self:
            if release.state != 'leido':
                raise UserError("Solo se marca revisado un release leído.")
            release.state = 'revisado'
        return True

    def action_discard(self):
        for release in self:
            if release.state == 'aplicado':
                raise UserError("Un release aplicado no se descarta: aplica el "
                                "siguiente y este quedará reemplazado.")
            release.state = 'descartado'
        return True

    def _qb_check_applicable(self):
        self.ensure_one()
        if self.state != 'revisado':
            raise UserError("Solo se aplica un release revisado por Ventas.")
        problems = []
        for part in self.part_ids:
            if part.catalog_part_id.state != 'confirmado':
                problems.append("%s: sin producto confirmado en el catálogo"
                                % part.customer_part)
                continue
            bad = part.line_ids.filtered(lambda ln: not ln.qty_ok)
            if bad:
                problems.append("%s: %d semana(s) sin conversión de %s a %s (falta "
                                "rendimiento m/kg en la ficha o factor fijo)" % (
                                    part.customer_part, len(bad), part.customer_uom,
                                    part.product_id.uom_id.name))
        if problems:
            raise UserError("No se aplica el release %s:\n- %s" % (
                self.name, "\n- ".join(problems)))

    def _qb_forecast_for(self, year):
        """Pronóstico vigente del cliente para ese año; se crea si no hay."""
        self.ensure_one()
        profile = self.profile_id
        partner = profile.partner_id.commercial_partner_id
        Budget = self.env['sgi.sales.budget']
        forecast = Budget.search([
            ('kind', '=', 'pronostico'), ('partner_id', '=', partner.id),
            ('year', '=', year), ('company_id', '=', profile.company_id.id),
            ('state', '!=', 'obsoleto')], limit=1)
        if not forecast:
            forecast = Budget.create({
                'kind': 'pronostico', 'partner_id': partner.id, 'year': year,
                'team_id': profile.team_id.id, 'company_id': profile.company_id.id,
            })
        return forecast

    def action_apply(self):
        """Escribe el release en el pronóstico del cliente y recalcula el MPS.

        Por producto y semana de embarque, la cantidad del pronóstico = la del
        release (suma de partes del mismo producto). Desde la primera semana del
        release en adelante, las semanas de esos productos que el release ya no
        trae quedan en 0; las semanas anteriores no se tocan. Si el release
        cruza de año, escribe en el pronóstico de cada año (N3)."""
        Line = self.env['sgi.sales.budget.line']
        for release in self:
            release._qb_check_applicable()
            partner = release.profile_id.partner_id.commercial_partner_id
            demand = defaultdict(float)
            for line in release.line_ids:
                demand[(line.product_id, line.week)] += line.qty
            start = min(week for (_p, week) in demand)
            products = release.line_ids.product_id
            by_year = defaultdict(dict)
            for (product, week), qty in demand.items():
                by_year[week.year][(product, week)] = qty
            forecasts = self.env['sgi.sales.budget']
            for year in sorted(set(by_year) | {start.year}):
                forecast = release._qb_forecast_for(year)
                forecasts |= forecast
                cells = by_year.get(year, {})
                existing = {(ln.product_id, ln.date): ln for ln in forecast.line_ids
                            if ln.product_id in products and ln.date >= start}
                for key, line in existing.items():
                    if key not in cells:
                        line.write({'qty_budget': 0.0, 'release_id': release.id,
                                    'forecast_source': 'release'})
                for (product, week), qty in cells.items():
                    vals = {'qty_budget': qty, 'release_id': release.id,
                            'forecast_source': 'release'}
                    line = existing.get((product, week))
                    if line:
                        line.write(vals)
                    else:
                        vals.update(budget_id=forecast.id, product_id=product.id,
                                    date=week, uom_id=product.uom_id.id,
                                    partner_id=partner.id)
                        Line.create(vals)
                # El release ya lo revisó Ventas: el pronóstico queda revisado
                # (editar sus líneas lo regresó a borrador).
                if forecast.state != 'revisado':
                    forecast.state = 'revisado'
                forecast.message_post(body="Actualizado desde el release %s (versión %s)."
                                      % (release.name, release.version))
            previous = self.search([
                ('profile_id', '=', release.profile_id.id),
                ('state', '=', 'aplicado'), ('id', '!=', release.id)])
            previous.write({'state': 'reemplazado'})
            release.write({
                'state': 'aplicado', 'forecast_ids': [(6, 0, forecasts.ids)],
                'applied_date': fields.Datetime.now(), 'applied_by_id': self.env.user.id,
            })
            details, _omitted = self.env['sgi.sales.budget']._qb_mps_refresh(
                products, release.company_id)
            release.message_post(
                body="Aplicado a %s. Programa Maestro: %d celda(s) recalculadas." % (
                    ", ".join(forecasts.mapped('name')), len(details)))
        return True


class QbReleasePart(models.Model):
    _name = 'qb.release.part'
    _description = "Parte de un release"
    _order = 'release_id, customer_part'

    release_id = fields.Many2one('qb.release', required=True, ondelete='cascade',
                                 index=True)
    catalog_part_id = fields.Many2one('qb.customer.part', string="Parte del catálogo",
                                      required=True)
    product_id = fields.Many2one(related='catalog_part_id.product_id', string="Producto")
    mapping_state = fields.Selection(related='catalog_part_id.state',
                                     string="Catálogo")
    customer_part = fields.Char(string="Parte del cliente", required=True)
    description = fields.Char(string="Descripción del cliente")
    customer_po = fields.Char(string="PO / contrato")
    customer_uom = fields.Char(string="Unidad del cliente")
    cum_received = fields.Float(string="CUM recibido (cliente)")
    in_transit_qty = fields.Float(string="En tránsito")
    last_receipt_date = fields.Date(string="Última recepción")
    last_receipt_qty = fields.Float(string="Cantidad última recepción")
    packing_slip = fields.Char(string="Último packing slip")
    fab_auth_qty = fields.Float(string="Autorización Fab (CUM)")
    fab_auth_date = fields.Date(string="Fab hasta")
    raw_auth_qty = fields.Float(string="Autorización Raw (CUM)")
    raw_auth_date = fields.Date(string="Raw hasta")
    vendor_auth_qty = fields.Float(string="Autorización del proveedor")
    line_ids = fields.One2many('qb.release.line', 'release_part_id', string="Semanas")


class QbReleaseLine(models.Model):
    _name = 'qb.release.line'
    _description = "Semana de un release"
    _order = 'release_part_id, date_customer'

    release_part_id = fields.Many2one('qb.release.part', required=True,
                                      ondelete='cascade', index=True)
    release_id = fields.Many2one(related='release_part_id.release_id', store=True,
                                 index=True)
    product_id = fields.Many2one(related='release_part_id.product_id', store=True,
                                 string="Producto")
    date_customer = fields.Date(string="Fecha del cliente", required=True)
    date_ship = fields.Date(string="Fecha de embarque", compute='_compute_dates',
                            store=True)
    week = fields.Date(string="Semana (lunes)", compute='_compute_dates', store=True)
    qty_customer = fields.Float(string="Cantidad (unidad del cliente)")
    qty = fields.Float(string="Cantidad (unidad del producto)",
                       compute='_compute_qty', store=True, digits='Product Unit')
    qty_ok = fields.Boolean(string="Convertida", compute='_compute_qty', store=True)
    line_type = fields.Char(string="Tipo (cliente)", help="P = planeado, F = firme…")
    zone = fields.Selection([
        ('firme', "Firme"),
        ('materia_prima', "Materia prima autorizada"),
        ('planeado', "Planeado"),
    ], string="Zona", compute='_compute_zone', store=True)
    cum_req = fields.Float(string="CUM requerido")
    net_req = fields.Float(string="Neto requerido")

    @api.depends('date_customer', 'release_part_id.release_id.profile_id.date_basis',
                 'release_part_id.release_id.profile_id.transit_days')
    def _compute_dates(self):
        for line in self:
            profile = line.release_part_id.release_id.profile_id
            ship = line.date_customer
            if ship and profile.date_basis == 'entrega' and profile.transit_days:
                ship = ship - timedelta(days=profile.transit_days)
            line.date_ship = ship
            line.week = _monday(ship) if ship else False

    @api.depends('qty_customer', 'release_part_id.catalog_part_id.product_id',
                 'release_part_id.catalog_part_id.state',
                 'release_part_id.catalog_part_id.conversion',
                 'release_part_id.catalog_part_id.factor',
                 'release_part_id.catalog_part_id.customer_uom')
    def _compute_qty(self):
        for line in self:
            catalog = line.release_part_id.catalog_part_id
            qty = catalog.to_product_qty(line.qty_customer) if catalog.product_id else None
            line.qty = qty or 0.0
            line.qty_ok = qty is not None

    @api.depends('week', 'release_part_id.fab_auth_date', 'release_part_id.raw_auth_date',
                 'release_part_id.release_id.release_date',
                 'release_part_id.release_id.profile_id.firm_rule',
                 'release_part_id.release_id.profile_id.firm_weeks')
    def _compute_zone(self):
        for line in self:
            part = line.release_part_id
            profile = part.release_id.profile_id
            if not line.week:
                line.zone = 'planeado'
                continue
            if profile.firm_rule == 'fab_auth' and part.fab_auth_date:
                firm = line.date_customer <= part.fab_auth_date
            else:
                start = _monday(part.release_id.release_date or line.week)
                firm = line.week < start + timedelta(weeks=profile.firm_weeks or 0)
            if firm:
                line.zone = 'firme'
            elif part.raw_auth_date and line.date_customer <= part.raw_auth_date:
                line.zone = 'materia_prima'
            else:
                line.zone = 'planeado'
