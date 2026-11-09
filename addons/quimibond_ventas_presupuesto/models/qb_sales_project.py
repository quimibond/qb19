# -*- coding: utf-8 -*-
"""Proyectos comerciales ligados a Desarrollo de producto (19.0.3.8.0, diseño v3 2.6 y 2.11).

Un **proyecto comercial** (`qb.sales.project`) es una combinación cliente–artículo en
arranque: mientras está vigente, su presupuesto va en «Proyectos industrial» o «Proyectos
confección». Tiene tres orígenes:

- **Desarrollo:** todo producto nuevo pasa por Desarrollo de producto (C1). Cuando el
  desarrollo llega a «Cotización» o más adelante con cliente y artículo (el acabado del
  desarrollo, o el artículo de línea si el análisis dijo «Producto de línea»), nace su
  proyecto comercial. Si el desarrollo cierra sin producto o no es factible, el proyecto
  queda **perdido** y sus renglones de presupuesto se ven con el filtro «Proyecto perdido».
- **Presupuesto:** un renglón de Proyectos crea o liga el suyo (por desarrollo, o por
  cliente y producto o descripción).
- **Ventas:** un producto de línea para un cliente nuevo no pasa por Desarrollo; Ventas lo
  da de alta a mano.

Cuando el desarrollo genera su producto, los renglones del presupuesto sin código ligados a
él lo toman solos (solo en presupuestos abiertos; uno aprobado es evidencia y no cambia).
El proyecto se **gradúa** a línea a mano, o cuando el presupuesto del año siguiente lo
presupuesta fuera de Proyectos.

Sin equipos falsos y sin marcar el producto: el mercado del proyecto sale del equipo del
desarrollo o del cliente.
"""
from markupsafe import Markup

from odoo import api, fields, models
from odoo.exceptions import UserError, ValidationError

from .sgi_sales_budget import _REAL_MOVE_TYPES, PROYECTO_MERCADOS, team_mercado

PROJECT_STATES = [('vigente', "Vigente"), ('graduado', "Pasó a línea"), ('perdido', "Perdido")]
PROJECT_ORIGINS = [('desarrollo', "Desarrollo de producto"), ('presupuesto', "Presupuesto"),
                   ('ventas', "Ventas")]
# Etapas del desarrollo desde las que nace el proyecto comercial (Cotización en adelante).
DEV_FROM_STAGE = 'cotizacion'
DEV_LOST_STAGE = 'cerrado_sin_producto'
# Campos del desarrollo que, al cambiar, revisan su proyecto comercial.
DEV_SYNC_FIELDS = {'stage_id', 'partner_id', 'sgi_dev_product_id', 'sgi_dev_base_product_id',
                   'sgi_dev_analysis_result', 'sgi_is_ft', 'sgi_dev_team_id'}


def proyecto_mercado(env, team):
    """Mercado de proyectos del equipo: Confección → «Proyectos confección»; cualquier otro
    (Industrial, Especiales o ninguno) → «Proyectos industrial»."""
    return 'proyectos_confeccion' if team_mercado(env, team) == 'confeccion' else 'proyectos_industrial'


class QbSalesProject(models.Model):
    _name = 'qb.sales.project'
    _description = "Proyecto comercial (cliente y artículo en arranque)"
    _inherit = ['mail.thread']
    _order = 'state, date_from desc, id desc'

    name = fields.Char(string="Proyecto", compute='_compute_name', store=True)
    company_id = fields.Many2one('res.company', string="Compañía", required=True, index=True,
                                 default=lambda self: self.env.company)
    partner_id = fields.Many2one(
        'res.partner', string="Cliente", index=True, tracking=True,
        domain="[('is_company', '=', True)]",
        help="Empresa (cliente comercial). Vacío solo en un proyecto de Confección, que se "
             "presupuesta sin cliente.")
    product_id = fields.Many2one(
        'product.product', string="Producto", index=True, tracking=True,
        help="Vacío mientras el desarrollo no genere su código: el proyecto se describe con texto "
             "y sus renglones toman el producto cuando se dé de alta.")
    articulo_id = fields.Many2one(related='product_id.qb_articulo_id', string="Artículo")
    descripcion = fields.Char(string="Descripción",
                              help="Qué es el proyecto mientras no tiene código (tela, uso).")
    mercado = fields.Selection(
        [('proyectos_industrial', "Proyectos industrial"),
         ('proyectos_confeccion', "Proyectos confección")],
        string="Mercado", required=True, default='proyectos_industrial', tracking=True)
    dev_project_id = fields.Many2one(
        'project.project', string="Desarrollo de producto", index=True, tracking=True,
        domain="[('sgi_is_ft', '=', True)]", ondelete='set null',
        help="Proyecto de Desarrollo de producto (C1) del que nace. Un producto de línea para un "
             "cliente nuevo no lo necesita.")
    dev_stage_id = fields.Many2one(related='dev_project_id.stage_id', string="Etapa del desarrollo")
    state = fields.Selection(PROJECT_STATES, string="Estado", required=True, default='vigente',
                             index=True, tracking=True)
    origen = fields.Selection(PROJECT_ORIGINS, string="Origen", required=True, default='ventas',
                              readonly=True)
    date_from = fields.Date(string="Desde", required=True, default=fields.Date.context_today,
                            tracking=True, help="Inicio del arranque (fecha de la cotización si "
                                                "nació del desarrollo).")
    date_to = fields.Date(string="Hasta", tracking=True,
                          help="Cuándo pasó a línea o se perdió. Vacío mientras esté vigente.")
    note = fields.Text(string="Nota")
    budget_line_ids = fields.One2many('sgi.sales.budget.line', 'qb_project_id',
                                      string="Renglones de presupuesto")
    budget_line_count = fields.Integer(string="Renglones", compute='_compute_budget')
    amount_budget = fields.Monetary(string="Presupuestado", compute='_compute_budget',
                                    currency_field='currency_id',
                                    help="Importe de sus renglones en presupuestos abiertos o "
                                         "aprobados (sin los reemplazados ni los obsoletos).")
    amount_invoiced = fields.Monetary(string="Facturado", compute='_compute_sales',
                                      currency_field='currency_id',
                                      help="Venta neta al cliente de los códigos del artículo (metros, "
                                           "kilos y m²) desde que arrancó el proyecto.")
    forecast_count = fields.Integer(string="Semanas en el pronóstico", compute='_compute_sales')
    currency_id = fields.Many2one(related='company_id.currency_id', string="Moneda")

    _dev_project_uniq = models.Constraint(
        'unique (dev_project_id)', "Ese desarrollo ya tiene su proyecto comercial.")

    @api.depends('partner_id', 'product_id', 'descripcion')
    def _compute_name(self):
        for project in self:
            what = (project.product_id.default_code or project.product_id.name
                    or project.descripcion or "Sin producto")
            project.name = "%s — %s" % (project.partner_id.name, what) if project.partner_id else what

    @api.constrains('product_id', 'descripcion')
    def _check_what(self):
        for project in self:
            if not project.product_id and not (project.descripcion or '').strip():
                raise ValidationError("Un proyecto lleva producto o, mientras no tenga código, "
                                      "una descripción.")

    @api.constrains('partner_id', 'product_id', 'state', 'company_id')
    def _check_one_open(self):
        for project in self.filtered(lambda p: p.state == 'vigente' and p.product_id):
            twin = self.search([
                ('id', '!=', project.id), ('state', '=', 'vigente'),
                ('company_id', '=', project.company_id.id),
                ('partner_id', '=', project.partner_id.id),
                ('product_id', '=', project.product_id.id)], limit=1)
            if twin:
                raise ValidationError(
                    "Ya hay un proyecto vigente de ese cliente y producto: %s." % twin.name)

    # --- Cifras --------------------------------------------------------------
    @api.depends('budget_line_ids.amount_budget', 'budget_line_ids.budget_id.state')
    def _compute_budget(self):
        for project in self:
            lines = project.budget_line_ids.filtered(
                lambda l: l.budget_id.state in ('borrador', 'revisado', 'aprobado'))
            project.budget_line_count = len(project.budget_line_ids)
            project.amount_budget = sum(lines.mapped('amount_budget'))

    def _qb_products(self):
        """Los códigos del artículo del proyecto (metros, kilos con «I», m²)."""
        self.ensure_one()
        if not self.product_id:
            return self.env['product.product']
        articulo = self.product_id.qb_articulo_id or self.product_id
        return articulo | self.product_id | self.env['product.product'].search(
            [('qb_articulo_id', '=', articulo.id)])

    def _compute_sales(self):
        AML = self.env['account.move.line'].sudo()
        Forecast = self.env['qb.forecast.line'].sudo()
        today = fields.Date.context_today(self)
        for project in self:
            project.amount_invoiced = 0.0
            project.forecast_count = 0
            products = project._qb_products() if project.id else self.env['product.product']
            if not products:
                continue
            domain = [('parent_state', '=', 'posted'), ('display_type', '=', 'product'),
                      ('move_id.move_type', 'in', _REAL_MOVE_TYPES),
                      ('company_id', '=', project.company_id.id),
                      ('product_id', 'in', products.ids),
                      ('move_id.invoice_date', '>=', project.date_from)]
            if project.date_to:
                domain.append(('move_id.invoice_date', '<=', project.date_to))
            if project.partner_id:
                domain.append(('move_id.commercial_partner_id', '=',
                               project.partner_id.commercial_partner_id.id))
            project.amount_invoiced = -sum(AML.search(domain).mapped('balance'))
            f_domain = [('company_id', '=', project.company_id.id),
                        ('product_id', 'in', products.ids), ('week', '>=', today)]
            if project.partner_id:
                f_domain.append(('partner_id.commercial_partner_id', '=',
                                 project.partner_id.commercial_partner_id.id))
            project.forecast_count = Forecast.search_count(f_domain)

    # --- Acciones ------------------------------------------------------------
    def action_qb_graduate(self):
        """Pasa a línea: desde ahora su venta cuenta en el mercado base."""
        for project in self.filtered(lambda p: p.state == 'vigente'):
            project.write({'state': 'graduado', 'date_to': fields.Date.context_today(self)})
            project.message_post(body="Pasó a línea.")
        return True

    def action_qb_lose(self):
        for project in self.filtered(lambda p: p.state == 'vigente'):
            project._qb_mark_lost("Marcado como perdido por %s." % self.env.user.name)
        return True

    def action_qb_reopen(self):
        for project in self.filtered(lambda p: p.state != 'vigente'):
            project.write({'state': 'vigente', 'date_to': False})
            project.message_post(body="Reabierto.")
        return True

    def _qb_mark_lost(self, reason):
        self.ensure_one()
        self.write({'state': 'perdido', 'date_to': fields.Date.context_today(self)})
        open_lines = self.budget_line_ids.filtered(
            lambda l: l.budget_id.state in ('borrador', 'revisado', 'aprobado'))
        body = reason
        if open_lines:
            body += " Tiene %d renglón(es) en presupuestos vigentes por %s: revíselos." % (
                len(open_lines), "{:,.2f}".format(sum(open_lines.mapped('amount_budget'))))
        self.message_post(body=body)

    def action_qb_view_budget_lines(self):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'name': "Renglones de %s" % self.name,
            'res_model': 'sgi.sales.budget.line',
            'view_mode': 'list,form',
            'domain': [('qb_project_id', '=', self.id)],
            'context': {'create': False, 'search_default_group_budget': 1},
        }

    def action_qb_view_forecast(self):
        self.ensure_one()
        domain = [('product_id', 'in', self._qb_products().ids)]
        if self.partner_id:
            domain.append(('partner_id.commercial_partner_id', '=',
                           self.partner_id.commercial_partner_id.id))
        return {
            'type': 'ir.actions.act_window',
            'name': "Pronóstico de %s" % self.name,
            'res_model': 'qb.forecast.line',
            'view_mode': 'list,pivot',
            'domain': domain,
            'context': {'create': False},
        }

    def action_qb_view_invoices(self):
        self.ensure_one()
        if not self.product_id:
            raise UserError("El proyecto todavía no tiene producto: no hay facturas que ver.")
        domain = [('parent_state', '=', 'posted'), ('display_type', '=', 'product'),
                  ('move_id.move_type', 'in', _REAL_MOVE_TYPES),
                  ('company_id', '=', self.company_id.id),
                  ('product_id', 'in', self._qb_products().ids),
                  ('move_id.invoice_date', '>=', self.date_from)]
        if self.date_to:
            domain.append(('move_id.invoice_date', '<=', self.date_to))
        if self.partner_id:
            domain.append(('move_id.commercial_partner_id', '=',
                           self.partner_id.commercial_partner_id.id))
        return {
            'type': 'ir.actions.act_window',
            'name': "Facturado de %s" % self.name,
            'res_model': 'account.move.line',
            'view_mode': 'list',
            'domain': domain,
            'context': {'create': False},
        }

    # --- Producto nuevo del desarrollo → renglones sin código ---------------
    def write(self, vals):
        res = super().write(vals)
        if vals.get('product_id'):
            self._qb_push_product_to_lines()
        return res

    def _qb_push_product_to_lines(self):
        """Los renglones sin producto de presupuestos abiertos toman el producto del proyecto.
        Uno aprobado no cambia (es evidencia); uno que no cabe (unidad de otra categoría, ya
        presupuestado con otro esquema) se queda como está y se avisa en el proyecto."""
        for project in self.filtered('product_id'):
            product = project.product_id
            pending = project.budget_line_ids.filtered(lambda l: not l.product_id)
            done, skipped = 0, []
            for line in pending:
                if line.budget_id.state not in ('borrador', 'revisado'):
                    skipped.append("%s (%s, aprobado)" % (line.budget_id.name, line.date))
                    continue
                if line.uom_id and not line.uom_id._has_common_reference(product.uom_id):
                    skipped.append("%s (%s, unidad %s)" % (line.budget_id.name, line.date,
                                                           line.uom_id.name))
                    continue
                try:
                    with self.env.cr.savepoint():
                        line.write({'product_id': product.id})
                    done += 1
                except (ValidationError, UserError) as exc:
                    skipped.append("%s (%s): %s" % (line.budget_id.name, line.date, exc))
            if done or skipped:
                body = Markup("Producto %s: %d renglón(es) del presupuesto lo tomaron.") % (
                    product.display_name, done)
                if skipped:
                    body += Markup("<br/>Se quedaron sin producto: %s") % "; ".join(skipped)
                project.message_post(body=body)

    # --- Desde el renglón del presupuesto -----------------------------------
    @api.model
    def _qb_for_budget_line(self, line):
        """El proyecto comercial de un renglón de Proyectos: el de su desarrollo; si no, el
        vigente del mismo cliente y producto (o descripción); si no hay, uno nuevo."""
        partner = line.partner_id.commercial_partner_id
        company = line.company_id or self.env.company
        if line.dev_project_id:
            project = self.search([('dev_project_id', '=', line.dev_project_id.id)], limit=1)
            if project:
                return project
            return line.dev_project_id._qb_create_sales_project(origen='presupuesto', line=line)
        domain = [('company_id', '=', company.id), ('state', '=', 'vigente'),
                  ('partner_id', '=', partner.id)]
        if line.product_id:
            domain.append(('product_id', '=', line.product_id.id))
        else:
            domain += [('product_id', '=', False),
                       ('descripcion', '=ilike', (line.proyecto_descripcion or '').strip())]
        project = self.search(domain, limit=1)
        if project:
            return project
        year = line.budget_id.year
        return self.create({
            'company_id': company.id,
            'partner_id': partner.id,
            'product_id': line.product_id.id,
            'descripcion': False if line.product_id else line.proyecto_descripcion,
            'mercado': line.mercado,
            'origen': 'presupuesto',
            'date_from': fields.Date.to_date('%s-01-01' % year) if year else fields.Date.context_today(self),
        })


class ProjectProject(models.Model):
    _inherit = 'project.project'

    qb_sales_project_ids = fields.One2many('qb.sales.project', 'dev_project_id',
                                           string="Proyecto comercial")
    qb_sales_project_count = fields.Integer(string="Proyectos comerciales",
                                            compute='_compute_qb_sales_project_count')

    def _compute_qb_sales_project_count(self):
        data = self.env['qb.sales.project'].sudo()._read_group(
            [('dev_project_id', 'in', self.ids)], ['dev_project_id'], ['__count'])
        counts = {dev.id: count for dev, count in data}
        for project in self:
            project.qb_sales_project_count = counts.get(project.id, 0)

    def write(self, vals):
        res = super().write(vals)
        if DEV_SYNC_FIELDS & set(vals) and not self.env.context.get('sgi_dev_migration'):
            self.filtered('sgi_is_ft')._qb_sync_sales_project()
        return res

    def _qb_dev_product(self):
        """Artículo que se va a vender: el acabado del desarrollo; si el análisis dijo
        «Producto de línea», el artículo de línea."""
        self.ensure_one()
        if self.sgi_dev_product_id:
            return self.sgi_dev_product_id
        if self.sgi_dev_analysis_result == 'linea':
            return self.sgi_dev_base_product_id
        return self.env['product.product']

    def _qb_dev_quote_date(self):
        """Fecha en que el desarrollo entró a Cotización (hoy si no hay registro)."""
        self.ensure_one()
        stage = self._sgi_dev_stage(DEV_FROM_STAGE)
        logs = self.sgi_dev_stage_log_ids.filtered(lambda l: l.stage_id == stage)
        if logs:
            return min(logs.mapped('date_start')).date()
        return fields.Date.context_today(self)

    def _qb_create_sales_project(self, origen='desarrollo', line=None):
        self.ensure_one()
        product = self._qb_dev_product()
        partner = self.partner_id.commercial_partner_id
        if line is not None and line.partner_id:
            partner = line.partner_id.commercial_partner_id
        Sales = self.env['qb.sales.project'].sudo()
        company = self.company_id or self.env.company
        if product:
            # Ventas o el presupuesto ya lo había dado de alta sin desarrollo: se liga.
            twin = Sales.search([
                ('dev_project_id', '=', False), ('state', '=', 'vigente'),
                ('company_id', '=', company.id), ('partner_id', '=', partner.id),
                ('product_id', '=', product.id)], limit=1)
            if twin:
                twin.write({'dev_project_id': self.id})
                twin.budget_line_ids.filtered(lambda l: not l.dev_project_id).with_context(
                    qb_project_link=True).write({'dev_project_id': self.id})
                twin.message_post(body=Markup("Se liga al desarrollo %s.") % self.display_name)
                return twin
        mercado = proyecto_mercado(self.env, self.sgi_dev_team_id or partner.team_id)
        if line is not None and line.mercado in PROYECTO_MERCADOS:
            mercado = line.mercado
        project = Sales.create({
            'company_id': company.id,
            'partner_id': partner.id,
            'product_id': product.id,
            'descripcion': False if product else (self.sgi_dev_product_name or self.name),
            'mercado': mercado,
            'dev_project_id': self.id,
            'origen': origen,
            'date_from': self._qb_dev_quote_date() if origen == 'desarrollo'
            else fields.Date.context_today(self),
        })
        project.message_post(body=Markup("Nace del desarrollo %s.") % self.display_name)
        return project

    def _qb_sync_sales_project(self):
        """Mantiene el proyecto comercial de cada desarrollo:

        - desde Cotización, con cliente y artículo, crea el suyo si no lo tiene;
        - si ya lo tiene sin producto y el desarrollo generó el suyo, se lo pone (y los
          renglones del presupuesto sin código lo toman);
        - cerrado sin producto o no factible: el proyecto vigente queda perdido;
        - de vuelta en una etapa abierta, un proyecto perdido se reabre."""
        Sales = self.env['qb.sales.project'].sudo()
        from_seq = self._sgi_dev_stage_order(DEV_FROM_STAGE)
        for dev in self.filtered(lambda p: p.sgi_is_ft and not p.is_template):
            project = Sales.search([('dev_project_id', '=', dev.id)], limit=1)
            lost = dev.sgi_dev_stage_key == DEV_LOST_STAGE or dev.sgi_dev_analysis_result == 'no_factible'
            if lost:
                if project and project.state == 'vigente':
                    project._qb_mark_lost(Markup("El desarrollo %s cerró sin producto o no es "
                                                 "factible.") % dev.display_name)
                continue
            if project and project.state == 'perdido':
                project.write({'state': 'vigente', 'date_to': False})
                project.message_post(body="El desarrollo volvió a una etapa abierta: se reabre.")
            product = dev._qb_dev_product()
            if project:
                if product and not project.product_id:
                    project.write({'product_id': product.id, 'descripcion': False})
                continue
            if dev.sgi_dev_stage_seq >= from_seq and dev.partner_id and product:
                dev._qb_create_sales_project()

    def action_qb_view_sales_project(self):
        """«Presupuesto y venta»: el proyecto comercial del desarrollo (presupuesto,
        pronóstico y facturado del cliente y artículo)."""
        self.ensure_one()
        projects = self.env['qb.sales.project'].search([('dev_project_id', '=', self.id)])
        action = {
            'type': 'ir.actions.act_window',
            'name': "Presupuesto y venta",
            'res_model': 'qb.sales.project',
            'context': {'default_dev_project_id': self.id,
                        'default_partner_id': self.partner_id.commercial_partner_id.id,
                        'default_origen': 'desarrollo'},
        }
        if len(projects) == 1:
            action.update(view_mode='form', res_id=projects.id)
        else:
            action.update(view_mode='list,form', domain=[('dev_project_id', '=', self.id)])
        return action
