# -*- coding: utf-8 -*-
"""Envío de la muestra y respuesta del cliente (57.129.0; brief §6.11; Jose 2026-10-08, 3.3).

Registro propio ``sgi.dev.shipment`` por cada muestra que sale al cliente:

* **Envío**: fecha, medio (paquetería, recoge el cliente, entrega directa),
  paquetería (lista «Paquetería» de las listas del desarrollo) y guía, y los
  **rollos avisados** con sus dimensiones (lote, metros, ancho, kilos). Sin el
  dictamen de Diseño de Producto sobre la corrida (C1.11) no hay baja ni envío
  («Diseño de Producto valida la muestra; sin eso no hay baja»). La baja de almacén es un
  ``stock.picking`` del tipo de operación del parámetro (producción: 267 «Baja
  de Muestras») creado desde aquí; al validarse, el envío queda registrado y
  el proyecto pasa a «Respuesta del cliente».
* **Aviso al cliente**: correo listo (plantilla con rollos, medio y guía) para
  que Administración de Ventas lo mande; al mandarlo queda la fecha y quién.
  Lo que acompaña a la muestra (CoA del lote, cuando el bloque 3.4 lo
  imprima) se adjunta al registro y viaja en el correo.
* **Respuesta del cliente** como selección: aprueba (el proyecto pasa a
  «Pilotaje» y el artículo a «En pilotaje»), pide cambios (sube revisión y el
  proyecto vuelve a «Muestra») o rechaza (motivo de la lista «Motivo de
  rechazo del cliente» y cierre sin producto). Siempre con medio, fecha y
  **evidencia adjunta**; sin evidencia no se registra.
* **Seguimiento**: si el cliente no contesta en los días del parámetro
  (vacío: sin seguimiento, el brief no lo define), actividad a quien mandó el
  aviso.

C1.12 se mide por el envío registrado; C1.13 por la respuesta registrada
(antes, por el paso del proyecto por la etapa).
"""
import logging
from datetime import timedelta

from odoo import api, fields, models
from odoo.exceptions import UserError

from .sgi_dev_customer import CUSTOMER_APPROVAL_MEDIA

_logger = logging.getLogger(__name__)

PARAM_OUT_PICKING_TYPE = 'quimibond_sgi.dev_sample_out_picking_type_id'
PARAM_NOTIFY_JOB = 'quimibond_sgi.dev_shipment_notify_job_id'
PARAM_FOLLOWUP_DAYS = 'quimibond_sgi.dev_shipment_followup_days'
OUT_PICKING_TYPE_NAME = 'Baja de Muestras'
NOTIFY_JOB_NAME = 'ADMINISTRADOR DE VENTAS'
SHIPMENT_MEDIA = [
    ('paqueteria', "Paquetería"), ('recoge_cliente', "Recoge el cliente"),
    ('entrega_directa', "Entrega directa de Quimibond"),
]
RESPONSES = [('aprueba', "Aprueba la muestra"), ('cambios', "Pide cambios"), ('rechaza', "Rechaza")]
STAGE_AFTER_SHIP = 'respuesta_cliente'
STAGE_APPROVED = 'pilotaje'
STAGE_CHANGES = 'muestra'
STAGE_REJECTED = 'cerrado_sin_producto'
ACTIVITY_NOTIFY = "Avisar al cliente el envío de la muestra"
ACTIVITY_FOLLOWUP = "Seguimiento: el cliente no ha respondido a la muestra"


def _rec_param(env, key, model):
    raw = (env['ir.config_parameter'].sudo().get_param(key, '') or '').strip()
    if raw.isdigit():
        return env[model].sudo().browse(int(raw)).exists()
    return env[model]


def _int_param(env, key, default=0):
    raw = (env['ir.config_parameter'].sudo().get_param(key, '') or '').strip()
    return int(raw) if raw.isdigit() else default


class SgiDevShipment(models.Model):
    _name = 'sgi.dev.shipment'
    _description = "Envío de muestra de desarrollo al cliente"
    _inherit = ['mail.thread', 'mail.activity.mixin']
    _order = 'id desc'

    name = fields.Char(string="Envío", compute='_compute_name', store=True)
    project_id = fields.Many2one('project.project', string="Desarrollo", required=True, ondelete='cascade', index=True,
                                 domain="[('sgi_is_ft', '=', True), ('is_template', '=', False)]",
                                 help="Proyecto de desarrollo cuya muestra se envía.")
    partner_id = fields.Many2one(related='project_id.partner_id', string="Cliente")
    company_id = fields.Many2one('res.company', default=lambda self: self.env.company, required=True)
    production_id = fields.Many2one('mrp.production', string="Orden de muestra", ondelete='set null',
                                    domain="[('sgi_dev_project_id', '=', project_id)]",
                                    help="Corrida de la que salen los rollos.")
    product_id = fields.Many2one('product.product', string="Artículo", compute='_compute_product_id', store=True,
                                 readonly=False, help="El artículo de la orden o, si no hay orden, el del proyecto.")
    state = fields.Selection([('borrador', "Por enviar"), ('enviada', "Enviada"), ('respondida', "Con respuesta")],
                             string="Estado", default='borrador', required=True, tracking=True, copy=False)
    # --- Envío -------------------------------------------------------------
    medium = fields.Selection(SHIPMENT_MEDIA, string="Medio de entrega", tracking=True)
    medium_label = fields.Char(string="Medio (texto)", compute='_compute_medium_label',
                               help="Etiqueta del medio para el correo al cliente.")
    carrier_id = fields.Many2one('sgi.dev.option', string="Paquetería", domain=[('kind', '=', 'paqueteria')],
                                 help="De la lista «Paquetería» (Ajustes → SGI → Listas del desarrollo de producto).")
    tracking_ref = fields.Char(string="Guía", help="Número de guía de la paquetería.")
    date_shipped = fields.Date(string="Fecha de envío", tracking=True)
    picking_id = fields.Many2one('stock.picking', string="Baja de almacén", readonly=True, copy=False,
                                 help="Traslado de salida (Baja de Muestras) creado desde este envío.")
    picking_state = fields.Selection(related='picking_id.state', string="Estado de la baja")
    roll_ids = fields.One2many('sgi.dev.shipment.roll', 'shipment_id', string="Rollos avisados")
    roll_count = fields.Integer(string="Rollos", compute='_compute_totals', store=True)
    total_m = fields.Float(string="Metros", compute='_compute_totals', store=True, digits=(16, 2))
    total_kg = fields.Float(string="Kilos", compute='_compute_totals', store=True, digits=(16, 2))
    attachment_ids = fields.Many2many('ir.attachment', 'sgi_dev_shipment_attachment_rel', 'shipment_id', 'attachment_id',
                                      string="Documentos que acompañan", copy=False,
                                      help="CoA del lote y lo demás que viaja con la muestra; se adjuntan al correo.")
    shipped_at = fields.Datetime(string="Envío registrado el", readonly=True, copy=False)
    shipped_by_id = fields.Many2one('res.users', string="Envío registrado por", readonly=True, copy=False)
    notified_date = fields.Datetime(string="Aviso al cliente enviado el", readonly=True, copy=False)
    notified_by_id = fields.Many2one('res.users', string="Aviso enviado por", readonly=True, copy=False)
    followup_date = fields.Datetime(string="Último seguimiento", readonly=True, copy=False)
    # --- Respuesta del cliente ---------------------------------------------
    response = fields.Selection(RESPONSES, string="Respuesta del cliente", tracking=True, copy=False)
    response_date = fields.Date(string="Fecha de la respuesta", copy=False)
    response_medium = fields.Selection(CUSTOMER_APPROVAL_MEDIA, string="Medio de la respuesta", copy=False)
    response_contact_id = fields.Many2one('res.partner', string="Quién respondió", copy=False)
    response_ref = fields.Char(string="Referencia de la respuesta", copy=False,
                               help="Asunto del correo, número de orden de compra…")
    response_file = fields.Binary(string="Evidencia de la respuesta", attachment=True, copy=False)
    response_filename = fields.Char(copy=False)
    response_note = fields.Text(string="Cambios que pide", copy=False,
                                help="Solo si pide cambios: qué ajusta; la especificación se corrige en la tabla "
                                     "del proyecto y queda como revisión.")
    reject_reason_id = fields.Many2one('sgi.dev.option', string="Motivo del rechazo", copy=False,
                                       domain=[('kind', '=', 'motivo_rechazo_cliente')],
                                       help="De la lista «Motivo de rechazo del cliente».")
    response_registered_at = fields.Datetime(string="Respuesta registrada el", readonly=True, copy=False)
    response_registered_by_id = fields.Many2one('res.users', string="Respuesta registrada por", readonly=True, copy=False)

    @api.depends('project_id.name', 'date_shipped')
    def _compute_name(self):
        for ship in self:
            ship.name = "Envío de muestra · %s%s" % (ship.project_id.name or '',
                                                     (" · %s" % ship.date_shipped) if ship.date_shipped else '')

    @api.depends('medium')
    def _compute_medium_label(self):
        labels = dict(SHIPMENT_MEDIA)
        for ship in self:
            ship.medium_label = labels.get(ship.medium, '')

    @api.depends('production_id', 'project_id.sgi_dev_product_id')
    def _compute_product_id(self):
        for ship in self:
            ship.product_id = ship.production_id.product_id or ship.project_id.sgi_dev_product_id

    @api.depends('roll_ids.length_m', 'roll_ids.weight_kg')
    def _compute_totals(self):
        for ship in self:
            ship.roll_count = len(ship.roll_ids)
            ship.total_m = sum(ship.roll_ids.mapped('length_m'))
            ship.total_kg = sum(ship.roll_ids.mapped('weight_kg'))

    @api.onchange('project_id')
    def _onchange_project_id(self):
        if self.project_id and not self.production_id:
            mos = self.project_id.sgi_dev_mo_ids.filtered(lambda m: m.state == 'done').sorted('id')
            self.production_id = mos[-1:].id if mos else False

    # ------------------------------------------------------------------------
    # Validación de la muestra (C1.11) y requisitos del envío
    # ------------------------------------------------------------------------
    def _lab_verdicts(self):
        self.ensure_one()
        return self.project_id.sgi_dev_lab_request_ids.filtered(
            lambda r: r.kind == 'corrida' and r.verdict_by_id and r.state == 'medida')

    def _check_sample_validated(self):
        for ship in self:
            if not ship._lab_verdicts():
                raise UserError("%s: Diseño de Producto no ha dictaminado los resultados de laboratorio de la "
                                "corrida (C1.11). Sin ese dictamen no hay baja ni envío de la muestra."
                                % ship.project_id.display_name)
        return True

    def _check_ready_to_ship(self):
        for ship in self:
            ship._check_sample_validated()
            faltan = []
            if not ship.medium:
                faltan.append("el medio de entrega")
            if ship.medium == 'paqueteria':
                if not ship.carrier_id:
                    faltan.append("la paquetería")
                if not ship.tracking_ref:
                    faltan.append("la guía")
            if not ship.roll_ids:
                faltan.append("los rollos con sus dimensiones")
            elif any(r.length_m <= 0 for r in ship.roll_ids):
                faltan.append("los metros de cada rollo")
            if faltan:
                raise UserError("Para registrar el envío falta %s." % ", ".join(faltan))
        return True

    # ------------------------------------------------------------------------
    # Baja de almacén
    # ------------------------------------------------------------------------
    @api.model
    def _out_picking_type(self):
        """El tipo de operación del parámetro o, si está vacío, «Baja de Muestras» por nombre."""
        ptype = _rec_param(self.env, PARAM_OUT_PICKING_TYPE, 'stock.picking.type')
        if ptype:
            return ptype
        types = self.env['project.project']._sgi_dev_search_langs(
            'stock.picking.type', [('name', 'ilike', OUT_PICKING_TYPE_NAME), ('code', '=', 'outgoing')])
        return (types.filtered('active') or types)[:1]

    def _picking_qty(self):
        """Cantidad del movimiento en la unidad del artículo: metros si el artículo se mide en largo,
        kilos si se mide en peso, si no el número de rollos."""
        self.ensure_one()
        uom = self.product_id.uom_id
        for total, ref in ((self.total_m, 'uom.product_uom_meter'), (self.total_kg, 'uom.product_uom_kgm')):
            unit = self.env.ref(ref, raise_if_not_found=False)
            if not unit or total <= 0:
                continue
            if unit == uom:
                return total
            try:
                return unit._compute_quantity(total, uom, raise_if_failure=True)
            except UserError:
                continue
        return float(self.roll_count)

    def action_create_picking(self):
        """Crea la baja (salida) con el artículo y la cantidad de los rollos; se valida en Inventario."""
        self.ensure_one()
        if self.picking_id:
            return self.action_open_picking()
        self._check_ready_to_ship()
        if not self.product_id:
            raise UserError("El proyecto no tiene artículo en desarrollo: no hay qué dar de baja.")
        ptype = self._out_picking_type()
        if not ptype:
            raise UserError("No hay tipo de operación para la baja de muestras: configúrelo en Ajustes → SGI "
                            "(«Tipo de operación de la baja de muestras»).")
        src = (self.production_id.location_dest_id or ptype.default_location_src_id
               or self.env['project.project']._sgi_dev_sample_location())
        dest = ptype.default_location_dest_id or self.env.ref('stock.stock_location_customers')
        picking = self.env['stock.picking'].create({
            'picking_type_id': ptype.id, 'partner_id': self.partner_id.id, 'origin': self.project_id.display_name,
            'location_id': src.id, 'location_dest_id': dest.id, 'company_id': self.company_id.id,
            'sgi_dev_shipment_id': self.id,
            'move_ids': [(0, 0, {
                'product_id': self.product_id.id,
                'product_uom_qty': self._picking_qty(), 'product_uom': self.product_id.uom_id.id,
                'location_id': src.id, 'location_dest_id': dest.id, 'company_id': self.company_id.id,
            })],
        })
        self.write({'picking_id': picking.id})
        self.message_post(body="Baja de muestras %s creada (%s)." % (picking.name, ptype.display_name))
        return self.action_open_picking()

    def action_open_picking(self):
        self.ensure_one()
        return {'type': 'ir.actions.act_window', 'res_model': 'stock.picking', 'res_id': self.picking_id.id,
                'view_mode': 'form', 'target': 'current'}

    # ------------------------------------------------------------------------
    # Envío
    # ------------------------------------------------------------------------
    def action_ship(self):
        """Registra el envío. Si hay baja ligada, se registra al validarla; aquí solo si ya está hecha."""
        for ship in self:
            if ship.state != 'borrador':
                raise UserError("El envío ya está registrado.")
            if ship.picking_id and ship.picking_id.state not in ('done', 'cancel'):
                raise UserError("Valide primero la baja %s en Inventario; el envío se registra solo."
                                % ship.picking_id.name)
            ship._check_ready_to_ship()
            ship._mark_shipped()
        return True

    def _notify_users(self):
        job = _rec_param(self.env, PARAM_NOTIFY_JOB, 'hr.job')
        if not job:
            jobs = self.env['project.project']._sgi_dev_search_langs('hr.job', [('name', 'ilike', NOTIFY_JOB_NAME)])
            job = (jobs.filtered('active') or jobs)[:1]
        users = job.sudo().employee_ids.mapped('user_id').filtered('active') if job else self.env['res.users']
        return users or self.env.user

    def _mark_shipped(self, when=None):
        when = when or fields.Datetime.now()
        stage = self.env['project.project']._sgi_dev_stage(STAGE_AFTER_SHIP)
        seq = self.env['project.project']._sgi_dev_stage_order(STAGE_AFTER_SHIP)
        for ship in self.filtered(lambda s: s.state == 'borrador'):
            ship.write({'state': 'enviada', 'shipped_at': when, 'shipped_by_id': self.env.uid,
                        'date_shipped': ship.date_shipped or fields.Date.context_today(ship)})
            ship.message_post(body="Envío registrado: %s%s%s, %d rollo(s), %.2f m." % (
                dict(SHIPMENT_MEDIA).get(ship.medium, ''),
                (" por %s" % ship.carrier_id.name) if ship.carrier_id else '',
                (" guía %s" % ship.tracking_ref) if ship.tracking_ref else '', ship.roll_count, ship.total_m))
            ship.project_id.message_post(body="Muestra enviada al cliente (%s): %d rollo(s), %.2f m." % (
                ship.date_shipped, ship.roll_count, ship.total_m))
            project = ship.project_id
            if stage and project.sgi_dev_stage_seq < seq and project.sgi_dev_stage_key not in ('cerrado_sin_producto',):
                project.write({'stage_id': stage.id})
            users = ship._notify_users()
            ship.activity_schedule('mail.mail_activity_data_todo', user_id=users[:1].id, summary=ACTIVITY_NOTIFY,
                                   note="Abra «Correo al cliente»: trae los rollos con sus dimensiones, el medio "
                                        "de entrega y la guía. Si lleva CoA, adjúntelo al envío antes.")
        return True

    # ------------------------------------------------------------------------
    # Aviso al cliente
    # ------------------------------------------------------------------------
    def action_open_mail(self):
        """Correo listo con rollos, medio y guía; lo manda Administración de Ventas."""
        self.ensure_one()
        if self.state == 'borrador':
            raise UserError("Registre el envío antes de avisar al cliente.")
        template = self.env.ref('quimibond_sgi.mail_template_sgi_dev_shipment')
        recipients = self.project_id.sgi_dev_contact_id or self.partner_id
        ctx = {
            'default_model': self._name, 'default_res_ids': self.ids, 'default_template_id': template.id,
            'default_composition_mode': 'comment', 'default_partner_ids': recipients.ids,
            'default_attachment_ids': self.attachment_ids.ids, 'sgi_dev_shipment_notify': True, 'force_email': True,
        }
        return {'type': 'ir.actions.act_window', 'res_model': 'mail.compose.message', 'view_mode': 'form',
                'views': [(self.env.ref('mail.email_compose_message_wizard_form').id, 'form')],
                'target': 'new', 'context': ctx}

    def message_post(self, **kwargs):
        if self.env.context.get('sgi_dev_shipment_notify'):
            when = fields.Datetime.now()
            for ship in self.filtered(lambda s: s.state != 'borrador' and not s.notified_date):
                ship.with_context(sgi_dev_shipment_notify=False).write({'notified_date': when, 'notified_by_id': self.env.uid})
            self.activity_feedback(['mail.mail_activity_data_todo'], feedback="Aviso enviado al cliente.")
        return super().message_post(**kwargs)

    # ------------------------------------------------------------------------
    # Respuesta del cliente
    # ------------------------------------------------------------------------
    def _check_response(self):
        for ship in self:
            if ship.state == 'borrador':
                raise UserError("Registre el envío antes que la respuesta del cliente.")
            if ship.state == 'respondida':
                raise UserError("La respuesta ya está registrada.")
            faltan = []
            if not ship.response:
                faltan.append("la respuesta")
            if not ship.response_date:
                faltan.append("la fecha")
            if not ship.response_medium:
                faltan.append("el medio")
            if not ship.response_file:
                faltan.append("la evidencia adjunta")
            if ship.response == 'rechaza' and not ship.reject_reason_id:
                faltan.append("el motivo del rechazo")
            if faltan:
                raise UserError("Para registrar la respuesta del cliente falta %s." % ", ".join(faltan))
        return True

    def action_register_response(self):
        Project = self.env['project.project']
        for ship in self:
            ship._check_response()
            project = ship.project_id
            ship.write({'state': 'respondida', 'response_registered_at': fields.Datetime.now(),
                        'response_registered_by_id': self.env.uid})
            label = dict(RESPONSES).get(ship.response)
            detail = "%s, %s%s%s" % (
                dict(CUSTOMER_APPROVAL_MEDIA).get(ship.response_medium), ship.response_date,
                (", %s" % ship.response_contact_id.name) if ship.response_contact_id else '',
                (", ref. %s" % ship.response_ref) if ship.response_ref else '')
            if ship.response == 'aprueba':
                # 57.131.0 (Jose 2026-10-08, 5.1): las características que van a la especificación
                # del cliente quedan marcadas como aprobadas por él (paso 19 del brief).
                lines = project.sgi_dev_line_ids.filtered(lambda l: l.in_customer_spec and not l.customer_approved)
                if lines:
                    lines.write({'customer_approved': True})
                stage = Project._sgi_dev_stage(STAGE_APPROVED)
                if stage and project.sgi_dev_stage_seq < Project._sgi_dev_stage_order(STAGE_APPROVED):
                    project.write({'stage_id': stage.id})
                project.message_post(body="El cliente aprobó la muestra (%s): %d característica(s) aprobadas. "
                                          "El desarrollo pasa a pilotaje." % (detail, len(lines)))
            elif ship.response == 'cambios':
                project.action_sgi_dev_new_revision()
                if ship.response_note:
                    project.sgi_dev_revision_ids.sorted('id')[-1:].write({
                        'note': "Revisión %d abierta: el cliente pide cambios. %s" % (project.sgi_dev_revision,
                                                                                      ship.response_note)})
                stage = Project._sgi_dev_stage(STAGE_CHANGES)
                if stage and project.stage_id != stage:
                    project.write({'stage_id': stage.id})
                project.message_post(body="El cliente pide cambios a la muestra (%s): revisión %d. %s" % (
                    detail, project.sgi_dev_revision, ship.response_note or ''))
            else:
                stage = Project._sgi_dev_stage(STAGE_REJECTED)
                if stage and project.stage_id != stage:
                    project.write({'stage_id': stage.id})
                project.message_post(body="El cliente rechazó la muestra (%s). Motivo: %s. El desarrollo cierra "
                                          "sin producto." % (detail, ship.reject_reason_id.name))
            ship.message_post(body="Respuesta del cliente registrada: %s (%s)." % (label, detail))
            ship.activity_feedback(['mail.mail_activity_data_todo'], feedback="Respuesta registrada: %s." % label)
        return True

    # ------------------------------------------------------------------------
    # Seguimiento si el cliente no contesta
    # ------------------------------------------------------------------------
    @api.model
    def cron_sgi_dev_shipment_followup(self):
        days = _int_param(self.env, PARAM_FOLLOWUP_DAYS, 0)
        if days <= 0:
            return 0
        now = fields.Datetime.now()
        limit = now - timedelta(days=days)
        done = 0
        for ship in self.search([('state', '=', 'enviada'), ('notified_date', '!=', False)]):
            last = ship.followup_date or ship.notified_date
            if last > limit:
                continue
            user = ship.notified_by_id or ship.shipped_by_id or self.env.user
            ship.activity_schedule('mail.mail_activity_data_todo', user_id=user.id, summary=ACTIVITY_FOLLOWUP,
                                   note="Aviso enviado el %s; %d días sin respuesta. Pregunte al cliente y registre "
                                        "su respuesta aquí." % (ship.notified_date, (now - ship.notified_date).days))
            ship.write({'followup_date': now})
            done += 1
        return done


class SgiDevShipmentRoll(models.Model):
    _name = 'sgi.dev.shipment.roll'
    _description = "Rollo avisado al cliente en un envío de muestra"
    _order = 'shipment_id, sequence, id'

    shipment_id = fields.Many2one('sgi.dev.shipment', string="Envío", required=True, ondelete='cascade', index=True)
    sequence = fields.Integer(default=10)
    product_id = fields.Many2one(related='shipment_id.product_id')
    lot_id = fields.Many2one('stock.lot', string="Lote", domain="[('product_id', '=', product_id)]",
                             help="Lote del rollo en la ubicación de desarrollos.")
    length_m = fields.Float(string="Metros", digits=(16, 2), required=True)
    width_m = fields.Float(string="Ancho (m)", digits=(16, 3))
    weight_kg = fields.Float(string="Kilos", digits=(16, 2))


class ProjectProjectDevShipment(models.Model):
    _inherit = 'project.project'

    sgi_dev_shipment_ids = fields.One2many('sgi.dev.shipment', 'project_id', string="Envíos de muestra")
    sgi_dev_shipment_count = fields.Integer(compute='_compute_sgi_dev_shipment_count')

    @api.depends('sgi_dev_shipment_ids')
    def _compute_sgi_dev_shipment_count(self):
        for project in self:
            project.sgi_dev_shipment_count = len(project.sgi_dev_shipment_ids)

    @api.model
    def _sgi_dev_sample_location(self):
        """Ubicación del sobrante de la muestra (parámetro del bloque F, 57.125.0)."""
        return _rec_param(self.env, 'quimibond_sgi.dev_sample_location_id', 'stock.location')

    def action_sgi_dev_shipments(self):
        self.ensure_one()
        action = self.env['ir.actions.act_window']._for_xml_id('quimibond_sgi.sgi_dev_shipment_action')
        action['domain'] = [('project_id', '=', self.id)]
        action['context'] = {'default_project_id': self.id, 'search_default_project_id': self.id}
        if self.sgi_dev_shipment_count == 1:
            action.update({'view_mode': 'form', 'views': [(False, 'form')], 'res_id': self.sgi_dev_shipment_ids.id})
        return action

    def action_sgi_dev_new_shipment(self):
        self.ensure_one()
        mos = self.sgi_dev_mo_ids.filtered(lambda m: m.state == 'done').sorted('id')
        return {'type': 'ir.actions.act_window', 'res_model': 'sgi.dev.shipment', 'view_mode': 'form', 'target': 'current',
                'context': {'default_project_id': self.id, 'default_production_id': mos[-1:].id if mos else False}}


class StockPickingDevShipment(models.Model):
    _inherit = 'stock.picking'

    sgi_dev_shipment_id = fields.Many2one('sgi.dev.shipment', string="Envío de muestra", readonly=True, copy=False,
                                          index=True, ondelete='set null')

    def button_validate(self):
        # «Diseño de Producto valida la muestra; sin eso no hay baja»: la baja exige el dictamen de C1.11 y los
        # datos del envío (medio, paquetería y guía, rollos con metros).
        self.sgi_dev_shipment_id.filtered(lambda s: s.state == 'borrador')._check_ready_to_ship()
        return super().button_validate()

    def _action_done(self):
        res = super()._action_done()
        for picking in self.filtered(lambda p: p.sgi_dev_shipment_id and p.state == 'done'):
            ship = picking.sgi_dev_shipment_id
            if ship.state == 'borrador':
                ship._mark_shipped(when=picking.date_done)
        return res
