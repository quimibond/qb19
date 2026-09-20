# -*- coding: utf-8 -*-
from odoo import models, fields, api
from odoo.exceptions import UserError


class StockPicking(models.Model):
    _inherit = 'stock.picking'

    sgi_nc_count = fields.Integer(string="# NC", compute='_compute_sgi_nc_count')

    def _compute_sgi_nc_count(self):
        Alert = self.env['quality.alert']
        for picking in self:
            picking.sgi_nc_count = Alert.search_count([('picking_id', '=', picking.id)]) if picking.id else 0

    def action_sgi_open_ncs(self):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'name': "No Conformidades",
            'res_model': 'quality.alert',
            'view_mode': 'list,form',
            'domain': [('picking_id', '=', self.id)],
            'context': {'default_picking_id': self.id, 'default_sgi_origin_type': 'proceso'},
        }

    # ----- Devolución de cliente → NC automática (aprovecha el flujo de
    # devoluciones que Odoo ya registra: una devolución validada de una
    # entrega a cliente es la señal de calidad más dura que existe). -----
    sgi_return_alert_id = fields.Many2one('quality.alert', string="NC de devolución",
                                          readonly=True, copy=False)
    # Sello del transporte en embarques de salida. Sustituye los formatos
    # F-IT-P-A07-01-07 (nacional) y -08 (exportación): un solo campo, la
    # entrega ya sabe si es nacional o exportación por su destino.
    sgi_seal_number = fields.Char(
        string="Sello de embarque", copy=False,
        help="Número de sello del transporte. Se imprime en la remisión. "
             "Sustituye F-IT-P-A07-01-07/08.")

    def _sgi_is_customer_return(self):
        """Recepción validada cuyos movimientos devuelven una ENTREGA a
        cliente (no una devolución a proveedor, que es un picking saliente)."""
        self.ensure_one()
        if self.picking_type_id.code != 'incoming':
            return False
        return any(
            move.origin_returned_move_id.picking_id.picking_type_id.code == 'outgoing'
            for move in self.move_ids if move.origin_returned_move_id)

    def _sgi_create_return_alert(self):
        for picking in self:
            if picking.sgi_return_alert_id or not picking._sgi_is_customer_return():
                continue
            partner = picking.partner_id.commercial_partner_id
            returned = picking.move_ids.filtered('origin_returned_move_id')
            product = returned[:1].product_id
            team = self.env.ref('quimibond_sgi.sgi_quality_team_internal',
                                raise_if_not_found=False)
            vals = {
                'title': "Devolución de cliente: %s" % (partner.display_name or ''),
                'sgi_origin_type': 'reclamacion',
                'partner_id': partner.id,
                'product_id': product.id or False,
                'product_tmpl_id': product.product_tmpl_id.id or False,
                'picking_id': picking.id,
                'sgi_deviation': "Devolución de cliente validada (%s). Productos: %s. "
                                 "Investigue la causa y la disposición del material."
                                 % (picking.name,
                                    ", ".join(returned.mapped('product_id.display_name'))),
            }
            if team:
                vals['team_id'] = team.id
            alert = self.env['quality.alert'].sgi_auto_create(
                'devolucion_cliente', vals)
            if alert:
                picking.sgi_return_alert_id = alert.id
                picking.message_post(
                    body="Devolución de cliente: se levantó la NC <b>%s</b>."
                         % (alert.sgi_folio or alert.title))

    def _action_done(self):
        res = super()._action_done()
        self._sgi_create_return_alert()
        return res


class MrpProduction(models.Model):
    _inherit = 'mrp.production'

    sgi_nc_count = fields.Integer(string="# NC", compute='_compute_sgi_nc_count')

    def _compute_sgi_nc_count(self):
        Alert = self.env['quality.alert']
        for prod in self:
            prod.sgi_nc_count = Alert.search_count([('production_id', '=', prod.id)]) if prod.id else 0

    def action_sgi_open_ncs(self):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'name': "No Conformidades",
            'res_model': 'quality.alert',
            'view_mode': 'list,form',
            'domain': [('production_id', '=', self.id)],
            'context': {'default_production_id': self.id, 'default_sgi_origin_type': 'proceso'},
        }


class PurchaseOrder(models.Model):
    _inherit = 'purchase.order'

    sgi_nc_count = fields.Integer(string="# NC proveedor", compute='_compute_sgi_nc_count')

    def _compute_sgi_nc_count(self):
        Alert = self.env['quality.alert']
        for order in self:
            partner = order.partner_id.commercial_partner_id
            order.sgi_nc_count = Alert.search_count(
                [('partner_id', '=', partner.id)]) if partner else 0

    def action_sgi_open_ncs(self):
        self.ensure_one()
        partner = self.partner_id.commercial_partner_id
        return {
            'type': 'ir.actions.act_window',
            'name': "No Conformidades del proveedor",
            'res_model': 'quality.alert',
            'view_mode': 'list,form',
            'domain': [('partner_id', '=', partner.id)],
            'context': {'default_partner_id': partner.id, 'default_sgi_origin_type': 'proceso'},
        }

    def button_confirm(self):
        # 8.4.1: un proveedor BLOQUEADO por el SGI no recibe órdenes de compra.
        # Un proveedor sin estatus (fuera del alcance SGI) no se bloquea.
        for order in self:
            partner = order.partner_id.commercial_partner_id
            if partner.sgi_supplier_status == 'bloqueado':
                raise UserError(
                    "El proveedor %s está BLOQUEADO por el SGI (8.4.1): no se "
                    "pueden confirmar órdenes de compra. Pida al Jefe de MAST "
                    "revisar su aprobación." % partner.display_name)
        return super().button_confirm()


class MaintenanceRequest(models.Model):
    _inherit = 'maintenance.request'

    sgi_alert_id = fields.Many2one('quality.alert', string="NC generada",
                                   readonly=True, copy=False)

    def action_sgi_raise_nc(self):
        """Levanta una NC (equipo NC Internas) desde una solicitud correctiva,
        pre-llenada con el equipo/máquina y la descripción de la falla."""
        self.ensure_one()
        if self.sgi_alert_id:
            return self._sgi_open_alert()
        team = self.env.ref('quimibond_sgi.sgi_quality_team_internal',
                            raise_if_not_found=False)
        equipment_name = self.equipment_id.name or ''
        vals = {
            'title': "Falla de mantenimiento: %s" % (equipment_name or self.name),
            'sgi_origin_type': 'proceso',
            'sgi_deviation': "Solicitud de mantenimiento «%s» sobre el equipo «%s».\n%s" % (
                self.name or '', equipment_name, self.description or ''),
        }
        if team:
            vals['team_id'] = team.id
        alert = self.env['quality.alert'].sgi_auto_create('mantenimiento_falla', vals)
        self.sgi_alert_id = alert.id
        self.message_post(body="Se levantó la NC <b>%s</b> por esta falla." % (
            alert.sgi_folio or alert.name))
        return self._sgi_open_alert()

    def _sgi_open_alert(self):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'name': "No Conformidad",
            'res_model': 'quality.alert',
            'view_mode': 'form',
            'res_id': self.sgi_alert_id.id,
        }


class HrJob(models.Model):
    _inherit = 'hr.job'

    sgi_document_ids = fields.Many2many('documents.document', 'sgi_document_job_rel',
                                        'job_id', 'document_id', string="Documentos aplicables")
    # Determinación del EPP por puesto (sustituye F-P-S03-01): la fuente
    # única vive en el puesto; la ficha del empleado la muestra en solo
    # lectura. La responsiva de entrega (S03-02) sigue documental.
    sgi_epp_required = fields.Text(
        string="EPP requerido (S03-01)",
        help="Equipo de protección personal que exige este puesto. "
             "Sustituye el formato F-P-S03-01; fuente única para RH y SST.")


class ProductTemplateSgiSpec(models.Model):
    _inherit = 'product.template'

    # Especificaciones controladas del producto (sustituyen F-P-C04-06 hoja
    # de especificación de MP y F-P-C14-02 manejo y empaque): la spec ES un
    # documento controlado; aquí solo se liga para tenerla a un clic desde
    # el producto y las inspecciones.
    sgi_spec_document_id = fields.Many2one(
        'documents.document', string="Especificación (C04-06)",
        domain="[('sgi_code', '!=', False)]",
        help="Documento controlado con la especificación del material "
             "(sustituye F-P-C04-06). La inspección de recepción valida "
             "contra esta spec.")
    sgi_packaging_notes = fields.Text(
        string="Manejo y empaque (C14-02)",
        help="Indicaciones de manejo y empaque del producto (sustituye "
             "F-P-C14-02). Visible para inspección y almacén.")

    def action_sgi_open_spec(self):
        self.ensure_one()
        if not self.sgi_spec_document_id:
            raise UserError(
                "El producto no tiene ligada su especificación (C04-06). "
                "Selecciónala en la pestaña SGI.")
        return self.sgi_spec_document_id.action_sgi_view_file()


class ProductProductSgiSpec(models.Model):
    """El formulario de variante hereda la vista de la plantilla (mismo caso
    documentado en el smart button de PPAP): el botón de la spec también debe
    resolver en product.product."""
    _inherit = 'product.product'

    def action_sgi_open_spec(self):
        self.ensure_one()
        return self.product_tmpl_id.action_sgi_open_spec()


class HrEmployee(models.Model):
    _inherit = 'hr.employee'

    sgi_procedure_count = fields.Integer(string="# Mis procedimientos", compute='_compute_sgi_counts')
    sgi_pending_ack_count = fields.Integer(string="# Acuses pendientes", compute='_compute_sgi_counts')

    def _compute_sgi_counts(self):
        Doc = self.env['documents.document']
        Ack = self.env['sgi.document.ack']
        for emp in self:
            if emp.job_id:
                emp.sgi_procedure_count = Doc.search_count([
                    ('sgi_state', '=', 'vigente'),
                    ('sgi_job_ids', 'in', emp.job_id.id),
                ])
            else:
                emp.sgi_procedure_count = 0
            emp.sgi_pending_ack_count = Ack.search_count([
                ('employee_id', '=', emp.id),
                ('state', '=', 'pendiente'),
            ]) if emp.id else 0

    def action_sgi_my_procedures(self):
        self.ensure_one()
        list_view = self.env.ref('quimibond_sgi.sgi_document_view_list',
                                 raise_if_not_found=False)
        form_view = self.env.ref('quimibond_sgi.sgi_document_view_form',
                                 raise_if_not_found=False)
        return {
            'type': 'ir.actions.act_window',
            'name': "Mis procedimientos",
            'res_model': 'documents.document',
            'view_mode': 'list,form',
            # Fija la ficha SGI (clave/estado/tipo/familia); sin ella Odoo abre
            # el formulario mínimo de captura de URL de la app Documentos.
            'views': [
                (list_view.id if list_view else False, 'list'),
                (form_view.id if form_view else False, 'form'),
            ],
            'domain': [('sgi_state', '=', 'vigente'), ('sgi_job_ids', 'in', self.job_id.ids)],
            'help': "<p class='o_view_nocontent_smiling_face'>Sin procedimientos "
                    "asignados a tu puesto</p><p>Aquí aparecen los documentos "
                    "VIGENTES que aplican a tu puesto — tu referencia de cómo se "
                    "hace el trabajo (botón «Ver archivo»). Si está vacío y no "
                    "debería, pide a RH que asigne tu puesto en tu ficha de "
                    "empleado y a MAST que ligue los puestos al documento.</p>",
        }

    def action_sgi_pending_acks(self):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'name': "Acuses pendientes",
            'res_model': 'sgi.document.ack',
            'view_mode': 'list,form',
            'domain': [('employee_id', '=', self.id), ('state', '=', 'pendiente')],
        }


class ResPartner(models.Model):
    _inherit = 'res.partner'

    sgi_complaint_count = fields.Integer(string="# Reclamaciones", compute='_compute_sgi_partner_counts')
    sgi_nc_count = fields.Integer(string="# NC", compute='_compute_sgi_partner_counts')

    def _compute_sgi_partner_counts(self):
        Ticket = self.env['helpdesk.ticket']
        Alert = self.env['quality.alert']
        for partner in self:
            if partner.id:
                partner.sgi_complaint_count = Ticket.search_count([
                    ('partner_id', '=', partner.id),
                    ('sgi_alert_id', '!=', False),
                ]) + Ticket.search_count([
                    ('partner_id', '=', partner.id),
                    ('sgi_disposition', '!=', False),
                    ('sgi_alert_id', '=', False),
                ])
                partner.sgi_nc_count = Alert.search_count([('partner_id', '=', partner.id)])
            else:
                partner.sgi_complaint_count = 0
                partner.sgi_nc_count = 0

    def action_sgi_open_complaints(self):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'name': "Reclamaciones",
            'res_model': 'helpdesk.ticket',
            'view_mode': 'list,form',
            'domain': [('partner_id', '=', self.id)],
            'context': {'default_partner_id': self.id},
        }

    def action_sgi_open_ncs(self):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'name': "No Conformidades",
            'res_model': 'quality.alert',
            'view_mode': 'list,form',
            'domain': [('partner_id', '=', self.id)],
            'context': {'default_partner_id': self.id},
        }


class IrUiMenu(models.Model):
    _inherit = 'ir.ui.menu'

    @api.model
    def _sgi_attach_quality_menus(self):
        """Cuelga 'Calidad preventiva' y 'Tableros (SGI)' del menú raíz de la
        app Calidad. El módulo quality es Enterprise y su xmlid no es
        verificable desde el repo: una referencia dura en el XML rompería
        la carga completa del registro si no coincide. Aquí el raíz se
        DESCUBRE en runtime: primero por los xmlids conocidos y, si no,
        buscando en ir.model.data el menú de primer nivel que pertenezca
        al módulo quality/quality_control. Si no hay app Calidad, los
        menús se quedan bajo el SGI (el parent declarado en
        sgi_menus.xml), que siempre existe."""
        root = None
        for xmlid in ('quality.menu_quality_root',
                      'quality_control.menu_quality_root'):
            root = self.env.ref(xmlid, raise_if_not_found=False)
            if root:
                break
        if not root:
            data = self.env['ir.model.data'].sudo().search([
                ('model', '=', 'ir.ui.menu'),
                ('module', 'in', ('quality', 'quality_control', 'quality_mrp')),
            ])
            candidates = self.sudo().browse(data.mapped('res_id')).exists()
            root = candidates.filtered(lambda m: not m.parent_id)[:1]
        if not root:
            return False
        # Acomodarlos ANTES del menú nativo de Configuración de la app
        # Calidad: los menús conservaban la secuencia del SGI (80/85) y en
        # Calidad eso los mandaba hasta después de Configuración. La
        # secuencia del vecino también se descubre en runtime.
        config = self.sudo().search([
            ('parent_id', '=', root.id), ('name', 'ilike', 'onfig')], limit=1)
        base_seq = max(config.sequence - 2, 0) if config else 50
        for offset, xmlid in enumerate((
                'quimibond_sgi.menu_sgi_automotive',
                'quimibond_sgi.menu_sgi_dashboards')):
            menu = self.env.ref(xmlid, raise_if_not_found=False)
            if not menu:
                continue
            vals = {}
            if menu.parent_id != root:
                vals['parent_id'] = root.id
            if menu.sequence != base_seq + offset:
                vals['sequence'] = base_seq + offset
            if vals:
                menu.write(vals)
        return True
