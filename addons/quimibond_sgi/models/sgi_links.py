# -*- coding: utf-8 -*-
"""Campos de liga entre la entrada y la salida de las actividades del SGI
(2026-09-25). Una actividad se mide cuando la salida (el registro que se
produce) apunta a la entrada (el registro que la disparó); si los modelos son
distintos el eslabón necesita un campo real (`sgi.activity.input.match_path`).
Estos son los campos que faltaban:

- C1 (desarrollo): la tarea del desarrollo (proyecto FT-…) en el AMEF, la
  lista de materiales, la orden de muestra, el plan de control y el PPAP; el
  plan de control en el producto.
- S5.06: la NC guarda la solicitud de mantenimiento de la que salió.
- S1.09: la orden de compra guarda la requisición aprobada (approval.request).
- S2.08: la factura guarda las entregas que factura.
- C2.34: el acuse adjunto guarda su entrega (y la entrega, sus acuses).
- C4.19: el traspaso a liberación guarda su orden de producción.
"""
from odoo import Command, api, fields, models


class SgiDydTaskMixin(models.AbstractModel):
    """Liga a la tarea del proyecto de desarrollo (Diseño y Desarrollo)."""
    _name = 'sgi.dyd.task.mixin'
    _description = "Liga a la tarea del desarrollo"

    sgi_dyd_task_id = fields.Many2one(
        'project.task', string="Tarea del desarrollo", index=True, ondelete='set null',
        copy=False,
        help="Tarea del proyecto de Diseño y Desarrollo (FT-…) de la que sale este "
             "registro. Es la liga que mide las actividades C1.")


class SgiFmeaLink(models.Model):
    _name = 'sgi.fmea'
    _inherit = ['sgi.fmea', 'sgi.dyd.task.mixin']


class MrpBomLink(models.Model):
    _name = 'mrp.bom'
    _inherit = ['mrp.bom', 'sgi.dyd.task.mixin']


class SgiControlPlanLink(models.Model):
    _name = 'sgi.control.plan'
    _inherit = ['sgi.control.plan', 'sgi.dyd.task.mixin']


class SgiPpapLink(models.Model):
    _name = 'sgi.ppap'
    _inherit = ['sgi.ppap', 'sgi.dyd.task.mixin']


class MrpProductionLink(models.Model):
    _name = 'mrp.production'
    _inherit = ['mrp.production', 'sgi.dyd.task.mixin']

    sgi_release_picking_ids = fields.One2many(
        'stock.picking', 'sgi_production_id', string="Traspasos a liberación",
        help="Traspasos (a liberación u otros) que salieron de esta orden (C4.19).")


class ProjectTaskLinks(models.Model):
    _inherit = 'project.task'

    sgi_fmea_ids = fields.One2many('sgi.fmea', 'sgi_dyd_task_id', string="AMEF")
    sgi_bom_ids = fields.One2many('mrp.bom', 'sgi_dyd_task_id', string="Listas de materiales")
    sgi_production_ids = fields.One2many('mrp.production', 'sgi_dyd_task_id', string="Órdenes de muestra")
    sgi_control_plan_ids = fields.One2many('sgi.control.plan', 'sgi_dyd_task_id', string="Planes de control")
    sgi_ppap_ids = fields.One2many('sgi.ppap', 'sgi_dyd_task_id', string="PPAP")
    sgi_dyd_link_count = fields.Integer(compute='_compute_sgi_dyd_link_count', string="Ligas del desarrollo")

    @api.depends('sgi_fmea_ids', 'sgi_bom_ids', 'sgi_production_ids', 'sgi_control_plan_ids', 'sgi_ppap_ids')
    def _compute_sgi_dyd_link_count(self):
        for task in self:
            task.sgi_dyd_link_count = (len(task.sgi_fmea_ids) + len(task.sgi_bom_ids)
                                       + len(task.sgi_production_ids) + len(task.sgi_control_plan_ids)
                                       + len(task.sgi_ppap_ids))


class ProductTemplateLink(models.Model):
    _inherit = 'product.template'

    sgi_control_plan_id = fields.Many2one(
        'sgi.control.plan', string="Plan de control", compute='_compute_sgi_control_plan_id',
        store=True, readonly=False, index=True, ondelete='set null',
        help="Plan de control vigente del artículo (C1.17). Se propone el último plan "
             "que lo incluye; se puede cambiar a mano.")

    def _compute_sgi_control_plan_id(self):
        Plan = self.env['sgi.control.plan']
        for template in self:
            if template.sgi_control_plan_id:
                continue
            plan = Plan.search([('product_tmpl_ids', 'in', template.ids)], order='id desc', limit=1) \
                if template.id else Plan
            template.sgi_control_plan_id = plan.id if plan else False


class SgiControlPlanProposesToProducts(models.Model):
    _inherit = 'sgi.control.plan'

    # 57.13.0: el plan del artículo es un compute guardado sin dependencias
    # (el producto no tiene el inverso de product_tmpl_ids), así que solo
    # corría al crear el producto, cuando aún ningún plan lo incluía: nunca
    # se proponía. Ahora lo propone el plan al incluir el artículo, a los que
    # todavía no tienen plan (el que se puso a mano no se toca).
    def _sgi_propose_to_products(self):
        templates = self.sudo().product_tmpl_ids.filtered(lambda t: not t.sgi_control_plan_id)
        for template in templates:
            template._compute_sgi_control_plan_id()

    @api.model_create_multi
    def create(self, vals_list):
        plans = super().create(vals_list)
        plans._sgi_propose_to_products()
        return plans

    def write(self, vals):
        res = super().write(vals)
        if 'product_tmpl_ids' in vals:
            self._sgi_propose_to_products()
        return res


class QualityAlertLink(models.Model):
    _inherit = 'quality.alert'

    sgi_maintenance_request_id = fields.Many2one(
        'maintenance.request', string="Solicitud de mantenimiento", index=True,
        ondelete='set null', copy=False,
        help="Solicitud correctiva de la que salió esta NC (S5.06).")


class PurchaseOrderLink(models.Model):
    _inherit = 'purchase.order'

    sgi_approval_request_id = fields.Many2one(
        'approval.request', string="Requisición aprobada", index=True, ondelete='set null',
        copy=False,
        help="Solicitud de aprobación (requisición de compra) de la que salió esta "
             "orden (S1.09). Se llena sola al crear la orden desde la requisición.")


class ApprovalRequestLink(models.Model):
    _inherit = 'approval.request'


    def action_create_purchase_orders(self):
        """approvals_purchase crea las órdenes desde las líneas; aquí se les
        deja escrita la requisición de la que salieron."""
        res = super().action_create_purchase_orders()
        self._sgi_link_purchase_orders()
        return res

    def _sgi_link_purchase_orders(self):
        Line = self.env['approval.product.line'] if 'approval.product.line' in self.env else None
        if Line is None or 'purchase_order_line_id' not in Line._fields:
            return
        for request in self:
            orders = request.product_line_ids.purchase_order_line_id.order_id
            orders.filtered(lambda o: not o.sgi_approval_request_id).write(
                {'sgi_approval_request_id': request.id})


class AccountMoveLink(models.Model):
    """S2.08: entregas que factura la factura. 57.95.0 (K-05): lo propuesto
    (entregas hechas de las líneas, sin guardar) va aparte de lo ajustado a
    mano (``sgi_picking_manual_ids``, guardado y no calculado, que solo llena
    ``write``). El cálculo de ``sgi_picking_ids`` siempre asigna: lo ajustado
    si hay ajuste; si no, lo propuesto."""
    _inherit = 'account.move'

    sgi_picking_ids = fields.Many2many(
        'stock.picking', 'sgi_move_picking_rel', 'move_id', 'picking_id',
        string="Entregas facturadas", compute='_compute_sgi_picking_ids', store=True,
        readonly=False, copy=False,
        help="Entregas (o recepciones) que esta factura cobra (S2.08). Se proponen "
             "desde las líneas del pedido; se pueden ajustar a mano.")
    sgi_picking_proposed_ids = fields.Many2many(
        'stock.picking', string="Entregas propuestas", compute='_compute_sgi_picking_proposed_ids',
        compute_sudo=True,
        help="Entregas hechas de las líneas de la factura: lo que el sistema propone.")
    sgi_picking_manual = fields.Boolean(
        string="Entregas ajustadas a mano", readonly=True, copy=False,
        help="Alguien cambió a mano las entregas facturadas: el sistema ya no las reemplaza "
             "con las propuestas. «Volver a las entregas propuestas» lo quita.")
    sgi_picking_manual_ids = fields.Many2many(
        'stock.picking', 'sgi_move_picking_manual_rel', 'move_id', 'picking_id',
        string="Entregas ajustadas a mano (guardadas)", readonly=True, copy=False,
        help="Las entregas que alguien escribió a mano; las conserva aunque cambie lo propuesto.")
    sgi_picking_outdated = fields.Boolean(
        string="Entregas distintas de las propuestas", compute='_compute_sgi_picking_outdated',
        compute_sudo=True,
        help="Las entregas guardadas no son las que el sistema propone hoy.")

    _SGI_PICKING_DEPENDS = ('move_type',
                            'invoice_line_ids.sale_line_ids.move_ids.picking_id.state',
                            'invoice_line_ids.purchase_line_id.move_ids.picking_id.state')

    def _sgi_proposed_pickings(self):
        self.ensure_one()
        Picking = self.env['stock.picking']
        if not self.is_invoice(include_receipts=True):
            return Picking
        lines = self.invoice_line_ids
        pickings = lines.sale_line_ids.move_ids.picking_id | lines.purchase_line_id.move_ids.picking_id
        return pickings.filtered(lambda p: p.state == 'done')

    @api.depends(*_SGI_PICKING_DEPENDS)
    def _compute_sgi_picking_proposed_ids(self):
        for move in self:
            move.sgi_picking_proposed_ids = move._sgi_proposed_pickings()

    # 57.95.0 (K-05): antes no dependía del estado de la entrega (una factura
    # hecha antes de validarla se quedaba vacía) y pisaba lo ajustado a mano.
    @api.depends('sgi_picking_manual', 'sgi_picking_manual_ids', *_SGI_PICKING_DEPENDS)
    def _compute_sgi_picking_ids(self):
        for move in self:
            if not move.is_invoice(include_receipts=True):
                move.sgi_picking_ids = False
                continue
            if move.sgi_picking_manual:
                move.sgi_picking_ids = move.sgi_picking_manual_ids
                continue
            proposed = move._sgi_proposed_pickings()
            if not proposed:
                # Sin propuesta se conserva lo guardado: protege los ajustes a
                # mano de antes de 57.95.0, que no traen la marca. En el
                # formulario (_origin) es lo guardado en la base, y el
                # onchange lo reenvía igual (write no lo toma por ajuste).
                kept = move._origin.sgi_picking_ids if move._origin else move.sgi_picking_manual_ids
                move.sgi_picking_ids = kept or move.sgi_picking_manual_ids
                continue
            move.sgi_picking_ids = proposed

    @api.depends('sgi_picking_ids', *_SGI_PICKING_DEPENDS)
    def _compute_sgi_picking_outdated(self):
        for move in self:
            move.sgi_picking_outdated = bool(move.is_invoice(include_receipts=True)) \
                and move.sgi_picking_ids != move._sgi_proposed_pickings()

    def write(self, vals):
        # La marca de ajuste a mano se decide DESPUÉS de escribir y solo si lo
        # escrito difiere de lo propuesto: el formulario de la factura manda
        # ``sgi_picking_ids`` al guardar cada vez que un onchange lo recalcula;
        # marcarlo siempre que viniera en ``vals`` congelaría facturas que
        # nadie ajustó. Todo se compara en sudo y por ids: una regla de
        # registro sobre las entregas no debe hacer parecer «a mano» una
        # factura que nadie tocó.
        track = 'sgi_picking_ids' in vals and 'sgi_picking_manual' not in vals
        before = {move.id: set(move.sudo().sgi_picking_ids.ids) for move in self} if track else {}
        res = super().write(vals)
        if track:
            for move in self.sudo():
                written = set(move.sgi_picking_ids.ids)
                proposed = set(move._sgi_proposed_pickings().ids)
                if written == proposed:
                    if move.sgi_picking_manual or move.sgi_picking_manual_ids:
                        super(AccountMoveLink, move).write({
                            'sgi_picking_manual': False,
                            'sgi_picking_manual_ids': [Command.clear()]})
                    continue
                if not proposed and written == before.get(move.id) and not move.sgi_picking_manual:
                    # Lo guardado que se conservó sin propuesta (ajuste de
                    # antes de 57.95.0) y que el formulario reenvía igual.
                    continue
                if not move.sgi_picking_manual or set(move.sgi_picking_manual_ids.ids) != written:
                    super(AccountMoveLink, move).write({
                        'sgi_picking_manual': True,
                        'sgi_picking_manual_ids': [Command.set(list(written))]})
        return res

    def action_sgi_picking_reset(self):
        """«Volver a las entregas propuestas»: quita el ajuste a mano y pone
        lo propuesto, aunque venga vacío (también quita lo que se conservaba
        sin propuesta)."""
        for move in self:
            proposed = move.sudo()._sgi_proposed_pickings()
            super(AccountMoveLink, move).write({
                'sgi_picking_manual': False,
                'sgi_picking_manual_ids': [Command.clear()],
                'sgi_picking_ids': [Command.set(proposed.ids)]})
        return True


class IrAttachmentLink(models.Model):
    _inherit = 'ir.attachment'

    sgi_picking_id = fields.Many2one(
        'stock.picking', string="Entrega del acuse", index=True, ondelete='set null',
        help="Entrega a la que pertenece este acuse firmado (C2.34).")

    @api.model_create_multi
    def create(self, vals_list):
        # Un acuse que se adjunta a la entrega (nombre ACUSE-…) queda ligado
        # por campo, no solo por res_model/res_id.
        for vals in vals_list:
            if (vals.get('res_model') == 'stock.picking' and vals.get('res_id')
                    and not vals.get('sgi_picking_id')
                    and (vals.get('name') or '').upper().startswith('ACUSE')):
                vals['sgi_picking_id'] = vals['res_id']
        return super().create(vals_list)


class StockPickingLink(models.Model):
    _inherit = 'stock.picking'

    sgi_production_id = fields.Many2one(
        'mrp.production', string="Orden de producción", compute='_compute_sgi_production_id',
        store=True, readonly=False, index=True, ondelete='set null', copy=False,
        help="Orden de producción de la que sale este traspaso (C4.19). Se propone "
             "desde los movimientos o el documento origen; se puede fijar a mano.")
    sgi_acuse_attachment_ids = fields.One2many(
        'ir.attachment', 'sgi_picking_id', string="Acuses firmados")
    sgi_acuse_count = fields.Integer(compute='_compute_sgi_acuse_count', string="Acuses")

    @api.depends('move_ids.production_id', 'move_ids.raw_material_production_id', 'origin')
    def _compute_sgi_production_id(self):
        Production = self.env['mrp.production']
        for picking in self:
            if picking.sgi_production_id:
                continue
            production = picking.move_ids.production_id[:1] or picking.move_ids.raw_material_production_id[:1]
            if not production and picking.origin:
                production = Production.search([('name', '=', picking.origin.strip())], limit=1)
            picking.sgi_production_id = production.id if production else False

    @api.depends('sgi_acuse_attachment_ids')
    def _compute_sgi_acuse_count(self):
        for picking in self:
            picking.sgi_acuse_count = len(picking.sgi_acuse_attachment_ids)

    def action_sgi_attach_acuse(self):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window', 'name': "Adjuntar acuse firmado",
            'res_model': 'sgi.acuse.attach.wizard', 'view_mode': 'form', 'target': 'new',
            'context': {'default_picking_id': self.id},
        }

    def action_sgi_open_acuses(self):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window', 'name': "Acuses — %s" % self.name,
            'res_model': 'ir.attachment', 'view_mode': 'list,form',
            'domain': [('sgi_picking_id', '=', self.id)],
        }


class SgiAcuseAttachWizard(models.TransientModel):
    """Asistente para adjuntar a una entrega (``stock.picking``) el acuse firmado por el cliente."""
    _name = 'sgi.acuse.attach.wizard'
    _description = "Adjuntar acuse firmado a la entrega"

    picking_id = fields.Many2one('stock.picking', required=True,
                                 help="Entrega a la que se adjunta el acuse firmado.")
    file = fields.Binary(string="Acuse firmado", required=True)
    file_name = fields.Char(string="Nombre del archivo")
    note = fields.Char(string="Nota")

    def action_attach(self):
        self.ensure_one()
        picking = self.picking_id
        name = "ACUSE-%s%s" % (picking.name, self._sgi_extension())
        attachment = self.env['ir.attachment'].create({
            'name': name, 'datas': self.file, 'res_model': 'stock.picking', 'res_id': picking.id,
            'sgi_picking_id': picking.id, 'description': self.note or False,
        })
        picking.message_post(body="Acuse firmado adjuntado: %s" % name, attachment_ids=[attachment.id])
        return {'type': 'ir.actions.act_window_close'}

    def _sgi_extension(self):
        name = self.file_name or ''
        return name[name.rfind('.'):] if '.' in name else '.pdf'
