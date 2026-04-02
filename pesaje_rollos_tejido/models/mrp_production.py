# -*- coding: utf-8 -*-
from odoo import models, fields, api, _
from odoo.exceptions import UserError
import datetime

class MrpProduction(models.Model):
    _inherit = 'mrp.production'

    roll_count = fields.Integer(string="Contador de Rollos", default=0, copy=False)
    last_zpl_label = fields.Text(string="Última Etiqueta ZPL", readonly=True, copy=False)

    def action_register_roll_with_weight(self, weight):
        """ 
        Registro de rollos con vinculación de lote y limpieza de movimiento total.
        Punto 1: Modificado para incluir Fecha, Hora y Centro de Trabajo en la etiqueta.
        """
        self.ensure_one()
        if weight <= 0:
            raise UserError(_("El peso debe ser mayor a cero."))

        self.roll_count += 1
        mo_identifier = self.name.split('/')[-1]
        lot_name = f"{mo_identifier}-{self.roll_count:04d}"
        barcode_data = f"{self.name}|{self.roll_count}"

        # 1. Crear Lote con peso inicial
        new_lot = self.env['stock.lot'].create({
            'name': lot_name,
            'product_id': self.product_id.id,
            'company_id': self.company_id.id,
            'production_id': self.id,
            'product_qty': weight, 
        })

        # 2. Lógica de limpieza y registro de movimiento (Mantenida intacta)
        finished_move = self.move_finished_ids.filtered(
            lambda x: x.product_id == self.product_id and x.state not in ('done', 'cancel')
        )[:1]

        if finished_move:
            if finished_move.product_uom_qty != 0:
                finished_move.write({'product_uom_qty': 0})

            current_wo = self.workorder_ids.filtered(lambda w: w.state in ('ready', 'progress'))[:1]

            self.env['stock.move.line'].create({
                'move_id': finished_move.id,
                'product_id': self.product_id.id,
                'lot_id': new_lot.id,
                'lot_name': lot_name,
                'quantity': weight,
                'location_id': finished_move.location_id.id,
                'location_dest_id': finished_move.location_dest_id.id,
                'workorder_id': current_wo.id if current_wo else False,
                'production_id': self.id,
            })

            self.qty_producing += weight
            if current_wo:
                current_wo.qty_produced += weight

        self.move_raw_ids._recompute_state()
        self.move_raw_ids._action_assign()

        # Validación automática de Calidad Nativa (Mantenida)
        quality_checks = self.env['quality.check'].search([
            ('production_id', '=', self.id),
            ('quality_state', '=', 'none')
        ])
        for check in quality_checks:
            if hasattr(check, 'do_pass'):
                check.do_pass()
            else:
                check.write({'quality_state': 'pass', 'user_id': self.env.user.id})

        # Punto 1: Impresión de etiqueta de Producto (Rollo)
        self._print_zpl_label(lot_name, weight, barcode_data)
        
        return {'type': 'ir.actions.client', 'tag': 'reload'}

    def action_register_subproduct_manual(self, weight, lot_name=False):
        """ 
        Registro de subproductos con validación contra el historial de revisión.
        Punto 10 y 11 del documento.
        """
        self.ensure_one()
        if weight <= 0:
            raise UserError(_("El peso debe ser mayor a cero."))

        # Punto 10: Validación del total del subproducto vs diferencia acumulada en revisión
        # revision_log_ids es el campo definido en el módulo mrp_revisado_telas
        total_diff_revisado = sum(self.revision_log_ids.mapped('diferencia'))
        
        if weight > total_diff_revisado:
            raise UserError(_("Error: El peso del subproducto (%.3f) no puede exceder la suma de diferencias de revisión (%.3f).") % (weight, total_diff_revisado))

        sub_move = self.move_byproduct_ids.filtered(lambda x: x.state not in ('done', 'cancel'))[:1]
        if not sub_move:
            raise UserError(_("No hay subproductos pendientes en esta orden."))

        if not lot_name:
            mo_identifier = self.name.split('/')[-1]
            lot_name = f"SUB-{mo_identifier}-{fields.Date.today()}"
        
        new_lot = self.env['stock.lot'].create({
            'name': lot_name,
            'product_id': sub_move.product_id.id,
            'company_id': self.company_id.id,
        })

        current_wo = self.workorder_ids.filtered(lambda w: w.state in ('ready', 'progress'))[:1]

        self.env['stock.move.line'].create({
            'move_id': sub_move.id,
            'product_id': sub_move.product_id.id,
            'lot_id': new_lot.id,
            'lot_name': lot_name,
            'quantity': weight,
            'location_id': sub_move.location_id.id,
            'location_dest_id': sub_move.location_dest_id.id,
            'workorder_id': current_wo.id if current_wo else False,
            'production_id': self.id,
        })

        self.qty_producing += weight
        if current_wo:
            current_wo.qty_produced += weight

        # Punto 11: Registrar la diferencia como merma si el subproducto es menor a la revisión
        diff_merma = total_diff_revisado - weight
        if diff_merma > 0:
            scrap_location = self.env['stock.location'].search([('scrap_location', '=', True)], limit=1)
            self.env['stock.scrap'].create({
                'production_id': self.id,
                'product_id': self.product_id.id, # Merma del producto principal tejido
                'scrap_qty': diff_merma,
                'location_id': self.location_src_id.id,
                'scrap_location_id': scrap_location.id,
            })

        self.move_raw_ids._recompute_state()
        self.move_raw_ids._action_assign()

        # Punto 3: Impresión de etiqueta de Subproducto
        self._print_subproduct_zpl(sub_move.product_id, weight, lot_name)
        return True

    def _print_zpl_label(self, lot_name, weight, barcode_data):
        """ Genera etiqueta ZPL para el rollo (Pesaje y Revisado) """
        self.ensure_one()
        # Punto 1: Obtener fecha, hora y centro de trabajo actual
        ahora = fields.Datetime.context_timestamp(self, datetime.datetime.now()).strftime('%d/%m/%Y %H:%M')
        current_wo = self.workorder_ids.filtered(lambda w: w.state in ('ready', 'progress'))[:1]
        wc_name = current_wo.workcenter_id.name if current_wo else "N/A"

        zpl = f"""^XA^PW812^LL1218^CI28^FO20,20^GB770,1170,4^FS
^FO50,60^A0N,30,30^FDFECHA: {ahora}^FS
^FO50,110^A0N,40,40^FDPRODUCTO: {self.product_id.display_name[:30]}^FS
^FO50,170^A0N,30,30^FDWC: {wc_name} | OF: {self.name}^FS
^FO180,280^A0N,180,180^FD{weight:0.3f} KG^FS
^FO50,550^A0N,60,60^FDLOTE: {lot_name}^FS
^FO100,700^BQN,2,10^FDQA,{lot_name}^FS^XZ"""
        self.last_zpl_label = zpl
        return True

    def _print_subproduct_zpl(self, product, weight, lot_name):
        """ Genera etiqueta ZPL para el subproducto """
        self.ensure_one()
        # Punto 3: Datos requeridos para subproducto
        ahora = fields.Datetime.context_timestamp(self, datetime.datetime.now()).strftime('%d/%m/%Y %H:%M')
        
        zpl = f"""^XA^CI28^FO50,50^A0N,30,30^FDFECHA: {ahora}^FS
^FO50,100^A0N,40,40^FDPRODUCTO: {product.name[:30]}^FS
^FO50,160^A0N,40,40^FDLOTE: {lot_name}^FS
^FO50,220^A0N,60,60^FDPESO: {weight:0.3f} KG^FS
^FO100,320^BQN,2,8^FDQA,{lot_name}^FS^XZ"""
        self.last_zpl_label = zpl
        return True
    
class StockLot(models.Model):
    _inherit = 'stock.lot'

    production_id = fields.Many2one('mrp.production', string="Orden de Fabricación")
    needs_review = fields.Boolean(string="Necesita Revisión", default=False)
    is_reviewed = fields.Boolean(string="Revisado", default=False)