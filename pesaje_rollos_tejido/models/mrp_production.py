# -*- coding: utf-8 -*-
from odoo import models, fields, api, _
from odoo.exceptions import UserError

class MrpProduction(models.Model):
    _inherit = 'mrp.production'

    roll_count = fields.Integer(string="Contador de Rollos", default=0, copy=False)
    last_zpl_label = fields.Text(string="Última Etiqueta ZPL", readonly=True, copy=False)

    def action_register_roll_with_weight(self, weight):
        """ Registro de rollos de tela con recálculo de consumo inmediato """
        self.ensure_one()
        if weight <= 0:
            raise UserError(_("El peso debe ser mayor a cero."))

        # 1. Incrementar contador y definir nombres
        self.roll_count += 1
        mo_identifier = self.name.split('/')[-1]
        lot_name = f"{mo_identifier}-{self.roll_count:04d}"
        barcode_data = f"{self.name}|{self.roll_count}"

        # 2. Crear Lote para el Rollo (CORRECCIÓN: Incluye Peso y Vínculo)
        new_lot = self.env['stock.lot'].create({
            'name': lot_name,
            'product_id': self.product_id.id,
            'company_id': self.company_id.id,
            'production_id': self.id,
            'product_qty': weight,
        })

        # 3. Gestionar Movimiento de Inventario
        finished_move = self.move_finished_ids.filtered(
            lambda x: x.product_id == self.product_id and x.state not in ('done', 'cancel')
        )[:1]

        if finished_move:
            # CORRECCIÓN PUNTO 1: Limpiar cantidad planificada original
            if finished_move.product_uom_qty != 0:
                finished_move.write({'product_uom_qty': 0})

            current_wo = self.workorder_ids.filtered(lambda w: w.state in ('ready', 'progress'))[:1]
            
            self.env['stock.move.line'].create({
                'move_id': finished_move.id,
                'product_id': self.product_id.id,
                'lot_id': new_lot.id,
                'quantity': weight,
                'location_id': finished_move.location_id.id,
                'location_dest_id': finished_move.location_dest_id.id,
                'workorder_id': current_wo.id if current_wo else False,
                'production_id': self.id,
            })
            
            self.qty_producing += weight
            if current_wo:
                current_wo.qty_produced += weight
            
            # Recálculo de consumos original
            self.move_raw_ids._recompute_state()
            self.move_raw_ids._action_assign()

        # 4. Imprimir etiqueta
        self._print_zpl_label(lot_name, weight, barcode_data)

        # 5. Sorteo de Calidad
        if hasattr(self, '_auto_sorteo_revision'):
            self._auto_sorteo_revision() 
        
        return {'type': 'ir.actions.client', 'tag': 'reload'}

    def action_register_subproduct_manual(self, weight, lot_name=False):
        """ Registro de subproductos íntegro """
        self.ensure_one()
        if weight <= 0:
            raise UserError(_("El peso del subproducto debe ser mayor a cero."))

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

        self.env['stock.move.line'].create({
            'move_id': sub_move.id,
            'product_id': sub_move.product_id.id,
            'lot_id': new_lot.id,
            'quantity': weight,
            'location_id': sub_move.location_id.id,
            'location_dest_id': sub_move.location_dest_id.id,
            'production_id': self.id,
        })

        self.move_raw_ids._recompute_state()
        self.move_raw_ids._action_assign()

        # Calidad Nativa
        quality_checks = self.env['quality.check'].search([
            ('production_id', '=', self.id),
            ('quality_state', '=', 'none')
        ])
        for check in quality_checks:
            if hasattr(check, 'do_pass'):
                check.do_pass()
            else:
                check.write({'quality_state': 'pass', 'user_id': self.env.user.id})

        self._print_subproduct_zpl(sub_move.product_id, weight, lot_name)
        return True

    def _print_zpl_label(self, lot_name, weight, barcode_data):
        zpl = f"""^XA^PW812^LL1218^CI28^FO20,20^GB770,1170,4^FS
^FO50,60^A0N,40,40^FDPRODUCTO: {self.product_id.display_name[:30]}^FS
^FO50,160^A0N,40,40^FDORDEN FAB: {self.name}^FS
^FO180,280^A0N,180,180^FD{weight:0.3f} KG^FS
^FO50,550^A0N,60,60^FDLOTE: {lot_name}^FS
^FO100,700^BQN,2,10^FDQA,{barcode_data}^FS
^XZ"""
        self.last_zpl_label = zpl
        return True

    def _print_subproduct_zpl(self, product, weight, lot_name):
        zpl = f"""^XA^CI28^FO50,50^A0N,40,40^FDSUBPRODUCTO: {product.name}^FS
^FO50,120^A0N,40,40^FDPESO: {weight:0.3f} KG^FS
^FO50,190^A0N,40,40^FDLOTE: {lot_name}^FS^XZ"""
        self.last_zpl_label = zpl
        return True