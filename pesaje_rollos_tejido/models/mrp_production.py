# -*- coding: utf-8 -*-
from odoo import models, fields, api, _
from odoo.exceptions import UserError

class MrpProduction(models.Model):
    _inherit = 'mrp.production'

    roll_count = fields.Integer(string="Contador de Rollos", default=0, copy=False)
    last_zpl_label = fields.Text(string="Última Etiqueta ZPL", readonly=True, copy=False)

    def action_register_roll_with_weight(self, weight):
        self.ensure_one()
        
        if weight <= 0:
            raise UserError(_("El peso debe ser mayor a cero."))

        self.roll_count += 1
        
        # 1. Intentar obtener un identificador de lote de la MO de forma segura
        # Si 'lot_producing_id' no existe, usamos el nombre de la MO
        mo_identifier = ""
        if hasattr(self, 'lot_producing_id') and self.lot_producing_id:
            mo_identifier = self.lot_producing_id.name
        else:
            # Limpiamos el nombre de la MO (ej. WH/MO/0001 -> 0001)
            mo_identifier = self.name.split('/')[-1]

        ref = self.product_id.default_code or "TELA"
        lot_name = f"{ref}{mo_identifier}-{self.roll_count:04d}"
        
        mo_clean = self.name.replace('/', '-')
        barcode_data = f"{ref}|{mo_clean}|{lot_name}|R{self.roll_count}"

        # 2. Crear el Lote
        new_lot = self.env['stock.lot'].create({
            'name': lot_name,
            'product_id': self.product_id.id,
            'company_id': self.company_id.id,
        })

        # 3. Registrar movimiento
        current_wo = self.workorder_ids.filtered(lambda w: w.state == 'progress')[:1]
        if not current_wo:
            current_wo = self.workorder_ids.filtered(lambda w: w.state == 'ready')[:1]

        finished_move = self.move_finished_ids.filtered(
            lambda m: m.product_id == self.product_id and m.state not in ['done', 'cancel']
        )[:1]

        if finished_move:
            self.env['stock.move.line'].create({
                'move_id': finished_move.id,
                'product_id': self.product_id.id,
                'lot_id': new_lot.id,
                'quantity': weight,
                'location_id': finished_move.location_id.id,
                'location_dest_id': finished_move.location_dest_id.id,
                'workorder_id': current_wo.id, 
                'production_id': self.id,
            })
            
            if current_wo:
                current_wo.qty_produced += weight

        self._print_zpl_label(lot_name, weight, barcode_data)
        
        return {'type': 'ir.actions.client', 'tag': 'reload'}

    def _print_zpl_label(self, lot_name, weight, barcode_data):
        # (El resto de la función _print_zpl_label se mantiene igual que la anterior)
        zpl_content = f"""^XA^PW812^LL1218^CI28
^FO20,20^GB770,1170,4^FS
^FO50,60^A0N,40,40^FDPRODUCTO: {self.product_id.display_name[:30]}^FS
^FO50,110^A0N,40,40^FDREF: {self.product_id.default_code or 'N/A'}^FS
^FO50,160^A0N,40,40^FDORDEN FAB: {self.name}^FS
^FO20,210^GB770,3,3^FS
^FO180,280^A0N,180,180^FD{weight:0.3f} KG^FS
^FO20,500^GB770,3,3^FS
^FO50,550^A0N,60,60^FDLOTE: {lot_name}^FS
^FO50,620^A0N,40,40^FDCONSECUTIVO: Rollo #{self.roll_count}^FS
^FO80,750^BY2,3,250^BCN,300,Y,N,N^FD{barcode_data}^FS
^FO50,1130^A0N,25,25^FDFECHA: {fields.Date.today()}^FS
^XZ"""
        self.write({'last_zpl_label': zpl_content})
        print(zpl_content)
        return True