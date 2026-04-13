from odoo import models, fields, api, _
from odoo.exceptions import UserError
import re


class StockPicking(models.Model):
    _inherit = 'stock.picking'

    show_barcode_scan = fields.Boolean(compute='_compute_show_barcode_scan', store=False)
    barcode_scan_batch = fields.Char(string="Escanear Código", copy=False)

    def action_prepare_for_physical_scan(self):
        """ Lógica Natural: Soltar reserva y borrar líneas """
        for rec in self:
            if rec.state in ['done', 'cancel']:
                raise UserError("No se puede limpiar una operación finalizada.")
            
            # 1. Soltamos lo que Odoo apartó automáticamente
            rec.do_unreserve()
            
            # 2. Borramos las líneas de operación
            rec.move_line_ids.sudo().unlink()
            
            # 3. Ponemos la demanda hecha a cero
            rec.move_ids.sudo().write({'quantity': 0})
            
            # 4. Refrescamos la vista
            rec.flush_recordset()
            rec.invalidate_recordset(['move_line_ids'])
            
        return {'type': 'ir.actions.client', 'tag': 'reload'}
                
    @api.depends('picking_type_id', 'state')
    def _compute_show_barcode_scan(self):
        for rec in self:
            name = (rec.picking_type_id.name or '').upper()
            is_valid = any(kw in name for kw in ['REQUISICI', 'FORMACI', 'DESPERDICIO'])
            rec.show_barcode_scan = is_valid and rec.state not in ['done', 'cancel']

    @api.onchange('barcode_scan_batch')
    def _onchange_barcode_scan_batch(self):
        if not self.barcode_scan_batch:
            return

        barcode = self.barcode_scan_batch
        self.barcode_scan_batch = False 
        op_name = (self.picking_type_id.name or '').upper()
        
        # Variables de control para el flujo
        qty_done = 0.0
        already_processed = False

        # ---------------------------------------------------------
        # 1. LIMPIEZA Y BÚSQUEDA DIFUSA (LÓGICA ORIGINAL)
        # ---------------------------------------------------------
        clean_search = re.sub(r'[^a-zA-Z0-9]', '', barcode)
        lot = self.env['stock.lot'].search([('name', '=', barcode)], limit=1)
        
        if not lot:
            product_ids = self.move_ids.product_id.ids
            all_lots = self.env['stock.lot'].search([('product_id', 'in', product_ids)])
            for l in all_lots:
                if re.sub(r'[^a-zA-Z0-9]', '', l.name or '') == clean_search:
                    lot = l
                    break

        if not lot:
            raise UserError(_("Lote no encontrado: %s") % barcode)

        # ---------------------------------------------------------
        # 2. VALIDACIONES ESPECÍFICAS SEGÚN LA OPERACIÓN
        # ---------------------------------------------------------

        # CASO A: REQUISICIÓN MP
        if 'REQUISICI' in op_name:
            if self.move_line_ids.filtered(lambda x: x.lot_id.id == lot.id and x.quantity > 0):
                raise UserError(_("La caja con el lote %s ya ha sido escaneada.") % lot.name)

            quant = self.env['stock.quant'].search([
                ('lot_id', '=', lot.id),
                ('location_id', '=', self.location_id.id),
                ('quantity', '>', 0)
            ], limit=1)
            
            if not quant:
                raise UserError(_("La caja %s no tiene existencias en %s.") % (lot.name, self.location_id.name))
            qty_done = quant.quantity



        # CASO B: FORMACIÓN DE BAÑOS (Tela 99999-9999)
        elif 'FORMACI' in op_name:
            if not re.match(r'^\d{5}-\d+$', barcode):
                 raise UserError(_("Formato de tela inválido para Baños. Debe ser: 99999-9999."))
        
            mo_part = barcode.split('-')[0]
            if mo_part not in (self.origin or ''):
               raise UserError(_("La tela %s no pertenece a la Orden de Fabricación %s.") % (barcode, self.origin))
        
            # VALIDACIÓN DE PRECARGADOS
            #existing_line = self.move_line_ids.filtered(lambda ml: ml.lot_id.id == lot.id)
            existing_line = self.move_line_ids.filtered(lambda ml: ml.lot_id == lot)
            if existing_line:
                return {
                    'type': 'ir.actions.client',
                    'tag': 'display_notification',
                    'params': {
                        'title': 'Rollo ya registrado',
                        'message': 'Este rollo ya fue registrado',
                        'sticky': True,  # True para que no desaparezca solo
                        'type': 'warning', # 'success', 'warning', 'danger', 'info'
                        'next': {'type': 'ir.actions.client', 'tag': 'reload'}, # Refresca la vista atrás
                    }
                }
                if existing_line[0].quantity <= 0:
                    existing_line[0].quantity = existing_line[0].move_id.product_uom_qty or 1.0
                    return {'type': 'ir.actions.client', 'tag': 'reload'}
                already_processed = True
               
            else:
                # SI NO ESTÁ PRECARGADO: Buscar stock real para dar de ALTA (Lo que faltaba)
                quant = self.env['stock.quant'].search([
                    ('lot_id', '=', lot.id),
                    ('location_id', '=', self.location_id.id),
                    ('quantity', '>', 0)
                ], limit=1)
                if not quant:
                    raise UserError(_("La tela %s no tiene stock en %s.") % (lot.name, self.location_id.name))
                qty_done = quant.quantity

        # CASO C: DESPERDICIO TEJIDO (Subproducto SUB-)
        elif 'DESPERDICIO' in op_name:
            if not barcode.startswith('SUB-') or not re.match(r'^SUB-\d+-\d{4}-\d{2}-\d{2}$', barcode):
                raise UserError(_("Formato de subproducto inválido para Desperdicio. Debe ser: SUB-MO-AAAA-MM-DD."))
        
            mo_part = barcode.split('-')[1]
            if mo_part not in (self.origin or ''):
                raise UserError(_("El subproducto %s no pertenece a la Orden de Fabricación %s.") % (barcode, self.origin))

            # Verificar si ya está precargado en las líneas de la operación
            #existing_line = self.move_line_ids.filtered(lambda ml: ml.lot_id.id == lot.id)
            existing_line = self.move_line_ids.filtered(lambda ml: ml.lot_id == lot)
            if existing_line:
                return {
                    'type': 'ir.actions.client',
                    'tag': 'display_notification',
                    'params': {
                        'title': 'Subproducto ya registrado',
                        'message': 'Este Subproducto ya fue registrado',
                        'sticky': True,  # True para que no desaparezca solo
                        'type': 'warning', # 'success', 'warning', 'danger', 'info'
                        'next': {'type': 'ir.actions.client', 'tag': 'reload'}, # Refresca la vista atrás
                    }
                }
                if existing_line[0].quantity <= 0:
                    existing_line[0].quantity = existing_line[0].move_id.product_uom_qty or 1.0
                    return {'type': 'ir.actions.client', 'tag': 'reload'}
                already_processed = True
            else:
                # Si no está precargado, buscamos existencias reales en la ubicación
                quant = self.env['stock.quant'].search([
                     ('lot_id', '=', lot.id),
                     ('location_id', '=', self.location_id.id),
                     ('quantity', '>', 0)
                ], limit=1)
                if not quant:
                    raise UserError(_("El lote %s no tiene existencias en la ubicación %s.") % (lot.name, self.location_id.name))
                qty_done = quant.quantity

        # ---------------------------------------------------------
        # 3. PROCESAMIENTO TÉCNICO Y PERSISTENCIA
        # ---------------------------------------------------------
        #if not already_processed and lot:
        # ---------------------------------------------------------
        # 3. PROCESAMIENTO TÉCNICO Y PERSISTENCIA (Ajustado)
        # ---------------------------------------------------------
        if not already_processed and (qty_done > 0 or 'REQUISICI' in op_name):
            # Identificamos el ID real (origin) para asegurar persistencia en Odoo.sh
            picking_id = self._origin.id if self._origin else self.id
            
            # Buscamos el movimiento de demanda
            move = self.move_ids.filtered(lambda m: m.product_id == lot.product_id and m.state not in ['done', 'cancel'])[:1]
            
            if move:
                # VALIDACIÓN DE DUPLICADOS: Buscamos en la DB si este lote ya se guardó para este picking
                existing_line = self.env['stock.move.line'].sudo().search([
                    ('picking_id', '=', picking_id),
                    ('lot_id', '=', lot.id),
                    ('quantity', '>', 0)
                ], limit=1)

                if existing_line:
                    raise UserError(_("Este lote (%s) ya fue guardado físicamente en la base de datos.") % lot.name)

                # CREACIÓN FÍSICA: Usamos sudo().create para forzar la escritura inmediata en disco
                self.env['stock.move.line'].sudo().create({
                    'picking_id': picking_id,
                    'move_id': move._origin.id if move._origin else move.id,
                    'product_id': lot.product_id.id,
                    'lot_id': lot.id,
                    'quantity': qty_done,
                    'location_id': self.location_id.id,
                    'location_dest_id': self.location_dest_id.id,
                    'product_uom_id': lot.product_id.uom_id.id,
                })
              
                # El reload obliga a la interfaz a leer lo que acabamos de escribir en la DB
                return {'type': 'ir.actions.client', 'tag': 'reload'}
            else:
                raise UserError(_("El producto %s no es requerido en este documento.") % lot.product_id.display_name)

    def button_validate(self):
        """ VALIDACIÓN POR CANTIDAD TOTAL """
        for rec in self:
            op_name = (rec.picking_type_id.name or '').upper()
            if any(kw in op_name for kw in ['FORMACI', 'DESPERDICIO']):
                
                # Sumamos la cantidad de todos los rollos leídos físicamente
                total_scanned_qty = sum(rec.move_line_ids.mapped('quantity'))
                
                # Sumamos la cantidad total requerida en la demanda original (move_ids)
                # Usamos product_uom_qty que es la demanda teórica
                total_demanded_qty = sum(rec.move_ids.mapped('product_uom_qty'))

                # Comparamos las sumas de cantidades
                if total_scanned_qty < total_demanded_qty:
                    raise UserError(_(
                        "CANTIDAD INSUFICIENTE:\n"
                        "- Escaneado: %s\n"
                        "- Demandado: %s\n"
                        "Debe escanear más rollos hasta completar la cantidad requerida."
                    ) % (total_scanned_qty, total_demanded_qty))
                    
                if total_scanned_qty > total_demanded_qty:
                    raise UserError(_(
                        "EXCESO DE CANTIDAD:\n"
                        "- Escaneado: %s\n"
                        "- Demandado: %s\n"
                        "No puede validar una cantidad mayor a la demandada."
                    ) % (total_scanned_qty, total_demanded_qty))
        
        return super(StockPicking, self).button_validate()