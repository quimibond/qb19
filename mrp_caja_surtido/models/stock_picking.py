from odoo import models, fields, api, _
from odoo.exceptions import UserError
from odoo.tools import float_compare, float_round
import re
import unicodedata


def _normalize_text(text):
    """ Quita acentos y pasa a mayúsculas, para comparar nombres de operación
    de forma robusta sin importar cómo se hayan tecleado los acentos. """
    if not text:
        return ''
    nfkd = unicodedata.normalize('NFKD', text)
    return ''.join(c for c in nfkd if not unicodedata.combining(c)).upper()


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
            name = _normalize_text(rec.picking_type_id.name)
            is_valid = any(kw in name for kw in ['REQUISICI', 'FORMACI', 'DESPERDICIO']) or 'DEVOLUCION PRODUCCION' in name
            rec.show_barcode_scan = is_valid and rec.state not in ['done', 'cancel']

    @api.onchange('barcode_scan_batch')
    def _onchange_barcode_scan_batch(self):
        if not self.barcode_scan_batch:
            return

        barcode = self.barcode_scan_batch
        self.barcode_scan_batch = False 
        op_name = _normalize_text(self.picking_type_id.name)
        
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
            
            raw_quantity = quant.quantity 
            uom = lot.product_id.uom_id
            qty_done = uom.round(raw_quantity) if uom else raw_quantity

        # CASO D: DEVOLUCIÓN PRODUCCIÓN (traslado interno de hilo/material sobrante)
        # Coincidencia exacta de frase para no activarse en otros tipos de devolución
        # (ej. Devolución de Cliente, Devolución de Proveedor).
        elif 'DEVOLUCION PRODUCCION' in op_name:
            if self.move_line_ids.filtered(lambda x: x.lot_id.id == lot.id and x.quantity > 0):
                raise UserError(_("El lote %s ya ha sido escaneado.") % lot.name)

            quant = self.env['stock.quant'].search([
                ('lot_id', '=', lot.id),
                ('location_id', '=', self.location_id.id),
                ('quantity', '>', 0)
            ], limit=1)

            if not quant:
                raise UserError(_("El lote %s no tiene existencias en %s.") % (lot.name, self.location_id.name))

            raw_quantity = quant.quantity
            uom = lot.product_id.uom_id
            qty_done = uom.round(raw_quantity) if uom else raw_quantity

        # CASO B: FORMACIÓN DE BAÑOS (Tela H99999-9999 o 99999-9999)
        elif 'FORMACI' in op_name:
            # REGEX: Valida que contenga letras, números, guiones intermedios y termine estrictamente en -[dígitos]
            if not re.match(r'^[A-Z]?[\w-]+-\d+$', barcode):
                 raise UserError(_("Formato de tela inválido para Baños. Debe terminar en '-[Número de Rollo]' (Ej: 12974-002-0001)."))
        
            # Corta desde el ÚLTIMO guion hacia la izquierda: '99999-001-0001' -> '99999-001'
            mo_part = barcode.rsplit('-', 1)[0]
            if mo_part not in (self.origin or ''):
               raise UserError(_("La tela %s no pertenece a la Orden de Fabricación %s.") % (barcode, self.origin))
        
            # VALIDACIÓN DE PRECARGADOS
            existing_line = self.move_line_ids.filtered(lambda ml: ml.lot_id == lot)
            if existing_line:
                # SE CONSERVA EL CONTROL DE FLUJO ORIGINAL
                already_processed = True
                return {
                    'type': 'ir.actions.client',
                    'tag': 'display_notification',
                    'params': {
                        'title': 'Rollo ya registrado',
                        'message': 'Este rollo ya fue registrado',
                        'sticky': True,
                        'type': 'warning',
                        'next': {'type': 'ir.actions.client', 'tag': 'reload'},
                    }
                }
               
            else:
                # SI NO ESTÁ PRECARGADO: Buscar stock real para dar de ALTA
                quant = self.env['stock.quant'].search([
                    ('lot_id', '=', lot.id),
                    ('location_id', '=', self.location_id.id),
                    ('quantity', '>', 0)
                ], limit=1)
                if not quant:
                    raise UserError(_("La tela %s no tiene stock en %s.") % (lot.name, self.location_id.name))
                
                raw_quantity = quant.quantity 
                uom = lot.product_id.uom_id
                qty_done = uom.round(raw_quantity) if uom else raw_quantity

        # CASO C: DESPERDICIO TEJIDO (Subproducto SUB-H99999-AAAA-MM-DD)
        elif 'DESPERDICIO' in op_name:
            # REGEX: Debe iniciar con SUB-, seguido de la estructura alfanumérica/guiones, y terminar con fecha
            if not barcode.startswith('SUB-') or not re.match(r'^SUB-[A-Z]?[\w-]+-\d{4}-\d{2}-\d{2}$', barcode):
                raise UserError(_("Formato de subproducto inválido para Desperdicio. Debe ser: SUB-MO-AAAA-MM-DD."))
        
            # Removemos el prefijo 'SUB-'
            clean_sub = barcode[4:]
            # Removemos los 3 bloques de la fecha de la derecha (AAAA, MM, DD) para aislar la MO con sus guiones
            mo_part = clean_sub.rsplit('-', 3)[0]
            if mo_part not in (self.origin or ''):
                raise UserError(_("El subproducto %s no pertenece a la Orden de Fabricación %s.") % (barcode, self.origin))

            # Verificar si ya está precargado en las líneas de la operación
            existing_line = self.move_line_ids.filtered(lambda ml: ml.lot_id == lot)
            if existing_line:
                # SE CONSERVA EL CONTROL DE FLUJO ORIGINAL
                already_processed = True
                return {
                    'type': 'ir.actions.client',
                    'tag': 'display_notification',
                    'params': {
                        'title': 'Subproducto ya registrado',
                        'message': 'Este Subproducto ya fue registrado',
                        'sticky': True,
                        'type': 'warning',
                        'next': {'type': 'ir.actions.client', 'tag': 'reload'},
                    }
                }
                
            else:
                # Si no está precargado, buscamos existencias reales en la ubicación
                quant = self.env['stock.quant'].search([
                     ('lot_id', '=', lot.id),
                     ('location_id', '=', self.location_id.id),
                     ('quantity', '>', 0)
                ], limit=1)
                if not quant:
                    raise UserError(_("El lote %s no tiene existencias en la ubicación %s.") % (lot.name, self.location_id.name))
                
                raw_quantity = quant.quantity 
                uom = lot.product_id.uom_id
                qty_done = uom.round(raw_quantity) if uom else raw_quantity

        # ---------------------------------------------------------
        # 3. PROCESAMIENTO TÉCNICO Y PERSISTENCIA
        # ---------------------------------------------------------
        if not already_processed and (qty_done > 0 or 'REQUISICI' in op_name or 'DEVOLUCION PRODUCCION' in op_name):
            picking_id = self._origin.id if self._origin else self.id
            move = self.move_ids.filtered(lambda m: m.product_id == lot.product_id and m.state not in ['done', 'cancel'])[:1]
            
            if move:
                # VALIDACIÓN DE DUPLICADOS
                existing_line = self.env['stock.move.line'].sudo().search([
                    ('picking_id', '=', picking_id),
                    ('lot_id', '=', lot.id),
                    ('quantity', '>', 0)
                ], limit=1)

                if existing_line:
                    raise UserError(_("Este lote (%s) ya fue guardado físicamente en la base de datos.") % lot.name)
               
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
              
                return {'type': 'ir.actions.client', 'tag': 'reload'}
            else:
                raise UserError(_("El producto %s no es requerido en este documento.") % lot.product_id.display_name)

    def button_validate(self):
        """ Validación con tolerancia técnica para industria textil """
        for rec in self:
            op_name = _normalize_text(rec.picking_type_id.name)
            if any(kw in op_name for kw in ['FORMACI', 'DESPERDICIO']) or 'DEVOLUCION PRODUCCION' in op_name:
                
                total_scanned = sum(rec.move_line_ids.mapped('quantity'))
                total_demanded = sum(rec.move_ids.mapped('product_uom_qty'))
                
                uom = rec.move_ids[0].product_uom if rec.move_ids else False
                rounding = uom.rounding if uom else 0.01

                res = float_compare(total_scanned, total_demanded, precision_rounding=rounding)

                if res != 0:
                    status = "INSUFICIENTE" if res == -1 else "EXCESO"
                    raise UserError(_(
                        "ERROR DE CANTIDAD (%s):\n"
                        "- Escaneado total: %s\n"
                        "- Demandado total: %s\n\n"
                        "La suma de los lotes no coincide con la demanda dentro de la precisión permitida."
                    ) % (status, total_scanned, total_demanded))
        
        return super(StockPicking, self).button_validate()