# -*- coding: utf-8 -*-
from odoo import models, fields, api, _
from odoo.exceptions import UserError
import datetime

class MrpProduction(models.Model):
    _inherit = 'mrp.production'

    product_id_category_name = fields.Char(
        related='product_id.categ_id.display_name', 
        string="Categoría del Producto", 
        store=False
    )

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

        # Punto 1: Impresión de etiqueta de Producto (Rollo)
        self._print_zpl_pesaje_original(lot_name, weight, lot_name)
        
        return {'type': 'ir.actions.client', 'tag': 'reload'}

    def action_register_subproduct_manual(self, weight, lot_name=False):
        """ 
        Registro de subproductos con validación contra el historial de revisión.
        Punto 10 y 11 del documento. Adaptado para Odoo 19.
        """
        self.ensure_one()
        if weight <= 0:
            raise UserError(_("El peso debe ser mayor a cero."))

        # --- VALIDACIÓN DE META ---
        revisados = getattr(self, 'rollos_revisados_count', 0)
        meta = getattr(self, 'rollos_requeridos_count', 0)

        if revisados < meta:
            raise UserError(_(
                "No se puede registrar el subproducto aún.\n"
                "Meta de revisión: %s rollos. Revisados actualmente: %s."
            ) % (meta, revisados))

        # Punto 10: Validación del total del subproducto vs diferencia acumulada en revisión
        total_diff_revisado = sum(self.revision_log_ids.mapped('diferencia'))
        
        if round(weight, 2) > round(total_diff_revisado, 2):
            raise UserError(_("Error: El peso del subproducto (%.3f) no puede exceder la suma de diferencias de revisión (%.3f).") % (weight, total_diff_revisado))

        sub_move = self.move_byproduct_ids.filtered(lambda x: x.state not in ('done', 'cancel'))[:1]
        if not sub_move:
            raise UserError(_("No hay subproductos pendientes en esta orden."))

        if not lot_name:
            mo_identifier = self.name.split('/')[-1]
            # Mantenemos tu formato original (SUB-MO-2026-04-13)
            lot_name = f"SUB-{mo_identifier}-{fields.Date.today()}"
        
        # CORRECCIÓN DE SINTAXIS Y BÚSQUEDA (Línea 112)
        # Aseguramos que lot_name sea string y cerramos correctamente el domain
        new_lot = self.env['stock.lot'].search([
            ('name', '=', str(lot_name)),
            ('product_id', '=', sub_move.product_id.id),
            ('company_id', '=', self.company_id.id)
        ], limit=1)

        # Solo se crea si no existe
        if not new_lot:
            new_lot = self.env['stock.lot'].create({
                'name': lot_name,
                'product_id': sub_move.product_id.id,
                'company_id': self.company_id.id,
                'production_id': self.id,     # Tu campo original
                'product_qty': weight,
            })

        current_wo = self.workorder_ids.filtered(lambda w: w.state in ('ready', 'progress'))[:1]
        
        move_line = sub_move.move_line_ids.filtered(lambda l: not l.lot_id)[:1]
        
              # Para que Odoo no borre mov al cerrar WO se marca cantidad y picked
        if move_line:
            move_line.write({
                'lot_id': new_lot.id,
                'quantity': weight,
                'picked': True,
                'workorder_id': current_wo.id if current_wo else False,
            })
        else:
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
                'picked': True, # Se marca linea de detalle como procesada
            })

        sub_move.move_line_ids.filtered(lambda l: not l.lot_id or l.quantity == 0).unlink()

        sub_move._set_quantity_done(weight)

        self.qty_producing += weight
        if current_wo:
            current_wo.qty_produced += weight

        for line in sub_move.move_line_ids:
            if not line.lot_id and line.lot_name:
                line.lot_id = new_lot.id

        # Punto 11: Registrar la diferencia como merma si el subproducto es menor a la revisión
        diff_merma = total_diff_revisado - weight
        if diff_merma > 0:
            scrap_location = self.env['stock.location'].search([('scrap_location', '=', True)], limit=1)
            self.env['stock.scrap'].create({
                'production_id': self.id,
                'product_id': self.product_id.id, 
                'scrap_qty': diff_merma,
                'location_id': self.location_src_id.id,
                'scrap_location_id': scrap_location.id,
            })

        self.move_raw_ids._recompute_state()
        self.move_raw_ids._action_assign()

        # --- CIERRE AUTOMÁTICO DE CONTROL DE CALIDAD (Odoo 19) ---
        # En Odoo 19 el campo es 'quality_state'
        checks = self.env['quality.check'].search([
            ('production_id', '=', self.id),
            ('quality_state', '!=', 'pass')
        ])
        for check in checks:
            # Forzamos el estado a 'pass' y registramos fecha y usuario
            check.write({
                'quality_state': 'pass',
                'user_id': self.env.user.id,
                'control_date': fields.Datetime.now()
            })
        # --- FIN CIERRE AUTOMÁTICO ---

        # Punto 3: Impresión de etiqueta de Subproducto
        self._print_subproduct_zpl(sub_move.product_id, weight, lot_name)
        
        return new_lot

    def _print_zpl_pesaje_original(self, lot_name, weight, barcode_data):
        """ Etiqueta de Pesaje Original Corregida (10x7.5cm) """
        self.ensure_one()
        # --- CAMBIO AQUÍ: Convertir UTC a Hora Local del Usuario ---
        ahora_utc = fields.Datetime.now()
        ahora_local = fields.Datetime.context_timestamp(self, ahora_utc)
        ahora = ahora_local.strftime('%d/%m/%Y %H:%M:%S')
        
        # Búsqueda mejorada del Centro de Trabajo (Punto 4)
        wo = self.workorder_ids.filtered(lambda w: w.state in ('ready', 'progress'))[:1]
        if not wo:
            wo = self.workorder_ids[:1]
        wc_name = wo.workcenter_id.name if wo else "N/A"

        producto_desc = self.product_id.display_name 

        # PW812 = 10cm | LL609 = 7.5cm
        # ^BY2 = Grosor fino | ^FO100 = Inicio a la izquierda
        zpl = f"""^XA^PW812^LL609^CI28
^FO50,40^A0N,25,25^FDFECHA : {ahora}^FS
^FO50,80^A0N,25,25^FDCENTRO DE TRABAJO : {wc_name}^FS
^FO50,120^A0N,25,25^FDORDEN DE FABRICACION : {self.name}^FS
^FO50,160^A0N,20,20^FDPRODUCTO : {producto_desc[:70]}^FS
^FO0,230^FB812,1,0,C^A0N,100,90^FD{weight:.4f} kg^FS
^BY2,3,110^FO100,360^BCN,110,N,N,N^FD{lot_name}^FS
^FO0,510^FB812,1,0,C^A0N,35,35^FDLOTE : {lot_name}^FS
^XZ"""
        self.last_zpl_label = zpl
        return True

    def _print_subproduct_zpl(self, product, weight, lot_name):
        """ Genera etiqueta ZPL para el subproducto corregida (10x7.5cm) """
        self.ensure_one()
        # --- CAMBIO AQUÍ: Convertir UTC a Hora Local del Usuario ---
        ahora_utc = fields.Datetime.now()
        ahora_local = fields.Datetime.context_timestamp(self, ahora_utc)
        ahora = ahora_local.strftime('%d/%m/%Y %H:%M:%S')
        # -----------------------------------------------------------

        # PW812 = 10cm de ancho | LL609 = 7.5cm de alto
        zpl = f"""^XA^PW812^LL609^CI28
^FO50,40^A0N,40,40^FDSUBPRODUCTO^FS
^FO50,100^A0N,25,25^FDFECHA : {ahora}^FS
^FO50,140^A0N,25,25^FDPRODUCTO : {product.display_name[:70]}^FS
^FO50,180^A0N,25,25^FDORIGEN : {self.name}^FS
^FO0,250^FB812,1,0,C^A0N,100,90^FD{weight:.4f} kg^FS
^BY2,3,110^FO100,380^BCN,110,N,N,N^FD{lot_name}^FS
^FO0,520^FB812,1,0,C^A0N,30,30^FDLOTE : {lot_name}^FS
^XZ"""
        self.last_zpl_label = zpl
        return True

    def button_mark_done(self):
        """
        Elimina el rollo de ajuste igualando la cantidad planeada de la MO
        al total realmente pesado (rollos + subproducto), justo antes del cierre.
        Esta solución NO toca los movimientos de inventario, por lo que no borra rollos.
        """
        self.ensure_one()
        
        # Solo aplicamos la lógica si hemos registrado rollos con nuestro módulo
        if self.roll_count > 0:
            # 1. Calculamos el total real producido (Producto Principal)
            finished_move = self.move_finished_ids.filtered(
                lambda x: x.product_id == self.product_id and x.state not in ('done', 'cancel')
            )[:1]
            
            if finished_move:
                # Sumamos el peso real de todos tus rollos
                total_rollos = sum(finished_move.move_line_ids.mapped('quantity'))
                
                # Buscamos si hay subproductos pesados
                # (Asumiendo que el subproducto se registra en su propio movimiento)
                byproduct_moves = self.move_byproduct_ids.filtered(
                    lambda x: x.state not in ('done', 'cancel')
                )
                total_subproductos = sum(byproduct_moves.mapped('move_line_ids.quantity'))
                
                # El total producido que Odoo debe considerar es la suma de ambos
                total_producido_real = total_rollos + total_subproductos
                
                if total_producido_real > 0:
                    # AJUSTE CRÍTICO: Igualamos la cantidad planeada de la MO al total real.
                    # Al hacer esto, Odoo 19 ve que se planeó X y se produjo X.
                    # Diferencia = 0, por lo tanto, NO genera rollo de ajuste.
                    self.write({
                        'product_qty': total_producido_real,
                        'qty_producing': total_rollos, # El progreso visual es solo sobre el principal
                    })
                    
                    # Opcional: Para el subproducto, aseguramos que su movimiento 
                    # tenga la demanda correcta para que no genere su propio ajuste.
                    for smove in byproduct_moves:
                        total_smove = sum(smove.move_line_ids.mapped('quantity'))
                        if total_smove > 0:
                            smove.write({'product_uom_qty': total_smove})

        # Ejecutamos el cierre estándar de Odoo. Como 'product_qty' ahora es igual
        # a lo producido, cerrará sin generar rollos fantasma.
        return super(MrpProduction, self).button_mark_done()

class StockLot(models.Model):
    _inherit = 'stock.lot'

    production_id = fields.Many2one('mrp.production', string="Orden de Fabricación")
    needs_review = fields.Boolean(string="Necesita Revisión", default=False)
    is_reviewed = fields.Boolean(string="Revisado", default=False)

