# -*- coding: utf-8 -*-
from odoo import models, fields, api, _
from odoo.exceptions import UserError

class MrpYarnReturnWizard(models.TransientModel):
    _name = 'mrp.yarn.return.wizard'
    _description = 'Asistente para Devolución de Hilo Sobrante por Producto'

    @api.model
    def _get_default_location_id(self):
        loc = self.env['stock.location'].search([
            ('complete_name', '=', 'Toluca/Stock/2 PRODUCCIÓN'),
            ('usage', '=', 'internal')
        ], limit=1)
        return loc.id if loc else False

    @api.model
    def _get_default_location_dest_id(self):
        loc = self.env['stock.location'].search([
            ('complete_name', '=', 'Toluca/Stock/1 Materia Prima'),
            ('usage', '=', 'internal')
        ], limit=1)
        return loc.id if loc else False

    location_id = fields.Many2one('stock.location', string='Ubicación Origen', required=True, default=_get_default_location_id)
    location_dest_id = fields.Many2one('stock.location', string='Ubicación Destino', required=True, default=_get_default_location_dest_id)
    picking_type_id = fields.Many2one('stock.picking.type', string='Tipo de Operación', required=True, domain=[('code', '=', 'internal')])
    iot_device_id = fields.Many2one('iot.device', string='Báscula IoT', domain=[('type', '=', 'scale')])
    product_id = fields.Many2one('product.product', string='Hilo a Devolver', required=True, domain="[('categ_id.complete_name', '=like', 'Materia Prima / Hilo%')]")
    
    # Una sola tabla persistente en base de datos temporal
    line_ids = fields.One2many('mrp.yarn.return.wizard.line', 'wizard_id', string='Lotes Encontrados')
    
    # CAMPOS DE CAPTURA RÁPIDA EN EL ENCABEZADO
    input_lot_name = fields.Char(string='Lote/Caja a Pesar')
    input_peso_bruto = fields.Float(string='Peso Bruto Captura (kg)', digits=(16, 3))
    input_tara = fields.Float(string='Tara Captura (kg)', digits=(16, 3))

    @api.onchange('location_id')
    def _onchange_location_id(self):
        if self.location_id:
            picking_type = self.env['stock.picking.type'].search([
                ('name', 'ilike', 'Devolucion Hilo Tejido'),
                ('code', '=', 'internal')
            ], limit=1)
            if picking_type:
                self.picking_type_id = picking_type

    def action_load_yarn_inventory(self):
        """ Carga física y persistente original (La que sí funcionaba) """
        self.ensure_one()
        if not self.product_id or not self.location_id:
            raise UserError(_("Por favor, seleccione un Hilo y la Ubicación Origen antes de continuar."))
            
        # Limpieza física en la BD antes de cargar para evitar duplicados o basura
        self.line_ids.unlink()
        
        quants = self.env['stock.quant'].search([
            ('product_id', '=', self.product_id.id),
            ('location_id', '=', self.location_id.id),
            ('quantity', '>', 0)
        ])
        
        new_lines = []
        for quant in quants:
            if not quant.lot_id:
                continue
            lot_name = quant.lot_id.name or ''
            box_no = lot_name[-4:] if len(lot_name) >= 4 else lot_name
            new_lines.append((0, 0, {
                'to_return': False,
                'lot_id': quant.lot_id.id,
                'box_no': box_no,
                'peso_actual': quant.quantity,
                'peso_bruto': 0.0,
                'tara': 0.0,
                'peso_neto': 0.0
            }))
            
        # Escritura directa en la base de datos sin comandos web conflictivos
        self.write({
            'line_ids': new_lines,
            'input_lot_name': False,
            'input_peso_bruto': 0.0,
            'input_tara': 0.0
        })
        
        # Mantiene el wizard abierto perfectamente redibujando la vista con el ID actual
        return {
            'type': 'ir.actions.act_window',
            'res_model': 'mrp.yarn.return.wizard',
            'res_id': self.id,
            'view_mode': 'form',
            'target': 'new',
        }

    def action_get_weight_from_scale(self):
        """ Captura el peso de la báscula IoT """
        self.ensure_one()
        if not self.iot_device_id:
            raise UserError(_("Por favor, seleccione una Báscula IoT."))
        weight = self.iot_device_id.value
        if weight:
            self.input_peso_bruto = float(weight)
        
        return {
            'type': 'ir.actions.act_window',
            'res_model': 'mrp.yarn.return.wizard',
            'res_id': self.id,
            'view_mode': 'form',
            'target': 'new',
        }

    def action_update_line_from_input(self):
        """ Actualiza la fila y limpia el encabezado manteniendo la ventana modal abierta """
        self.ensure_one()
        if not self.input_lot_name:
            raise UserError(_("Por favor, ingrese o escanee un número de lote o caja."))
        
        clean_input = str(self.input_lot_name).replace(" ", "").upper()
        
        target_line = self.line_ids.filtered(
            lambda l: (l.lot_id and l.lot_id.name and l.lot_id.name.replace(" ", "").upper() == clean_input) or 
                      (l.box_no and l.box_no.replace(" ", "").upper() == clean_input)
        )
        
        if not target_line:
            raise UserError(_("El lote o caja '%s' no se encuentra en las existencias cargadas abajo.") % self.input_lot_name)
        
        line = target_line[0]
        line.write({
            'peso_bruto': self.input_peso_bruto,
            'tara': self.input_tara,
            'to_return': True,
        })
        
        self.write({
            'input_lot_name': False,
            'input_peso_bruto': 0.0,
            'input_tara': 0.0
        })
        
        return {
            'type': 'ir.actions.act_window',
            'res_model': 'mrp.yarn.return.wizard',
            'res_id': self.id,
            'view_mode': 'form',
            'target': 'new',
        }

    def action_generate_internal_transfer(self):
        """ Genera el movimiento de inventario real e imprime las etiquetas ZPL """
        self.ensure_one()
        if not self.line_ids:
            raise UserError(_("No hay lotes en la lista."))

        lines_to_process = self.line_ids.filtered(lambda l: l.to_return)
        if not lines_to_process:
            raise UserError(_("Por favor, registre y marque al menos un lote para devolver."))

        for line in lines_to_process:
            if line.peso_neto <= 0:
                raise UserError(_("El lote %s debe tener un peso neto mayor a cero.") % line.lot_id.name)

        picking = self.env['stock.picking'].create({
            'picking_type_id': self.picking_type_id.id,
            'location_id': self.location_id.id,
            'location_dest_id': self.location_dest_id.id,
            'origin': _('Devolución de Hilo Sobrante - %s') % self.product_id.name,
        })

        for line in lines_to_process:
            move = self.env['stock.move'].create({
                'name': self.product_id.name,
                'product_id': self.product_id.id,
                'product_uom_qty': line.peso_neto,
                'product_uom': self.product_id.uom_id.id,
                'picking_id': picking.id,
                'location_id': self.location_id.id,
                'location_dest_id': self.location_dest_id.id,
            })
            self.env['stock.move.line'].create({
                'move_id': move.id,
                'product_id': self.product_id.id,
                'lot_id': line.lot_id.id,
                'quantity': line.peso_neto,
                'product_uom_id': self.product_id.uom_id.id,
                'location_id': self.location_id.id,
                'location_dest_id': self.location_dest_id.id,
                'picking_id': picking.id,
            })

        picking.action_confirm()
        picking.action_assign()
        if picking.state == 'assigned':
            picking.button_validate()
        else:
            raise UserError(_("La transferencia quedó en espera de disponibilidad."))

        return self.env.ref('devolucion_hilo_produccion.action_report_yarn_box_zpl').report_action(lines_to_process)


class MrpYarnReturnWizardLine(models.TransientModel):
    _name = 'mrp.yarn.return.wizard.line'
    _description = 'Línea de Lote Encontrado para Devolución'

    wizard_id = fields.Many2one('mrp.yarn.return.wizard', ondelete='cascade')
    product_id = fields.Many2one('product.product', string='Hilo', related='wizard_id.product_id', store=True)
    lot_id = fields.Many2one('stock.lot', string='Lote / Serie', readonly=True)
    box_no = fields.Char(string='N. Caja', readonly=True)
    peso_actual = fields.Float(string='Stock Sistema (kg)', readonly=True, digits=(16, 3))
    peso_bruto = fields.Float(string='Peso Bruto (kg)', digits=(16, 3))
    tara = fields.Float(string='Tara (kg)', digits=(16, 3))
    peso_neto = fields.Float(string='Peso Neto (kg)', compute='_compute_peso_neto', store=True, digits=(16, 3))
    to_return = fields.Boolean(string='Devolver', default=False)

    @api.depends('peso_bruto', 'tara')
    def _compute_peso_neto(self):
        for record in self:
            record.peso_neto = max(0.0, record.peso_bruto - record.tara)

    def get_zpl_label(self):
        self.ensure_one()
        ref = self.product_id.default_code or ''
        name = self.product_id.name or ''
        lot = self.lot_id.name or ''
        qty = self.peso_neto
        box = self.box_no or ''
        
        return f"^XA^CI28\n" \
               f"^CF0,50,50^FO50,50^FDREF: {ref}^FS\n" \
               f"^CF0,40,40^FO50,110^FB700,2,0,C^FD{name}^FS\n" \
               f"^FO50,210^FDLote: {lot}^FS\n" \
               f"^FO550,210^FDCant: {qty}^FS\n" \
               f"^FO50,270^FDN. CAJA: {box}^FS\n" \
               f"^FO030,350^BY3^BCN,100,Y,N,N^FD{lot}^FS\n" \
               f"^XZ"