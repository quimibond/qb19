# -*- coding: utf-8 -*-
from odoo import models, fields, api, _
from odoo.exceptions import ValidationError, UserError

class MrpWeighRollWizard(models.TransientModel):
    _name = 'mrp.weigh.roll.wizard'
    _description = 'Asistente de Pesado de Rollos'

    # Campo técnico para manejar el estado de la alerta
    confirm_threshold = fields.Boolean(string="Confirmar fuera de rango", default=False)

    employee_number = fields.Char(string="Número de Empleado", required=True)
    employee_name = fields.Char(string="Nombre del Empleado", compute="_compute_employee_name", store=False)

    @api.depends('employee_number')
    def _compute_employee_name(self):
        for reg in self:
            if reg.employee_number:
                # Buscamos al empleado que coincida con el número Y con el puesto
                emp = self.env['hr.employee'].search([
                    ('x_studio_nmero_de_trabajador', '=', reg.employee_number),
                    '|',
                    ('job_title', 'ilike', 'OPERADOR DE TEJIDO%'),
                    ('job_title', 'ilike', 'COMODIN TEJIDO')
                ], limit=1)
                
                if emp:
                    reg.employee_name = emp.name
                else:
                    # Si el número existe pero el puesto no coincide, daremos un aviso claro
                    all_emp = self.env['hr.employee'].search([('x_studio_nmero_de_trabajador', '=', reg.employee_number)], limit=1)
                    if all_emp:
                        reg.employee_name = "PUESTO NO AUTORIZADO"
                    else:
                        reg.employee_name = "EMPLEADO NO ENCONTRADO"
            else:
                reg.employee_name = False

    @api.onchange('employee_number')
    def _onchange_employee_number(self):
        if self.employee_number:
            # Aplicamos el mismo filtro en el onchange para el bloqueo de la vista
            emp = self.env['hr.employee'].search([
                ('x_studio_nmero_de_trabajador', '=', self.employee_number),
                '|',
                ('job_title', 'ilike', 'OPERADOR DE TEJIDO%'),
                ('job_title', 'ilike', 'COMODIN TEJIDO%')
            ], limit=1)
            
            if not emp:
                val_erroneo = self.employee_number
                self.employee_number = False
                self.employee_name = False
                
                # Verificamos si el error es por puesto o porque no existe el número
                check_exists = self.env['hr.employee'].search([('x_studio_nmero_de_trabajador', '=', val_erroneo)], limit=1)
                
                if check_exists:
                    mensaje = _("El empleado '%s' no tiene un puesto autorizado (OPERADOR DE TEJIDO o COMODIN).") % check_exists.name
                else:
                    mensaje = _("Acceso Denegado: El número de empleado '%s' no existe.") % val_erroneo
                
                raise ValidationError(mensaje)

    workorder_id = fields.Many2one('mrp.workorder', string="Orden de Trabajo")
    production_id = fields.Many2one('mrp.production', related="workorder_id.production_id", string="Orden de Fabricación")
    workcenter_id = fields.Many2one('mrp.workcenter', related="workorder_id.workcenter_id", string="Centro de Trabajo")
    product_id = fields.Many2one('product.product', related="production_id.product_id", string="Artículo")
    
    qty_to_produce = fields.Float(related="production_id.product_qty", string="Total a Producir")
    qty_produced = fields.Float(related="production_id.qty_producing", string="Producido al Momento")
    roll_count = fields.Integer(related="production_id.roll_count", string="Rollos Registrados")

    production_percentage = fields.Float(
        string="Porcentaje de Producción", 
        compute="_compute_production_percentage"
    )
    
    next_lot_name = fields.Char(string="Número de Lote a Generar", compute="_compute_next_lot_name")
    weight = fields.Float(string="Peso del Rollo Actual (kg)", digits=(12, 3), required=True)
    
    # --- NUEVOS CAMPOS ---
    tara = fields.Float(string="Tara (kg)", compute="_compute_tara_neta", store=True)
    net_weight = fields.Float(string="Peso Neto (kg)", compute="_compute_tara_neta", store=True)

    @api.depends('product_id', 'workcenter_id', 'weight')
    def _compute_tara_neta(self):
        for reg in self:
            # 1. Buscar tara específica para Producto + Centro de Trabajo
            tara_obj = self.env['mrp.tara'].search([
                ('product_id', '=', reg.product_id.id),
                ('workcenter_id', '=', reg.workcenter_id.id)
            ], limit=1)
            
            # 2. Si no hay específica, buscar la que no tiene centro de trabajo (aplica a todos)
            if not tara_obj:
                tara_obj = self.env['mrp.tara'].search([
                    ('product_id', '=', reg.product_id.id),
                    ('workcenter_id', '=', False)
                ], limit=1)
            
            reg.tara = tara_obj.tara if tara_obj else 0.0
            reg.net_weight = reg.weight - reg.tara

    @api.depends('workorder_id', 'production_id.roll_count')
    def _compute_next_lot_name(self):
        for reg in self:
            if reg.production_id:
                mo_identifier = reg.production_id.name.split('/')[-1]
                reg.next_lot_name = f"{mo_identifier}-{(reg.production_id.roll_count + 1):04d}"
            else:
                reg.next_lot_name = False

    @api.depends('weight', 'qty_produced', 'qty_to_produce', 'net_weight')
    def _compute_production_percentage(self):
        for reg in self:
            # Usamos net_weight para que el avance de producción sea real (sin taras)
            total_con_este_rollo = reg.qty_produced + reg.net_weight
            
            if reg.qty_to_produce > 0:
                reg.production_percentage = total_con_este_rollo / reg.qty_to_produce
            else:
                reg.production_percentage = 0.0

    def action_reset_threshold(self):
        self.ensure_one()
        # Simplemente apagamos la bandera de confirmación
        self.confirm_threshold = False
        # Recargamos el wizard para que el usuario pueda volver a pesar o corregir
        return {
            'type': 'ir.actions.act_window',
            'res_model': self._name,
            'res_id': self.id,
            'view_mode': 'form',
            'target': 'new',
            'context': self.env.context,
        }

    def confirm_weighing(self):
        """ Registro de pesaje, impresión y cierre automático """
        self.ensure_one()

        # 1. Obtener el producto de la Orden de Fabricación actual
        producto = self.production_id.product_id
        
        # 2. Buscar la configuración de peso estándar
        config = self.env['mrp.rollo.estandar'].search([('product_id', '=', producto.id)], limit=1)
        
        if config and not self.confirm_threshold:
            # Calculamos los límites
            limite_inferior = config.rollo_teorico - 3.0
            limite_superior = config.rollo_teorico + 3.0
            
            # 3. Validar si el peso neto está fuera del rango permitido
            if self.net_weight < limite_inferior or self.net_weight > limite_superior:
                # Activamos la bandera y recargamos el wizard para mostrar la alerta
                self.confirm_threshold = True
                return {
                    'type': 'ir.actions.act_window',
                    'res_model': self._name,
                    'res_id': self.id,
                    'view_mode': 'form',
                    'target': 'new',
                    'context': self.env.context,
                }

        # 1. Registra el PESO NETO en la MO (se cambió self.weight por self.net_weight)
        self.production_id.action_register_roll_with_weight(self.net_weight,pesador=self.employee_name )
        
        # 2. Generar datos para la nueva función de impresión
        mo_identifier = self.production_id.name.split('/')[-1]
        lot_name = f"{mo_identifier}-{self.production_id.roll_count:04d}"
        
        # 3. LLAMADA CRÍTICA: Se envía el peso NETO para la etiqueta
        self.production_id._print_zpl_pesaje_original(lot_name, self.net_weight, lot_name, pesador=self.employee_name)
        
        # 4. Disparo del reporte
        report_action = self.env.ref('pesaje_rollos_tejido.action_report_weigh_roll').report_action(self.production_id)
        
        # 5. Cerramos la ventana automáticamente
        report_action.update({'close_on_report_download': True})
        
        return report_action

class MrpSubproductWizard(models.TransientModel):
    _name = 'mrp.subproduct.wizard'
    _description = 'Asistente de Pesado de Subproducto'

    # 1. Añadir los campos de empleado
    employee_number = fields.Char(string="Número de Empleado", required=True)
    employee_name = fields.Char(string="Nombre del Empleado", compute="_compute_employee_name")

    @api.depends('employee_number')
    def _compute_employee_name(self):
        for reg in self:
            if reg.employee_number:
                # Buscamos al empleado que coincida con el número Y con el puesto
                emp = self.env['hr.employee'].search([
                    ('x_studio_nmero_de_trabajador', '=', reg.employee_number),
                    ('job_title', 'ilike', 'COMODIN TEJIDO')
                ], limit=1)
                
                if emp:
                    reg.employee_name = emp.name
                else:
                    # Si el número existe pero el puesto no coincide, daremos un aviso claro
                    all_emp = self.env['hr.employee'].search([('x_studio_nmero_de_trabajador', '=', reg.employee_number)], limit=1)
                    if all_emp:
                        reg.employee_name = "PUESTO NO AUTORIZADO"
                    else:
                        reg.employee_name = "EMPLEADO NO ENCONTRADO"
            else:
                reg.employee_name = False

    @api.onchange('employee_number')
    def _onchange_employee_number(self):
        if self.employee_number:
            # Filtro exclusivo: Número correcto Y puesto COMODIN
            emp = self.env['hr.employee'].search([
                ('x_studio_nmero_de_trabajador', '=', self.employee_number),
                ('job_title', 'ilike', 'COMODIN TEJIDO')
            ], limit=1)
            
            if not emp:
                val_erroneo = self.employee_number
                # Limpiamos para ocultar la sección de pesaje en el XML
                self.employee_number = False
                self.employee_name = False
                
                # Verificamos por qué falló para dar el mensaje exacto
                check_exists = self.env['hr.employee'].search([
                    ('x_studio_nmero_de_trabajador', '=', val_erroneo)
                ], limit=1)
                
                if check_exists:
                    puesto_actual = check_exists.job_title or "SIN PUESTO"
                    raise ValidationError(_("El trabajador %s tiene el puesto '%s'. Solo los COMODINES pueden registrar subproductos.") % (check_exists.name, puesto_actual))
                else:
                    raise ValidationError(_("Acceso Denegado: El número de empleado '%s' no existe.") % val_erroneo)

    workorder_id = fields.Many2one('mrp.workorder', string="Orden de Trabajo")
    production_id = fields.Many2one('mrp.production', related="workorder_id.production_id", string="Orden de Fabricación")
    workcenter_id = fields.Many2one('mrp.workcenter', related="workorder_id.workcenter_id", string="Centro de Trabajo")
    
    product_id = fields.Many2one('product.product', string="Subproducto", compute="_compute_subproduct")
    next_lot_name = fields.Char(string="Lote a Generar", compute="_compute_next_lot_name")
    weight = fields.Float(string="Peso Subproducto (kg)", digits=(12, 3), required=True)
    
    @api.depends('production_id')
    def _compute_subproduct(self):
        for reg in self:
            move = reg.production_id.move_byproduct_ids[:1]
            reg.product_id = move.product_id if move else False

    @api.depends('production_id')
    def _compute_next_lot_name(self):
        for reg in self:
            if reg.production_id:
                mo_identifier = reg.production_id.name.split('/')[-1]
                reg.next_lot_name = f"SUB-{mo_identifier}-{fields.Date.today()}"
            else:
                reg.next_lot_name = False

    def confirm_subproduct(self):
        self.ensure_one()
        # CORRECCIÓN: Pasar self.next_lot_name en lugar de self.product_id
        lot = self.production_id.action_register_subproduct_manual(self.weight, self.next_lot_name,pesador=self.employee_name )
        
        # Ahora 'lot' es un objeto de stock.lot gracias al return anterior
        # Ejecutamos la acción de reporte para que salga el PDF/Impresión
        res = self.env.ref('pesaje_rollos_tejido.action_report_subproduct_weigh').report_action(self.production_id)
        res.update({'close_on_report_download': True})
        return res
    
   