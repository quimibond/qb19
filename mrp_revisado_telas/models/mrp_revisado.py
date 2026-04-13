# -*- coding: utf-8 -*-
from odoo import models, fields, api, _
from odoo.exceptions import UserError

class ProductTemplate(models.Model):
    _inherit = 'product.template'
    
    categ_name_static = fields.Char(related='categ_id.complete_name', string="Categoría", store=False)
    porcentaje_revision_standard = fields.Float(string='% Revisión Estándar', default=0.1)

class MrpProduction(models.Model):
    _inherit = 'mrp.production'

    # Agregamos este campo para que coincida con lo que pide tu vista XML
    needs_review = fields.Boolean(string="Necesita Revisión", default=False)

    # El conteo de revisados sí se almacena para reportes y velocidad
    rollos_revisados_count = fields.Integer(
        string="Rollos Revisados", 
        compute="_compute_rollos_revisados_count",
        store=True
    )

    # REVISIÓN: store=False para que siempre lea el valor real del Centro de Trabajo
    rollos_requeridos_count = fields.Integer(
        string="Meta de Revisión (WC)",
        compute="_compute_rollos_requeridos_count",
        store=False, 
        help="Número de rollos requeridos según la configuración del Centro de Trabajo."
    )

    porcentaje_revision = fields.Float(string='% de Revisión', compute='_compute_revision_perc')
    revision_log_ids = fields.One2many('mrp.revision.log', 'production_id', string="Log Calidad")

    def _compute_revision_perc(self):
        get_param = self.env['ir.config_parameter'].sudo().get_param
        val = float(get_param('mrp_revisado_telas.mrp_revision_percentage', default=0.1))
        for reg in self:
            reg.porcentaje_revision = val

    @api.depends('revision_log_ids')
    def _compute_rollos_revisados_count(self):
        for reg in self:
            reg.rollos_revisados_count = len(reg.revision_log_ids)

    @api.depends('workorder_ids.workcenter_id', 'workorder_ids.state')
    def _compute_rollos_requeridos_count(self):
        """
        CORRECCIÓN: Obtiene la meta del centro de trabajo real desde las órdenes de trabajo,
        permitiendo que el dato cambie si planeación mueve la orden a un centro alterno.
        """
        for reg in self:
            # Buscamos la orden de trabajo activa (lista o en proceso)
            active_wo = reg.workorder_ids.filtered(lambda w: w.state in ('ready', 'progress'))[:1]
            
            # Si no hay activa, tomamos la primera de la lista como referencia
            target_wo = active_wo or reg.workorder_ids[:1]
            
            if target_wo and target_wo.workcenter_id:
                # Accedemos al campo 'numero_rollos_revisar' del centro de trabajo vinculado a la WO
                reg.rollos_requeridos_count = target_wo.workcenter_id.numero_rollos_revisar
            else:
                # Respaldo: si no hay WOs, intenta el campo directo del respaldo
                if reg.workcenter_id:
                    reg.rollos_requeridos_count = reg.workcenter_id.numero_rollos_revisar
                else:
                    reg.rollos_requeridos_count = 0

    def _print_zpl_label(self, lote_name, peso, nombre_producto):
        """ Etiqueta de Revisado Corregida (10x7.5cm) """
        self.ensure_one()
        # Mantenemos FECHA REVISADO para este proceso
        ahora = fields.Datetime.now().strftime('%d/%m/%Y %H:%M:%S')
        
        # Punto 3: Centro de Trabajo (WC)
        wc_name = self.workcenter_id.name if self.workcenter_id else "N/A"

        # Punto 2: Referencia y Descripción del producto
        producto_desc = self.product_id.display_name 

        # PW812 = 10cm | LL609 = 7.5cm
        zpl = f"""^XA^PW812^LL609^CI28
^FO50,40^A0N,25,25^FDFECHA REVISADO : {ahora}^FS
^FO50,80^A0N,25,25^FDCENTRO DE TRABAJO : {wc_name}^FS
^FO50,120^A0N,25,25^FDORDEN DE FABRICACION : {self.name}^FS
^FO50,160^A0N,20,20^FDPRODUCTO : {producto_desc[:70]}^FS
^FO0,230^FB812,1,0,C^A0N,100,90^FD{peso:.4f} kg^FS
^FO180,360^BCN,110,N,N,N^FD{lote_name}^FS
^FO0,510^FB812,1,0,C^A0N,30,30^FDLOTE : {lote_name}^FS
^XZ"""
        self.last_zpl_label = zpl
        return True

class MrpWorkorder(models.Model):
    _inherit = 'mrp.workorder'

    product_id_category_name = fields.Char(
        related='production_id.product_id.categ_id.display_name',
        string="Categoria de Producto",
        readonly=True
    )

    def button_finish(self):
        """ Validación de cierre de la Orden de Trabajo (Tableta/Lista) """
        for wo in self:
            prod = wo.production_id
            if prod.product_id.categ_id.complete_name == 'Producto En Proceso / Tac-Producto en proceso-Tejido Circular-kg':
                if prod.rollos_revisados_count < prod.rollos_requeridos_count:
                    raise UserError(_(
                        "Control de Calidad Obligatorio:\n"
                        "Se deben revisar al menos %s rollos en el centro de trabajo %s.\n"
                        "Progreso: %s de %s rollos.") % (
                            prod.rollos_requeridos_count, 
                            wo.workcenter_id.name,
                            prod.rollos_revisados_count, 
                            prod.rollos_requeridos_count
                        ))
        return super(MrpWorkorder, self).button_finish()

class MrpRevisionLog(models.Model):
    _name = 'mrp.revision.log'
    _description = 'Log de Revisión de Calidad de Telas'
    _order = 'create_date desc'

    production_id = fields.Many2one('mrp.production', string="Orden de Fabricación", ondelete='cascade', index=True)
    lot_id = fields.Many2one('stock.lot', string="Rollo", required=True)
    user_id = fields.Many2one('res.users', string="Revisor", default=lambda self: self.env.user)
    
    peso_original = fields.Float(string="Peso Inicial", readonly=True)
    peso_final = fields.Float(string="Peso Revisado")
    diferencia = fields.Float(compute="_compute_diff", string="Diferencia", store=True)
    create_date = fields.Datetime(string="Fecha/Hora", default=fields.Datetime.now)

    @api.depends('peso_original', 'peso_final')
    def _compute_diff(self):
        for reg in self:
            # Lógica original: Peso Inicial - Peso Revisado
            reg.diferencia = reg.peso_original - reg.peso_final
    
    causa_id = fields.Many2one(
        'quality.tag', 
        string="Causa de Desviación",
        domain="[('name', '=like', 'TEJIDO%')]",
        required=False
    )

class StockLot(models.Model):
    _inherit = 'stock.lot'
    needs_review = fields.Boolean(string="Necesita Revisión", default=False)
    is_reviewed = fields.Boolean(string="Revisado", default=False)
    production_id = fields.Many2one('mrp.production', string="Orden de Fabricación")

class MrpWorkcenter(models.Model):
    _inherit = 'mrp.workcenter'

    numero_rollos_revisar = fields.Integer(
        string='Muestreo: Rollos a Revisar', 
        default=1,
        help="Número de rollos que el sistema exigirá pesar en este Centro de Trabajo."
    )