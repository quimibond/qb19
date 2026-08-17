# -*- coding: utf-8 -*-
from odoo import models, fields, api, _, tools
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
        # Lógica para detectar si es DNP
        # Ajusta según si quieres validar por código del producto final o código de la BoM
            
        for reg in self:
            # 1. Validamos el prefijo de la BoM (usamos 'or' para evitar errores si no tiene BoM)
            es_dnp = reg.bom_id and reg.bom_id.code and reg.bom_id.code.startswith('DNP')
            if es_dnp:
                # Si es DNP, forzamos el valor a 1
                reg.rollos_requeridos_count = 1
            else:
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

    def _print_zpl_label(self, lote_name, peso, nombre_producto, pesador=False):
        """ Etiqueta de Revisado Corregida (10x7.5cm) """
        self.ensure_one()
        # --- CAMBIO AQUÍ: Convertir UTC a Hora Local del Usuario ---
        ahora_utc = fields.Datetime.now()
        ahora_local = fields.Datetime.context_timestamp(self, ahora_utc)
        ahora = ahora_local.strftime('%d/%m/%Y %H:%M:%S')
        
        # Corrección Centro de Trabajo: Usamos el campo workcenter_id de la MO
        wc_name = self.workcenter_id.name if self.workcenter_id else "N/A"

        # Referencia y Descripción
        producto_desc = self.product_id.display_name 

        # PW812 = 10cm | LL609 = 7.5cm
        # ^BY2 = Grosor fino | ^FO100 = Inicio a la izquierda
        zpl = f"""^XA^PW812^LL609^CI28
^FO50,40^A0N,25,25^FDFECHA REVISADO : {ahora}^FS
^FO50,80^A0N,25,25^FDC.T. : {wc_name} / {pesador or ''}^FS
^FO50,120^A0N,25,25^FDORDEN DE FABRICACION : {self.name}^FS
^FO50,160^A0N,20,20^FDPRODUCTO : {producto_desc[:70]}^FS
^FO0,230^FB812,1,0,C^A0N,100,90^FD{peso:.4f} kg^FS
^BY2,3,110^FO100,360^BCN,110,N,N,N^FD{lote_name}^FS
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
        # Definimos la lista de categorías que requieren este control
        categorias_validas = [
            'Producto En Proceso / Tac-Producto en proceso-Tejido Circular-kg',
            'Producto En Proceso / Tac-Producto en proceso-DNP Tejido Circular-kg'
        ]
        for wo in self:
            prod = wo.production_id
            # Verificamos si la categoría del producto está en nuestra lista permitida
            if prod.product_id.categ_id.complete_name in categorias_validas:
                if prod.rollos_revisados_count < prod.rollos_requeridos_count:
                    raise UserError(_(
                        "Control de Calidad Pendiente:\n"
                        "Se requiere un MÍNIMO de %s rollos revisados para este centro de trabajo (%s).\n"
                        "Progreso actual: %s de %s rollos.") % (
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
    user_id = fields.Many2one('res.users', string="Usuario", default=lambda self: self.env.user)
    inspector = fields.Char(string="Inspector")
    peso_original = fields.Float(string="Peso Inicial", digits=(12, 4), readonly=True)
    peso_final = fields.Float(string="Peso Revisado", digits=(12, 4))
    diferencia = fields.Float(compute="_compute_diff", string="Diferencia", digits=(12, 4), store=True)
    create_date = fields.Datetime(string="Fecha/Hora", default=fields.Datetime.now)

    @api.depends('peso_original', 'peso_final')
    def _compute_diff(self):
        for reg in self:
            # Usamos float_round para evitar residuos binarios en la resta
            # 1. Obtenemos la precisión de la UoM del lote
            uom = reg.lot_id.product_uom_id
            
            # 2. Realizamos la resta
            diff = reg.peso_original - reg.peso_final
            
            # 3. REDONDEO CRÍTICO: Usamos .round() (sin guion bajo) 
            # para que 0.55000000001 sea exactamente 0.55
            reg.diferencia = uom.round(diff) if uom else diff
    
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
    consecutivo_maquina = fields.Integer(string="Rollo Circular", default=0)


class MrpRevisionReport(models.Model):
    """
    Vista SQL de solo lectura para el "Reporte de Revisión de Rollos".

    Deliberadamente NO es una tabla física (_auto = False): no guarda nada,
    no necesita recomputo ni backfill al hacer -u. Cada vez que se abre el
    reporte, PostgreSQL resuelve la consulta contra los datos actuales de
    mrp_revision_log / mrp_production / mrp_workorder. Los registros
    históricos y los nuevos se comportan exactamente igual, sin pasos
    manuales ni hooks de instalación.

    El centro de trabajo se toma de mrp_workorder.workcenter_id (primera
    orden de trabajo con centro de trabajo asignado). mrp_production.
    workcenter_id NO se usa: en esta instancia es un campo calculado sin
    columna física en la base de datos (confirmado por error UndefinedColumn
    al intentar leerlo directo en SQL), así que la única fuente confiable a
    nivel de tabla es mrp_workorder.
    """
    _name = 'mrp.revision.report'
    _description = 'Reporte de Revisión de Rollos'
    _auto = False
    _order = 'production_date_finished desc, create_date desc'

    production_id = fields.Many2one('mrp.production', string="Orden de Fabricación", readonly=True)
    production_date_finished = fields.Datetime(string="Fecha de Terminación de la Orden", readonly=True)
    product_id = fields.Many2one('product.product', string="Producto", readonly=True)
    workcenter_id = fields.Many2one('mrp.workcenter', string="Centro de Trabajo", readonly=True)
    create_date = fields.Datetime(string="Fecha de Revisión", readonly=True)
    lot_id = fields.Many2one('stock.lot', string="Rollo", readonly=True)
    user_id = fields.Many2one('res.users', string="Usuario", readonly=True)
    inspector = fields.Char(string="Inspector", readonly=True)
    peso_original = fields.Float(string="Peso Inicial", digits=(12, 4), readonly=True)
    peso_final = fields.Float(string="Peso Revisado", digits=(12, 4), readonly=True)
    diferencia = fields.Float(string="Diferencia", digits=(12, 4), readonly=True)
    causa_id = fields.Many2one('quality.tag', string="Causa de la Desviación", readonly=True)

    def init(self):
        tools.drop_view_if_exists(self.env.cr, self._table)
        self.env.cr.execute(f"""
            CREATE OR REPLACE VIEW {self._table} AS (
                SELECT
                    l.id                                    AS id,
                    l.production_id                         AS production_id,
                    mp.date_finished                        AS production_date_finished,
                    mp.product_id                            AS product_id,
                    wo.workcenter_id                         AS workcenter_id,
                    l.create_date                           AS create_date,
                    l.lot_id                                AS lot_id,
                    l.user_id                                AS user_id,
                    l.inspector                              AS inspector,
                    l.peso_original                          AS peso_original,
                    l.peso_final                             AS peso_final,
                    l.diferencia                             AS diferencia,
                    l.causa_id                               AS causa_id
                FROM mrp_revision_log l
                JOIN mrp_production mp ON mp.id = l.production_id
                LEFT JOIN LATERAL (
                    SELECT w.workcenter_id
                    FROM mrp_workorder w
                    WHERE w.production_id = mp.id AND w.workcenter_id IS NOT NULL
                    ORDER BY w.id
                    LIMIT 1
                ) wo ON true
            )
        """)