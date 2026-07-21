# -*- coding: utf-8 -*-
from odoo import models, fields, api


class ResConfigSettings(models.TransientModel):
    """Panel de Ajustes del SGI: la cara amigable de los parámetros.

    Cada campo guarda en el mismo ir.config_parameter que ya usa el código,
    así que editar aquí o en Parámetros del sistema es equivalente — pero
    aquí con nombres claros, ayuda y selectores (sin modo desarrollador).
    """
    _inherit = 'res.config.settings'

    sgi_nc_escalation_days = fields.Integer(
        string="Días para escalar una NC sin acciones",
        config_parameter='quimibond_sgi.nc_escalation_days',
        help="NC interna sin acciones tras estos días → actividad al responsable "
             "y aviso a MAST.")
    sgi_nc_escalation_days_external = fields.Integer(
        string="Días para escalar una NC externa/cliente",
        config_parameter='quimibond_sgi.nc_escalation_days_external',
        help="Las NC de auditoría externa y reclamaciones de cliente escalan más "
             "rápido que las internas.")
    sgi_doc_review_notice_days = fields.Integer(
        string="Primer aviso de revisión documental (días)",
        config_parameter='quimibond_sgi.doc_review_notice_days')
    sgi_doc_review_notice_days_final = fields.Integer(
        string="Segundo aviso de revisión documental (días)",
        config_parameter='quimibond_sgi.doc_review_notice_days_final')
    sgi_doc_pilot_notice_days = fields.Integer(
        string="Aviso de piloto por vencer (días)",
        config_parameter='quimibond_sgi.doc_pilot_notice_days')
    sgi_doc_ack_pending_days = fields.Integer(
        string="Días para reclamar un acuse pendiente",
        config_parameter='quimibond_sgi.doc_ack_pending_days')
    sgi_nc_recurrence_months = fields.Integer(
        string="Ventana de reincidencia de NC (meses)",
        config_parameter='quimibond_sgi.nc_recurrence_months',
        help="Una NC del SGI cuenta como reincidente si en este número de meses "
             "hubo otra NC del mismo proceso (misma cláusula pesa doble).")
    sgi_action_escalation_manager_days = fields.Integer(
        string="Días para escalar una acción vencida al jefe",
        config_parameter='quimibond_sgi.action_escalation_manager_days',
        help="Acción vencida por más de estos días → además del responsable, "
             "se avisa a su jefe directo (fallback Jefe MAST).")
    sgi_action_escalation_director_days = fields.Integer(
        string="Días para escalar una acción vencida a Dirección",
        config_parameter='quimibond_sgi.action_escalation_director_days',
        help="Acción vencida por más de estos días → además, se avisa a "
             "Dirección.")
    sgi_fmea_npr_action = fields.Integer(
        string="NPR que exige acción en el AMEF",
        config_parameter='quimibond_sgi.fmea_npr_action')
    sgi_risk_ryo_inmediata = fields.Integer(
        string="RyO: puntaje para atención Inmediata",
        config_parameter='quimibond_sgi.risk_ryo_inmediata')
    sgi_risk_ryo_media = fields.Integer(
        string="RyO: puntaje para atención Media",
        config_parameter='quimibond_sgi.risk_ryo_media')
    sgi_risk_ryo_intermedia = fields.Integer(
        string="RyO: puntaje para atención Intermedia",
        config_parameter='quimibond_sgi.risk_ryo_intermedia')
    sgi_supplier_weight_otd = fields.Float(
        string="Peso de entregas a tiempo",
        config_parameter='quimibond_sgi.supplier_weight_otd',
        help="Peso (0-1) de la puntualidad en la calificación del proveedor.")
    sgi_supplier_weight_quality = fields.Float(
        string="Peso de calidad",
        config_parameter='quimibond_sgi.supplier_weight_quality')
    sgi_supplier_nc_penalty = fields.Float(
        string="Puntos que descuenta cada NC al proveedor",
        config_parameter='quimibond_sgi.supplier_nc_penalty')
    sgi_pesaje_tolerance_kg = fields.Float(
        string="Tolerancia de peso de rollo (kg)",
        config_parameter='quimibond_sgi.pesaje_tolerance_kg',
        help="Rollo confirmado fuera de esta tolerancia → alerta de calidad automática.")
    sgi_monthly_sales_budget = fields.Float(
        string="Presupuesto mensual de ventas (MXN)",
        config_parameter='quimibond_sgi.monthly_sales_budget',
        help="Alimenta los KPIs de cumplimiento y eficiencia del presupuesto.")
    sgi_rh_user_id = fields.Many2one(
        'res.users', string="Usuario de RH",
        help="Recibe las actividades automáticas de RH (competencias por vencer, DNC).")
    sgi_waste_categ_id = fields.Many2one(
        'product.category', string="Categoría del byproduct de desperdicio",
        help="Categoría del SALDO (desperdicio) para el KPI automático.")
    sgi_purchase_approval_category_id = fields.Many2one(
        'approval.category', string="Categoría de requisiciones de compra",
        help="KPI CO-02 (Requisiciones): categoría de aprobación que cuenta como "
             "requisición de compra. Déjalo vacío para detectar automáticamente "
             "la(s) categoría(s) de tipo compra; configúralo solo si hay varias.")
    sgi_production_monthly_capacity = fields.Float(
        string="Capacidad instalada mensual de producción",
        config_parameter='quimibond_sgi.production_monthly_capacity',
        help="KPI MA-02 (Producido vs capacidad): capacidad mensual en la misma "
             "unidad que la producción (p.ej. kg). Para periodos no mensuales se "
             "prorratea por días. 0 = captura manual.")
    sgi_energy_partner_id = fields.Many2one(
        'res.partner', string="Proveedor de energía",
        help="KPI TR-03 (Consumo de energía): proveedor cuyas facturas del periodo "
             "suman el consumo. Sin configurar, la medición queda en 0 con nota.")
    sgi_sales_budget_alert_pct = fields.Integer(
        string="Umbral de aviso de presupuesto de ventas (%)",
        config_parameter='quimibond_sgi.sales_budget_alert_pct',
        help="Al cierre de mes, si un equipo con presupuesto aprobado lleva "
             "acumulado por debajo de este % del presupuesto del año, se avisa a "
             "su responsable.")

    @api.model
    def get_values(self):
        res = super().get_values()
        Param = self.env['ir.config_parameter'].sudo()
        rh_id = int(Param.get_param('quimibond_sgi.rh_user_id', '0') or 0)
        res['sgi_rh_user_id'] = rh_id if rh_id and self.env['res.users'].browse(rh_id).exists() else False
        categ_name = Param.get_param('quimibond_sgi.waste_subproduct_category', 'SubProducto')
        categ = self.env['product.category'].search([('name', '=', categ_name)], limit=1)
        res['sgi_waste_categ_id'] = categ.id or False
        cat_id = int(Param.get_param('quimibond_sgi.purchase_approval_category_id', '0') or 0)
        res['sgi_purchase_approval_category_id'] = (
            cat_id if cat_id and self.env['approval.category'].browse(cat_id).exists()
            else False)
        energy_id = int(Param.get_param('quimibond_sgi.energy_partner_id', '0') or 0)
        res['sgi_energy_partner_id'] = (
            energy_id if energy_id and self.env['res.partner'].browse(energy_id).exists()
            else False)
        return res

    def set_values(self):
        super().set_values()
        Param = self.env['ir.config_parameter'].sudo()
        Param.set_param('quimibond_sgi.rh_user_id', self.sgi_rh_user_id.id or 0)
        if self.sgi_waste_categ_id:
            Param.set_param('quimibond_sgi.waste_subproduct_category',
                            self.sgi_waste_categ_id.name)
        Param.set_param('quimibond_sgi.purchase_approval_category_id',
                        self.sgi_purchase_approval_category_id.id or 0)
        Param.set_param('quimibond_sgi.energy_partner_id',
                        self.sgi_energy_partner_id.id or 0)
