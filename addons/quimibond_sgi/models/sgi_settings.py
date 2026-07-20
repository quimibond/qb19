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
             "y aviso a MAST (las externas/cliente escalan a 3 días, fijo).")
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

    @api.model
    def get_values(self):
        res = super().get_values()
        Param = self.env['ir.config_parameter'].sudo()
        rh_id = int(Param.get_param('quimibond_sgi.rh_user_id', '0') or 0)
        res['sgi_rh_user_id'] = rh_id if rh_id and self.env['res.users'].browse(rh_id).exists() else False
        categ_name = Param.get_param('quimibond_sgi.waste_subproduct_category', 'SubProducto')
        categ = self.env['product.category'].search([('name', '=', categ_name)], limit=1)
        res['sgi_waste_categ_id'] = categ.id or False
        return res

    def set_values(self):
        super().set_values()
        Param = self.env['ir.config_parameter'].sudo()
        Param.set_param('quimibond_sgi.rh_user_id', self.sgi_rh_user_id.id or 0)
        if self.sgi_waste_categ_id:
            Param.set_param('quimibond_sgi.waste_subproduct_category',
                            self.sgi_waste_categ_id.name)
