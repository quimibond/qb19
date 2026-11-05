# -*- coding: utf-8 -*-
"""Ajustes, parámetros y avisos del Diagnóstico del presupuesto y del
pronóstico de ventas. Vivían en quimibond_sgi (sgi_settings.py,
sgi_format_map.py y sgi_diagnostic.py) hasta 57.10.0 (A-016). Las claves de
los parámetros no cambian (``quimibond_sgi.*``)."""
from odoo import api, fields, models

from odoo.addons.quimibond_sgi.models.sgi_format_map import SgiConfig

BUDGET_PARAMS = {
    # Umbral (%) de cumplimiento acumulado del presupuesto de ventas bajo el
    # cual el cierre de mes avisa al responsable del equipo.
    'quimibond_sgi.sales_budget_alert_pct': '80',
    # Tipo de cambio presupuestal USD→MXN para sugerir precios de listas en
    # otra moneda. 0 = usar el tipo de cambio vigente del día de captura.
    'quimibond_sgi.budget_planning_rate': '0',
    # Control de precios (lista vs facturado): tolerancia % del gap para
    # 'leve' y umbral 'grave'.
    'quimibond_sgi.price_gap_tolerance_pct': '3.0',
    'quimibond_sgi.price_gap_grave_pct': '10.0',
    # Cumplimiento mínimo (%) del presupuesto por debajo del cual se pide
    # justificación del incumplimiento (P-A28 4.3.6.1). No bloquea nada.
    'quimibond_sgi.budget_fulfillment_min': '80',
    # Cobertura del pronóstico (P-A28 4.2.2.7): tolerancia % sobre 100 para
    # 'excedido', y horizonte de captura en semanas (semana actual + N-1).
    'quimibond_sgi.forecast_over_tolerance_pct': '10.0',
    'quimibond_sgi.forecast_capture_horizon_weeks': '3',
    # Precio de lista mínimo plausible (moneda compañía): por debajo se toma
    # como placebo (placeholder $1) y la línea queda sin precio de lista.
    'quimibond_sgi.price_min_plausible': '5.0',
    # Lista de precios PRESUPUESTAL para líneas sin cliente (id). 0 = sin
    # configurar: esas líneas quedan sin precio (nunca una lista arbitraria).
    'quimibond_sgi.budget_pricelist_id': '0',
    # 3.5.0: objetivo y mínimo de crecimiento del presupuesto anual.
    'quimibond_ventas_presupuesto.objetivo_pct': '20',
    'quimibond_ventas_presupuesto.minimo_pct': '15',
}


class SgiConfigSalesBudget(models.AbstractModel):
    _inherit = 'sgi.config'

    _SGI_DEFAULT_PARAMS = dict(SgiConfig._SGI_DEFAULT_PARAMS, **BUDGET_PARAMS)


class SgiDiagnosticSalesBudget(models.TransientModel):
    _inherit = 'sgi.diagnostic'

    @api.model
    def _sgi_key_settings_checks(self):
        return super()._sgi_key_settings_checks() + [
            ('quimibond_sgi.budget_pricelist_id',
             "Lista de precios presupuestal sin configurar: las líneas globales del presupuesto quedan sin precio."),
        ]


class ResConfigSettings(models.TransientModel):
    _inherit = 'res.config.settings'

    sgi_sales_budget_alert_pct = fields.Integer(
        string="Umbral de aviso de presupuesto de ventas (%)",
        config_parameter='quimibond_sgi.sales_budget_alert_pct',
        help="Al cierre de mes, si un equipo con presupuesto aprobado lleva "
             "acumulado por debajo de este % del presupuesto del año, se avisa a "
             "su responsable.")
    sgi_budget_planning_rate = fields.Float(
        string="Tipo de cambio presupuestal USD→MXN",
        config_parameter='quimibond_sgi.budget_planning_rate',
        help="Para sugerir precios de listas en otra moneda al presupuestar. "
             "0 = usar el tipo de cambio vigente del día de captura.")
    sgi_price_gap_tolerance_pct = fields.Float(
        string="Tolerancia de desviación de precio (%)",
        config_parameter='quimibond_sgi.price_gap_tolerance_pct',
        help="Control de precios: gap facturado vs lista dentro de este % = OK.")
    sgi_price_gap_grave_pct = fields.Float(
        string="Desviación de precio grave (%)",
        config_parameter='quimibond_sgi.price_gap_grave_pct',
        help="Gap por encima de este % = grave (entre la tolerancia y este umbral "
             "= leve).")
    sgi_forecast_over_tolerance_pct = fields.Float(
        string="Tolerancia de pronóstico excedido (%)",
        config_parameter='quimibond_sgi.forecast_over_tolerance_pct',
        help="Cobertura del pronóstico: comprometido por encima de 100% + este % "
             "= 'excedido'.")
    sgi_forecast_capture_horizon_weeks = fields.Integer(
        string="Horizonte de captura del pronóstico (semanas)",
        config_parameter='quimibond_sgi.forecast_capture_horizon_weeks',
        help="Solo se evalúa la cobertura de las semanas dentro de este horizonte "
             "(semana actual + N-1); las de fuera quedan 'fuera_horizonte'.")
    sgi_budget_fulfillment_min = fields.Integer(
        string="Cumplimiento mínimo del presupuesto (%)",
        config_parameter='quimibond_sgi.budget_fulfillment_min',
        help="P-A28 4.3.6.1: si un presupuesto aprobado va por debajo de este % de "
             "cumplimiento, se pide justificación (banner rojo y actividad al Admin "
             "de ventas). No bloquea nada.")
    sgi_price_min_plausible = fields.Float(
        string="Precio de lista mínimo plausible (moneda compañía)",
        config_parameter='quimibond_sgi.price_min_plausible',
        help="Un precio de lista resuelto por debajo de este umbral se toma como "
             "placebo (placeholder $1) y la línea queda 'sin precio de lista', "
             "aunque haya una regla. Cierra el hoyo de los precios placeholder.")
    sgi_budget_pricelist_id = fields.Many2one(
        'product.pricelist', string="Lista de precios presupuestal",
        help="Lista con que se valúan las líneas del presupuesto SIN cliente "
             "(global). Sin configurar, esas líneas quedan sin precio (NUNCA se "
             "toma una lista arbitraria: eso valuaba el global con la tarifa de un "
             "cliente).")

    # 3.5.0: equipo de ventas de cada mercado base del presupuesto anual (el real y lo
    # pedido de los renglones se miden en ese equipo) y objetivo y mínimo de crecimiento.
    qb_team_industrial_id = fields.Many2one(
        'crm.team', string="Equipo de Industrial",
        config_parameter='quimibond_ventas_presupuesto.team_industrial',
        help="Equipo de ventas de Industrial y Proyectos industrial en el presupuesto.")
    qb_team_confeccion_id = fields.Many2one(
        'crm.team', string="Equipo de Confección",
        config_parameter='quimibond_ventas_presupuesto.team_confeccion',
        help="Equipo de ventas de Confección y Proyectos confección en el presupuesto.")
    qb_team_especiales_id = fields.Many2one(
        'crm.team', string="Equipo de Especiales",
        config_parameter='quimibond_ventas_presupuesto.team_especiales')
    qb_objetivo_pct = fields.Float(
        string="Objetivo de crecimiento del presupuesto (%)",
        config_parameter='quimibond_ventas_presupuesto.objetivo_pct',
        help="Valor inicial de cada revisión nueva (20 % por omisión).")
    qb_minimo_pct = fields.Float(
        string="Crecimiento mínimo del presupuesto (%)",
        config_parameter='quimibond_ventas_presupuesto.minimo_pct',
        help="Valor inicial de cada revisión nueva (15 % por omisión).")

    @api.model
    def get_values(self):
        res = super().get_values()
        Param = self.env['ir.config_parameter'].sudo()
        pl_id = int(Param.get_param('quimibond_sgi.budget_pricelist_id', '0') or 0)
        res['sgi_budget_pricelist_id'] = (
            pl_id if pl_id and self.env['product.pricelist'].browse(pl_id).exists()
            else False)
        return res

    def set_values(self):
        Param = self.env['ir.config_parameter'].sudo()
        team_keys = ['quimibond_ventas_presupuesto.team_%s' % b
                     for b in ('industrial', 'confeccion', 'especiales')]
        before = [Param.get_param(key) for key in team_keys]
        super().set_values()
        Param.set_param('quimibond_sgi.budget_pricelist_id', self.sgi_budget_pricelist_id.id or 0)
        # 3.5.1: si cambia el equipo de un mercado, los renglones lo toman y se recalcula su real.
        if [Param.get_param(key) for key in team_keys] != before:
            self.env['sgi.sales.budget.line'].sudo()._qb_refresh_market_teams()
