# -*- coding: utf-8 -*-
from odoo import models, fields, api


class ResConfigSettings(models.TransientModel):
    """Panel de Ajustes del SGI: la cara amigable de los parámetros.

    Cada campo guarda en el mismo ir.config_parameter que ya usa el código,
    así que editar aquí o en Parámetros del sistema es equivalente — pero
    aquí con nombres claros, ayuda y selectores (sin modo desarrollador).
    """
    _inherit = 'res.config.settings'

    # 57.119.0 (C1, bloque 3): bloqueo del artículo genérico de muestra.
    sgi_dev_block_generic_sample_from = fields.Char(
        string="Bloquear «MUESTRA PILOTO» en órdenes nuevas desde (AAAA-MM-DD)",
        config_parameter='quimibond_sgi.dev_block_generic_sample_from',
        help="A partir de esta fecha no se crean ni confirman órdenes de fabricación con los artículos "
             "genéricos «MUESTRA PILOTO TEJIDO / TINTORERÍA»: las muestras de desarrollo se piden desde el "
             "proyecto con su artículo generado. Vacío: sin bloqueo (hay órdenes abiertas con ellos; la "
             "fecha la decide Dirección de Finanzas).")
    # 57.120.0 (C1, bloque 4): quién autoriza las pruebas de laboratorio de los desarrollos.
    sgi_dev_lab_authorizer_job_id = fields.Many2one(
        'hr.job', string="Puesto que autoriza pruebas de laboratorio de desarrollos",
        config_parameter='quimibond_sgi.dev_lab_authorizer_job_id',
        help="Puesto cuyas personas autorizan las solicitudes de pruebas de los proyectos de desarrollo. "
             "Por omisión, el Coordinador de Laboratorio y MP (se busca por nombre si el parámetro está "
             "vacío). El Jefe MAST siempre puede.")
    sgi_nc_escalation_days = fields.Integer(
        string="Días hábiles para escalar una NC sin acciones",
        config_parameter='quimibond_sgi.nc_escalation_days',
        help="NC interna sin acciones tras estos días → actividad al responsable "
             "y aviso a MAST.")
    sgi_nc_escalation_days_external = fields.Integer(
        string="Días hábiles para escalar una NC externa/cliente",
        config_parameter='quimibond_sgi.nc_escalation_days_external',
        help="Las NC de auditoría externa y reclamaciones de cliente escalan más "
             "rápido que las internas.")
    sgi_nc_days_containment = fields.Integer(
        string="Plazo de contención de una NC (días hábiles)",
        config_parameter='quimibond_sgi.nc_days_containment',
        help="Días hábiles, desde que se abre una NC, para registrar la contención.")
    sgi_nc_days_root_cause = fields.Integer(
        string="Plazo de causa raíz de una NC (días hábiles)",
        config_parameter='quimibond_sgi.nc_days_root_cause',
        help="Días hábiles, desde que se abre una NC, para capturar la causa raíz.")
    sgi_nc_days_plan = fields.Integer(
        string="Plazo del plan de acción de una NC (días hábiles)",
        config_parameter='quimibond_sgi.nc_days_plan',
        help="Días hábiles, desde que se abre una NC, para registrar el plan de acción.")
    sgi_nc_escalation_mast_days = fields.Integer(
        string="Días hábiles vencido un plazo de NC antes de escalar a MAST",
        config_parameter='quimibond_sgi.nc_escalation_mast_days',
        help="Días hábiles que puede estar vencido un plazo de NC antes de escalar al Jefe MAST y SGI.")
    sgi_nc_days_supplier_response = fields.Integer(
        string="Días hábiles para que el proveedor conteste una NC (portal)",
        config_parameter='quimibond_sgi.nc_days_supplier_response',
        help="Días hábiles que tiene el proveedor para contestar una NC por el portal.")
    sgi_nc_effectiveness_days = fields.Integer(
        string="Días para verificar la eficacia tras la última correctiva",
        config_parameter='quimibond_sgi.nc_effectiveness_days',
        help="Días después de terminar la última acción correctiva en que se pide verificar la eficacia.")
    sgi_doc_review_notice_days = fields.Integer(
        string="Primer aviso de revisión documental (días)",
        config_parameter='quimibond_sgi.doc_review_notice_days',
        help="Días antes de la próxima revisión de un documento en que llega el primer aviso.")
    sgi_doc_review_notice_days_final = fields.Integer(
        string="Segundo aviso de revisión documental (días)",
        config_parameter='quimibond_sgi.doc_review_notice_days_final',
        help="Días antes de la próxima revisión de un documento en que llega el segundo aviso.")
    sgi_doc_pilot_notice_days = fields.Integer(
        string="Aviso de piloto por vencer (días)",
        config_parameter='quimibond_sgi.doc_pilot_notice_days',
        help="Días antes del fin de una prueba piloto documental en que llega el aviso.")
    sgi_doc_ack_pending_days = fields.Integer(
        string="Días hábiles para reclamar un acuse pendiente",
        config_parameter='quimibond_sgi.doc_ack_pending_days',
        help="Días hábiles que puede estar pendiente un acuse de lectura antes de avisar.")
    sgi_nc_recurrence_months = fields.Integer(
        string="Ventana de reincidencia de NC (meses)",
        config_parameter='quimibond_sgi.nc_recurrence_months',
        help="Una NC del SGI cuenta como reincidente si en este número de meses "
             "hubo otra NC del mismo proceso (misma cláusula pesa doble).")
    sgi_action_escalation_manager_days = fields.Integer(
        string="Días hábiles para escalar una acción vencida al jefe",
        config_parameter='quimibond_sgi.action_escalation_manager_days',
        help="Acción vencida por más de estos días → además del responsable, "
             "se avisa a su jefe directo (fallback Jefe MAST).")
    sgi_action_escalation_director_days = fields.Integer(
        string="Días hábiles para escalar una acción vencida a Dirección",
        config_parameter='quimibond_sgi.action_escalation_director_days',
        help="Acción vencida por más de estos días → además, se avisa a "
             "Dirección.")
    sgi_fmea_npr_action = fields.Integer(
        string="NPR que exige acción en el AMEF",
        config_parameter='quimibond_sgi.fmea_npr_action',
        help="Número de prioridad de riesgo a partir del cual un modo de falla del AMEF exige acción.")
    sgi_risk_ryo_inmediata = fields.Integer(
        string="RyO: puntaje para atención Inmediata",
        config_parameter='quimibond_sgi.risk_ryo_inmediata',
        help="Puntaje (probabilidad × impacto) desde el cual un riesgo es de atención inmediata.")
    sgi_risk_ryo_media = fields.Integer(
        string="RyO: puntaje para atención Media",
        config_parameter='quimibond_sgi.risk_ryo_media',
        help="Puntaje (probabilidad × impacto) desde el cual un riesgo es de atención media.")
    sgi_risk_ryo_intermedia = fields.Integer(
        string="RyO: puntaje para atención Intermedia",
        config_parameter='quimibond_sgi.risk_ryo_intermedia',
        help="Puntaje (probabilidad × impacto) desde el cual un riesgo es de atención intermedia.")
    sgi_supplier_weight_otd = fields.Float(
        string="Peso de entregas a tiempo",
        config_parameter='quimibond_sgi.supplier_weight_otd',
        help="Peso (0-1) de la puntualidad en la calificación del proveedor.")
    sgi_supplier_weight_quality = fields.Float(
        string="Peso de calidad",
        config_parameter='quimibond_sgi.supplier_weight_quality',
        help="Peso de la calidad en la calificación del proveedor; el resto es la entrega a tiempo.")
    sgi_supplier_nc_penalty = fields.Float(
        string="Puntos que descuenta cada NC al proveedor",
        config_parameter='quimibond_sgi.supplier_nc_penalty',
        help="Puntos que resta cada NC a la calificación de calidad del proveedor (sobre 100).")
    sgi_supplier_otd_tolerance_days = fields.Integer(
        string="Tolerancia OTD de proveedores (días)",
        config_parameter='quimibond_sgi.supplier_otd_tolerance_days',
        help="Días de gracia sobre la fecha compromiso para contar una "
             "recepción como a tiempo (comparación por día calendario).")
    # 57.10.0 (A-020): la tolerancia de peso de rollo (sgi_pesaje_tolerance_kg)
    # la declara quimibond_sgi_pesaje, dueño del parámetro.
    sgi_monthly_sales_budget = fields.Float(
        string="Presupuesto mensual de ventas (MXN)",
        config_parameter='quimibond_sgi.monthly_sales_budget',
        help="Alimenta los KPIs de cumplimiento y eficiencia del presupuesto.")
    sgi_rh_user_id = fields.Many2one(
        'res.users', string="Usuario de RH",
        help="Recibe las actividades automáticas de RH (competencias por vencer, DNC).")
    sgi_calibration_block_expired = fields.Boolean(
        string="Bloquear equipos con calibración vencida",
        config_parameter='quimibond_sgi.calibration_block_expired',
        help="Apagado (default, decisión de la tanda 2): el cron solo avisa con un "
             "resumen diario al Coordinador de Laboratorio y al Jefe de Calidad. "
             "Encendido: además marca «No usar» cada equipo vencido y manda el "
             "correo crítico. La inspección de calidad rechaza un equipo vencido "
             "en cualquier caso.")
    sgi_checklist_pin_required = fields.Boolean(
        string="PIN obligatorio para firmar checklists",
        config_parameter='quimibond_sgi.checklist_pin_required',
        help="Apagado (default): quien no tiene PIN firma la hoja y queda «(sin PIN "
             "registrado)». Encendido (D-08): sin PIN capturado en su ficha de "
             "empleado no se firma. Encender solo cuando RH haya capturado los PIN "
             "(el mismo del quiosco de asistencia).")
    sgi_mast_user_id = fields.Many2one(
        'res.users', string="Jefe MAST y SGI",
        help="Recibe los avisos automáticos del SGI que no tienen dueño "
             "(parámetro quimibond_sgi.mast_user_id). Vacío: el usuario "
             "mas@quimibond.com o, si no existe, el primer miembro directo del "
             "grupo Jefe MAST y SGI. Al cambiarlo, los avisos abiertos se "
             "reasignan en la siguiente corrida de cada cron (no se duplican).")
    sgi_purchase_approval_category_id = fields.Many2one(
        'approval.category', string="Categoría de requisiciones de compra",
        help="KPI CO-02 (Requisiciones): categoría de aprobación que cuenta como "
             "requisición de compra. Déjelo vacío para detectar automáticamente "
             "la(s) categoría(s) de tipo compra; configúrelo solo si hay varias.")
    sgi_supplier_critical_categ_ids = fields.Many2many(
        'product.category', string="Categorías de proveedores críticos",
        help="Materia prima y maquila: solo los proveedores que entregan productos de "
             "estas categorías (o los marcados como críticos en el contacto) entran a "
             "la evaluación trimestral. Vacío = la categoría de materia prima más las "
             "que se llaman «maquila».")
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
    sgi_satisfaction_survey_id = fields.Many2one(
        'survey.survey', string="Encuesta de satisfacción (CA-02)",
        help="Encuesta cuyas respuestas alimentan el KPI CA-02. Sin configurar "
             "se usa la sembrada por el módulo. Útil para re-apuntar al "
             "histórico de respuestas (aunque esté archivado).")
    # 57.11.0 (A-016): los ajustes del presupuesto y del pronóstico de ventas
    # (umbral de aviso, tipo de cambio, lista presupuestal, precio mínimo,
    # desviación de precio, cumplimiento mínimo, cobertura del pronóstico) los
    # declara quimibond_ventas_presupuesto. Las claves no cambian.

    @api.model
    def get_values(self):
        res = super().get_values()
        Param = self.env['ir.config_parameter'].sudo()
        rh_id = int(Param.get_param('quimibond_sgi.rh_user_id', '0') or 0)
        res['sgi_rh_user_id'] = rh_id if rh_id and self.env['res.users'].browse(rh_id).exists() else False
        raw_mast = Param.get_param('quimibond_sgi.mast_user_id', '') or ''
        mast_id = int(raw_mast) if raw_mast.isdigit() else 0
        res['sgi_mast_user_id'] = (
            mast_id if mast_id and self.env['res.users'].browse(mast_id).exists() else False)
        raw_critical = Param.get_param('quimibond_sgi.supplier_critical_categ_ids', '') or ''
        res['sgi_supplier_critical_categ_ids'] = [(6, 0, self.env['product.category'].browse(
            [int(x) for x in raw_critical.split(',') if x.strip().isdigit()]).exists().ids)]
        cat_id = int(Param.get_param('quimibond_sgi.purchase_approval_category_id', '0') or 0)
        res['sgi_purchase_approval_category_id'] = (
            cat_id if cat_id and self.env['approval.category'].browse(cat_id).exists()
            else False)
        energy_id = int(Param.get_param('quimibond_sgi.energy_partner_id', '0') or 0)
        res['sgi_energy_partner_id'] = (
            energy_id if energy_id and self.env['res.partner'].browse(energy_id).exists()
            else False)
        survey_id = int(Param.get_param('quimibond_sgi.satisfaction_survey_id', '0') or 0)
        res['sgi_satisfaction_survey_id'] = (
            survey_id if survey_id and self.env['survey.survey'].with_context(
                active_test=False).browse(survey_id).exists()
            else False)
        return res

    def set_values(self):
        super().set_values()
        Param = self.env['ir.config_parameter'].sudo()
        Param.set_param('quimibond_sgi.rh_user_id', self.sgi_rh_user_id.id or 0)
        Param.set_param('quimibond_sgi.mast_user_id', self.sgi_mast_user_id.id or 0)
        Param.set_param('quimibond_sgi.purchase_approval_category_id',
                        self.sgi_purchase_approval_category_id.id or 0)
        Param.set_param('quimibond_sgi.supplier_critical_categ_ids',
                        ",".join(str(i) for i in self.sgi_supplier_critical_categ_ids.ids))
        Param.set_param('quimibond_sgi.energy_partner_id',
                        self.sgi_energy_partner_id.id or 0)
        Param.set_param('quimibond_sgi.satisfaction_survey_id',
                        self.sgi_satisfaction_survey_id.id or 0)
