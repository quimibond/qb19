# -*- coding: utf-8 -*-
from dateutil.relativedelta import relativedelta

from odoo import models, fields, api
from odoo.exceptions import UserError

CALC_MODES = [
    ('manual', "Captura manual"),
    ('otif_ventas', "OTIF ventas (embarques a tiempo)"),
    ('otd_compras', "OTD compras (recepciones a tiempo)"),
    ('produccion_vs_programado', "Producido vs programado"),
    ('reproceso', "Reproceso"),
    ('desperdicio', "Desperdicio (subproducto SALDO TEJIDO D)"),
    ('desperdicio_scrap', "Desperdicio (por desechos / scrap)"),
    ('calidad_pq', "Calidad PQ (rollos revisados sin defecto)"),
    ('cumplimiento_programa', "Cumplimiento del programa (MPS)"),
    ('cierre_nc', "Cierre de No Conformidades"),
    ('reclamos_cliente', "Reclamos de cliente"),
    ('disponibilidad_mantto', "Disponibilidad de mantenimiento"),
    ('preventivo_cumplido', "Preventivo cumplido"),
    ('rotacion_rh', "Rotación de personal"),
    ('plantilla_rh', "Cobertura de plantilla"),
    ('presupuesto_ventas', "Cumplimiento de presupuesto de ventas"),
    ('crecimiento_ventas', "Crecimiento anual de ventas (vs año anterior)"),
    ('inventario_diferencia', "Diferencia de inventario físico vs sistema"),
    ('inventario_ciclico', "Diferencia de inventario cíclico (ajustes)"),
    ('ots_atendidas', "Órdenes de trabajo atendidas (mantenimiento)"),
    ('requisiciones', "Requisiciones atendidas (aprobaciones de compra)"),
    ('embarques_sin_error', "Embarques sin error (sin devolución de cliente)"),
    ('produccion_vs_capacidad', "Producido vs capacidad instalada"),
    ('consumo_energia', "Consumo de energía (facturado por el proveedor)"),
    ('compras_sin_devolucion', "Compras sin devolución a proveedor (proxy de errores en OC)"),
    ('capacitacion', "Capacitación (competencias vigentes vs requeridas)"),
    ('satisfaccion_cliente', "Satisfacción del cliente (encuesta)"),
]


class SgiIndicator(models.Model):
    _name = 'sgi.indicator'
    _description = "Indicador SGI (F-P-A10-03)"
    _inherit = ['mail.thread', 'mail.activity.mixin']
    _order = 'code'

    code = fields.Char(string="Clave", required=True, index=True)
    name = fields.Char(string="Nombre", required=True)
    process_id = fields.Many2one('sgi.process', string="Proceso")
    sgi_area_id = fields.Many2one('sgi.area', string="Área SGI")
    responsible_id = fields.Many2one('res.users', string="Responsable")
    objective_id = fields.Many2one('sgi.objective', string="Objetivo integral")
    uom = fields.Char(string="Unidad", help="% , MXN, unidades, kg, m…")
    direction = fields.Selection([
        ('higher_better', "Más alto es mejor"),
        ('lower_better', "Más bajo es mejor"),
    ], string="Sentido", default='higher_better', required=True)
    target_objective = fields.Float(string="Objetivo")
    target_acceptable = fields.Float(string="Aceptable")
    frequency = fields.Selection([
        ('monthly', "Mensual"),
        ('weekly', "Semanal"),
    ], string="Frecuencia", default='monthly', required=True)
    calc_mode = fields.Selection(CALC_MODES, string="Modo de cálculo",
                                 default='manual', required=True)
    source_type = fields.Selection([
        ('auto', "Automático"),
        ('manual', "Manual"),
    ], string="Origen del dato", compute='_compute_source', store=True)
    source_info = fields.Char(string="Fuente del dato", compute='_compute_source', store=True)

    # De dónde sale el valor de cada modo, en lenguaje humano (para el usuario).
    #
    # Quedan MANUALES a propósito (no hay fuente confiable en Odoo, así que
    # automatizarlos daría un dato falso):
    #   TR-02 Consumo de papel: no se captura la merma de papel por documento;
    #         requeriría un contador físico o una hoja de consumo inexistente.
    #   TR-04 Separación de residuos: la báscula de residuos por tipo no está en
    #         Odoo (se registra en bitácora física del centro de acopio).
    #   LO-02 Documentación de exportaciones: la completitud del pedimento/carta
    #         porte se audita a mano; no hay un campo que marque "expediente OK".
    #   TI-01 Disponibilidad de Odoo: el uptime lo mide Odoo.sh (externo), no la
    #         base; se captura desde el reporte del proveedor.
    _SOURCE_INFO = {
        'manual': "El responsable lo captura cada periodo (aún no sale de Odoo).",
        'otif_ventas': "Inventario → entregas a clientes: embarques a tiempo vs total.",
        'otd_compras': "Inventario → recepciones de compras: recibidas a tiempo vs total.",
        'produccion_vs_programado': "Fabricación → órdenes de producción: producido vs programado.",
        'reproceso': "Fabricación → órdenes de reproceso del periodo.",
        'desperdicio': "Fabricación → byproduct SALDO (categoría de desperdicio) vs producción.",
        'desperdicio_scrap': "Inventario → desechos (scrap) del periodo.",
        'calidad_pq': "Piso → revisado de telas: rollos sin defecto vs revisados.",
        'cumplimiento_programa': "Fabricación → cumplimiento del plan maestro (MPS).",
        'cierre_nc': "SGI → No Conformidades: cerradas a tiempo vs abiertas.",
        'reclamos_cliente': "Helpdesk → tickets de reclamación de clientes del periodo.",
        'disponibilidad_mantto': "Mantenimiento → tiempo de paro vs disponible.",
        'preventivo_cumplido': "Mantenimiento → OTs preventivas cumplidas a tiempo.",
        'rotacion_rh': "Empleados → bajas del periodo vs plantilla.",
        'plantilla_rh': "Empleados → puestos cubiertos vs plantilla autorizada.",
        'presupuesto_ventas': "Ventas → facturación real vs presupuesto de ventas APROBADO del periodo (importe en moneda de la compañía; nunca cantidades mezcladas). Sin presupuesto aprobado, cae al parámetro de Ajustes.",
        'crecimiento_ventas': "Contabilidad → facturación neta timbrada del periodo vs el mismo periodo del año anterior (variación %).",
        'inventario_diferencia': "Inventario → ajustes de inventario físico vs sistema.",
        'inventario_ciclico': "Inventario → ajustes de conteos cíclicos.",
        'ots_atendidas': "Mantenimiento → solicitudes cerradas (etapa terminada) en el periodo vs creadas en el periodo.",
        'requisiciones': "Aprobaciones → requisiciones de compra aprobadas en el periodo vs solicitadas.",
        'embarques_sin_error': "Inventario → embarques a clientes del periodo sin devolución ligada vs total.",
        'produccion_vs_capacidad': "Fabricación → producción real del periodo vs la capacidad instalada configurada en Ajustes (prorrateada por días si el periodo no es mensual).",
        'consumo_energia': "Contabilidad → total facturado del periodo por el proveedor de energía configurado en Ajustes.",
        'compras_sin_devolucion': "PROXY (a validar por MAST): órdenes de compra confirmadas del periodo sin devolución a proveedor vs total. No mide directamente los 'errores en OC'; MAST debe validar la definición antes de fiarse del dato.",
        'capacitacion': "Empleados → competencias del puesto vigentes (certificación al día) vs requeridas.",
        'satisfaccion_cliente': "Encuestas → respuestas de la Encuesta de Satisfacción del Cliente (promedio 1-5 → %).",
    }

    @api.depends('calc_mode')
    def _compute_source(self):
        for indicator in self:
            indicator.source_type = 'manual' if indicator.calc_mode == 'manual' else 'auto'
            indicator.source_info = self._SOURCE_INFO.get(
                indicator.calc_mode, "Cálculo automático desde datos de Odoo.")
    monthly_budget = fields.Float(string="Presupuesto mensual",
                                  help="Meta mensual para el cálculo de presupuesto de ventas.")
    nc_on_red = fields.Boolean(
        string="Generar NC en rojo", default=False,
        help="Actívese indicador por indicador cuando el dato ya se validó contra "
             "el Excel F-P-A10-03. Una medición roja validada creará una NC automática.")
    active = fields.Boolean(default=True)

    measure_ids = fields.One2many('sgi.indicator.measure', 'indicator_id', string="Mediciones")
    last_measure_id = fields.Many2one('sgi.indicator.measure', string="Última medición",
                                      compute='_compute_last_measure')
    last_value = fields.Float(string="Último valor", compute='_compute_last_measure')
    last_semaphore = fields.Selection([
        ('verde', "Verde"),
        ('amarillo', "Amarillo"),
        ('rojo', "Rojo"),
    ], string="Último semáforo", compute='_compute_last_measure')

    _code_uniq = models.Constraint(
        'unique(code)',
        "La clave de indicador debe ser única.",
    )

    @api.depends('measure_ids.period_date', 'measure_ids.value', 'measure_ids.semaphore')
    def _compute_last_measure(self):
        for indicator in self:
            last = indicator.measure_ids.sorted('period_date', reverse=True)[:1]
            indicator.last_measure_id = last.id
            indicator.last_value = last.value if last else 0.0
            indicator.last_semaphore = last.semaphore if last else False

    @api.depends('code', 'name')
    def _compute_display_name(self):
        for indicator in self:
            indicator.display_name = "%s - %s" % (indicator.code, indicator.name) \
                if indicator.code else indicator.name

    def action_view_trend(self):
        """La pregunta real de MAST frente a un KPI: ¿cómo viene la tendencia?
        Abre las mediciones del indicador en gráfica de línea por periodo."""
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'name': "Tendencia — %s" % (self.code or self.name),
            'res_model': 'sgi.indicator.measure',
            'view_mode': 'graph,list,form',
            'domain': [('indicator_id', '=', self.id)],
            'context': {'default_indicator_id': self.id},
        }

    # ------------------------------------------------------------------
    # Motor de cálculo automático
    # ------------------------------------------------------------------
    def _sgi_compute_value(self, date_from, date_to):
        """Devuelve el valor calculado del periodo o None (captura manual)."""
        self.ensure_one()
        if self.calc_mode == 'manual':
            return None
        method = getattr(self, '_calc_%s' % self.calc_mode, None)
        if not method:
            return None
        return method(date_from, date_to)

    def _sgi_period_bounds(self, period_date):
        """(desde, hasta-INCLUSIVO) del periodo — misma semántica que el cron."""
        self.ensure_one()
        if self.frequency == 'weekly':
            return period_date, period_date + relativedelta(days=6)
        return period_date, period_date + relativedelta(months=1, days=-1)

    def _sgi_dt_bounds(self, date_from, date_to):
        return (fields.Datetime.to_datetime(date_from),
                fields.Datetime.to_datetime(date_to) + relativedelta(days=1))

    def _calc_otif_ventas(self, date_from, date_to):
        dt_from, dt_to = self._sgi_dt_bounds(date_from, date_to)
        pickings = self.env['stock.picking'].search([
            ('picking_type_id.code', '=', 'outgoing'),
            ('state', '=', 'done'),
            ('date_done', '>=', dt_from), ('date_done', '<', dt_to),
        ])
        if not pickings:
            return None
        on_time = 0
        for pick in pickings:
            deadline = pick.date_deadline or pick.scheduled_date
            if deadline and pick.date_done and pick.date_done <= deadline:
                on_time += 1
        return round(on_time / len(pickings) * 100.0, 2)

    def _calc_otd_compras(self, date_from, date_to):
        dt_from, dt_to = self._sgi_dt_bounds(date_from, date_to)
        pickings = self.env['stock.picking'].search([
            ('picking_type_id.code', '=', 'incoming'),
            ('state', '=', 'done'),
            ('date_done', '>=', dt_from), ('date_done', '<', dt_to),
        ])
        if not pickings:
            return None
        on_time = 0
        for pick in pickings:
            po = pick.purchase_id if 'purchase_id' in pick._fields else False
            deadline = (po and po.date_planned) or pick.date_deadline or pick.scheduled_date
            if deadline and pick.date_done and pick.date_done <= deadline:
                on_time += 1
        return round(on_time / len(pickings) * 100.0, 2)

    def _sgi_production_done(self, date_from, date_to):
        dt_from, dt_to = self._sgi_dt_bounds(date_from, date_to)
        return self.env['mrp.production'].search([
            ('state', '=', 'done'),
            ('date_finished', '>=', dt_from), ('date_finished', '<', dt_to),
        ])

    def _calc_produccion_vs_programado(self, date_from, date_to):
        productions = self._sgi_production_done(date_from, date_to)
        programmed = sum(productions.mapped('product_qty'))
        if not programmed:
            return None
        produced = sum(productions.mapped('qty_produced'))
        return round(produced / programmed * 100.0, 2)

    def _sgi_waste_category_ids(self):
        """Categoría de subproducto de desperdicio (SALDO TEJIDO D) y sus hijas."""
        name = self.env['ir.config_parameter'].sudo().get_param(
            'quimibond_sgi.waste_subproduct_category', 'SubProducto')
        categ = self.env['product.category'].search([('name', '=', name)], limit=1)
        if not categ:
            return self.env['product.category']
        return self.env['product.category'].search([('id', 'child_of', categ.id)])

    def _calc_desperdicio(self, date_from, date_to):
        """Desperdicio real = kilos producidos del subproducto SALDO TEJIDO D
        (categoría SubProducto) sobre los kilos producidos del periodo."""
        productions = self._sgi_production_done(date_from, date_to)
        produced = sum(productions.mapped('qty_produced'))
        if not produced:
            return None
        categ_ids = self._sgi_waste_category_ids()
        if not categ_ids:
            return None
        waste_moves = productions.move_byproduct_ids.filtered(
            lambda m: m.state == 'done' and m.product_id.categ_id.id in categ_ids.ids)
        waste = sum(waste_moves.mapped('quantity'))
        return round(waste / produced * 100.0, 2)

    def _calc_desperdicio_scrap(self, date_from, date_to):
        """Cálculo histórico por desechos (stock.scrap), conservado como modo aparte."""
        dt_from, dt_to = self._sgi_dt_bounds(date_from, date_to)
        scraps = self.env['stock.scrap'].search([
            ('state', '=', 'done'),
            ('date_done', '>=', dt_from), ('date_done', '<', dt_to),
        ])
        scrap_qty = sum(scraps.mapped('scrap_qty'))
        produced = sum(self._sgi_production_done(date_from, date_to).mapped('qty_produced'))
        if not produced:
            return None
        return round(scrap_qty / produced * 100.0, 2)

    def _calc_calidad_pq(self, date_from, date_to):
        """% de rollos revisados SIN defecto, según el registro de revisado de
        tela (mrp.revision.log): un defecto se marca con una causa (etiqueta
        TEJIDO-*). Si el módulo de revisado no está instalado, devuelve None."""
        if 'mrp.revision.log' not in self.env:
            return None
        dt_from, dt_to = self._sgi_dt_bounds(date_from, date_to)
        logs = self.env['mrp.revision.log'].search([
            ('create_date', '>=', dt_from), ('create_date', '<', dt_to),
        ])
        total = len(logs)
        if not total:
            return None
        con_defecto = len(logs.filtered(lambda l: l.causa_id))
        return round((total - con_defecto) / total * 100.0, 2)

    def _calc_cumplimiento_programa(self, date_from, date_to):
        """Cumplimiento del programa (MPS semanal): kilos producidos vs kilos
        planificados de las órdenes cuyo inicio programado cae en el periodo."""
        dt_from, dt_to = self._sgi_dt_bounds(date_from, date_to)
        scheduled = self.env['mrp.production'].search([
            ('state', '!=', 'cancel'),
            ('date_start', '>=', dt_from), ('date_start', '<', dt_to),
        ])
        planned = sum(scheduled.mapped('product_qty'))
        if not planned:
            return None
        done_qty = sum(scheduled.filtered(lambda m: m.state == 'done').mapped('qty_produced'))
        return round(done_qty / planned * 100.0, 2)

    def _calc_reproceso(self, date_from, date_to):
        # Sin fuente confiable todavía (ver README). Captura manual.
        return None

    def _calc_cierre_nc(self, date_from, date_to):
        dt_from, dt_to = self._sgi_dt_bounds(date_from, date_to)
        Alert = self.env['quality.alert']
        detected = Alert.search_count([
            ('sgi_folio', '!=', False),
            ('create_date', '>=', dt_from), ('create_date', '<', dt_to),
        ])
        if not detected:
            return None
        closed = Alert.search_count([
            ('sgi_folio', '!=', False),
            ('date_close', '>=', dt_from), ('date_close', '<', dt_to),
        ])
        return round(closed / detected * 100.0, 2)

    def _calc_reclamos_cliente(self, date_from, date_to):
        dt_from, dt_to = self._sgi_dt_bounds(date_from, date_to)
        team = self.env.ref('quimibond_sgi.sgi_helpdesk_team_complaints',
                            raise_if_not_found=False)
        if not team:
            return None
        return float(self.env['helpdesk.ticket'].search_count([
            ('team_id', '=', team.id),
            ('create_date', '>=', dt_from), ('create_date', '<', dt_to),
        ]))

    def _calc_disponibilidad_mantto(self, date_from, date_to):
        # Requiere paros de centros de trabajo; sin datos confiables aún (README).
        return None

    def _calc_preventivo_cumplido(self, date_from, date_to):
        Request = self.env['maintenance.request']
        requests = Request.search([
            ('maintenance_type', '=', 'preventive'),
            ('request_date', '>=', date_from), ('request_date', '<=', date_to),
        ])
        if not requests:
            return None
        done = requests.filtered(lambda r: r.stage_id.done)
        return round(len(done) / len(requests) * 100.0, 2)

    def _calc_rotacion_rh(self, date_from, date_to):
        Employee = self.env['hr.employee'].with_context(active_test=False)
        headcount = self.env['hr.employee'].search_count([])
        if not headcount:
            return None
        departures = Employee.search_count([
            ('departure_date', '>=', date_from), ('departure_date', '<=', date_to),
        ])
        return round(departures / headcount * 100.0, 2)

    def _calc_plantilla_rh(self, date_from, date_to):
        # Requiere plantilla presupuestada por puesto; captura manual (README).
        return None

    def _sgi_year_budget_amount(self, date_from, date_to):
        """Importe presupuestado del periodo: suma de amount_budget de las líneas
        de presupuesto APROBADO (todos los equipos/mercados) cuyo mes cae en el
        periodo. Prorrateo mensual = las líneas de ese mes. Siempre en importe y
        moneda de la compañía (nunca cantidades mezcladas)."""
        lines = self.env['sgi.sales.budget.line'].sudo().search([
            ('budget_id.kind', '=', 'presupuesto'),
            ('budget_id.state', '=', 'aprobado'),
            ('date', '>=', date_from), ('date', '<=', date_to),
        ])
        return sum(lines.mapped('amount_budget'))

    def _calc_presupuesto_ventas(self, date_from, date_to):
        """Cumplimiento SIEMPRE sobre importe en moneda de la compañía: facturación
        neta del periodo / presupuesto aprobado del periodo. Si no hay presupuesto
        aprobado, cae al parámetro de Ajustes (con nota)."""
        budget = self._sgi_year_budget_amount(date_from, date_to)
        if not budget:
            budget = self.monthly_budget or float(
                self.env['ir.config_parameter'].sudo().get_param(
                    'quimibond_sgi.monthly_sales_budget', 0) or 0)
        if not budget:
            return None
        invoiced = self._sgi_net_invoiced(date_from, date_to)
        return round(invoiced / budget * 100.0, 2)

    def _note_presupuesto_ventas(self, date_from, date_to):
        if not self._sgi_year_budget_amount(date_from, date_to):
            return ("Sin presupuesto de ventas aprobado del periodo: se usó el "
                    "presupuesto configurado en Ajustes.")
        return ''

    def _sgi_net_invoiced(self, date_from, date_to):
        """Facturación neta timbrada del periodo: ventas timbradas (out_invoice)
        menos notas de crédito (out_refund), sin impuestos. amount_untaxed_signed
        ya trae las notas de crédito en negativo, así que la suma es neta."""
        moves = self.env['account.move'].search([
            ('move_type', 'in', ('out_invoice', 'out_refund')),
            ('state', '=', 'posted'),
            ('invoice_date', '>=', date_from), ('invoice_date', '<=', date_to),
        ])
        return sum(moves.mapped('amount_untaxed_signed'))

    def _calc_crecimiento_ventas(self, date_from, date_to):
        """Variación % de la facturación neta del periodo contra el mismo periodo
        del año anterior. None si no hay base comparable (año anterior en cero)."""
        current = self._sgi_net_invoiced(date_from, date_to)
        prev = self._sgi_net_invoiced(
            date_from - relativedelta(years=1), date_to - relativedelta(years=1))
        if not prev:
            return None
        return round((current - prev) / prev * 100.0, 2)

    def _calc_ots_atendidas(self, date_from, date_to):
        """OTs de mantenimiento atendidas: solicitudes cerradas (etapa terminada)
        en el periodo sobre las creadas en el periodo. request_date y close_date
        son Date, así que la comparación es inclusiva sin datetime."""
        Request = self.env['maintenance.request']
        created = Request.search_count([
            ('request_date', '>=', date_from), ('request_date', '<=', date_to),
        ])
        if not created:
            return None
        closed = Request.search_count([
            ('stage_id.done', '=', True),
            ('close_date', '>=', date_from), ('close_date', '<=', date_to),
        ])
        return round(closed / created * 100.0, 2)

    def _sgi_purchase_approval_categories(self):
        """Categoría(s) de aprobación de compras (approvals_purchase marca la
        categoría con approval_type='purchase'). Si MAST configuró una categoría
        específica en Ajustes (por ambigüedad), se usa esa."""
        Category = self.env['approval.category']
        param = self.env['ir.config_parameter'].sudo().get_param(
            'quimibond_sgi.purchase_approval_category_id')
        if param:
            cat = Category.browse(int(param)).exists()
            if cat:
                return cat
        if 'approval_type' in Category._fields:
            return Category.search([('approval_type', '=', 'purchase')])
        return Category.browse()

    def _calc_requisiciones(self, date_from, date_to):
        """Requisiciones de compra atendidas: aprobadas sobre solicitadas del
        periodo. El módulo de aprobaciones no guarda fecha de aprobación, así que
        el cohorte es 'solicitadas en el periodo' (create_date) y de ellas se mide
        la fracción con request_status='approved' — mide '% atendidas'."""
        categories = self._sgi_purchase_approval_categories()
        if not categories:
            return None
        dt_from, dt_to = self._sgi_dt_bounds(date_from, date_to)
        requested = self.env['approval.request'].search([
            ('category_id', 'in', categories.ids),
            ('create_date', '>=', dt_from), ('create_date', '<', dt_to),
        ])
        if not requested:
            return None
        approved = requested.filtered(lambda r: r.request_status == 'approved')
        return round(len(approved) / len(requested) * 100.0, 2)

    def _sgi_outgoing_done(self, date_from, date_to):
        dt_from, dt_to = self._sgi_dt_bounds(date_from, date_to)
        return self.env['stock.picking'].search([
            ('picking_type_id.code', '=', 'outgoing'),
            ('state', '=', 'done'),
            ('date_done', '>=', dt_from), ('date_done', '<', dt_to),
        ])

    def _sgi_shipments_with_return(self, pickings):
        """Embarques con devolución de cliente ligada: la devolución de una entrega
        es un movimiento de retorno (returned_move_ids) creado desde sus movimientos."""
        return pickings.filtered(lambda p: p.move_ids.returned_move_ids)

    def _calc_embarques_sin_error(self, date_from, date_to):
        """% de embarques a clientes del periodo SIN devolución ligada."""
        pickings = self._sgi_outgoing_done(date_from, date_to)
        total = len(pickings)
        if not total:
            return None
        with_error = len(self._sgi_shipments_with_return(pickings))
        return round((total - with_error) / total * 100.0, 2)

    # ----- Paso 2: modos con parámetro + proxy -----------------------------
    def _calc_produccion_vs_capacidad(self, date_from, date_to):
        """Producción real del periodo (misma base que produccion_vs_programado)
        sobre la capacidad instalada configurada en Ajustes. La capacidad es
        mensual; para periodos no mensuales se prorratea por los días del periodo.
        Sin capacidad configurada → None (se captura manual, patrón presupuesto)."""
        capacity = float(self.env['ir.config_parameter'].sudo().get_param(
            'quimibond_sgi.production_monthly_capacity', 0) or 0)
        if not capacity:
            return None
        produced = sum(self._sgi_production_done(date_from, date_to).mapped('qty_produced'))
        if self.frequency == 'monthly':
            cap = capacity
        else:
            month_start = date_from.replace(day=1)
            days_in_month = ((month_start + relativedelta(months=1)) - month_start).days
            period_days = (date_to - date_from).days + 1  # fin inclusivo
            cap = capacity * period_days / days_in_month
        if not cap:
            return None
        return round(produced / cap * 100.0, 2)

    def _sgi_energy_partner(self):
        """Proveedor de energía configurado en Ajustes (m2o), o vacío."""
        param = self.env['ir.config_parameter'].sudo().get_param(
            'quimibond_sgi.energy_partner_id')
        if not param or not int(param or 0):
            return self.env['res.partner']
        return self.env['res.partner'].browse(int(param)).exists()

    def _calc_consumo_energia(self, date_from, date_to):
        """Total facturado del periodo por el proveedor de energía (facturas de
        proveedor menos notas de crédito, sin impuestos). Sin proveedor
        configurado devuelve None: la medición queda PENDIENTE con la nota que
        pide configurarlo (un 0 "capturado" pintaría verde un KPI lower_better
        sin haber medido nada)."""
        partner = self._sgi_energy_partner()
        if not partner:
            return None
        moves = self.env['account.move'].search([
            ('move_type', 'in', ('in_invoice', 'in_refund')),
            ('state', '=', 'posted'),
            ('partner_id', 'child_of', partner.id),
            ('invoice_date', '>=', date_from), ('invoice_date', '<=', date_to),
        ])
        total = 0.0
        for move in moves:
            total += move.amount_untaxed if move.move_type == 'in_invoice' \
                else -move.amount_untaxed
        return round(total, 2)

    def _note_consumo_energia(self, date_from, date_to):
        if not self._sgi_energy_partner():
            return ("Configure el proveedor de energía en Ajustes para medir este "
                    "indicador automáticamente.")
        return ''

    def _calc_compras_sin_devolucion(self, date_from, date_to):
        """PROXY (a validar por MAST): órdenes de compra confirmadas del periodo
        sin devolución a proveedor sobre el total. La devolución a proveedor es un
        movimiento de retorno ligado a las recepciones de la OC."""
        dt_from, dt_to = self._sgi_dt_bounds(date_from, date_to)
        orders = self.env['purchase.order'].search([
            ('state', 'in', ('purchase', 'done')),
            ('date_approve', '>=', dt_from), ('date_approve', '<', dt_to),
        ])
        total = len(orders)
        if not total:
            return None
        with_return = orders.filtered(
            lambda o: o.picking_ids.move_ids.returned_move_ids)
        return round((total - len(with_return)) / total * 100.0, 2)

    def _calc_capacitacion(self, date_from, date_to):
        """% de competencias del puesto VIGENTES (certificación al día) vs las
        requeridas, a través de la vista de brechas (sgi.competence.gap): una
        competencia caducada (valid_to vencido) cuenta como brecha. Es una foto
        del estado actual, no acumula por periodo; las cotas del periodo no aplican
        (competencia = vigencia a hoy)."""
        Employee = self.env['hr.employee']
        JobSkill = self.env['hr.job.skill']
        employees = Employee.search([])
        required = 0
        for employee in employees:
            if employee.job_id:
                required += JobSkill.search_count([('job_id', '=', employee.job_id.id)])
        if not required:
            return None
        gaps = self.env['sgi.competence.gap'].search_count(
            [('employee_id', 'in', employees.ids)])
        return round((required - gaps) / required * 100.0, 2)

    def _sgi_satisfaction_survey(self):
        return self.env.ref('quimibond_sgi.sgi_survey_satisfaction',
                            raise_if_not_found=False)

    def _calc_satisfaccion_cliente(self, date_from, date_to):
        """Promedio de las respuestas 1-5 de la Encuesta de Satisfacción del
        Cliente contestadas en el periodo, convertido a %. Sin respuestas (o
        sin encuesta instalada) → None: la medición queda pendiente."""
        survey = self._sgi_satisfaction_survey()
        if not survey:
            return None
        dt_from, dt_to = self._sgi_dt_bounds(date_from, date_to)
        inputs = self.env['survey.user_input'].sudo().search([
            ('survey_id', '=', survey.id), ('state', '=', 'done'),
            ('create_date', '>=', dt_from), ('create_date', '<', dt_to),
        ])
        if not inputs:
            return None
        lines = self.env['survey.user_input.line'].sudo().search([
            ('user_input_id', 'in', inputs.ids),
            ('answer_type', '=', 'suggestion'),
        ])
        values = [float(line.suggested_answer_id.value)
                  for line in lines
                  if (line.suggested_answer_id.value or '').strip().isdigit()]
        if not values:
            return None
        return round(sum(values) / len(values) / 5.0 * 100.0, 2)

    def _note_satisfaccion_cliente(self, date_from, date_to):
        if not self._sgi_satisfaction_survey():
            return ("La Encuesta de Satisfacción del Cliente no está instalada: "
                    "reinstale los datos del módulo o capture el valor a mano.")
        return ''

    def _sgi_compute_note(self, date_from, date_to):
        """Nota opcional que acompaña la medición generada por el cron (p.ej.
        avisar de una configuración faltante). '' salvo que el modo la provea."""
        self.ensure_one()
        if self.calc_mode == 'manual':
            return ''
        method = getattr(self, '_note_%s' % self.calc_mode, None)
        return method(date_from, date_to) if method else ''

    def _calc_inventario_diferencia(self, date_from, date_to):
        # Requiere conteos físicos registrados; captura manual (README).
        return None

    def _calc_inventario_ciclico(self, date_from, date_to):
        """Diferencia de inventario cíclico: |cantidad ajustada| en el periodo
        (movimientos de ajuste de inventario) sobre las existencias contadas.
        Requiere conteos cíclicos activos; ver README."""
        dt_from, dt_to = self._sgi_dt_bounds(date_from, date_to)
        adj_lines = self.env['stock.move.line'].search([
            ('move_id.is_inventory', '=', True),
            ('state', '=', 'done'),
            ('date', '>=', dt_from), ('date', '<', dt_to),
        ])
        if not adj_lines:
            return None
        adjusted = sum(abs(line.quantity) for line in adj_lines)
        # Existencias contadas: proxy = existencias actuales en ubicaciones internas.
        quants = self.env['stock.quant'].search([('location_id.usage', '=', 'internal')])
        on_hand = sum(quants.mapped('quantity'))
        if not on_hand:
            return None
        return round(adjusted / on_hand * 100.0, 2)


class SgiIndicatorMeasure(models.Model):
    _name = 'sgi.indicator.measure'
    _description = "Medición de indicador SGI"
    _order = 'period_date desc, indicator_id'

    indicator_id = fields.Many2one('sgi.indicator', string="Indicador",
                                   required=True, ondelete='cascade', index=True)
    source_type = fields.Selection(related='indicator_id.source_type',
                                   string="Origen del dato")
    source_info = fields.Char(related='indicator_id.source_info',
                              string="Fuente del dato")
    period_date = fields.Date(string="Periodo", required=True,
                              help="Día 1 del mes medido.")
    value = fields.Float(string="Valor")
    direction = fields.Selection(related='indicator_id.direction')
    target_objective = fields.Float(related='indicator_id.target_objective', string="Objetivo")
    target_acceptable = fields.Float(related='indicator_id.target_acceptable', string="Aceptable")
    uom = fields.Char(related='indicator_id.uom', string="Unidad")
    semaphore = fields.Selection([
        ('verde', "Verde"),
        ('amarillo', "Amarillo"),
        ('rojo', "Rojo"),
    ], string="Semáforo", compute='_compute_semaphore', store=True)
    note = fields.Text(string="Nota")
    state = fields.Selection([
        ('pendiente', "Pendiente"),
        ('capturado', "Capturado"),
        ('validado', "Validado"),
    ], string="Estado", default='pendiente', required=True)
    alert_id = fields.Many2one('quality.alert', string="No Conformidad", readonly=True)
    sgi_nc_suppressed = fields.Boolean(
        string="NC omitida (fuente apagada)", readonly=True, copy=False,
        help="La medición ameritaba NC pero la fuente «Indicador en semáforo rojo» "
             "estaba desactivada. Se reintenta sola en cuanto se reactive.")

    _indicator_period_uniq = models.Constraint(
        'unique(indicator_id, period_date)',
        "Ya existe una medición para este indicador y periodo.",
    )

    @api.depends('value', 'state', 'indicator_id.direction',
                 'indicator_id.target_objective', 'indicator_id.target_acceptable')
    def _compute_semaphore(self):
        for measure in self:
            # Una medición aún pendiente (sin dato capturado) no tiene semáforo:
            # evita mostrar rojo con value=0 antes de la captura.
            if measure.state == 'pendiente':
                measure.semaphore = False
                continue
            indicator = measure.indicator_id
            obj = indicator.target_objective
            acc = indicator.target_acceptable
            val = measure.value
            if indicator.direction == 'lower_better':
                if val <= obj:
                    measure.semaphore = 'verde'
                elif val <= acc:
                    measure.semaphore = 'amarillo'
                else:
                    measure.semaphore = 'rojo'
            else:
                if val >= obj:
                    measure.semaphore = 'verde'
                elif val >= acc:
                    measure.semaphore = 'amarillo'
                else:
                    measure.semaphore = 'rojo'

    @api.depends('indicator_id', 'period_date')
    def _compute_display_name(self):
        for measure in self:
            period = measure.period_date and measure.period_date.strftime('%m/%Y') or ''
            measure.display_name = "%s — %s" % (measure.indicator_id.code or '', period)

    # Evidencia por modo: (modelo, dominio base, campo fecha, es_datetime).
    # Mismo universo de registros que usa el cálculo correspondiente.
    _EVIDENCE = {
        'otif_ventas': ('stock.picking', [('picking_type_id.code', '=', 'outgoing'), ('state', '=', 'done')], 'date_done', True),
        'otd_compras': ('stock.picking', [('picking_type_id.code', '=', 'incoming'), ('state', '=', 'done')], 'date_done', True),
        'produccion_vs_programado': ('mrp.production', [('state', '=', 'done')], 'date_finished', True),
        'reproceso': ('mrp.production', [('state', '=', 'done')], 'date_finished', True),
        'desperdicio': ('mrp.production', [('state', '=', 'done')], 'date_finished', True),
        'desperdicio_scrap': ('stock.scrap', [('state', '=', 'done')], 'date_done', True),
        'calidad_pq': ('mrp.revision.log', [], 'create_date', True),
        'cumplimiento_programa': ('mrp.production', [('state', '!=', 'cancel')], 'date_finished', True),
        'cierre_nc': ('quality.alert', [], 'create_date', True),
        'disponibilidad_mantto': ('maintenance.request', [], 'create_date', True),
        'preventivo_cumplido': ('maintenance.request', [('maintenance_type', '=', 'preventive')], 'create_date', True),
        'crecimiento_ventas': ('account.move', [('move_type', 'in', ('out_invoice', 'out_refund')), ('state', '=', 'posted')], 'invoice_date', False),
        'inventario_diferencia': ('stock.move.line', [('state', '=', 'done'), ('move_id.is_inventory', '=', True)], 'date', True),
        'inventario_ciclico': ('stock.move.line', [('state', '=', 'done'), ('move_id.is_inventory', '=', True)], 'date', True),
        'ots_atendidas': ('maintenance.request', [], 'request_date', False),
        'produccion_vs_capacidad': ('mrp.production', [('state', '=', 'done')], 'date_finished', True),
        # requisiciones, embarques_sin_error, consumo_energia,
        # compras_sin_devolucion y capacitacion no caben en un dominio de fecha
        # simple (categoría/proveedor dinámicos, relación de devolución, o foto de
        # vigencia): su evidencia se resuelve en ramas propias de
        # action_view_evidence.
    }

    def action_view_evidence(self):
        """Abre los registros reales del periodo que explican el valor."""
        self.ensure_one()
        indicator = self.indicator_id
        mode = indicator.calc_mode
        date_from, date_to = indicator._sgi_period_bounds(self.period_date)
        if mode == 'manual':
            raise UserError(
                "Este indicador es de captura manual: la evidencia la aporta el "
                "responsable (%s). Cuando se automatice, aquí verás los registros."
                % (indicator.responsible_id.name or 'sin asignar'))
        if mode == 'embarques_sin_error':
            # Evidencia = los embarques CON devolución (los errores): más útil que
            # ver los buenos. Respeta las cotas del periodo vía _sgi_outgoing_done.
            pickings = indicator._sgi_outgoing_done(date_from, date_to)
            errors = indicator._sgi_shipments_with_return(pickings)
            return {
                'type': 'ir.actions.act_window',
                'name': "Embarques con devolución — evidencia de %s" % self.period_date,
                'res_model': 'stock.picking',
                'view_mode': 'list,form',
                'domain': [('id', 'in', errors.ids)],
            }
        if mode == 'compras_sin_devolucion':
            # Evidencia = las OCs CON devolución a proveedor (los errores del proxy).
            dt_from, dt_to = indicator._sgi_dt_bounds(date_from, date_to)
            orders = self.env['purchase.order'].search([
                ('state', 'in', ('purchase', 'done')),
                ('date_approve', '>=', dt_from), ('date_approve', '<', dt_to),
            ])
            errors = orders.filtered(
                lambda o: o.picking_ids.move_ids.returned_move_ids)
            return {
                'type': 'ir.actions.act_window',
                'name': "OCs con devolución — evidencia de %s" % self.period_date,
                'res_model': 'purchase.order',
                'view_mode': 'list,form',
                'domain': [('id', 'in', errors.ids)],
            }
        if mode == 'presupuesto_ventas':
            # Evidencia = las líneas del presupuesto aprobado del periodo.
            return {
                'type': 'ir.actions.act_window',
                'name': "Presupuesto del periodo — evidencia de %s" % self.period_date,
                'res_model': 'sgi.sales.budget.line',
                'view_mode': 'list,form',
                'domain': [('budget_id.kind', '=', 'presupuesto'),
                           ('budget_id.state', '=', 'aprobado'),
                           ('date', '>=', date_from), ('date', '<=', date_to)],
            }
        if mode == 'satisfaccion_cliente':
            survey = indicator._sgi_satisfaction_survey()
            dt_from, dt_to = indicator._sgi_dt_bounds(date_from, date_to)
            return {
                'type': 'ir.actions.act_window',
                'name': "Respuestas de satisfacción — evidencia de %s" % indicator.name,
                'res_model': 'survey.user_input',
                'view_mode': 'list,form',
                'domain': [('survey_id', '=', survey.id if survey else False),
                           ('state', '=', 'done'),
                           ('create_date', '>=', dt_from), ('create_date', '<', dt_to)],
            }
        if mode == 'capacitacion':
            # Evidencia = las brechas de competencia (foto a hoy; sin cota de periodo).
            employees = self.env['hr.employee'].search([])
            return {
                'type': 'ir.actions.act_window',
                'name': "Brechas de competencia — evidencia",
                'res_model': 'sgi.competence.gap',
                'view_mode': 'list,pivot',
                'domain': [('employee_id', 'in', employees.ids)],
                'context': {'search_default_group_skill_type': 1},
            }
        if mode == 'consumo_energia':
            partner = indicator._sgi_energy_partner()
            domain = [('move_type', 'in', ('in_invoice', 'in_refund')),
                      ('state', '=', 'posted')]
            domain += [('partner_id', 'child_of', partner.id)] if partner \
                else [('id', '=', False)]
            model, date_field, is_dt = 'account.move', 'invoice_date', False
        elif mode == 'requisiciones':
            categories = indicator._sgi_purchase_approval_categories()
            domain = [('category_id', 'in', categories.ids)] if categories else [('id', '=', False)]
            model, date_field, is_dt = 'approval.request', 'create_date', True
        elif mode == 'reclamos_cliente':
            team = self.env.ref('quimibond_sgi.sgi_helpdesk_team_complaints',
                                raise_if_not_found=False)
            domain = [('team_id', '=', team.id)] if team else []
            model, date_field, is_dt = 'helpdesk.ticket', 'create_date', True
        elif mode == 'rotacion_rh':
            return {
                'type': 'ir.actions.act_window',
                'name': "Bajas del periodo — evidencia",
                'res_model': 'hr.employee',
                'view_mode': 'list,form',
                'domain': [('active', '=', False),
                           ('departure_date', '>=', date_from),
                           ('departure_date', '<=', date_to)],
                'context': {'active_test': False},
            }
        elif mode == 'plantilla_rh':
            return {
                'type': 'ir.actions.act_window',
                'name': "Plantilla — evidencia",
                'res_model': 'hr.job',
                'view_mode': 'list,form',
                'domain': [],
            }
        elif mode in self._EVIDENCE:
            model, domain, date_field, is_dt = self._EVIDENCE[mode]
            domain = list(domain)
        else:
            raise UserError("Este modo de cálculo aún no tiene vista de evidencia.")
        if is_dt:
            dt_from, dt_to = indicator._sgi_dt_bounds(date_from, date_to)
            domain += [(date_field, '>=', dt_from), (date_field, '<', dt_to)]
        else:
            domain += [(date_field, '>=', date_from), (date_field, '<=', date_to)]
        return {
            'type': 'ir.actions.act_window',
            'name': "%s — evidencia de %s" % (indicator.name, self.period_date),
            'res_model': model,
            'view_mode': 'list,form',
            'domain': domain,
        }

    # Una medición VALIDADA es evidencia: ni su valor NI su estado se tocan sin
    # privilegio. El estado forma parte del candado: sin él, bastaba regresarla
    # a 'pendiente' (write de state) para editar el valor ya des-validado.
    _SGI_LOCKED_FIELDS = {'value', 'period_date', 'indicator_id', 'state'}

    def write(self, vals):
        if self._SGI_LOCKED_FIELDS & set(vals.keys()) and not self.env.su:
            locked = self.filtered(lambda m: m.state == 'validado')
            # Re-escribir 'validado' sobre una ya validada no reabre nada.
            if set(vals.keys()) == {'state'} and vals.get('state') == 'validado':
                locked = self.browse()
            if locked and not self.env.user.has_group('quimibond_sgi.group_sgi_manager'):
                raise UserError(
                    "La medición validada de %s es evidencia del SGI y no puede "
                    "modificarse ni regresarse a borrador. Pide al Jefe de MAST "
                    "reabrirla si hay un error real." % ', '.join(
                        locked.mapped('indicator_id.name')))
        return super().write(vals)

    def action_capture(self):
        self.write({'state': 'capturado'})

    def action_validate(self):
        self._sgi_check_validate_access()
        self.filtered(lambda m: m.state != 'validado').write({'state': 'validado'})
        self._sgi_maybe_create_nc()

    def action_reset(self):
        # El write bloquea la reapertura de validadas para quien no sea MAST.
        self.write({'state': 'pendiente'})

    def _sgi_check_validate_access(self):
        """Solo el responsable del indicador o el Jefe MAST valida la medición."""
        if self.env.user.has_group('quimibond_sgi.group_sgi_manager'):
            return
        for measure in self:
            responsible = measure.indicator_id.responsible_id
            if not responsible or responsible != self.env.user:
                raise UserError(
                    "Solo el responsable del indicador %s o el Jefe MAST y SGI "
                    "puede validar su medición." % measure.indicator_id.code)

    def _sgi_maybe_create_nc(self):
        """Crea la NC de un indicador rojo validado (idempotente)."""
        Alert = self.env['quality.alert']
        team = self.env.ref('quimibond_sgi.sgi_quality_team_internal', raise_if_not_found=False)
        for measure in self:
            indicator = measure.indicator_id
            if measure.state != 'validado' or measure.semaphore != 'rojo':
                continue
            if not indicator.nc_on_red or measure.alert_id:
                continue
            existing = Alert.search([('sgi_indicator_measure_id', '=', measure.id)], limit=1)
            if existing:
                measure.alert_id = existing.id
                continue
            mes = measure.period_date.strftime('%m/%Y') if measure.period_date else ''
            deviation = (
                "Indicador %s %s: valor %s vs objetivo %s en %s." % (
                    indicator.code, indicator.name,
                    measure.value, indicator.target_objective, mes))
            vals = {
                'title': "Indicador incumplido: %s" % indicator.code,
                'sgi_origin_type': 'indicador',
                'sgi_process_id': indicator.process_id.id,
                'sgi_deviation': deviation,
                'sgi_indicator_measure_id': measure.id,
            }
            if team:
                vals['team_id'] = team.id
            if indicator.responsible_id:
                vals['user_id'] = indicator.responsible_id.id
                vals['sgi_responsible_ids'] = [(4, indicator.responsible_id.id)]
            # El interruptor global de la fuente y el `nc_on_red` de cada
            # indicador se componen: basta apagar cualquiera de los dos.
            #
            # `count_suppression` evita inflar el contador: el cron vuelve a
            # evaluar esta misma medición en cada corrida, y una omisión ya
            # contabilizada no es un evento nuevo. Se reintenta igual, así que
            # al reactivar la fuente la NC se genera sola.
            alert = Alert.sgi_auto_create(
                'indicador_semaforo_rojo', vals,
                count_suppression=not measure.sgi_nc_suppressed)
            if not alert:
                measure.sgi_nc_suppressed = True
                continue
            measure.alert_id = alert.id
            if measure.sgi_nc_suppressed:
                measure.sgi_nc_suppressed = False
        return True


class QualityAlert(models.Model):
    _inherit = 'quality.alert'

    sgi_indicator_measure_id = fields.Many2one(
        'sgi.indicator.measure', string="Medición de indicador", readonly=True, copy=False)
