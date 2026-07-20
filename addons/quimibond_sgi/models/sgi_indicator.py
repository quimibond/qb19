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
    ('inventario_diferencia', "Diferencia de inventario físico vs sistema"),
    ('inventario_ciclico', "Diferencia de inventario cíclico (ajustes)"),
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
        'presupuesto_ventas': "Ventas → facturación real vs presupuesto configurado en Ajustes.",
        'inventario_diferencia': "Inventario → ajustes de inventario físico vs sistema.",
        'inventario_ciclico': "Inventario → ajustes de conteos cíclicos.",
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

    def _calc_presupuesto_ventas(self, date_from, date_to):
        budget = self.monthly_budget
        if not budget:
            param = self.env['ir.config_parameter'].sudo().get_param(
                'quimibond_sgi.monthly_sales_budget', 0)
            budget = float(param or 0)
        if not budget:
            return None
        moves = self.env['account.move'].search([
            ('move_type', 'in', ('out_invoice', 'out_refund')),
            ('state', '=', 'posted'),
            ('invoice_date', '>=', date_from), ('invoice_date', '<=', date_to),
        ])
        # amount_untaxed_signed ya trae las notas de crédito (out_refund) en negativo,
        # por lo que la suma es la facturación neta del periodo.
        invoiced = sum(moves.mapped('amount_untaxed_signed'))
        return round(invoiced / budget * 100.0, 2)

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
        'presupuesto_ventas': ('account.move', [('move_type', '=', 'out_invoice'), ('state', '=', 'posted')], 'invoice_date', False),
        'inventario_diferencia': ('stock.move.line', [('state', '=', 'done'), ('move_id.is_inventory', '=', True)], 'date', True),
        'inventario_ciclico': ('stock.move.line', [('state', '=', 'done'), ('move_id.is_inventory', '=', True)], 'date', True),
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
        if mode == 'reclamos_cliente':
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

    def action_capture(self):
        self.write({'state': 'capturado'})

    def action_validate(self):
        self._sgi_check_validate_access()
        self.write({'state': 'validado'})
        self._sgi_maybe_create_nc()

    def action_reset(self):
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
            alert = Alert.create(vals)
            measure.alert_id = alert.id
        return True


class QualityAlert(models.Model):
    _inherit = 'quality.alert'

    sgi_indicator_measure_id = fields.Many2one(
        'sgi.indicator.measure', string="Medición de indicador", readonly=True, copy=False)
