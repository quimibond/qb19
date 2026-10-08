# -*- coding: utf-8 -*-
"""Ruta y fichas de proceso (57.126.0, C1 bloque G; brief §6.7).

- La **ruta** son las operaciones de la lista de materiales (C1.04b). El
  diagrama de flujo de proceso (F-P-D01-32) se **imprime** desde ahí.
- **Fichas de proceso de tintorería (F-P-D01-33) y acabado (F-P-D01-05)**
  con la misma estructura que la de tejido (`sgi.machine.sheet`): cada
  parámetro lleva el valor **propuesto** (Diseño de Procesos) y el **real**
  (supervisor del área), con número de ajuste y motivo de lista; firmas
  iguales en las tres áreas: propone Diseño de Procesos, valida el supervisor
  del área (en tejido el Jefe de Manufactura), el laboratorio mide. La ficha
  validada de la corrida aprobada **pasa a ser la ficha vigente del artículo**
  sin recaptura.
- Acabado maneja hasta **tres pases de rama**; al tercero sin cumplir se
  avisa a Diseño de Producto. La **gráfica de tintorería** se genera con los
  tramos de gradiente, temperatura y tiempo.
- **Químicos:** la ficha de tintorería y la receta de rama no capturan
  químicos (decisión de Jose, 2026-10-06): llevan el número de fórmula y
  apuntarán al modelo de fórmulas en g/L de Jose Sacramento cuando exista.
"""
import logging

from odoo import api, fields, models
from odoo.exceptions import UserError, ValidationError


_logger = logging.getLogger(__name__)

AREAS = [('tintoreria', "Tintorería"), ('acabado', "Acabado")]
SHEET_STATES = [('borrador', "Borrador"), ('propuesta', "Propuesta"), ('validada', "Validada"),
                ('vigente', "Vigente"), ('obsoleta', "Obsoleta")]
PARAM_KINDS = [('num', "Número"), ('text', "Texto corto"), ('bool', "Sí / No")]
MAX_PASSES = 3
MAX_ROUTE_STEPS = 10
PARAM_VALIDATOR_JOB = {
    'tejido': 'quimibond_sgi.dev_validator_job_tejido_id',
    'tintoreria': 'quimibond_sgi.dev_validator_job_tintoreria_id',
    'acabado': 'quimibond_sgi.dev_validator_job_acabado_id',
}
PARAM_PRODUCT_DESIGN_JOB = 'quimibond_sgi.dev_product_design_job_id'
VALIDATOR_JOB_NAMES = {'tejido': 'JEFE DE MANUFACTURA', 'tintoreria': 'SUPERVISOR TINTORERIA',
                       'acabado': 'SUPERVISOR TAC'}
PRODUCT_DESIGN_JOB_NAME = 'DISEÑO Y DESARROLLO DE PRODUCTO'

# (sección, nombre, unidad, tipo). Anexo B del brief.
DYE_PARAMS = [
    ('bano', "Tipo de acabado", "", 'text'), ('bano', "Relación de baño", "L/kg", 'num'),
    ('bano', "Volumen de baño", "L", 'num'), ('bano', "Peso total de tela", "kg", 'num'),
    ('bano', "Composición de la tela", "", 'text'),
    ('maquina', "Porcentaje del brazo del plegador", "%", 'num'), ('maquina', "Bomba: velocidad", "rpm", 'num'),
    ('maquina', "Bomba: presión", "bar", 'num'), ('maquina', "Acumulador inicial", "%", 'num'),
    ('maquina', "Acumulador cargado", "%", 'num'),
    ('tiempos', "Tiempo empleado", "min", 'num'), ('tiempos', "Temperatura", "°C", 'num'),
    ('tiempos', "Gradiente de subida", "°C/min", 'num'), ('tiempos', "Gradiente de bajada", "°C/min", 'num'),
]
FINISH_PASS_PARAMS = (
    [('pase', "Color", "", 'text'), ('pase', "Rama", "Bruckner / Unitech", 'text')]
    + [('pase', "Temperatura campo %d lado %s" % (i, lado), "°C", 'num') for i in range(1, 9) for lado in ('A', 'B')]
    + [('pase', "Velocidad", "m/min", 'num'), ('pase', "Ancho de entrada 1", "m", 'num'),
       ('pase', "Ancho de entrada 2", "m", 'num'), ('pase', "Ancho de cadena", "m", 'num'),
       ('pase', "Ancho de salida", "m", 'num'), ('pase', "Rodillo de presión superior", "", 'bool'),
       ('pase', "Alimentación rodillo superior", "%", 'num'), ('pase', "Alimentación rodillo inferior", "%", 'num'),
       ('pase', "Alimentación rueda izquierda", "%", 'num'), ('pase', "Alimentación rueda derecha", "%", 'num'),
       ('pase', "Tensión de salida", "", 'num'), ('pase', "Extracción de humedad", "", 'num'),
       ('pase', "Pick up", "%", 'num'), ('pase', "Número de fórmula de la receta de rama", "", 'text'),
       ('pase', "Temperatura del foulard", "°C", 'num'), ('pase', "Vaporizar en entrada", "", 'bool'),
       ('pase', "Campo de enfriamiento en salida", "", 'bool'), ('pase', "Ancho de entrada de tela", "m", 'num'),
       ('pase', "Ancho de salida de tela", "m", 'num'), ('pase', "Masa de entrada", "g/m²", 'num'),
       ('pase', "Masa de salida", "g/m²", 'num'), ('pase', "Corte de orillas", "", 'bool'),
       ('pase', "Engomado de orillas", "", 'bool'), ('pase', "Tipo de goma", "discontinua / continua", 'text')]
)
TEJIDO_PREFIXES = ('CIRCULAR', 'CARDA', 'TEJIDO', 'V10', 'V18', 'V25')
TINTORERIA_PREFIXES = ('TINTORERIA', 'HTJ')


def _workcenter_area(workcenter):
    name = (workcenter.name or '').upper().strip()
    code = (workcenter.code or '').upper()
    if name.startswith(TINTORERIA_PREFIXES) or code.startswith('HTJ'):
        return 'tintoreria'
    if name.startswith(TEJIDO_PREFIXES):
        return 'tejido'
    return 'acabado'


class SgiDevProcessSheet(models.Model):
    """Ficha de proceso de tintorería o acabado de un artículo (y de la corrida de muestra que la originó)."""
    _name = 'sgi.dev.process.sheet'
    _description = "Ficha de proceso de tintorería o acabado"
    _inherit = ['mail.thread', 'mail.activity.mixin']
    _order = 'product_id, area, revision desc, id desc'

    name = fields.Char(string="Folio", readonly=True, copy=False, default="Nuevo")
    area = fields.Selection(AREAS, required=True, default='tintoreria', tracking=True)
    product_id = fields.Many2one('product.product', string="Artículo", required=True, index=True, tracking=True)
    workcenter_id = fields.Many2one('mrp.workcenter', string="Máquina / centro de trabajo", required=True, tracking=True)
    project_id = fields.Many2one('project.project', string="Proyecto de desarrollo", index=True, ondelete='set null',
                                 domain="[('sgi_is_ft', '=', True), ('is_template', '=', False)]")
    production_id = fields.Many2one('mrp.production', string="Orden de la corrida", ondelete='set null',
                                    help="Orden de muestra cuyos parámetros registra esta ficha.")
    workorder_id = fields.Many2one('mrp.workorder', string="Orden de trabajo", ondelete='set null',
                                   domain="[('production_id', '=', production_id)]")
    date = fields.Date(default=fields.Date.context_today, required=True)
    revision = fields.Integer(default=0, readonly=True)
    state = fields.Selection(SHEET_STATES, default='borrador', required=True, tracking=True, index=True)
    active = fields.Boolean(default=True)
    company_id = fields.Many2one('res.company', default=lambda self: self.env.company)
    param_line_ids = fields.One2many('sgi.dev.process.param', 'sheet_id', string="Parámetros")
    note = fields.Text(string="Observaciones")
    # --- Firmas (iguales en las tres áreas) ----------------------------------
    proposed_by_id = fields.Many2one('res.users', string="Propuso (Diseño y Desarrollo de Procesos)", readonly=True, copy=False)
    proposed_date = fields.Datetime(readonly=True, copy=False)
    validated_by_id = fields.Many2one('res.users', string="Validó (supervisor del área)", readonly=True, copy=False)
    validated_date = fields.Datetime(readonly=True, copy=False)
    lab_user_id = fields.Many2one('res.users', string="Midió (Laboratorio)", copy=False)
    lab_date = fields.Datetime(copy=False)
    # --- Tintorería -----------------------------------------------------------
    formula_code = fields.Char(string="Número de fórmula",
                               help="Los químicos no se capturan aquí: la fórmula en g/L vive en el modelo de "
                                    "fórmulas (Jose Sacramento). Mientras llega, el número de fórmula.")
    dye_step_ids = fields.One2many('sgi.dev.dye.step', 'sheet_id', string="Tramos de la gráfica de proceso")
    dye_total_min = fields.Float(string="Tiempo total de la gráfica (min)", compute='_compute_dye_chart', digits=(16, 1))
    dye_chart_svg = fields.Html(string="Gráfica de proceso", compute='_compute_dye_chart', sanitize=False)
    # --- Acabado --------------------------------------------------------------
    route_line_ids = fields.One2many('sgi.dev.finish.route', 'sheet_id', string="Ruta de proceso (hasta 10 pasos)")
    dye_machine = fields.Char(string="Teñido: máquina")
    dye_brand = fields.Char(string="Teñido: marca")
    dye_model = fields.Char(string="Teñido: modelo")
    pass_count = fields.Integer(string="Pases de rama", compute='_compute_pass_count', store=True)
    pass_result_1 = fields.Selection([('cumple', "Cumple"), ('no_cumple', "No cumple")], string="Resultado pase 1")
    pass_result_2 = fields.Selection([('cumple', "Cumple"), ('no_cumple', "No cumple")], string="Resultado pase 2")
    pass_result_3 = fields.Selection([('cumple', "Cumple"), ('no_cumple', "No cumple")], string="Resultado pase 3")
    third_pass_alerted = fields.Boolean(readonly=True, copy=False)

    # ------------------------------------------------------------------------
    @api.model
    def _seed_params(self, area, pass_number=0):
        if area == 'tintoreria':
            return [(0, 0, {'section': s, 'name': n, 'unit': u, 'kind': k, 'sequence': i * 10})
                    for i, (s, n, u, k) in enumerate(DYE_PARAMS)]
        return [(0, 0, {'section': s, 'name': n, 'unit': u, 'kind': k, 'sequence': i * 10,
                        'pass_number': pass_number or 1})
                for i, (s, n, u, k) in enumerate(FINISH_PASS_PARAMS)]

    @api.model_create_multi
    def create(self, vals_list):
        Seq = self.env['ir.sequence']
        for vals in vals_list:
            if not vals.get('name') or vals['name'] == "Nuevo":
                vals['name'] = Seq.next_by_code('sgi.dev.process.sheet') or "Nuevo"
            if not vals.get('param_line_ids'):
                vals['param_line_ids'] = self._seed_params(vals.get('area', 'tintoreria'))
        sheets = super().create(vals_list)
        return sheets

    @api.onchange('workcenter_id')
    def _onchange_workcenter_id(self):
        if self.workcenter_id:
            area = _workcenter_area(self.workcenter_id)
            if area in dict(AREAS):
                self.area = area

    @api.depends('param_line_ids.pass_number')
    def _compute_pass_count(self):
        for sheet in self:
            sheet.pass_count = max(sheet.param_line_ids.mapped('pass_number') or [0]) if sheet.area == 'acabado' else 0

    @api.constrains('route_line_ids')
    def _check_route_steps(self):
        for sheet in self:
            if len(sheet.route_line_ids) > MAX_ROUTE_STEPS:
                raise ValidationError("La ruta de acabado lleva hasta %d pasos." % MAX_ROUTE_STEPS)

    @api.constrains('state', 'product_id', 'area')
    def _check_single_current(self):
        for sheet in self.filtered(lambda s: s.state == 'vigente'):
            other = self.search([('id', '!=', sheet.id), ('state', '=', 'vigente'),
                                 ('product_id', '=', sheet.product_id.id), ('area', '=', sheet.area)], limit=1)
            if other:
                raise ValidationError("Ya hay una ficha vigente de %s en %s (%s)." % (
                    sheet.product_id.display_name, dict(AREAS)[sheet.area], other.name))

    # ------------------------------------------------------------------------
    # Gráfica de tintorería
    # ------------------------------------------------------------------------
    @api.depends('dye_step_ids.temp_start', 'dye_step_ids.temp_end', 'dye_step_ids.gradient',
                 'dye_step_ids.hold_min', 'dye_step_ids.sequence')
    def _compute_dye_chart(self):
        for sheet in self:
            points, t = [], 0.0
            for step in sheet.dye_step_ids.sorted(lambda s: (s.sequence, s.id)):
                if not points:
                    points.append((0.0, step.temp_start))
                ramp = abs(step.temp_end - step.temp_start) / step.gradient if step.gradient else 0.0
                t += ramp
                points.append((t, step.temp_end))
                if step.hold_min:
                    t += step.hold_min
                    points.append((t, step.temp_end))
            sheet.dye_total_min = t
            sheet.dye_chart_svg = sheet._dye_chart_svg(points) if len(points) > 1 else False

    @staticmethod
    def _dye_chart_svg(points):
        w, h, pad = 600, 220, 36
        max_t = max(p[0] for p in points) or 1.0
        max_c = max(max(p[1] for p in points), 10.0)

        def x(t):
            return pad + (w - 2 * pad) * t / max_t

        def y(c):
            return h - pad - (h - 2 * pad) * c / max_c
        poly = " ".join("%.1f,%.1f" % (x(t), y(c)) for t, c in points)
        labels = "".join(
            '<text x="%.1f" y="%.1f" font-size="9" text-anchor="middle">%g°C · %g min</text>'
            % (x(t), y(c) - 4, round(c, 1), round(t, 1)) for t, c in points)
        return ('<svg xmlns="http://www.w3.org/2000/svg" width="%d" height="%d" viewBox="0 0 %d %d">'
                '<rect x="0" y="0" width="%d" height="%d" fill="#fff"/>'
                '<line x1="%d" y1="%d" x2="%d" y2="%d" stroke="#333"/>'
                '<line x1="%d" y1="%d" x2="%d" y2="%d" stroke="#333"/>'
                '<text x="%d" y="%d" font-size="10">°C</text><text x="%d" y="%d" font-size="10" text-anchor="end">min</text>'
                '<polyline fill="none" stroke="#c0392b" stroke-width="2" points="%s"/>%s</svg>'
                % (w, h, w, h, w, h, pad, h - pad, w - pad, h - pad, pad, pad, pad, h - pad,
                   4, pad, w - 4, h - 8, poly, labels))

    # ------------------------------------------------------------------------
    # Pases de rama
    # ------------------------------------------------------------------------
    def action_add_pass(self):
        for sheet in self:
            if sheet.area != 'acabado':
                raise UserError("Los pases de rama son de la ficha de acabado.")
            if sheet.pass_count >= MAX_PASSES:
                raise UserError("Acabado maneja hasta %d pases de rama." % MAX_PASSES)
            sheet.write({'param_line_ids': self._seed_params('acabado', sheet.pass_count + 1)})
        return True

    def _sgi_dev_product_design_users(self):
        job = self.env['hr.job'].sudo().browse(
            int(self.env['ir.config_parameter'].sudo().get_param(PARAM_PRODUCT_DESIGN_JOB, '0') or 0)).exists()
        if not job:
            return self.env['res.users']
        return self.env['hr.employee'].sudo().search([('job_id', '=', job.id)]).user_id.filtered(
            lambda u: u.active and not u.share)

    def write(self, vals):
        res = super().write(vals)
        if vals.get('pass_result_3') == 'no_cumple':
            for sheet in self.filtered(lambda s: not s.third_pass_alerted):
                users = sheet._sgi_dev_product_design_users()
                for user in users:
                    sheet.activity_schedule('mail.mail_activity_data_todo', user_id=user.id,
                                            summary="Tercer pase de rama sin cumplir: %s" % sheet.product_id.display_name,
                                            note="Ficha %s. Decidir qué sigue con el desarrollo." % sheet.name)
                sheet.message_post(body="Tercer pase de rama sin cumplir: aviso a Diseño de Producto (%s)."
                                   % (", ".join(users.mapped('name')) or "sin puesto configurado"))
                sheet.third_pass_alerted = True
        return res

    # ------------------------------------------------------------------------
    # Firmas y ciclo
    # ------------------------------------------------------------------------
    @api.model
    def _sgi_dev_user_has_job(self, user, param_key):
        raw = self.env['ir.config_parameter'].sudo().get_param(param_key, '') or ''
        if not raw.strip().isdigit():
            return False
        return bool(self.env['hr.employee'].sudo().search_count(
            [('job_id', '=', int(raw)), ('user_id', '=', user.id)]))

    def _sgi_dev_can_validate(self, user):
        self.ensure_one()
        return (user.has_group('quimibond_sgi.group_sgi_manager')
                or self._sgi_dev_user_has_job(user, PARAM_VALIDATOR_JOB[self.area]))

    def action_propose(self):
        for sheet in self:
            if sheet.state != 'borrador':
                raise UserError("Solo un borrador se propone.")
            sheet.write({'state': 'propuesta', 'proposed_by_id': self.env.uid, 'proposed_date': fields.Datetime.now()})
            sheet.message_post(body="Parámetros propuestos por %s." % self.env.user.name)
        return True

    def action_validate(self):
        for sheet in self:
            if sheet.state != 'propuesta':
                raise UserError("Solo una ficha propuesta se valida.")
            if not sheet._sgi_dev_can_validate(self.env.user):
                raise UserError("La ficha de %s la valida el supervisor del área (Ajustes → SGI → Desarrollo de "
                                "producto) o un administrador del SGI." % dict(AREAS)[sheet.area])
            sheet.write({'state': 'validada', 'validated_by_id': self.env.uid, 'validated_date': fields.Datetime.now()})
            sheet.message_post(body="Parámetros reales validados por %s." % self.env.user.name)
        return True

    def action_set_current(self):
        """La ficha validada de la corrida aprobada pasa a ser la ficha vigente del artículo."""
        for sheet in self:
            if sheet.state not in ('validada', 'borrador', 'propuesta'):
                raise UserError("Solo una ficha validada (o un borrador, para artículos de línea) se pone en vigor.")
            previous = self.search([('id', '!=', sheet.id), ('state', '=', 'vigente'),
                                    ('product_id', '=', sheet.product_id.id), ('area', '=', sheet.area)])
            previous.write({'state': 'obsoleta'})
            sheet.write({'state': 'vigente',
                         'revision': (max(previous.mapped('revision')) + 1) if previous else sheet.revision})
            sheet.message_post(body="Ficha vigente de %s en %s%s." % (
                sheet.product_id.display_name, dict(AREAS)[sheet.area],
                (" (sustituye a %s)" % ", ".join(previous.mapped('name'))) if previous else ""))
        return True

    def action_set_obsolete(self):
        self.write({'state': 'obsoleta'})
        return True

    def action_back_to_draft(self):
        self.write({'state': 'borrador'})
        return True

    def action_new_revision(self):
        self.ensure_one()
        new = self.copy({'state': 'borrador', 'revision': self.revision + 1, 'date': fields.Date.context_today(self),
                         'proposed_by_id': False, 'validated_by_id': False})
        return {'type': 'ir.actions.act_window', 'res_model': self._name, 'res_id': new.id, 'view_mode': 'form'}

    def copy_data(self, default=None):
        default = dict(default or {})
        vals_list = super().copy_data(default=default)
        for sheet, vals in zip(self, vals_list):
            vals['param_line_ids'] = [(0, 0, l.copy_data()[0]) for l in sheet.param_line_ids]
            vals['dye_step_ids'] = [(0, 0, l.copy_data()[0]) for l in sheet.dye_step_ids]
            vals['route_line_ids'] = [(0, 0, l.copy_data()[0]) for l in sheet.route_line_ids]
        return vals_list

    def action_print(self):
        return self.env.ref('quimibond_sgi.action_report_dev_process_sheet').report_action(self)

    def sgi_format_info(self):
        self.ensure_one()
        ref = 'format_ref_dye_sheet' if self.area == 'tintoreria' else 'format_ref_finish_sheet'
        return self.env['sgi.format.map'].sudo().sgi_ref_label(ref)


class SgiDevProcessParam(models.Model):
    """Parámetro de la ficha: propuesto por Diseño de Procesos, real del supervisor, ajuste y motivo."""
    _name = 'sgi.dev.process.param'
    _description = "Parámetro de la ficha de proceso"
    _order = 'pass_number, section, sequence, id'

    sheet_id = fields.Many2one('sgi.dev.process.sheet', required=True, ondelete='cascade', index=True)
    pass_number = fields.Integer(string="Pase", default=0, help="Pase de rama (acabado); 0 en tintorería.")
    section = fields.Selection([('bano', "Baño"), ('maquina', "Máquina"), ('tiempos', "Tiempos y temperatura"),
                                ('pase', "Pase de rama")], required=True, default='maquina')
    sequence = fields.Integer(default=10)
    name = fields.Char(string="Parámetro", required=True)
    unit = fields.Char(string="Unidad")
    kind = fields.Selection(PARAM_KINDS, required=True, default='num')
    proposed_num = fields.Float(string="Propuesto", digits=(16, 3))
    real_num = fields.Float(string="Real", digits=(16, 3))
    proposed_text = fields.Char(string="Propuesto (texto)")
    real_text = fields.Char(string="Real (texto)")
    proposed_bool = fields.Boolean(string="Propuesto (sí)")
    real_bool = fields.Boolean(string="Real (sí)")
    adjustment = fields.Integer(string="Número de ajuste", help="Cuántas veces se ajustó este parámetro en la corrida.")
    reason_id = fields.Many2one('sgi.dev.option', string="Motivo del ajuste", domain="[('kind', '=', 'motivo_ajuste')]",
                                help="Motivo de lista (SGI → Desarrollos → Opciones, lista «Motivo de ajuste»).")
    proposed_label = fields.Char(compute='_compute_labels')
    real_label = fields.Char(compute='_compute_labels')

    @api.depends('kind', 'proposed_num', 'real_num', 'proposed_text', 'real_text', 'proposed_bool', 'real_bool', 'unit')
    def _compute_labels(self):
        for line in self:
            if line.kind == 'num':
                p = ('%g %s' % (line.proposed_num, line.unit or '')).strip() if line.proposed_num else ''
                r = ('%g %s' % (line.real_num, line.unit or '')).strip() if line.real_num else ''
            elif line.kind == 'bool':
                p, r = ('Sí' if line.proposed_bool else 'No'), ('Sí' if line.real_bool else 'No')
            else:
                p, r = line.proposed_text or '', line.real_text or ''
            line.proposed_label, line.real_label = p, r


class SgiDevDyeStep(models.Model):
    """Tramo de la gráfica de proceso de tintorería: de una temperatura a otra con un gradiente y un sostenimiento."""
    _name = 'sgi.dev.dye.step'
    _description = "Tramo de la gráfica de tintorería"
    _order = 'sequence, id'

    sheet_id = fields.Many2one('sgi.dev.process.sheet', required=True, ondelete='cascade', index=True)
    sequence = fields.Integer(default=10)
    name = fields.Char(string="Tramo", required=True, help="Por ejemplo: calentamiento, agotamiento, enfriamiento.")
    temp_start = fields.Float(string="Temperatura inicial (°C)", digits=(6, 1))
    temp_end = fields.Float(string="Temperatura final (°C)", digits=(6, 1))
    gradient = fields.Float(string="Gradiente (°C/min)", digits=(6, 2))
    hold_min = fields.Float(string="Sostener (min)", digits=(6, 1))
    minutes = fields.Float(string="Minutos del tramo", compute='_compute_minutes', digits=(6, 1))

    @api.depends('temp_start', 'temp_end', 'gradient', 'hold_min')
    def _compute_minutes(self):
        for step in self:
            ramp = abs(step.temp_end - step.temp_start) / step.gradient if step.gradient else 0.0
            step.minutes = ramp + (step.hold_min or 0.0)


class SgiDevFinishRoute(models.Model):
    """Paso de la ruta de proceso de acabado (hasta 10)."""
    _name = 'sgi.dev.finish.route'
    _description = "Paso de la ruta de acabado"
    _order = 'sequence, id'

    sheet_id = fields.Many2one('sgi.dev.process.sheet', required=True, ondelete='cascade', index=True)
    sequence = fields.Integer(default=10)
    name = fields.Char(string="Paso", required=True)
    workcenter_id = fields.Many2one('mrp.workcenter', string="Centro de trabajo")


class ProjectProjectDevProcessSheet(models.Model):
    _inherit = 'project.project'

    sgi_dev_process_sheet_ids = fields.One2many('sgi.dev.process.sheet', 'project_id', string="Fichas de proceso")
    sgi_dev_process_sheet_count = fields.Integer(compute='_compute_sgi_dev_process_sheet_count')

    @api.depends('sgi_dev_process_sheet_ids')
    def _compute_sgi_dev_process_sheet_count(self):
        for project in self:
            project.sgi_dev_process_sheet_count = len(project.sgi_dev_process_sheet_ids)

    def _sgi_dev_route_operations(self):
        """[(artículo, operación)] en el orden de la ruta: crudo → teñido → acabado, y dentro de cada
        artículo las operaciones de su lista de materiales."""
        self.ensure_one()
        out = []
        Bom = self.env['mrp.bom'].sudo()
        for product in (self.sgi_dev_product_crudo_id | self.sgi_dev_product_tenido_id | self.sgi_dev_product_id):
            bom = Bom._bom_find(product, company_id=self.company_id.id).get(product)
            if not bom:
                continue
            for op in bom.operation_ids.sorted(lambda o: (o.sequence, o.id)):
                out.append((product, op))
        return out

    def action_sgi_dev_print_flow(self):
        self.ensure_one()
        if not self._sgi_dev_route_operations():
            raise UserError("El diagrama de flujo se imprime desde la ruta: capture las operaciones en la lista de "
                            "materiales del artículo (C1.04b).")
        return self.env.ref('quimibond_sgi.action_report_dev_flow').report_action(self)

    def action_sgi_dev_view_process_sheets(self):
        self.ensure_one()
        product = self.sgi_dev_product_tenido_id or self.sgi_dev_product_id
        mo = self.sgi_dev_mo_ids[:1] if 'sgi_dev_mo_ids' in self._fields else False
        return {'type': 'ir.actions.act_window', 'name': "Fichas de proceso de %s" % (self.sgi_ft_folio or self.name),
                'res_model': 'sgi.dev.process.sheet', 'view_mode': 'list,form',
                'domain': [('project_id', '=', self.id)],
                'context': {'default_project_id': self.id, 'default_product_id': product.id if product else False,
                            'default_production_id': mo.id if mo else False}}


class SgiMachineSheetDev(models.Model):
    """La ficha de tejido con las firmas por puestos vigentes y propuesto / real por parámetro."""
    _inherit = 'sgi.machine.sheet'

    project_id = fields.Many2one('project.project', string="Proyecto de desarrollo", index=True, ondelete='set null',
                                 domain="[('sgi_is_ft', '=', True), ('is_template', '=', False)]")
    production_id = fields.Many2one('mrp.production', string="Orden de la corrida", ondelete='set null')
    proposed_date = fields.Datetime(readonly=True, copy=False)
    validated_date = fields.Datetime(readonly=True, copy=False)
    lab_date = fields.Datetime(copy=False)
    # 57.126.0: las firmas viejas («Jefe técnico de tejido y acabado», «Jefe de ingeniería de
    # procesos») no existen como puestos; las columnas se quedan con el papel vigente.
    engineering_by_id = fields.Many2one(string="Propuso (Diseño y Desarrollo de Procesos)", readonly=True, copy=False)
    approved_by_id = fields.Many2one(string="Validó (Jefe de Manufactura)", readonly=True, copy=False)
    lab_user_id = fields.Many2one(string="Midió (Laboratorio)")

    def action_propose(self):
        for sheet in self:
            if sheet.state != 'borrador':
                raise UserError("Solo un borrador se propone.")
            sheet.write({'engineering_by_id': self.env.uid, 'proposed_date': fields.Datetime.now()})
            sheet.message_post(body="Parámetros propuestos por %s." % self.env.user.name)
        return True

    def action_validate(self):
        Sheet = self.env['sgi.dev.process.sheet']
        for sheet in self:
            if not sheet.engineering_by_id:
                raise UserError("Primero propone Diseño de Procesos.")
            if not (self.env.user.has_group('quimibond_sgi.group_sgi_manager')
                    or Sheet._sgi_dev_user_has_job(self.env.user, PARAM_VALIDATOR_JOB['tejido'])):
                raise UserError("La ficha de tejido la valida el Jefe de Manufactura (Ajustes → SGI → Desarrollo de "
                                "producto) o un administrador del SGI.")
            sheet.write({'approved_by_id': self.env.uid, 'validated_date': fields.Datetime.now()})
            sheet.message_post(body="Parámetros reales validados por %s." % self.env.user.name)
        return True


class SgiMachineSheetParamDev(models.Model):
    _inherit = 'sgi.machine.sheet.param'

    real = fields.Char(string="Real (corrida)", help="Lo que se corrió de verdad; lo valida el supervisor.")
    adjustment = fields.Integer(string="Número de ajuste")
    reason_id = fields.Many2one('sgi.dev.option', string="Motivo del ajuste", domain="[('kind', '=', 'motivo_ajuste')]")
