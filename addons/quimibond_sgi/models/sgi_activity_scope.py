# -*- coding: utf-8 -*-
"""Líneas de negocio (56.16.0): «Aplica a» de cada actividad.

Un mismo proceso atiende pedidos industriales y de confección, nacionales y de
exportación («Pedido a entrega» tiene pasos que solo son de una línea). Cada
actividad lleva las líneas a las que aplica; **vacío = aplica a todas**. Con
eso:

- las actividades se filtran por línea (el filtro trae también las generales);
- el diagrama del proceso atenúa lo que no aplica a la línea elegida;
- «Quién hace qué» se filtra y se agrupa por línea y por departamento;
- «Mi procedimiento» de un puesto (o de una persona) con líneas muestra solo
  las actividades generales y las de sus líneas;
- un indicador con fórmula puede medirse además por línea, cuando sus
  registros traen equipo de ventas o etiqueta del pedido: la medición oficial
  (el total) no cambia y el desglose por línea queda en la misma medición.

La lista de líneas la edita MAST (Administración SGI → Configuración → Líneas
de negocio); no está fija en el código.
"""
from collections.abc import Iterable

from odoo import api, fields, models
from odoo.exceptions import UserError, ValidationError

# Caminos que se prueban, en orden, para llegar del modelo del término del
# indicador al equipo de ventas o a las etiquetas del pedido.
_TEAM_PATHS = ('team_id', 'sale_id.team_id', 'order_id.team_id',
               'sale_line_id.order_id.team_id', 'move_id.team_id')
_TAG_PATHS = ('tag_ids', 'sale_id.tag_ids', 'order_id.tag_ids',
              'sale_line_id.order_id.tag_ids')


def _as_list(value):
    # Odoo 19 normaliza '=' a 'in' y manda un OrderedSet (no es list).
    if isinstance(value, Iterable) and not isinstance(value, str):
        return list(value)
    return [value]


class SgiActivityScope(models.Model):
    _name = 'sgi.activity.scope'
    _description = "Línea de negocio SGI (aplica a)"
    _order = 'sequence, name'

    name = fields.Char(string="Línea", required=True)
    sequence = fields.Integer(string="Secuencia", default=10)
    color = fields.Integer(string="Color")
    active = fields.Boolean(default=True)
    description = fields.Text(string="Descripción")
    team_ids = fields.Many2many(
        'crm.team', 'sgi_activity_scope_team_rel', 'scope_id', 'team_id',
        string="Equipos de venta",
        help="Para medir indicadores por línea: los registros de estos equipos "
             "de venta son de esta línea.")
    crm_tag_ids = fields.Many2many(
        'crm.tag', 'sgi_activity_scope_crm_tag_rel', 'scope_id', 'tag_id',
        string="Etiquetas del pedido",
        help="Para medir indicadores por línea: los pedidos con alguna de estas "
             "etiquetas son de esta línea.")
    activity_count = fields.Integer(string="Actividades", compute='_compute_activity_count')

    _name_uniq = models.Constraint('unique(name)', "Ya existe una línea con ese nombre.")

    def _compute_activity_count(self):
        counts = dict(self.env['sgi.process.activity']._read_group(
            [('scope_ids', 'in', self.ids)], ['scope_ids'], ['__count']))
        for scope in self:
            scope.activity_count = counts.get(scope, 0)

    def action_view_activities(self):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'name': "Actividades — %s" % self.name,
            'res_model': 'sgi.process.activity',
            'view_mode': 'list,kanban,form',
            'domain': [('scope_ids', 'in', self.ids)],
            'context': {'search_default_group_process': 1},
        }

    @api.model
    def _sgi_applies(self, activity_scopes, scopes):
        """¿Una actividad con ``activity_scopes`` aplica a alguna de ``scopes``?
        Sin líneas en la actividad, o sin línea elegida, aplica."""
        return not activity_scopes or not scopes or bool(activity_scopes & scopes)

    @api.model
    def _sgi_domain(self, scopes, path='scope_ids'):
        """Dominio «generales o de estas líneas» sobre ``path``."""
        return ['|', (path, '=', False), (path, 'in', scopes.ids)]


class SgiProcessActivityScope(models.Model):
    _inherit = 'sgi.process.activity'

    scope_ids = fields.Many2many(
        'sgi.activity.scope', 'sgi_activity_scope_rel', 'activity_id', 'scope_id',
        string="Aplica a",
        help="Líneas de negocio a las que aplica la actividad (industrial, "
             "confección, exportación…). Vacío = aplica a todas.")
    scope_filter_id = fields.Many2one(
        'sgi.activity.scope', string="Línea (con las generales)",
        compute='_compute_scope_filter_id', search='_search_scope_filter_id',
        help="Filtro: las actividades de la línea más las que aplican a todas.")

    def _compute_scope_filter_id(self):
        self.scope_filter_id = False

    @api.model
    def _search_scope_filter_id(self, operator, value):
        Scope = self.env['sgi.activity.scope'].with_context(active_test=False)
        if operator in ('any', 'not any'):
            scopes = Scope.search(value)
        else:
            values = [v for v in _as_list(value) if v]
            scopes = Scope.browse([v for v in values if isinstance(v, int)])
            for text in (v for v in values if isinstance(v, str)):
                scopes |= Scope.search([('name', 'ilike', text)])
        domain = Scope._sgi_domain(scopes)
        if operator in ('!=', 'not in', 'not ilike', 'not any'):
            return ['!'] + domain
        return domain

    def write(self, vals):
        res = super().write(vals)
        # Cambiar las líneas cambia el «Mi procedimiento» guardado de quienes
        # tienen rol en la actividad.
        if 'scope_ids' in vals:
            roles = self.sudo().with_context(active_test=False).role_ids
            self.env['hr.employee']._sgi_mp_touch_jobs(roles._sgi_mp_jobs())
        return res


class SgiProcessScope(models.Model):
    _inherit = 'sgi.process'

    activity_scope_ids = fields.Many2many(
        'sgi.activity.scope', string="Líneas de sus actividades",
        compute='_compute_activity_scope_ids', search='_search_activity_scope_ids',
        help="Líneas de negocio que aparecen en las actividades del proceso.")

    def _compute_activity_scope_ids(self):
        Activity = self.env['sgi.process.activity']
        for process in self:
            process.activity_scope_ids = Activity.search(
                [('process_id', '=', process.id)]).scope_ids if process.id else False

    @api.model
    def _search_activity_scope_ids(self, operator, value):
        activities = self.env['sgi.process.activity'].search([('scope_ids', operator, value)])
        return [('id', 'in', activities.process_id.ids)]


class HrJobScope(models.Model):
    _inherit = 'hr.job'

    sgi_scope_ids = fields.Many2many(
        'sgi.activity.scope', 'hr_job_sgi_scope_rel', 'job_id', 'scope_id',
        string="Líneas",
        help="Líneas de negocio del puesto (ej. vendedor de confección = "
             "Confección + Nacional). «Mi procedimiento» muestra solo las "
             "actividades generales y las de estas líneas. Vacío = todas.")

    def _sgi_mp_scopes(self):
        """Líneas que filtran «Mi procedimiento»: las de la persona si se
        imprime o calcula para ella y las tiene, si no las del puesto."""
        emp_id = self.env.context.get('sgi_mp_employee_id')
        if emp_id and len(self) == 1:
            emp = self.env['hr.employee'].sudo().browse(emp_id).exists()
            if emp and emp.sgi_scope_ids and emp.sgi_mp_job_id == self:
                return emp.sgi_scope_ids
        return self.sudo().sgi_scope_ids


class HrEmployeeScope(models.Model):
    _inherit = 'hr.employee'

    sgi_scope_ids = fields.Many2many(
        'sgi.activity.scope', 'hr_employee_sgi_scope_rel', 'employee_id', 'scope_id',
        string="Líneas",
        help="Solo si la persona atiende menos líneas que su puesto. Vacío = "
             "las del puesto.")


class SgiExecStatScope(models.Model):
    _inherit = 'sgi.activity.exec.stat'

    department_id = fields.Many2one(
        related='job_id.department_id', string="Departamento", store=True, index=True)
    scope_ids = fields.Many2many(
        'sgi.activity.scope', 'sgi_exec_stat_scope_rel', 'stat_id', 'scope_id',
        string="Aplica a", compute='_compute_scope_ids', store=True)
    scope_filter_id = fields.Many2one(
        related='activity_id.scope_filter_id', string="Línea (con las generales)")

    @api.depends('activity_id.scope_ids')
    def _compute_scope_ids(self):
        for stat in self:
            stat.scope_ids = stat.activity_id.scope_ids


class SgiIndicatorScope(models.Model):
    _inherit = 'sgi.indicator'

    measure_by_scope = fields.Boolean(
        string="Medir por línea", tracking=True,
        help="Además del total, una medición por línea de negocio. Solo con "
             "fórmula configurable y cuando los registros traen equipo de "
             "ventas o etiqueta del pedido (ver «Medición por línea»).")
    measure_scope_ids = fields.Many2many(
        'sgi.activity.scope', 'sgi_indicator_scope_rel', 'indicator_id', 'scope_id',
        string="Líneas a medir",
        help="Vacío = todas las líneas que tienen equipo de ventas o etiqueta.")
    scope_support = fields.Char(
        string="Medición por línea", compute='_compute_scope_support',
        help="Si la fórmula permite separar por línea y por qué campo.")

    @api.depends('measure_by_scope', 'calc_mode', 'term_ids.model_id')
    def _compute_scope_support(self):
        for indicator in self:
            indicator.scope_support = indicator._sgi_scope_problem() or (
                "Se separa por %s." % ", ".join(sorted({
                    path for term in indicator.term_ids
                    for path in term._sgi_scope_paths() if path})))

    def _sgi_scope_problem(self):
        """Texto de por qué no se puede medir por línea, o '' si se puede."""
        self.ensure_one()
        if self.calc_mode != 'configurable':
            return "Solo los indicadores con fórmula configurable se miden por línea."
        if not self.term_ids:
            return "Captura la fórmula (pestaña Fórmula) antes de medir por línea."
        missing = self.term_ids.filtered(lambda t: not any(t._sgi_scope_paths()))
        if missing:
            return "El modelo %s no trae equipo de ventas ni etiquetas del pedido." % ", ".join(
                sorted(set(missing.mapped('model_id.name'))))
        return ''

    def _sgi_scopes_to_measure(self):
        self.ensure_one()
        scopes = self.measure_scope_ids or self.env['sgi.activity.scope'].search([])
        return scopes.filtered(lambda s: s.team_ids or s.crm_tag_ids)

    @api.constrains('measure_by_scope', 'calc_mode', 'term_ids')
    def _check_measure_by_scope(self):
        for indicator in self.filtered('measure_by_scope'):
            problem = indicator._sgi_scope_problem()
            if problem:
                raise ValidationError("No se puede medir %s por línea: %s" % (indicator.name, problem))

    def _sgi_semaphore_on(self, value, period_date):
        """Semáforo de un valor en el periodo, con la misma regla que la
        medición (rango o metas de la trayectoria)."""
        self.ensure_one()
        if self.direction == 'range':
            lo, hi, tol = self.range_min, self.range_max, self.range_tolerance
            if lo <= value <= hi:
                return 'verde'
            return 'amarillo' if lo - tol <= value <= hi + tol else 'rojo'
        obj, acc = self._sgi_targets_on(period_date)
        if self.direction == 'lower_better':
            return 'verde' if value <= obj else 'amarillo' if value <= acc else 'rojo'
        return 'verde' if value >= obj else 'amarillo' if value >= acc else 'rojo'

    def _sgi_measure_vals(self, date_from, date_to):
        """El total como siempre y, si mide por línea, un renglón por línea."""
        vals = super()._sgi_measure_vals(date_from, date_to)
        if not self.measure_by_scope or self.calc_mode != 'configurable' or self._sgi_scope_problem():
            return vals
        lines = [(5, 0, 0)]
        for scope in self._sgi_scopes_to_measure():
            detail = self.with_context(sgi_scope_id=scope.id)._detail_configurable(date_from, date_to)
            value = detail.get('value')
            ids = detail.get('ids') or []
            lines.append((0, 0, {
                'scope_id': scope.id,
                'value': value or 0.0,
                'numerator': detail.get('numerator'),
                'denominator': detail.get('denominator'),
                'sample_size': len(ids),
                'state': 'sin_dato' if value is None else 'capturado',
                'detail_model': detail.get('model') or False,
                'detail_ids': ",".join(str(i) for i in ids) if ids else False,
            }))
        vals['scope_line_ids'] = lines
        return vals


class SgiIndicatorTermScope(models.Model):
    _inherit = 'sgi.indicator.term'

    def _sgi_scope_paths(self):
        """(camino al equipo de ventas, camino a las etiquetas) del modelo del
        término; None en el que no exista."""
        self.ensure_one()
        model = self.model_id.model
        if not model or model not in self.env:
            return (None, None)
        return (self._sgi_first_path(model, _TEAM_PATHS, 'crm.team'),
                self._sgi_first_path(model, _TAG_PATHS, 'crm.tag'))

    def _sgi_first_path(self, model, paths, comodel):
        for path in paths:
            Model = self.env[model]
            field = None
            for name in path.split('.'):
                field = Model._fields.get(name)
                if not field or not field.relational:
                    field = None
                    break
                Model = self.env[field.comodel_name]
            if field and field.comodel_name == comodel:
                return path
        return None

    def _sgi_scope_term_domain(self, scope):
        """Dominio de la línea sobre los registros del término (equipo de
        ventas o etiquetas del pedido; con los dos, cualquiera)."""
        team_path, tag_path = self._sgi_scope_paths()
        parts = []
        if team_path and scope.team_ids:
            parts.append([(team_path, 'in', scope.team_ids.ids)])
        if tag_path and scope.crm_tag_ids:
            parts.append([(tag_path, 'in', scope.crm_tag_ids.ids)])
        if not parts:
            return [('id', '=', 0)]  # la línea no se puede separar aquí: nada
        return parts[0] if len(parts) == 1 else ['|'] + parts[0] + parts[1]

    def _sgi_records(self, date_from, date_to):
        records = super()._sgi_records(date_from, date_to)
        scope_id = self.env.context.get('sgi_scope_id')
        if not scope_id:
            return records
        scope = self.env['sgi.activity.scope'].browse(scope_id)
        return records.filtered_domain(self._sgi_scope_term_domain(scope))


class SgiIndicatorMeasureScope(models.Model):
    _inherit = 'sgi.indicator.measure'

    scope_line_ids = fields.One2many(
        'sgi.indicator.measure.scope', 'measure_id', string="Por línea")


class SgiIndicatorMeasureScopeLine(models.Model):
    """Una medición por línea dentro de la medición del periodo. La oficial
    (NC, semáforo del proceso, tablero, RxD) sigue siendo el total."""
    _name = 'sgi.indicator.measure.scope'
    _description = "Medición de indicador por línea de negocio"
    _order = 'measure_id, scope_sequence, id'

    measure_id = fields.Many2one(
        'sgi.indicator.measure', string="Medición", required=True, ondelete='cascade', index=True)
    indicator_id = fields.Many2one(
        related='measure_id.indicator_id', string="Indicador", store=True, index=True)
    period_date = fields.Date(related='measure_id.period_date', string="Periodo", store=True)
    scope_id = fields.Many2one(
        'sgi.activity.scope', string="Línea", required=True, ondelete='restrict', index=True)
    scope_sequence = fields.Integer(related='scope_id.sequence', store=True)
    value = fields.Float(string="Valor", digits=(16, 2))
    numerator = fields.Float(string="Numerador", digits=(16, 2))
    denominator = fields.Float(string="Denominador", digits=(16, 2))
    sample_size = fields.Integer(string="Casos")
    state = fields.Selection([
        ('capturado', "Calculado"),
        ('sin_dato', "Sin dato"),
    ], string="Estado", default='capturado', required=True)
    uom = fields.Char(related='indicator_id.uom', string="Unidad")
    semaphore = fields.Selection([
        ('verde', "Verde"),
        ('amarillo', "Amarillo"),
        ('rojo', "Rojo"),
    ], string="Semáforo", compute='_compute_semaphore', store=True)
    detail_model = fields.Char(string="Modelo del detalle")
    detail_ids = fields.Text(string="Registros del detalle")

    _measure_scope_uniq = models.Constraint(
        'unique(measure_id, scope_id)', "Una fila por línea en cada medición.")

    @api.depends('value', 'state', 'period_date', 'indicator_id.direction',
                 'indicator_id.target_objective', 'indicator_id.target_acceptable',
                 'indicator_id.range_min', 'indicator_id.range_max',
                 'indicator_id.range_tolerance',
                 'indicator_id.step_ids.objective', 'indicator_id.step_ids.acceptable',
                 'indicator_id.step_ids.date_from')
    def _compute_semaphore(self):
        for line in self:
            if line.state == 'sin_dato' or not line.indicator_id:
                line.semaphore = False
            else:
                line.semaphore = line.indicator_id._sgi_semaphore_on(line.value, line.period_date)

    def action_view_evidence(self):
        self.ensure_one()
        ids = [int(i) for i in (self.detail_ids or '').split(',') if i.strip().isdigit()]
        if not self.detail_model or self.detail_model not in self.env:
            raise UserError("Esta línea no tiene registros de detalle.")
        return {
            'type': 'ir.actions.act_window',
            'name': "%s — %s" % (self.indicator_id.name, self.scope_id.name),
            'res_model': self.detail_model,
            'view_mode': 'list,form',
            'domain': [('id', 'in', ids)],
        }
