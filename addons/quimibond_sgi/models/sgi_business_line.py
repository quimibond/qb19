# -*- coding: utf-8 -*-
"""Líneas de negocio con lo que Odoo ya tiene (56.16.0).

Un mismo proceso atiende pedidos industriales y de confección, nacionales y de
exportación («C2 Pedido a entrega»). En vez de un catálogo propio del SGI se
usan los objetos de Odoo:

- **Equipos de venta** (``crm.team``: Industrial, Confección…): a qué línea
  aplica una actividad («Aplica a»; vacío = a todas). Las líneas de una
  persona son los equipos de los que ya es miembro o líder en Ventas.
- **Posiciones fiscales** (``account.fiscal.position``: Nacional, Cliente
  extranjero…): a qué mercado aplica una actividad (vacío = a todos).
- **Departamentos** (``hr.department``): no se capturan; salen de los puestos
  que ejecutan la actividad.

Con eso las actividades se filtran por equipo y por mercado (el filtro trae
también las generales), el diagrama atenúa lo que no aplica, «Quién hace
qué» se agrupa por departamento y equipo, «Mi procedimiento» de cada persona
muestra solo lo general y lo de sus equipos, y un indicador con fórmula puede
desglosarse por equipo de ventas o por mercado (nacional / exportación, por
el país del cliente). La medición oficial sigue siendo el total.
"""
from collections.abc import Iterable

from odoo import api, fields, models
from odoo.exceptions import UserError, ValidationError

# Caminos que se prueban, en orden, del modelo de un término del indicador al
# equipo de ventas y al país del cliente.
_TEAM_PATHS = ('team_id', 'sale_id.team_id', 'order_id.team_id',
               'sale_line_id.order_id.team_id', 'move_id.team_id')
_COUNTRY_PATHS = ('partner_id.country_id', 'sale_id.partner_id.country_id',
                  'order_id.partner_id.country_id', 'move_id.partner_id.country_id')
_MARKETS = [('nacional', "Nacional"), ('exportacion', "Exportación"), ('sin_pais', "Cliente sin país")]


def _as_list(value):
    # Odoo 19 normaliza '=' a 'in' y manda un OrderedSet (no es list).
    if isinstance(value, Iterable) and not isinstance(value, str):
        return list(value)
    return [value]


def _general_or(path, records):
    """Dominio «sin nada capturado o con alguno de estos»."""
    return ['|', (path, '=', False), (path, 'in', records.ids)]


def _search_general_or(comodel, path, operator, value):
    """Búsqueda de los filtros «con las generales»: acepta el registro elegido
    (id), el texto escrito (nombre) o un subdominio (any)."""
    Model = comodel.with_context(active_test=False)
    if operator in ('any', 'not any'):
        records = Model.search(value)
    else:
        values = [v for v in _as_list(value) if v]
        records = Model.browse([v for v in values if isinstance(v, int)])
        for text in (v for v in values if isinstance(v, str)):
            records |= Model.search([('name', 'ilike', text)])
    domain = _general_or(path, records)
    if operator in ('!=', 'not in', 'not ilike', 'not any'):
        return ['!'] + domain
    return domain


class SgiProcessActivityLine(models.Model):
    _inherit = 'sgi.process.activity'

    sale_team_ids = fields.Many2many(
        'crm.team', 'sgi_activity_crm_team_rel', 'activity_id', 'team_id',
        string="Aplica a (equipo de ventas)",
        help="Líneas de negocio (equipos de venta de Odoo) a las que aplica la "
             "actividad. Vacío = a todas.")
    fiscal_position_ids = fields.Many2many(
        'account.fiscal.position', 'sgi_activity_fiscal_position_rel', 'activity_id', 'position_id',
        string="Mercado (posición fiscal)",
        help="Mercado al que aplica (Nacional, Cliente extranjero…). Vacío = a todos.")
    department_ids = fields.Many2many(
        'hr.department', 'sgi_activity_department_rel', 'activity_id', 'department_id',
        string="Departamentos", compute='_compute_department_ids', store=True,
        help="Departamentos de los puestos que la ejecutan (se calcula).")
    team_filter_id = fields.Many2one(
        'crm.team', string="Equipo (con las generales)",
        compute='_compute_line_filters', search='_search_team_filter_id',
        help="Filtro por equipo de ventas que incluye las actividades generales.")
    market_filter_id = fields.Many2one(
        'account.fiscal.position', string="Mercado (con las generales)",
        compute='_compute_line_filters', search='_search_market_filter_id',
        help="Filtro por mercado que incluye las actividades generales.")

    @api.depends('responsible_job_ids.department_id')
    def _compute_department_ids(self):
        for activity in self:
            activity.department_ids = activity.responsible_job_ids.department_id

    def _compute_line_filters(self):
        self.team_filter_id = False
        self.market_filter_id = False

    @api.model
    def _search_team_filter_id(self, operator, value):
        return _search_general_or(self.env['crm.team'], 'sale_team_ids', operator, value)

    @api.model
    def _search_market_filter_id(self, operator, value):
        return _search_general_or(self.env['account.fiscal.position'], 'fiscal_position_ids', operator, value)

    def _sgi_applies_to_teams(self, teams):
        """¿Aplica a alguno de estos equipos? Sin equipos en la actividad, o
        sin equipo elegido, aplica."""
        self.ensure_one()
        return not self.sale_team_ids or not teams or bool(self.sale_team_ids & teams)

    def write(self, vals):
        res = super().write(vals)
        # Cambiar los equipos cambia el «Mi procedimiento» guardado de quienes
        # tienen rol en la actividad.
        if 'sale_team_ids' in vals:
            roles = self.sudo().with_context(active_test=False).role_ids
            self.env['hr.employee']._sgi_mp_touch_jobs(roles._sgi_mp_jobs())
        return res


class SgiProcessLine(models.Model):
    _inherit = 'sgi.process'

    activity_team_ids = fields.Many2many(
        'crm.team', string="Equipos de sus actividades",
        compute='_compute_activity_team_ids', search='_search_activity_team_ids',
        help="Equipos de venta que aparecen en las actividades del proceso.")

    def _compute_activity_team_ids(self):
        Activity = self.env['sgi.process.activity']
        for process in self:
            process.activity_team_ids = Activity.search(
                [('process_id', '=', process.id)]).sale_team_ids if process.id else False

    @api.model
    def _search_activity_team_ids(self, operator, value):
        activities = self.env['sgi.process.activity'].search([('sale_team_ids', operator, value)])
        return [('id', 'in', activities.process_id.ids)]


class HrJobLine(models.Model):
    _inherit = 'hr.job'

    sgi_team_ids = fields.Many2many(
        'crm.team', string="Equipos de venta de sus personas", compute='_compute_sgi_team_ids',
        help="Equipos de venta de los que son miembros o líderes las personas "
             "del puesto. Si todas están en alguno, «Mi procedimiento» del "
             "puesto muestra solo lo general y lo de esos equipos.")

    def _compute_sgi_team_ids(self):
        for job in self:
            job.sgi_team_ids = job._sgi_mp_teams(for_employee=False)

    @api.model
    def _sgi_user_teams(self, users):
        """Equipos de venta donde estos usuarios son miembros o líderes."""
        if not users:
            return self.env['crm.team']
        Team = self.env['crm.team'].sudo()
        members = self.env['crm.team.member'].sudo().search([('user_id', 'in', users.ids)])
        return members.crm_team_id | Team.search([('user_id', 'in', users.ids)])

    def _sgi_mp_teams(self, for_employee=True):
        """Equipos que filtran «Mi procedimiento». Para una persona (contexto
        ``sgi_mp_employee_id``): los suyos. Para el puesto: los de todas sus
        personas, solo si cada una está en algún equipo (si alguna no, el
        puesto ve todo). Vacío = sin filtro."""
        Team = self.env['crm.team']
        if len(self) != 1 or not self.id:
            return Team
        emp_id = self.env.context.get('sgi_mp_employee_id') if for_employee else False
        if emp_id:
            emp = self.env['hr.employee'].sudo().browse(emp_id).exists()
            if emp and emp.sgi_mp_job_id == self:
                return self._sgi_user_teams(emp.user_id)
        employees = self.env['hr.employee'].sudo().search([('sgi_mp_job_id', '=', self.id)])
        teams = Team
        for emp in employees:
            mine = self._sgi_user_teams(emp.user_id)
            if not mine:
                return Team
            teams |= mine
        return teams


class HrEmployeeLine(models.Model):
    _inherit = 'hr.employee'

    sgi_team_ids = fields.Many2many(
        'crm.team', string="Equipos de venta", compute='_compute_sgi_team_ids',
        help="Equipos de venta de los que su usuario es miembro o líder (se "
             "capturan en Ventas). Filtran su «Mi procedimiento».")

    def _compute_sgi_team_ids(self):
        Job = self.env['hr.job']
        for emp in self:
            emp.sgi_team_ids = Job._sgi_user_teams(emp.sudo().user_id)


class CrmTeamMemberSgi(models.Model):
    """Entrar o salir de un equipo de ventas cambia «Mi procedimiento»."""
    _inherit = 'crm.team.member'

    def _sgi_touch(self):
        users = self.sudo().user_id
        if users:
            jobs = self.env['hr.employee'].sudo().search([('user_id', 'in', users.ids)]).sgi_mp_job_id
            self.env['hr.employee']._sgi_mp_touch_jobs(jobs)

    @api.model_create_multi
    def create(self, vals_list):
        members = super().create(vals_list)
        members._sgi_touch()
        return members

    def write(self, vals):
        self._sgi_touch()
        res = super().write(vals)
        self._sgi_touch()
        return res

    def unlink(self):
        self._sgi_touch()
        return super().unlink()


class CrmTeamSgi(models.Model):
    _inherit = 'crm.team'

    def write(self, vals):
        before = self.sudo().user_id
        res = super().write(vals)
        if 'user_id' in vals:
            users = before | self.sudo().user_id
            jobs = self.env['hr.employee'].sudo().search([('user_id', 'in', users.ids)]).sgi_mp_job_id
            self.env['hr.employee']._sgi_mp_touch_jobs(jobs)
        return res


class SgiExecStatLine(models.Model):
    _inherit = 'sgi.activity.exec.stat'

    department_id = fields.Many2one(
        related='job_id.department_id', string="Departamento", store=True, index=True,
        help="Departamento del puesto de quien ejecutó.")
    sale_team_ids = fields.Many2many(
        'crm.team', 'sgi_exec_stat_crm_team_rel', 'stat_id', 'team_id',
        string="Aplica a (equipo de ventas)", compute='_compute_sale_team_ids', store=True,
        help="Equipos de ventas a los que aplica la actividad. Se calcula solo.")
    team_filter_id = fields.Many2one(
        related='activity_id.team_filter_id', string="Equipo (con las generales)",
        help="Filtro por equipo de ventas que incluye las actividades generales.")

    @api.depends('activity_id.sale_team_ids')
    def _compute_sale_team_ids(self):
        for stat in self:
            stat.sale_team_ids = stat.activity_id.sale_team_ids


class SgiIndicatorLine(models.Model):
    _inherit = 'sgi.indicator'

    measure_split = fields.Selection([
        ('none', "Solo el total"),
        ('team', "Por equipo de ventas"),
        ('market', "Por mercado (nacional / exportación)"),
    ], string="Desglose", default='none', required=True, tracking=True,
        help="Además del total, una medición por equipo de ventas o por "
             "mercado (país del cliente contra el de la compañía). Solo con "
             "fórmula configurable y registros que lleguen al equipo o al "
             "cliente.")
    measure_team_ids = fields.Many2many(
        'crm.team', 'sgi_indicator_crm_team_rel', 'indicator_id', 'team_id',
        string="Equipos a desglosar",
        help="Vacío = los equipos de venta de la compañía del indicador.")
    split_support = fields.Char(
        string="Desglose posible", compute='_compute_split_support')

    @api.depends('measure_split', 'calc_mode', 'term_ids.model_id')
    def _compute_split_support(self):
        for indicator in self:
            split = indicator.measure_split if indicator.measure_split != 'none' else 'team'
            problem = indicator._sgi_split_problem(split)
            if problem:
                indicator.split_support = problem
            else:
                paths = {term._sgi_split_path(split) for term in indicator.term_ids}
                indicator.split_support = "Se separa por %s." % ", ".join(sorted(paths))

    def _sgi_split_problem(self, split=None):
        """Por qué no se puede desglosar, o '' si se puede."""
        self.ensure_one()
        split = split or self.measure_split
        if self.calc_mode != 'configurable':
            return "Solo los indicadores con fórmula configurable se desglosan."
        if not self.term_ids:
            return "Captura la fórmula (pestaña Fórmula) antes de desglosar."
        missing = self.term_ids.filtered(lambda t: not t._sgi_split_path(split))
        if missing:
            what = "equipo de ventas" if split == 'team' else "cliente con país"
            return "El modelo %s no llega a %s." % (
                ", ".join(sorted(set(missing.mapped('model_id.name')))), what)
        return ''

    @api.constrains('measure_split', 'calc_mode', 'term_ids')
    def _check_measure_split(self):
        for indicator in self.filtered(lambda i: i.measure_split != 'none'):
            problem = indicator._sgi_split_problem()
            if problem:
                raise ValidationError("No se puede desglosar %s: %s" % (indicator.name, problem))

    def _sgi_split_keys(self):
        """[(vals de la fila, valor para el contexto)] de cada renglón."""
        self.ensure_one()
        if self.measure_split == 'team':
            teams = self.measure_team_ids or self.env['crm.team'].search(
                ['|', ('company_id', '=', False), ('company_id', '=', self._sgi_kpi_company().id)])
            return [({'team_id': team.id}, ('team', team.id)) for team in teams]
        if self.measure_split == 'market':
            return [({'market': code}, ('market', code)) for code, _label in _MARKETS]
        return []

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
        """El total como siempre y, si se desglosa, un renglón por equipo o
        por mercado."""
        vals = super()._sgi_measure_vals(date_from, date_to)
        if self.measure_split == 'none' or self.calc_mode != 'configurable' or self._sgi_split_problem():
            return vals
        rows = [(5, 0, 0)]
        for row_vals, key in self._sgi_split_keys():
            detail = self.with_context(sgi_split=key)._detail_configurable(date_from, date_to)
            value = detail.get('value')
            ids = detail.get('ids') or []
            rows.append((0, 0, dict(row_vals, **{
                'value': value or 0.0,
                'numerator': detail.get('numerator'),
                'denominator': detail.get('denominator'),
                'sample_size': len(ids),
                'state': 'sin_dato' if value is None else 'capturado',
                'detail_model': detail.get('model') or False,
                'detail_ids': ",".join(str(i) for i in ids) if ids else False,
            })))
        vals['split_ids'] = rows
        return vals


class SgiIndicatorTermLine(models.Model):
    _inherit = 'sgi.indicator.term'

    def _sgi_split_path(self, split):
        """Camino del modelo del término al equipo de ventas (``team``) o al
        país del cliente (``market``); None si no hay."""
        self.ensure_one()
        model = self.model_id.model
        if not model or model not in self.env:
            return None
        if split == 'team':
            return self._sgi_first_path(model, _TEAM_PATHS, 'crm.team')
        return self._sgi_first_path(model, _COUNTRY_PATHS, 'res.country')

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

    def _sgi_split_domain(self, key):
        """Dominio del renglón del desglose sobre los registros del término."""
        kind, value = key
        path = self._sgi_split_path(kind)
        if not path:
            return [('id', '=', 0)]
        if kind == 'team':
            return [(path, '=', value)]
        country = self.indicator_id._sgi_kpi_company().country_id
        if value == 'sin_pais':
            return [(path, '=', False)]
        if value == 'nacional':
            return [(path, '=', country.id)]
        return [(path, '!=', False), (path, '!=', country.id)]

    def _sgi_records(self, date_from, date_to):
        records = super()._sgi_records(date_from, date_to)
        key = self.env.context.get('sgi_split')
        if not key:
            return records
        return records.filtered_domain(self._sgi_split_domain(key))


class SgiIndicatorMeasureLine(models.Model):
    _inherit = 'sgi.indicator.measure'

    split_ids = fields.One2many(
        'sgi.indicator.measure.split', 'measure_id', string="Desglose")


class SgiIndicatorMeasureSplit(models.Model):
    """Un renglón del desglose (equipo de ventas o mercado) dentro de la
    medición del periodo. La oficial (NC, semáforo del proceso, tablero, RxD)
    sigue siendo el total."""
    _name = 'sgi.indicator.measure.split'
    _description = "Desglose de medición de indicador (equipo o mercado)"
    _order = 'measure_id, market, team_id, id'

    measure_id = fields.Many2one(
        'sgi.indicator.measure', string="Medición", required=True, ondelete='cascade', index=True)
    indicator_id = fields.Many2one(
        related='measure_id.indicator_id', string="Indicador", store=True, index=True)
    period_date = fields.Date(related='measure_id.period_date', string="Periodo", store=True)
    team_id = fields.Many2one('crm.team', string="Equipo de ventas", ondelete='cascade', index=True)
    market = fields.Selection(_MARKETS, string="Mercado")
    label = fields.Char(string="Renglón", compute='_compute_label', store=True)
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

    @api.depends('team_id', 'market')
    def _compute_label(self):
        markets = dict(_MARKETS)
        for row in self:
            row.label = row.team_id.name if row.team_id else markets.get(row.market, '')

    @api.depends('value', 'state', 'period_date', 'indicator_id.direction',
                 'indicator_id.target_objective', 'indicator_id.target_acceptable',
                 'indicator_id.range_min', 'indicator_id.range_max',
                 'indicator_id.range_tolerance',
                 'indicator_id.step_ids.objective', 'indicator_id.step_ids.acceptable',
                 'indicator_id.step_ids.date_from')
    def _compute_semaphore(self):
        for row in self:
            if row.state == 'sin_dato' or not row.indicator_id:
                row.semaphore = False
            else:
                row.semaphore = row.indicator_id._sgi_semaphore_on(row.value, row.period_date)

    def action_view_evidence(self):
        self.ensure_one()
        ids = [int(i) for i in (self.detail_ids or '').split(',') if i.strip().isdigit()]
        if not self.detail_model or self.detail_model not in self.env:
            raise UserError("Este renglón no tiene registros de detalle.")
        return {
            'type': 'ir.actions.act_window',
            'name': "%s — %s" % (self.indicator_id.name, self.label),
            'res_model': self.detail_model,
            'view_mode': 'list,form',
            'domain': [('id', 'in', ids)],
        }
