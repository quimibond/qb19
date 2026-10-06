# -*- coding: utf-8 -*-
"""57.109.0: crear un indicador por fórmula sin escribir dominios.

Hasta aquí, los 87 términos de las 43 fórmulas de producción los capturó
Jose (por MCP) o el sistema: cada término pide modelo técnico, dominio
escrito a mano y nombre técnico del campo de fecha. El asistente
(``sgi.indicator.wizard``) pregunta:

1. ¿Qué quiere saber? «% de ___ que cumplen ___», «Cuántos ___» o «Suma
   de ___».
2. ¿De qué registros? Una fuente del catálogo con nombre de negocio
   (``sgi.indicator.source``: «Facturas de cliente publicadas», «Entregas
   validadas»…), que trae el modelo, el filtro base y la fecha.
3. ¿Cuáles cuentan? El filtro visual nativo de Odoo (``widget="domain"``),
   con el conteo de registros al lado.
4. Meta, aceptable, sentido y frecuencia.

Antes de crear, la vista previa calcula los últimos tres periodos con el
mismo motor de la fórmula (``_detail_configurable``) sobre registros en
memoria, y «Ver registros» abre los que cuenta el último. Crea el indicador
en «prueba» con sus términos; las fórmulas de fechas (B − A) y las fechas
relativas se siguen capturando en la pestaña Fórmula.
"""
from dateutil.relativedelta import relativedelta
from markupsafe import Markup

from odoo import Command, api, fields, models
from odoo.exceptions import AccessError, UserError
from odoo.tools.safe_eval import safe_eval

KINDS = [
    ('pct', "Qué porcentaje cumple algo"),
    ('count', "Cuántos hay"),
    ('sum', "Cuánto suman (un monto o una cantidad)"),
]
WINDOWS = [
    ('period', "Del periodo"),
    ('3m', "Últimos 3 meses"),
    ('12m', "Últimos 12 meses"),
]


def _literal_domain(text):
    domain = safe_eval(text or '[]')
    if not isinstance(domain, (list, tuple)):
        raise ValueError("no es una lista")
    return list(domain)


class SgiIndicatorSource(models.Model):
    """Fuente de datos con nombre de negocio para las fórmulas de indicadores."""
    _name = 'sgi.indicator.source'
    _description = "Fuente de datos de indicadores"
    _order = 'sequence, name'

    name = fields.Char(string="Fuente", required=True,
                       help="Nombre de negocio: «Facturas de cliente publicadas».")
    sequence = fields.Integer(default=10)
    active = fields.Boolean(default=True)
    model_name = fields.Char(string="Modelo técnico", required=True,
                             help="account.move, stock.picking…")
    domain = fields.Char(string="Filtro base", default='[]', required=True,
                         help="Qué registros son de esta fuente.")
    date_field = fields.Char(string="Fecha que cuenta", required=True, default='create_date')
    amount_field = fields.Char(string="Monto o cantidad por omisión",
                               help="Campo que se suma en «Cuánto suman».")
    available = fields.Boolean(string="Instalada", compute='_compute_available',
                               search='_search_available')
    description = fields.Char(string="Qué cuenta")

    def _compute_available(self):
        for source in self:
            source.available = source.model_name in self.env

    def _search_available(self, operator, value):
        ids = self.with_context(active_test=False).search([]).filtered(
            lambda s: s.model_name in self.env).ids
        positive = (operator in ('=', '==') and value) or (operator == '!=' and not value)
        return [('id', 'in' if positive else 'not in', ids)]


class SgiIndicatorWizard(models.TransientModel):
    """Nuevo indicador por fórmula, con preguntas y vista previa."""
    _name = 'sgi.indicator.wizard'
    _description = "Nuevo indicador por fórmula"

    name = fields.Char(string="¿Cómo se llama?", required=True)
    code = fields.Char(string="Clave", required=True,
                       help="Clave corta, p. ej. C2-09. Se propone con la clave del proceso.")
    process_id = fields.Many2one('sgi.process', string="¿De qué proceso?", required=True)
    responsible_id = fields.Many2one('res.users', string="¿Quién es el dueño?",
                                     default=lambda self: self.env.user, required=True)
    kind = fields.Selection(KINDS, string="¿Qué quiere saber?", required=True, default='pct')
    source_id = fields.Many2one('sgi.indicator.source', string="¿De qué registros?", required=True,
                                domain=[('available', '=', True)])
    source_model = fields.Char(related='source_id.model_name')
    filter_domain = fields.Char(
        string="¿Cuáles cuentan?", default='[]',
        help="En «Qué porcentaje cumple»: los que cumplen (el numerador). En los otros: solo si quiere "
             "contar o sumar algunos.")
    amount_field_id = fields.Many2one(
        'ir.model.fields', string="¿Qué se suma?",
        domain="[('model', '=', source_model), ('ttype', 'in', ('float', 'integer', 'monetary')), "
               "('store', '=', True)]")
    window = fields.Selection(WINDOWS, string="¿Qué periodo mira?", default='period', required=True)
    frequency = fields.Selection([('monthly', "Mensual"), ('weekly', "Semanal")],
                                 string="¿Cada cuánto se mide?", default='monthly', required=True)
    direction = fields.Selection([('higher_better', "Más alto es mejor"),
                                  ('lower_better', "Más bajo es mejor")],
                                 string="¿Qué es mejor?", default='higher_better', required=True)
    target_objective = fields.Float(string="Meta")
    target_acceptable = fields.Float(string="Aceptable")
    preview_html = fields.Html(string="Así habría salido", compute='_compute_preview', sanitize=False)
    formula_sentence = fields.Char(string="Fórmula en palabras", compute='_compute_preview')

    @api.onchange('process_id')
    def _onchange_process_id(self):
        if self.process_id and not self.code:
            self.code = self._sgi_next_code(self.process_id)

    @api.onchange('source_id')
    def _onchange_source_id(self):
        source = self.source_id
        if source and source.amount_field and source.model_name in self.env:
            self.amount_field_id = self.env['ir.model.fields']._get(source.model_name, source.amount_field)
        else:
            self.amount_field_id = False

    @api.model
    def _sgi_next_code(self, process):
        prefix = (process.code or 'IND').strip()
        codes = self.env['sgi.indicator'].with_context(active_test=False).search(
            [('code', '=like', '%s-%%' % prefix)]).mapped('code')
        numbers = [int(c.rsplit('-', 1)[1]) for c in codes if c.rsplit('-', 1)[1].isdigit()]
        return "%s-%02d" % (prefix, (max(numbers) if numbers else 0) + 1)

    # ------------------------------------------------------------------
    def _sgi_domains(self):
        """(dominio de la fuente, dominio de los que cuentan) como texto."""
        self.ensure_one()
        base = _literal_domain(self.source_id.domain)
        extra = _literal_domain(self.filter_domain)
        return repr(base), repr(base + extra)

    def _sgi_term_vals(self):
        """Términos de la fórmula (sin indicador)."""
        self.ensure_one()
        model = self.env['ir.model']._get(self.source_id.model_name)
        base, counted = self._sgi_domains()
        common = {'model_id': model.id, 'date_field': self.source_id.date_field, 'window': self.window}
        if self.kind == 'pct':
            return [dict(common, role='numerator', aggregation='count', domain=counted),
                    dict(common, role='denominator', aggregation='count', domain=base)]
        if self.kind == 'count':
            return [dict(common, role='numerator', aggregation='count', domain=counted)]
        if not self.amount_field_id:
            raise UserError("Elija qué se suma.")
        return [dict(common, role='numerator', aggregation='sum', domain=counted,
                     field_name=self.amount_field_id.name)]

    def _sgi_uom(self):
        if self.kind == 'pct':
            return '%'
        if self.kind == 'sum' and self.amount_field_id.ttype == 'monetary':
            return 'MXN'
        return 'registros' if self.kind == 'count' else ''

    def _sgi_preview_indicator(self):
        """Indicador y términos en memoria (no se guardan) para calcular."""
        return self.env['sgi.indicator'].new({
            'code': self.code or 'PREVIA', 'name': self.name or 'Vista previa',
            'frequency': self.frequency, 'direction': self.direction, 'uom': self._sgi_uom(),
            'calc_mode': 'configurable',
            'term_ids': [Command.create(vals) for vals in self._sgi_term_vals()],
        })

    def _sgi_last_periods(self, count=3):
        today = fields.Date.context_today(self)
        if self.frequency == 'weekly':
            monday = today - relativedelta(days=today.weekday())
            return [monday - relativedelta(days=7 * n) for n in range(count, 0, -1)]
        first = today.replace(day=1)
        return [first - relativedelta(months=n) for n in range(count, 0, -1)]

    @api.depends('kind', 'source_id', 'filter_domain', 'amount_field_id', 'window', 'frequency',
                 'direction', 'target_objective', 'target_acceptable', 'name')
    def _compute_preview(self):
        for wiz in self:
            wiz.formula_sentence = wiz._sgi_sentence() if wiz.source_id else False
            if not wiz.source_id or (wiz.kind == 'sum' and not wiz.amount_field_id):
                wiz.preview_html = False
                continue
            try:
                indicator = wiz._sgi_preview_indicator()
                rows = []
                for period in wiz._sgi_last_periods():
                    date_from, date_to = indicator._sgi_period_bounds(period)
                    detail = indicator._detail_configurable(date_from, date_to)
                    label = ("semana del %s" % period.strftime('%d/%m/%Y')) \
                        if wiz.frequency == 'weekly' else period.strftime('%m/%Y')
                    value = detail.get('value')
                    shown = "sin dato" if value is None else "%s %s" % (
                        "{:,.2f}".format(value).rstrip('0').rstrip('.'), wiz._sgi_uom())
                    extra = ""
                    if wiz.kind == 'pct' and detail.get('denominator') is not None:
                        extra = " (%d de %d)" % (detail.get('numerator') or 0, detail.get('denominator') or 0)
                    rows.append(Markup("<tr><td>%s</td><td><b>%s</b>%s</td></tr>") % (label, shown, extra))
                wiz.preview_html = Markup(
                    "<table class='table table-sm mb-0'><tbody>%s</tbody></table>") % Markup('').join(rows)
            except Exception as exc:  # noqa: BLE001 - un filtro a medio capturar no truena la pantalla
                wiz.preview_html = Markup("<p class='text-danger mb-0'>No se puede calcular todavía: %s</p>") % (
                    str(exc) or exc.__class__.__name__)

    def _sgi_sentence(self):
        source = self.source_id.name or ''
        filtered = self.filter_domain and self.filter_domain.strip() not in ('', '[]')
        window = dict(WINDOWS)[self.window].lower()
        if self.kind == 'pct':
            return "Porcentaje de %s que cumplen el filtro, %s" % (source.lower(), window)
        if self.kind == 'count':
            return "Cuántos %s%s, %s" % (source.lower(), " (con el filtro)" if filtered else "", window)
        return "Suma de %s de %s%s, %s" % (
            (self.amount_field_id.field_description or '').lower(), source.lower(),
            " (con el filtro)" if filtered else "", window)

    # ------------------------------------------------------------------
    def action_view_records(self):
        """Los registros que cuenta el último periodo cerrado."""
        self.ensure_one()
        indicator = self._sgi_preview_indicator()
        period = self._sgi_last_periods(1)[0]
        date_from, date_to = indicator._sgi_period_bounds(period)
        num = indicator.term_ids.filtered(lambda t: t.role == 'numerator')[:1]
        records = num._sgi_records(date_from, date_to)
        return {
            'type': 'ir.actions.act_window', 'name': "Lo que cuenta: %s" % (self.name or ''),
            'res_model': records._name, 'view_mode': 'list,form', 'target': 'new',
            'domain': [('id', 'in', records.ids)],
        }

    def action_create(self):
        """Crea el indicador en «prueba» con su fórmula y lo abre."""
        self.ensure_one()
        if not self.env.user.has_group('quimibond_sgi.group_sgi_manager'):
            raise AccessError("Solo el Jefe MAST crea indicadores.")
        if self.kind == 'pct' and (self.filter_domain or '[]').strip() == '[]':
            raise UserError("Diga cuáles cumplen («¿Cuáles cuentan?»): sin filtro el porcentaje siempre "
                            "es 100 %.")
        Indicator = self.env['sgi.indicator']
        if Indicator.with_context(active_test=False).search_count([('code', '=', self.code)]):
            raise UserError("Ya existe un indicador con la clave %s." % self.code)
        terms = self._sgi_term_vals()
        indicator = Indicator.create({
            'code': self.code, 'name': self.name, 'process_id': self.process_id.id,
            'responsible_id': self.responsible_id.id, 'frequency': self.frequency,
            'direction': self.direction, 'target_objective': self.target_objective,
            'target_acceptable': self.target_acceptable, 'uom': self._sgi_uom(),
            'calc_mode': 'configurable',
            'formula': self.formula_sentence,
            'source': "%s (%s)" % (self.source_id.name, self.source_id.description or self.source_id.model_name),
        })
        # Los términos los edita solo el administrador del SGI; aquí los crea
        # el asistente a nombre del Jefe MAST, con el indicador en «prueba».
        self.env['sgi.indicator.term'].sudo().create([dict(vals, indicator_id=indicator.id) for vals in terms])
        indicator.message_post(body="Creado con el asistente «Nuevo indicador»: %s." % self.formula_sentence)
        return {'type': 'ir.actions.act_window', 'res_model': 'sgi.indicator', 'res_id': indicator.id,
                'view_mode': 'form', 'target': 'current'}


class SgiActivityMeasureFriendly(models.Model):
    """57.109.0: la medición de la actividad sin nombres técnicos: la fecha y
    el usuario se eligen por su etiqueta y una vista previa dice cuántos
    registros cuenta y a quién se los atribuye."""
    _inherit = 'sgi.process.activity'

    measure_date_field_id = fields.Many2one(
        'ir.model.fields', string="Fecha que cuenta", compute='_compute_measure_field_ids',
        inverse='_inverse_measure_date_field_id', readonly=False,
        domain="[('model', '=', measure_model_name), ('ttype', 'in', ('date', 'datetime'))]",
        help="La fecha del registro que dice cuándo se hizo la actividad (Fecha efectiva, "
             "Fecha de factura…).")
    measure_user_field_id = fields.Many2one(
        'ir.model.fields', string="Quién la hizo", compute='_compute_measure_field_ids',
        inverse='_inverse_measure_user_field_id', readonly=False,
        domain="[('model', '=', measure_model_name), ('ttype', '=', 'many2one'), "
               "('relation', '=', 'res.users'), ('store', '=', True)]",
        help="El usuario del registro que hizo la actividad (Responsable, Validado por…). Evite "
             "«Última actualización por»: es el último que editó, no quien la hizo.")
    measure_preview_html = fields.Html(
        string="Lo que cuenta hoy", compute='_compute_measure_preview_html', sanitize=False)

    @api.depends('measure_model_name', 'measure_date_field', 'measure_user_field')
    def _compute_measure_field_ids(self):
        Fields = self.env['ir.model.fields'].sudo()
        for activity in self:
            model = activity.measure_model_name
            date = Fields._get(model, activity.measure_date_field) \
                if model and activity.measure_date_field and model in self.env \
                and activity.measure_date_field in self.env[model]._fields else Fields
            user = Fields._get(model, activity.measure_user_field) \
                if model and activity.measure_user_field and model in self.env \
                and activity.measure_user_field in self.env[model]._fields else Fields
            activity.measure_date_field_id = date
            activity.measure_user_field_id = user

    def _inverse_measure_date_field_id(self):
        for activity in self:
            name = activity.measure_date_field_id.name or 'create_date'
            if activity.measure_date_field != name:
                activity.measure_date_field = name

    def _inverse_measure_user_field_id(self):
        for activity in self:
            name = activity.measure_user_field_id.name or False
            if (activity.measure_user_field or False) != name:
                activity.measure_user_field = name

    @api.depends('measure_model_id', 'measure_domain', 'measure_date_field', 'measure_user_field',
                 'measure_method', 'measure_user_history')
    def _compute_measure_preview_html(self):
        for activity in self:
            activity.measure_preview_html = False
            if not activity.id or not hasattr(activity, '_sgi_evidence_source'):
                continue
            try:
                source = activity._sgi_evidence_source()
            except Exception:  # noqa: BLE001
                source = None
            if not source:
                if activity.measure_model_id and activity.measure_method in (False, 'odoo', 'entregable'):
                    activity.measure_preview_html = Markup(
                        "<p class='text-danger mb-0'>El filtro no se puede leer: revise sus condiciones.</p>")
                continue
            Model, domain, date_field = source
            since = fields.Datetime.now() - relativedelta(days=30)
            if Model._fields[date_field].type == 'date':
                since = since.date()
            window = domain + [(date_field, '>=', since)]
            count = Model.search_count(window)
            parts = [Markup("<b>%d</b> registros en los últimos 30 días.") % count]
            user_field = activity.measure_user_field
            field = Model._fields.get(user_field or '')
            # 57.111.0: «quien lo pasó a su estado» se lee del historial.
            if count and hasattr(activity, '_sgi_uses_history') and activity._sgi_uses_history(Model):
                per_user = {}
                for user, _day, n in activity._sgi_history_groups(Model, domain, date_field, since):
                    per_user[user] = per_user.get(user, 0) + n
                top = sorted(per_user.items(), key=lambda kv: -kv[1])[:3]
                who = ", ".join("%s (%d)" % (user.name or "sin usuario", n) for user, n in top)
                parts.append(Markup(" Según el historial, los pasaron a su estado: %s.") % who)
            elif count and field and field.type == 'many2one' and field.comodel_name == 'res.users' and field.store:
                groups = Model._read_group(window, [user_field], ['__count'], order='__count desc', limit=3)
                who = ", ".join("%s (%d)" % (user.name or "sin usuario", n) for user, n in groups)
                parts.append(Markup(" Se le atribuyen a: %s.") % who)
            elif count:
                parts.append(Markup(" Sin «Quién la hizo» no se sabe a quién atribuirlos."))
            activity.measure_preview_html = Markup('').join(parts)
