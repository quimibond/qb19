# -*- coding: utf-8 -*-
"""Modo «fórmula configurable» (2026-09-24).

Un indicador en modo ``configurable`` se calcula con dos términos capturados
en su ficha (``sgi.indicator.term``): numerador y denominador. Cada término
dice de qué modelo sale, con qué filtro (dominio), sobre qué campo de fecha se
recorta la ventana, cómo se agrega (contar, sumar un campo o sumar su valor
absoluto), por qué factor se multiplica (−1 para invertir signo, 0.001 para
pasar a toneladas) y qué ventana usa (el periodo, 3 o 12 meses móviles, o
acumulado hasta el cierre). La medición guarda numerador, denominador y los
registros del numerador, igual que los modos con detalle (I-1).

55.0.0 (2026-09-25), para que 28 indicadores capturados a mano pasen a fórmula:

- **Fechas relativas** en el filtro: ``'{cierre}'`` (fin del periodo),
  ``'{inicio}'``, ``'{hoy}'``, ``'{bloqueo}'`` (fecha de bloqueo contable de la
  compañía de los KPI: la mayor entre cierre fiscal y bloqueo duro; sin
  bloqueo, el cierre del periodo) y desplazamientos ``{cierre-30d}`` (días),
  ``{cierre-2dh}`` (días hábiles, calendario del SGI) y ``{cierre-48h}`` (horas).
- **Comparar dos fechas del mismo registro**: agregación «Contar donde B − A
  cumple» (unidad días, horas, días hábiles, «B a más tardar el día N del mes
  siguiente a A» o «B en el mismo mes que A») y «Promedio de B − A».
- **Solo conteo**: un indicador sin denominador vale lo que su numerador; sin
  registros vale 0, no «sin dato».
- **Varios términos con el mismo papel se suman** (un término con factor −1
  resta): EBITDA = ingresos − costo − gastos en tres numeradores.

Reglas:
- El dominio se valida con ``safe_eval`` (nunca ``eval``) y con una búsqueda
  de prueba al guardar; los campos tienen que existir en el modelo.
- Solo el administrador del SGI edita términos (``ir.model.access``).
- Cualquier cambio de fórmula regresa el indicador a «prueba» y queda en su
  chatter, con el antes y el después.
- Un indicador que sigue en un modo de código pero ya tiene términos corre
  la fórmula **en paralelo**: cada medición nueva guarda también el valor de
  la fórmula (``parallel_value``) para compararla un mes antes de migrar.
"""
import re
from datetime import datetime, timedelta

from dateutil.relativedelta import relativedelta
from markupsafe import Markup

from odoo import api, fields, models
from odoo.exceptions import ValidationError
from odoo.tools.safe_eval import safe_eval

from .sgi_calendar import sgi_add_business_days, sgi_business_days

WINDOWS = [
    ('period', "El periodo"),
    ('3m', "3 meses móviles"),
    ('12m', "12 meses móviles"),
    ('to_date', "Acumulado al cierre"),
]
AGGREGATIONS = [
    ('count', "Contar registros"),
    ('sum', "Sumar un campo"),
    ('sum_abs', "Sumar el valor absoluto de un campo"),
    ('count_delta', "Contar donde B − A cumple"),
    ('avg_delta', "Promedio de B − A"),
]
DELTA_UNITS = [
    ('days', "días"),
    ('hours', "horas"),
    ('business_days', "días hábiles"),
    ('next_month_day', "B a más tardar el día N del mes siguiente a A"),
    ('same_month', "B en el mismo mes que A"),
]
DELTA_OPS = [('<=', "≤"), ('<', "<"), ('>=', "≥"), ('>', ">"), ('=', "=")]
_TRACKED = ('model_id', 'domain', 'date_field', 'aggregation', 'field_name', 'field_name_2',
            'delta_unit', 'delta_op', 'delta_value', 'factor', 'window')
# '{cierre}', '{cierre-30d}', '{cierre-2dh}', '{cierre+48h}', '{inicio}', '{hoy}', '{bloqueo}'
_PLACEHOLDER = re.compile(r"\{(cierre|inicio|hoy|bloqueo)(?:([+-]\d+)(dh|d|h))?\}")


class SgiIndicatorTerm(models.Model):
    _name = 'sgi.indicator.term'
    _description = "Término de la fórmula de un indicador SGI"
    _order = 'indicator_id, role'

    indicator_id = fields.Many2one('sgi.indicator', required=True, ondelete='cascade', index=True)
    role = fields.Selection([('numerator', "Numerador"), ('denominator', "Denominador")],
                            required=True, default='numerator')
    model_id = fields.Many2one('ir.model', string="Modelo", required=True, ondelete='cascade')
    model_name = fields.Char(related='model_id.model', string="Modelo técnico")
    domain = fields.Text(string="Filtro", default='[]', required=True,
                         help="Dominio de Odoo, p. ej. [('state', '=', 'done')].")
    date_field = fields.Char(string="Campo de fecha", default='create_date',
                             help="Campo de fecha o fecha-hora del modelo sobre el que se "
                                  "recorta la ventana. Vacío solo con ventana «Acumulado al "
                                  "cierre»: entonces cuenta todo lo que hay hoy (p. ej. las "
                                  "existencias).")
    aggregation = fields.Selection(AGGREGATIONS, string="Agregación", required=True, default='count')
    field_name = fields.Char(string="Campo a sumar / fecha A",
                             help="Campo numérico a sumar; en las agregaciones de fechas, la fecha A (inicio).")
    field_name_2 = fields.Char(string="Fecha B (fin)",
                               help="Segunda fecha del registro para las agregaciones «B − A».")
    delta_unit = fields.Selection(DELTA_UNITS, string="Unidad", default='days')
    delta_op = fields.Selection(DELTA_OPS, string="Condición", default='<=')
    delta_value = fields.Float(string="N", digits=(16, 2),
                               help="Días, horas o el día del mes siguiente, según la unidad.")
    factor = fields.Float(string="Factor", default=1.0, digits=(16, 6),
                          help="Multiplica el resultado: −1 invierte el signo, 0.001 pasa "
                               "kg a toneladas.")
    window = fields.Selection(WINDOWS, string="Ventana", required=True, default='period')

    # 55.0.0: varios términos por papel se suman (antes: uno por papel).

    # ---- fechas relativas --------------------------------------------------
    def _sgi_lock_date(self):
        """Fecha de bloqueo contable de la compañía de los KPI: la mayor entre
        el cierre fiscal y el bloqueo duro; sin ninguna, None."""
        company = self.indicator_id._sgi_kpi_company().sudo()
        dates = [d for d in (company.fiscalyear_lock_date, getattr(company, 'hard_lock_date', False)) if d]
        return max(dates) if dates else None

    def _sgi_resolve_placeholders(self, text, date_from, date_to):
        """Sustituye '{cierre-30d}' y compañía por la fecha ISO que toca."""
        env = self.env
        base = {
            'cierre': date_to, 'inicio': date_from, 'hoy': fields.Date.context_today(self),
            'bloqueo': self._sgi_lock_date() or date_to,
        }

        def repl(match):
            anchor, amount, unit = match.group(1), match.group(2), match.group(3)
            value = base[anchor]
            if amount:
                n = int(amount)
                if unit == 'h':
                    value = fields.Datetime.to_datetime(value) + timedelta(hours=n)
                    return fields.Datetime.to_string(value)
                if unit == 'dh':
                    value = sgi_add_business_days(env, value, n)
                else:
                    value = value + timedelta(days=n)
            return fields.Date.to_string(value)
        return _PLACEHOLDER.sub(repl, text or '[]')

    # ---- validación -----------------------------------------------------
    def _sgi_domain(self, date_from=None, date_to=None):
        self.ensure_one()
        if not date_to:
            today = fields.Date.context_today(self)
            date_from, date_to = today.replace(day=1), today
        domain = safe_eval(self._sgi_resolve_placeholders(self.domain, date_from or date_to, date_to))
        if not isinstance(domain, (list, tuple)):
            raise ValueError("no es una lista")
        return list(domain)

    @api.constrains(*_TRACKED)
    def _check_term(self):
        for term in self:
            model_name = term.model_id.model
            if model_name not in self.env:
                raise ValidationError("El modelo %s no está instalado." % model_name)
            Model = self.env[model_name]
            try:
                Model.sudo().search_count(term._sgi_domain(), limit=1)
            except Exception as exc:  # noqa: BLE001 - el mensaje va al usuario
                raise ValidationError("%s: filtro inválido para %s: %s" % (
                    term.indicator_id.code, model_name, exc))
            if not term.date_field and term.window != 'to_date':
                raise ValidationError("%s: sin campo de fecha la ventana tiene que ser "
                                      "«Acumulado al cierre»." % term.indicator_id.code)
            date_field = Model._fields.get(term.date_field or '')
            if term.date_field and (not date_field or date_field.type not in ('date', 'datetime')):
                raise ValidationError("%s: «%s» no es un campo de fecha de %s." % (
                    term.indicator_id.code, term.date_field, model_name))
            if term.aggregation in ('sum', 'sum_abs'):
                field = Model._fields.get(term.field_name or '')
                if not field or field.type not in ('float', 'integer', 'monetary'):
                    raise ValidationError("%s: «%s» no es un campo numérico de %s." % (
                        term.indicator_id.code, term.field_name, model_name))
            elif term.aggregation in ('count_delta', 'avg_delta'):
                for name in (term.field_name, term.field_name_2):
                    field = Model._fields.get(name or '')
                    if not field or field.type not in ('date', 'datetime'):
                        raise ValidationError("%s: «%s» no es un campo de fecha de %s (fechas A y B)." % (
                            term.indicator_id.code, name, model_name))
                if term.aggregation == 'avg_delta' and term.delta_unit not in ('days', 'hours', 'business_days'):
                    raise ValidationError("%s: el promedio de B − A va en días, horas o días hábiles." % (
                        term.indicator_id.code))
                if term.delta_unit == 'next_month_day' and not 1 <= int(term.delta_value or 0) <= 28:
                    raise ValidationError("%s: «día N del mes siguiente» necesita N entre 1 y 28." % (
                        term.indicator_id.code))
            if not term.factor:
                raise ValidationError("%s: el factor no puede ser cero." % term.indicator_id.code)

    # ---- cálculo --------------------------------------------------------
    def _sgi_window(self, date_from, date_to):
        """(desde, hasta) de la ventana del término; desde=None si acumulado."""
        self.ensure_one()
        if self.window == '3m':
            return date_to - relativedelta(months=3) + relativedelta(days=1), date_to
        if self.window == '12m':
            return date_to - relativedelta(months=12) + relativedelta(days=1), date_to
        if self.window == 'to_date':
            return None, date_to
        return date_from, date_to

    def _sgi_records(self, date_from, date_to):
        """Registros del término en su ventana (filtro + fecha + compañía)."""
        self.ensure_one()
        Model = self.env[self.model_id.model].sudo()
        start, end = self._sgi_window(date_from, date_to)
        domain = self._sgi_domain(date_from, date_to)
        if not self.date_field:
            pass  # acumulado sin fecha: todo lo que hay hoy
        elif Model._fields[self.date_field].type == 'datetime':
            dt_from, dt_to = self.indicator_id._sgi_dt_bounds(start or date_to, end)
            domain += [(self.date_field, '<', dt_to)]
            if start:
                domain += [(self.date_field, '>=', dt_from)]
        else:
            domain += [(self.date_field, '<=', end)]
            if start:
                domain += [(self.date_field, '>=', start)]
        company_field = Model._fields.get('company_id')
        if company_field and company_field.store:
            domain += [('company_id', '=', self.indicator_id._sgi_kpi_company().id)]
        # Las pólizas de cierre anual nunca cuentan (ver _sgi_closing_move_domain).
        if Model._name == 'account.move.line':
            domain += self.env['sgi.indicator']._sgi_closing_move_domain('move_id')
        elif Model._name == 'account.move':
            domain += self.env['sgi.indicator']._sgi_closing_move_domain()
        return Model.search(domain)

    def _sgi_delta(self, record):
        """B − A del registro en la unidad del término; None si falta una fecha.
        Para «día N del mes siguiente» y «mismo mes» devuelve True/False."""
        a, b = record[self.field_name], record[self.field_name_2]
        if not a or not b:
            return None
        if isinstance(a, datetime) and not isinstance(b, datetime):
            b = datetime.combine(b, datetime.min.time())
        elif isinstance(b, datetime) and not isinstance(a, datetime):
            a = datetime.combine(a, datetime.min.time())
        if self.delta_unit == 'same_month':
            return (a.year, a.month) == (b.year, b.month)
        if self.delta_unit == 'next_month_day':
            a_date = a.date() if isinstance(a, datetime) else a
            b_date = b.date() if isinstance(b, datetime) else b
            limit = (a_date.replace(day=1) + relativedelta(months=1)) + timedelta(days=int(self.delta_value) - 1)
            return b_date <= limit
        if self.delta_unit == 'business_days':
            a_dt = a if isinstance(a, datetime) else datetime.combine(a, datetime.min.time())
            b_dt = b if isinstance(b, datetime) else datetime.combine(b, datetime.min.time())
            return float(sgi_business_days(self.env, a_dt, b_dt)) if b_dt > a_dt else 0.0
        seconds = (b - a).total_seconds()
        return seconds / 3600.0 if self.delta_unit == 'hours' else seconds / 86400.0

    def _sgi_delta_ok(self, delta):
        if isinstance(delta, bool):
            return delta
        n = self.delta_value or 0.0
        return {'<=': delta <= n, '<': delta < n, '>=': delta >= n, '>': delta > n,
                '=': abs(delta - n) < 1e-9}[self.delta_op or '<=']

    def _sgi_matching(self, records):
        """Los registros que cuentan: todos, o los que cumplen «B − A»."""
        self.ensure_one()
        if self.aggregation != 'count_delta':
            return records
        return records.filtered(lambda r: (lambda d: d is not None and self._sgi_delta_ok(d))(self._sgi_delta(r)))

    def _sgi_value(self, records):
        """Valor del término; None solo en «Promedio de B − A» sin registros."""
        self.ensure_one()
        if self.aggregation == 'count':
            raw = float(len(records))
        elif self.aggregation == 'sum':
            raw = float(sum(records.mapped(self.field_name)))
        elif self.aggregation == 'sum_abs':
            raw = float(sum(abs(v) for v in records.mapped(self.field_name)))
        elif self.aggregation == 'count_delta':
            raw = float(len(self._sgi_matching(records)))
        else:  # avg_delta
            deltas = [d for d in (self._sgi_delta(r) for r in records) if d is not None]
            if not deltas:
                return None
            raw = sum(deltas) / len(deltas)
        return raw * self.factor

    # ---- trazabilidad -----------------------------------------------------
    def _sgi_describe(self):
        self.ensure_one()
        what = dict(AGGREGATIONS)[self.aggregation]
        if self.aggregation in ('sum', 'sum_abs'):
            what += " «%s»" % (self.field_name or '')
        elif self.aggregation in ('count_delta', 'avg_delta'):
            what += " (A «%s», B «%s»" % (self.field_name or '', self.field_name_2 or '')
            if self.delta_unit in ('same_month',):
                what += ", %s)" % dict(DELTA_UNITS)[self.delta_unit]
            elif self.delta_unit == 'next_month_day':
                what += ", día %d del mes siguiente)" % int(self.delta_value or 0)
            elif self.aggregation == 'count_delta':
                what += ", %s %s %s)" % (dict(DELTA_OPS)[self.delta_op or '<='], self.delta_value or 0,
                                         dict(DELTA_UNITS)[self.delta_unit or 'days'])
            else:
                what += ", en %s)" % dict(DELTA_UNITS)[self.delta_unit or 'days']
        text = "%s: %s de %s con filtro %s por «%s», ventana %s" % (
            dict(self._fields['role'].selection)[self.role], what, self.model_id.model,
            self.domain or '[]', self.date_field or 'sin fecha', dict(WINDOWS)[self.window])
        if self.factor != 1.0:
            text += ", factor %s" % self.factor
        return text

    def _sgi_log_change(self, before):
        """Chatter del indicador con antes/después; el indicador vuelve a prueba."""
        for term in self:
            body = Markup("Fórmula modificada.<br/>Antes: %s<br/>Ahora: %s") % (
                before.get(term.id, "(sin término)"), term._sgi_describe())
            term.indicator_id._sgi_formula_changed(body)

    @api.model_create_multi
    def create(self, vals_list):
        terms = super().create(vals_list)
        terms._sgi_log_change({})
        return terms

    def write(self, vals):
        tracked = [k for k in vals if k in _TRACKED]
        before = {t.id: t._sgi_describe() for t in self} if tracked else {}
        res = super().write(vals)
        if tracked:
            self._sgi_log_change(before)
        return res

    def unlink(self):
        for term in self:
            term.indicator_id._sgi_formula_changed(
                Markup("Fórmula modificada.<br/>Se quitó: %s") % term._sgi_describe())
        return super().unlink()


class SgiIndicatorFormula(models.Model):
    _inherit = 'sgi.indicator'

    term_ids = fields.One2many('sgi.indicator.term', 'indicator_id', string="Términos de la fórmula")
    has_formula = fields.Boolean(compute='_compute_has_formula')
    formula_text = fields.Text(string="Fórmula configurada", compute='_compute_has_formula')
    # depends_context uid: la caché es una por transacción; sin esto el valor
    # calculado para un usuario se reutiliza para otro (with_user).
    can_edit_formula = fields.Boolean(compute='_compute_can_edit_formula', depends_context=('uid',))

    def _compute_can_edit_formula(self):
        allowed = self.env.user.has_group('quimibond_sgi.group_sgi_admin')
        for indicator in self:
            indicator.can_edit_formula = allowed

    @api.depends('term_ids', 'term_ids.model_id', 'term_ids.domain', 'term_ids.date_field',
                 'term_ids.aggregation', 'term_ids.field_name', 'term_ids.field_name_2',
                 'term_ids.delta_unit', 'term_ids.delta_op', 'term_ids.delta_value',
                 'term_ids.factor', 'term_ids.window')
    def _compute_has_formula(self):
        for indicator in self:
            terms = indicator._sgi_terms()
            # Con numerador basta: sin denominador el indicador es de solo conteo.
            indicator.has_formula = bool(terms[0])
            indicator.formula_text = "\n".join(t._sgi_describe() for t in indicator.term_ids)

    def _sgi_terms(self):
        """(numeradores, denominadores): varios términos por papel se suman."""
        self.ensure_one()
        num = self.term_ids.filtered(lambda t: t.role == 'numerator')
        den = self.term_ids.filtered(lambda t: t.role == 'denominator')
        return num, den

    def _sgi_formula_changed(self, body):
        """Todo cambio de fórmula queda en el chatter y regresa el indicador a
        prueba: hay que volver a revisar la lista de registros."""
        for indicator in self:
            vals = {'status': 'prueba'} if indicator.status == 'oficial' else {}
            if vals:
                indicator.write(vals)
                body += Markup("<br/>El indicador regresa a «prueba».")
            indicator.message_post(body=body)

    def _detail_configurable(self, date_from, date_to):
        nums, dens = self._sgi_terms()
        if not nums:
            return {'value': None}
        numerator, ids, model = 0.0, [], nums[0].model_id.model
        for term in nums:
            records = term._sgi_records(date_from, date_to)
            value = term._sgi_value(records)
            if value is None:  # promedio sin registros: sin dato
                return {'value': None, 'numerator': None, 'denominator': None, 'model': model, 'ids': []}
            numerator += value
            if term.model_id.model == model:
                ids += term._sgi_matching(records).ids
        pct = '%' in (self.uom or '')
        if not dens:
            # Solo conteo: el valor es el numerador; sin registros, 0.
            return {'value': round(numerator, 2), 'numerator': numerator, 'denominator': None,
                    'model': model, 'ids': ids}
        denominator = 0.0
        for term in dens:
            value = term._sgi_value(term._sgi_records(date_from, date_to))
            if value is None:
                return {'value': None, 'numerator': numerator, 'denominator': None, 'model': model, 'ids': ids}
            denominator += value
        value = None
        if denominator:
            value = round(numerator / denominator * (100.0 if pct else 1.0), 2)
        return {
            'value': value, 'numerator': numerator, 'denominator': denominator,
            'model': model, 'ids': ids,
        }

    def _calc_configurable(self, date_from, date_to):
        return self._detail_configurable(date_from, date_to)['value']

    def _note_configurable(self, date_from, date_to):
        if not self.has_formula:
            return "Capture al menos un numerador en la pestaña Fórmula (el denominador es opcional)."
        return ''

    # ---- recálculo bajo demanda (55.0.0) --------------------------------------
    def sgi_recalculate(self, period_date=None, save=False):
        """Calcula el indicador en un periodo con su modo actual, sin esperar al
        cron. Por MCP: ``call_model_method('sgi.indicator', 'sgi_recalculate',
        [ids], {'period_date': '2026-08-01', 'save': True})``.

        :param period_date: primer día del periodo (mes o semana); por omisión
            el último periodo cerrado.
        :param save: True escribe la medición (la crea si no existe; nunca toca
            una validada, que es evidencia).
        :return: por indicador, {code, period_date, value, state, numerator,
            denominator, sample_size, note, detail_model, detail_ids, measure_id}.
        """
        results = []
        Measure = self.env['sgi.indicator.measure']
        for indicator in self:
            period = fields.Date.to_date(period_date) if period_date else indicator._sgi_default_period()
            date_from, date_to = indicator._sgi_period_bounds(period)
            vals = indicator._sgi_measure_vals(date_from, date_to)
            result = {
                'code': indicator.code, 'period_date': fields.Date.to_string(period),
                'value': vals.get('value'), 'state': vals.get('state'),
                'numerator': vals.get('numerator'), 'denominator': vals.get('denominator'),
                'sample_size': vals.get('sample_size'), 'note': vals.get('note') or '',
                'detail_model': vals.get('detail_model') or '', 'detail_ids': vals.get('detail_ids') or '',
                'measure_id': False,
            }
            if save:
                measure = Measure.search([('indicator_id', '=', indicator.id),
                                          ('period_date', '=', period)], limit=1)
                if measure and measure.state == 'validado':
                    result['note'] = "La medición ya está validada (evidencia): no se tocó."
                else:
                    if measure:
                        measure.write(vals)
                    else:
                        measure = Measure.create(dict(vals, indicator_id=indicator.id, period_date=period))
                    indicator.message_post(body="Recalculado bajo demanda el periodo %s con el modo «%s»: %s." % (
                        period, indicator.calc_mode,
                        "sin dato calculable" if vals.get('state') == 'sin_dato' else vals.get('value')))
                result['measure_id'] = measure.id if measure else False
            results.append(result)
        return results

    def _sgi_default_period(self):
        """Primer día del último periodo cerrado (mes anterior o semana pasada)."""
        self.ensure_one()
        today = fields.Date.context_today(self)
        if self.frequency == 'weekly':
            monday = today - relativedelta(days=today.weekday())
            return monday - relativedelta(days=7)
        return today.replace(day=1) - relativedelta(months=1)

    def action_recalculate_now(self):
        """Botón «Recalcular ahora»: el último periodo cerrado, guardando."""
        self.sgi_recalculate(save=True)
        return True

    def _sgi_measure_vals(self, date_from, date_to):
        """Además del modo del indicador, la fórmula en paralelo si existe."""
        vals = super()._sgi_measure_vals(date_from, date_to)
        if self.calc_mode not in ('manual', 'configurable') and self.has_formula:
            detail = self._detail_configurable(date_from, date_to)
            vals.update({
                'parallel_value': detail.get('value'),
                'parallel_numerator': detail.get('numerator'),
                'parallel_denominator': detail.get('denominator'),
            })
        return vals


class SgiIndicatorMeasureFormula(models.Model):
    _inherit = 'sgi.indicator.measure'

    parallel_value = fields.Float(
        string="Valor de la fórmula", digits=(16, 2),
        help="Lo que daría la fórmula configurada del indicador en este "
             "periodo, mientras el indicador sigue en su modo de código. "
             "Cuando coincidan un mes, se migra.")
    parallel_numerator = fields.Float(string="Numerador (fórmula)", digits=(16, 2))
    parallel_denominator = fields.Float(string="Denominador (fórmula)", digits=(16, 2))
