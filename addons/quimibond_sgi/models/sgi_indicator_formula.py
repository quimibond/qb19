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
from dateutil.relativedelta import relativedelta
from markupsafe import Markup

from odoo import api, fields, models
from odoo.exceptions import ValidationError
from odoo.tools.safe_eval import safe_eval

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
]
_TRACKED = ('model_id', 'domain', 'date_field', 'aggregation', 'field_name', 'factor', 'window')


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
    date_field = fields.Char(string="Campo de fecha", required=True, default='create_date',
                             help="Campo de fecha o fecha-hora del modelo sobre el que se "
                                  "recorta la ventana.")
    aggregation = fields.Selection(AGGREGATIONS, string="Agregación", required=True, default='count')
    field_name = fields.Char(string="Campo a sumar")
    factor = fields.Float(string="Factor", default=1.0, digits=(16, 6),
                          help="Multiplica el resultado: −1 invierte el signo, 0.001 pasa "
                               "kg a toneladas.")
    window = fields.Selection(WINDOWS, string="Ventana", required=True, default='period')

    _role_uniq = models.Constraint(
        'unique(indicator_id, role)',
        "Un indicador tiene un solo numerador y un solo denominador.")

    # ---- validación -----------------------------------------------------
    def _sgi_domain(self):
        self.ensure_one()
        domain = safe_eval(self.domain or '[]')
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
            date_field = Model._fields.get(term.date_field)
            if not date_field or date_field.type not in ('date', 'datetime'):
                raise ValidationError("%s: «%s» no es un campo de fecha de %s." % (
                    term.indicator_id.code, term.date_field, model_name))
            if term.aggregation != 'count':
                field = Model._fields.get(term.field_name or '')
                if not field or field.type not in ('float', 'integer', 'monetary'):
                    raise ValidationError("%s: «%s» no es un campo numérico de %s." % (
                        term.indicator_id.code, term.field_name, model_name))
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
        domain = self._sgi_domain()
        if Model._fields[self.date_field].type == 'datetime':
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
        return Model.search(domain)

    def _sgi_value(self, records):
        self.ensure_one()
        if self.aggregation == 'count':
            raw = float(len(records))
        elif self.aggregation == 'sum':
            raw = float(sum(records.mapped(self.field_name)))
        else:
            raw = float(sum(abs(v) for v in records.mapped(self.field_name)))
        return raw * self.factor

    # ---- trazabilidad -----------------------------------------------------
    def _sgi_describe(self):
        self.ensure_one()
        what = dict(AGGREGATIONS)[self.aggregation]
        if self.aggregation != 'count':
            what += " «%s»" % (self.field_name or '')
        text = "%s: %s de %s con filtro %s por «%s», ventana %s" % (
            dict(self._fields['role'].selection)[self.role], what, self.model_id.model,
            self.domain or '[]', self.date_field, dict(WINDOWS)[self.window])
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

    term_ids = fields.One2many('sgi.indicator.term', 'indicator_id', string="Fórmula")
    has_formula = fields.Boolean(compute='_compute_has_formula')
    formula_text = fields.Text(string="Fórmula configurada", compute='_compute_has_formula')
    can_edit_formula = fields.Boolean(compute='_compute_can_edit_formula')

    def _compute_can_edit_formula(self):
        allowed = self.env.user.has_group('quimibond_sgi.group_sgi_admin')
        for indicator in self:
            indicator.can_edit_formula = allowed

    @api.depends('term_ids', 'term_ids.model_id', 'term_ids.domain', 'term_ids.date_field',
                 'term_ids.aggregation', 'term_ids.field_name', 'term_ids.factor',
                 'term_ids.window')
    def _compute_has_formula(self):
        for indicator in self:
            terms = indicator._sgi_terms()
            indicator.has_formula = bool(terms[0] and terms[1])
            indicator.formula_text = "\n".join(t._sgi_describe() for t in indicator.term_ids)

    def _sgi_terms(self):
        self.ensure_one()
        num = self.term_ids.filtered(lambda t: t.role == 'numerator')[:1]
        den = self.term_ids.filtered(lambda t: t.role == 'denominator')[:1]
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
        num, den = self._sgi_terms()
        if not num or not den:
            return {'value': None}
        num_records = num._sgi_records(date_from, date_to)
        numerator = num._sgi_value(num_records)
        denominator = den._sgi_value(den._sgi_records(date_from, date_to))
        pct = '%' in (self.uom or '')
        value = None
        if denominator:
            value = round(numerator / denominator * (100.0 if pct else 1.0), 2)
        return {
            'value': value, 'numerator': numerator, 'denominator': denominator,
            'model': num.model_id.model, 'ids': num_records.ids,
        }

    def _calc_configurable(self, date_from, date_to):
        return self._detail_configurable(date_from, date_to)['value']

    def _note_configurable(self, date_from, date_to):
        if not self.has_formula:
            return "Capture numerador y denominador en la pestaña Fórmula."
        return ''

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
