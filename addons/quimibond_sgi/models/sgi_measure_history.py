# -*- coding: utf-8 -*-
"""57.111.0: quién hizo la actividad según el historial del registro.

«write_uid» dice quién editó el registro al último, no quién lo hizo: en las
transferencias de producción OdooBot aparece en 598 de 874 (2026-10-06) y el
campo «Responsable» está vacío en 556. El seguimiento de Odoo
(mail.tracking.value) sí guarda quién pasó cada registro a su estado: con la
casilla «Quién lo hizo: quien lo pasó a su estado (historial)» la medición
atribuye cada registro a quien hizo el último cambio de su campo de estado
(state, stage_id, request_status…). Si un registro no tiene ese cambio en el
historial, cuenta el campo de usuario como antes.
"""
from datetime import datetime

from odoo import api, fields, models

from .sgi_process_procedure import SgiProcessActivity as _BaseActivity

# Campos de estado en el orden en que se buscan; el primero que exista en el
# modelo y lleve seguimiento es el que se lee del historial.
HISTORY_STATE_FIELDS = ('state', 'stage_id', 'request_status', 'sgi_state',
                        'sgi_supplier_state', 'sgi_ext_state')

HISTORY_HELP = (
    "Atribuye cada registro a quien lo pasó a su estado actual (lo guarda el "
    "historial de Odoo), no a quien lo editó al último. Úsela cuando el "
    "modelo no tiene un campo de «quién lo validó» (transferencias, órdenes "
    "de fabricación, no conformidades, solicitudes). Si un registro no tiene "
    "ese cambio en el historial, cuenta el campo de usuario.")


def sgi_history_field(Model):
    """Nombre del campo de estado con seguimiento de Model, o None."""
    for name in HISTORY_STATE_FIELDS:
        field = Model._fields.get(name)
        if field and field.store and getattr(field, 'tracking', False):
            return name
    return None


class SgiDeliverableHistory(models.Model):
    _inherit = 'sgi.deliverable'

    _SGI_MEASURE_KEYS = ('odoo_model_id', 'measure_domain', 'measure_date_field',
                         'measure_user_field', 'measure_user_history')

    measure_user_history = fields.Boolean(
        string="Quién lo hizo: quien lo pasó a su estado (historial)", help=HISTORY_HELP)

    def _sgi_measure_vals(self):
        vals = super()._sgi_measure_vals()
        vals['measure_user_history'] = self.measure_user_history
        return vals


class SgiActivityHistory(models.Model):
    _inherit = 'sgi.process.activity'

    _SGI_MEASURE_FIELDS = _BaseActivity._SGI_MEASURE_FIELDS | {
        'spec_complete', 'measure_user_history'}

    measure_user_history = fields.Boolean(
        string="Quién lo hizo: quien lo pasó a su estado (historial)", help=HISTORY_HELP)
    measure_history_field = fields.Char(
        string="Campo de estado del historial", compute='_compute_measure_history_field',
        help="El campo cuyo cambio dice quién hizo la actividad. Vacío: el modelo "
             "no guarda historial de su estado y la casilla no sirve.")

    @api.depends('measure_model_id')
    def _compute_measure_history_field(self):
        for act in self:
            model = act.measure_model_id.model
            act.measure_history_field = sgi_history_field(self.env[model]) \
                if model and model in self.env else False

    def _sgi_history_groups(self, Model, domain, date_field, since):
        """Las mismas tuplas (usuario, día, cuántos) que el read_group por
        campo de usuario, pero con el usuario que hizo el último cambio del
        campo de estado de cada registro (el modelo debe tener uno con
        historial: ver _sgi_uses_history)."""
        self.ensure_one()
        state_field = sgi_history_field(Model)
        records = Model.search(domain + [(date_field, '>=', since)])
        if not records:
            return []
        field = self.env['ir.model.fields'].sudo()._get(Model._name, state_field)
        messages = self.env['mail.message'].sudo().search_read(
            [('model', '=', Model._name), ('res_id', 'in', records.ids),
             ('tracking_value_ids.field_id', '=', field.id)],
            ['res_id', 'create_uid'], order='id desc')
        who = {}
        for msg in messages:
            if msg['res_id'] not in who:
                who[msg['res_id']] = msg['create_uid'] and msg['create_uid'][0]
        user_field = (self.measure_user_field or '').strip()
        fallback = user_field if user_field in Model._fields \
            and Model._fields[user_field].type == 'many2one' \
            and Model._fields[user_field].comodel_name == 'res.users' else None
        counts = {}
        for rec in records:
            user_id = who.get(rec.id)
            if user_id is None and fallback:
                user_id = rec[fallback].id
            day = rec[date_field]
            day = day.date() if isinstance(day, datetime) else day
            if not day:
                continue
            key = (user_id or False, day)
            counts[key] = counts.get(key, 0) + 1
        Users = self.env['res.users'].sudo()
        return [(Users.browse(user_id) if user_id else Users, day, count)
                for (user_id, day), count in counts.items()]

    def _sgi_uses_history(self, Model):
        return bool(self.measure_user_history and sgi_history_field(Model))

    def _sgi_executor_attributable(self, Model):
        return self._sgi_uses_history(Model) or super()._sgi_executor_attributable(Model)

    def _sgi_executor_groups(self, Model, domain, date_field, since):
        if self._sgi_uses_history(Model):
            return self._sgi_history_groups(Model, domain, date_field, since)
        return super()._sgi_executor_groups(Model, domain, date_field, since)

    def _sgi_weak_attribution(self):
        out = super()._sgi_weak_attribution()
        if self.measure_user_history and self.measure_history_field:
            out = [msg for msg in out if '«write_uid»' not in msg]
        return out

    def _sgi_spec_problems(self):
        problems = super()._sgi_spec_problems()
        if self.measure_user_history and self._sgi_measures_itself() \
                and self.measure_model_id and not self.measure_history_field:
            problems.append(('weak_attribution', "Marca «quien lo pasó a su estado», pero %s no "
                             "guarda historial de su estado: use un campo de usuario."
                             % self.measure_model_id.model))
        return problems
