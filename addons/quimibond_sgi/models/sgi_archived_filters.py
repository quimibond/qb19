# -*- coding: utf-8 -*-
"""Mi procedimiento y Mis pendientes sin lo archivado ni lo cerrado (56.24.0).

- Mi procedimiento: además de las actividades archivadas (activity_active,
  56.15.0), fuera los roles de actividades de **procesos archivados** (los
  25 procesos viejos P-xxx / MP-xxx). Archivar o reactivar un proceso
  recalcula el procedimiento guardado de los empleados con rol en él.
- Mis pendientes:
  - aprobaciones de Studio ya decididas (hay registro de aprobado o
    rechazado para esa regla y ese documento), de reglas archivadas, de
    documentos borrados o cancelados, o cuyo paso ya pasó (el pedido ya se
    confirmó, la orden de compra ya se facturó); en producción eran 63
    solicitudes de agosto, casi todas de pedidos ya confirmados;
  - aprobaciones de rol del SGI cuya actividad o proceso está archivado;
  - solicitudes de Aprobaciones de categorías archivadas (las cerradas ya
    se excluían por estado);
  - acciones, NC y mediciones de procesos archivados.
"""
from odoo import api, models

_CLOSED_STATES = ('cancel', 'canceled', 'cancelled')
# Estados en los que el paso que pedía la aprobación ya quedó atrás.
_PAST_STEP = {
    'action_confirm': ('draft', 'sent', 'to approve'),
}


class HrJobArchivedFilter(models.Model):
    _inherit = 'hr.job'

    def _sgi_roles_domain(self):
        return super()._sgi_roles_domain() + [('activity_id.process_id.active', '=', True)]


class SgiProcessArchiveTouch(models.Model):
    _inherit = 'sgi.process'

    def write(self, vals):
        res = super().write(vals)
        if 'active' in vals:
            activities = self.env['sgi.process.activity'].sudo().with_context(active_test=False).search(
                [('process_id', 'in', self.ids)])
            roles = activities.with_context(active_test=False).role_ids
            self.env['hr.employee']._sgi_mp_touch_jobs(roles._sgi_mp_jobs())
        return res


class SgiMyPendingArchivedFilter(models.TransientModel):
    _inherit = 'sgi.my.pending'

    @staticmethod
    def _sgi_process_alive(process):
        return not process or process.active

    @api.model
    def _sgi_pending_records(self, users):
        records = super()._sgi_pending_records(users)
        alive = self._sgi_process_alive
        if 'accion' in records:
            records['accion'] = records['accion'].filtered(lambda l: alive(l.alert_id.sgi_process_id))
        if 'nc' in records and 'sgi_process_id' in records['nc']._fields:
            records['nc'] = records['nc'].filtered(lambda a: alive(a.sgi_process_id))
        if 'medicion' in records:
            records['medicion'] = records['medicion'].filtered(
                lambda m: alive(m.indicator_id.process_id))
        if 'solicitud' in records:
            records['solicitud'] = records['solicitud'].filtered(
                lambda a: a.request_id.category_id.active and self._sgi_role_alive(
                    a.request_id.category_id))
        if 'aprobacion' in records:
            records['aprobacion'] = self._sgi_open_studio_requests(records['aprobacion'])
        return records

    @api.model
    def _sgi_role_alive(self, holder):
        """Una regla o categoría ligada a un rol del SGI vale mientras la
        actividad y su proceso estén vivos; sin rol, vale."""
        role = holder.sgi_role_id if 'sgi_role_id' in holder._fields else False
        if not role:
            return True
        activity = role.sudo().activity_id
        return bool(activity.active and activity.process_id.active)

    @api.model
    def _sgi_open_studio_requests(self, requests):
        """Solo las aprobaciones de Studio que siguen esperando a alguien."""
        if not requests:
            return requests
        Entry = self.env['studio.approval.entry'].sudo() if 'studio.approval.entry' in self.env else None
        decided = set()
        if Entry is not None:
            for entry in Entry.search([('rule_id', 'in', requests.rule_id.ids),
                                       ('res_id', 'in', requests.mapped('res_id'))]):
                decided.add((entry.rule_id.id, entry.res_id))

        def still_open(req):
            rule = req.rule_id
            if not rule.active or (rule.id, req.res_id) in decided:
                return False
            if not self._sgi_role_alive(rule):
                return False
            if not rule.model_name or rule.model_name not in self.env:
                return False
            record = self.env[rule.model_name].sudo().browse(req.res_id).exists()
            if not record:
                return False
            state = record.state if 'state' in record._fields else None
            if state in _CLOSED_STATES:
                return False
            waiting = _PAST_STEP.get(rule.method)
            if waiting and state is not None and state not in waiting:
                return False  # el pedido ya se confirmó: el paso quedó atrás
            if rule.method == 'action_create_invoice' and 'invoice_status' in record._fields \
                    and record.invoice_status == 'invoiced':
                return False
            return True

        return requests.filtered(still_open)
