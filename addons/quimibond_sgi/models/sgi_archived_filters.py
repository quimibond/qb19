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

Entrega 1b (auditoría 2026-09):
  - documentos por revisar de procesos archivados;
  - D-03: el SGI es de UNA sola empresa (`sgi.config._sgi_company`, la
    principal salvo `quimibond_sgi.sgi_company_id`). Nada de otra empresa
    entra a Mis pendientes: en producción salían aprobaciones de Studio de
    pedidos de BDC BOSQUES (empresa 4) a usuarios de la empresa 1.
"""
from odoo import api, models

_CLOSED_STATES = ('cancel', 'canceled', 'cancelled')
# Estados en los que el paso que pedía la aprobación ya quedó atrás.
_PAST_STEP = {
    'action_confirm': ('draft', 'sent', 'to approve'),
}


class SgiConfigCompany(models.AbstractModel):
    _inherit = 'sgi.config'

    @api.model
    def _sgi_company(self):
        """D-03: la única empresa del SGI. Parámetro
        `quimibond_sgi.sgi_company_id`; por default, la empresa principal."""
        param = int(self.env['ir.config_parameter'].sudo().get_param(
            'quimibond_sgi.sgi_company_id', 0) or 0)
        company = self.env['res.company'].browse(param).exists() if param else None
        return company or self.env.ref('base.main_company', raise_if_not_found=False) \
            or self.env.company


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
        for kind in ('medicion', 'validacion'):
            if kind in records:
                records[kind] = records[kind].filtered(lambda m: alive(m.indicator_id.process_id))
        if 'solicitud' in records:
            records['solicitud'] = records['solicitud'].filtered(
                lambda a: a.request_id.category_id.active and self._sgi_role_alive(
                    a.request_id.category_id))
        if 'aprobacion' in records:
            records['aprobacion'] = self._sgi_open_studio_requests(records['aprobacion'])
        if 'documento' in records and 'sgi_process_id' in records['documento']._fields:
            records['documento'] = records['documento'].filtered(lambda d: alive(d.sgi_process_id))
        company = self.env['sgi.config']._sgi_company()
        for kind, recs in records.items():
            records[kind] = recs.filtered(lambda rec: self._sgi_in_company(kind, rec, company))
        return records

    @api.model
    def _sgi_in_company(self, kind, rec, company):
        """D-03: el renglón es de la empresa del SGI. Lo que no tiene empresa
        (documentos compartidos, catálogos) sí entra."""
        if kind == 'aprobacion':
            rule = rec.rule_id
            if not rule.model_name or rule.model_name not in self.env:
                return True
            target = self.env[rule.model_name].sudo().browse(rec.res_id).exists()
        elif kind == 'solicitud':
            target = rec.request_id
        elif kind == 'accion':
            target = rec.alert_id or rec
        elif kind in ('medicion', 'validacion'):
            target = rec.indicator_id
        elif kind == 'firma':
            # 56.36.0: la firma es de la empresa del registro que se firma.
            ref = rec.sign_request_id.reference_doc if 'reference_doc' in rec.sign_request_id._fields \
                else False
            target = ref.sudo().exists() if ref else False
        else:
            target = rec
        if not target or 'company_id' not in target._fields:
            return True
        record_company = target.company_id
        return not record_company or record_company == company

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
