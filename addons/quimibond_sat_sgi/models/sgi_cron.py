# -*- coding: utf-8 -*-
"""Pendientes del SAT en el motor de avisos del SGI (``sgi.cron``).

Un aviso por tipo de pendiente, colgado de la actividad del mapa de procesos
que lo resuelve y dirigido a quien ocupa el puesto responsable de esa
actividad (``responsible_job_ids``). Mismas reglas que los demás avisos del
SGI: clave estable por tipo (``_sgi_schedule``), se actualiza en vez de
duplicarse y la corrida cierra los que ya no aplican (``_sgi_sweep``).
"""
import logging
from datetime import timedelta

from odoo import api, fields, models

from odoo.addons.quimibond_sgi.models.sgi_guard import sgi_require_system

_logger = logging.getLogger(__name__)

# Cuántos renglones van en la nota del aviso (el resto, en la lista).
NOTE_ROWS = 15
# Días que se dejan pasar antes de pedir un CFDI recibido que no está en Odoo:
# la captura normal va unos días atrás.
FALTA_ODOO_GRACE_DAYS = 5

# Cada pendiente: clave del aviso, numeral de la actividad del SGI que lo
# resuelve, modelo y dominio de lo pendiente, título y qué hacer.
SAT_PENDINGS = [
    {
        'key': 'sat_cancelado_proveedor', 'number': 'S1.21', 'model': 'sat.compare.line',
        'domain': [('direction', '=', 'received'), ('issue', '=', 'cancelado_sat')],
        'title': "facturas de proveedor publicadas con su CFDI CANCELADO en el SAT",
        'todo': "Un CFDI cancelado no es deducible: pida al proveedor el que lo sustituye y concílielo, "
                "o revierta la factura.",
        'days': 0,
    },
    {
        'key': 'sat_69b', 'number': 'S1.21', 'model': 'sat.compare.line',
        'domain': [('lista_69b', 'in', ('presumed', 'definitive')), ('bucket', '=', 'ambos'),
                   ('estado_sat', '=', 'vigente'), ('state_odoo', '=', 'posted')],
        'title': "facturas publicadas de proveedores en la lista 69-B del SAT",
        'todo': "Sus CFDI no son deducibles salvo que se acredite que la operación fue real: "
                "revíselo con el contador.",
        'days': 3,
    },
    {
        'key': 'sat_falta_odoo', 'number': 'S1.21', 'model': 'sat.compare.line',
        'domain': [('direction', '=', 'received'), ('issue', '=', 'solo_sat')],
        'grace': True,
        'title': "CFDI de proveedores que no están en Odoo",
        'todo': "Registre la factura (en el CFDI: «Crear factura desde el XML») o concíliela con la que ya "
                "existe («Conciliar»).",
        'days': 3,
    },
    {
        'key': 'sat_diferencia_proveedor', 'number': 'S1.21', 'model': 'sat.compare.line',
        'domain': [('direction', '=', 'received'), ('issue', 'in', ('monto', 'moneda', 'borrador'))],
        'title': "facturas de proveedor que no cuadran con su CFDI (monto, moneda o en borrador)",
        'todo': "Corrija la captura o la liga, o publique la que está en borrador.",
        'days': 5,
    },
    {
        'key': 'sat_rep_proveedor', 'number': 'S1.25', 'model': 'sat.pago.compare',
        'domain': [('direction', '=', 'received'), ('issue', 'in', ('sin_complemento', 'complemento_duplicado'))],
        'title': "pagos a proveedores sin su complemento de pago (o con complementos de más)",
        'todo': "Pida al proveedor el complemento que falta, o que cancele el que timbró de más.",
        'days': 5,
    },
    {
        'key': 'sat_rep_emitido', 'number': 'S2.21', 'model': 'sat.pago.compare',
        'domain': [('direction', '=', 'issued'),
                   ('issue', 'in', ('sin_complemento', 'complemento_duplicado', 'complemento_sin_pago'))],
        'title': "cobros sin complemento de pago timbrado (o con complementos de más)",
        'todo': "Timbre el complemento del pago, cancele el que sobra en el SAT o registre el cobro en Odoo.",
        'days': 3,
    },
    {
        'key': 'sat_cancelar_emitida', 'number': 'S2.13', 'model': 'sat.compare.line',
        'domain': [('direction', '=', 'issued'), ('issue', 'in', ('cancelado_odoo', 'cancelado_sat'))],
        'title': "facturas emitidas canceladas en Odoo y vigentes en el SAT (o al revés)",
        'todo': "Cancele el CFDI en el SAT o la factura en Odoo, según corresponda.",
        'days': 3,
    },
]
SAT_PENDING_KEYS = [p['key'] for p in SAT_PENDINGS]


class SgiCronSat(models.AbstractModel):
    _inherit = 'sgi.cron'

    @api.model
    def _sat_pending_spec(self, key):
        return next((p for p in SAT_PENDINGS if p['key'] == key), None)

    @api.model
    def _sat_pending_domain(self, spec, company):
        domain = list(spec['domain']) + [('company_id', '=', company.id)]
        if spec.get('grace'):
            limit = fields.Date.context_today(self) - timedelta(days=FALTA_ODOO_GRACE_DAYS)
            domain.append(('fecha', '<=', limit))
        return domain

    @api.model
    def _sat_sgi_activity(self, number, company):
        """La actividad del mapa de procesos con ese numeral (o vacío)."""
        return self.env['sgi.process.activity'].sudo().search(
            [('number', '=', number), ('company_id', 'in', (company.id, False))], limit=1)

    @api.model
    def _sat_activity_user_id(self, activity):
        """Quien ocupa el puesto responsable de la actividad (el primero con
        usuario, por id); si nadie, el Jefe MAST."""
        employees = self.env['hr.employee'].sudo().search(
            [('job_id', 'in', activity.responsible_job_ids.ids), ('user_id', '!=', False)], order='id')
        users = employees.user_id.filtered('active')
        return users[:1].id or self._sgi_manager_user_id()

    @api.model
    def _sat_pending_note(self, spec, records):
        rows = []
        for rec in records[:NOTE_ROWS]:
            amount = getattr(rec, 'total_sat', None)
            if amount is None:
                amount = rec.total
            rows.append('%s · %s · %s · %s' % (
                rec.fecha or '', rec.counterparty_name or '', rec.move_name or '—',
                '{:,.2f} {}'.format(amount or 0.0, rec.moneda or '')))
        more = len(records) - NOTE_ROWS
        if more > 0:
            rows.append("… y %s más" % more)
        return "%s\n\n- %s\n\nÁbralo con «Ir»: la lista del SAT ya filtrada." % (spec['todo'], "\n- ".join(rows))

    @api.model
    def cron_sat_pendientes(self):
        """Un aviso por tipo de pendiente del SAT a quien ocupa el puesto de su
        actividad del SGI. Sin pendientes, el aviso se cierra solo."""
        sgi_require_system(self.env)
        self = self._sgi_new_run()
        company = self.env['sgi.config']._sgi_company()
        if not company or not company.sat_sync_enabled:
            return False
        today = fields.Date.context_today(self)

        def _process(spec):
            activity = self._sat_sgi_activity(spec['number'], company)
            if not activity:
                return
            records = self.env[spec['model']].sudo().search(
                self._sat_pending_domain(spec, company), order='fecha')
            if not records:
                return
            user_id = self._sat_activity_user_id(activity)
            self._sgi_schedule(
                activity, "SAT: %s %s" % (len(records), spec['title']),
                self._sat_pending_note(spec, records), user_id,
                date_deadline=today + timedelta(days=spec['days']), key=spec['key'])

        # Un savepoint por tipo: el que truena no tumba a los demás (como
        # _sgi_for_each, que espera registros y no estas especificaciones).
        failures = 0
        for spec in SAT_PENDINGS:
            try:
                with self.env.cr.savepoint():
                    _process(spec)
            except Exception:
                failures += 1
                _logger.exception("SGI cron (pendientes del SAT): falló %s; continúo.", spec['key'])
        self._sgi_sweep(SAT_PENDING_KEYS, "ya no hay pendientes del SAT de este tipo", failures)
        return True

    @api.model
    def _sat_pending_action(self, key):
        """La lista del SAT filtrada en lo que pide el aviso ``key``."""
        spec = self._sat_pending_spec(key)
        if not spec:
            return False
        company = self.env['sgi.config']._sgi_company()
        return {
            'type': 'ir.actions.act_window',
            'name': spec['title'][:1].upper() + spec['title'][1:],
            'res_model': spec['model'], 'view_mode': 'list,form',
            'domain': self._sat_pending_domain(spec, company),
            'target': 'current',
        }
