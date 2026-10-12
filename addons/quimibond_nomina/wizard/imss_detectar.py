# -*- coding: utf-8 -*-
"""Detecta los movimientos afiliatorios a partir de lo que ya está en Odoo:

* **Altas / reingresos (08)**: contratos cuya fecha de inicio cae en el rango
  (la primera versión del contrato del trabajador), con el SDI declarado.
* **Bajas (02)**: empleados con fecha de salida en el rango; la causa sale del
  motivo de salida (``CAUSA_POR_MOTIVO``) y, si no se reconoce, queda vacía
  para que RH la escoja antes de generar el lote.
* **Modificaciones (07)**: versiones del contrato con fecha de versión en el
  rango cuyo SDI declarado (``sdi_imss``) difiere del de la versión anterior.
  Las modificaciones bimestrales por salario variable NO salen de aquí sino
  del cálculo (``imss_sdi_bimestre.py``), que ya crea su movimiento.

No crea dos veces el mismo movimiento (trabajador + tipo + fecha)."""
from odoo import _, api, fields, models

from ..models.imss import registro_patronal_plano


class ImssDetectar(models.TransientModel):
    _name = 'qb.nomina.imss.detectar'
    _description = "Detectar movimientos IMSS"

    fecha_desde = fields.Date(string="Desde", required=True,
                              default=lambda self: fields.Date.context_today(self).replace(day=1))
    fecha_hasta = fields.Date(string="Hasta", required=True, default=fields.Date.context_today)
    registro_patronal = fields.Char(string="Registro patronal", size=11,
                                    help="Vacío: todos. Sin guiones (C6759945101).")
    altas = fields.Boolean(string="Altas y reingresos", default=True)
    bajas = fields.Boolean(string="Bajas", default=True)
    modificaciones = fields.Boolean(string="Modificaciones de salario (cambios de SDI en el contrato)",
                                    default=True)
    company_id = fields.Many2one('res.company', default=lambda self: self.env.company, required=True)

    @api.onchange('registro_patronal')
    def _onchange_registro_patronal(self):
        for w in self:
            rp = registro_patronal_plano(w.registro_patronal)
            if rp != (w.registro_patronal or ''):
                w.registro_patronal = rp

    def _existe(self, employee, tipo, fecha):
        return bool(self.env['qb.nomina.imss.movimiento'].search_count(
            [('employee_id', '=', employee.id), ('tipo', '=', tipo), ('fecha', '=', fecha)]))

    def _en_registro(self, version):
        rp = registro_patronal_plano(self.registro_patronal)
        return not rp or version._qb_imss_registro_patronal() == rp

    def action_detectar(self):
        self.ensure_one()
        Mov = self.env['qb.nomina.imss.movimiento']
        Version = self.env['hr.version'].with_context(active_test=False)
        nuevos = Mov
        base = [('company_id', '=', self.company_id.id), ('employee_id', '!=', False)]

        if self.altas:
            versiones = Version.search(base + [('contract_date_start', '>=', self.fecha_desde),
                                               ('contract_date_start', '<=', self.fecha_hasta)],
                                       order='employee_id, date_version')
            vistos = set()
            for v in versiones:
                emp = v.employee_id
                if emp.id in vistos or not self._en_registro(v):
                    continue
                vistos.add(emp.id)
                if self._existe(emp, '08', v.contract_date_start):
                    continue
                nuevos |= Mov.create({
                    'employee_id': emp.id, 'version_id': v.id, 'company_id': self.company_id.id,
                    'registro_patronal': v._qb_imss_registro_patronal(), 'tipo': '08',
                    'fecha': v.contract_date_start, 'sdi': v._qb_imss_sdi(), 'origen': 'alta',
                })

        if self.bajas:
            empleados = self.env['hr.employee'].with_context(active_test=False).search([
                ('company_id', '=', self.company_id.id),
                ('departure_date', '>=', self.fecha_desde), ('departure_date', '<=', self.fecha_hasta)])
            for emp in empleados:
                v = emp.version_id
                if not v or not self._en_registro(v) or self._existe(emp, '02', emp.departure_date):
                    continue
                causa = Mov._causa_para_motivo(emp.departure_reason_id)
                nuevos |= Mov.create({
                    'employee_id': emp.id, 'version_id': v.id, 'company_id': self.company_id.id,
                    'registro_patronal': v._qb_imss_registro_patronal(), 'tipo': '02',
                    'fecha': emp.departure_date, 'causa_baja': causa or False, 'origen': 'baja',
                    'nota': False if causa else _("Motivo de salida «%s» sin causa IMSS: escoja una.")
                    % (emp.departure_reason_id.name or '—'),
                })

        if self.modificaciones:
            versiones = Version.search(base + [('date_version', '>=', self.fecha_desde),
                                               ('date_version', '<=', self.fecha_hasta)],
                                       order='employee_id, date_version')
            for v in versiones:
                if not self._en_registro(v) or v.date_version == v.contract_date_start:
                    continue
                anterior = Version.search([('employee_id', '=', v.employee_id.id),
                                           ('date_version', '<', v.date_version)],
                                          order='date_version desc', limit=1)
                if not anterior:
                    continue
                sdi, sdi_ant = v._qb_imss_sdi(), anterior._qb_imss_sdi()
                if not sdi or abs(sdi - sdi_ant) < 0.005 or self._existe(v.employee_id, '07', v.date_version):
                    continue
                nuevos |= Mov.create({
                    'employee_id': v.employee_id.id, 'version_id': v.id, 'company_id': self.company_id.id,
                    'registro_patronal': v._qb_imss_registro_patronal(), 'tipo': '07',
                    'fecha': v.date_version, 'sdi': sdi, 'origen': 'version',
                    'nota': _("SDI anterior %.2f") % sdi_ant,
                })

        return {
            'type': 'ir.actions.act_window', 'name': _("Movimientos IMSS por exportar"),
            'res_model': 'qb.nomina.imss.movimiento', 'view_mode': 'list,form',
            'domain': [('state', '=', 'borrador')],
            'context': {'search_default_por_exportar': 1, 'qb_nuevos': nuevos.ids},
        }
