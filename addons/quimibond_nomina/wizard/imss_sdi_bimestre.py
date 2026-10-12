# -*- coding: utf-8 -*-
"""Modificación bimestral del salario base de cotización por percepciones
variables (LSS art. 30 fracción II), como la hace NOI cada dos meses.

Para cada trabajador con contrato vigente:

    SDI nuevo = min( SD × factor de integración + variables del bimestre / días cotizados ,  25 UMA )

* **SD**: sueldo del contrato entre los días de su periodicidad (semanal / 7,
  quincenal / 15, mensual / 30).
* **Factor de integración**: 1 + (días de aguinaldo + vacaciones × prima) / 365
  según el año de servicio en la fecha de aplicación, a cuatro decimales
  (``factor_integracion``); aguinaldo 16 días y prima 25 % como NOI
  (parámetros ``quimibond_nomina.sdi_dias_aguinaldo`` y ``..._prima_vacacional``).
* **Variables**: suma de las líneas de los recibos del bimestre cuyos códigos
  están en ``quimibond_nomina.sdi_variables_codigos`` (comisiones, bonos,
  logro de objetivos, prima dominical, ayuda de transporte). Horas extra y
  premios de asistencia / puntualidad dentro de los topes de la LSS no integran
  y no se incluyen; si en Quimibond deben integrar, se agregan al parámetro.
* **Días cotizados**: suma de la línea ``DIAS_IMSS`` de esos recibos; si no la
  hay, los días del bimestre dentro del contrato.
* Recibos: los del bimestre (``date_from`` y ``date_to`` dentro) que no estén
  cancelados. En producción serán validados o pagados; en las pilotos están en
  borrador y también cuentan.

El resultado se revisa en pantalla (NOI lo hace en Excel) y al aplicar se
escribe el SDI nuevo en el contrato y se crea el movimiento ``07`` con fecha
del primer día del bimestre siguiente, solo para quien cambia en un centavo
o más."""
from odoo import _, api, fields, models
from odoo.exceptions import UserError

from ..models.imss import (CODIGO_DIAS_COTIZADOS, CODIGOS_VARIABLES_DEFAULT, PARAM_CODIGOS_VARIABLES,
                           PARAM_DIAS_AGUINALDO, PARAM_PRIMA_VACACIONAL, PARAM_UMA_DIARIA, UMA_TOPE_VECES,
                           anio_de_servicio, bimestre_rango, factor_integracion, registro_patronal_plano)


class ImssSdiBimestre(models.TransientModel):
    _name = 'qb.nomina.imss.sdi.bimestre'
    _description = "SDI del bimestre (salario variable)"

    anio = fields.Integer(string="Año", required=True, default=lambda self: fields.Date.context_today(self).year)
    bimestre = fields.Selection([('1', "1.º (enero-febrero)"), ('2', "2.º (marzo-abril)"), ('3', "3.º (mayo-junio)"),
                                 ('4', "4.º (julio-agosto)"), ('5', "5.º (septiembre-octubre)"),
                                 ('6', "6.º (noviembre-diciembre)")], string="Bimestre", required=True,
                                default=lambda self: str(((fields.Date.context_today(self).month - 1) // 2) or 6))
    registro_patronal = fields.Char(string="Registro patronal", size=11, help="Vacío: todos.")
    company_id = fields.Many2one('res.company', default=lambda self: self.env.company, required=True)
    state = fields.Selection([('nuevo', "Nuevo"), ('calculado', "Calculado")], default='nuevo')
    fecha_aplica = fields.Date(string="Fecha de aplicación", readonly=True)
    tope = fields.Float(string="Tope (25 UMA)", digits=(16, 2), readonly=True)
    dias_aguinaldo = fields.Integer(string="Días de aguinaldo", readonly=True)
    linea_ids = fields.One2many('qb.nomina.imss.sdi.bimestre.linea', 'wizard_id', string="Trabajadores")
    resumen = fields.Char(readonly=True)

    @api.onchange('registro_patronal')
    def _onchange_registro_patronal(self):
        for w in self:
            rp = registro_patronal_plano(w.registro_patronal)
            if rp != (w.registro_patronal or ''):
                w.registro_patronal = rp

    # --- parámetros --------------------------------------------------
    def _param(self, clave, default):
        valor = self.env['ir.config_parameter'].sudo().get_param(clave)
        return valor if valor not in (None, False, '') else default

    def _uma_diaria(self, fecha):
        try:
            uma = self.env['hr.rule.parameter']._get_parameter_from_code('l10n_mx_uma', fecha)
            if isinstance(uma, dict) and uma.get('daily'):
                return float(uma['daily'])
            if isinstance(uma, (int, float)) and uma:
                return float(uma)
        except Exception:  # noqa: BLE001 — sin parámetro de la localización se usa el propio
            pass
        valor = self._param(PARAM_UMA_DIARIA, '')
        if not valor:
            raise UserError(_("No encuentro la UMA diaria: cargue el parámetro de nómina l10n_mx_uma "
                              "o ponga %s en Parámetros del sistema.") % PARAM_UMA_DIARIA)
        return float(valor)

    def _codigos_variables(self):
        return {c.strip().upper() for c in self._param(PARAM_CODIGOS_VARIABLES, CODIGOS_VARIABLES_DEFAULT).split(',')
                if c.strip()}

    # --- cálculo -----------------------------------------------------
    def action_calcular(self):
        self.ensure_one()
        inicio, fin, aplica = bimestre_rango(self.anio, self.bimestre)
        uma = self._uma_diaria(aplica)
        tope = round(UMA_TOPE_VECES * uma, 2)
        dias_aguinaldo = int(float(self._param(PARAM_DIAS_AGUINALDO, '16')))
        prima = float(self._param(PARAM_PRIMA_VACACIONAL, '0.25'))
        codigos = self._codigos_variables()
        rp_filtro = registro_patronal_plano(self.registro_patronal)

        self.linea_ids.unlink()
        Linea = self.env['qb.nomina.imss.sdi.bimestre.linea']
        empleados = self.env['hr.employee'].search([('company_id', '=', self.company_id.id)])
        lineas = []
        for emp in empleados:
            v = emp.version_id
            if not v or not v.contract_date_start or v.contract_date_start > fin:
                continue
            if v.contract_date_end and v.contract_date_end < inicio:
                continue
            if rp_filtro and v._qb_imss_registro_patronal() != rp_filtro:
                continue
            recibos = self.env['hr.payslip'].search([
                ('employee_id', '=', emp.id), ('state', '!=', 'cancel'),
                ('date_from', '>=', inicio), ('date_to', '<=', fin)])
            lineas_recibo = recibos.mapped('line_ids')
            variables = sum(l.total for l in lineas_recibo if (l.code or '').upper() in codigos)
            dias = sum(l.total for l in lineas_recibo if l.code == CODIGO_DIAS_COTIZADOS)
            if not dias:
                desde = max(inicio, v.contract_date_start)
                hasta = min(fin, v.contract_date_end or fin)
                dias = max(0, (hasta - desde).days + 1)
            sd = v._qb_imss_salario_diario()
            factor = factor_integracion(anio_de_servicio(v.contract_date_start, aplica), dias_aguinaldo, prima)
            sdv = round(variables / dias, 4) if dias and variables else 0.0
            sdi_calc = round(sd * factor + sdv, 2)
            sdi_nuevo = min(sdi_calc, tope)
            sdi_actual = v._qb_imss_sdi()
            dif = round(sdi_nuevo - sdi_actual, 2)
            lineas.append({
                'wizard_id': self.id, 'employee_id': emp.id, 'version_id': v.id,
                'registro_patronal': v._qb_imss_registro_patronal(), 'sd': sd, 'factor': factor,
                'variables': variables, 'dias': dias, 'sdv': sdv, 'sdi_calculado': sdi_calc,
                'sdi_nuevo': sdi_nuevo, 'sdi_actual': sdi_actual, 'diferencia': dif,
                'topado': sdi_calc > tope, 'aplicar': abs(dif) >= 0.01, 'recibos': len(recibos),
            })
        Linea.create(lineas)
        cambian = sum(1 for l in lineas if l['aplicar'])
        self.write({
            'state': 'calculado', 'fecha_aplica': aplica, 'tope': tope, 'dias_aguinaldo': dias_aguinaldo,
            'resumen': _("%(n)d trabajadores con contrato en el bimestre %(i)s a %(f)s; %(c)d cambian de SDI "
                         "(aplica el %(a)s). Códigos variables: %(cod)s.") % {
                'n': len(lineas), 'i': inicio, 'f': fin, 'c': cambian, 'a': aplica,
                'cod': ', '.join(sorted(codigos))},
        })
        return {'type': 'ir.actions.act_window', 'res_model': self._name, 'res_id': self.id,
                'view_mode': 'form', 'target': 'new'}

    def action_aplicar(self):
        """Escribe el SDI nuevo en el contrato y crea el movimiento 07 de quien cambia."""
        self.ensure_one()
        if self.state != 'calculado':
            raise UserError(_("Primero calcule el bimestre."))
        Mov = self.env['qb.nomina.imss.movimiento']
        creados = Mov
        for linea in self.linea_ids.filtered('aplicar'):
            if abs(linea.diferencia) < 0.01:
                continue
            existente = Mov.search([('employee_id', '=', linea.employee_id.id), ('tipo', '=', '07'),
                                    ('fecha', '=', self.fecha_aplica)], limit=1)
            if existente and existente.state == 'exportado':
                raise UserError(_("%s ya tiene una modificación exportada con fecha %s.")
                                % (linea.employee_id.name, self.fecha_aplica))
            linea.version_id._qb_imss_escribir_sdi(linea.sdi_nuevo)
            vals = {
                'employee_id': linea.employee_id.id, 'version_id': linea.version_id.id,
                'company_id': self.company_id.id, 'registro_patronal': linea.registro_patronal,
                'tipo': '07', 'fecha': self.fecha_aplica, 'sdi': linea.sdi_nuevo, 'origen': 'bimestre',
                'nota': _("Bimestre %(b)s/%(a)s: SD %(sd).2f × %(f).4f + variables %(v).2f / %(d).0f días"
                          " = %(c).2f%(t)s; antes %(ant).2f") % {
                    'b': self.bimestre, 'a': self.anio, 'sd': linea.sd, 'f': linea.factor, 'v': linea.variables,
                    'd': linea.dias, 'c': linea.sdi_calculado,
                    't': _(" (topado a %.2f)") % self.tope if linea.topado else '', 'ant': linea.sdi_actual},
            }
            if existente:
                existente.write(vals)
                creados |= existente
            else:
                creados |= Mov.create(vals)
        return {
            'type': 'ir.actions.act_window', 'name': _("Modificaciones del bimestre"),
            'res_model': 'qb.nomina.imss.movimiento', 'view_mode': 'list,form',
            'domain': [('id', 'in', creados.ids)],
        }


class ImssSdiBimestreLinea(models.TransientModel):
    _name = 'qb.nomina.imss.sdi.bimestre.linea'
    _description = "SDI del bimestre: trabajador"
    _order = 'employee_id'

    wizard_id = fields.Many2one('qb.nomina.imss.sdi.bimestre', required=True, ondelete='cascade')
    employee_id = fields.Many2one('hr.employee', string="Trabajador", required=True)
    version_id = fields.Many2one('hr.version', string="Contrato")
    registro_patronal = fields.Char(string="Reg. patronal")
    recibos = fields.Integer(string="Recibos")
    sd = fields.Float(string="SD", digits=(16, 2))
    factor = fields.Float(string="Factor", digits=(16, 4))
    variables = fields.Float(string="Variables del bimestre", digits=(16, 2))
    dias = fields.Float(string="Días cotizados", digits=(16, 2))
    sdv = fields.Float(string="SDI variable", digits=(16, 4))
    sdi_calculado = fields.Float(string="SDI calculado", digits=(16, 2))
    sdi_nuevo = fields.Float(string="SDI nuevo", digits=(16, 2))
    sdi_actual = fields.Float(string="SDI actual", digits=(16, 2))
    diferencia = fields.Float(string="Diferencia", digits=(16, 2))
    topado = fields.Boolean(string="Topado a 25 UMA")
    aplicar = fields.Boolean(string="Aplicar")
