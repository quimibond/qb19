# -*- coding: utf-8 -*-
import re

from odoo import api, fields, models

PARAM_DIGITOS = 'qb_checador.digito_por_nomina'
DIGITOS_DEFAULT = 'S:1,Q:2,C:3'


class HrEmployee(models.Model):
    _inherit = 'hr.employee'

    qb_checador_pin = fields.Char(
        string="Usuario en el checador", groups="hr.group_hr_user", copy=False,
        help="El número con el que el empleado está dado de alta en los relojes ZKTeco. Odoo lo pone al sincronizar "
             "usuarios: un dígito por nómina y el número de nómina a tres cifras (S-4 → 1004, Q-109 → 2109, "
             "C-26 → 3026). Si cambia, Odoo borra el número viejo de los relojes y da de alta el nuevo con sus "
             "huellas.")

    # ------------------------------------------------------------------
    @api.model
    def _qb_digitos_por_nomina(self):
        valor = self.env['ir.config_parameter'].sudo().get_param(PARAM_DIGITOS) or DIGITOS_DEFAULT
        salida = {}
        for par in valor.split(','):
            prefijo, _sep, digito = par.partition(':')
            if prefijo.strip() and digito.strip().isdigit():
                salida[prefijo.strip().upper()] = digito.strip()
        return salida

    def _qb_numero_checador(self):
        """El número de checador que le toca por su Referencia (S-4 → 1004), o '' si no se puede armar."""
        self.ensure_one()
        if 'registration_number' not in self._fields:
            return ''
        m = re.match(r'^\s*([A-Za-z]+)\s*-?\s*0*(\d{1,3})\s*$', self.registration_number or '')
        if not m:
            return ''
        digito = self._qb_digitos_por_nomina().get(m.group(1).upper())
        return '%s%03d' % (digito, int(m.group(2))) if digito else ''

    @api.model
    def _qb_referencia_de_numero(self, numero):
        """Al revés: 1004 → 'S-4'. None si el número no tiene la forma de 4 dígitos con dígito de nómina."""
        m = re.match(r'^([1-9])(\d{3})$', (numero or '').strip())
        if not m or int(m.group(2)) == 0:
            return None
        prefijo = {d: p for p, d in self._qb_digitos_por_nomina().items()}.get(m.group(1))
        return '%s-%d' % (prefijo, int(m.group(2))) if prefijo else None

    def _qb_asignar_numero_checador(self):
        """Pone a cada empleado el número que le toca por su Referencia (si cambia, ``write`` avisa a los relojes)."""
        for emp in self.sudo():
            numero = emp._qb_numero_checador()
            if numero and emp.qb_checador_pin != numero:
                emp.qb_checador_pin = numero

    # ------------------------------------------------------------------
    @api.model_create_multi
    def create(self, vals_list):
        empleados = super().create(vals_list)
        # Ingreso nuevo con Referencia (S-…, Q-…): recibe su número y entra solo a los relojes que sincronizan.
        empleados.sudo().filtered('active')._qb_asignar_numero_checador()
        return empleados

    def write(self, vals):
        anteriores = {}
        if 'qb_checador_pin' in vals or 'active' in vals:
            anteriores = {e.id: (e.sudo().qb_checador_pin, e.active) for e in self}
        res = super().write(vals)
        if anteriores:
            self._qb_avisar_relojes(anteriores)
        if 'registration_number' in vals:
            # Cambió de nómina (S-4 → Q-12) o se le puso Referencia: su número de checador cambia y los
            # relojes se enteran.
            self.sudo().filtered('active')._qb_asignar_numero_checador()
        return res

    def _qb_avisar_relojes(self, anteriores):
        """Si cambió el número de checador, el viejo se borra de los relojes que sincronizan y el nuevo se da
        de alta con sus huellas. Si el empleado se archivó, se borra de los relojes; si se reactivó, vuelve."""
        Equipo = self.env['qb.checador.equipo'].sudo()
        for emp in self.sudo():
            viejo, estaba_activo = anteriores.get(emp.id, (None, None))
            if viejo and (viejo != emp.qb_checador_pin or (estaba_activo and not emp.active)):
                Equipo._encolar_en_relojes(emp, 'DATA DELETE USERINFO PIN=%s' % viejo,
                                           "Borrar usuario %s %s" % (viejo, emp.name))
            reactivado = estaba_activo is False and emp.active
            if emp.active and emp.qb_checador_pin and (viejo != emp.qb_checador_pin or reactivado):
                relojes = Equipo.search([('marca', '=', 'zkteco'), ('sincronizar_usuarios', '=', True),
                                         ('autorizado', '=', True), ('company_id', '=', emp.company_id.id)])
                relojes.filtered(lambda r: emp in r._empleados_a_sincronizar())._enviar_empleados(emp)
