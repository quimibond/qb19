# -*- coding: utf-8 -*-
"""Registro de fuentes de NC automáticas.

Cada automatismo que levanta una No Conformidad (rollo fuera de peso, equipo
descalibrado, indicador en rojo, incidente SST…) declara aquí su *fuente*. El
Jefe de MAST puede apagar una fuente sin tocar código y sin desinstalar módulos,
y la NC deja de generarse.

Un automatismo nuevo sólo necesita dos cosas: un registro de datos en este
modelo y llamar a `quality.alert.sgi_auto_create(code, vals)`. No hay que tocar
Ajustes ni vistas: el interruptor aparece solo en la lista de fuentes.
"""
import logging

from odoo import models, fields, api

_logger = logging.getLogger(__name__)


class SgiAlertSource(models.Model):
    _name = 'sgi.alert.source'
    _description = "Fuente de NC automática"
    _inherit = ['mail.thread']
    _order = 'sequence, code'

    code = fields.Char(
        string="Clave técnica", required=True, index=True, readonly=True,
        help="Identificador que usa el código para pedir permiso antes de "
             "levantar la NC. No se edita: lo fija el módulo que la dispara.")
    name = fields.Char(string="Fuente", required=True, translate=False)
    sequence = fields.Integer(default=10)
    enabled = fields.Boolean(
        string="Activa", default=True, tracking=True,
        help="Desactívala para dejar de generar No Conformidades por este "
             "motivo. El cambio queda registrado en el historial con autor y "
             "fecha, para poder justificarlo en auditoría.")
    trigger_type = fields.Selection([
        ('automatico', "Automático"),
        ('manual', "Manual (botón)"),
    ], string="Disparo", default='automatico', required=True, readonly=True,
        help="Automático: lo levanta el sistema solo; al desactivarlo, la NC "
             "simplemente no se crea.\n"
             "Manual: lo levanta una persona con un botón; al desactivarlo, el "
             "botón avisa que la fuente está apagada en vez de fallar en silencio.")
    trigger_note = fields.Text(
        string="Qué la dispara",
        help="Descripción en lenguaje de piso de la condición que genera la NC.")
    origin_module = fields.Char(string="Módulo", readonly=True)

    alert_count = fields.Integer(string="# NC generadas", compute='_compute_alert_count')
    suppressed_count = fields.Integer(
        string="# NC omitidas", readonly=True, copy=False,
        help="Cuántas veces se cumplió la condición mientras la fuente estaba "
             "apagada. Sirve para dimensionar lo que se dejó de registrar.")
    last_suppressed_on = fields.Datetime(string="Última omisión", readonly=True, copy=False)

    _code_uniq = models.Constraint(
        'unique(code)',
        "La clave técnica de la fuente de NC debe ser única.",
    )

    def _compute_alert_count(self):
        Alert = self.env['quality.alert']
        counts = {}
        if self.ids:
            groups = Alert._read_group(
                [('sgi_source_id', 'in', self.ids)], ['sgi_source_id'], ['__count'])
            counts = {source.id: count for source, count in groups}
        for source in self:
            source.alert_count = counts.get(source.id, 0)

    @api.model
    def _get_by_code(self, code):
        """Devuelve la fuente declarada con esa clave (recordset vacío si no existe)."""
        if not code:
            return self.browse()
        return self.sudo().search([('code', '=', code)], limit=1)

    def _register_suppression(self, count=True):
        """Deja rastro de una NC que no se creó porque la fuente está apagada.

        Se cuenta en lugar de sólo callar: un auditor que pregunte «¿por qué no
        hay NC de pesaje desde marzo?» necesita ver que la fuente se apagó, quién
        lo hizo (historial) y cuántos eventos se omitieron desde entonces.

        `count=False` para llamadores re-entrantes (un cron que reevalúa el mismo
        hecho cada corrida): la fecha se refresca, pero el contador sólo avanza
        con omisiones nuevas, para que el número siga significando algo.
        """
        for source in self.sudo():
            vals = {'last_suppressed_on': fields.Datetime.now()}
            if count:
                vals['suppressed_count'] = source.suppressed_count + 1
            source.write(vals)

    def action_toggle_enabled(self):
        for source in self:
            source.enabled = not source.enabled
        return True

    def action_view_alerts(self):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'name': "NC generadas por %s" % self.name,
            'res_model': 'quality.alert',
            'view_mode': 'list,form',
            'domain': [('sgi_source_id', '=', self.id)],
        }
