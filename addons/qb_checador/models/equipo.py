# -*- coding: utf-8 -*-
"""Un reloj checador ZKTeco registrado en Odoo.

Solo los equipos registrados (por número de serie) y activos pueden entregar
checadas. Un equipo desconocido que se conecte queda registrado inactivo,
para que TI vea su serie y lo autorice; sus checadas no se guardan.
"""
from datetime import datetime

import pytz

from odoo import api, fields, models
from odoo.addons.base.models.res_partner import _tz_get


class ChecadorEquipo(models.Model):
    _name = 'qb.checador.equipo'
    _description = "Reloj checador"
    _order = 'name'

    name = fields.Char(string="Nombre", required=True, help="Cómo se le llama en la planta, p. ej. «Entrada planta».")
    serial = fields.Char(string="Número de serie", required=True, index=True,
                         help="El SN que el equipo manda en cada conexión (Menú → Info del sistema). Es la llave "
                              "con la que Odoo lo reconoce.")
    modelo = fields.Char(string="Modelo", help="MB10-VL, UA860…")
    ubicacion = fields.Char(string="Ubicación")
    ip = fields.Char(string="IP en la planta", help="Solo informativa; Odoo no se conecta al equipo, el equipo se "
                                                     "conecta a Odoo.")
    tz = fields.Selection(_tz_get, string="Zona horaria del equipo", required=True, default='America/Mexico_City',
                          help="La hora que marca el reloj es hora local; con esto se convierte a UTC.")
    prefijos = fields.Char(string="Prefijos de referencia", default='S',
                           help="Cómo se liga el usuario del checador con el empleado cuando el empleado no tiene "
                                "capturado su usuario: se busca la Referencia de empleado «PREFIJO-usuario». Varios "
                                "prefijos separados por coma (S,Q). Vacío: solo coincidencia exacta.")
    active = fields.Boolean(default=True)
    autorizado = fields.Boolean(string="Autorizado", default=True,
                                help="Un equipo que se conectó solo (sin estar registrado) queda sin autorizar: "
                                     "se ve su serie pero sus checadas no se guardan hasta autorizarlo.")
    firmware = fields.Char(string="Firmware / versión PUSH", readonly=True)
    ultima_conexion = fields.Datetime(string="Última conexión", readonly=True)
    ultima_checada = fields.Datetime(string="Última checada recibida", readonly=True)
    attlog_stamp = fields.Char(string="Marca ATTLOG", readonly=True, default='0',
                               help="Último «Stamp» que mandó el equipo. Se le devuelve en cada conexión para que "
                                    "solo envíe registros nuevos. Ponerlo en 0 hace que vuelva a mandar todo su "
                                    "historial (las repetidas se ignoran).")
    checada_count = fields.Integer(string="Checadas", compute='_compute_checada_count')
    notas = fields.Text(string="Notas")
    company_id = fields.Many2one('res.company', string="Empresa", default=lambda self: self.env.company,
                                 required=True)

    _serial_uniq = models.Constraint('unique(serial)', 'Ya hay un equipo con ese número de serie.')

    def _compute_checada_count(self):
        datos = self.env['qb.checada']._read_group([('equipo_id', 'in', self.ids)], ['equipo_id'], ['__count'])
        conteo = {eq.id: c for eq, c in datos}
        for eq in self:
            eq.checada_count = conteo.get(eq.id, 0)

    # ------------------------------------------------------------------
    @api.model
    def _por_serie(self, serial):
        """Equipo por serie, incluidos los inactivos (para decir «te conozco pero no estás autorizado»)."""
        serial = (serial or '').strip()
        if not serial:
            return self.browse()
        return self.with_context(active_test=False).search([('serial', '=', serial)], limit=1)

    @api.model
    def _registrar_desconocido(self, serial, pushver=None):
        """Un equipo que se conecta sin estar registrado queda inactivo y sin autorizar.
        Solo si el parámetro qb_checador.auto_registrar no está en 0."""
        valor = self.env['ir.config_parameter'].sudo().get_param('qb_checador.auto_registrar', '1')
        if str(valor).strip() in ('0', 'false', 'False', ''):
            return self.browse()
        return self.sudo().create({
            'name': "Equipo sin autorizar %s" % serial,
            'serial': serial,
            'active': False,
            'autorizado': False,
            'firmware': pushver or False,
            'ultima_conexion': fields.Datetime.now(),
            'notas': "Se conectó solo el %s. Revisar que sea un equipo de la empresa, ponerle nombre, "
                     "zona horaria y prefijos, y marcarlo autorizado y activo." % fields.Date.today(),
        })

    def _acepta_checadas(self):
        self.ensure_one()
        return bool(self.active and self.autorizado)

    def _tz(self):
        self.ensure_one()
        return pytz.timezone(self.tz or 'America/Mexico_City')

    def _a_utc(self, texto):
        """'2026-10-05 06:58:10' en hora del equipo → datetime naive en UTC (como lo guarda Odoo)."""
        self.ensure_one()
        naive = datetime.strptime(texto.strip(), '%Y-%m-%d %H:%M:%S')
        return self._tz().localize(naive).astimezone(pytz.utc).replace(tzinfo=None)

    def _offset_horas(self):
        """Desfase actual de la zona del equipo respecto a UTC, en horas enteras (lo que el protocolo pide en TimeZone)."""
        self.ensure_one()
        ahora = datetime.utcnow().replace(tzinfo=pytz.utc)
        segundos = self._tz().utcoffset(ahora.astimezone(self._tz()).replace(tzinfo=None)).total_seconds()
        return int(segundos // 3600)

    def _tocar(self, firmware=None, stamp=None, checada=False):
        """Registra la conexión (y opcionalmente firmware, stamp y última checada)."""
        vals = {'ultima_conexion': fields.Datetime.now()}
        if firmware:
            vals['firmware'] = firmware[:120]
        if stamp:
            vals['attlog_stamp'] = str(stamp)[:40]
        if checada:
            vals['ultima_checada'] = fields.Datetime.now()
        self.sudo().write(vals)

    def action_reenviar_todo(self):
        """Pone la marca en 0: en la siguiente conexión el equipo manda todo lo que guarda."""
        self.write({'attlog_stamp': '0'})
