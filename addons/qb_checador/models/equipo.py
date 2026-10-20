# -*- coding: utf-8 -*-
"""Un reloj checador ZKTeco registrado en Odoo.

Solo los equipos registrados (por número de serie) y activos pueden entregar
checadas. Un equipo desconocido que se conecte queda registrado inactivo,
para que TI vea su serie y lo autorice; sus checadas no se guardan.
"""
import secrets
from datetime import datetime

import pytz

from odoo import api, fields, models
from odoo.addons.base.models.res_partner import _tz_get

# Un ZKTeco conectado se presenta cada pocos segundos (Delay=10 en las opciones que Odoo le manda);
# si lleva más de esto sin presentarse, está desconectado.
MINUTOS_CONECTADO = 2
# Un Hikvision no se presenta solo: habla cuando alguien checa (o con la prueba de su pantalla de
# escucha HTTP). «Conectado» = mandó algo en las últimas 24 horas.
MINUTOS_CONECTADO_HIKVISION = 24 * 60
RUTA_HIKVISION = '/qb_checador/hikvision/'


class ChecadorEquipo(models.Model):
    _name = 'qb.checador.equipo'
    _description = "Reloj checador"
    _order = 'name'

    name = fields.Char(string="Nombre", required=True, help="Cómo se le llama en la planta, p. ej. «Entrada planta».")
    serial = fields.Char(string="Número de serie", required=True, index=True,
                         help="El SN que el equipo manda en cada conexión (Menú → Info del sistema). Es la llave "
                              "con la que Odoo lo reconoce.")
    marca = fields.Selection([('zkteco', "ZKTeco (ADMS)"), ('hikvision', "Hikvision (ISAPI)")], string="Marca",
                             required=True, default='zkteco',
                             help="Cómo habla con Odoo. ZKTeco: «Servidor en la nube» (ADMS) apuntando a Odoo; se "
                                  "identifica por su número de serie. Hikvision: «Escucha HTTP» (ISAPI) apuntando a "
                                  "la URL de su ficha; se identifica por el token de esa URL.")
    modelo = fields.Char(string="Modelo", help="MB10-VL, UA860, DS-K1T804AEF…")
    token = fields.Char(string="Token", copy=False, readonly=True, index=True,
                        help="Identifica a un reloj Hikvision en la URL a la que manda sus eventos. Quien lo conoce "
                             "puede mandar checadas a nombre de este reloj: no se comparte fuera de TI.")
    url_push = fields.Char(string="URL de escucha", compute='_compute_url_push',
                           help="Lo que se captura en el reloj Hikvision como servidor de escucha HTTP.")
    ruta_push = fields.Char(string="Ruta (URL) en el reloj", compute='_compute_url_push')
    ultimo_mensaje = fields.Text(string="Último mensaje recibido", readonly=True, copy=False,
                                 help="Hikvision: qué mandó el reloj la última vez (tipo de contenido, eventos leídos y "
                                      "por qué cuentan o no como checada). Se sobrescribe con cada mensaje; sirve "
                                      "para diagnosticar un reloj que se conecta pero no deja checadas.")
    host_push = fields.Char(string="Servidor", compute='_compute_url_push')
    ubicacion = fields.Char(string="Ubicación")
    ip = fields.Char(string="IP en la planta", help="Solo informativa; Odoo no se conecta al equipo, el equipo se "
                                                     "conecta a Odoo.")
    tz = fields.Selection(_tz_get, string="Zona horaria del equipo", required=True, default='America/Mexico_City',
                          help="La hora que marca el reloj es hora local; con esto se convierte a UTC.")
    prefijos = fields.Char(string="Prefijos de referencia", default='S',
                           help="Cómo se liga el usuario del checador con el empleado cuando el empleado no tiene "
                                "capturado su usuario: se busca la Referencia de empleado «PREFIJO-usuario». Varios "
                                "prefijos separados por coma (S,Q). Vacío: solo coincidencia exacta. Si el reloj "
                                "numera a su gente distinto que la nómina, use la tabla «Usuarios del reloj».")
    usuario_ids = fields.One2many('qb.checador.usuario', 'equipo_id', string="Usuarios del reloj",
                                  help="Número del reloj → empleado, solo para este reloj. Gana sobre el usuario "
                                       "capturado en el empleado y sobre la referencia con prefijo.")
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
    checadas_desde = fields.Datetime(
        string="Checadas desde",
        help="Las checadas de este reloj anteriores a esta fecha se guardan como «Ignorada» y nunca se convierten "
             "en asistencias: es el historial que un reloj trae al conectarlo por primera vez (el UA860 mandó "
             "checadas de 2024). Al cambiarla, Odoo ignora las checadas anteriores que no estaban ignoradas y "
             "borra las asistencias que el checador había creado con ellas. Vacío: sin corte.")
    checada_count = fields.Integer(string="Checadas", compute='_compute_checada_count')
    estado_conexion = fields.Selection([
        ('nunca', "Nunca conectado"),
        ('conectado', "Conectado"),
        ('sin_conexion', "Sin conexión"),
    ], string="Estado", compute='_compute_estado_conexion',
        help="ZKTeco: se presenta a Odoo cada pocos segundos mientras está conectado; «Conectado» = se presentó "
             "hace menos de %d minutos. Hikvision: solo habla cuando alguien checa; «Conectado» = mandó algo en las "
             "últimas 24 horas. «Sin conexión»: llegó antes, pero lleva más tiempo callado (red, apagado o "
             "configuración cambiada). «Nunca conectado»: no ha llegado ni una vez (red, firewall o servidor sin "
             "configurar)." % MINUTOS_CONECTADO)
    conexion_hace = fields.Char(string="Última conexión hace", compute='_compute_estado_conexion')
    notas = fields.Text(string="Notas")
    company_id = fields.Many2one('res.company', string="Empresa", default=lambda self: self.env.company,
                                 required=True)

    _serial_uniq = models.Constraint('unique(serial)', 'Ya hay un equipo con ese número de serie.')
    _token_uniq = models.Constraint('unique(token)', 'Ya hay un equipo con ese token.')

    @api.model_create_multi
    def create(self, vals_list):
        for vals in vals_list:
            if vals.get('marca') == 'hikvision' and not vals.get('token'):
                vals['token'] = secrets.token_urlsafe(18)
        return super().create(vals_list)

    def write(self, vals):
        res = super().write(vals)
        if vals.get('marca') == 'hikvision':
            for eq in self.filtered(lambda e: not e.token):
                super(ChecadorEquipo, eq).write({'token': secrets.token_urlsafe(18)})
        if vals.get('checadas_desde'):
            self._aplicar_checadas_desde()
        return res

    def _aplicar_checadas_desde(self):
        """Lo anterior a «Checadas desde» no es asistencia: sus checadas quedan «Ignorada» y las asistencias
        que el checador abrió con ellas se borran (la salida que las cerró, si es posterior, vuelve a «Nueva»).
        Devuelve cuántas asistencias se borraron."""
        Checada = self.env['qb.checada'].sudo()
        Att = self.env['hr.attendance'].sudo()
        borradas = 0
        for eq in self.filtered('checadas_desde'):
            viejas = Checada.search([('equipo_id', '=', eq.id), ('timestamp', '<', eq.checadas_desde)])
            if not viejas:
                continue
            asistencias = Att.search([('qb_checada_in_id', 'in', viejas.ids)])
            salidas = asistencias.qb_checada_out_id - viejas
            nota = "Anterior a «Checadas desde» del reloj: historial, no es asistencia"
            viejas.filtered(lambda c: c.state != 'ignorada' or c.attendance_id).write(
                {'state': 'ignorada', 'attendance_id': False, 'nota': nota})
            salidas.write({'state': 'nueva', 'attendance_id': False, 'nota': False})
            borradas += len(asistencias)
            asistencias.unlink()
        return borradas

    def action_nuevo_token(self):
        """Cambia el token: la URL anterior deja de funcionar y hay que capturar la nueva en el reloj."""
        for eq in self:
            eq.token = secrets.token_urlsafe(18)

    @api.model
    def _por_token(self, token):
        """Equipo Hikvision por token, incluidos los inactivos (para contestar «no autorizado»)."""
        token = (token or '').strip()
        if len(token) < 16:
            return self.browse()
        return self.with_context(active_test=False).search([('token', '=', token), ('marca', '=', 'hikvision')],
                                                           limit=1)

    @api.depends('token', 'marca')
    def _compute_url_push(self):
        base = (self.env['ir.config_parameter'].sudo().get_param('web.base.url') or '').rstrip('/')
        host = base.split('://', 1)[-1].split('/', 1)[0]
        for eq in self:
            if eq.marca == 'hikvision' and eq.token:
                eq.ruta_push = RUTA_HIKVISION + eq.token
                eq.host_push = host
                eq.url_push = base + eq.ruta_push
            else:
                eq.ruta_push = eq.host_push = eq.url_push = False

    def _compute_checada_count(self):
        datos = self.env['qb.checada']._read_group([('equipo_id', 'in', self.ids)], ['equipo_id'], ['__count'])
        conteo = {eq.id: c for eq, c in datos}
        for eq in self:
            eq.checada_count = conteo.get(eq.id, 0)

    def _minutos_conectado(self):
        self.ensure_one()
        return MINUTOS_CONECTADO_HIKVISION if self.marca == 'hikvision' else MINUTOS_CONECTADO

    @api.depends('ultima_conexion', 'marca')
    def _compute_estado_conexion(self):
        """Semáforo a partir de la última vez que el reloj se presentó (no se guarda: se calcula al leer)."""
        ahora = fields.Datetime.now()
        for eq in self:
            if not eq.ultima_conexion:
                eq.estado_conexion = 'nunca'
                eq.conexion_hace = "nunca"
                continue
            # Sin microsegundos: la base no los guarda y fields.Datetime.now() tampoco los trae.
            segundos = max(0, int((ahora - eq.ultima_conexion.replace(microsecond=0)).total_seconds()))
            eq.estado_conexion = 'conectado' if segundos < eq._minutos_conectado() * 60 else 'sin_conexion'
            if segundos < 60:
                eq.conexion_hace = "hace menos de 1 minuto"
            elif segundos < 3600:
                eq.conexion_hace = "hace %d min" % (segundos // 60)
            elif segundos < 86400:
                eq.conexion_hace = "hace %d h %d min" % (segundos // 3600, (segundos % 3600) // 60)
            else:
                eq.conexion_hace = "hace %d días" % (segundos // 86400)

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
            'checadas_desde': fields.Datetime.now(),
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
