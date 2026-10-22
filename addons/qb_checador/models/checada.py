# -*- coding: utf-8 -*-
"""Una checada: un registro del reloj tal como llegó, y su emparejamiento.

Flujo:

1. Llega (ADMS por ``/iclock/cdata`` o API ``ingresar_api``) → se guarda
   «nueva» con el empleado resuelto, o «sin_empleado» si el usuario del
   reloj no corresponde a nadie.
2. El cron ``_cron_emparejar`` recorre las nuevas por persona en orden de
   tiempo y arma **una asistencia por turno**: las checadas a menos de
   ``horas_entre_turnos`` (8) de la anterior son el mismo turno (entrada,
   salida a comer, regreso, salida); la primera es la entrada y la última la
   salida, con la comida dentro, como el contrato. Una checada sola acepta su
   salida hasta ``horas_turno_max`` (16) después: el turno de 12 horas de
   noche sin checar a la comida. Hasta 1.6.x se alternaba entrada/salida y
   una checada de más o de menos corría todo un lugar (la salida de la tarde
   quedaba como entrada de una «asistencia» hasta la mañana siguiente).
   Dos checadas en menos de N minutos son la misma (la gente checa dos veces
   por nervios): la segunda se ignora. Una entrada sin salida en más de M
   horas se cierra en cero horas y se marca «huérfana»: nunca se le
   acreditan horas que nadie vio.
3. Lo huérfano y lo sin empleado lo resuelve el supervisor en la lista.
"""
import logging
from datetime import timedelta

from odoo import api, fields, models
from odoo.exceptions import ValidationError

_logger = logging.getLogger(__name__)

PARAM_DUP_MIN = 'qb_checador.minutos_duplicado'     # dos checadas en menos de esto = una
PARAM_TURNO_MAX = 'qb_checador.horas_turno_max'    # entrada sin salida más vieja que esto = huérfana
PARAM_SEPARACION = 'qb_checador.horas_entre_turnos'  # un hueco así o mayor después de una salida = otro turno
DUP_MIN_DEFAULT = 2
TURNO_MAX_DEFAULT = 16
SEPARACION_DEFAULT = 8

ESTADOS = [
    ('nueva', "Nueva"),
    ('emparejada', "Emparejada"),
    ('ignorada', "Ignorada (repetida)"),
    ('huerfana', "Huérfana"),
    ('sin_empleado', "Sin empleado"),
    ('error', "Error"),
]


class Checada(models.Model):
    _name = 'qb.checada'
    _description = "Checada"
    _order = 'timestamp desc, id desc'

    equipo_id = fields.Many2one('qb.checador.equipo', string="Equipo", required=True, ondelete='restrict', index=True)
    pin = fields.Char(string="Usuario del reloj", required=True, index=True,
                      help="El número de usuario tal como lo manda el equipo (PIN).")
    timestamp = fields.Datetime(string="Fecha y hora", required=True, index=True,
                                help="En UTC, como todo en Odoo; la vista la muestra en la zona del usuario.")
    status = fields.Char(string="Estado del reloj", help="Lo que el reloj manda como estado (0 entrada, 1 salida…). "
                                                        "Informativo: el emparejamiento no lo usa, porque en la "
                                                        "planta no se selecciona entrada/salida al checar.")
    verify = fields.Char(string="Verificación", help="1 huella, 15 rostro, 0 contraseña, 2 tarjeta…")
    workcode = fields.Char(string="Código de trabajo")
    source = fields.Selection([('adms', "Empujada por el equipo"), ('isapi', "Empujada por Hikvision (ISAPI)"),
                               ('api', "Puente (API)"), ('manual', "Manual")],
                              string="Origen", required=True, default='adms')
    employee_id = fields.Many2one('hr.employee', string="Empleado", index=True)
    state = fields.Selection(ESTADOS, string="Estado", required=True, default='nueva', index=True)
    attendance_id = fields.Many2one('hr.attendance', string="Asistencia", readonly=True, ondelete='set null')
    nota = fields.Char(string="Nota", readonly=True)
    raw = fields.Char(string="Línea original", readonly=True)
    company_id = fields.Many2one(related='equipo_id.company_id', store=True)

    _equipo_pin_ts_uniq = models.Constraint(
        'unique(equipo_id, pin, timestamp)',
        'Esa checada ya está registrada (mismo equipo, usuario y hora).')

    @api.depends('pin', 'timestamp', 'employee_id')
    def _compute_display_name(self):
        for c in self:
            quien = c.employee_id.name or ("usuario %s" % c.pin)
            c.display_name = "%s · %s" % (quien, fields.Datetime.context_timestamp(c, c.timestamp).strftime('%d/%m %H:%M')
                                          if c.timestamp else '')

    # ------------------------------------------------------------------
    # Parámetros
    # ------------------------------------------------------------------
    @api.model
    def _param_int(self, clave, default):
        valor = self.env['ir.config_parameter'].sudo().get_param(clave)
        try:
            n = int(valor)
        except (TypeError, ValueError):
            return default
        return n if n > 0 else default

    # ------------------------------------------------------------------
    # Entrada: ADMS y API
    # ------------------------------------------------------------------
    @api.model
    def _resolver_empleado(self, equipo, pin):
        """El empleado detrás de un usuario del reloj.

        1. La tabla «Usuarios del reloj» del equipo con empleado (solo vale para ese reloj).
        2. ``hr.employee.qb_checador_pin`` igual al PIN.
        3. En un reloj que sincroniza usuarios, el número de 4 dígitos de la Referencia (1004 → S-4).
        4. Referencia de empleado igual a «PREFIJO-PIN» para cada prefijo del equipo, o igual al PIN.
        Varios candidatos = ambiguo = nadie: mejor una checada sin empleado que una en la persona equivocada.
        """
        pin = (pin or '').strip()
        if not pin:
            return self.env['hr.employee']
        Emp = self.env['hr.employee'].sudo()
        de_tabla = self.env['qb.checador.usuario'].sudo().search(
            [('equipo_id', '=', equipo.id), ('pin', '=', pin), ('employee_id', '!=', False)], limit=1)
        if de_tabla:
            return de_tabla.employee_id
        dominio_base = [('company_id', '=', equipo.company_id.id)]
        por_pin = Emp.search(dominio_base + [('qb_checador_pin', '=', pin)])
        if len(por_pin) == 1:
            return por_pin
        if len(por_pin) > 1:
            return Emp
        if equipo.sincronizar_usuarios:
            por_numero = equipo._empleado_por_numero(pin)
            if por_numero:
                return por_numero
        # La Referencia de empleado (registration_number) la agrega hr_payroll (Enterprise); en una
        # base sin nómina no existe y solo queda la liga por usuario capturado.
        if 'registration_number' not in Emp._fields:
            return Emp
        prefijos = [p.strip() for p in (equipo.prefijos or '').split(',') if p.strip()]
        referencias = ["%s-%s" % (p, pin) for p in prefijos] + [pin]
        pin_sin_ceros = pin.lstrip('0')
        if pin_sin_ceros and pin_sin_ceros != pin:
            referencias += ["%s-%s" % (p, pin_sin_ceros) for p in prefijos] + [pin_sin_ceros]
        por_ref = Emp.search(dominio_base + [('registration_number', 'in', referencias)])
        return por_ref if len(por_ref) == 1 else Emp

    @api.model
    def _registrar(self, equipo, registros, source):
        """Guarda una lista de registros [{pin, timestamp(UTC naive), status, verify, workcode, raw}].
        Las repetidas (mismo equipo, pin, hora) se saltan. Devuelve (creadas, repetidas)."""
        if not registros:
            return 0, 0
        llaves = {(r['pin'], r['timestamp']) for r in registros}
        existentes = self.sudo().search([
            ('equipo_id', '=', equipo.id),
            ('pin', 'in', list({k[0] for k in llaves})),
            ('timestamp', 'in', list({k[1] for k in llaves})),
        ])
        ya = {(c.pin, c.timestamp) for c in existentes}
        vals_list, vistas = [], set()
        cache_emp = {}
        for r in registros:
            llave = (r['pin'], r['timestamp'])
            if llave in ya or llave in vistas:
                continue
            vistas.add(llave)
            if r['pin'] not in cache_emp:
                cache_emp[r['pin']] = self._resolver_empleado(equipo, r['pin'])
            emp = cache_emp[r['pin']]
            if equipo.checadas_desde and r['timestamp'] < equipo.checadas_desde:
                estado, nota = 'ignorada', "Anterior a «Checadas desde» del reloj: historial, no es asistencia"
            elif emp:
                estado, nota = 'nueva', False
            else:
                estado, nota = 'sin_empleado', "Ningún empleado con usuario %s (o hay más de uno)" % r['pin']
            vals_list.append({
                'equipo_id': equipo.id,
                'pin': r['pin'],
                'timestamp': r['timestamp'],
                'status': r.get('status') or False,
                'verify': r.get('verify') or False,
                'workcode': r.get('workcode') or False,
                'source': source,
                'employee_id': emp.id or False,
                'state': estado,
                'nota': nota,
                'raw': (r.get('raw') or '')[:200] or False,
            })
        if vals_list:
            self.sudo().create(vals_list)
        return len(vals_list), len(registros) - len(vals_list)

    @api.model
    def procesar_attlog(self, equipo, cuerpo):
        """Cuerpo de un POST ATTLOG del protocolo PUSH:
        ``PIN<TAB>AAAA-MM-DD HH:MM:SS<TAB>STATUS<TAB>VERIFY<TAB>WORKCODE<TAB>…`` por línea.
        Devuelve el número de líneas que el equipo mandó (lo que se le contesta en «OK: n»)."""
        registros, lineas = [], 0
        for linea in (cuerpo or '').splitlines():
            linea = linea.strip('\r\n')
            if not linea.strip():
                continue
            lineas += 1
            partes = linea.split('\t')
            if len(partes) < 2:
                _logger.warning("qb_checador %s: línea ATTLOG ilegible: %r", equipo.serial, linea[:80])
                continue
            try:
                ts = equipo._a_utc(partes[1])
            except ValueError:
                _logger.warning("qb_checador %s: hora ilegible en ATTLOG: %r", equipo.serial, linea[:80])
                continue
            registros.append({
                'pin': partes[0].strip(),
                'timestamp': ts,
                'status': partes[2].strip() if len(partes) > 2 else None,
                'verify': partes[3].strip() if len(partes) > 3 else None,
                'workcode': partes[4].strip() if len(partes) > 4 else None,
                'raw': linea,
            })
        creadas, repetidas = self._registrar(equipo, registros, 'adms')
        _logger.info("qb_checador %s: ATTLOG %d líneas, %d nuevas, %d repetidas", equipo.serial, lineas, creadas, repetidas)
        return lineas

    @api.model
    def procesar_hikvision(self, equipo, eventos):
        """Eventos del push ISAPI de un reloj Hikvision (dicts ya leídos del JSON/XML).
        Guarda las verificaciones con empleado; devuelve cuántas checadas nuevas quedaron."""
        from ..controllers.hikvision import PARAM_EXCLUIDOS, checada_del_evento
        valor = self.env['ir.config_parameter'].sudo().get_param(PARAM_EXCLUIDOS) or ''
        excluidos = {v.strip() for v in valor.split(',') if v.strip()}
        registros = []
        for evento in eventos or []:
            r = checada_del_evento(evento, excluidos)
            if not r:
                continue
            try:
                r['timestamp'] = equipo._a_utc(r.pop('hora_local'))
            except ValueError:
                _logger.warning("qb_checador %s: hora ilegible en evento Hikvision: %r", equipo.serial, r)
                continue
            registros.append(r)
        creadas, repetidas = self._registrar(equipo, registros, 'isapi')
        _logger.info("qb_checador %s: Hikvision %d eventos, %d checadas nuevas, %d repetidas",
                     equipo.serial, len(eventos or []), creadas, repetidas)
        return creadas

    @api.model
    def ingresar_api(self, serial, registros):
        """Para el puente en la PC de la planta (JSON-RPC con un usuario técnico).

        ``registros``: lista de dicts ``{'pin': '123', 'time': 'AAAA-MM-DD HH:MM:SS', 'status': '0', 'verify': '1'}``
        con la hora en la zona del equipo. Devuelve ``{'creadas': n, 'repetidas': m}``.
        """
        equipo = self.env['qb.checador.equipo']._por_serie(serial)
        if not equipo or not equipo._acepta_checadas():
            raise ValidationError("Equipo %s no registrado o no autorizado." % serial)
        limpios = []
        for r in registros or []:
            try:
                ts = equipo._a_utc(str(r.get('time') or r.get('timestamp') or ''))
            except ValueError:
                raise ValidationError("Hora ilegible: %r (se espera AAAA-MM-DD HH:MM:SS)" % r)
            limpios.append({
                'pin': str(r.get('pin') or '').strip(),
                'timestamp': ts,
                'status': str(r['status']) if r.get('status') is not None else None,
                'verify': str(r['verify']) if r.get('verify') is not None else None,
                'workcode': str(r['workcode']) if r.get('workcode') is not None else None,
                'raw': None,
            })
        creadas, repetidas = self._registrar(equipo, limpios, 'api')
        equipo._tocar(checada=bool(creadas))
        return {'creadas': creadas, 'repetidas': repetidas}

    # ------------------------------------------------------------------
    # Emparejar
    # ------------------------------------------------------------------
    def action_reintentar(self):
        """Vuelve a poner en «nueva» las checadas seleccionadas (resuelve el empleado otra vez si falta)."""
        for c in self:
            if not c.employee_id:
                c.employee_id = self._resolver_empleado(c.equipo_id, c.pin)
            if c.attendance_id or (c.equipo_id.checadas_desde and c.timestamp < c.equipo_id.checadas_desde):
                continue
            c.write({'state': 'nueva' if c.employee_id else 'sin_empleado', 'nota': False})

    @api.model
    def _cron_emparejar(self, limite=5000):
        dup = timedelta(minutes=self._param_int(PARAM_DUP_MIN, DUP_MIN_DEFAULT))
        turno_max = timedelta(hours=self._param_int(PARAM_TURNO_MAX, TURNO_MAX_DEFAULT))
        separacion = timedelta(hours=self._param_int(PARAM_SEPARACION, SEPARACION_DEFAULT))
        nuevas = self.search([('state', '=', 'nueva'), ('employee_id', '!=', False)],
                             order='employee_id, timestamp, id', limit=limite)
        por_empleado = {}
        for c in nuevas:
            por_empleado.setdefault(c.employee_id, self.browse())
            por_empleado[c.employee_id] |= c
        hechas = 0
        for empleado, checadas in por_empleado.items():
            try:
                with self.env.cr.savepoint():
                    hechas += self._emparejar_empleado(empleado, checadas, dup, turno_max, separacion)
            except ValidationError as e:
                _logger.warning("qb_checador: no se pudieron emparejar las checadas de %s: %s", empleado.name, e)
                checadas.filtered(lambda c: c.state == 'nueva').write({'state': 'error', 'nota': str(e)[:200]})
        return hechas

    @staticmethod
    def _mismo_turno(asistencia, ts, turno_max, separacion):
        """¿La checada ``ts`` sigue el turno de ``asistencia`` (la última de la persona)?"""
        if ts - asistencia.check_in > turno_max:
            return False
        if not asistencia.check_out:
            return True     # una sola checada: es su salida (turno de 12 h sin checar a la comida)
        if asistencia.in_mode != 'checador':
            return False    # una asistencia capturada a mano no se alarga sola
        return ts - asistencia.check_out < separacion

    def _emparejar_empleado(self, empleado, checadas, dup, turno_max, separacion):
        Att = self.env['hr.attendance'].sudo()
        checadas = checadas.sorted(key=lambda c: (c.timestamp, c.id))
        previa = self.search([('employee_id', '=', empleado.id), ('state', 'in', ('emparejada', 'ignorada', 'huerfana')),
                              ('timestamp', '<', checadas[0].timestamp)], order='timestamp desc', limit=1)
        ts_previa = previa.timestamp if previa else None
        hechas = 0
        for c in checadas:
            if ts_previa and c.timestamp - ts_previa <= dup:
                c.write({'state': 'ignorada', 'nota': "Repetida: %s después de la anterior" % (c.timestamp - ts_previa)})
                continue
            ts_previa = c.timestamp
            ultima = Att.search([('employee_id', '=', empleado.id), ('check_in', '<=', c.timestamp)],
                                order='check_in desc', limit=1)
            if ultima and ultima.check_out and c.timestamp <= ultima.check_out:
                if ultima.in_mode == 'checador':
                    # Cae dentro de un turno ya armado (la de la comida que llegó tarde): no cambia nada.
                    c.write({'state': 'emparejada', 'attendance_id': ultima.id, 'nota': "Dentro del turno; llegó tarde"})
                else:
                    c.write({'state': 'huerfana', 'nota': "Cae dentro de una asistencia capturada a mano (%s)" % ultima.id})
                continue
            if Att.search_count([('employee_id', '=', empleado.id), ('check_in', '>', c.timestamp)]):
                c.write({'state': 'huerfana', 'nota': "Llegó fuera de orden: hay una asistencia posterior"})
                continue
            if ultima and self._mismo_turno(ultima, c.timestamp, turno_max, separacion):
                ultima.write({'check_out': c.timestamp, 'out_mode': 'checador', 'qb_checada_out_id': c.id})
                c.write({'state': 'emparejada', 'attendance_id': ultima.id})
                hechas += 1
                continue
            if ultima and not ultima.check_out:
                # Entrada sin salida: se cierra en cero horas y se marca; nunca se inventan horas.
                ultima.write({'check_out': ultima.check_in + timedelta(minutes=1), 'out_mode': 'technical'})
                if ultima.qb_checada_in_id:
                    ultima.qb_checada_in_id.write({
                        'state': 'huerfana',
                        'nota': "Sin salida en %d horas: la asistencia se cerró en cero; corregir a mano"
                                % (turno_max.total_seconds() // 3600)})
            asistencia = Att.create({
                'employee_id': empleado.id,
                'check_in': c.timestamp,
                'in_mode': 'checador',
                'qb_checada_in_id': c.id,
            })
            c.write({'state': 'emparejada', 'attendance_id': asistencia.id})
            hechas += 1
        return hechas

    @api.model
    def _reconstruir_asistencias(self):
        """Borra las asistencias que armó el checador y vuelve a emparejar sus checadas con la regla
        vigente. Lo llama la migración de 1.7.0 (las de 1.6.x quedaron corridas un lugar); las
        asistencias capturadas a mano no se tocan. Devuelve cuántas asistencias se borraron."""
        Att = self.env['hr.attendance'].sudo()
        asistencias = Att.search([('in_mode', '=', 'checador')])
        checadas = self.search([
            '|', '|', ('attendance_id', 'in', asistencias.ids),
            ('state', '=', 'huerfana'),
            '&', ('state', '=', 'ignorada'), ('nota', '=like', 'Repetida%'),
        ])
        checadas.write({'state': 'nueva', 'attendance_id': False, 'nota': False})
        n = len(asistencias)
        asistencias.unlink()
        # Lo anterior a «Checadas desde» vuelve a «Ignorada» (historial del reloj, no asistencia).
        self.env['qb.checador.equipo'].sudo().search([])._aplicar_checadas_desde()
        self._cron_emparejar(limite=100000)
        return n
