# -*- coding: utf-8 -*-
"""Una checada: un registro del reloj tal como llegó, y su emparejamiento.

Flujo:

1. Llega (ADMS por ``/iclock/cdata`` o API ``ingresar_api``) → se guarda
   «nueva» con el empleado resuelto, o «sin_empleado» si el usuario del
   reloj no corresponde a nadie.
2. El cron ``_cron_emparejar`` recorre las nuevas por persona en orden de
   tiempo: la primera abre una asistencia, la siguiente la cierra. Dos
   checadas seguidas en menos de N minutos son la misma (la gente checa
   dos veces por nervios): la segunda se ignora. Una entrada sin salida en
   más de M horas se cierra en cero horas y se marca «huérfana»: nunca se
   le acreditan horas que nadie vio.
3. Lo huérfano y lo sin empleado lo resuelve el supervisor en la lista.
"""
import logging
from datetime import timedelta

from odoo import api, fields, models
from odoo.exceptions import ValidationError

_logger = logging.getLogger(__name__)

PARAM_DUP_MIN = 'qb_checador.minutos_duplicado'     # dos checadas en menos de esto = una
PARAM_TURNO_MAX = 'qb_checador.horas_turno_max'    # entrada sin salida más vieja que esto = huérfana
DUP_MIN_DEFAULT = 2
TURNO_MAX_DEFAULT = 16

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
    source = fields.Selection([('adms', "Empujada por el equipo"), ('api', "Puente (API)"), ('manual', "Manual")],
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

        1. ``hr.employee.qb_checador_pin`` igual al PIN.
        2. Referencia de empleado igual a «PREFIJO-PIN» para cada prefijo del equipo, o igual al PIN.
        Varios candidatos = ambiguo = nadie: mejor una checada sin empleado que una en la persona equivocada.
        """
        pin = (pin or '').strip()
        if not pin:
            return self.env['hr.employee']
        Emp = self.env['hr.employee'].sudo()
        dominio_base = [('company_id', '=', equipo.company_id.id)]
        por_pin = Emp.search(dominio_base + [('qb_checador_pin', '=', pin)])
        if len(por_pin) == 1:
            return por_pin
        if len(por_pin) > 1:
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
            vals_list.append({
                'equipo_id': equipo.id,
                'pin': r['pin'],
                'timestamp': r['timestamp'],
                'status': r.get('status') or False,
                'verify': r.get('verify') or False,
                'workcode': r.get('workcode') or False,
                'source': source,
                'employee_id': emp.id or False,
                'state': 'nueva' if emp else 'sin_empleado',
                'nota': False if emp else "Ningún empleado con usuario %s (o hay más de uno)" % r['pin'],
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
            if c.attendance_id:
                continue
            c.write({'state': 'nueva' if c.employee_id else 'sin_empleado', 'nota': False})

    @api.model
    def _cron_emparejar(self, limite=5000):
        dup = timedelta(minutes=self._param_int(PARAM_DUP_MIN, DUP_MIN_DEFAULT))
        turno_max = timedelta(hours=self._param_int(PARAM_TURNO_MAX, TURNO_MAX_DEFAULT))
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
                    hechas += self._emparejar_empleado(empleado, checadas, dup, turno_max)
            except ValidationError as e:
                _logger.warning("qb_checador: no se pudieron emparejar las checadas de %s: %s", empleado.name, e)
                checadas.filtered(lambda c: c.state == 'nueva').write({'state': 'error', 'nota': str(e)[:200]})
        return hechas

    def _emparejar_empleado(self, empleado, checadas, dup, turno_max):
        Att = self.env['hr.attendance'].sudo()
        checadas = checadas.sorted(key=lambda c: (c.timestamp, c.id))
        previa = self.search([('employee_id', '=', empleado.id), ('state', 'in', ('emparejada', 'ignorada', 'huerfana')),
                              ('timestamp', '<', checadas[0].timestamp)], order='timestamp desc', limit=1)
        ts_previa = previa.timestamp if previa else None
        abierta = Att.search([('employee_id', '=', empleado.id), ('check_out', '=', False)],
                             order='check_in desc', limit=1)
        hechas = 0
        for c in checadas:
            if ts_previa and c.timestamp - ts_previa <= dup:
                c.write({'state': 'ignorada', 'nota': "Repetida: %s después de la anterior" % (c.timestamp - ts_previa)})
                continue
            ts_previa = c.timestamp
            if abierta:
                if c.timestamp <= abierta.check_in:
                    c.write({'state': 'huerfana', 'nota': "Llegó fuera de orden: hay una asistencia abierta posterior"})
                    continue
                if c.timestamp - abierta.check_in <= turno_max:
                    abierta.write({'check_out': c.timestamp, 'out_mode': 'checador', 'qb_checada_out_id': c.id})
                    c.write({'state': 'emparejada', 'attendance_id': abierta.id})
                    abierta = Att
                    hechas += 1
                    continue
                # Entrada sin salida: se cierra en cero horas y se marca; nunca se inventan horas.
                abierta.write({'check_out': abierta.check_in + timedelta(minutes=1), 'out_mode': 'technical'})
                if abierta.qb_checada_in_id:
                    abierta.qb_checada_in_id.write({
                        'state': 'huerfana',
                        'nota': "Sin salida en %d horas: la asistencia se cerró en cero; corregir a mano"
                                % (turno_max.total_seconds() // 3600)})
                abierta = Att
            # ¿Cae dentro de una asistencia ya cerrada? (registro que llegó tarde)
            encimada = Att.search([('employee_id', '=', empleado.id), ('check_in', '<=', c.timestamp),
                                   ('check_out', '>', c.timestamp)], limit=1)
            if encimada:
                c.write({'state': 'huerfana', 'nota': "Cae dentro de una asistencia ya cerrada (%s)" % encimada.id})
                continue
            abierta = Att.create({
                'employee_id': empleado.id,
                'check_in': c.timestamp,
                'in_mode': 'checador',
                'qb_checada_in_id': c.id,
            })
            c.write({'state': 'emparejada', 'attendance_id': abierta.id})
            hechas += 1
        return hechas
