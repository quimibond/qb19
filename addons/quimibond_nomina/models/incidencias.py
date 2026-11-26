# -*- coding: utf-8 -*-
"""Lista de incidencias semanal (la F-P-A01-49 de RH) armada desde el checador.

Una lista por semana de nómina (lunes a domingo) con un renglón por persona de
la nómina semanal. «Generar» lee las checadas del reloj (``qb.checada``, no las
asistencias emparejadas), el horario del contrato y las ausencias aprobadas, y
llena los días, faltas, incapacidades, retardos, salidas antes, tiempo extra
según checador y prima dominical (reglas en ``incidencias_calculo.py``).

El supervisor o RH revisan y capturan lo que no sale del reloj: las horas extra
**autorizadas** (Odoo propone las del checador) y las sencillas a mano (bono
rama, pendientes de la nómina anterior). «Pasar a recibos» escribe las
incidencias en los recibos en borrador de la corrida elegida: ``FALTAS``,
``HE_DOBLE``, ``H_SENCILLA``, ``DOM``, ``HE_DIAS`` y ``FESTIVO`` (días festivos trabajados, 1.26.0).
Nunca valida ni timbra.

Las ausencias aprobadas en Odoo (incapacidad, permiso sin goce) se ven en la
lista pero **no** van al recibo como incidencia: el recibo ya las descuenta por
su entrada de trabajo (``IMSS_DISABLE``, ``DIS_ABSENCE``) y se descontarían dos
veces. Por eso ``INCAP`` no se escribe ni se borra.

El turno de cada semana sale de las checadas (``calc.inferir_turno``) con el
turno de RH de la ficha del empleado (``qb_turno``) como confirmación; si el
reloj dice otro, se usa el del reloj y se anota. Sin checadas ni turno de RH,
se mide contra el horario del contrato como antes.

Mientras la nómina siga saliendo del Excel de RH, la lista sirve para comparar
persona por persona antes de cambiar."""
import logging
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from odoo import _, api, fields, models
from odoo.exceptions import UserError

from . import incidencias_calculo as calc

_logger = logging.getLogger(__name__)

PARAM_ENTRADAS = 'quimibond_nomina.incidencias_entradas'
PARAM_COMIDA_MAX = 'quimibond_nomina.incidencias_comida_max'
PARAM_PREMIO_UN_RETARDO = 'quimibond_nomina.premio_asistencia_un_retardo'   # % con un solo retardo
PARAM_CORTE = 'quimibond_nomina.incidencias_corte'     # último día que cuenta la lista: 0 lunes … 6 domingo
CORTE_DEFAULT = 2                                        # miércoles, como NOI (1.27.0)
# Lo que se calcula de nuevo de la semana anterior: de su corte al domingo.
CAMPOS_PENDIENTES = ('faltas', 'falta_horas', 'retardos', 'retardo_min', 'extra_horas', 'he_propuestas',
                     'dias_con_extra', 'dom_horas', 'festivos_trabajados')
TZ_DEFAULT = 'America/Mexico_City'
ESTADOS_CHECADA_FUERA = ('ignorada', 'sin_empleado', 'error')
CODIGOS_INCAPACIDAD = ('LEAVE1000', 'LEAVE1100', 'LEAVE1200')
CODIGO_SIN_GOCE = 'LEAVE90'
CODIGO_VACACIONES = 'LEAVE120'
# Incidencia del recibo → campo del renglón.
ENTRADAS_RECIBO = (
    ('FALTAS', 'faltas_dias'),
    ('HE_DOBLE', 'he_dobles'),
    ('H_SENCILLA', 'he_sencillas'),
    ('DOM', 'dom_horas'),
    ('HE_DIAS', 'he_dias'),
    ('FESTIVO', 'festivos_trabajados'),
)


class QbNominaIncidencias(models.Model):
    _name = 'qb.nomina.incidencias'
    _description = 'Lista de incidencias semanal'
    _inherit = ['mail.thread']
    _order = 'fecha_inicio desc, id desc'

    name = fields.Char(string="Lista", compute='_compute_name', store=True)
    fecha_inicio = fields.Date(string="Lunes", required=True, tracking=True,
                               default=lambda self: self._lunes(fields.Date.context_today(self)))
    fecha_fin = fields.Date(string="Domingo", compute='_compute_fecha_fin', store=True)
    company_id = fields.Many2one('res.company', string="Empresa", required=True,
                                 default=lambda self: self.env.company)
    payslip_run_id = fields.Many2one(
        'hr.payslip.run', string="Corrida de nómina", tracking=True,
        help="Corrida de la semana. «Pasar a recibos» escribe las incidencias en sus recibos en borrador.")
    state = fields.Selection([('borrador', "Borrador"), ('revisada', "Revisada"), ('pasada', "Pasada a recibos")],
                             string="Estado", default='borrador', required=True, tracking=True)
    linea_ids = fields.One2many('qb.nomina.incidencias.linea', 'incidencias_id', string="Personas")
    generada_en = fields.Datetime(string="Generada el", readonly=True)
    corte = fields.Date(
        string="Corte", tracking=True,
        default=lambda self: self._corte_default(self._lunes(fields.Date.context_today(self))),
        help="Último día de la semana que paga esta lista, como el corte de NOI (miércoles). Lo que pasa del "
             "día siguiente al domingo (faltas, tiempo extra, prima dominical) se paga en la lista de la "
             "semana siguiente como «Semana anterior». Si se pone el domingo, la lista cubre la semana "
             "completa. Se ajusta solo a los días ya terminados al generar.")
    corte_aplicado = fields.Date(
        string="Contado hasta", readonly=True,
        help="Último día que de verdad contó la lista al generarla: el corte, o ayer si se generó antes de "
             "que terminara el corte. La lista siguiente toma de nuevo lo que pasó después de este día.")
    pendientes_desde = fields.Date(
        string="Semana anterior desde", readonly=True,
        help="Primer día de la semana anterior que esta lista pagó como pendiente (el día después del corte "
             "de la lista anterior). Vacío: no hubo pendientes.")
    n_lineas = fields.Integer(string="Renglones", compute='_compute_resumen')
    n_faltas = fields.Integer(string="Faltas de la semana", compute='_compute_resumen')
    n_sin_checadas = fields.Integer(string="Personas sin checadas", compute='_compute_resumen')

    @staticmethod
    def _lunes(dia):
        return dia - timedelta(days=dia.weekday()) if dia else dia

    @api.model
    def _corte_default(self, lunes):
        valor = self.env['ir.config_parameter'].sudo().get_param(PARAM_CORTE)
        try:
            dia = int(valor) if valor not in (None, False, '') else CORTE_DEFAULT
        except (TypeError, ValueError):
            dia = CORTE_DEFAULT
        return lunes and lunes + timedelta(days=min(max(dia, 0), 6))

    @api.depends('fecha_inicio')
    def _compute_fecha_fin(self):
        for rec in self:
            rec.fecha_fin = rec.fecha_inicio and rec.fecha_inicio + timedelta(days=6)

    @api.depends('fecha_inicio', 'fecha_fin')
    def _compute_name(self):
        for rec in self:
            rec.name = rec.fecha_inicio and _("Incidencias del %(ini)s al %(fin)s",
                                              ini=rec.fecha_inicio.strftime('%d/%m'),
                                              fin=rec.fecha_fin.strftime('%d/%m/%Y')) or False

    @api.depends('linea_ids.faltas', 'linea_ids.sin_checadas')
    def _compute_resumen(self):
        for rec in self:
            rec.n_lineas = len(rec.linea_ids)
            rec.n_faltas = sum(rec.linea_ids.mapped('faltas'))
            rec.n_sin_checadas = len(rec.linea_ids.filtered('sin_checadas'))

    @api.onchange('fecha_inicio')
    def _onchange_fecha_inicio(self):
        if self.fecha_inicio and self.fecha_inicio.weekday():
            self.fecha_inicio = self._lunes(self.fecha_inicio)
        if self.fecha_inicio:
            self.corte = self._corte_default(self.fecha_inicio)

    @api.model_create_multi
    def create(self, vals_list):
        for vals in vals_list:
            if vals.get('fecha_inicio'):
                vals['fecha_inicio'] = self._lunes(fields.Date.to_date(vals['fecha_inicio']))
                if 'corte' not in vals:
                    vals['corte'] = self._corte_default(vals['fecha_inicio'])
        return super().create(vals_list)

    @api.constrains('corte', 'fecha_inicio')
    def _check_corte(self):
        for rec in self:
            if rec.corte and rec.fecha_inicio and not rec.fecha_inicio <= rec.corte <= rec.fecha_fin:
                raise UserError(_("El corte tiene que caer entre el lunes y el domingo de la semana."))

    # ------------------------------------------------------------------
    # Generar
    # ------------------------------------------------------------------
    def _entradas(self):
        valor = self.env['ir.config_parameter'].sudo().get_param(PARAM_ENTRADAS)
        return calc.leer_entradas(valor)

    def _comida_max(self):
        valor = self.env['ir.config_parameter'].sudo().get_param(PARAM_COMIDA_MAX)
        try:
            return int(valor) if valor not in (None, False, '') else calc.COMIDA_MAX_DEFAULT
        except (TypeError, ValueError):
            return calc.COMIDA_MAX_DEFAULT

    @staticmethod
    def _sindicalizado(empleado):
        """Nómina semanal de planta: referencia ``S-`` (los empleados son ``Q-`` y ``C-``)."""
        return (empleado.sudo().registration_number or '').upper().startswith('S-')

    def _contratos(self, lunes=None):
        """Un contrato vigente por persona de la nómina semanal en la semana.

        Se toma la versión vigente al domingo de cada persona, sea cual sea su periodicidad, y solo después
        se deja fuera a quien no es semanal: quien pasó de la semanal a la quincenal (una versión quincenal
        más nueva) ya no sale en la lista aunque su versión semanal vieja siga abierta (Q-129, 25-nov-2026)."""
        self.ensure_one()
        lunes = lunes or self.fecha_inicio
        domingo = lunes + timedelta(days=6)
        Version = self.env['hr.version'].with_context(active_test=False)
        versiones = Version.search([
            ('company_id', '=', self.company_id.id),
            ('employee_id', '!=', False),
            ('contract_date_start', '!=', False),
            ('contract_date_start', '<=', domingo),
            '|', ('contract_date_end', '=', False), ('contract_date_end', '>=', lunes),
        ], order='employee_id, date_version desc, id desc')
        por_empleado = {}
        for v in versiones:
            if v.employee_id.id in por_empleado:
                continue
            if v.date_version and v.date_version > domingo:
                continue
            por_empleado[v.employee_id.id] = v
        return [v for v in por_empleado.values()
                if v.schedule_pay == 'weekly'
                and (v.employee_id.active or (v.contract_date_end and v.contract_date_end >= lunes))]

    @staticmethod
    def _programado(version):
        horas = defaultdict(float)
        for att in version.resource_calendar_id.attendance_ids:
            if att.display_type or att.day_period == 'lunch':
                continue
            horas[int(att.dayofweek)] += att.hour_to - att.hour_from
        return dict(horas)

    def _zona(self, version):
        return ZoneInfo(version.resource_calendar_id.tz or self.env.user.tz or TZ_DEFAULT)

    def _marcas(self, empleados, zona, lunes=None):
        """Checadas locales por empleado, con un día de margen a cada lado (el
        turno de noche del domingo termina el lunes)."""
        lunes = lunes or self.fecha_inicio
        desde = datetime.combine(lunes - timedelta(days=1), datetime.min.time())
        hasta = datetime.combine(lunes + timedelta(days=8), datetime.min.time())
        checadas = self.env['qb.checada'].sudo().search([
            ('employee_id', 'in', empleados.ids),
            ('state', 'not in', ESTADOS_CHECADA_FUERA),
            ('timestamp', '>=', desde), ('timestamp', '<', hasta),
        ], order='timestamp')
        marcas = defaultdict(list)
        for ch in checadas:
            local = ch.timestamp.replace(tzinfo=timezone.utc).astimezone(zona).replace(tzinfo=None)
            marcas[ch.employee_id.id].append(local)
        return marcas

    def _ausencias(self, empleados, programado_por_empleado, lunes=None):
        """``{employee_id: {date: (código, horas_sin_goce)}}`` de las ausencias aprobadas."""
        lunes = lunes or self.fecha_inicio
        domingo = lunes + timedelta(days=6)
        ausencias = defaultdict(dict)
        leaves = self.env['hr.leave'].sudo().search([
            ('employee_id', 'in', empleados.ids),
            ('state', '=', 'validate'),
            ('request_date_from', '<=', domingo),
            ('request_date_to', '>=', lunes),
        ])
        for leave in leaves:
            codigo_entrada = leave.holiday_status_id.work_entry_type_id.code or ''
            if codigo_entrada in CODIGOS_INCAPACIDAD:
                codigo = 'I'
            elif codigo_entrada == CODIGO_SIN_GOCE or leave.holiday_status_id.unpaid:
                codigo = calc.CODIGO_SIN_GOCE
            elif codigo_entrada == CODIGO_VACACIONES:
                codigo = 'V'
            else:
                codigo = 'P'
            dia = max(leave.request_date_from, lunes)
            fin = min(leave.request_date_to or leave.request_date_from, domingo)
            while dia <= fin:
                horas = 0.0
                if codigo == calc.CODIGO_SIN_GOCE:
                    if leave.request_unit_hours:
                        horas = leave.number_of_hours or 0.0
                    else:
                        horas = programado_por_empleado.get(leave.employee_id.id, {}).get(dia.weekday(), 0.0)
                ausencias[leave.employee_id.id][dia] = (codigo, horas)
                dia += timedelta(days=1)
        return ausencias

    def _turno_semana(self, empleado, version, marcas, ausencias, hoy, lunes=None):
        """``(código, nota)``: el turno con que se mide la semana. El del checador, con el de RH como
        confirmación; si no coinciden gana el del checador y queda la nota. Sin checadas, el de RH."""
        rh = empleado.sudo().qb_turno or None
        codigo, _puntajes = calc.inferir_turno(
            lunes or self.fecha_inicio, marcas, ausencias, version.contract_date_start, version.contract_date_end,
            hoy, preferido=rh)
        if not codigo:
            return rh, None
        if rh and codigo != rh:
            fijo = _(" (turno fijo)") if empleado.sudo().qb_turno_fijo else ""
            return codigo, _("Turno %(c)s según checador; RH lo tiene en el %(r)s%(f)s",
                             c=codigo, r=rh, f=fijo)
        if not rh:
            return codigo, _("Turno %(c)s según checador; RH no le tiene turno", c=codigo)
        return codigo, None

    def action_generar(self):
        for rec in self:
            if rec.state == 'pasada':
                raise UserError(_("La lista ya se pasó a los recibos. Regrésela a borrador para volver a generarla."))
            rec._generar()
        return True

    def _calcular(self, lunes, contratos, hoys):
        """Corre las reglas de la semana del ``lunes`` para cada contrato, una vez por cada fecha de ``hoys``
        (los días posteriores se proyectan). ``{employee_id: (version, turno, nota_turno, [r, …])}``; el
        turno se infiere con la primera fecha."""
        empleados = self.env['hr.employee'].with_context(active_test=False).browse(
            [v.employee_id.id for v in contratos])
        programado = {v.employee_id.id: self._programado(v) for v in contratos}
        zona = self._zona(contratos[0]) if contratos else ZoneInfo(TZ_DEFAULT)
        marcas = self._marcas(empleados, zona, lunes)
        ausencias = self._ausencias(empleados, programado, lunes)
        entradas = self._entradas()
        comida_max = self._comida_max()
        festivos = self.env['qb.nomina.festivo'].qb_festivos(lunes, lunes + timedelta(days=6), self.company_id)
        resultado = {}
        for version in contratos:
            emp = version.employee_id
            turno, nota_turno = self._turno_semana(emp, version, marcas.get(emp.id, []),
                                                   ausencias.get(emp.id, {}), hoys[0], lunes)
            rs = [calc.calcular_semana(
                lunes, programado[emp.id], calc.marcas_hasta(marcas.get(emp.id, []), hoy, turno=calc.TURNOS.get(turno)),
                ausencias.get(emp.id, {}), version.contract_date_start, version.contract_date_end,
                hoy, entradas, turno=turno if turno in calc.TURNOS else None,
                comida_max=comida_max, sindicalizado=self._sindicalizado(emp), festivos=festivos,
                he_fijas=bool(emp.sudo().qb_cuadrilla_id.he_fijas)) for hoy in hoys]
            resultado[emp.id] = (version, turno, nota_turno, rs)
        return resultado

    def _corte_anterior(self):
        """Último día de la semana anterior que ya pagó su lista: lo que contó de verdad la lista anterior
        (``corte_aplicado``) o, si no hay lista o es de antes de 1.27.0, el corte por omisión (miércoles)."""
        self.ensure_one()
        lunes_ant = self.fecha_inicio - timedelta(days=7)
        anteriores = self.search([('fecha_inicio', '=', lunes_ant), ('company_id', '=', self.company_id.id),
                                  ('id', '!=', self.id), ('corte_aplicado', '!=', False)], order='id desc')
        # Manda la que se pasó a recibos, luego la revisada, luego la más nueva.
        orden = {'pasada': 0, 'revisada': 1, 'borrador': 2}
        anterior = min(anteriores, key=lambda l: orden.get(l.state, 3), default=None)
        return anterior.corte_aplicado if anterior else self._corte_default(lunes_ant)

    def _pendientes(self, hoy):
        """Lo de la semana anterior que su lista no alcanzó a contar (del día después de su corte al domingo):
        las reglas de esa semana completa menos las de esa semana hasta su corte. ``(desde, {employee_id:
        {campo: valor, 'notas': [...]}})``."""
        lunes_ant = self.fecha_inicio - timedelta(days=7)
        domingo_ant = lunes_ant + timedelta(days=6)
        corte_ant = self._corte_anterior()
        if not corte_ant or corte_ant >= domingo_ant:
            return False, {}
        contratos = self._contratos(lunes_ant)
        completo = min(domingo_ant, hoy)
        calculos = self._calcular(lunes_ant, contratos, [completo, corte_ant])
        pendientes = {}
        for emp_id, (_version, _turno, _nota, (r_total, r_corte)) in calculos.items():
            p = {c: max(0, r_total[c] - r_corte[c]) for c in CAMPOS_PENDIENTES}
            vistas = set(r_corte['notas'])
            p['notas'] = [n for n in r_total['notas'] if n not in vistas and not n.startswith('Sin checadas')]
            if any(p[c] for c in CAMPOS_PENDIENTES) or p['notas']:
                pendientes[emp_id] = p
        return corte_ant + timedelta(days=1), pendientes

    def _generar(self):
        self.ensure_one()
        hoy = fields.Date.context_today(self)
        corte = self.corte or self._corte_default(self.fecha_inicio)
        # Solo cuentan los días ya terminados: un día en curso se proyecta y lo toma la lista siguiente.
        corte_aplicado = max(min(corte, hoy - timedelta(days=1)), self.fecha_inicio - timedelta(days=1))
        contratos = self._contratos()
        calculos = self._calcular(self.fecha_inicio, contratos, [corte_aplicado])
        desde_ant, pendientes = self._pendientes(hoy)
        previas = {l.employee_id.id: l for l in self.linea_ids}
        vistos = set()
        Linea = self.env['qb.nomina.incidencias.linea']
        for version in contratos:
            emp = version.employee_id
            _v, turno, nota_turno, (r,) = calculos[emp.id]
            if nota_turno:
                r['notas'].insert(0, nota_turno)
            ant = pendientes.get(emp.id, {})
            if ant:
                partes = []
                if ant['faltas']:
                    partes.append(_("%s falta(s)", ant['faltas']))
                if ant['he_propuestas']:
                    partes.append(_("%s HE", ant['he_propuestas']))
                if ant['dom_horas']:
                    partes.append(_("%s h de prima dominical", calc._fmt(ant['dom_horas'])))
                if ant['festivos_trabajados']:
                    partes.append(_("%s festivo(s) trabajado(s)", ant['festivos_trabajados']))
                if ant['retardos']:
                    partes.append(_("%s retardo(s)", ant['retardos']))
                encabezado = _("Semana anterior (del %(d)s al domingo)%(p)s",
                               d=desde_ant.strftime('%d/%m'), p=(': ' + ', '.join(partes)) if partes else '')
                r['notas'] += [encabezado] + ['  ' + n for n in ant['notas']]
            vals = {
                'version_id': version.id, 'turno': turno or False, 'turno_rh': emp.sudo().qb_turno or False,
                'dia_0': r['dias'][0], 'dia_1': r['dias'][1], 'dia_2': r['dias'][2], 'dia_3': r['dias'][3],
                'dia_4': r['dias'][4], 'dia_5': r['dias'][5], 'dia_6': r['dias'][6],
                'horas_base': r['horas_base'],
                'faltas': r['faltas'] + ant.get('faltas', 0),
                'falta_horas': r['falta_horas'] + ant.get('falta_horas', 0.0),
                'incap_dias': r['incap_dias'], 'horas_sin_goce': r['horas_sin_goce'],
                'retardos': r['retardos'] + ant.get('retardos', 0),
                'retardo_min': r['retardo_min'] + ant.get('retardo_min', 0),
                'salidas_antes': r['salidas_antes'], 'salida_min': r['salida_min'],
                'extra_checador': round(r['extra_horas'] + ant.get('extra_horas', 0.0), 2),
                'he_propuestas': r['he_propuestas'] + ant.get('he_propuestas', 0),
                'dias_con_extra': r['dias_con_extra'] + ant.get('dias_con_extra', 0),
                'dom_horas': r['dom_horas'] + ant.get('dom_horas', 0.0),
                'comida_min': r['comida_min'], 'comidas_largas': r['comidas_largas'],
                'sin_checadas': r['sin_checadas'], 'proyectado': r['proyectado'],
                'festivos_trabajados': r['festivos_trabajados'] + ant.get('festivos_trabajados', 0),
                'ant_faltas': ant.get('faltas', 0), 'ant_he': ant.get('he_propuestas', 0),
                'ant_dom': ant.get('dom_horas', 0.0), 'ant_festivos': ant.get('festivos_trabajados', 0),
                'observaciones': '\n'.join(r['notas']) or False,
            }
            linea = previas.get(emp.id)
            if linea:
                # Lo que capturó el supervisor no se pisa; solo se propone si sigue igual a la propuesta anterior.
                if linea.he_autorizadas == linea.he_propuestas:
                    vals['he_autorizadas'] = vals['he_propuestas']
                linea.write(vals)
            else:
                vals.update({'incidencias_id': self.id, 'employee_id': emp.id,
                             'he_autorizadas': vals['he_propuestas']})
                Linea.create(vals)
            vistos.add(emp.id)
        (self.linea_ids.filtered(lambda l: l.employee_id.id not in vistos)).unlink()
        sin_renglon = [e for e in pendientes if e not in vistos]
        self.write({'generada_en': fields.Datetime.now(), 'corte_aplicado': corte_aplicado,
                    'pendientes_desde': desde_ant if pendientes else False})
        cuerpo = _("Generada desde el checador: %(n)s personas, %(f)s faltas, %(s)s sin checadas. "
                   "Cuenta del %(i)s al %(c)s.",
                   n=len(contratos), f=sum(self.linea_ids.mapped('faltas')),
                   s=len(self.linea_ids.filtered('sin_checadas')),
                   i=self.fecha_inicio.strftime('%d/%m'), c=corte_aplicado.strftime('%d/%m'))
        if pendientes:
            cuerpo += ' ' + _("Más lo de la semana anterior del %(d)s al domingo (%(n)s personas).",
                              d=desde_ant.strftime('%d/%m'), n=len(pendientes) - len(sin_renglon))
        if sin_renglon:
            nombres = self.env['hr.employee'].with_context(active_test=False).browse(sin_renglon).mapped('name')
            cuerpo += ' ' + _("Con pendientes de la semana anterior pero ya sin contrato semanal: %s.",
                              ', '.join(nombres))
        self.message_post(body=cuerpo)

    # ------------------------------------------------------------------
    # Estados y recibos
    # ------------------------------------------------------------------
    def action_revisada(self):
        self.write({'state': 'revisada'})

    def action_borrador(self):
        self.write({'state': 'borrador'})

    def action_pasar_a_recibos(self):
        self.ensure_one()
        if not self.payslip_run_id:
            raise UserError(_("Elija la corrida de nómina de la semana."))
        tipos = self.env['hr.payslip.input.type'].search([('code', 'in', [c for c, _f in ENTRADAS_RECIBO])])
        tipo_por_codigo = {t.code: t for t in tipos}
        faltan = [c for c, _f in ENTRADAS_RECIBO if c not in tipo_por_codigo]
        if faltan:
            raise UserError(_("Faltan los tipos de incidencia del recibo: %s", ', '.join(faltan)))
        recibos = self.payslip_run_id.slip_ids.filtered(lambda s: s.state in ('draft', 'verify'))
        por_empleado = {s.employee_id.id: s for s in recibos}
        pasados, sin_recibo = 0, []
        for linea in self.linea_ids:
            recibo = por_empleado.get(linea.employee_id.id)
            if not recibo:
                sin_recibo.append(linea.employee_id.name)
                continue
            recibo.input_line_ids.filtered(lambda i: i.input_type_id in tipos).unlink()
            nuevas = []
            for codigo, campo in ENTRADAS_RECIBO:
                valor = linea[campo]
                if valor:
                    nuevas.append({'payslip_id': recibo.id, 'input_type_id': tipo_por_codigo[codigo].id,
                                   'amount': valor})
            if nuevas:
                self.env['hr.payslip.input'].create(nuevas)
            pasados += 1
        recibos.compute_sheet()
        self.state = 'pasada'
        cuerpo = _("Pasada a %(n)s recibos de %(run)s.", n=pasados, run=self.payslip_run_id.display_name)
        if sin_recibo:
            cuerpo += ' ' + _("Sin recibo en la corrida: %s.", ', '.join(sin_recibo))
        self.message_post(body=cuerpo)
        self.payslip_run_id.message_post(body=_("Incidencias tomadas de «%s».", self.display_name))
        return True

    # ------------------------------------------------------------------
    # Premio de asistencia del mes
    # ------------------------------------------------------------------
    @api.model
    def _lunes_del_mes(self, mes):
        """Los lunes de las semanas que paga el mes: las de viernes (día de pago) dentro del mes, como
        ``_qb_nomina_periodos_del_mes``."""
        inicio = mes.replace(day=1)
        fin = (inicio + timedelta(days=32)).replace(day=1)
        viernes = inicio + timedelta(days=(4 - inicio.weekday()) % 7)
        lunes = []
        while viernes < fin:
            lunes.append(viernes - timedelta(days=4))
            viernes += timedelta(days=7)
        return lunes

    @api.model
    def qb_premio_asistencia_mes(self, mes, employee_ids=None):
        """Premio de asistencia de un mes desde las listas semanales: faltas y retardos de las semanas
        que paga el mes y el porcentaje del Contrato Colectivo. Devuelve ``{'faltan': [lunes sin lista],
        'personas': {employee_id: {'pct', 'faltas', 'retardos', 'semanas', 'sin_checadas'}}}``; ``pct``
        es None si alguna semana salió sin checadas (no hay con qué calificar)."""
        mes = fields.Date.to_date(mes)
        lunes = self._lunes_del_mes(mes)
        listas = self.search([('fecha_inicio', 'in', lunes), ('company_id', '=', self.env.company.id)],
                             order='fecha_inicio, id desc')
        por_semana = {}
        for lista in listas:
            por_semana.setdefault(lista.fecha_inicio, lista)
        valor = self.env['ir.config_parameter'].sudo().get_param(PARAM_PREMIO_UN_RETARDO)
        try:
            un_retardo = float(valor) if valor else 4.5
        except ValueError:
            un_retardo = 4.5
        personas = {}
        for lista in por_semana.values():
            for linea in lista.linea_ids:
                if employee_ids is not None and linea.employee_id.id not in employee_ids:
                    continue
                p = personas.setdefault(linea.employee_id.id, {'faltas': 0, 'retardos': 0, 'semanas': 0,
                                                               'sin_checadas': 0})
                p['faltas'] += linea.faltas
                p['retardos'] += linea.retardos
                p['semanas'] += 1
                p['sin_checadas'] += 1 if linea.sin_checadas else 0
        for p in personas.values():
            p['pct'] = None if p['sin_checadas'] else calc.premio_asistencia(p['faltas'], p['retardos'], un_retardo)
        return {'faltan': [d for d in lunes if d not in por_semana], 'personas': personas}

    # ------------------------------------------------------------------
    # Menú
    # ------------------------------------------------------------------
    MENU_XMLID = 'quimibond_nomina.menu_qb_nomina_incidencias'

    @api.model
    def qb_nomina_colgar_menu_incidencias(self):
        """Cuelga «Incidencias semanales» de la raíz de la app de Nómina, como el
        menú «IMSS» (``qb_nomina_colgar_menu_imss``)."""
        menu = self.env.ref(self.MENU_XMLID, raise_if_not_found=False)
        if not menu:
            return False
        Menu = self.env['ir.ui.menu'].sudo().with_context(active_test=False)
        icono = self.env['qb.nomina.rule.sentinel'].PAYROLL_APP_ICON
        root = Menu.search([('parent_id', '=', False), ('web_icon', '=', icono)], limit=1)
        if not root:
            menu.sudo().write({'active': False})
            return False
        menu.sudo().write({'parent_id': root.id, 'active': True})
        return True


class QbNominaIncidenciasLinea(models.Model):
    _name = 'qb.nomina.incidencias.linea'
    _description = 'Renglón de la lista de incidencias'
    _order = 'registro, employee_id'

    incidencias_id = fields.Many2one('qb.nomina.incidencias', string="Lista de incidencias", required=True,
                                     ondelete='cascade', index=True)
    employee_id = fields.Many2one('hr.employee', string="Persona", required=True, index=True)
    version_id = fields.Many2one('hr.version', string="Contrato")
    registro = fields.Char(string="Referencia", related='employee_id.registration_number', store=True)
    department_id = fields.Many2one(related='employee_id.department_id', string="Área", store=True)
    turno = fields.Selection(calc.TURNOS_NOMBRE, string="Turno de la semana",
                             help="Con el que se midieron faltas, retardos y tiempo extra: el que reconoce el "
                                  "checador, con el de RH como confirmación. Vacío: contra el horario del contrato.")
    turno_rh = fields.Selection(calc.TURNOS_NOMBRE, string="Turno según RH",
                                help="El de la ficha del empleado al generar la lista.")
    dia_0 = fields.Char(string="Lu")
    dia_1 = fields.Char(string="Ma")
    dia_2 = fields.Char(string="Mi")
    dia_3 = fields.Char(string="Ju")
    dia_4 = fields.Char(string="Vi")
    dia_5 = fields.Char(string="Sa")
    dia_6 = fields.Char(string="Do")
    horas_base = fields.Float(string="Horas", digits=(6, 1))
    faltas = fields.Integer(string="Faltas")
    falta_horas = fields.Float(string="Horas de falta", digits=(6, 1),
                               help="Horas no trabajadas por falta: el día más 2 horas de lunes a jueves.")
    faltas_dias = fields.Float(string="Faltas (días)", compute='_compute_recibo', store=True, digits=(6, 4),
                               help="Horas de falta entre 8, como NOI. Va a la incidencia FALTAS. Los permisos sin "
                                    "goce no: el recibo ya los descuenta por la ausencia de Odoo.")
    incap_dias = fields.Integer(string="Incapacidad (días)",
                                help="De las ausencias aprobadas. No va al recibo: Odoo ya la descuenta (IMSS_DISABLE).")
    horas_sin_goce = fields.Float(string="Sin goce (h)", digits=(6, 1),
                                  help="De las ausencias aprobadas. No va al recibo: Odoo ya lo descuenta (DIS_ABSENCE).")
    retardos = fields.Integer(string="Retardos")
    retardo_min = fields.Integer(string="Minutos de retardo")
    salidas_antes = fields.Integer(string="Salidas antes")
    salida_min = fields.Integer(string="Minutos de salida antes")
    extra_checador = fields.Float(string="Extra según checador (h)", digits=(6, 2))
    he_propuestas = fields.Integer(string="HE propuestas")
    he_autorizadas = fields.Integer(string="HE autorizadas",
                                    help="Las que autoriza el supervisor. Odoo propone las horas completas del checador.")
    sencillas_extra = fields.Float(string="Sencillas a mano", digits=(6, 1),
                                   help="Bono rama y horas pendientes de la nómina anterior.")
    he_dobles = fields.Float(string="HE dobles", compute='_compute_recibo', store=True, digits=(6, 1))
    he_sencillas = fields.Float(string="HE sencillas", compute='_compute_recibo', store=True, digits=(6, 1))
    dias_con_extra = fields.Integer(string="Días con extra")
    he_dias = fields.Integer(string="Días con tiempo extra", compute='_compute_recibo', store=True)
    dom_horas = fields.Float(string="Prima dominical (h)", digits=(6, 1))
    comida_min = fields.Integer(string="Comida (min)",
                                help="Minutos de comida de la semana según el checador (salida y regreso de comer). "
                                     "Solo se anota: no se descuenta.")
    comidas_largas = fields.Integer(string="Comidas largas",
                                    help="Días en que la comida pasó de lo permitido: sindicalizados 30 minutos en jornadas de 10 "
                                         "horas y 45 en las de 12; empleados el parámetro "
                                         "quimibond_nomina.incidencias_comida_max (60 si no está).")
    ant_faltas = fields.Integer(string="Faltas sem. anterior",
                                help="Ya incluidas en «Faltas»: las de la semana anterior después de su corte.")
    ant_he = fields.Integer(string="HE sem. anterior",
                            help="Ya incluidas en «HE propuestas»: las de la semana anterior después de su corte.")
    ant_dom = fields.Float(string="Prima dominical sem. anterior (h)", digits=(6, 1),
                           help="Ya incluida en «Prima dominical»: la de la semana anterior después de su corte.")
    ant_festivos = fields.Integer(string="Festivos sem. anterior",
                                  help="Ya incluidos en «Festivos trabajados».")
    festivos_trabajados = fields.Integer(
        string="Festivos trabajados",
        help="Días de descanso obligatorio (calendario de festivos) que la persona trabajó: se pagan con "
             "salario doble además del día (art. 75 LFT). Pasan al recibo como la entrada FESTIVO (días).")
    sin_checadas = fields.Boolean(string="Sin checadas")
    proyectado = fields.Boolean(string="Con días proyectados")
    observaciones = fields.Text(string="Observaciones")

    @api.depends('falta_horas', 'he_autorizadas', 'sencillas_extra', 'dias_con_extra')
    def _compute_recibo(self):
        for linea in self:
            linea.faltas_dias = round((linea.falta_horas or 0.0) / 8.0, 4)
            dobles, sencillas = calc.repartir_horas_extra(linea.he_autorizadas, linea.sencillas_extra)
            linea.he_dobles = dobles
            linea.he_sencillas = sencillas
            linea.he_dias = min(linea.dias_con_extra, 3 * max(int(dobles), 0)) if dobles else 0
