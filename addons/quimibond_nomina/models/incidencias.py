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
``HE_DOBLE``, ``H_SENCILLA``, ``DOM`` y ``HE_DIAS``. Nunca valida ni timbra.

Las ausencias aprobadas en Odoo (incapacidad, permiso sin goce) se ven en la
lista pero **no** van al recibo como incidencia: el recibo ya las descuenta por
su entrada de trabajo (``IMSS_DISABLE``, ``DIS_ABSENCE``) y se descontarían dos
veces. Por eso ``INCAP`` no se escribe ni se borra.

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
PARAM_PREMIO_UN_RETARDO = 'quimibond_nomina.premio_asistencia_un_retardo'   # % con un solo retardo
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
    n_lineas = fields.Integer(string="Renglones", compute='_compute_resumen')
    n_faltas = fields.Integer(string="Faltas de la semana", compute='_compute_resumen')
    n_sin_checadas = fields.Integer(string="Personas sin checadas", compute='_compute_resumen')

    @staticmethod
    def _lunes(dia):
        return dia - timedelta(days=dia.weekday()) if dia else dia

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

    @api.model_create_multi
    def create(self, vals_list):
        for vals in vals_list:
            if vals.get('fecha_inicio'):
                vals['fecha_inicio'] = self._lunes(fields.Date.to_date(vals['fecha_inicio']))
        return super().create(vals_list)

    # ------------------------------------------------------------------
    # Generar
    # ------------------------------------------------------------------
    def _entradas(self):
        valor = self.env['ir.config_parameter'].sudo().get_param(PARAM_ENTRADAS)
        return calc.leer_entradas(valor)

    def _contratos(self):
        """Un contrato vigente por persona de la nómina semanal en la semana."""
        self.ensure_one()
        Version = self.env['hr.version'].with_context(active_test=False)
        versiones = Version.search([
            ('company_id', '=', self.company_id.id),
            ('employee_id', '!=', False),
            ('schedule_pay', '=', 'weekly'),
            ('contract_date_start', '!=', False),
            ('contract_date_start', '<=', self.fecha_fin),
            '|', ('contract_date_end', '=', False), ('contract_date_end', '>=', self.fecha_inicio),
        ], order='employee_id, date_version desc, id desc')
        por_empleado = {}
        for v in versiones:
            if v.employee_id.id in por_empleado:
                continue
            if v.date_version and v.date_version > self.fecha_fin:
                continue
            if not v.employee_id.active and not (v.contract_date_end and v.contract_date_end >= self.fecha_inicio):
                continue
            por_empleado[v.employee_id.id] = v
        return list(por_empleado.values())

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

    def _marcas(self, empleados, zona):
        """Checadas locales por empleado, con un día de margen a cada lado (el
        turno de noche del domingo termina el lunes)."""
        desde = datetime.combine(self.fecha_inicio - timedelta(days=1), datetime.min.time())
        hasta = datetime.combine(self.fecha_fin + timedelta(days=2), datetime.min.time())
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

    def _ausencias(self, empleados, programado_por_empleado):
        """``{employee_id: {date: (código, horas_sin_goce)}}`` de las ausencias aprobadas."""
        ausencias = defaultdict(dict)
        leaves = self.env['hr.leave'].sudo().search([
            ('employee_id', 'in', empleados.ids),
            ('state', '=', 'validate'),
            ('request_date_from', '<=', self.fecha_fin),
            ('request_date_to', '>=', self.fecha_inicio),
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
            dia = max(leave.request_date_from, self.fecha_inicio)
            fin = min(leave.request_date_to or leave.request_date_from, self.fecha_fin)
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

    def action_generar(self):
        for rec in self:
            if rec.state == 'pasada':
                raise UserError(_("La lista ya se pasó a los recibos. Regrésela a borrador para volver a generarla."))
            rec._generar()
        return True

    def _generar(self):
        self.ensure_one()
        contratos = self._contratos()
        empleados = self.env['hr.employee'].with_context(active_test=False).browse(
            [v.employee_id.id for v in contratos])
        programado = {v.employee_id.id: self._programado(v) for v in contratos}
        zona = self._zona(contratos[0]) if contratos else ZoneInfo(TZ_DEFAULT)
        marcas = self._marcas(empleados, zona)
        ausencias = self._ausencias(empleados, programado)
        entradas = self._entradas()
        hoy = fields.Date.context_today(self)
        previas = {l.employee_id.id: l for l in self.linea_ids}
        vistos = set()
        Linea = self.env['qb.nomina.incidencias.linea']
        for version in contratos:
            emp = version.employee_id
            r = calc.calcular_semana(
                self.fecha_inicio, programado[emp.id], marcas.get(emp.id, []),
                ausencias.get(emp.id, {}), version.contract_date_start, version.contract_date_end,
                hoy, entradas)
            vals = {
                'version_id': version.id,
                'dia_0': r['dias'][0], 'dia_1': r['dias'][1], 'dia_2': r['dias'][2], 'dia_3': r['dias'][3],
                'dia_4': r['dias'][4], 'dia_5': r['dias'][5], 'dia_6': r['dias'][6],
                'horas_base': r['horas_base'], 'faltas': r['faltas'], 'falta_horas': r['falta_horas'],
                'incap_dias': r['incap_dias'], 'horas_sin_goce': r['horas_sin_goce'],
                'retardos': r['retardos'], 'retardo_min': r['retardo_min'],
                'salidas_antes': r['salidas_antes'], 'salida_min': r['salida_min'],
                'extra_checador': round(r['extra_horas'], 2), 'he_propuestas': r['he_propuestas'],
                'dias_con_extra': r['dias_con_extra'], 'dom_horas': r['dom_horas'],
                'sin_checadas': r['sin_checadas'], 'proyectado': r['proyectado'],
                'observaciones': '\n'.join(r['notas']) or False,
            }
            linea = previas.get(emp.id)
            if linea:
                # Lo que capturó el supervisor no se pisa; solo se propone si sigue igual a la propuesta anterior.
                if linea.he_autorizadas == linea.he_propuestas:
                    vals['he_autorizadas'] = r['he_propuestas']
                linea.write(vals)
            else:
                vals.update({'incidencias_id': self.id, 'employee_id': emp.id,
                             'he_autorizadas': r['he_propuestas']})
                Linea.create(vals)
            vistos.add(emp.id)
        (self.linea_ids.filtered(lambda l: l.employee_id.id not in vistos)).unlink()
        self.generada_en = fields.Datetime.now()
        self.message_post(body=_("Generada desde el checador: %(n)s personas, %(f)s faltas, %(s)s sin checadas.",
                                 n=len(contratos), f=sum(self.linea_ids.mapped('faltas')),
                                 s=len(self.linea_ids.filtered('sin_checadas'))))

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
