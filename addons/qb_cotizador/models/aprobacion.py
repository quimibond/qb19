# -*- coding: utf-8 -*-
"""Ciclo y aprobación por puesto, compartidos por la cotización y la negociación de precio (1.8.0).

Hasta 1.7.1 todo esto vivía en ``qb.cotizador.cotizacion``. La negociación de precio (renovaciones, rebajas,
escaleras por volumen) sigue el mismo procedimiento C1, así que el ciclo se movió aquí sin cambiar su
comportamiento:

    borrador → por_aprobar → presentada → ganada | perdida | vencida
                 ↓ regreso con motivo                     ↓ renovar (solo la cotización)
               borrador                              reemplazada (la vieja)

El modelo que lo usa define ``folio``, ``partner_id``, ``currency_id`` y ``semaforo`` (rojo = debajo del costo
variable: solo lo aprueba «Autoriza precio bajo piso») y los ganchos ``_validar_para_aprobacion``,
``_nota_aprobacion``, ``_validar_para_presentar`` y ``_al_ganar``.
"""
from datetime import timedelta

from odoo import api, fields, models
from odoo.exceptions import UserError

from .settings import (
    PARAM_APROBADOR, PARAM_SEGUIMIENTO_DIAS, PARAM_SUPLENTE, PARAM_VALIDEZ_DIAS, PARAM_VENTAS,
)

STATES = [
    ('borrador', 'Borrador'),
    ('por_aprobar', 'Por aprobar'),
    ('presentada', 'Presentada'),
    ('ganada', 'Ganada'),
    ('perdida', 'Perdida'),
    ('vencida', 'Vencida'),
    ('reemplazada', 'Reemplazada'),
]
VIVAS = ('borrador', 'por_aprobar', 'presentada', 'vencida')
APROBADAS = ('presentada', 'ganada', 'perdida', 'vencida')
SEMAFOROS = [
    ('rojo', 'Debajo del costo variable'),
    ('ambar', 'Entre pisos: aporta a fijos'),
    ('verde', 'Cubre todo y deja margen'),
]
CLIENTE_MEDIOS = [
    ('correo', 'Correo'), ('oc', 'Orden de compra'), ('whatsapp', 'WhatsApp'),
    ('cotizacion_firmada', 'Cotización firmada'),
    ('direccion', 'Dirección (desarrollo interno)'),  # 1.2.0: mismo catálogo que la aprobación del SGI
]


class QbCotizadorAprobacion(models.AbstractModel):
    _name = 'qb.cotizador.aprobacion'
    _description = 'Ciclo y aprobación por puesto del cotizador'

    # Cómo se nombra el documento en mensajes y actividades.
    _ciclo_nombre = 'cotización'

    state = fields.Selection(STATES, default='borrador', tracking=True, copy=False, index=True)
    aprobador_job_id = fields.Many2one('hr.job', compute='_compute_aprobacion')
    puede_aprobar = fields.Boolean(compute='_compute_aprobacion')
    solicitada_por_id = fields.Many2one('res.users', readonly=True, copy=False)
    solicitada_el = fields.Datetime(readonly=True, copy=False)
    approved_by_id = fields.Many2one('res.users', string='Aprobó', readonly=True, copy=False)
    approved_date = fields.Datetime(string='Aprobada el', readonly=True, copy=False)
    regreso_motivo_id = fields.Many2one('qb.cotizador.motivo', string='Último motivo de regreso',
                                        readonly=True, copy=False)
    regreso_count = fields.Integer(string='Veces regresada', readonly=True, copy=False)
    presented_by_id = fields.Many2one('res.users', string='Presentó', readonly=True, copy=False)
    presented_date = fields.Datetime(string='Presentada el', readonly=True, copy=False)
    validez_hasta = fields.Date(string='Válida hasta', tracking=True, copy=False)
    seguimiento_fecha = fields.Date(string='Seguimiento el', readonly=True, copy=False)
    seguimiento_hecho = fields.Boolean(readonly=True, copy=False)
    cliente_aprobo = fields.Boolean(string='El cliente aprobó la muestra', tracking=True, copy=False)
    cliente_medio = fields.Selection(CLIENTE_MEDIOS, string='Medio de aprobación', copy=False)
    cliente_fecha = fields.Date(string='Fecha de aprobación del cliente', copy=False)
    cliente_evidencia = fields.Binary(string='Evidencia', attachment=True, copy=False)
    cliente_evidencia_nombre = fields.Char(copy=False)
    ganada_date = fields.Datetime(string='Ganada el', readonly=True, copy=False)
    perdida_motivo_id = fields.Many2one('qb.cotizador.motivo', string='Motivo de pérdida',
                                        readonly=True, copy=False)
    perdida_nota = fields.Char(readonly=True, copy=False)

    @api.depends_context('uid')
    @api.depends('state', 'semaforo')
    def _compute_aprobacion(self):
        job = self._job_from_param(PARAM_APROBADOR)
        for rec in self:
            rec.aprobador_job_id = job
            rec.puede_aprobar = rec._puede_aprobar(self.env.user)

    # ==================================================================
    # Parámetros y puestos
    # ==================================================================
    @api.model
    def _param_float(self, key, default=0.0):
        val = self.env['ir.config_parameter'].sudo().get_param(key)
        try:
            return float(val) if val not in (None, False, '') else default
        except ValueError:
            return default

    @api.model
    def _param_int(self, key, default=0):
        return int(self._param_float(key, default))

    @api.model
    def _job_from_param(self, key):
        val = self.env['ir.config_parameter'].sudo().get_param(key)
        try:
            job = self.env['hr.job'].sudo().browse(int(val)) if val else self.env['hr.job']
        except ValueError:
            return self.env['hr.job']
        return job if job.exists() else self.env['hr.job']

    @api.model
    def _users_of_job(self, job):
        """Usuarios de las personas con ese puesto (todas las compañías)."""
        if not job:
            return self.env['res.users']
        employees = self.env['hr.employee'].sudo().search([('job_id', '=', job.id)])
        return employees.mapped('user_id').filtered('active')

    def _pidio_la_aprobacion(self, user):
        """1.5.0 (Dirección General 2026-10-09): nadie aprueba lo que él mismo
        pidió. Quien mandó el documento a aprobar no lo aprueba, tenga el
        puesto titular, el suplente o el grupo bajo piso."""
        self.ensure_one()
        return bool(self.solicitada_por_id) and user == self.solicitada_por_id

    def _puede_aprobar(self, user):
        self.ensure_one()
        if self._pidio_la_aprobacion(user):
            return False
        if user.has_group('qb_cotizador.group_autoriza_bajo_piso'):
            return True
        if self.semaforo == 'rojo':
            return False
        titulares = self._users_of_job(self._job_from_param(PARAM_APROBADOR))
        suplentes = self._users_of_job(self._job_from_param(PARAM_SUPLENTE))
        return user in (titulares | suplentes)

    def _aprobadores_salvo(self, user):
        """Titular y suplente que no son ``user`` (a quién le toca cuando quien
        pide es aprobador)."""
        self.ensure_one()
        users = self._users_of_job(self._job_from_param(PARAM_APROBADOR)) \
            | self._users_of_job(self._job_from_param(PARAM_SUPLENTE))
        return users - user

    def _usuarios_ventas(self):
        users = self._users_of_job(self._job_from_param(PARAM_VENTAS))
        return users or self.user_id or self.create_uid

    def _quien_aprueba_texto(self):
        job = self._job_from_param(PARAM_APROBADOR)
        sup = self._job_from_param(PARAM_SUPLENTE)
        partes = [job.name if job else 'el puesto configurado en Ajustes → Ventas → Cotizador']
        if sup:
            partes.append('su suplente (%s)' % sup.name)
        return ' o '.join(partes)

    # ==================================================================
    # Ganchos del modelo que usa el ciclo
    # ==================================================================
    def _validar_para_aprobacion(self):
        """Levanta UserError con lo que falta antes de pedir aprobación."""
        return True

    def _nota_aprobacion(self):
        """Nota de la actividad «Aprobar …»."""
        return ''

    def _aviso_aprobacion(self):
        """Aviso extra en el mensaje «Enviada a aprobación»."""
        return ''

    def _validar_para_presentar(self):
        """Levanta UserError si no se puede presentar (también al aprobar)."""
        return True

    def _al_ganar(self):
        """Lo que pasa al ganar con la aprobación del cliente (precio en tarifa…)."""
        return True

    # ==================================================================
    # Ciclo: aprobación
    # ==================================================================
    def action_enviar_aprobacion(self):
        for rec in self:
            if rec.state != 'borrador':
                raise UserError('Solo un borrador se manda a aprobar.')
            rec._validar_para_aprobacion()
            rec.write({'state': 'por_aprobar', 'solicitada_por_id': self.env.uid,
                       'solicitada_el': fields.Datetime.now()})
            aviso = rec._aviso_aprobacion()
            if rec.semaforo == 'rojo':
                aviso += ' Precio debajo del costo variable: solo la autoriza «Autoriza precio bajo piso».'
            rec.message_post(body='Enviada a aprobación.%s' % aviso)
            # 1.5.0: el aviso de aprobar no le llega a quien la pidió (no la aprueba).
            users = rec._aprobadores_salvo(self.env.user)
            if not users and rec._pidio_la_aprobacion(self.env.user) and rec.semaforo != 'rojo':
                rec.message_post(body='Usted pidió la aprobación y es quien aprueba: nadie más puede '
                                      'aprobarla hasta que Dirección nombre el suplente en Ajustes → '
                                      'Ventas → Cotizador, o que la mande otra persona.')
            for user in users:
                rec.activity_schedule(
                    'mail.mail_activity_data_todo', user_id=user.id,
                    summary='Aprobar %s %s' % (rec._ciclo_nombre, rec.folio),
                    note=rec._nota_aprobacion())
        return True

    def action_aprobar(self):
        for rec in self:
            if rec.state != 'por_aprobar':
                raise UserError('Solo una %s «Por aprobar» se aprueba.' % rec._ciclo_nombre)
            if not rec._puede_aprobar(self.env.user):
                if rec._pidio_la_aprobacion(self.env.user):
                    otros = rec._aprobadores_salvo(self.env.user)
                    raise UserError('Usted mandó esta %s a aprobar: quien pide no la aprueba. %s' % (
                        rec._ciclo_nombre,
                        ('Le toca a %s.' % ', '.join(otros.mapped('name'))) if otros else
                        'No hay suplente nombrado (Ajustes → Ventas → Cotizador): nadie más puede aprobarla.'))
                if rec.semaforo == 'rojo':
                    raise UserError('El precio está debajo del costo variable: solo el grupo '
                                    '«Autoriza precio bajo piso» la puede aprobar.')
                raise UserError('Esta %s la aprueba %s; su usuario no tiene ese puesto.'
                                % (rec._ciclo_nombre, rec._quien_aprueba_texto()))
            rec.write({'approved_by_id': self.env.uid, 'approved_date': fields.Datetime.now()})
            rec.activity_unlink(['mail.mail_activity_data_todo'])
            rec._presentar()
        return True

    def action_regresar(self):
        self.ensure_one()
        if self.state != 'por_aprobar':
            raise UserError('Solo una %s «Por aprobar» se regresa.' % self._ciclo_nombre)
        if not self._puede_aprobar(self.env.user) and self.semaforo != 'rojo':
            raise UserError('Esta %s la regresa %s.' % (self._ciclo_nombre, self._quien_aprueba_texto()))
        return {
            'type': 'ir.actions.act_window', 'name': 'Regresar a recotizar',
            'res_model': 'qb.cotizador.decision.wizard', 'view_mode': 'form', 'target': 'new',
            'context': {'qb_decision_model': self._name, 'qb_decision_id': self.id, 'default_tipo': 'regreso'},
        }

    def _regresar(self, motivo, nota=None):
        for rec in self:
            rec.write({'state': 'borrador', 'regreso_motivo_id': motivo.id,
                       'regreso_count': rec.regreso_count + 1})
            rec.activity_unlink(['mail.mail_activity_data_todo'])
            rec.message_post(body='Regresada a recotizar. Motivo: %s.%s'
                             % (motivo.name, (' ' + nota) if nota else ''))
            destino = rec.solicitada_por_id or rec.user_id
            if destino:
                rec.activity_schedule('mail.mail_activity_data_todo', user_id=destino.id,
                                      summary='Recotizar %s: %s' % (rec.folio, motivo.name),
                                      note=nota or '')
        return True

    def action_volver_a_borrador(self):
        for rec in self:
            if rec.state != 'por_aprobar' or rec.solicitada_por_id != self.env.user:
                raise UserError('Solo quien la mandó a aprobar puede retirarla antes de la aprobación.')
            rec.write({'state': 'borrador'})
            rec.activity_unlink(['mail.mail_activity_data_todo'])
        return True

    # ==================================================================
    # Ciclo: presentada, seguimiento, vencimiento
    # ==================================================================
    def _fecha_habil(self, dias, desde=None):
        """`dias` hábiles después de `desde` según el calendario de la
        compañía; sin calendario, días naturales."""
        self.ensure_one()
        desde = desde or fields.Datetime.now()
        cal = self.company_id.resource_calendar_id
        if cal and dias:
            try:
                return cal.plan_days(dias, desde, compute_leaves=True).date()
            except Exception:  # noqa: BLE001 - calendario sin horarios: días naturales
                pass
        return (desde + timedelta(days=dias)).date()

    def _presentar(self):
        hoy = fields.Date.context_today(self)
        for rec in self:
            rec._validar_para_presentar()
            validez = self._param_int(PARAM_VALIDEZ_DIAS, 15)
            # 1.1.0: sin días de seguimiento en el parámetro no hay seguimiento automático.
            dias_seg = self._param_int(PARAM_SEGUIMIENTO_DIAS, 0)
            vals = {'state': 'presentada', 'presented_by_id': self.env.uid,
                    'presented_date': fields.Datetime.now(), 'seguimiento_hecho': dias_seg <= 0,
                    'seguimiento_fecha': rec._fecha_habil(dias_seg) if dias_seg > 0 else False}
            if not rec.validez_hasta or rec.validez_hasta < hoy:
                vals['validez_hasta'] = hoy + timedelta(days=validez)
            rec.write(vals)
            rec.message_post(body='Aprobada por %s y presentada. Válida hasta %s%s.' % (
                self.env.user.name, vals.get('validez_hasta') or rec.validez_hasta,
                ('; seguimiento el %s' % vals['seguimiento_fecha']) if vals['seguimiento_fecha']
                else '; sin seguimiento automático (días no definidos)'))
        return True

    def _vencer(self):
        for rec in self:
            rec.write({'state': 'vencida'})
            rec.message_post(body='Venció el %s sin pedido: decidir renovar, ganada o perdida.'
                             % rec.validez_hasta)
            for user in rec._usuarios_ventas():
                rec.activity_schedule('mail.mail_activity_data_todo', user_id=user.id,
                                      summary='%s %s vencida: renovar, ganada o perdida'
                                              % (rec._ciclo_nombre.capitalize(), rec.folio))
        return True

    def _seguimiento(self):
        for rec in self:
            rec.write({'seguimiento_hecho': True})
            for user in rec._usuarios_ventas():
                rec.activity_schedule('mail.mail_activity_data_todo', user_id=user.id,
                                      summary='Seguimiento a %s (%s)' % (rec.folio,
                                                                        rec.partner_id.name),
                                      note='Presentada el %s sin respuesta del cliente.'
                                           % (rec.presented_date and rec.presented_date.date()))
        return True

    @api.model
    def _cron_ciclo(self):
        """Vence las presentadas sin respuesta y manda el seguimiento a Ventas."""
        hoy = fields.Date.context_today(self)
        self.search([('state', '=', 'presentada'), ('validez_hasta', '<', hoy)])._vencer()
        self.search([('state', '=', 'presentada'), ('seguimiento_hecho', '=', False),
                     ('seguimiento_fecha', '<=', hoy)])._seguimiento()
        return True

    # ==================================================================
    # Ciclo: ganada, perdida
    # ==================================================================
    def action_ganar(self):
        for rec in self:
            if rec.state not in ('presentada', 'vencida'):
                raise UserError('Solo una %s presentada o vencida se marca ganada.' % rec._ciclo_nombre)
            rec._marcar_ganada()
        return True

    def _vals_ganada(self, **kwargs):
        return {'state': 'ganada', 'ganada_date': fields.Datetime.now()}

    def _marcar_ganada(self, automatico=False, **kwargs):
        for rec in self:
            rec.write(rec._vals_ganada(**kwargs))
            rec.activity_unlink(['mail.mail_activity_data_todo'])
            rec.message_post(body='Ganada%s%s.' % (' automáticamente' if automatico else '',
                                                    rec._texto_ganada(**kwargs)))
            rec._al_ganar()
        return True

    def _texto_ganada(self, **kwargs):
        return ''

    def action_perder(self):
        self.ensure_one()
        if self.state not in ('presentada', 'vencida', 'por_aprobar'):
            raise UserError('Solo una %s presentada, vencida o por aprobar se da por perdida.' % self._ciclo_nombre)
        return {
            'type': 'ir.actions.act_window', 'name': 'Dar por perdida',
            'res_model': 'qb.cotizador.decision.wizard', 'view_mode': 'form', 'target': 'new',
            'context': {'qb_decision_model': self._name, 'qb_decision_id': self.id, 'default_tipo': 'perdida'},
        }

    def _marcar_perdida(self, motivo, nota=None):
        for rec in self:
            rec.write({'state': 'perdida', 'perdida_motivo_id': motivo.id, 'perdida_nota': nota})
            rec.activity_unlink(['mail.mail_activity_data_todo'])
            rec.message_post(body='Perdida. Motivo: %s.%s' % (motivo.name,
                                                               (' ' + nota) if nota else ''))
        return True

    # ==================================================================
    # Aprobación del cliente y tarifa
    # ==================================================================
    def write(self, vals):
        res = super().write(vals)
        if vals.get('cliente_aprobo'):
            for rec in self:
                if not (rec.cliente_medio and rec.cliente_fecha):
                    raise UserError('La aprobación del cliente lleva medio y fecha.')
                if rec.state == 'ganada':
                    rec._al_ganar()
        return res

    def _tarifa_del_cliente(self):
        """Tarifa propia del cliente; si usa una compartida, se le crea una."""
        self.ensure_one()
        partner = self.partner_id.commercial_partner_id
        Pricelist = self.env['product.pricelist'].sudo().with_company(self.company_id)
        actual = partner.with_company(self.company_id).property_product_pricelist
        # Convención de la base: la tarifa propia de un cliente se llama
        # «TARIFA <cliente>»; cualquier otra (pública, predeterminada) es
        # compartida y no se le mete un precio de un solo cliente.
        propia = bool(actual) and actual.name.upper().startswith('TARIFA ')
        if propia and actual.currency_id == self.currency_id:
            return actual
        tarifa = Pricelist.create({'name': 'TARIFA %s' % partner.name,
                                   'currency_id': self.currency_id.id,
                                   'company_id': self.company_id.id})
        partner.sudo().with_company(self.company_id).write({'property_product_pricelist': tarifa.id})
        self.message_post(body='Tarifa %s creada y asignada al cliente.' % tarifa.name)
        return tarifa
