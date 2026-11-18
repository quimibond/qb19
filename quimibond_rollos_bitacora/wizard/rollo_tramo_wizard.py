# -*- coding: utf-8 -*-
"""Quitar un tramo a un rollo con defectos y juntar los tramos en un rollo de FE o Segunda (Junta con el cliente,
2026-11-13, punto 1).

Inspección le quita a un rollo con defectos una cantidad y la aparta; esa cantidad se acumula por baño con cada rollo al
que se le quita. Al final se juntan todos los tramos en un rollo nuevo de FE o Segunda, con su etiqueta y los defectos
de los tramos, como parte de la orden en kg. No se mueve inventario (D7): el rollo de Rama se registra después con su
peso ya sin el tramo y el rollo de tramos entra a su calidad al cerrar el baño, como cualquier rollo final.
"""
from odoo import api, fields, models, _
from odoo.exceptions import UserError

from .pesaje_manual import TOLERANCIA_KG, modo_por_defecto, motivo_por_defecto
from ..models.mrp_rollo_bitacora import CLASIFICACIONES_CAPTURA, PARAM_ALERTA_DIFERENCIA_PCT
from ..models.mrp_rollo_final import PARAM_ALERTA_MERMA_PCT

CALIDADES_TRAMOS = [c for c in CLASIFICACIONES_CAPTURA if c[0] in ('FE', 'SEGUNDA')]


class RolloTramoWizard(models.TransientModel):
    """Quitar tramo, desde la ventana INSPECCIONAR (ajuste de tramos, 2026-11-16): el rollo ya está pesado en la
    estación; lo que le falta para llegar al peso de Rama es el peso inicial del tramo, que el operador pesa de nuevo,
    con sus metros y defectos. El tramo se guarda sin etiqueta y se regresa a la misma ventana INSPECCIONAR."""
    _name = 'rollo.tramo.wizard'
    _inherit = ['scale.wizard.mixin']
    _description = 'Inspección: quitar un tramo a un rollo'

    estacion_id = fields.Many2one('rollo.inspeccion.estacion.wizard', 'Ventana de Inspección', required=True,
                                  readonly=True, ondelete='cascade')
    production_id = fields.Many2one('mrp.production', 'Orden (kg)', required=True, readonly=True)
    workorder_id = fields.Many2one('mrp.workorder', 'Operación de Inspección', required=True, readonly=True)
    product_id = fields.Many2one(related='production_id.product_id', string='Artículo')
    workcenter_id = fields.Many2one('mrp.workcenter', 'Revisadora', required=True, readonly=True)
    empleado_numero = fields.Char('Número de empleado', required=True, readonly=True)
    empleado_nombre = fields.Char('Operador', compute='_compute_empleado')

    bitacora_id = fields.Many2one('mrp.rollo.bitacora', 'Rollo de Rama', required=True, readonly=True)
    kg_rama = fields.Float('Kg de Rama', related='bitacora_id.kg_rama', digits=(12, 4))
    kg_rollo = fields.Float('Kg netos del rollo (ya pesado)', readonly=True, digits=(12, 4))
    kg_ya_cortados = fields.Float('Kg de rollos ya registrados', readonly=True, digits=(12, 4))
    kg_tramos = fields.Float('Kg de tramos ya quitados', readonly=True, digits=(12, 4))
    kg_disponibles = fields.Float('Kg que le faltan al rollo', compute='_compute_disponibles', digits=(12, 4),
                                  help='Rama − rollo pesado − registrados − tramos ya quitados: el máximo del tramo.')

    kg_bruto = fields.Float('Peso bruto (kg)', digits=(12, 4))
    tara_kg = fields.Float('Tara de Inspección (kg)', compute='_compute_pesos', digits=(12, 4))
    tara_faltante = fields.Boolean(compute='_compute_pesos')
    kg_neto = fields.Float('Peso neto (kg)', compute='_compute_pesos', digits=(12, 4))
    motivo_manual = fields.Char('Motivo de la captura manual', default=lambda self: motivo_por_defecto(self))
    weighing_mode = fields.Selection(default=lambda self: modo_por_defecto(self))
    metros = fields.Float('Metros del tramo', digits=(12, 2))
    defecto_ids = fields.Many2many('calidad.defecto.tela', 'rollo_tramo_wizard_defecto_rel', 'wizard_id', 'defecto_id',
                                   'Defectos', domain="[('tipo_tela', '=', 'tejido_acabado')]")
    alertas = fields.Text('Alertas', compute='_compute_disponibles')

    @api.depends('empleado_numero')
    def _compute_empleado(self):
        for wiz in self:
            emp = self.env['hr.employee']._qb_operador(wiz.empleado_numero)
            wiz.empleado_nombre = emp.name if emp else (_('EMPLEADO NO ENCONTRADO') if wiz.empleado_numero else False)

    @api.depends('kg_rama', 'kg_rollo', 'kg_ya_cortados', 'kg_tramos', 'kg_neto')
    def _compute_disponibles(self):
        for wiz in self:
            wiz.kg_disponibles = max(wiz.kg_rama - wiz.kg_rollo - wiz.kg_ya_cortados - wiz.kg_tramos, 0.0)
            wiz.alertas = _('El tramo pesa %(t).2f kg y solo quedan %(d).2f kg por asignar del rollo de Rama.',
                            t=wiz.kg_neto, d=wiz.kg_disponibles) \
                if wiz.kg_neto and wiz.kg_neto > wiz.kg_disponibles + TOLERANCIA_KG else False

    @api.depends('product_id', 'kg_bruto')
    def _compute_pesos(self):
        Tara = self.env['mrp.tara.producto']
        for wiz in self:
            # Misma tara que el resto de Inspección («Clasificación final» en la tabla de taras).
            tara = Tara._buscar_tara(wiz.product_id, 'clasificacion_final') if wiz.product_id else Tara
            wiz.tara_faltante = bool(wiz.product_id) and not tara
            wiz.tara_kg = tara.tara_kg
            wiz.kg_neto = wiz.kg_bruto - tara.tara_kg if tara else 0.0

    def _validar(self):
        self.ensure_one()
        if self.production_id.state in ('done', 'cancel'):
            raise UserError(_('La orden %s ya está cerrada o cancelada: no se quitan tramos.', self.production_id.name))
        rollo = self.bitacora_id
        if rollo.estado_inspeccion == 'inspeccionado':
            raise UserError(_('El rollo %s ya fue inspeccionado: el tramo se quita antes de registrar su último rollo.',
                              rollo.name))
        if self.tara_faltante:
            raise UserError(_(
                'No hay tara de Inspección para %s. Captúrala en Manufactura → Configuración → Taras por producto '
                '(operación «Clasificación final»); el pesaje no asume tara 0.', self.product_id.display_name))
        if self.kg_bruto <= 0:
            raise UserError(_('Captura el peso bruto del tramo.'))
        if self.kg_neto <= 0:
            raise UserError(_('El peso bruto (%(b).4f kg) no supera la tara (%(t).4f kg).', b=self.kg_bruto,
                              t=self.tara_kg))
        if self.weighing_mode == 'manual' and not (self.motivo_manual or '').strip():
            raise UserError(_('Captura el motivo de la captura manual del peso.'))
        if self.metros <= 0:
            raise UserError(_('Captura los metros del tramo.'))
        if not self.defecto_ids:
            raise UserError(_('Captura al menos un defecto: el tramo se quitó por una causa.'))
        if self.kg_neto > self.kg_disponibles + TOLERANCIA_KG:
            raise UserError(_(
                'El rollo pesa %(p).2f kg y el tramo %(t).2f kg: juntos pasan de los %(k).2f kg del rollo de Rama %(r)s '
                '(ya registrados %(f).2f kg y tramos anteriores %(tr).2f kg). Solo caben %(d).2f kg.',
                p=self.kg_rollo, t=self.kg_neto, k=self.kg_rama, r=rollo.name, f=self.kg_ya_cortados,
                tr=self.kg_tramos, d=self.kg_disponibles))
        emp = self.env['hr.employee']._qb_operador_requerido(self.empleado_numero)
        self.env['mrp.inspeccion.restriccion']._validar_centro(self.workcenter_id, self.product_id)
        return emp

    def _volver(self):
        """Regresa a la misma ventana INSPECCIONAR, con lo que ya tenía capturado."""
        return self.estacion_id._reabrir()

    def action_volver(self):
        self.ensure_one()
        return self._volver()

    def action_confirmar(self):
        self.ensure_one()
        self.env.cr.execute('SELECT id FROM mrp_rollo_bitacora WHERE id = %s FOR UPDATE', [self.bitacora_id.id])
        self.env.invalidate_all()
        emp = self._validar()
        operador = ' - '.join(filter(None, [self.empleado_numero, emp.name or False]))
        tramo = self.env['mrp.rollo.tramo'].create({
            'production_id': self.production_id.id,
            'bitacora_id': self.bitacora_id.id,
            'kg_bruto': self.kg_bruto,
            'tara': self.tara_kg,
            'kg_neto': self.kg_neto,
            'captura_manual': self.weighing_mode == 'manual',
            'motivo_manual': (self.motivo_manual or False) if self.weighing_mode == 'manual' else False,
            'metros': self.metros,
            'defecto_ids': [(6, 0, self.defecto_ids.ids)],
            'workcenter_id': self.workcenter_id.id,
            'operador': operador,
        })
        self.production_id.message_post(body=_(
            'Tramo de %(kg).2f kg y %(m).2f m quitado al rollo %(r)s por %(o)s (defectos: %(d)s). Queda pendiente de '
            'juntar en un rollo de FE o Segunda.', kg=tramo.kg_neto, m=tramo.metros, r=self.bitacora_id.name, o=operador,
            d=tramo.siglas_defectos or '-'))
        return self._volver()


class RolloTramosJuntarWizard(models.TransientModel):
    _name = 'rollo.tramos.juntar.wizard'
    _inherit = ['scale.wizard.mixin']
    _description = 'Inspección: juntar tramos en un rollo de FE o Segunda'

    production_id = fields.Many2one('mrp.production', 'Orden (kg)', required=True, readonly=True)
    workorder_id = fields.Many2one('mrp.workorder', 'Operación de Inspección', required=True, readonly=True)
    product_id = fields.Many2one(related='production_id.product_id', string='Artículo')
    workcenter_id = fields.Many2one(
        'mrp.workcenter', 'Revisadora', required=True, default=lambda self: self._default_workcenter(),
        domain="[('ft_tipo_id.name', '=', 'Inspección')]")
    empleado_numero = fields.Char('Número de empleado', required=True)
    empleado_nombre = fields.Char('Operador', compute='_compute_empleado')

    tramo_ids = fields.Many2many('mrp.rollo.tramo', 'rollo_tramos_juntar_tramo_rel', 'wizard_id', 'tramo_id',
                                 'Tramos a juntar', domain="[('production_id', '=', production_id), ('rollo_final_id', '=', False)]")
    n_tramos = fields.Integer('Tramos', compute='_compute_tramos')
    kg_tramos = fields.Float('Kg de los tramos', compute='_compute_tramos', digits=(12, 4))
    metros_tramos = fields.Float('Metros de los tramos', compute='_compute_tramos', digits=(12, 2))

    kg_bruto = fields.Float('Peso bruto (kg)', digits=(12, 4))
    tara_kg = fields.Float('Tara de Inspección (kg)', compute='_compute_pesos', digits=(12, 4))
    tara_faltante = fields.Boolean(compute='_compute_pesos')
    kg_neto = fields.Float('Peso neto (kg)', compute='_compute_pesos', digits=(12, 4))
    motivo_manual = fields.Char('Motivo de la captura manual', default=lambda self: motivo_por_defecto(self))
    weighing_mode = fields.Selection(default=lambda self: modo_por_defecto(self))
    metros = fields.Float('Metros del rollo', digits=(12, 2), help='Propone la suma de los tramos: remídelo si cambió.')
    no_empalmes = fields.Integer('No. de empalmes')
    clasificacion = fields.Selection(CALIDADES_TRAMOS, 'Calidad')
    defecto_ids = fields.Many2many('calidad.defecto.tela', 'rollo_tramos_juntar_defecto_rel', 'wizard_id', 'defecto_id',
                                   'Defectos', domain="[('tipo_tela', '=', 'tejido_acabado')]",
                                   help='Propone la unión de los defectos de los tramos.')
    lote_name = fields.Char('Lote del rollo de tramos', compute='_compute_lote')
    alertas = fields.Text('Alertas', compute='_compute_alertas')

    @api.model
    def _default_workcenter(self):
        return self.env['mrp.workorder'].browse(self.env.context.get('default_workorder_id')).workcenter_id.id or False

    @api.model
    def default_get(self, fields_list):
        res = super().default_get(fields_list)
        orden = self.env['mrp.production'].browse(res.get('production_id') or self.env.context.get('default_production_id'))
        if orden:
            tramos = orden.tramo_ids.filtered(lambda t: not t.rollo_final_id)
            res['tramo_ids'] = [(6, 0, tramos.ids)]
            res['metros'] = sum(tramos.mapped('metros'))
            res['defecto_ids'] = [(6, 0, tramos.defecto_ids.ids)]
        return res

    @api.depends('empleado_numero')
    def _compute_empleado(self):
        for wiz in self:
            emp = self.env['hr.employee']._qb_operador(wiz.empleado_numero)
            wiz.empleado_nombre = emp.name if emp else (_('EMPLEADO NO ENCONTRADO') if wiz.empleado_numero else False)

    @api.depends('tramo_ids')
    def _compute_tramos(self):
        for wiz in self:
            wiz.n_tramos = len(wiz.tramo_ids)
            wiz.kg_tramos = sum(wiz.tramo_ids.mapped('kg_neto'))
            wiz.metros_tramos = sum(wiz.tramo_ids.mapped('metros'))

    @api.depends('product_id', 'kg_bruto')
    def _compute_pesos(self):
        Tara = self.env['mrp.tara.producto']
        for wiz in self:
            tara = Tara._buscar_tara(wiz.product_id, 'clasificacion_final') if wiz.product_id else Tara
            wiz.tara_faltante = bool(wiz.product_id) and not tara
            wiz.tara_kg = tara.tara_kg
            wiz.kg_neto = wiz.kg_bruto - tara.tara_kg if tara else 0.0

    @api.depends('production_id')
    def _compute_lote(self):
        Final = self.env['mrp.rollo.final']
        for wiz in self:
            orden_kg = wiz.production_id
            orden_m = orden_kg._insp_orden_metros() if orden_kg else self.env['mrp.production']
            wiz.lote_name = Final._nombre_lote(orden_m, Final._siguiente_secuencia(orden_m, orden_kg), orden_kg) \
                if orden_kg else False

    @api.depends('kg_neto', 'kg_tramos', 'metros', 'metros_tramos')
    def _compute_alertas(self):
        params = self.env['ir.config_parameter'].sudo()
        limite = float(params.get_param(PARAM_ALERTA_MERMA_PCT, 3.0))
        limite_m = float(params.get_param(PARAM_ALERTA_DIFERENCIA_PCT, 3.0))
        for wiz in self:
            avisos = []
            if wiz.kg_neto and wiz.kg_tramos and abs(wiz.kg_neto - wiz.kg_tramos) > wiz.kg_tramos * limite / 100.0:
                avisos.append(_('El rollo armado pesa %(n).2f kg y la suma de los tramos %(t).2f kg (diferencia mayor '
                                'a %(l).1f %%): revisa el peso o los tramos.', n=wiz.kg_neto, t=wiz.kg_tramos, l=limite))
            if wiz.metros and wiz.metros_tramos and abs(wiz.metros - wiz.metros_tramos) > wiz.metros_tramos * limite_m / 100.0:
                avisos.append(_('Mides %(m).2f m y la suma de los tramos es %(t).2f m (diferencia mayor a %(l).1f %%).',
                                m=wiz.metros, t=wiz.metros_tramos, l=limite_m))
            wiz.alertas = '\n'.join(avisos) or False

    def _validar(self):
        self.ensure_one()
        orden = self.production_id
        if orden.state in ('done', 'cancel'):
            raise UserError(_('La orden %s ya está cerrada o cancelada: no se juntan tramos.', orden.name))
        sin_inspeccionar = orden.rollo_bitacora_ids.filtered(lambda r: r.estado_inspeccion != 'inspeccionado')
        if sin_inspeccionar:
            raise UserError(_(
                'Faltan rollos de Rama por inspeccionar (%(n)s): los tramos se juntan cuando todos los rollos del baño '
                'están inspeccionados.', n=', '.join(sin_inspeccionar.mapped('name'))))
        if not self.tramo_ids:
            raise UserError(_('No hay tramos por juntar: quita primero el tramo de un rollo con defectos.'))
        ajenos = self.tramo_ids.filtered(lambda t: t.production_id != orden or t.rollo_final_id)
        if ajenos:
            raise UserError(_('Solo se juntan tramos pendientes de este baño.'))
        if self.tara_faltante:
            raise UserError(_(
                'No hay tara de Inspección para %s. Captúrala en Manufactura → Configuración → Taras por producto '
                '(operación «Clasificación final»); el pesaje no asume tara 0.', self.product_id.display_name))
        if self.kg_bruto <= 0:
            raise UserError(_('Captura el peso bruto del rollo armado.'))
        if self.kg_neto <= 0:
            raise UserError(_('El peso bruto (%(b).4f kg) no supera la tara (%(t).4f kg).', b=self.kg_bruto,
                              t=self.tara_kg))
        if self.weighing_mode == 'manual' and not (self.motivo_manual or '').strip():
            raise UserError(_('Captura el motivo de la captura manual del peso.'))
        if self.metros <= 0:
            raise UserError(_('Captura los metros del rollo.'))
        if not self.clasificacion:
            raise UserError(_('Elige la calidad del rollo de tramos: FE o Segunda.'))
        if not self.defecto_ids:
            raise UserError(_('Captura al menos un defecto: un rollo de %s necesita la causa.',
                              dict(CALIDADES_TRAMOS).get(self.clasificacion)))
        emp = self.env['hr.employee']._qb_operador_requerido(self.empleado_numero)
        if emp and not emp.siglas:
            raise UserError(_('El operador %s no tiene siglas: captúralas en el empleado (van en la etiqueta).', emp.name))
        self.env['mrp.inspeccion.restriccion']._validar_centro(self.workcenter_id, self.product_id)
        return emp

    def action_confirmar(self):
        self.ensure_one()
        orden_kg = self.production_id
        orden_m = orden_kg._insp_orden_metros()
        ordenes = (orden_m | orden_kg).ids
        self.env.cr.execute('SELECT id FROM mrp_production WHERE id IN %s FOR UPDATE', [tuple(ordenes)])
        self.env.invalidate_all()
        emp = self._validar()
        Final = self.env['mrp.rollo.final']
        orden_kg._asignar_numero_bano()
        secuencia = Final._siguiente_secuencia(orden_m, orden_kg)
        ahora = fields.Datetime.now()
        operador = ' - '.join(filter(None, [self.empleado_numero, emp.name or False]))
        rollo = Final.create({
            'name': Final._nombre_lote(orden_m, secuencia, orden_kg),
            'secuencia': secuencia,
            'production_id': orden_kg.id,
            'production_m_id': orden_m.id or False,
            'es_tramos': True,
            'kg_bruto': self.kg_bruto,
            'tara': self.tara_kg,
            'kg_neto': self.kg_neto,
            'captura_manual': self.weighing_mode == 'manual',
            'motivo_manual': (self.motivo_manual or False) if self.weighing_mode == 'manual' else False,
            'metros': self.metros,
            'no_empalmes': self.no_empalmes,
            'clasificacion': self.clasificacion,
            'defecto_ids': [(6, 0, self.defecto_ids.ids)],
            'workcenter_id': self.workcenter_id.id,
            'operador': operador,
            'siglas_operador': emp.siglas or False,
            'fecha': ahora,
        })
        self.tramo_ids.write({'rollo_final_id': rollo.id})
        rollo.zpl_etiqueta = rollo._zpl_etiqueta()
        rollo.zpl_etiqueta_preliminar = rollo._zpl_etiqueta_preliminar()
        orden_kg._historial_pesaje(self.workcenter_id, rollo.name, self.kg_bruto, self.tara_kg, rollo.kg_neto, operador,
                                   fecha=ahora)
        orden_kg.message_post(body=_(
            'Rollo de tramos %(r)s (%(c)s, %(kg).2f kg, %(m).2f m) armado por %(o)s con %(n)s tramo(s) de los rollos '
            '%(rr)s.', r=rollo.name, c=dict(CALIDADES_TRAMOS).get(self.clasificacion), kg=rollo.kg_neto, m=rollo.metros,
            o=operador, n=len(self.tramo_ids), rr=', '.join(self.tramo_ids.bitacora_id.mapped('name'))))
        if Final._accion_al_confirmar() == 'final':
            accion = rollo.action_imprimir_etiqueta()
        else:
            accion = rollo.action_imprimir_etiqueta_preliminar()
        accion.update({'close_on_report_download': True})
        return accion
