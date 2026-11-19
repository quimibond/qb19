# -*- coding: utf-8 -*-
from odoo import Command, api, fields, models, _
from odoo.exceptions import UserError

from ..models.mrp_rollo_bitacora import CLASIFICACIONES

CALIDADES_LAB = [c for c in CLASIFICACIONES if c[0] in ('PQ', 'FE', 'SEGUNDA')]


class RolloLiberacionLabWizard(models.TransientModel):
    """Liberación de la orden en metros para Empaque por laboratorio de control de calidad (3.20.0). Desde 3.21.0
    registra además la calidad final de cada rollo PQ revisado (Junta de Calidad, 2026-11-13)."""
    _name = 'rollo.liberacion.lab.wizard'
    _description = 'Liberación de laboratorio para Empaque'

    production_domain_ids = fields.Many2many('mrp.production', compute='_compute_production_domain_ids')
    production_id = fields.Many2one('mrp.production', 'Orden de fabricación en metros', required=True)
    workorder_id = fields.Many2one('mrp.workorder', 'Operación', compute='_compute_workorder_id', store=False)
    empleado_numero = fields.Char('Número de empleado', required=True)
    empleado_nombre = fields.Char('Laboratorista', compute='_compute_empleado_nombre')
    empleado_ok = fields.Boolean(compute='_compute_empleado_nombre')
    rollos_pq_count = fields.Integer('Rollos PQ revisados', compute='_compute_rollos_pq')
    rollos_pq = fields.Char('Rollos', compute='_compute_rollos_pq')
    linea_ids = fields.One2many('rollo.liberacion.lab.linea', 'wizard_id', 'Calidad final de los rollos PQ',
                                compute='_compute_lineas', store=True, readonly=False)

    @api.depends_context('uid')
    def _compute_production_domain_ids(self):
        # Menú Calidad → Liberar para Empaque: solo órdenes en metros con rollos PQ por revisar.
        ordenes = self.env['mrp.production'].sudo()._ordenes_clasificacion(solo_liberacion_pendiente=True)
        for wiz in self:
            wiz.production_domain_ids = ordenes

    @api.depends('production_id')
    def _compute_workorder_id(self):
        for wiz in self:
            wiz.workorder_id = wiz.production_id.workorder_ids.filtered('es_clasificacion')[:1]

    @api.depends('production_id')
    def _compute_lineas(self):
        for wiz in self:
            rollos = wiz.production_id.sudo()._rollos_pq_por_revisar() if wiz.production_id else self.env['mrp.rollo.final']
            wiz.linea_ids = [Command.clear()] + [
                Command.create({'rollo_id': r.id, 'clasificacion': 'PQ'}) for r in rollos]

    @api.depends('empleado_numero')
    def _compute_empleado_nombre(self):
        Empleado = self.env['hr.employee']
        for wiz in self:
            nombre = False
            emp = Empleado.browse()
            if wiz.empleado_numero:
                emp = Empleado.sudo()._qb_operador(wiz.empleado_numero)
                nombre = emp.name if emp else (
                    Empleado._qb_aviso_numero_repetido(wiz.empleado_numero) or _('EMPLEADO NO ENCONTRADO'))
            wiz.empleado_nombre = nombre
            wiz.empleado_ok = bool(emp)

    @api.depends('production_id')
    def _compute_rollos_pq(self):
        for wiz in self:
            rollos = wiz.production_id.sudo()._rollos_pq_por_revisar() if wiz.production_id else self.env['mrp.rollo.final']
            wiz.rollos_pq_count = len(rollos)
            wiz.rollos_pq = ', '.join(rollos.mapped('name'))

    def _validar_puesto(self, empleado):
        """Gancho para validar que el empleado tenga el puesto autorizado a liberar. Por ahora basta el número de
        empleado (José, 2026-11-12); cuando Quimibond indique los puestos, se valida aquí."""
        return True

    def action_confirmar(self):
        self.ensure_one()
        orden = self.production_id.sudo()
        if orden.state in ('done', 'cancel'):
            raise UserError(_('La orden %s ya está cerrada o cancelada.', orden.name))
        if orden.liberada_lab:
            raise UserError(_('La orden %(o)s ya fue liberada por %(p)s.', o=orden.name, p=orden.liberada_lab_operador))
        pendientes = orden._rollos_pq_por_revisar()
        if not pendientes:
            raise UserError(_('La orden %s no tiene rollos PQ por revisar: no necesita liberación.', orden.name))
        if set(self.linea_ids.rollo_id.ids) != set(pendientes.ids):
            raise UserError(_('Los rollos por revisar de %s cambiaron desde que abriste la pantalla. Ciérrala y vuelve '
                              'a abrirla.', orden.name))
        for linea in self.linea_ids:
            linea._validar()
        empleado = self.env['hr.employee'].sudo()._qb_operador_requerido(self.empleado_numero)
        self._validar_puesto(empleado)
        operador = ' - '.join(filter(None, [self.empleado_numero, empleado.name if empleado else False]))
        ahora = fields.Datetime.now()
        cambios = []
        for linea in self.linea_ids:
            rollo = linea.rollo_id.sudo()
            cambia = linea.clasificacion != 'PQ'
            rollo.write({
                'clasificacion_lab': linea.clasificacion,
                'defecto_lab_ids': [Command.set(linea.defecto_ids.ids if cambia else [])],
                'motivo_lab': (linea.motivo or '').strip() if cambia else False,
                'operador_lab': operador,
                'fecha_lab': ahora,
            })
            if cambia:
                cambios.append(_('%(r)s: PQ → %(c)s (%(d)s). Causa: %(m)s', r=rollo.name,
                                 c=dict(CLASIFICACIONES).get(linea.clasificacion),
                                 d=', '.join(linea.defecto_ids.mapped('clave')), m=linea.motivo))
        orden.write({
            'liberada_lab': True,
            'liberada_lab_operador': operador,
            'liberada_lab_fecha': ahora,
        })
        cuerpo = _('Orden liberada para Empaque por laboratorio (%(p)s). Rollos PQ revisados: %(r)s.',
                   p=operador, r=self.rollos_pq)
        if cambios:
            cuerpo += '<br/>' + _('Cambios de calidad:') + '<br/>' + '<br/>'.join(cambios)
        orden.message_post(body=cuerpo)
        return {'type': 'ir.actions.act_window_close'}


class RolloLiberacionLabLinea(models.TransientModel):
    """Un rollo PQ por revisar: la calidad final que le da control de calidad."""
    _name = 'rollo.liberacion.lab.linea'
    _description = 'Liberación de laboratorio: calidad final de un rollo'
    _order = 'id'

    wizard_id = fields.Many2one('rollo.liberacion.lab.wizard', required=True, ondelete='cascade')
    rollo_id = fields.Many2one('mrp.rollo.final', 'Rollo', required=True, readonly=True)
    clasificacion_insp = fields.Selection(related='rollo_id.clasificacion', string='Inspección')
    metros = fields.Float(related='rollo_id.metros', string='Metros', digits=(12, 2))
    clasificacion = fields.Selection(CALIDADES_LAB, 'Calidad final', required=True, default='PQ')
    defecto_ids = fields.Many2many('calidad.defecto.tela', 'rollo_liberacion_lab_linea_defecto_rel', 'linea_id',
                                   'defecto_id', 'Defectos', domain="[('tipo_tela', '=', 'tejido_acabado')]")
    motivo = fields.Char('Causa del cambio')

    def _validar(self):
        self.ensure_one()
        if self.clasificacion != 'PQ':
            nombre = dict(CALIDADES_LAB).get(self.clasificacion)
            if not self.defecto_ids:
                raise UserError(_('Rollo %(r)s: captura al menos un defecto, un rollo %(c)s necesita la causa.',
                                  r=self.rollo_id.name, c=nombre))
            if not (self.motivo or '').strip():
                raise UserError(_('Rollo %(r)s: captura la causa del cambio de PQ a %(c)s.', r=self.rollo_id.name,
                                  c=nombre))
