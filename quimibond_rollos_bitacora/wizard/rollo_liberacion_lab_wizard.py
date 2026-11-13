# -*- coding: utf-8 -*-
from odoo import api, fields, models, _
from odoo.exceptions import UserError


class RolloLiberacionLabWizard(models.TransientModel):
    """Liberación de la orden en metros para Empaque por laboratorio de control de calidad (3.20.0)."""
    _name = 'rollo.liberacion.lab.wizard'
    _description = 'Liberación de laboratorio para Empaque'

    production_domain_ids = fields.Many2many('mrp.production', compute='_compute_production_domain_ids')
    production_id = fields.Many2one('mrp.production', 'Orden de fabricación en metros', required=True)
    workorder_id = fields.Many2one('mrp.workorder', 'Operación', compute='_compute_workorder_id', store=False)
    empleado_numero = fields.Char('Número de empleado', required=True)
    empleado_nombre = fields.Char('Laboratorista', compute='_compute_empleado_nombre')
    rollos_pq_count = fields.Integer('Rollos PQ revisados', compute='_compute_rollos_pq')
    rollos_pq = fields.Char('Rollos', compute='_compute_rollos_pq')

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

    @api.depends('empleado_numero')
    def _compute_empleado_nombre(self):
        Empleado = self.env['hr.employee']
        for wiz in self:
            nombre = False
            if wiz.empleado_numero:
                emp = Empleado.sudo()._qb_operador(wiz.empleado_numero)
                nombre = emp.name if emp else _('EMPLEADO NO ENCONTRADO')
            wiz.empleado_nombre = nombre

    @api.depends('production_id')
    def _compute_rollos_pq(self):
        for wiz in self:
            rollos = wiz.production_id._rollos_pq_por_revisar() if wiz.production_id else self.env['mrp.rollo.final']
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
        if not orden._rollos_pq_por_revisar():
            raise UserError(_('La orden %s no tiene rollos PQ por revisar: no necesita liberación.', orden.name))
        empleado = self.env['hr.employee'].sudo()._qb_operador_requerido(self.empleado_numero)
        self._validar_puesto(empleado)
        operador = ' - '.join(filter(None, [self.empleado_numero, empleado.name if empleado else False]))
        orden.write({
            'liberada_lab': True,
            'liberada_lab_operador': operador,
            'liberada_lab_fecha': fields.Datetime.now(),
        })
        orden.message_post(body=_(
            'Orden liberada para Empaque por laboratorio (%(p)s). Rollos PQ revisados: %(r)s.',
            p=operador, r=self.rollos_pq))
        return {'type': 'ir.actions.act_window_close'}
