# -*- coding: utf-8 -*-
"""Liberación de laboratorio para Empaque (3.20.0, José 2026-11-12).

En la orden en metros (OP-M), el laboratorio de control de calidad revisa los rollos que Inspección clasificó como PQ y
confirma que la orden quedó liberada para que Empaque registre la clasificación final. La revisión se hace en el libro de
hoja de cálculo de Inspección (módulo ``quimibond_hojas_calidad``); aquí solo vive la liberación:

- La libera quien teclea su número de empleado, como en los pesajes. Más adelante se validará que el empleado tenga el
  puesto autorizado (``rollo.liberacion.lab.wizard._validar_puesto``); por ahora basta el número.
- Una orden sin rollos PQ por revisar no necesita liberación: Empaque clasifica directo.
- Mientras la liberación esté pendiente, CLASIFICAR no abre y la orden en metros no cierra.
"""
from odoo import api, fields, models, _
from odoo.exceptions import UserError


class MrpProduction(models.Model):
    _inherit = 'mrp.production'

    liberada_lab = fields.Boolean('Liberada por laboratorio', readonly=True, copy=False)
    liberada_lab_operador = fields.Char('Liberada por', readonly=True, copy=False)
    liberada_lab_fecha = fields.Datetime('Liberada el', readonly=True, copy=False)
    liberacion_lab_pendiente = fields.Boolean(
        'Pendiente de liberación de laboratorio', compute='_compute_liberacion_lab_pendiente',
        help='La orden en metros tiene rollos PQ por revisar en laboratorio y todavía no se libera para Empaque.')

    @api.depends('state', 'liberada_lab', 'rollo_final_m_ids.clasificacion', 'rollo_final_m_ids.estado')
    def _compute_liberacion_lab_pendiente(self):
        for orden in self:
            orden.liberacion_lab_pendiente = bool(
                not orden.liberada_lab and orden._insp_orden_m_en_empaque() and orden._rollos_pq_por_revisar())

    @api.model
    def _ordenes_clasificacion(self, solo_liberacion_pendiente=False):
        """Órdenes en metros abiertas con operación de Clasificación final (las que laboratorio revisa y libera). Con
        ``solo_liberacion_pendiente``, solo las que tienen rollos PQ por revisar sin liberar."""
        operaciones = self.env['mrp.workorder'].search([
            ('state', 'not in', ('done', 'cancel')), ('production_id.state', 'not in', ('done', 'cancel'))])
        ordenes = operaciones.filtered('es_clasificacion').production_id
        if solo_liberacion_pendiente:
            ordenes = ordenes.filtered('liberacion_lab_pendiente')
        return ordenes

    def _rollos_pq_por_revisar(self):
        """Rollos finales que Inspección clasificó como PQ y Empaque todavía no clasifica: son los que revisa laboratorio.
        Los que Empaque ya clasificó no cuentan (órdenes en curso al desplegar esta versión)."""
        self.ensure_one()
        return self.rollo_final_m_ids.filtered(
            lambda r: r.estado == 'inspeccionado' and r.clasificacion == 'PQ').sorted('secuencia')

    def _validar_liberacion_lab(self):
        for orden in self:
            if orden.liberacion_lab_pendiente:
                raise UserError(_(
                    'La orden %(o)s tiene %(n)s rollos PQ por revisar en laboratorio. Laboratorio debe liberarla con '
                    '«LIBERAR PARA EMPAQUE» antes de clasificar o cerrar.', o=orden.name,
                    n=len(orden._rollos_pq_por_revisar())))

    def action_liberar_empaque(self):
        """Abre la ventana con la que laboratorio libera la orden en metros para Empaque."""
        self.ensure_one()
        if self.state in ('done', 'cancel'):
            raise UserError(_('La orden %s ya está cerrada o cancelada.', self.name))
        if self.liberada_lab:
            raise UserError(_('La orden %(o)s ya fue liberada por %(p)s.', o=self.name, p=self.liberada_lab_operador))
        if not self.liberacion_lab_pendiente:
            raise UserError(_('La orden %s no tiene rollos PQ por revisar: no necesita liberación.', self.name))
        return {
            'type': 'ir.actions.act_window',
            'name': _('Liberar para Empaque'),
            'res_model': 'rollo.liberacion.lab.wizard',
            'view_mode': 'form',
            'target': 'new',
            'context': {'default_production_id': self.id, **(
                {'default_workorder_id': self.env.context['default_workorder_id']}
                if self.env.context.get('default_workorder_id') else {})},
        }

    def action_clasificar_empaque(self):
        self._validar_liberacion_lab()
        return super().action_clasificar_empaque()

    def button_mark_done(self):
        # Mismo criterio que el cierre desde Empaque (3.11.0): la llamada interna de ese cierre ya pasó por aquí.
        if not self.env.context.get('qb_insp_desde_empaque'):
            self.filtered(lambda o: o._insp_orden_m_en_empaque())._validar_liberacion_lab()
        return super().button_mark_done()
