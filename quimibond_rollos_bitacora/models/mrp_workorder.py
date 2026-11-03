# -*- coding: utf-8 -*-
from odoo import api, fields, models, _
from odoo.exceptions import UserError


class MrpWorkorder(models.Model):
    _inherit = 'mrp.workorder'

    es_rama = fields.Boolean('Es operación de Rama', compute='_compute_es_rama')
    es_inspeccion = fields.Boolean('Es operación de Inspección', compute='_compute_es_inspeccion')
    es_clasificacion = fields.Boolean('Es operación de Clasificación final', compute='_compute_es_clasificacion')

    @api.depends('workcenter_id.name', 'workcenter_id.ft_tipo_id.name')
    def _compute_es_rama(self):
        """Rama = centro de tipo de máquina «Acabado» (fichas técnicas, §3) o, si no tiene tipo,
        de nombre RAMA."""
        for wo in self:
            centro = wo.workcenter_id
            tipo = centro.ft_tipo_id
            wo.es_rama = (tipo.name == 'Acabado') if tipo else 'RAMA' in (centro.name or '').upper()

    @api.depends('workcenter_id.name', 'workcenter_id.ft_tipo_id.name')
    def _compute_es_inspeccion(self):
        """Inspección = centro de tipo de máquina «Inspección» o, sin tipo, de nombre INSPECCION."""
        for wo in self:
            centro = wo.workcenter_id
            tipo = centro.ft_tipo_id
            wo.es_inspeccion = (tipo.name == 'Inspección') if tipo else 'INSPEC' in (centro.name or '').upper()

    @api.depends('workcenter_id.name', 'name')
    def _compute_es_clasificacion(self):
        """Clasificación final = operación en el centro EMPAQUE (diseño §3) o de nombre CLASIFIC…"""
        for wo in self:
            wo.es_clasificacion = 'EMPAQUE' in (wo.workcenter_id.name or '').upper() \
                or 'CLASIFIC' in (wo.name or '').upper()

    def action_clasificar_rollo(self):
        return self._abrir_ventana_operacion(
            self.es_clasificacion, 'rollo.clasificacion.wizard', _('Clasificar rollo (Clasificación final)'),
            'Clasificación final')

    def _abrir_ventana_operacion(self, es_tipo, modelo, titulo, nombre_operacion):
        """Abre un wizard desde la lista de operaciones (mismo patrón que PESAR en pesaje_rollos_tejido)."""
        self.ensure_one()
        if not es_tipo:
            raise UserError(_('Esta operación no es de %s.', nombre_operacion))
        if self.state not in ('ready', 'progress'):
            raise UserError(_(
                'La operación de %s de %s no está lista (estado actual: %s). Termina primero las operaciones '
                'anteriores.', nombre_operacion, self.production_id.name,
                dict(self._fields['state'].selection).get(self.state)))
        return {
            'type': 'ir.actions.act_window',
            'name': titulo,
            'res_model': modelo,
            'view_mode': 'form',
            'target': 'new',
            'context': {'default_production_id': self.production_id.id, 'default_workorder_id': self.id},
        }

    def action_pesar_rollo_rama(self):
        return self._abrir_ventana_operacion(
            self.es_rama, 'rollo.pesaje.rama.wizard', _('Pesar rollo (Rama)'), 'Rama')

    def action_pesar_desperdicio_rama(self):
        return self._abrir_ventana_operacion(
            self.es_rama, 'rollo.desperdicio.rama.wizard', _('Pesar desperdicio (Rama)'), 'Rama')

    def action_inspeccionar_rollo(self):
        return self._abrir_ventana_operacion(
            self.es_inspeccion, 'rollo.inspeccion.wizard', _('Inspeccionar rollo'), 'Inspección')

    def button_finish(self):
        # §10: la operación de Inspección debe terminar todos los rollos registrados en Rama.
        for wo in self.filtered('es_inspeccion'):
            wo.production_id._validar_rollos_inspeccionados()
        return super().button_finish()
