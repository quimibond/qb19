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
        if self.env['mrp.production']._insp_empaque_activo():
            # 3.12.0: con el flujo de Empaque la operación abre la estación de Clasificación final de la orden en metros.
            self.ensure_one()
            if not self.es_clasificacion:
                raise UserError(_('Esta operación no es de Clasificación final.'))
            return self.production_id.with_context(default_workorder_id=self.id).action_clasificar_empaque()
        return self._abrir_ventana_operacion(
            self.es_clasificacion, 'rollo.clasificacion.wizard', _('Clasificar rollo (Clasificación final)'),
            'Clasificación final')

    def _abrir_ventana_operacion(self, es_tipo, modelo, titulo, nombre_operacion):
        """Abre un wizard desde la lista de operaciones (mismo patrón que PESAR en pesaje_rollos_tejido)."""
        self.ensure_one()
        if not es_tipo:
            raise UserError(_('Esta operación no es de %s.', nombre_operacion))
        if self.state not in ('ready', 'progress') and not self._insp_en_paralelo(es_tipo, modelo):
            raise UserError(_(
                'La operación de %s de %s no está lista (estado actual: %s). Termina primero las operaciones '
                'anteriores.', nombre_operacion, self.production_id.name,
                dict(self._fields['state'].selection).get(self.state)))
        if modelo != 'rollo.clasificacion.wizard':  # 3.5.0: los lotes del baño llevan su número
            self.production_id._asignar_numero_bano()
        return {
            'type': 'ir.actions.act_window',
            'name': titulo,
            'res_model': modelo,
            'view_mode': 'form',
            'target': 'new',
            'context': {'default_production_id': self.production_id.id, 'default_workorder_id': self.id},
        }

    def _insp_en_paralelo(self, es_tipo, modelo):
        """3.3.5: Inspección empieza mientras Rama sigue. En Odoo 19 la operación pasa sola de «bloqueada» a «por hacer»
        en cuanto Rama produce (cada pesaje sube ``qty_produced`` de Rama; prueba en main, 2026-11-05); esto cubre el
        caso de una operación que siguiera bloqueada con rollos pendientes. La operación de Inspección no termina ni la
        orden cierra hasta que Rama terminó y todos sus rollos están inspeccionados (``button_finish``)."""
        self.ensure_one()
        return (modelo == 'rollo.inspeccion.estacion.wizard' and es_tipo and self.state == 'blocked'
                and self.production_id.state in ('confirmed', 'progress', 'to_close')
                and any(r.estado_inspeccion != 'inspeccionado' for r in self.production_id.rollo_bitacora_ids))

    def button_start(self, *args, **kwargs):
        # 3.5.0: el baño recibe su número al empezar su primera operación (Tintorería lo usa al cargar el jet).
        self.production_id.filtered(lambda o: any(o.workorder_ids.mapped('es_rama')))._asignar_numero_bano()
        return super().button_start(*args, **kwargs)

    def action_pesar_rollo_rama(self):
        return self._abrir_ventana_operacion(
            self.es_rama, 'rollo.pesaje.rama.wizard', _('Pesar rollo (Rama)'), 'Rama')

    def action_pesar_desperdicio_rama(self):
        return self._abrir_ventana_operacion(
            self.es_rama, 'rollo.desperdicio.rama.wizard', _('Pesar desperdicio (Rama)'), 'Rama')

    def action_inspeccionar_rollo(self):
        # 3.1.0: Inspección como estación única (diseño 2026-11-03): pesa, corta o junta, clasifica y etiqueta.
        return self._abrir_ventana_operacion(
            self.es_inspeccion, 'rollo.inspeccion.estacion.wizard', _('Inspeccionar rollo'), 'Inspección')

    def button_finish(self):
        # 3.6.1: Rama de un baño del flujo nuevo no termina sin rollos pesados.
        self.filtered('es_rama').production_id._validar_rollos_pesados()
        # §10: la operación de Inspección debe terminar todos los rollos registrados en Rama.
        for wo in self.filtered('es_inspeccion'):
            # 3.3.5: con Inspección en paralelo, no termina mientras Rama siga pesando rollos.
            # El cierre nativo de la orden termina todas sus operaciones juntas: ahí Rama también termina y no se frena.
            rama = wo.production_id.workorder_ids.filtered(
                lambda w: w.es_rama and w.state not in ('done', 'cancel') and w not in self)
            if rama:
                raise UserError(_('Inspección de %(o)s no termina mientras Rama siga abierta (%(r)s): pueden salir más '
                                  'rollos. Termina primero Rama.', o=wo.production_id.name,
                                  r=', '.join(rama.mapped('name'))))
            wo.production_id._validar_rollos_inspeccionados()
        return super().button_finish()
