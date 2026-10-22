# -*- coding: utf-8 -*-
from odoo import _, api, fields, models
from odoo.exceptions import UserError
from odoo.tools import float_compare


class MrpProduction(models.Model):
    _name = 'mrp.production'
    _inherit = ['mrp.production', 'tintoreria.dosificacion.mixin']

    tiene_dosificacion = fields.Boolean(
        'Tiene dosificación', compute='_compute_tiene_dosificacion',
        help="La lista de materiales tiene químicos/colorantes dosificados (g/L o %). "
             "Solo en esas órdenes (p. ej. OP-KG) aplica el recálculo.")

    @api.depends('bom_id.bom_line_ids.dosificado')
    def _compute_tiene_dosificacion(self):
        for orden in self:
            orden.tiene_dosificacion = any(orden.bom_id.bom_line_ids.mapped('dosificado'))

    # ------------------------------------------------------------------
    # Cálculo
    # ------------------------------------------------------------------
    def _dosificacion_kg_orden(self):
        self.ensure_one()
        try:
            return self.product_uom_id._compute_quantity(self.product_qty, self._kg_uom())
        except UserError:
            return self.product_qty

    def _dosificacion_contexto(self, operacion):
        """(work center vigente, relación de baño) para la operación: el de la orden de trabajo
        si ya existe; si no, el work center por defecto de la operación de la BoM."""
        self.ensure_one()
        orden_trabajo = self.workorder_ids.filtered(lambda wo: wo.operation_id == operacion)[:1]
        centro = orden_trabajo.workcenter_id or operacion.workcenter_id
        return centro, self._rb_workcenter(centro, self.product_id)

    def _dosificacion_operacion(self, linea_bom):
        """Operación donde se dosifica la línea: la suya o, si no tiene, la de Tintorería de la BoM."""
        return linea_bom.operation_id or linea_bom.bom_id._operacion_tintoreria()

    def _dosificacion_qty(self, linea_bom, uom):
        """Cantidad a surtir de la línea de BoM dosificada, en la UdM indicada."""
        self.ensure_one()
        _centro, rb = self._dosificacion_contexto(self._dosificacion_operacion(linea_bom))
        dosis_kg = self._calcular_dosis_kg(
            linea_bom.tipo_componente, linea_bom.gramos_por_litro, linea_bom.porcentaje_colorante,
            self._dosificacion_kg_orden(), rb)
        return self._kg_uom()._compute_quantity(dosis_kg, uom)

    def _get_move_raw_values(self, product, product_uom_qty, product_uom, operation_id=False, bom_line=False):
        datos = super()._get_move_raw_values(product, product_uom_qty, product_uom, operation_id, bom_line)
        if bom_line and bom_line.dosificado:
            datos['product_uom_qty'] = self._dosificacion_qty(bom_line, product_uom)
        return datos

    # ------------------------------------------------------------------
    # Recálculo
    # ------------------------------------------------------------------
    def _recalcular_dosificacion(self, estricto=False):
        """Recalcula los consumos dosificados de la orden.

        * No toca movimientos hechos, cancelados ni consumidos (picked).
        * Si la requisición ligada ya se surtió (algún movimiento origen en 'done'), conserva
          la cantidad vigente.
        * Ajusta también la cantidad de la requisición aún no surtida (diferencia).
        * ``estricto``: exige relación de baño para los químicos y falla con un mensaje claro.
        """
        for orden in self:
            if orden.state in ('done', 'cancel'):
                continue
            cambios = []
            movimientos = orden.move_raw_ids.filtered(
                lambda m: m.bom_line_id.dosificado and m.state not in ('done', 'cancel') and not m.picked)
            for mov in movimientos:
                linea = mov.bom_line_id
                centro, rb = orden._dosificacion_contexto(orden._dosificacion_operacion(linea))
                if linea.tipo_componente == 'quimico' and not rb:
                    if estricto:
                        raise UserError(_(
                            "No hay relación de baño configurada para el work center %(wc)s. "
                            "Configúrala en Tintorería > Rendimiento antes de continuar.",
                            wc=centro.display_name or _("(sin work center)")))
                    continue
                nuevo = orden._dosificacion_qty(linea, mov.product_uom)
                if float_compare(nuevo, mov.product_uom_qty, precision_rounding=mov.product_uom.rounding) == 0:
                    continue
                origenes = mov.move_orig_ids
                if any(o.state == 'done' for o in origenes):
                    continue  # la requisición ya se surtió: se conserva lo surtido
                delta = nuevo - mov.product_uom_qty
                pendientes = origenes.filtered(lambda o: o.state not in ('done', 'cancel'))
                anterior = mov.product_uom_qty
                mov.write({'product_uom_qty': nuevo})
                if pendientes:
                    origen = pendientes[-1]
                    delta_origen = mov.product_uom._compute_quantity(delta, origen.product_uom)
                    origen.write({'product_uom_qty': max(origen.product_uom_qty + delta_origen, 0.0)})
                cambios.append('%s: %s → %s %s' % (
                    mov.product_id.default_code or mov.product_id.display_name,
                    round(anterior, 6), round(nuevo, 6), mov.product_uom.name))
            if cambios:
                orden.message_post(body=_(
                    "Dosificación de tintorería recalculada: %s", '; '.join(cambios)))
        return True

    def action_recalcular_dosificacion(self):
        return self._recalcular_dosificacion(estricto=True)

    def action_confirm(self):
        self._recalcular_dosificacion(estricto=True)
        return super().action_confirm()
