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
    def _kg_carga_jet(self):
        """1.4.0: (kg que entran al jet, producto crudo). El jet se carga con el crudo, no con la tela terminada: en
        IWJ038Q22JNT160 entran 1.1505 kg de crudo por kg de tela (auditoría del flujo, 2026-11-06). La relación de baño
        (L por kg cargado) y el % de colorante (sobre el peso cargado) van sobre esos kg, y la programación de
        Tintorería reparte los baños con ellos. El crudo es el componente en peso de una categoría de tejido con más
        kg: el de los movimientos de la orden y, mientras no existen (al crearla), el de la lista de materiales. Sin
        componente de tejido, los kg de la orden, como antes."""
        self.ensure_one()
        kg = self._kg_uom()
        try:
            kg_orden = self.product_uom_id._compute_quantity(self.product_qty, kg, round=False)
        except UserError:
            kg_orden = self.product_qty
        tela = self.move_raw_ids.filtered(
            lambda m: m.state != 'cancel' and m.product_uom._has_common_reference(kg)
            and self._es_tejido(m.product_id))
        if tela:
            planeado = {}
            for move in tela:
                planeado[move.product_id] = planeado.get(move.product_id, 0.0) + move.product_uom._compute_quantity(
                    move.product_uom_qty, kg, round=False)
            # 1.4.2 (revisión de código, 2026-11-06): con dos crudos en el baño entran los dos al jet; se suman (antes solo
            # el de más kg). El producto que se informa es el de más kg.
            crudo = max(planeado, key=planeado.get)
            return sum(planeado.values()), crudo
        if self.bom_id:
            kg_lote, kg_crudo, crudo = self.bom_id._kg_carga_jet_lote()
            if crudo and kg_lote:
                return kg_orden * kg_crudo / kg_lote, crudo
        return kg_orden, False

    def _ft_kg_carga_jet(self):
        """1.4.1: los avisos y las cargas por jet de la ficha técnica (2.28.0) cuentan los kg de crudo que entran al
        jet, los mismos que usan la dosificación y la programación."""
        self.ensure_one()
        return self._kg_carga_jet()

    def _dosificacion_kg_orden(self):
        """Kg sobre los que se dosifica: los que entran al jet (1.4.0)."""
        self.ensure_one()
        return self._kg_carga_jet()[0]

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
                            "Configúrala en Fichas técnicas: la del artículo en Tintorería → Artículos en los jets "
                            "o la del jet en Máquinas → Configuraciones por fecha.",
                            wc=centro.display_name or _("(sin work center)")))
                    continue
                nuevo = orden._dosificacion_qty(linea, mov.product_uom)
                if float_compare(nuevo, mov.product_uom_qty, precision_rounding=mov.product_uom.rounding) == 0:
                    continue
                origenes = mov.move_orig_ids
                if any(o.state == 'done' for o in origenes):
                    continue  # la requisición ya se surtió: se conserva lo surtido
                pendientes = origenes.filtered(lambda o: o.state not in ('done', 'cancel'))
                anterior = mov.product_uom_qty
                origen = pendientes[-1] if pendientes else self.env['stock.move']
                qty_origen_antes = origen.product_uom_qty
                mov.write({'product_uom_qty': nuevo})
                if origen:
                    # Odoo ya propaga el cambio de cantidad del movimiento a la requisición MTO que lo
                    # abastece. Solo se ajusta a mano si no lo hizo; si no, la diferencia se aplicaba
                    # dos veces (y n veces tras dividir la MO en baños).
                    if float_compare(origen.product_uom_qty, qty_origen_antes,
                                     precision_rounding=origen.product_uom.rounding) == 0:
                        delta_origen = mov.product_uom._compute_quantity(nuevo - anterior, origen.product_uom)
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
