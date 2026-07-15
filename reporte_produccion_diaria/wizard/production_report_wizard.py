from odoo import models, fields, api, _
from odoo.exceptions import ValidationError


class ProductionReportWizard(models.TransientModel):
    _name = 'production.report.wizard'
    _description = 'Asistente de Reporte de Producción Tejido'

    date_start = fields.Date(
        string='Fecha Inicio',
        required=True,
        default=fields.Date.context_today
    )
    date_end = fields.Date(
        string='Fecha Fin',
        required=True,
        default=fields.Date.context_today
    )

    # ------------------------------------------------------------------
    # Helpers de costeo
    # ------------------------------------------------------------------
    def _get_moves_cost(self, moves):
        """Costo real de un conjunto de movimientos (componentes o subproductos).

        Odoo 19 eliminó el modelo stock.valuation.layer: el valor real de
        la valoración ahora se guarda directamente en el campo 'value' de
        stock.move (confirmado en esta instancia: coincide exactamente con
        el 'Costo real' del resumen de la orden). Si un movimiento no está
        valorado (is_valued=False) se recurre al costo estándar del
        producto como respaldo.
        """
        if not moves:
            return 0.0

        total = 0.0
        for m in moves:
            if m.is_valued and m.value:
                total += m.value
            else:
                total += m.quantity * m.product_id.standard_price
        return total

    def _prepare_lines_vals(self, production_orders):
        vals_list = []

        for mo in production_orders:
            workcenter_names = ', '.join(mo.workorder_ids.mapped('workcenter_id.name'))

            finished_moves = mo.move_finished_ids.filtered(
                lambda m: m.product_id == mo.product_id and m.state == 'done'
            )
            qty_produced = sum(finished_moves.mapped('quantity'))

            qty_scrap = sum(mo.scrap_ids.mapped('scrap_qty'))

            components_cost = self._get_moves_cost(
                mo.move_raw_ids.filtered(lambda m: m.state == 'done')
            )

            # Costo de operaciones: se usan las tarifas guardadas en el
            # propio workorder (costs_hour + employee_costs_hour), que son
            # las tarifas "congeladas" al momento de trabajar la orden —
            # no wo.workcenter_id.costs_hour, que es la tarifa actual/en
            # vivo del centro de trabajo y puede haber cambiado desde
            # entonces. Confirmado contra el resumen real de la MO:
            # 51.13h * (74.57 + 29.65) ≈ MX$5,328.76.
            operations_cost = sum(
                (wo.duration / 60.0) * (wo.costs_hour + wo.employee_costs_hour)
                for wo in mo.workorder_ids
            )

            # Campo opcional 'extra_cost' (costo extra por unidad definido
            # al cerrar la orden). Se incluye solo si existe en el modelo.
            extra_cost_unit = getattr(mo, 'extra_cost', 0.0) or 0.0
            total_cost = components_cost + operations_cost + (extra_cost_unit * qty_produced)

            components_avg = components_cost / qty_produced if qty_produced else 0.0
            operations_avg = operations_cost / qty_produced if qty_produced else 0.0
            total_avg = total_cost / qty_produced if qty_produced else 0.0

            common_vals = {
                'date_finished': mo.date_finished or mo.date_start,
                'production_id': mo.id,
                'product_id': mo.product_id.id,
                'workcenter_names': workcenter_names,
                'qty_planned': mo.product_qty,
                'qty_produced': qty_produced,
                'qty_scrap': qty_scrap,
                'uom_id': mo.product_uom_id.id,
                'cost_components_total': components_cost,
                'cost_operations_total': operations_cost,
                'cost_production_total': total_cost,
                'cost_components_avg': components_avg,
                'cost_operations_avg': operations_avg,
                'cost_total_avg': total_avg,
            }

            byproducts = mo.move_byproduct_ids.filtered(lambda m: m.state == 'done')
            if byproducts:
                for bp in byproducts:
                    vals = dict(common_vals)
                    vals.update({
                        'byproduct_id': bp.product_id.id,
                        'qty_byproduct': bp.quantity,
                    })
                    vals_list.append(vals)
            else:
                # Orden sin subproductos: una sola línea con esos campos vacíos.
                vals_list.append(dict(common_vals))

        return vals_list

    # ------------------------------------------------------------------
    # Acción principal: ahora abre una vista de lista, ya no un PDF
    # ------------------------------------------------------------------
    def action_view_report(self):
        workcenter_ids = self.env['mrp.workcenter'].search([
            ('name', 'ilike', 'CIRCULAR%')
        ]).ids

        production_orders = self.env['mrp.production'].search([
            ('date_start', '>=', self.date_start),
            ('date_start', '<=', self.date_end),
            ('state', 'in', ['to_close', 'done']),
            ('qty_producing', '>', 0),
            ('workorder_ids.workcenter_id', 'in', workcenter_ids)
        ], order='date_start asc')

        # Doble verificación de seguridad en Python para excluir canceladas.
        production_orders = production_orders.filtered(lambda x: x.state in ['to_close', 'done'])

        if not production_orders:
            raise ValidationError(_(
                "No se encontraron órdenes finalizadas para los centros CIRCULAR "
                "en las fechas seleccionadas."
            ))

        vals_list = self._prepare_lines_vals(production_orders)
        lines = self.env['production.report.line'].create(vals_list)

        action = self.env['ir.actions.act_window']._for_xml_id(
            'reporte_produccion_diaria.action_production_report_line_view'
        )
        action['domain'] = [('id', 'in', lines.ids)]
        return action
