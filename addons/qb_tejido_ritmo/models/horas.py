# -*- coding: utf-8 -*-
"""Horas por unidad desde el pesaje: fuente «pesaje», antes que las órdenes."""
from odoo import api, models


class QbProductoHoras(models.Model):
    _inherit = 'qb.producto.horas'

    @api.model
    def _fuentes_externas(self, products, centros, desde, hasta):
        out = super()._fuentes_externas(products, centros, desde, hasta)
        Ritmo = self.env['qb.tejido.ritmo']
        meses = int(self.env['qb.parametro'].get_float('horas_historia_meses', 12)) or 12
        kg = self.env.ref('uom.product_uom_kgm', raise_if_not_found=False)
        for centro in centros.filtered(lambda c: c.driver == 'workorder' and c.workcenter_ids):
            efectiva = Ritmo.efectiva_por_producto(centro.workcenter_ids, meses=meses, hasta=hasta)
            for p in products:
                dato = efectiva.get(p.id)
                if not dato or not kg or not p.uom_id._has_common_reference(kg):
                    continue
                kg_h_ef, rollos, kg_h_corr = dato
                factor = p.uom_id._compute_quantity(1.0, kg, round=False)  # kg por unidad
                out[(p.id, centro.id)] = (
                    factor / kg_h_ef, 'pesaje',
                    'Pesaje: %d rollos, %.2f kg/h efectiva (%.2f en corrida)'
                    % (rollos, kg_h_ef, kg_h_corr), rollos)
        return out
