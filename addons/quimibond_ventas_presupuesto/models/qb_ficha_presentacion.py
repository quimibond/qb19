# -*- coding: utf-8 -*-
"""Presentación estándar del producto en la ficha de acabado (19.0.3.26.0, Jose 19-nov-2026).

Jose: «muchos de los datos de la preparación del producto deberían de salir del módulo de ficha técnica…
y ya en ventas lo hacemos por cliente». Tres niveles:

1. **Especificación** (ancho, peso, espesor, elongación, encogimiento y sus tolerancias): ya vive en la ficha de
   acabado; Condiciones de entrega la muestra sin capturarla.
2. **Presentación estándar** (largo de rollo, tubo, empalmes, tramo mínimo, peso máximo, rollos por tarima,
   empaque): campos nuevos de la ficha de acabado, que llena Ingeniería o Calidad una vez por producto. Se agregan
   desde aquí (herencia de otro módulo) para no tocar el módulo de la ficha.
3. **Por cliente** (Condiciones de entrega): lo que el cliente pide distinto; vacío = lo de la ficha.
"""
from odoo import api, fields, models

from .qb_delivery_terms import ROLL_UNITS


class FichaTecnicaAcabadoPresentacion(models.Model):
    _inherit = 'ficha.tecnica.acabado'

    qb_pres_roll_qty = fields.Float(string="Largo del rollo estándar",
                                    help="Lo que lleva un rollo de este producto si el cliente no pide otro. 0 = sin dato.")
    qb_pres_roll_uom = fields.Selection(ROLL_UNITS, string="Unidad del rollo estándar", default='m')
    qb_pres_core = fields.Char(string="Tubo / core estándar", help="Diámetro y material: 3\" cartón, 6\" PVC…")
    qb_pres_max_splices = fields.Integer(string="Empalmes máximos por rollo", default=-1,
                                         help="-1 = sin dato (sin límite).")
    qb_pres_min_piece = fields.Float(string="Tramo mínimo entre empalmes",
                                     help="En la unidad del rollo. 0 = sin dato.")
    qb_pres_max_weight_kg = fields.Float(string="Peso máximo por rollo (kg)", help="0 = sin dato.")
    qb_pres_rolls_per_pallet = fields.Integer(string="Rollos por tarima", help="0 = sin dato.")
    qb_pres_packaging = fields.Char(string="Empaque estándar", help="Emplayado, bolsa, tarima, esquineros…")

    @api.model
    def _qb_for_product(self, product):
        """La ficha de acabado del producto (en metros o en kg), o de su artículo."""
        if not product:
            return self.browse()
        ficha = getattr(product, 'ft_acabado_ficha_id', self.browse())
        if ficha:
            return ficha
        ids = [product.id]
        articulo = getattr(product, 'qb_articulo_id', False)
        if articulo:
            ids.append(articulo.id)
        return self.search(['|', ('product_acabado_id', 'in', ids), ('product_proceso_id', 'in', ids)], limit=1)
