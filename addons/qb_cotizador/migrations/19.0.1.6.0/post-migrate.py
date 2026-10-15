# -*- coding: utf-8 -*-
"""19.0.1.6.0 — Dirección General 2026-10-15: las cotizaciones ya calculadas reciben la unidad en que está su
foto del costo (`costo_uom`), para que la que compara un costo por m contra un volumen en kg (o al revés)
lo avise y no se apruebe hasta recalcular. No cambia costos, precios ni estados: solo anota la unidad que la
foto ya tenía (la del costo de origen; lo capturado a mano, la del volumen). Idempotente. Prefijo en el log:
«qb_cotizador 1.6.0»."""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    cots = env['qb.cotizador.cotizacion'].sudo().with_context(active_test=False).search(
        [('calculado_el', '!=', False), ('costo_uom', '=', False), ('costo_fuente', '!=', 'legado')])
    cruzadas = []
    for cot in cots:
        if cot.costo_fuente == 'manual' or not cot.costo_id:
            uom = cot.volumen_uom or 'm'
        else:
            name = (cot.costo_id.uom_name or cot.costo_id.product_id.uom_id.name or '').strip().lower()
            uom = 'kg' if name.startswith('k') else 'm'
        cot.write({'costo_uom': uom, 'costo_uom_origen': uom})
        if uom != (cot.volumen_uom or 'm'):
            cruzadas.append(cot.folio or str(cot.id))
    _logger.info("qb_cotizador 1.6.0: unidad del costo anotada en %d cotización(es); con unidades cruzadas "
                 "(recalcular): %s.", len(cots), ', '.join(cruzadas) or 'ninguna')
