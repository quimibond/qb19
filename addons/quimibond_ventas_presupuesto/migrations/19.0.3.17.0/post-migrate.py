# -*- coding: utf-8 -*-
"""3.17.0: «Pide el cliente» y los ajustes de Ventas en el pronóstico vigente.

Las semanas que escribió un release toman su cantidad como «Pide el cliente». Las que Ventas ya
corrigió en la matriz después de un release (pasaron a «Captura» y conservan el release) toman lo
que pedía ese release (de Cambios del pronóstico) y quedan «Ajustadas», para que el siguiente release
no las pise. En producción, al 18-nov-2026, era una: Shawmut, semana del 9-nov (245.68 → 5 kg)."""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {'active_test': False})
    Line = env['qb.forecast.line']
    Change = env['qb.forecast.change']
    plain = adjusted = 0
    for line in Line.search([('release_id', '!=', False)]):
        try:
            with cr.savepoint():
                if line.origen == 'release':
                    line.write({'qty_release': line.qty_venta})
                    plain += 1
                    continue
                last = Change.search([('line_id', '=', line.id), ('origen', '=', 'release')],
                                     order='create_date desc, id desc', limit=1)
                asked = last.qty_after if last else line.qty_venta
                line.write({'qty_release': asked, 'ajustado': abs(asked - line.qty_venta) > 1e-6})
                adjusted += 1
        except Exception as exc:  # noqa: BLE001 - una semana no detiene la actualización
            _logger.warning("3.17.0: la semana %s no tomó «Pide el cliente»: %s", line.id, exc)
    _logger.info("3.17.0: %d semana(s) del release con «Pide el cliente» y %d ajustada(s) por Ventas",
                 plain, adjusted)
