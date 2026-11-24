# -*- coding: utf-8 -*-
"""2.33.0: estado de la ficha de acabado (Borrador, En revisión, Activa, Obsoleta).

Las fichas que ya existen toman el estado que ya tenían de hecho: Activa si su lista en kg del flujo está activa y con
operaciones (Odoo ya fabrica con ella), En revisión si la ficha está activa y sus listas no, y Borrador si la ficha
está archivada (en producción, 24-nov-2026: 1 activa, 31 en revisión y 80 archivadas que siguen archivadas)."""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    Revision = env['ficha.tecnica.revision']
    fichas = env['ficha.tecnica.acabado'].with_context(active_test=False).search([])
    cuenta = {'borrador': 0, 'revision': 0, 'activa': 0}
    for ficha in fichas:
        if not ficha.active:
            estado = 'borrador'
        else:
            bom = Revision._ft_bom_de(ficha.product_proceso_id)
            activa = bom.active and bool(bom.with_context(active_test=False).operation_ids)
            estado = 'activa' if activa else 'revision'
        cuenta[estado] += 1
        cr.execute('UPDATE ficha_tecnica_acabado SET estado = %s WHERE id = %s', (estado, ficha.id))
    _logger.info('fichas 2.33.0: estado de las fichas de acabado: %s', cuenta)
