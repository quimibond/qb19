# -*- coding: utf-8 -*-
"""57.60.0 (bloque 2 de formularios 1/6, decisión 2026-09-30 «Inventario de
formularios»: un formato por modelo pasa al bloque 2): ``sgi.format.map`` deja
de exigir un mapeo por modelo (``unique(model_id)``). La regla nueva es un
mapeo **general** activo por modelo (restricción en Python) más los que
tengan criterio (tipo de operación, centro de trabajo, categoría o filtro).

Quita la restricción SQL vieja con ``IF EXISTS`` (los dos nombres posibles)
para que los mapeos nuevos no choquen con ella aunque Odoo no la retire solo.
Los 22 mapeos de producción (9 con modelo y 13 por referencia) quedan como
generales: el campo calculado ``is_general`` sale verdadero en todos porque
ninguno tiene criterio, y cada registro imprime la misma clave que antes.
Idempotente. Nada se borra.
"""
import logging

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    if not version:
        return
    for name in ('sgi_format_map_model_uniq', 'sgi_format_map__model_uniq'):
        cr.execute('ALTER TABLE sgi_format_map DROP CONSTRAINT IF EXISTS "%s"' % name)
    _logger.info("SGI 57.60.0: sgi.format.map ya admite varios formatos por modelo "
                 "(sin unique(model_id)).")
