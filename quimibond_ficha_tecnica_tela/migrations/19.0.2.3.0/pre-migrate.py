# -*- coding: utf-8 -*-
"""19.0.2.3.0 — Ficha de la máquina con configuraciones por fecha.

Dirección General 2026-10-13: los datos de máquina (diámetro, galga, agujas,
alimentadores, velocidad máxima) viven en configuraciones con fecha de cada
centro de trabajo (``ficha.tecnica.maquina.config``) y la ficha de tejido los
toma de la configuración con la que se elaboró.

Este paso solo cambia el tipo de ``ficha_tecnica_tejido.diametro`` de texto a
número (``double precision``), como las tolerancias de 2.2.0. Un valor que no
se pueda leer como número se reporta en el log y su texto queda en
``diametro_texto_2_2``. No crea configuraciones de máquina: se capturan en
planta. En producción la tabla estaba vacía (2026-10-13). Idempotente."""
import logging
import re

_logger = logging.getLogger(__name__)

TAG = "ficha_tecnica_tela 2.3.0"
NUMBER = re.compile(r'(\d+(?:[.,]\d+)?)')


def migrate(cr, version):
    cr.execute("SELECT data_type FROM information_schema.columns "
               "WHERE table_name = 'ficha_tecnica_tejido' AND column_name = 'diametro'")
    row = cr.fetchone()
    if not row or row[0] not in ('character varying', 'text'):
        return
    cr.execute('ALTER TABLE ficha_tecnica_tejido RENAME COLUMN diametro TO diametro_texto_2_2')
    cr.execute('ALTER TABLE ficha_tecnica_tejido ADD COLUMN diametro double precision')
    cr.execute("SELECT id, diametro_texto_2_2 FROM ficha_tecnica_tejido "
               "WHERE coalesce(trim(diametro_texto_2_2), '') != ''")
    unread = []
    for rec_id, text in cr.fetchall():
        match = NUMBER.search(text)
        if match:
            cr.execute('UPDATE ficha_tecnica_tejido SET diametro = %s WHERE id = %s',
                       (float(match.group(1).replace(',', '.')), rec_id))
        else:
            unread.append((rec_id, text))
    if unread:
        _logger.warning("%s: %d diámetros no son número; su texto queda en diametro_texto_2_2: %s",
                        TAG, len(unread), unread[:20])
    else:
        cr.execute('ALTER TABLE ficha_tecnica_tejido DROP COLUMN diametro_texto_2_2')
    _logger.info("%s: ficha_tecnica_tejido.diametro pasa a número.", TAG)
