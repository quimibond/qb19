# -*- coding: utf-8 -*-
"""19.0.2.2.0 — La máquina es un centro de trabajo y las tolerancias son números.

Dirección General 2026-10-13 (una sola ficha por proceso, la de este módulo):

1. ``ficha_tecnica_tejido.maquina_tejido`` era texto y pasa a Many2one a
   ``mrp.workcenter``. La columna de texto se renombra a
   ``maquina_tejido_texto`` (dato histórico); la máquina nueva nace vacía y
   se captura en la ficha. No se adivina el centro de trabajo por el texto.
2. Las tolerancias que eran texto pasan a número (``double precision``). Un
   valor numérico se conserva; el formato asimétrico del peso («+12 / -6»)
   se parte en la tolerancia de arriba y la nueva de abajo
   (``*_tol_minus``). Lo que no se puede leer como número se reporta en el
   log y su texto se conserva en una columna ``<campo>_texto_2_1`` para
   revisarlo; nada se borra.

En producción las dos tablas estaban vacías al subir esta versión
(2026-10-13): el paso solo cambia el esquema. Idempotente."""
import logging
import re

_logger = logging.getLogger(__name__)

TAG = "ficha_tecnica_tela 2.2.0"
NUMBER = re.compile(r'^\s*[+±-]?\s*(\d+(?:[.,]\d+)?)\s*$')
ASYM = re.compile(r'^\s*\+?\s*(\d+(?:[.,]\d+)?)\s*/\s*-?\s*(\d+(?:[.,]\d+)?)\s*$')
TOLERANCES = {
    'ficha_tecnica_tejido': [
        'tension_tol', 'punto_cilindro_tol', 'punto_plato_tol', 'altura_plato_tol', 'estiraje_tol',
        'peso_acondicionado_tol', 'ancho_acondicionado_tol', 'espesor_acondicionado_tol', 'columnas_tol',
        'mallas_tol', 'elongacion_carga_largo_tol', 'elongacion_carga_ancho_tol',
    ],
    'ficha_tecnica_acabado': [
        'peso_acabado_tol', 'ancho_acabado_tol', 'encogimiento_largo_tol', 'encogimiento_ancho_tol',
        'espesor_acabado_tol', 'elongacion_largo_acabado_tol', 'elongacion_ancho_acabado_tol',
    ],
}
ASYMMETRIC = {'peso_acondicionado_tol', 'peso_acabado_tol'}


def _column_type(cr, table, column):
    cr.execute("SELECT data_type FROM information_schema.columns WHERE table_name = %s AND column_name = %s",
               (table, column))
    row = cr.fetchone()
    return row[0] if row else None


def _num(text):
    return float(text.replace(',', '.'))


def migrate(cr, version):
    if _column_type(cr, 'ficha_tecnica_tejido', 'maquina_tejido') in ('character varying', 'text') \
            and not _column_type(cr, 'ficha_tecnica_tejido', 'maquina_tejido_texto'):
        cr.execute('ALTER TABLE ficha_tecnica_tejido RENAME COLUMN maquina_tejido TO maquina_tejido_texto')
        _logger.info("%s: maquina_tejido (texto) → maquina_tejido_texto.", TAG)

    for table, columns in TOLERANCES.items():
        for column in columns:
            if _column_type(cr, table, column) not in ('character varying', 'text'):
                continue
            legacy = '%s_texto_2_1' % column
            cr.execute('ALTER TABLE %s RENAME COLUMN %s TO %s' % (table, column, legacy))
            cr.execute('ALTER TABLE %s ADD COLUMN %s double precision' % (table, column))
            minus = '%s_minus' % column if column in ASYMMETRIC else None
            if minus and not _column_type(cr, table, minus):
                cr.execute('ALTER TABLE %s ADD COLUMN %s double precision' % (table, minus))
            cr.execute("SELECT id, %s FROM %s WHERE coalesce(trim(%s), '') != ''" % (legacy, table, legacy))
            unread = []
            for rec_id, text in cr.fetchall():
                asym = ASYM.match(text) if minus else None
                plain = NUMBER.match(text)
                if asym:
                    cr.execute('UPDATE %s SET %s = %%s, %s = %%s WHERE id = %%s' % (table, column, minus),
                               (_num(asym.group(1)), _num(asym.group(2)), rec_id))
                elif plain:
                    cr.execute('UPDATE %s SET %s = %%s WHERE id = %%s' % (table, column),
                               (_num(plain.group(1)), rec_id))
                else:
                    unread.append((rec_id, text))
            if unread:
                _logger.warning("%s: %s.%s, %d valores no son número; su texto queda en %s: %s",
                                TAG, table, column, len(unread), legacy, unread[:20])
            else:
                cr.execute('ALTER TABLE %s DROP COLUMN %s' % (table, legacy))
            _logger.info("%s: %s.%s pasa a número.", TAG, table, column)
