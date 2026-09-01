# -*- coding: utf-8 -*-
"""Mapa cuenta contable → clasificación de costeo (vista SQL read-only).

El matching cuenta↔clase se resuelve en Python (qb.costeo.cuenta.class
mantiene account_ids porque el código de cuenta es company-dependent en
Odoo 19); aquí solo se elige la MEJOR clase por cuenta cuando varios
patrones matchean: cuenta específica > patrón más largo > id menor.

CUENTA_MAP_SQL se reusa como CTE en las demás vistas SQL del módulo para
no depender del orden de creación de vistas en el registry.
"""
from odoo import fields, models

def mo_qty_sql(env, alias='mp'):
    """Expresión SQL de la cantidad producida de una mrp.production.

    Odoo 19 eliminó la columna qty_produced del MO: para órdenes done la
    cantidad real vive en qty_producing (con product_qty como fallback).
    Se detecta en runtime para funcionar en 19 y sobrevivir si vuelve a
    cambiar en versiones futuras.
    """
    field = env['mrp.production']._fields.get('qty_produced')
    if field and field.store and field.column_type:
        return '%s.qty_produced' % alias
    return ('COALESCE(NULLIF(%(a)s.qty_producing, 0), %(a)s.product_qty)'
            % {'a': alias})


def wo_qty_sql(env, alias='wo'):
    """Expresión SQL de la cantidad producida de una mrp.workorder,
    con fallback a la cantidad de su orden de producción si la columna
    no existe en esta versión."""
    field = env['mrp.workorder']._fields.get('qty_produced')
    if field and field.store and field.column_type:
        return '%s.qty_produced' % alias
    # Fallback sin columna qty_produced: la orden REPARTIDA entre sus
    # workorders (antes devolvía la orden COMPLETA por cada workorder →
    # una MO con N workorders contaba N× su producción). Al sumar los
    # workorders se recupera la producción de la MO una sola vez.
    return ('(SELECT COALESCE(NULLIF(p.qty_producing, 0), p.product_qty) '
            '/ NULLIF((SELECT COUNT(*) FROM mrp_workorder w2 '
            'WHERE w2.production_id = p.id), 0) '
            'FROM mrp_production p WHERE p.id = %s.production_id)' % alias)


# Los asientos de CIERRE ANUAL reversan las cuentas de resultados del año
# ENTERO contra una sola póliza de diciembre. En producción es una sola:
# `Dr/2025/12/32`, ref «POLIZA DE CIERRE ANUAL», $190,684,760.
#
# Dejarla dentro hace dos daños. La conciliación de diciembre sale sin
# sentido —$-163M de "ventas" y $-147M de "gasto"— y el promedio de cada
# pool pierde diciembre entero, porque `_smooth` descarta el mes por salir
# negativo. O sea: cada año que se quiera ver pierde un mes real.
#
# Se filtra por la referencia del asiento. Sin `%` en la expresión: el SQL de
# las vistas pasa por formateo estilo printf y un porcentaje suelto lo
# rompería, así que se usa `position(... in ...)` en vez de ILIKE.
# Por default salen dos naturalezas:
#
# CIERRE ANUAL — la póliza que reversa el año entero contra diciembre.
#
# ENAJENACI — la baja de un activo vendido. En dic-2025 se cargaron
#   $5,827,157 a `504.08.0001 DEPRECIACIÓN MAQUINARIA` por dos máquinas
#   (FONGS JET y CIRCULAR INTERLOCK) que salieron del activo. No es costo del
#   período por tres razones que apuntan al mismo lado:
#
#     1. Ya está compensado: `704.23.0003 UTILIDAD EN VENTA DE ACTIVO FIJO`
#        tiene $5,896,997 en el mismo mes, y esa cuenta es `income_other`,
#        que el costeo no mira ni debe mirar. El módulo veía media operación.
#     2. Es un evento único —el saldo pendiente de depreciar reconocido de
#        golpe al vender— y el suavizado a 12 meses lo vuelve recurrente.
#     3. Es doble conteo: fue una venta con arrendamiento en reversa. Esas
#        máquinas hoy se pagan como renta, y la renta YA está en el pool
#        (`701.11%`, que saltó de 10 a 16 contratos justo en dic-2025).
#        Repartir además su depreciación de cuando eran propias le cobra a
#        cada producto la misma máquina dos veces.
#
# Es la misma regla que ya se aplicó al régimen híbrido de TEJIDO: cuando un
# costo cambia de vehículo, el vehículo viejo tiene que salir.
REFS_FUERA_DE_COSTEO_DEFAULT = 'CIERRE ANUAL,ENAJENACI'

# Solo estos caracteres pasan a la condición SQL. Las referencias vienen de un
# parámetro que edita un administrador y se INTERPOLAN —`_table_query` no
# admite parámetros—, así que la lista blanca es la que evita que un valor
# raro se convierta en SQL.
_REF_PERMITIDO = set(
    'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 ÁÉÍÓÚÑÜ./-_')


def excluir_refs_sql(env, alias='am'):
    """Condición SQL que deja fuera los asientos «fuera de costeo».

    Sin `%` en el resultado: el SQL de las vistas pasa por formateo estilo
    printf y un porcentaje suelto lo rompería, así que se usa
    `position(... in ...)` en vez de ILIKE.
    """
    crudo = env['qb.costeo.factor.config'].get_param_text(
        'refs_fuera_de_costeo', REFS_FUERA_DE_COSTEO_DEFAULT)
    refs = []
    for parte in (crudo or '').split(','):
        limpia = ''.join(c for c in parte.strip().upper()
                         if c in _REF_PERMITIDO).strip()
        if limpia:
            refs.append(limpia)
    if not refs:
        return 'TRUE'
    return ' AND '.join(
        "position('{ref}' in upper(coalesce({a}.ref, ''))) = 0".format(
            ref=r, a=alias)
        for r in refs)


# Parámetros que las vistas SQL leen del config, con su valor por omisión.
# Cada uno se escribía a mano en su vista —trece veces entre seis archivos— y
# no todas iguales: `weeks_per_month` era el único sin `NULLIF`, así que un 0
# guardado en ese parámetro (por error de captura, no hay validación que lo
# impida) dejaba la capacidad de TODA la planta en cero, mientras que en los
# demás un 0 caía al default. Cero no es un valor válido para ninguno de
# estos seis, así que la regla es la misma para todos.
CFG_PARAMS = {
    'weeks_per_month': ('weeks_per_month', '4.33'),
    'window_months': ('production_window_months', '3'),
    'm_per_kg': ('m_per_kg_default', '8.0'),
    'smoothing_months': ('smoothing_months', '12'),
    'rmin': ('rendimiento_min', '2.0'),
    'rmax': ('rendimiento_max', '25.0'),
}


def cfg_sql(*alias):
    """CTE `cfg` con los parámetros pedidos, uno por columna.

    Se usa como `{cfg}` en el `_table_query` de las vistas. Sin `%` en el
    resultado: ese SQL pasa por formateo estilo printf y un porcentaje
    suelto lo rompería.
    """
    faltan = [a for a in alias if a not in CFG_PARAMS]
    if faltan:
        raise KeyError('parámetro de cfg desconocido: %s' % ', '.join(faltan))
    columnas = ',\n'.join(
        "                COALESCE(NULLIF((SELECT value FROM qb_costeo_factor_config\n"
        "                                 WHERE key = '{key}' AND active\n"
        "                                 LIMIT 1), 0), {default}) AS {alias}"
        .format(key=CFG_PARAMS[a][0], default=CFG_PARAMS[a][1], alias=a)
        for a in alias)
    return 'WITH cfg AS (\n                SELECT\n%s\n            )' % columnas


# CTE reutilizable: una fila por cuenta con su mejor clasificación activa.
CUENTA_MAP_SQL = """
    SELECT DISTINCT ON (rel.account_id)
        rel.account_id,
        c.id AS class_id,
        c.bucket,
        c.es_variable,
        COALESCE(c.es_renta, FALSE) AS es_renta,
        COALESCE(c.filtro_etiqueta, '') AS filtro_etiqueta,
        c.centro_id,
        c.driver,
        COALESCE(c.allocation_pct, 100.0) AS allocation_pct,
        c.company_id
    FROM qb_cuenta_class_account_rel rel
    JOIN qb_costeo_cuenta_class c ON c.id = rel.class_id
    WHERE c.active
    ORDER BY rel.account_id,
        -- COALESCE: para una clase de PATRON c.account_id es NULL y
        -- "NULL = x" da NULL; con DESC a secas Postgres pone NULL antes
        -- que TRUE y el patron le ganaba a la cuenta especifica (al reves
        -- de lo documentado: especifica > patron).
        COALESCE(c.account_id = rel.account_id, FALSE) DESC,
        char_length(COALESCE(c.code_pattern, '')) DESC,
        c.id
"""


class QbCosteoCuentaMap(models.Model):
    _name = 'qb.costeo.cuenta.map'
    _inherit = 'qb.sql.view'
    _description = 'Cuenta → bucket de costeo (resuelto)'
    _auto = False
    _order = 'bucket, account_id'
    _rec_name = 'account_id'

    account_id = fields.Many2one('account.account', readonly=True)
    class_id = fields.Many2one('qb.costeo.cuenta.class', readonly=True)
    bucket = fields.Char(readonly=True)
    es_variable = fields.Boolean(readonly=True)
    es_renta = fields.Boolean(readonly=True)
    filtro_etiqueta = fields.Char(readonly=True)
    centro_id = fields.Many2one('qb.costeo.centro', readonly=True)
    driver = fields.Char(readonly=True)
    allocation_pct = fields.Float(readonly=True)
    company_id = fields.Many2one('res.company', readonly=True)

    @property
    def _table_query(self):
        return """
            SELECT m.account_id AS id,
                   m.account_id,
                   m.class_id,
                   m.bucket,
                   m.es_variable,
                   m.es_renta,
                   m.filtro_etiqueta,
                   m.centro_id,
                   m.driver,
                   m.allocation_pct,
                   m.company_id
            FROM (%s) m
        """ % CUENTA_MAP_SQL
