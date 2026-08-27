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
CIERRE_ANUAL_REF = 'CIERRE ANUAL'

EXCLUIR_CIERRE_SQL = (
    "position('" + CIERRE_ANUAL_REF + "' in upper(coalesce(am.ref, ''))) = 0")


# CTE reutilizable: una fila por cuenta con su mejor clasificación activa.
CUENTA_MAP_SQL = """
    SELECT DISTINCT ON (rel.account_id)
        rel.account_id,
        c.id AS class_id,
        c.bucket,
        c.es_variable,
        COALESCE(c.es_renta, FALSE) AS es_renta,
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
    _description = 'Cuenta → bucket de costeo (resuelto)'
    _auto = False
    _order = 'bucket, account_id'
    _rec_name = 'account_id'

    account_id = fields.Many2one('account.account', readonly=True)
    class_id = fields.Many2one('qb.costeo.cuenta.class', readonly=True)
    bucket = fields.Char(readonly=True)
    es_variable = fields.Boolean(readonly=True)
    es_renta = fields.Boolean(readonly=True)
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
                   m.centro_id,
                   m.driver,
                   m.allocation_pct,
                   m.company_id
            FROM (%s) m
        """ % CUENTA_MAP_SQL
