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
    return ('(SELECT COALESCE(NULLIF(p.qty_producing, 0), p.product_qty) '
            'FROM mrp_production p WHERE p.id = %s.production_id)' % alias)


# CTE reutilizable: una fila por cuenta con su mejor clasificación activa.
CUENTA_MAP_SQL = """
    SELECT DISTINCT ON (rel.account_id)
        rel.account_id,
        c.id AS class_id,
        c.bucket,
        c.es_variable,
        c.centro_id,
        c.driver,
        COALESCE(c.allocation_pct, 100.0) AS allocation_pct,
        c.company_id
    FROM qb_cuenta_class_account_rel rel
    JOIN qb_costeo_cuenta_class c ON c.id = rel.class_id
    WHERE c.active
    ORDER BY rel.account_id,
        (c.account_id = rel.account_id) DESC,
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
                   m.centro_id,
                   m.driver,
                   m.allocation_pct,
                   m.company_id
            FROM (%s) m
        """ % CUENTA_MAP_SQL
