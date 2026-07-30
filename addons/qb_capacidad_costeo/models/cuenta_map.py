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
