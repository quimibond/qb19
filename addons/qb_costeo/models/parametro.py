# -*- coding: utf-8 -*-
"""Parámetros del costeo: pocos, cada uno con su ayuda.

El módulo anterior llegó a 45 claves sin explicación en la pantalla. Aquí cada
parámetro se siembra desde `data/parametros.xml` con `help`, y el código solo
lee los que existen ahí: una clave que no está sembrada no es un parámetro,
es un error.
"""
from odoo import api, fields, models


class QbParametro(models.Model):
    _name = 'qb.parametro'
    _description = 'Parámetro del costeo'
    _order = 'key'

    key = fields.Char(required=True, index=True)
    name = fields.Char(string='Nombre', required=True)
    value_float = fields.Float(string='Valor', digits=(16, 4))
    value_text = fields.Char(string='Texto')
    tipo = fields.Selection([('float', 'Número'), ('text', 'Texto')],
                            required=True, default='float')
    help = fields.Text(string='Para qué sirve')
    company_id = fields.Many2one(
        'res.company', required=True,
        default=lambda self: self.env.company)

    _key_company_uniq = models.Constraint(
        'unique(key, company_id)',
        'Ya existe ese parámetro para la compañía.')

    @api.model
    def _rec(self, key):
        return self.search([('key', '=', key),
                            ('company_id', '=', self.env.company.id)],
                           limit=1)

    @api.model
    def get_float(self, key, default=0.0):
        rec = self._rec(key)
        return rec.value_float if rec else default

    @api.model
    def get_text(self, key, default=''):
        rec = self._rec(key)
        return (rec.value_text or default) if rec else default

    @api.model
    def get_list(self, key, default=''):
        """Texto separado por comas → lista sin espacios ni vacíos."""
        return [t.strip() for t in self.get_text(key, default).split(',')
                if t.strip()]

    @api.model
    def set_float(self, key, value):
        rec = self._rec(key)
        if rec:
            rec.value_float = value
        return rec
