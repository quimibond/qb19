# -*- coding: utf-8 -*-
"""Motivos de lista para regresar una cotización a recotizar y para darla por
perdida (brief C1 §6.8: sin texto libre para datos; la nota es opcional)."""
from odoo import fields, models

MOTIVO_TIPOS = [('regreso', 'Regreso a recotizar'), ('perdida', 'Pérdida')]


class QbCotizadorMotivo(models.Model):
    _name = 'qb.cotizador.motivo'
    _description = 'Motivo de regreso o de pérdida de una cotización'
    _order = 'tipo, sequence, id'

    name = fields.Char(required=True, translate=False)
    tipo = fields.Selection(MOTIVO_TIPOS, required=True, default='regreso',
                            help='Regreso: la aprobación la devuelve a recotizar. '
                                 'Pérdida: por qué no se ganó.')
    sequence = fields.Integer(default=10)
    active = fields.Boolean(default=True)
