# -*- coding: utf-8 -*-
from odoo import fields, models


class CalidadDefectoTela(models.Model):
    """Catálogo de defectos de tela (diseño §10). Se carga el de Tejido/Acabado; el de No Tejido queda
    fuera de alcance, pero el campo tipo_tela permite cargarlo después."""
    _name = 'calidad.defecto.tela'
    _description = 'Defecto de tela'
    _order = 'clave'
    _rec_name = 'clave'

    clave = fields.Char('Sigla', required=True, index=True)
    name = fields.Char('Descripción', required=True)
    tipo_tela = fields.Selection(
        [('tejido_acabado', 'Tejido / Acabado'), ('no_tejido', 'No tejido')],
        'Tipo de tela', default='tejido_acabado', required=True)
    active = fields.Boolean(default=True)

    _unique_clave = models.Constraint(
        'unique(clave, tipo_tela)', 'La sigla del defecto ya existe para ese tipo de tela.')

    def _compute_display_name(self):
        for defecto in self:
            defecto.display_name = '%s - %s' % (defecto.clave, defecto.name) if defecto.name else defecto.clave
