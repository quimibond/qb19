# -*- coding: utf-8 -*-
"""Catálogos del pedimento: claves (Anexo 22, apéndice 2), aduanas (apéndice 1)
y tipos de documento del expediente (columnas de checklist de F-P-A-16-05 y
F-P-A-16-06). Se siembran solo los que aparecen en la operación de Quimibond;
el resto se da de alta a mano."""
from odoo import api, fields, models

OPERACIONES = [('importacion', 'Importación'), ('exportacion', 'Exportación')]


class QbPedimentoClave(models.Model):
    _name = 'qb.pedimento.clave'
    _description = 'Clave de pedimento'
    _order = 'code'
    _rec_names_search = ['code', 'name']

    code = fields.Char('Clave', required=True, size=2)
    name = fields.Char('Descripción', required=True)
    operacion = fields.Selection(OPERACIONES + [('ambas', 'Ambas')], default='ambas', required=True)
    virtual = fields.Boolean(
        'Virtual', help='Operación virtual (V1, V5): la mercancía no cruza la frontera; la factura es a un '
                        'cliente nacional con programa IMMEX.')
    rectificacion = fields.Boolean(
        'Rectificación', help='Corrige un pedimento anterior (R1). Exige el pedimento original.')
    active = fields.Boolean(default=True)

    _code_uniq = models.Constraint('unique(code)', 'Esa clave de pedimento ya existe.')

    @api.depends('code', 'name')
    def _compute_display_name(self):
        for rec in self:
            rec.display_name = '%s - %s' % (rec.code, rec.name) if rec.code else rec.name


class QbPedimentoAduana(models.Model):
    _name = 'qb.pedimento.aduana'
    _description = 'Aduana'
    _order = 'code'
    _rec_names_search = ['code', 'name']

    code = fields.Char('Código', required=True, size=2, help='Dos dígitos del Anexo 22, apéndice 1.')
    name = fields.Char('Aduana', required=True)
    active = fields.Boolean(default=True)

    _code_uniq = models.Constraint('unique(code)', 'Esa aduana ya existe.')

    @api.depends('code', 'name')
    def _compute_display_name(self):
        for rec in self:
            rec.display_name = '%s %s' % (rec.code, rec.name) if rec.code else rec.name

    @api.model
    def _get_or_create(self, code):
        """La aduana del número de pedimento; si el código no está en el
        catálogo se da de alta con el nombre por llenar (no se inventa)."""
        aduana = self.with_context(active_test=False).search([('code', '=', code)], limit=1)
        return aduana or self.sudo().create({'code': code, 'name': 'Aduana %s (por nombrar)' % code})


class QbPedimentoDocumentoTipo(models.Model):
    _name = 'qb.pedimento.documento.tipo'
    _description = 'Tipo de documento del expediente'
    _order = 'sequence, id'

    name = fields.Char('Documento', required=True)
    sequence = fields.Integer(default=10)
    operacion = fields.Selection(OPERACIONES + [('ambas', 'Ambas')], default='ambas', required=True)
    required = fields.Boolean(
        'Obligatorio para cerrar', default=True,
        help='El expediente no se cierra mientras este documento no tenga archivo o esté marcado «No aplica».')
    solo_virtual = fields.Boolean(
        'Solo virtuales', help='Se pide solo en pedimentos con clave virtual (V1, V5).')
    excepto_virtual = fields.Boolean(
        'Excepto virtuales', help='No se pide en pedimentos virtuales (la mercancía no cruza).')
    active = fields.Boolean(default=True)

    def _applies_to(self, operacion, virtual):
        return self.filtered(lambda t: t.operacion in (operacion, 'ambas')
                             and not (t.solo_virtual and not virtual)
                             and not (t.excepto_virtual and virtual))
