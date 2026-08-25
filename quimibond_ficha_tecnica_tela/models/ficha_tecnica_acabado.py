# -*- coding: utf-8 -*-
from odoo import api, fields, models


class FichaTecnicaAcabado(models.Model):
    _name = 'ficha.tecnica.acabado'
    _description = 'Ficha Técnica de Acabado'
    _rec_name = 'articulo'
    _order = 'articulo'

    # ------------------------------------------------------------------
    # Encabezado
    # ------------------------------------------------------------------
    articulo = fields.Char(
        string='Artículo (acabado)', required=True, index=True,
        help='Código del artículo de tela acabada, ej. WJ044Q22HNT235-NEGRO.')
    revision = fields.Char(string='Revisión', default='0')
    active = fields.Boolean(default=True)
    fecha_elaboracion = fields.Date(string='Fecha de elaboración')

    tejido_id = fields.Many2one(
        'ficha.tecnica.tejido', string='Ficha Técnica de Tejido (base)',
        required=True, ondelete='restrict',
        help='Ficha de tejido sobre la que se construye este acabado. '
             'Varias fichas de acabado (ej. distintos colores) pueden '
             'compartir la misma ficha de tejido base.')
    product_acabado_id = fields.Many2one(
        'product.product', string='Producto — Tela Acabada (m)', required=True,
        help='Producto terminado específico (ej. un color en particular), '
             'resultado de Inspección y Empaque + Control de Calidad.')

    # ------------------------------------------------------------------
    # Datos de Acabado
    # ------------------------------------------------------------------
    rendimiento_tela_acabada = fields.Float(
        string='Rendimiento de tela acabada (mts/kg)', digits=(12, 4),
        help='Valor teórico fijo. Se compara contra el rendimiento real '
             'calculado en el pesaje/registro final de cada rollo.')

    peso_acabado = fields.Float(string='Peso')
    peso_acabado_tol = fields.Char(
        string='Tolerancia peso', help='Admite formato asimétrico, ej. "+12 / -6".')
    peso_acabado_tol_unit = fields.Char(string='Unidad — Peso', default='g/m2')

    ancho_acabado = fields.Float(string='Ancho')
    ancho_acabado_tol = fields.Char(string='Tolerancia ancho')
    ancho_acabado_tol_unit = fields.Char(string='Unidad — Ancho', default='cm')

    encogimiento_largo = fields.Float(string='Encogimiento a lo largo')
    encogimiento_largo_tol = fields.Char(string='Tolerancia encogimiento largo')
    encogimiento_largo_tol_unit = fields.Char(string='Unidad — Encogimiento largo', default='%')

    encogimiento_ancho = fields.Float(string='Encogimiento a lo ancho')
    encogimiento_ancho_tol = fields.Char(string='Tolerancia encogimiento ancho')
    encogimiento_ancho_tol_unit = fields.Char(string='Unidad — Encogimiento ancho', default='%')

    espesor_acabado = fields.Float(string='Espesor')
    espesor_acabado_tol = fields.Char(string='Tolerancia espesor')
    espesor_acabado_tol_unit = fields.Char(string='Unidad — Espesor', default='in')

    elongacion_largo_acabado = fields.Float(string='Elongación largo')
    elongacion_largo_acabado_tol = fields.Char(string='Tolerancia elongación largo')
    elongacion_largo_acabado_tol_unit = fields.Char(string='Unidad — Elongación largo', default='%')

    elongacion_ancho_acabado = fields.Float(string='Elongación ancho')
    elongacion_ancho_acabado_tol = fields.Char(string='Tolerancia elongación ancho')
    elongacion_ancho_acabado_tol_unit = fields.Char(string='Unidad — Elongación ancho', default='%')

    encogimiento_largo_dentro_norma = fields.Boolean(
        string='Encogimiento largo dentro de norma (≤5%)',
        compute='_compute_dentro_norma', store=True)
    encogimiento_ancho_dentro_norma = fields.Boolean(
        string='Encogimiento ancho dentro de norma (≤5%)',
        compute='_compute_dentro_norma', store=True)

    @api.depends('encogimiento_largo', 'encogimiento_ancho')
    def _compute_dentro_norma(self):
        for rec in self:
            rec.encogimiento_largo_dentro_norma = abs(rec.encogimiento_largo) <= 5.0
            rec.encogimiento_ancho_dentro_norma = abs(rec.encogimiento_ancho) <= 5.0

    notas = fields.Text(string='Notas')

    _product_acabado_uniq = models.Constraint(
        'unique(product_acabado_id)',
        'Ya existe una ficha técnica de acabado para ese producto.',
    )
