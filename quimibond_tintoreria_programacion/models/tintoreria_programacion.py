# -*- coding: utf-8 -*-
from odoo import fields, models

BANDAS = [('A', 'A (3 a <7 m/kg)'), ('B', 'B (7 a <11 m/kg)'), ('C', 'C (11 a <16 m/kg)')]
ORIGENES_TIEMPO = [
    ('ficha', 'Ficha técnica (artículo en el jet)'),
    ('formula', 'Fórmula × HTJ'),
    ('bom', 'Operación de la BoM (sin dato de ficha ni de fórmula)'),
]


class TintoreriaProgramacion(models.Model):
    """Registro de una programación de Tintorería: la MO de origen, el HTJ elegido, el cálculo
    del tamaño de baño y las MOs resultantes de la división."""
    _name = 'tintoreria.programacion'
    _description = 'Programación de Tintorería'
    _inherit = ['mail.thread']
    _order = 'id desc'

    name = fields.Char('Orden de origen', required=True, readonly=True,
                       help="Nombre de la MO antes de dividirla.")
    state = fields.Selection(
        [('programada', 'Programada'), ('cancelada', 'Cancelada')],
        'Estado', default='programada', required=True, tracking=True)
    product_id = fields.Many2one('product.product', 'Producto (kg)', required=True, readonly=True)
    workcenter_id = fields.Many2one('mrp.workcenter', 'HTJ', required=True, readonly=True)
    kg_total = fields.Float('kg totales', digits=(16, 3), readonly=True)
    rendimiento = fields.Float('Rendimiento (m/kg)', digits=(12, 4), readonly=True)
    banda = fields.Selection(BANDAS, 'Banda de rendimiento', readonly=True)
    capacidad_kg = fields.Float('Capacidad por baño (kg)', digits=(16, 3), readonly=True)
    n_banos = fields.Integer('No. de baños', readonly=True)
    kg_bano = fields.Float('kg por baño', digits=(16, 3), readonly=True)
    minutos_bano = fields.Float('Minutos por baño', digits=(12, 2), readonly=True)
    origen_tiempo = fields.Selection(ORIGENES_TIEMPO, 'Origen del tiempo', readonly=True)
    production_ids = fields.One2many('mrp.production', 'tintoreria_programacion_id', 'Órdenes de fabricación')
    user_id = fields.Many2one('res.users', 'Programó', default=lambda self: self.env.user, readonly=True)
    fecha = fields.Datetime('Fecha de programación', default=fields.Datetime.now, readonly=True)
    company_id = fields.Many2one('res.company', 'Compañía', default=lambda self: self.env.company, readonly=True)
