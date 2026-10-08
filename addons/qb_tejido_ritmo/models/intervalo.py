# -*- coding: utf-8 -*-
"""Un intervalo por rollo pesado y los paros generales de planta."""
from odoo import fields, models

CLASES = [
    ('primer_rollo', 'Primer rollo'),
    ('captura_atrasada', 'Captura atrasada'),
    ('cambio', 'Cambio de artículo'),
    ('pesado_junto', 'Pesado junto con el anterior'),
    ('sin_referencia', 'Sin referencia o sin tiempo medible'),
    ('paro', 'Paro de máquina'),
    ('corrida', 'Corrida'),
]
MOTIVOS = [
    ('mecanico', 'Mecánico'),
    ('falta_hilo', 'Falta de hilo'),
    ('falta_personal', 'Falta de personal'),
    ('mantenimiento', 'Mantenimiento programado'),
    ('sin_orden', 'Sin orden'),
    ('otro', 'Otro'),
]


class QbTejidoIntervalo(models.Model):
    _name = 'qb.tejido.intervalo'
    _description = 'Intervalo de tejido (uno por rollo pesado)'
    _order = 'workcenter_id, fecha_hora'
    _rec_name = 'roll_number'

    weighing_id = fields.Integer(string='Pesaje (id)', index=True, required=True)
    roll_number = fields.Char(string='Rollo')
    company_id = fields.Many2one('res.company', required=True,
                                 default=lambda self: self.env.company)
    workcenter_id = fields.Many2one('mrp.workcenter', string='Máquina', required=True,
                                    index=True, ondelete='cascade')
    production_id = fields.Many2one('mrp.production', string='Orden', ondelete='set null')
    product_id = fields.Many2one('product.product', string='Artículo', index=True,
                                 ondelete='set null')
    default_code = fields.Char(related='product_id.default_code', store=True)
    fecha_hora = fields.Datetime(string='Fecha y hora (UTC)', required=True, index=True)
    fecha_local = fields.Datetime(string='Fecha y hora de planta',
                                  help='Hora local, guardada sin zona para agrupar.')
    semana = fields.Date(string='Semana (lunes)', index=True)
    mes = fields.Date(string='Mes', index=True)
    turno = fields.Selection([('dia', 'Día 7:00-19:00'), ('noche', 'Noche')])
    kg = fields.Float(digits=(12, 3))
    horas_brutas = fields.Float(digits=(12, 4))
    horas_descanso = fields.Float(digits=(12, 4))
    horas_paro_general = fields.Float(digits=(12, 4))
    horas_netas = fields.Float(digits=(12, 4))
    horas_referencia = fields.Float(digits=(12, 4))
    clase = fields.Selection(CLASES, required=True, index=True)
    horas_corrida = fields.Float(digits=(12, 4))
    horas_paro = fields.Float(digits=(12, 4))
    horas_cambio = fields.Float(digits=(12, 4))
    horas_sin_medir = fields.Float(digits=(12, 4))
    kg_corrida = fields.Float(digits=(12, 3))
    motivo_paro = fields.Selection(
        MOTIVOS, string='Motivo del paro',
        help='Lo llena el supervisor de tejido en los intervalos clasificados '
             'como paro. Se conserva al recalcular.')
    calculado_el = fields.Datetime(readonly=True)

    _weighing_uniq = models.Constraint(
        'unique(weighing_id)', 'Ya hay un intervalo para ese pesaje.')


class QbTejidoParoGeneral(models.Model):
    _name = 'qb.tejido.paro.general'
    _description = 'Paro general de planta (hueco sin pesajes)'
    _order = 'inicio desc'
    _rec_name = 'inicio'

    company_id = fields.Many2one('res.company', required=True,
                                 default=lambda self: self.env.company)
    inicio = fields.Datetime(string='Inicio (UTC)', required=True, index=True)
    fin = fields.Datetime(string='Fin (UTC)', required=True)
    inicio_local = fields.Datetime(string='Inicio de planta')
    fin_local = fields.Datetime(string='Fin de planta')
    horas = fields.Float(digits=(12, 2), help='Horas dentro del calendario.')
    es_paro = fields.Boolean(
        string='Fue paro real', default=True,
        help='Desmárquelo si el hueco fue un error de captura y no un paro; '
             'el siguiente recálculo deja de restarlo a las máquinas.')
    motivo = fields.Char()
    calculado_el = fields.Datetime(readonly=True)

    _inicio_uniq = models.Constraint(
        'unique(inicio, company_id)', 'Ya hay un paro general con ese inicio.')
