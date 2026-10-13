# -*- coding: utf-8 -*-
"""Ficha de la máquina: configuraciones por fecha (2.3.0).

Dirección General 2026-10-13: los datos de una circular (diámetro, galga,
agujas, alimentadores, velocidad máxima) son de la máquina, no del artículo.
Una circular puede cambiar de cilindro y con eso cambian galga y diámetro,
así que se guardan como **configuraciones con fecha** («vigente desde»): la
vigente es la de fecha más reciente que ya empezó. La ficha de tejido guarda
la configuración con la que se elaboró y toma de ella sus datos de máquina;
un cambio de cilindro posterior no altera las fichas anteriores.

Marca y modelo son de la máquina (no cambian con el cilindro) y viven en el
centro de trabajo.
"""
import operator as py_operator

from odoo import api, fields, models
from odoo.exceptions import UserError

OPERATORS = {'=': py_operator.eq, '!=': py_operator.ne, '<': py_operator.lt, '<=': py_operator.le,
             '>': py_operator.gt, '>=': py_operator.ge}


class FichaTecnicaMaquinaConfig(models.Model):
    _name = 'ficha.tecnica.maquina.config'
    _description = 'Configuración de máquina por fecha (cilindro)'
    _order = 'workcenter_id, date_from desc, id desc'

    workcenter_id = fields.Many2one('mrp.workcenter', string='Máquina', required=True, index=True,
                                    ondelete='cascade', help='Centro de trabajo (máquina) de esta configuración.')
    date_from = fields.Date(string='Vigente desde', required=True, default=fields.Date.context_today,
                            help='Desde cuándo la máquina corre con esta configuración (p. ej. el cambio de '
                                 'cilindro). La vigente es la de fecha más reciente que ya empezó.')
    date_to = fields.Date(string='Vigente hasta', compute='_compute_date_to',
                          help='Un día antes de la siguiente configuración de la máquina; vacío si es la última.')
    diametro = fields.Float(string='Diámetro (in)', digits=(16, 2))
    galga = fields.Float(string='Galga', digits=(16, 0))
    no_agujas = fields.Integer(string='No. de agujas')
    no_alimentadores = fields.Integer(string='No. de alimentadores')
    velocidad_max = fields.Float(string='Velocidad máxima (rpm)', digits=(16, 1),
                                 help='Velocidad máxima de la máquina con esta configuración.')
    motivo = fields.Char(string='Motivo', help='Por qué cambió la configuración (cambio de cilindro, alta…).')
    notas = fields.Text(string='Notas')
    is_current = fields.Boolean(string='Vigente hoy', compute='_compute_is_current')
    ficha_tejido_ids = fields.One2many('ficha.tecnica.tejido', 'maquina_config_id', string='Fichas de tejido')
    ficha_tejido_count = fields.Integer(string='Fichas de tejido', compute='_compute_ficha_tejido_count')

    _workcenter_date_uniq = models.Constraint(
        'unique(workcenter_id, date_from)',
        'La máquina ya tiene una configuración con esa fecha de inicio.',
    )

    @api.depends('workcenter_id.name', 'galga', 'diametro', 'date_from')
    def _compute_display_name(self):
        for config in self:
            parts = [config.workcenter_id.name or '']
            if config.galga:
                parts.append('E%g' % config.galga)
            if config.diametro:
                parts.append('%g"' % config.diametro)
            if config.date_from:
                parts.append('desde %s' % config.date_from.strftime('%d/%m/%Y'))
            config.display_name = ' · '.join(p for p in parts if p)

    @api.depends('workcenter_id.ft_config_ids.date_from', 'date_from')
    def _compute_date_to(self):
        for config in self:
            later = config.workcenter_id.ft_config_ids.filtered(
                lambda c: c.date_from and config.date_from and c.date_from > config.date_from)
            nxt = min(later.mapped('date_from')) if later else False
            config.date_to = fields.Date.subtract(nxt, days=1) if nxt else False

    def _compute_is_current(self):
        today = fields.Date.context_today(self)
        for config in self:
            config.is_current = config.workcenter_id._ft_config_at(today) == config

    @api.depends('ficha_tejido_ids')
    def _compute_ficha_tejido_count(self):
        for config in self:
            config.ficha_tejido_count = len(config.ficha_tejido_ids)

    def unlink(self):
        used = self.filtered('ficha_tejido_ids')
        if used:
            raise UserError('La configuración %s ya la usan fichas de tejido: no se borra. Registre una nueva '
                            'configuración con su fecha.' % used[0].display_name)
        return super().unlink()


class MrpWorkcenterFicha(models.Model):
    _inherit = 'mrp.workcenter'

    ft_marca = fields.Char(string='Marca de la máquina')
    ft_modelo = fields.Char(string='Modelo de la máquina')
    ft_config_ids = fields.One2many('ficha.tecnica.maquina.config', 'workcenter_id', string='Configuraciones')
    ft_config_id = fields.Many2one('ficha.tecnica.maquina.config', string='Configuración vigente',
                                   compute='_compute_ft_current')
    ft_galga = fields.Float(string='Galga vigente', digits=(16, 0), compute='_compute_ft_current',
                            search='_search_ft_galga')
    ft_diametro = fields.Float(string='Diámetro vigente (in)', digits=(16, 2), compute='_compute_ft_current')
    ft_no_agujas = fields.Integer(string='Agujas vigentes', compute='_compute_ft_current')
    ft_no_alimentadores = fields.Integer(string='Alimentadores vigentes', compute='_compute_ft_current')
    ft_velocidad_max = fields.Float(string='Velocidad máxima vigente (rpm)', compute='_compute_ft_current')
    ft_ficha_tejido_count = fields.Integer(string='Fichas de tejido', compute='_compute_ft_ficha_tejido_count')

    def _ft_config_at(self, day=None):
        """La configuración vigente en ``day`` (hoy si no se da): la de fecha de inicio más reciente que ya
        empezó. Vacía si la máquina no tiene ninguna para esa fecha."""
        self.ensure_one()
        day = fields.Date.to_date(day) if day else fields.Date.context_today(self)
        configs = self.ft_config_ids.filtered(lambda c: c.date_from and c.date_from <= day)
        return configs.sorted(lambda c: (c.date_from, c.id), reverse=True)[:1]

    @api.depends('ft_config_ids.date_from', 'ft_config_ids.galga', 'ft_config_ids.diametro',
                 'ft_config_ids.no_agujas', 'ft_config_ids.no_alimentadores', 'ft_config_ids.velocidad_max')
    def _compute_ft_current(self):
        for wc in self:
            config = wc._ft_config_at()
            wc.ft_config_id = config
            wc.ft_galga = config.galga
            wc.ft_diametro = config.diametro
            wc.ft_no_agujas = config.no_agujas
            wc.ft_no_alimentadores = config.no_alimentadores
            wc.ft_velocidad_max = config.velocidad_max

    def _search_ft_galga(self, operator, value):
        """Máquinas cuya configuración vigente hoy tiene esa galga (para ofrecer solo las compatibles)."""
        compare = OPERATORS.get(operator)
        if not compare:
            raise UserError('La galga vigente no se puede buscar con «%s».' % operator)
        today = fields.Date.context_today(self)
        current = {}
        for config in self.env['ficha.tecnica.maquina.config'].search(
                [('date_from', '<=', today)], order='workcenter_id, date_from desc, id desc'):
            current.setdefault(config.workcenter_id.id, config)
        return [('id', 'in', [wc_id for wc_id, config in current.items()
                              if compare(config.galga, float(value or 0.0))])]

    def _compute_ft_ficha_tejido_count(self):
        data = self.env['ficha.tecnica.tejido']._read_group(
            [('maquina_tejido', 'in', self.ids)], ['maquina_tejido'], ['__count'])
        counts = {wc.id: count for wc, count in data}
        for wc in self:
            wc.ft_ficha_tejido_count = counts.get(wc.id, 0)

    def action_ft_fichas_tejido(self):
        self.ensure_one()
        return {'type': 'ir.actions.act_window', 'name': 'Fichas de tejido — %s' % self.name,
                'res_model': 'ficha.tecnica.tejido', 'view_mode': 'list,form',
                'domain': [('maquina_tejido', '=', self.id)], 'context': {'default_maquina_tejido': self.id}}
