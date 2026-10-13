# -*- coding: utf-8 -*-
"""Ficha de la máquina: tipos, parámetros y configuraciones por fecha.

2.3.0 (Dirección General 2026-10-13): los datos de una circular (diámetro,
galga, agujas, alimentadores, velocidad máxima) son de la máquina, no del
artículo. Una circular puede cambiar de cilindro y con eso cambian galga y
diámetro, así que se guardan como **configuraciones con fecha** («vigente
desde»): la vigente es la de fecha más reciente que ya empezó. La ficha de
tejido guarda la configuración con la que se elaboró y toma de ella sus datos
de máquina; un cambio de cilindro posterior no altera las fichas anteriores.

2.4.0 (Dirección General 2026-10-13): la ficha es para **cualquier tipo de
máquina** (tejido, tintorería, acabado, inspección, abridora, devanadora…).
Cada centro de trabajo tiene un **tipo de máquina**
(``ficha.tecnica.maquina.tipo``) y cada tipo su lista de **parámetros**
(``ficha.tecnica.maquina.parametro``: clave, nombre, unidad, numérico o
texto). Una configuración guarda un **valor por parámetro de su tipo**
(``ficha.tecnica.maquina.valor``). Los parámetros se dan de alta en
Configuración → Tipos de máquina; el módulo solo trae los de tejido circular,
que ya estaban definidos (los demás no se inventan). Galga, diámetro, agujas,
alimentadores y velocidad máxima siguen como columnas de la configuración,
calculadas desde los valores con esas claves, porque la ficha de tejido y la
búsqueda de máquinas por galga los usan.

Marca y modelo son de la máquina (no cambian con el cilindro) y viven en el
centro de trabajo.
"""
import operator as py_operator

from odoo import Command, api, fields, models
from odoo.exceptions import UserError, ValidationError

OPERATORS = {'=': py_operator.eq, '!=': py_operator.ne, '<': py_operator.lt, '<=': py_operator.le,
             '>': py_operator.gt, '>=': py_operator.ge}
PARAM_KINDS = [('num', 'Numérico'), ('text', 'Texto')]
# Columnas de la configuración que salen del valor del parámetro con la misma
# clave (las usan la ficha de tejido y la búsqueda de máquinas por galga).
CONFIG_VALUE_FIELDS = ('diametro', 'galga', 'no_agujas', 'no_alimentadores', 'velocidad_max')
INT_VALUE_FIELDS = ('no_agujas', 'no_alimentadores')


def _fmt(value):
    return ('%.3f' % value).rstrip('0').rstrip('.')


class FichaTecnicaMaquinaTipo(models.Model):
    """Tipo de máquina (tejido circular, tintorería, acabado…) con sus parámetros."""
    _name = 'ficha.tecnica.maquina.tipo'
    _description = 'Tipo de máquina (ficha de la máquina)'
    _order = 'sequence, name, id'

    sequence = fields.Integer(default=10)
    code = fields.Char(string='Clave', required=True, index=True,
                       help='Clave corta y estable del tipo (p. ej. «tejido_circular»).')
    name = fields.Char(string='Tipo de máquina', required=True)
    parametro_ids = fields.One2many('ficha.tecnica.maquina.parametro', 'tipo_id', string='Parámetros',
                                    context={'active_test': False})
    workcenter_ids = fields.One2many('mrp.workcenter', 'ft_tipo_id', string='Máquinas')
    workcenter_count = fields.Integer(string='Máquinas', compute='_compute_workcenter_count')
    active = fields.Boolean(default=True)

    _code_uniq = models.Constraint('unique(code)', 'La clave del tipo de máquina debe ser única.')

    @api.depends('workcenter_ids')
    def _compute_workcenter_count(self):
        for tipo in self:
            tipo.workcenter_count = len(tipo.workcenter_ids)


class FichaTecnicaMaquinaParametro(models.Model):
    """Parámetro que se captura en la ficha de las máquinas de un tipo."""
    _name = 'ficha.tecnica.maquina.parametro'
    _description = 'Parámetro de la ficha de la máquina'
    _order = 'tipo_id, sequence, id'

    tipo_id = fields.Many2one('ficha.tecnica.maquina.tipo', string='Tipo de máquina', required=True,
                              index=True, ondelete='cascade')
    sequence = fields.Integer(default=10)
    code = fields.Char(string='Clave', required=True,
                       help='Clave corta y estable dentro del tipo. En tejido, «galga», «diametro», «no_agujas», '
                            '«no_alimentadores» y «velocidad_max» alimentan la ficha de tejido.')
    name = fields.Char(string='Parámetro', required=True)
    unit = fields.Char(string='Unidad', help='Unidad de medida (in, rpm, kg, m/min, °C…).')
    kind = fields.Selection(PARAM_KINDS, string='Tipo de dato', required=True, default='num')
    notas = fields.Char(string='Cómo se mide', help='De dónde sale el dato o cómo se mide.')
    active = fields.Boolean(default=True, help='Los parámetros archivados no se agregan a configuraciones nuevas.')

    _tipo_code_uniq = models.Constraint('unique(tipo_id, code)', 'Ese tipo de máquina ya tiene un parámetro '
                                                                   'con esa clave.')

    def unlink(self):
        lines = self.env['ficha.tecnica.maquina.valor'].search([('parametro_id', 'in', self.ids)])
        if lines.filtered(lambda v: v.valor or v.valor_texto):
            raise UserError('El parámetro ya tiene valores capturados en configuraciones: archívelo en lugar '
                            'de borrarlo.')
        lines.unlink()
        return super().unlink()


class FichaTecnicaMaquinaValor(models.Model):
    """Valor de un parámetro en una configuración de máquina."""
    _name = 'ficha.tecnica.maquina.valor'
    _description = 'Valor de parámetro en la configuración de la máquina'
    _order = 'config_id, sequence, id'

    config_id = fields.Many2one('ficha.tecnica.maquina.config', string='Configuración', required=True,
                                index=True, ondelete='cascade')
    parametro_id = fields.Many2one('ficha.tecnica.maquina.parametro', string='Parámetro', required=True,
                                   ondelete='restrict')
    sequence = fields.Integer(related='parametro_id.sequence', store=True)
    code = fields.Char(related='parametro_id.code')
    kind = fields.Selection(related='parametro_id.kind')
    unit = fields.Char(related='parametro_id.unit')
    valor = fields.Float(string='Valor', digits=(16, 3))
    valor_texto = fields.Char(string='Valor (texto)')

    _config_parametro_uniq = models.Constraint('unique(config_id, parametro_id)',
                                               'El parámetro ya está en esa configuración.')

    @api.constrains('parametro_id', 'config_id')
    def _check_tipo(self):
        for line in self:
            if line.parametro_id.tipo_id != line.config_id.tipo_id:
                raise ValidationError('El parámetro «%s» es de %s y la configuración es de %s.' % (
                    line.parametro_id.name, line.parametro_id.tipo_id.name, line.config_id.tipo_id.name or '(sin tipo)'))

    def _display_value(self):
        self.ensure_one()
        if self.kind == 'text':
            return self.valor_texto or ''
        if not self.valor:
            return ''
        return '%s %s' % (_fmt(self.valor), self.unit) if self.unit else _fmt(self.valor)


class FichaTecnicaMaquinaConfig(models.Model):
    _name = 'ficha.tecnica.maquina.config'
    _description = 'Configuración de máquina por fecha'
    _order = 'workcenter_id, date_from desc, id desc'

    workcenter_id = fields.Many2one('mrp.workcenter', string='Máquina', required=True, index=True,
                                    ondelete='cascade', help='Centro de trabajo (máquina) de esta configuración.')
    tipo_id = fields.Many2one('ficha.tecnica.maquina.tipo', string='Tipo de máquina', required=True, index=True,
                              compute='_compute_tipo_id', store=True, readonly=False, precompute=True,
                              help='Sale del tipo de la máquina; define qué parámetros se capturan.')
    date_from = fields.Date(string='Vigente desde', required=True, default=fields.Date.context_today,
                            help='Desde cuándo la máquina corre con esta configuración (p. ej. el cambio de '
                                 'cilindro). La vigente es la de fecha más reciente que ya empezó.')
    date_to = fields.Date(string='Vigente hasta', compute='_compute_date_to',
                          help='Un día antes de la siguiente configuración de la máquina; vacío si es la última.')
    valor_ids = fields.One2many('ficha.tecnica.maquina.valor', 'config_id', string='Parámetros')
    resumen = fields.Char(string='Parámetros', compute='_compute_resumen')
    diametro = fields.Float(string='Diámetro (in)', digits=(16, 2), compute='_compute_value_fields',
                            inverse='_inverse_value_fields', store=True)
    galga = fields.Float(string='Galga', digits=(16, 0), compute='_compute_value_fields',
                         inverse='_inverse_value_fields', store=True)
    no_agujas = fields.Integer(string='No. de agujas', compute='_compute_value_fields',
                               inverse='_inverse_value_fields', store=True)
    no_alimentadores = fields.Integer(string='No. de alimentadores', compute='_compute_value_fields',
                                      inverse='_inverse_value_fields', store=True)
    velocidad_max = fields.Float(string='Velocidad máxima (rpm)', digits=(16, 1), compute='_compute_value_fields',
                                 inverse='_inverse_value_fields', store=True,
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

    @api.depends('workcenter_id')
    def _compute_tipo_id(self):
        for config in self:
            if config.workcenter_id.ft_tipo_id or not config.tipo_id:
                config.tipo_id = config.workcenter_id.ft_tipo_id

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

    @api.depends('valor_ids.valor', 'valor_ids.valor_texto', 'valor_ids.parametro_id.name',
                 'valor_ids.parametro_id.unit')
    def _compute_resumen(self):
        for config in self:
            parts = []
            for line in config.valor_ids:
                shown = line._display_value()
                if shown:
                    parts.append('%s: %s' % (line.parametro_id.name, shown))
            config.resumen = ' · '.join(parts)

    @api.depends('valor_ids.valor', 'valor_ids.parametro_id.code')
    def _compute_value_fields(self):
        for config in self:
            values = {line.parametro_id.code: line.valor for line in config.valor_ids}
            for name in CONFIG_VALUE_FIELDS:
                value = values.get(name) or 0.0
                config[name] = int(round(value)) if name in INT_VALUE_FIELDS else value

    def _inverse_value_fields(self):
        for config in self:
            # Primero se leen todas: crear un renglón recalcula las demás columnas desde los renglones.
            wanted = {name: config[name] for name in CONFIG_VALUE_FIELDS}
            for name, value in wanted.items():
                config._ft_set_value(name, value)

    def _ft_set_value(self, code, value):
        """Escribe el valor del parámetro ``code`` del tipo de la configuración (crea el renglón si falta).
        Un valor distinto de cero en un parámetro que el tipo no tiene es un error."""
        self.ensure_one()
        line = self.valor_ids.filtered(lambda v: v.parametro_id.code == code)[:1]
        if line:
            if line.valor != (value or 0.0):
                line.valor = value or 0.0
            return line
        if not value:
            return line
        param = self.env['ficha.tecnica.maquina.parametro'].with_context(active_test=False).search(
            [('tipo_id', '=', self.tipo_id.id), ('code', '=', code)], limit=1)
        if not param:
            raise UserError('El tipo de máquina «%s» no tiene el parámetro «%s»; dele de alta en Configuración '
                            '→ Tipos de máquina.' % (self.tipo_id.name or '(sin tipo)', code))
        return self.env['ficha.tecnica.maquina.valor'].create(
            {'config_id': self.id, 'parametro_id': param.id, 'valor': value})

    def _ft_value(self, code):
        """Valor del parámetro ``code`` (número o texto según el parámetro); False si no está."""
        self.ensure_one()
        line = self.valor_ids.filtered(lambda v: v.parametro_id.code == code)[:1]
        if not line:
            return False
        return line.valor_texto if line.kind == 'text' else line.valor

    def _ft_sync_valores(self):
        """Un renglón por parámetro activo del tipo (vacío si no se capturó); quita los de otro tipo."""
        Valor = self.env['ficha.tecnica.maquina.valor']
        for config in self:
            config.valor_ids.filtered(lambda v: v.parametro_id.tipo_id != config.tipo_id).unlink()
            present = config.valor_ids.parametro_id
            missing = config.tipo_id.parametro_ids.filtered(lambda p: p.active and p not in present)
            if missing:
                Valor.create([{'config_id': config.id, 'parametro_id': p.id} for p in missing])

    @api.onchange('workcenter_id', 'tipo_id')
    def _onchange_tipo_id(self):
        for config in self:
            if config.valor_ids.parametro_id.tipo_id == config.tipo_id and \
                    config.tipo_id.parametro_ids.filtered('active') <= config.valor_ids.parametro_id:
                continue
            kept = {line.parametro_id.id: line for line in config.valor_ids
                    if line.parametro_id.tipo_id == config.tipo_id}
            commands = [Command.clear()]
            for param in config.tipo_id.parametro_ids.filtered('active'):
                old = kept.get(param.id)
                commands.append(Command.create({'parametro_id': param.id, 'valor': old.valor if old else 0.0,
                                                'valor_texto': old.valor_texto if old else False}))
            config.valor_ids = commands

    @api.model_create_multi
    def create(self, vals_list):
        configs = super().create(vals_list)
        for config in configs:
            if config.tipo_id and not config.workcenter_id.ft_tipo_id:
                config.workcenter_id.sudo().ft_tipo_id = config.tipo_id
        configs._ft_sync_valores()
        return configs

    def write(self, vals):
        res = super().write(vals)
        if 'tipo_id' in vals:
            self._ft_sync_valores()
        return res

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

    ft_tipo_id = fields.Many2one('ficha.tecnica.maquina.tipo', string='Tipo de máquina', index=True,
                                 help='Define qué parámetros lleva la ficha de la máquina.')
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
    # 2.5.0: calculados con la vigente (no «related» sobre ft_config_id, que no se puede buscar y dejaba
    # aviso en el build).
    ft_valor_ids = fields.Many2many('ficha.tecnica.maquina.valor', string='Parámetros vigentes',
                                    compute='_compute_ft_current')
    ft_resumen = fields.Char(string='Parámetros vigentes (resumen)', compute='_compute_ft_current')
    ft_ficha_tejido_count = fields.Integer(string='Fichas de tejido', compute='_compute_ft_ficha_tejido_count')

    def _ft_config_at(self, day=None):
        """La configuración vigente en ``day`` (hoy si no se da): la de fecha de inicio más reciente que ya
        empezó. Vacía si la máquina no tiene ninguna para esa fecha."""
        self.ensure_one()
        day = fields.Date.to_date(day) if day else fields.Date.context_today(self)
        configs = self.ft_config_ids.filtered(lambda c: c.date_from and c.date_from <= day)
        return configs.sorted(lambda c: (c.date_from, c.id), reverse=True)[:1]

    @api.depends('ft_config_ids.date_from', 'ft_config_ids.galga', 'ft_config_ids.diametro',
                 'ft_config_ids.no_agujas', 'ft_config_ids.no_alimentadores', 'ft_config_ids.velocidad_max',
                 'ft_config_ids.valor_ids', 'ft_config_ids.resumen')
    def _compute_ft_current(self):
        for wc in self:
            config = wc._ft_config_at()
            wc.ft_config_id = config
            wc.ft_valor_ids = config.valor_ids
            wc.ft_resumen = config.resumen
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
