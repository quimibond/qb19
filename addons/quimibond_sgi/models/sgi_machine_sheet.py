# -*- coding: utf-8 -*-
"""Ficha técnica de proceso por máquina (56.0.0).

Sustituye F-IT-P-P01-08-05 «Ficha técnica de control de proceso máquinas
circulares»: los parámetros con los que se corre un artículo en un centro de
trabajo (hilos, poleas, condiciones de máquina y datos de la tela
acondicionada con especificación y tolerancia). Una ficha vigente por
artículo y centro; las anteriores quedan obsoletas, nunca se borran.
"""
from odoo import api, fields, models
from odoo.exceptions import ValidationError

MACHINE_PARAMS = [
    ('maquina', "Consumo (cm/vuelta – longitud de malla)", "cm"),
    ('maquina', "Tensión", "g"), ('maquina', "Polea de alimentación", "pto"),
    ('maquina', "Punto cilindro", "pto"), ('maquina', "Punto plato", "pto"),
    ('maquina', "Altura plato", "pto"), ('maquina', "No. de alimentadores", ""),
    ('maquina', "Ancho bastidor", "m"), ('maquina', "Estiraje", ""),
    ('maquina', "Diámetro / galga / agujas", ""),
    ('tela', "Peso", "g/m²"), ('tela', "Ancho", "cm"), ('tela', "Espesor", "mm"),
    ('tela', "Columnas", "/plg"), ('tela', "Mallas", "/plg"),
    ('tela', "Elongación bajo carga largo", "%"), ('tela', "Elongación bajo carga ancho", "%"),
]


class SgiMachineSheet(models.Model):
    _name = 'sgi.machine.sheet'
    _description = "Ficha técnica de proceso por máquina (F-IT-P-P01-08-05)"
    _inherit = ['mail.thread', 'mail.activity.mixin']
    _order = 'product_id, workcenter_id, revision desc, id desc'

    name = fields.Char(string="Folio", readonly=True, copy=False, default="Nuevo")
    product_id = fields.Many2one('product.product', string="Artículo", required=True, index=True, tracking=True)
    workcenter_id = fields.Many2one('mrp.workcenter', string="Máquina / centro de trabajo", required=True, index=True, tracking=True)
    date = fields.Date(string="Fecha", default=fields.Date.context_today, required=True)
    area = fields.Selection([('produccion', "Producción"), ('desarrollos', "Desarrollos")], default='produccion', required=True)
    revision = fields.Integer(string="Revisión", default=0)
    state = fields.Selection([('borrador', "Borrador"), ('vigente', "Vigente"), ('obsoleta', "Obsoleta")],
                             default='borrador', required=True, tracking=True)
    active = fields.Boolean(default=True)
    yarn_line_ids = fields.One2many('sgi.machine.sheet.yarn', 'sheet_id', string="Materia prima (hilos)")
    pulley_1 = fields.Char(string="Polea 1")
    pulley_2 = fields.Char(string="Polea 2")
    pulley_3 = fields.Char(string="Polea 3")
    pulley_4 = fields.Char(string="Polea 4")
    yarn_note = fields.Text(string="Observaciones de materia prima")
    param_line_ids = fields.One2many('sgi.machine.sheet.param', 'sheet_id', string="Parámetros")
    machine_note = fields.Text(string="Observaciones de máquina")
    fabric_note = fields.Text(string="Observaciones de tela acondicionada")
    lab_user_id = fields.Many2one('res.users', string="Laboratorista de tejido")
    mechanic_user_id = fields.Many2one('res.users', string="Mecánico de tejido")
    approved_by_id = fields.Many2one('res.users', string="Jefe técnico de tejido y acabado")
    engineering_by_id = fields.Many2one('res.users', string="Jefe de ingeniería de procesos")
    company_id = fields.Many2one('res.company', default=lambda self: self.env.company)

    @api.model_create_multi
    def create(self, vals_list):
        Seq = self.env['ir.sequence']
        for vals in vals_list:
            if not vals.get('name') or vals['name'] == "Nuevo":
                vals['name'] = Seq.next_by_code('sgi.machine.sheet') or "Nuevo"
            if not vals.get('param_line_ids'):
                vals['param_line_ids'] = [(0, 0, {'section': section, 'name': name, 'unit': unit, 'sequence': i * 10})
                                          for i, (section, name, unit) in enumerate(MACHINE_PARAMS)]
        return super().create(vals_list)

    @api.constrains('state', 'product_id', 'workcenter_id')
    def _check_single_current(self):
        for sheet in self.filtered(lambda s: s.state == 'vigente'):
            other = self.search([('id', '!=', sheet.id), ('state', '=', 'vigente'),
                                 ('product_id', '=', sheet.product_id.id),
                                 ('workcenter_id', '=', sheet.workcenter_id.id)], limit=1)
            if other:
                raise ValidationError(
                    "Ya hay una ficha vigente de %s en %s (%s). Márcala obsoleta antes de poner esta en vigor."
                    % (sheet.product_id.display_name, sheet.workcenter_id.name, other.name))

    def action_set_current(self):
        for sheet in self:
            previous = self.search([('id', '!=', sheet.id), ('state', '=', 'vigente'),
                                    ('product_id', '=', sheet.product_id.id),
                                    ('workcenter_id', '=', sheet.workcenter_id.id)])
            previous.write({'state': 'obsoleta'})
            sheet.write({'state': 'vigente', 'revision': (max(previous.mapped('revision')) + 1) if previous else sheet.revision})
        return True

    def action_set_obsolete(self):
        self.write({'state': 'obsoleta'})
        return True

    def action_new_revision(self):
        self.ensure_one()
        new = self.copy({'state': 'borrador', 'revision': self.revision + 1, 'date': fields.Date.context_today(self)})
        return {'type': 'ir.actions.act_window', 'res_model': self._name, 'res_id': new.id, 'view_mode': 'form'}

    def copy_data(self, default=None):
        default = dict(default or {})
        vals_list = super().copy_data(default=default)
        for sheet, vals in zip(self, vals_list):
            vals['yarn_line_ids'] = [(0, 0, l.copy_data()[0]) for l in sheet.yarn_line_ids]
            vals['param_line_ids'] = [(0, 0, l.copy_data()[0]) for l in sheet.param_line_ids]
        return vals_list

    def sgi_format_info(self):
        self.ensure_one()
        code = 'F-IT-P-P01-08-05'
        revision = self.env['sgi.format.map'].sudo()._revision_of(code)
        return "%s · Rev. %s" % (code, revision) if revision else code


class SgiMachineSheetYarn(models.Model):
    _name = 'sgi.machine.sheet.yarn'
    _description = "Hilo de la ficha de proceso por máquina"
    _order = 'sequence, id'

    sheet_id = fields.Many2one('sgi.machine.sheet', required=True, ondelete='cascade')
    sequence = fields.Integer(default=10)
    fiber = fields.Selection([('poliester', "Poliéster"), ('algodon', "Algodón"), ('poliamida', "Poliamida"), ('otro', "Otro")],
                             string="Fibra", required=True, default='poliester')
    feed = fields.Char(string="Disposición del hilo")
    yarn_type = fields.Char(string="Tipo de hilo")
    title = fields.Char(string="Título de hilo")
    twist = fields.Char(string="Torsión")
    consumption = fields.Char(string="cm/vuelta – longitud (m)")
    pct = fields.Float(string="% de hilo")


class SgiMachineSheetParam(models.Model):
    _name = 'sgi.machine.sheet.param'
    _description = "Parámetro de la ficha de proceso por máquina"
    _order = 'section, sequence, id'

    sheet_id = fields.Many2one('sgi.machine.sheet', required=True, ondelete='cascade')
    sequence = fields.Integer(default=10)
    section = fields.Selection([('maquina', "Datos de máquina"), ('tela', "Datos de tela acondicionada")], required=True, default='maquina')
    name = fields.Char(string="Condición", required=True)
    spec = fields.Char(string="Especificación")
    tolerance = fields.Char(string="Tolerancia (±)")
    unit = fields.Char(string="Unidad")
