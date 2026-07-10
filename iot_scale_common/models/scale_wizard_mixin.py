# -*- coding: utf-8 -*-
from odoo import fields, models


class ScaleWizardMixin(models.AbstractModel):
    """Mixin técnico común para wizards (TransientModel) que capturan peso
    desde una báscula IoT: revisado de calidad, pesaje de rollo, pesaje de
    subproducto, y cualquier otro que se agregue en el futuro.

    Uso en un wizard nuevo o existente:

        class MiWizard(models.TransientModel):
            _name = 'mi.wizard'
            _inherit = ['scale.wizard.mixin']
            _description = '...'

    Esto aporta 'weighing_mode', 'iot_device_id' y 'scale_read_url' sin
    tener que redeclararlos (y sin arriesgarse a que queden desincronizados
    entre wizards, como pasaba antes).
    """
    _name = 'scale.wizard.mixin'
    _description = 'Mixin de Wizard de Báscula IoT'

    weighing_mode = fields.Selection([
        ('iot', 'Báscula Automática IoT'),
        ('manual', 'Captura Manual (Teclado)'),
    ], string="Modo de Pesaje", default='iot', required=True)

    iot_device_id = fields.Many2one(
        'iot.device',
        string="Báscula",
        domain="[('type', '=', 'scale')]",
        default=lambda self: self.env['iot.device'].search(
            [('type', '=', 'scale')], limit=1
        ).id,
    )

    # Campo puente: el widget Owl 'peso_bascula' lee esta URL directamente
    # de this.props.record.data.scale_read_url, en vez de tener una IP
    # quemada en JavaScript. Debe incluirse en la vista como
    # <field name="scale_read_url" invisible="1"/> para que el framework
    # lo cargue en el estado del record (los campos relacionados no
    # declarados en el arch no llegan al cliente).
    scale_read_url = fields.Char(
        string="URL Báscula (interno)",
        related='iot_device_id.scale_read_url',
        readonly=True,
    )
