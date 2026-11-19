from odoo import fields, models


class Website(models.Model):
    _inherit = 'website'

    qb_diseno_b2b = fields.Boolean(
        string='Diseño B2B de Quimibond',
        help='Muestra en este sitio el encabezado, el pie, la portada y las '
             'páginas B2B del módulo website_quimibond (catálogo técnico, '
             'mercados, planta, calidad y solicitud de cotización). '
             'Desmárquela para regresar al diseño anterior.',
    )
