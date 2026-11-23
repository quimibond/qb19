from odoo import fields, models


class ResPartner(models.Model):
    _inherit = 'res.partner'

    qb_mi_cuenta = fields.Boolean(
        string='Acceso a Mi cuenta',
        copy=False,
        groups='sales_team.group_sale_manager',
        help='Invita a esta empresa a «Mi cuenta», el portal del cliente en el sitio: '
             'sus contactos con usuario de portal ven ahí los pedidos, entregas, '
             'facturas y saldo de la empresa. Sin la casilla, sus usuarios entran al '
             'portal de siempre y «Mi cuenta» no se les abre. Se marca en la empresa, '
             'no en cada contacto.',
    )
