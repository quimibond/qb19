from odoo import fields, models


class Website(models.Model):
    _inherit = 'website'

    qb_prov_cxp_user_id = fields.Many2one(
        'res.users', string='Recibe las facturas de proveedores',
        domain="[('share', '=', False)]",
        help='Persona de Cuentas por pagar que recibe una actividad cada vez que un proveedor sube '
             'una factura en el portal. Si se deja vacío, la recibe el comprador de la orden.')
    qb_prov_alta_user_id = fields.Many2one(
        'res.users', string='Recibe las altas de proveedores',
        domain="[('share', '=', False)]",
        help='Persona de Compras que recibe una actividad por cada solicitud de alta de proveedor '
             'que llega del sitio web.')
    qb_prov_dia_pago = fields.Selection([
        ('0', 'Lunes'), ('1', 'Martes'), ('2', 'Miércoles'), ('3', 'Jueves'), ('4', 'Viernes'),
    ], string='Día de pago a proveedores',
        help='Si la empresa paga un día fijo de la semana, el portal le muestra al proveedor como '
             'fecha programada el primer día de pago en o después del vencimiento de su factura. '
             'Vacío: se muestra el vencimiento.')
