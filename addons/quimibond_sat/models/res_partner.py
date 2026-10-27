# -*- coding: utf-8 -*-
from odoo import fields, models

# Estados de la lista 69-B del SAT tal como los manda Syntage
# (issuer.blacklistStatus): presunto → definitivo, o desvirtuado / sentencia
# favorable cuando el contribuyente acredita sus operaciones.
LISTA_69B = [
    ('presumed', 'Presunto'),
    ('definitive', 'Definitivo'),
    ('dismissed', 'Desvirtuado'),
    ('favorable', 'Sentencia favorable'),
]
RIESGO_69B = ('presumed', 'definitive')


class ResPartner(models.Model):
    _inherit = 'res.partner'

    sat_cfdi_policy = fields.Selection([
        ('factura', 'Factura con CFDI (se concilia)'),
        ('poliza', 'Se registra por póliza (banco, impuestos): el CFDI se ignora'),
        ('sin_cfdi', 'No emite CFDI (extranjero, nómina): sus facturas no se comparan'),
    ], string='CFDI del SAT', default='factura', required=True,
        help='Cómo trata la comparación SAT vs Odoo a este contacto. Los contactos con '
             'país distinto de México cuentan como "no emite CFDI" aunque estén en "factura".')
    sat_terceros_product_ids = fields.Many2many(
        'product.template', 'res_partner_sat_terceros_rel', 'partner_id', 'product_tmpl_id',
        string='Pagos a terceros con este proveedor',
        help='Productos que, en las facturas de este proveedor, son dinero que pasa a terceros sin '
             'su CFDI: la carga de gasolina, peajes o vales en las facturas de Edenred o Si Vale, '
             'cuyo CFDI ampara solo la comisión. No cuentan contra el CFDI (como los productos '
             'marcados «Pago a terceros», pero solo con este proveedor).')
    sat_69b_status = fields.Selection(
        LISTA_69B, string='Lista 69-B (SAT)', readonly=True, copy=False,
        help='Estado del proveedor en la lista 69-B del SAT (operaciones simuladas, EFOS) según '
             'su CFDI más reciente en Syntage. Con "Definitivo", sus facturas no se pueden '
             'publicar en Odoo hasta que Contabilidad documente las operaciones.')
    sat_69b_seen = fields.Datetime(
        string='69-B según el CFDI del', readonly=True, copy=False,
        help='Emisión del CFDI más reciente de este proveedor: de ahí sale el estado 69-B.')
    sat_69b_autorizado = fields.Boolean(
        string='69-B: operaciones documentadas', copy=False,
        help='Contabilidad documentó que las operaciones con este proveedor son reales '
             '(materialidad). Con la casilla, sus facturas se publican aunque esté en la lista '
             'definitiva; el aviso en la factura se mantiene.')
