# -*- coding: utf-8 -*-
from odoo import models, fields, api
from odoo.exceptions import ValidationError

from .sgi_document import SGI_CODE_REGEX


class SgiFormatMap(models.Model):
    """Mapeo formato SGI ↔ documento de Odoo que lo sustituye.

    El registro nativo (cotización, OC, remisión…) porta la clave del formato
    controlado que reemplaza; la revisión NUNCA se captura aquí: se lee en vivo
    del documento vigente en la app Documentos (única fuente de verdad).
    """
    _name = 'sgi.format.map'
    _description = "Formato SGI en documentos de Odoo"
    _order = 'sgi_code'

    model_id = fields.Many2one('ir.model', string="Modelo de Odoo", required=True,
                               ondelete='cascade',
                               help="El documento de Odoo que sustituye al formato en Excel.")
    model_name = fields.Char(related='model_id.model', string="Modelo técnico", store=True)
    sgi_code = fields.Char(string="Clave SGI", required=True,
                           help="Clave del formato controlado (ej. F-P-A28-04).")
    sgi_code_alt = fields.Char(
        string="Clave alternativa",
        help="Clave que aplica cuando el registro está confirmado (solo ventas: "
             "cotización vs pedido). Vacío = siempre la clave principal.")
    active = fields.Boolean(default=True)
    note = fields.Char(string="Nota")

    _model_uniq = models.Constraint(
        'unique(model_id)',
        "Ya existe un mapeo de formato para este modelo.",
    )

    @api.constrains('sgi_code', 'sgi_code_alt')
    def _check_codes(self):
        for fmap in self:
            for code in filter(None, (fmap.sgi_code, fmap.sgi_code_alt)):
                if not SGI_CODE_REGEX.match(code.strip()):
                    raise ValidationError(
                        "La clave '%s' no cumple la nomenclatura del SGI "
                        "(ej. F-P-A28-04, F-IT-P-P01-08-01)." % code)

    @api.model
    def _get_for_model(self, model_name):
        return self.search([('model_name', '=', model_name)], limit=1)

    @api.model
    def _revision_of(self, code):
        """Revisión del documento VIGENTE con esa clave, o False si no existe."""
        doc = self.env['documents.document'].sudo().search([
            ('sgi_code', '=', code),
            ('sgi_state', '=', 'vigente'),
        ], limit=1)
        return doc.sgi_revision or False


class SgiConfig(models.AbstractModel):
    """Utilidades de configuración del SGI (siembra idempotente)."""
    _name = 'sgi.config'
    _description = "Configuración SGI"

    # Parámetros operativos: default de arranque. Solo se crean si NO existen;
    # lo editado en Ajustes > Técnico > Parámetros del sistema nunca se pisa.
    _SGI_DEFAULT_PARAMS = {
        'quimibond_sgi.nc_escalation_days': '5',
        'quimibond_sgi.fmea_npr_action': '100',
        'quimibond_sgi.risk_ryo_inmediata': '16',
        'quimibond_sgi.risk_ryo_media': '9',
        'quimibond_sgi.risk_ryo_intermedia': '4',
        'quimibond_sgi.supplier_weight_otd': '0.7',
        'quimibond_sgi.supplier_weight_quality': '0.3',
        'quimibond_sgi.supplier_nc_penalty': '10.0',
        'quimibond_sgi.pesaje_tolerance_kg': '3.0',
        'quimibond_sgi.waste_subproduct_category': 'SubProducto',
        'quimibond_sgi.monthly_sales_budget': '0',
        'quimibond_sgi.rh_user_id': '0',
    }

    @api.model
    def seed_parameters(self):
        Param = self.env['ir.config_parameter'].sudo()
        for key, value in self._SGI_DEFAULT_PARAMS.items():
            if Param.get_param(key) is False:
                Param.set_param(key, value)
        return True


class SgiFormatMixin(models.AbstractModel):
    """Agrega al modelo la clave del formato SGI que sustituye (pantalla y PDF)."""
    _name = 'sgi.format.mixin'
    _description = "Mixin: clave de formato SGI"

    sgi_format_banner = fields.Char(
        string="Formato SGI", compute='_compute_sgi_format_banner')

    def _sgi_format_applies(self):
        """Si este registro en particular porta la clave (hook por modelo)."""
        self.ensure_one()
        return True

    def _sgi_format_code(self, fmap):
        """Clave a usar para este registro (hook por modelo)."""
        self.ensure_one()
        return fmap.sgi_code

    def sgi_format_info(self):
        """'F-P-A28-04 · Rev. 03' | 'F-P-A28-04' (sin doc vigente) | False."""
        self.ensure_one()
        fmap = self.env['sgi.format.map'].sudo()._get_for_model(self._name)
        if not fmap or not self._sgi_format_applies():
            return False
        code = self._sgi_format_code(fmap)
        revision = self.env['sgi.format.map'].sudo()._revision_of(code)
        return "%s · Rev. %s" % (code, revision) if revision else code

    def _compute_sgi_format_banner(self):
        for record in self:
            record.sgi_format_banner = record.sgi_format_info()


# --- Aplicación del mixin a los modelos mapeados -----------------------------

class SaleOrder(models.Model):
    _name = 'sale.order'
    _inherit = ['sale.order', 'sgi.format.mixin']

    def _sgi_format_code(self, fmap):
        self.ensure_one()
        if fmap.sgi_code_alt and self.state == 'sale':
            return fmap.sgi_code_alt
        return fmap.sgi_code


class PurchaseOrder(models.Model):
    _name = 'purchase.order'
    _inherit = ['purchase.order', 'sgi.format.mixin']


class StockPicking(models.Model):
    _name = 'stock.picking'
    _inherit = ['stock.picking', 'sgi.format.mixin']

    def _sgi_format_applies(self):
        self.ensure_one()
        return self.picking_type_code == 'outgoing'


class MrpProduction(models.Model):
    _name = 'mrp.production'
    _inherit = ['mrp.production', 'sgi.format.mixin']


class MaintenanceRequest(models.Model):
    _name = 'maintenance.request'
    _inherit = ['maintenance.request', 'sgi.format.mixin']


class QualityAlert(models.Model):
    _name = 'quality.alert'
    _inherit = ['quality.alert', 'sgi.format.mixin']


class StockLot(models.Model):
    _name = 'stock.lot'
    _inherit = ['stock.lot', 'sgi.format.mixin']


class SgiManagementReview(models.Model):
    _name = 'sgi.management.review'
    _inherit = ['sgi.management.review', 'sgi.format.mixin']
