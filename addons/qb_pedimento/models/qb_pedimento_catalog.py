# -*- coding: utf-8 -*-
"""Catálogos del pedimento: claves (Anexo 22, apéndice 2), aduanas (apéndice 1)
y tipos de documento del expediente (columnas de checklist de F-P-A-16-05 y
F-P-A-16-06). Se siembran solo los que aparecen en la operación de Quimibond;
el resto se da de alta a mano."""
import re

from odoo import api, fields, models
from odoo.exceptions import ValidationError

OPERACIONES = [('importacion', 'Importación'), ('exportacion', 'Exportación')]


# Patrones por tipo sembrado (xmlid → regex sobre el nombre del archivo). Salen
# de los nombres con que las agencias y Logística mandan los archivos en el
# correo (memoria, 2025-10 a 2026-10): «17404306000661NORMAL_PAGADO.PDF»,
# «…SIMPDEC_PAGADO.PDF», «26-07-1612-6000590.pdf» y «5001894 A1.PDF» (pedimento),
# «Doda5001894.PDF», «JJ28104.PDF/XML» (cuenta de gastos), «INVOICE AND PL
# BIANCO.pdf» (factura y packing en uno), «CEO TMEC 2026 SHAWMUT.pdf»,
# «C-INV…-MX-Invoice-4.0.pdf» (CFDI de Quimibond), «TDS …pdf» (ficha técnica),
# «UPS GUIA …pdf».
PATRONES_ARCHIVO = {
    'doc_pedimento': r'NORMAL_PAGADO|SIMPDEC|PEDIMENTO|(?<!\d)\d{2}[- ]\d{2}[- ]\d{4}[- ]\d{7}(?!\d)|\d{7} ?[AVR][1-9]\b',
    'doc_cuenta_gastos': r'CUENTA.?DE.?GASTOS|GASTOS|^(JJ|I|DD|CC|SIN-?)\s?\d{3,}',
    'doc_cove': r'COVE|VUCEM',
    'doc_factura_proveedor': r'INVOICE|FACTURA|COMMERCIAL',
    'doc_bl': r'\bB/?L\b|BILL.?OF.?LADING|CONOCIMIENTO|GU[IÍ]A|AWB|WAYBILL|TELEX',
    'doc_packing_imp': r'PACKING|\bP\.?L\b|LISTA.?DE.?EMPAQUE',
    'doc_carta_encomienda': r'ENCOMIENDA|ENCARGO',
    'doc_carta_318': r'3\.?1\.?8',
    'doc_mv': r'MANIFESTACI[OÓ]N|\bMVE?\b|HOJA.?DE.?C[AÁ]LCULO|HOJACALCULO',
    'doc_especificaciones': r'FICHA|\bTDS\b|ESPECIFICACI|DATA.?SHEET|CARTA.?T[EÉ]CNICA',
    'doc_declaracion_origen': r'DECLARACI[OÓ]N.?DE.?ORIGEN',
    'doc_certificado_origen_imp': r'CERTIFICADO|\bCEO\b|T-?MEC|USMCA',
    'doc_doda_imp': r'DODA',
    'doc_pago_derechos': r'PAGO.?DE.?DERECHOS|COMPROBANTE.?DE.?PAGO|\bPECA\b',
    'doc_factura_cfdi': r'C-INV|INVOICE-4\.0|\bINV[/_ -]?\d{4}|CFDI|INVOICE|FACTURA',
    'doc_packing_exp': r'PACKING|\bP\.?L\b|LISTA.?DE.?EMPAQUE',
    'doc_certificado_origen_exp': r'CERTIFICADO|\bCEO\b|T-?MEC|USMCA',
    'doc_carta_porte': r'CARTA.?PORTE|\bCCP\b',
    'doc_doda_exp': r'DODA',
    'doc_bl_exp': r'\bB/?L\b|BILL.?OF.?LADING|GU[IÍ]A|AWB|WAYBILL',
    'doc_entry': r'ENTRY|\b7501\b',
    'doc_edocuments': r'ACUSE|DIGITALIZACI|E-?DOCUMENT',
    'doc_csf': r'CONSTANCIA|SITUACI[OÓ]N.?FISCAL|\bCSF\b',
    'doc_proforma_v1': r'PROFORMA',
    'doc_materialidad': r'MATERIALIDAD|CONTRATO|EVIDENCIA',
}


class QbPedimentoClave(models.Model):
    _name = 'qb.pedimento.clave'
    _description = 'Clave de pedimento'
    _order = 'code'
    _rec_names_search = ['code', 'name']

    code = fields.Char('Clave', required=True, size=2)
    name = fields.Char('Descripción', required=True)
    operacion = fields.Selection(OPERACIONES + [('ambas', 'Ambas')], default='ambas', required=True)
    virtual = fields.Boolean(
        'Virtual', help='Operación virtual (V1, V5): la mercancía no cruza la frontera; la factura es a un '
                        'cliente nacional con programa IMMEX.')
    rectificacion = fields.Boolean(
        'Rectificación', help='Corrige un pedimento anterior (R1). Exige el pedimento original.')
    active = fields.Boolean(default=True)

    _code_uniq = models.Constraint('unique(code)', 'Esa clave de pedimento ya existe.')

    @api.depends('code', 'name')
    def _compute_display_name(self):
        for rec in self:
            rec.display_name = '%s - %s' % (rec.code, rec.name) if rec.code else rec.name


class QbPedimentoAduana(models.Model):
    _name = 'qb.pedimento.aduana'
    _description = 'Aduana'
    _order = 'code'
    _rec_names_search = ['code', 'name']

    code = fields.Char('Código', required=True, size=2, help='Dos dígitos del Anexo 22, apéndice 1.')
    name = fields.Char('Aduana', required=True)
    active = fields.Boolean(default=True)

    _code_uniq = models.Constraint('unique(code)', 'Esa aduana ya existe.')

    @api.depends('code', 'name')
    def _compute_display_name(self):
        for rec in self:
            rec.display_name = '%s %s' % (rec.code, rec.name) if rec.code else rec.name

    @api.model
    def _get_or_create(self, code):
        """La aduana del número de pedimento; si el código no está en el
        catálogo se da de alta con el nombre por llenar (no se inventa)."""
        aduana = self.with_context(active_test=False).search([('code', '=', code)], limit=1)
        return aduana or self.sudo().create({'code': code, 'name': 'Aduana %s (por nombrar)' % code})


class QbPedimentoDocumentoTipo(models.Model):
    _name = 'qb.pedimento.documento.tipo'
    _description = 'Tipo de documento del expediente'
    _order = 'sequence, id'

    name = fields.Char('Documento', required=True)
    sequence = fields.Integer(default=10)
    operacion = fields.Selection(OPERACIONES + [('ambas', 'Ambas')], default='ambas', required=True)
    required = fields.Boolean(
        'Obligatorio para cerrar', default=True,
        help='El expediente no se cierra mientras este documento no tenga archivo o esté marcado «No aplica».')
    solo_virtual = fields.Boolean(
        'Solo virtuales', help='Se pide solo en pedimentos con clave virtual (V1, V5).')
    excepto_virtual = fields.Boolean(
        'Excepto virtuales', help='No se pide en pedimentos virtuales (la mercancía no cruza).')
    patron_archivo = fields.Char(
        'Patrón del nombre de archivo',
        help='Expresión regular (sin distinguir mayúsculas) que reconoce el documento por el nombre del archivo '
             'al cargar el expediente completo. Ej.: NORMAL_PAGADO|SIMPDEC para el pedimento pagado.')
    active = fields.Boolean(default=True)

    @api.constrains('patron_archivo')
    def _check_patron_archivo(self):
        for rec in self.filtered('patron_archivo'):
            try:
                re.compile(rec.patron_archivo)
            except re.error as exc:
                raise ValidationError('El patrón de «%s» no es una expresión regular válida: %s' % (rec.name, exc))

    def _reconoce(self, nombre_archivo):
        """True si el nombre del archivo coincide con el patrón del tipo."""
        self.ensure_one()
        return bool(self.patron_archivo and re.search(self.patron_archivo, nombre_archivo or '', re.I))

    @api.model
    def _qb_sembrar_patrones(self):
        """Pone el patrón de los tipos sembrados por el módulo que aún no lo
        tienen. Corre en cada instalación o actualización (los tipos son
        noupdate) y no pisa lo que el usuario haya editado."""
        for xmlid, patron in PATRONES_ARCHIVO.items():
            tipo = self.env.ref('qb_pedimento.%s' % xmlid, raise_if_not_found=False)
            if tipo and not tipo.patron_archivo:
                tipo.patron_archivo = patron

    def _applies_to(self, operacion, virtual):
        return self.filtered(lambda t: t.operacion in (operacion, 'ambas')
                             and not (t.solo_virtual and not virtual)
                             and not (t.excepto_virtual and virtual))
