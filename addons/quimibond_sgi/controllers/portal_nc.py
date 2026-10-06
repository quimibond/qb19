# -*- coding: utf-8 -*-
"""NC-6: el proveedor ve la NC y contesta causa y acción en el portal.

Acceso con el token del portal (enlace del correo) o como usuario portal
del proveedor; sin token válido, 403. Solo lectura de lo que el proveedor
necesita (folio, producto, lote, desviación, plazo) y un formulario de dos
campos que escribe por `quality.alert._sgi_supplier_answer`.

57.93.0 (K-07): la URL lleva un código de error, no el texto: un enlace
armado no pone texto en una página de Quimibond. Un código desconocido no
muestra nada.
"""
from odoo import http
from odoo.exceptions import AccessError, MissingError, UserError
from odoo.http import request

from odoo.addons.portal.controllers.portal import CustomerPortal

SGI_PORTAL_ERRORS = {
    'estado': "Esta NC ya no espera su respuesta. Si necesita corregirla, escriba a quien se la envió.",
    'faltan': "La causa y la acción son obligatorias.",
    'otro': "No se pudo registrar su respuesta. Escriba a quien le envió la NC.",
}


class SgiSupplierNcPortal(CustomerPortal):

    def _sgi_nc(self, alert_id, access_token):
        try:
            return self._document_check_access('quality.alert', alert_id, access_token=access_token)
        except (AccessError, MissingError):
            return None

    @http.route(['/my/nc/<int:alert_id>'], type='http', auth='public', website=False, sitemap=False)
    def portal_nc(self, alert_id, access_token=None, **kw):
        alert = self._sgi_nc(alert_id, access_token)
        if alert is None or not alert.sgi_folio:
            return request.redirect('/my')
        return request.render('quimibond_sgi.portal_nc_page', {
            'nc': alert.sudo(), 'access_token': access_token or '', 'saved': kw.get('saved'),
            'error': SGI_PORTAL_ERRORS.get(kw.get('error') or ''), 'page_name': 'sgi_nc',
        })

    @http.route(['/my/nc/<int:alert_id>/answer'], type='http', auth='public', methods=['POST'],
                website=False, csrf=True)
    def portal_nc_answer(self, alert_id, access_token=None, cause=None, action=None, **kw):
        alert = self._sgi_nc(alert_id, access_token)
        if alert is None or not alert.sgi_folio:
            return request.redirect('/my')
        base = '/my/nc/%d?access_token=%s' % (alert.id, access_token or '')
        if alert.sudo().sgi_supplier_state != 'enviada':
            return request.redirect(base + '&error=estado')
        if not (cause or '').strip() or not (action or '').strip():
            return request.redirect(base + '&error=faltan')
        try:
            alert.sudo()._sgi_supplier_answer(cause, action)
        except UserError:
            return request.redirect(base + '&error=otro')
        return request.redirect(base + '&saved=1')
