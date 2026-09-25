# -*- coding: utf-8 -*-
"""NC-6: el proveedor ve la NC y contesta causa y acción en el portal.

Acceso con el token del portal (enlace del correo) o como usuario portal
del proveedor; sin token válido, 403. Solo lectura de lo que el proveedor
necesita (folio, producto, lote, desviación, plazo) y un formulario de dos
campos que escribe por `quality.alert.sgi_supplier_answer`.
"""
from urllib.parse import quote

from odoo import http
from odoo.exceptions import AccessError, MissingError, UserError
from odoo.http import request

from odoo.addons.portal.controllers.portal import CustomerPortal


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
            'error': kw.get('error'), 'page_name': 'sgi_nc',
        })

    @http.route(['/my/nc/<int:alert_id>/answer'], type='http', auth='public', methods=['POST'],
                website=False, csrf=True)
    def portal_nc_answer(self, alert_id, access_token=None, cause=None, action=None, **kw):
        alert = self._sgi_nc(alert_id, access_token)
        if alert is None or not alert.sgi_folio:
            return request.redirect('/my')
        base = '/my/nc/%d?access_token=%s' % (alert.id, access_token or '')
        try:
            alert.sudo().sgi_supplier_answer(cause, action)
        except UserError as exc:
            return request.redirect(base + '&error=%s' % quote(str(exc)))
        return request.redirect(base + '&saved=1')
