"""Páginas públicas para proveedores: requisitos y alta en línea.

`/proveedores` explica cómo trabajar con Quimibond y `/proveedores/alta`
recibe la solicitud con sus documentos. La solicitud queda en Compras
(`qb.proveedor.alta`); nada se publica ni crea un contacto hasta que alguien
la aprueba.
"""
import base64
import re
from datetime import timedelta

from odoo import fields, http
from odoo.http import request

from odoo.addons.website_quimibond.controllers.main import _exigir_activo

from ..models.cfdi import RE_RFC, clabe_valida, limpiar_rfc
from ..models.proveedor_alta import GIROS

MAX_ARCHIVO = 5 * 1024 * 1024
MAX_SOLICITUD = 24 * 1024 * 1024
MAX_POR_HORA = 30
FIRMAS = (b'%PDF-', b'\xff\xd8\xff', b'\x89PNG')
RE_CORREO = re.compile(r'^[^@\s]+@[^@\s]+\.[^@\s]+$')
DOCUMENTOS = [
    ('constancia', 'Constancia de situación fiscal', True),
    ('opinion', 'Opinión de cumplimiento del SAT (positiva)', True),
    ('caratula', 'Carátula del estado de cuenta bancario', True),
    ('certificados', 'Certificados de calidad o de producto', False),
]
REQUISITOS = [
    {'titulo': 'Constancia de situación fiscal', 'detalle': 'Con fecha de emisión de los últimos tres meses.'},
    {'titulo': 'Opinión de cumplimiento', 'detalle': 'Positiva y vigente, emitida por el SAT.'},
    {'titulo': 'Carátula bancaria', 'detalle': 'Del estado de cuenta, a nombre de la razón social, con la CLABE.'},
    {'titulo': 'Certificados', 'detalle': 'De calidad, ambientales o de producto, si los tiene.'},
]
COMPROMISOS = [
    'Cumplir con sus obligaciones fiscales, laborales y de seguridad social.',
    'Entregar lo que se pide en la orden de compra, en la fecha y con la calidad acordadas.',
    'Facturar solo contra una orden de compra y lo efectivamente recibido.',
    'Cuidar la seguridad de las personas y el medio ambiente dentro de nuestra planta.',
    'Tratar como confidencial la información que reciba de Quimibond.',
    'No ofrecer regalos ni pagos al personal de Quimibond.',
]
CAMPOS = ['name', 'rfc', 'giro', 'descripcion', 'contacto', 'puesto', 'email', 'telefono', 'sitio_web', 'ciudad',
          'banco', 'clabe', 'titular', 'moneda_cuenta']


class QuimibondProveedoresAlta(http.Controller):

    def _valores(self, **extra):
        valores = {
            'giros': GIROS,
            'documentos': DOCUMENTOS,
            'requisitos': REQUISITOS,
            'compromisos': COMPROMISOS,
            'datos': {},
            'errores': {},
        }
        valores.update(extra)
        return valores

    @staticmethod
    def _sitemap(env, rule, qs):
        website = env['website'].get_current_website()
        if website.qb_diseno_b2b and (not qs or qs.lower() in '/proveedores'):
            yield {'loc': '/proveedores'}

    @http.route('/proveedores', type='http', auth='public', website=True, sitemap=_sitemap)
    def proveedores(self, **kw):
        _exigir_activo()
        return request.render('website_quimibond_proveedores.proveedores', self._valores(
            title='Proveedores | Quimibond',
            website_meta_description='Cómo ser proveedor de Quimibond: requisitos, alta en línea y portal para '
                                     'consultar órdenes de compra, subir facturas y ver fechas de pago.',
        ))

    @http.route('/proveedores/alta', type='http', auth='public', website=True, methods=['GET'], sitemap=False)
    def alta(self, **kw):
        _exigir_activo()
        return request.render('website_quimibond_proveedores.proveedores_alta', self._valores(
            title='Alta de proveedor | Quimibond'))

    def _validar(self, post, archivos):
        """(valores, errores) de la solicitud."""
        datos = {campo: (post.get(campo) or '').strip()[:500] for campo in CAMPOS}
        errores = {}
        for campo, etiqueta in (('name', 'la razón social'), ('contacto', 'el nombre del contacto')):
            if not datos[campo]:
                errores[campo] = 'Escriba %s.' % etiqueta
        datos['rfc'] = limpiar_rfc(datos['rfc'])
        if not RE_RFC.match(datos['rfc']):
            errores['rfc'] = 'El RFC no tiene el formato correcto (12 o 13 caracteres).'
        if not RE_CORREO.match(datos['email']):
            errores['email'] = 'Escriba un correo válido.'
        if datos['giro'] not in dict(GIROS):
            errores['giro'] = 'Elija qué ofrece.'
        datos['clabe'] = re.sub(r'\s', '', datos['clabe'])
        if datos['clabe'] and not clabe_valida(datos['clabe']):
            errores['clabe'] = 'La CLABE debe tener 18 dígitos. Revise que esté bien escrita.'
        if datos['moneda_cuenta'] not in ('MXN', 'USD'):
            datos['moneda_cuenta'] = 'MXN'
        if not post.get('acepta'):
            errores['acepta'] = 'Marque la casilla para aceptar los compromisos del proveedor.'
        valores = dict(datos)
        for campo, etiqueta, requerido in DOCUMENTOS:
            archivo = archivos.get(campo)
            if not archivo or not archivo.filename:
                if requerido:
                    errores[campo] = 'Falta este documento.'
                continue
            contenido = archivo.read(MAX_ARCHIVO + 1)
            if len(contenido) > MAX_ARCHIVO:
                errores[campo] = 'El archivo pesa más de 5 MB.'
            elif not contenido.startswith(FIRMAS):
                errores[campo] = 'Suba el documento en PDF, JPG o PNG.'
            else:
                valores[campo] = base64.b64encode(contenido)
                valores[campo + '_nombre'] = archivo.filename[:120]
        return datos, valores, errores

    @http.route('/proveedores/alta', type='http', auth='public', website=True, methods=['POST'], sitemap=False,
                max_content_length=MAX_SOLICITUD)
    def alta_enviar(self, **post):
        _exigir_activo()
        # Campo trampa: una persona no lo ve; un robot lo llena.
        if post.get('empresa_web'):
            return request.redirect('/proveedores/alta/gracias')
        datos, valores, errores = self._validar(post, request.httprequest.files)
        Alta = request.env['qb.proveedor.alta'].sudo()
        if not errores and Alta.search_count(
                [('create_date', '>=', fields.Datetime.now() - timedelta(hours=1))]) >= MAX_POR_HORA:
            errores['general'] = 'No pudimos recibir su solicitud en este momento. Intente de nuevo más tarde.'
        if errores:
            errores.setdefault('general', 'Revise los campos marcados. Los documentos hay que elegirlos de nuevo.')
            return request.render('website_quimibond_proveedores.proveedores_alta', self._valores(
                datos=datos, errores=errores, title='Alta de proveedor | Quimibond'))
        # Una solicitud abierta por RFC: la segunda no se duplica (y no se dice si ya existía).
        if not Alta.search_count([('rfc', '=', valores['rfc']), ('state', 'in', ('nueva', 'revision'))]):
            valores.update({
                'company_id': request.website.company_id.id,
                'acepta_codigo': True,
                'acepta_fecha': fields.Datetime.now(),
                'acepta_ip': request.httprequest.remote_addr,
            })
            Alta.create(valores)._avisar_compras()
        return request.redirect('/proveedores/alta/gracias')

    @http.route('/proveedores/alta/gracias', type='http', auth='public', website=True, sitemap=False)
    def alta_gracias(self, **kw):
        _exigir_activo()
        return request.render('website_quimibond_proveedores.proveedores_gracias', self._valores(
            title='Solicitud recibida | Quimibond'))
