import re

from werkzeug.exceptions import NotFound

from odoo import http
from odoo.http import request
from odoo.tools import html2plaintext

from odoo.addons.website.controllers.main import Website

from . import contenido, familias, idioma, planta


# ---------------------------------------------------------------------------
# Utilidades
# ---------------------------------------------------------------------------

def _activo(website=None):
    website = website or request.website
    return bool(website and website.qb_diseno_b2b)


def _exigir_activo():
    if not _activo():
        raise NotFound()


RE_GRAMAJE = re.compile(r'(\d+(?:[.,]\d+)?)\s*(?:gr|g)\b', re.I)
RE_COMPOSICION = re.compile(r'((?:PES|PA|CO|PP|PET|VI|NY)[\s/]*\d{1,3}\s*%(?:\s*[/,]?\s*(?:PES|PA|CO|PP|PET|VI|NY)\s*\d{1,3}\s*%)*)', re.I)
COLORES = ['blanco', 'negro', 'gris', 'beige', 'marfil', 'natural', 'azul', 'crudo', 'hueso']
TACTOS = [('semi - rigido', 'Semirrígido'), ('semi-rigido', 'Semirrígido'), ('semirrigido', 'Semirrígido'),
          ('rigido', 'Rígido'), ('rígido', 'Rígido'), ('suave', 'Suave')]


def ficha_producto(product):
    """Datos técnicos que se pueden leer del nombre y la descripción del producto.

    Los nombres en Odoo vienen como «A40GO152: Entretela no tejida "No
    Fusionable" 40 gr PES 100% Gris semi - rigido». No inventa nada: lo que no
    se encuentra queda vacío y no se muestra.
    """
    nombre = product.name or ''
    codigo = product.default_code or ''
    titulo = nombre
    if ':' in nombre:
        cabeza, cola = nombre.split(':', 1)
        if not codigo:
            codigo = cabeza.strip()
        titulo = cola.strip()
    descripcion = html2plaintext(product.website_description or '').strip() if product.website_description else ''
    texto = ' '.join([titulo, descripcion])
    bajo = texto.lower()

    gramaje = RE_GRAMAJE.search(texto)
    composicion = RE_COMPOSICION.search(texto)
    color = next((c.capitalize() for c in COLORES if re.search(r'\b%s\b' % c, bajo)), '')
    tacto = next((etq for clave, etq in TACTOS if clave in bajo), '')
    if 'no fusionable' in bajo:
        fusion = 'No fusionable'
    elif 'fusionable' in bajo:
        fusion = 'Fusionable'
    else:
        fusion = ''
    if 'circular' in bajo or 'tricot' in bajo or 'tejido circular' in bajo:
        construccion = 'tejido'
    else:
        construccion = 'notejido'

    titulo_limpio = re.sub(r'\s+', ' ', titulo.replace('"', '')).strip()
    return {
        'codigo': codigo,
        'titulo': titulo_limpio[:1].upper() + titulo_limpio[1:] if titulo_limpio else codigo,
        'gramaje': ('%s g/m²' % gramaje.group(1).replace(',', '.')) if gramaje else '',
        'gramaje_num': float(gramaje.group(1).replace(',', '.')) if gramaje else 0,
        'composicion': re.sub(r'\s+', ' ', composicion.group(1)).upper() if composicion else '',
        'color': color,
        'tacto': tacto,
        'fusion': fusion,
        'construccion': construccion,
        'imagen': '/web/image/product.template/%s/image_512' % product.id if product.image_128 else '',
    }


class QuimibondSitio(http.Controller):
    """Rutas del sitio B2B. Todas responden 404 si el sitio no tiene la casilla."""

    # -- consultas -----------------------------------------------------------

    def _dominio_productos(self):
        website = request.website
        return [
            ('is_published', '=', True),
            ('sale_ok', '=', True),
            ('company_id', 'in', [False, website.company_id.id]),
        ] + website.website_domain()

    def _categorias_raiz(self):
        Cat = request.env['product.public.category'].sudo()
        return Cat.search([('parent_id', '=', False)] + request.website.website_domain(), order='sequence, name')

    def _productos(self, categoria=None, limit=None):
        dominio = self._dominio_productos()
        if categoria:
            dominio.append(('public_categ_ids', 'child_of', categoria.id))
        else:
            dominio.append(('public_categ_ids', '!=', False))
        return request.env['product.template'].sudo().search(dominio, order='website_sequence, default_code, name', limit=limit)

    def _resumen_categorias(self):
        """Categorías raíz con sus subcategorías y número de productos publicados."""
        resumen = []
        for cat in self._categorias_raiz():
            productos = self._productos(cat)
            if not productos:
                continue
            hijos = []
            for hijo in cat.child_id.sorted(lambda c: (c.sequence, c.name)):
                n = len(self._productos(hijo))
                if n:
                    hijos.append({'categoria': hijo, 'total': n})
            bajo = (cat.name or '').lower()
            construccion = 'tejido' if ('circular' in bajo or 'tejido' in bajo and 'no-tejido' not in bajo and 'no tejido' not in bajo) else 'notejido'
            resumen.append({
                'categoria': cat,
                'total': len(productos),
                'hijos': hijos,
                'fusionable': 'fusionable' in bajo and 'no-fusionable' not in bajo and 'no fusionable' not in bajo,
                'construccion': construccion,
            })
        return resumen

    def _vacantes(self):
        if 'hr.job' not in request.env:
            return request.env['ir.ui.view'].browse()
        Job = request.env['hr.job'].sudo()
        dominio = [('website_published', '=', True)] + request.website.website_domain()
        return Job.search(dominio, limit=6)

    def _valores_comunes(self, **extra):
        maquinaria = planta.maquinaria(request.env, request.website.company_id)
        valores = {
            'qb_mercados': contenido.mercados(),
            'qb_cifras': contenido.CIFRAS,
            'qb_proceso': contenido.PROCESO,
            'qb_historia': contenido.HISTORIA,
            'qb_sustentabilidad': contenido.SUSTENTABILIDAD,
            'qb_sustentabilidad_cifras': contenido.SUSTENTABILIDAD_CIFRAS,
            'qb_tejidos': contenido.TEJIDOS,
            'qb_familias': contenido.familias(),
            'qb_presentaciones': contenido.PRESENTACIONES,
            'qb_maquinaria': maquinaria,
            'qb_disenador': planta.disenador(request.env),
        }
        valores.update(extra)
        # Versión en inglés: los textos de contenido.py y los que pasa cada ruta
        # (título, descripción) se traducen aquí; las plantillas las traduce Odoo.
        lang = self._idioma()
        valores = idioma.traducir(valores, lang)
        valores['qb_t'] = lambda texto: idioma.traducir(texto, lang)
        return valores

    @staticmethod
    def _idioma():
        return request.env.context.get('lang') or request.env.lang

    def _articulos(self):
        """{clave de familia: [(producto, ficha)]} con los productos publicados de las
        familias de entretelas, clasificados por su nombre."""
        productos = request.env['product.template'].sudo().search(
            self._dominio_productos(), order='default_code, name')
        por_familia = {}
        for producto in productos:
            fam = familias.familia_de(producto.name)
            if fam:
                por_familia.setdefault(fam['clave'], []).append((producto, ficha_producto(producto)))
        return por_familia

    # -- sitemap -------------------------------------------------------------

    @staticmethod
    def _sitemap_fijo(rutas):
        def sitemap(env, rule, qs):
            website = env['website'].get_current_website()
            if not _activo(website):
                return
            for ruta in rutas:
                if not qs or qs.lower() in ruta:
                    yield {'loc': ruta}
        return sitemap

    def sitemap_mercados(env, rule, qs):
        website = env['website'].get_current_website()
        if not _activo(website):
            return
        for m in contenido.MERCADOS:
            loc = '/mercados/%s' % m['clave']
            if not qs or qs.lower() in loc:
                yield {'loc': loc}

    def sitemap_catalogo(env, rule, qs):
        website = env['website'].get_current_website()
        if not _activo(website):
            return
        yield {'loc': '/catalogo'}
        for fam in familias.FAMILIAS:
            yield {'loc': '/catalogo/familia/%s' % fam['clave']}

    def sitemap_productos(env, rule, qs):
        website = env['website'].get_current_website()
        if not _activo(website):
            return
        slug = env['ir.http']._slug
        productos = env['product.template'].sudo().search([
            ('is_published', '=', True), ('sale_ok', '=', True),
        ] + website.website_domain())
        for p in productos:
            if familias.familia_de(p.name):
                yield {'loc': '/catalogo/producto/%s' % slug(p)}

    # -- páginas -------------------------------------------------------------

    @http.route('/catalogo', type='http', auth='public', website=True, sitemap=sitemap_catalogo)
    def catalogo(self, **kw):
        _exigir_activo()
        articulos = self._articulos()
        return request.render('website_quimibond.catalogo', self._valores_comunes(
            grupos=familias.grupos(),
            totales={clave: len(lista) for clave, lista in articulos.items()},
            title='Catálogo por familias | Quimibond',
            website_meta_description='Scrims automotrices, entretelas fusionables, no tejidos y telas de punto. '
                                     'Rangos de peso, ancho y color por familia. Solicite la ficha técnica.',
        ))

    @http.route('/catalogo/familia/<string:clave>', type='http', auth='public', website=True, sitemap=False)
    def catalogo_familia(self, clave, **kw):
        _exigir_activo()
        fam = familias.familia(clave)
        if not fam:
            raise NotFound()
        fam_t = idioma.traducir(fam, self._idioma())
        return request.render('website_quimibond.familia', self._valores_comunes(
            fam=fam,
            fam_grupo=next(g for g in familias.GRUPOS if g['clave'] == fam['grupo']),
            articulos=self._articulos().get(clave, []),
            otras=[f for f in familias.FAMILIAS if f['grupo'] == fam['grupo'] and f['clave'] != clave],
            asunto_ficha='Ficha técnica: %s' % fam_t['nombre'],
            title='%s | Quimibond' % fam_t['nombre'],
            website_meta_description=fam['frase'],
        ))

    @http.route('/catalogo/<model("product.public.category"):categoria>', type='http', auth='public',
                website=True, sitemap=False)
    def catalogo_categoria(self, categoria, **kw):
        # Las categorías del sitio ya no organizan el catálogo: va por familias.
        _exigir_activo()
        return request.redirect('/catalogo', code=301)

    @http.route('/catalogo/producto/<model("product.template"):producto>', type='http', auth='public',
                website=True, sitemap=sitemap_productos)
    def catalogo_producto(self, producto, **kw):
        _exigir_activo()
        producto = producto.sudo()
        if not producto.exists() or not producto.filtered_domain(self._dominio_productos()):
            raise NotFound()
        fam = familias.familia_de(producto.name)
        if not fam:
            raise NotFound()
        ficha = ficha_producto(producto)
        relacionados = [par for par in self._articulos().get(fam['clave'], []) if par[0] != producto][:6]
        return request.render('website_quimibond.producto', self._valores_comunes(
            producto=producto,
            ficha=ficha,
            fam=fam,
            relacionados=relacionados,
            main_object=producto,
        ))

    @http.route('/catalogo/buscar.json', type='http', auth='public', website=True, sitemap=False, readonly=True)
    def catalogo_buscar(self, **kw):
        _exigir_activo()
        slug = request.env['ir.http']._slug
        lang = self._idioma()
        datos = []
        for clave, lista in self._articulos().items():
            fam = idioma.traducir(familias.familia(clave), lang)
            for p, f in lista:
                datos.append({
                    'codigo': f['codigo'],
                    'titulo': f['titulo'],
                    'detalle': ' · '.join(x for x in [f['gramaje'], f['composicion'], f['color'], f['tacto']] if x),
                    'categoria': fam['nombre'],
                    'url': '/catalogo/producto/%s' % slug(p),
                })
        paginas = [
            {'codigo': '', 'titulo': 'Solicitar cotización o muestra', 'detalle': 'Ventas', 'categoria': 'Páginas', 'url': '/cotizar'},
            {'codigo': '', 'titulo': 'Diseñe su tela', 'detalle': 'Diseñador interactivo', 'categoria': 'Páginas', 'url': '/disenar'},
            {'codigo': '', 'titulo': 'Planta y proceso', 'detalle': 'Capacidades', 'categoria': 'Páginas', 'url': '/planta'},
            {'codigo': '', 'titulo': 'Calidad y sustentabilidad', 'detalle': 'ISO 9001 · ISO 14001 · ISO 45001', 'categoria': 'Páginas', 'url': '/calidad'},
            {'codigo': '', 'titulo': 'Nosotros', 'detalle': 'Desde 1992', 'categoria': 'Páginas', 'url': '/nosotros'},
            {'codigo': '', 'titulo': 'Empleos', 'detalle': 'Trabaje con nosotros', 'categoria': 'Páginas', 'url': '/jobs'},
            {'codigo': '', 'titulo': 'Portal de clientes', 'detalle': 'Pedidos, facturas y entregas', 'categoria': 'Páginas', 'url': '/my'},
            {'codigo': '', 'titulo': 'Compradores automotrices', 'detalle': 'Certificados, laboratorio y desarrollo', 'categoria': 'Páginas', 'url': '/compradores-automotrices'},
            {'codigo': '', 'titulo': 'Agendar una llamada', 'detalle': 'Ventas', 'categoria': 'Páginas', 'url': '/agendar'},
        ]
        paginas += [
            {'codigo': '', 'titulo': f['nombre'], 'detalle': f['frase'], 'categoria': 'Catálogo', 'url': '/catalogo/familia/%s' % f['clave']}
            for f in familias.FAMILIAS
        ]
        paginas += [
            {'codigo': '', 'titulo': m['nombre'], 'detalle': m['titulo'], 'categoria': 'Mercados', 'url': '/mercados/%s' % m['clave']}
            for m in contenido.MERCADOS
        ]
        return request.make_json_response(idioma.traducir(paginas, lang) + datos, headers={'Cache-Control': 'public, max-age=600'})

    @http.route('/mercados/<string:clave>', type='http', auth='public', website=True, sitemap=sitemap_mercados)
    def mercado(self, clave, **kw):
        _exigir_activo()
        m = contenido.mercado(clave)
        if not m:
            raise NotFound()
        titulo = idioma.traducir(m['titulo'], self._idioma())
        categorias = []
        if m['palabras_categoria']:
            for item in self._resumen_categorias():
                nombres = [item['categoria'].name.lower()] + [h['categoria'].name.lower() for h in item['hijos']]
                es_no_fus = any('no fusionable' in n or 'no-fusionable' in n for n in nombres[:1])
                if m['clave'] == 'confeccion' and es_no_fus:
                    continue
                if any(p in n for p in m['palabras_categoria'] for n in nombres):
                    categorias.append(item)
        return request.render('website_quimibond.mercado', self._valores_comunes(
            mercado=m,
            categorias=categorias,
            title='%s | Quimibond' % titulo,
            website_meta_description=m['resumen'],
        ))

    @http.route('/planta', type='http', auth='public', website=True, sitemap=_sitemap_fijo(['/planta']))
    def planta(self, **kw):
        _exigir_activo()
        return request.render('website_quimibond.planta', self._valores_comunes(
            title='Planta y proceso | Quimibond',
            website_meta_description='Tejido circular de una y doble fontura, jets de teñido, ramas, línea de '
                                     'no tejidos e inspección rollo por rollo en nuestra planta de Toluca.',
        ))

    @http.route('/calidad', type='http', auth='public', website=True, sitemap=_sitemap_fijo(['/calidad']))
    def calidad(self, **kw):
        _exigir_activo()
        return request.render('website_quimibond.calidad', self._valores_comunes(
            title='Calidad y sustentabilidad | Quimibond',
            website_meta_description='Sistema de gestión integral certificado en ISO 9001:2015, ISO 14001:2015 e ISO 45001:2018. '
                                     'Paneles solares, planta tratadora de agua, agua pluvial y reciclaje.',
        ))

    @http.route('/nosotros', type='http', auth='public', website=True, sitemap=_sitemap_fijo(['/nosotros']))
    def nosotros(self, **kw):
        _exigir_activo()
        return request.render('website_quimibond.nosotros', self._valores_comunes(
            qb_mision=contenido.MISION,
            qb_vision=contenido.VISION,
            qb_valores=contenido.VALORES,
            qb_razones=contenido.RAZONES,
            title='Nosotros | Quimibond',
            website_meta_description='Productora de No Tejidos Quimibond: fabricantes de scrims automotrices, entretelas y textiles '
                                     'técnicos en Toluca, México, desde 1992.',
        ))

    @http.route('/cotizar', type='http', auth='public', website=True, sitemap=_sitemap_fijo(['/cotizar']))
    def cotizar(self, producto=None, mercado=None, **kw):
        _exigir_activo()
        asunto = ''
        if producto:
            asunto = '%s: %s' % (idioma.traducir('Cotización', self._idioma()), producto[:80])
        m = contenido.mercado(mercado) if mercado else None
        return request.render('website_quimibond.cotizar', self._valores_comunes(
            asunto=asunto,
            mercado_sel=m and m['nombre'] or '',
            title='Solicitar cotización o muestra | Quimibond',
            website_meta_description='Solicite cotización o muestra de scrims, entretelas y textiles técnicos. '
                                     'Le responde un asesor de Ventas en un día hábil.',
        ))

    @http.route('/disenar', type='http', auth='public', website=True, sitemap=_sitemap_fijo(['/disenar']))
    def disenar(self, **kw):
        _exigir_activo()
        return request.render('website_quimibond.disenar', self._valores_comunes(
            asunto='',
            mercado_sel='',
            title='Diseñe su tela | Quimibond',
            website_meta_description='Diseñe su tela en línea: aplicación, peso, ancho, color, tacto y '
                                     'acabado. Vea la tela al momento y envíe el diseño a Ventas.',
        ))

    @http.route('/cotizar/gracias', type='http', auth='public', website=True, sitemap=False)
    def cotizar_gracias(self, o=None, **kw):
        _exigir_activo()
        from .formulario import ORIGENES
        return request.render('website_quimibond.gracias', self._valores_comunes(
            qb_origen=o if o in ORIGENES else 'cotizar',
            title='Gracias | Quimibond',
        ))


class QuimibondPortada(Website):

    @http.route()
    def index(self, **kw):
        if _activo():
            sitio = QuimibondSitio()
            return request.render('website_quimibond.portada', sitio._valores_comunes(
                categorias=sitio._resumen_categorias(),
                vacantes=sitio._vacantes(),
                total_productos=len(sitio._productos()),
                title='Quimibond | Scrims automotrices, entretelas y textiles técnicos',
                website_meta_description='Fabricantes de scrims automotrices, tejido circular, no tejidos y entretelas para '
                                         'la industria automotriz, blancos y confección. ISO 9001 e ISO 14001. '
                                         'Solicite cotización o muestra.',
            ))
        return super().index(**kw)
