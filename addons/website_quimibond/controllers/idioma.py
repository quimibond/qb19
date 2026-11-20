"""Versión en inglés del sitio B2B.

El módulo está escrito en español, así que para Odoo el español es el texto
de origen. El inglés va en ``i18n/en_CA.po`` (``en_US`` no admite traducción:
es el idioma de origen de Odoo) y sale por tres caminos:

* las plantillas las traduce Odoo solo, término por término;
* los textos de ``contenido.py`` y de los controladores pasan por
  :func:`traducir` al armar los valores de la página;
* los textos del script van en un atributo de la página (:func:`textos_js`)
  y el script los busca con ``T("…")``.

Lo que no tiene traducción se queda en español: una página nunca falla ni sale
vacía por un texto sin traducir.
"""
import json
import re

from odoo.tools.translate import code_translations

MODULO = 'website_quimibond'
IDIOMA_INGLES = 'en_CA'

# Llaves cuyo valor es un identificador (clase CSS, clave de ruta, color) y no
# un texto que se muestra.
NO_TRADUCIR = frozenset({
    'clave', 'icono', 'tipo', 'familia', 'construccion', 'color', 'enlace', 'anio', 'paso', 'num',
    'url', 'grupo', 'palabras', 'excluye', 'origen',
    # Fichas leídas del nombre del producto: las plantillas comparan sus
    # valores en español («Fusionable», «blanco» como clase); se muestran con qb_t.
    'productos', 'ficha', 'relacionados', 'articulos',
})

# «5 jets de teñido», «3 ramas»: la cantidad se arma en Python.
RE_CANTIDAD = re.compile(r'^(\d[\d.,]*\s+)(.+)$')

# Textos que escribe el script (static/src/js/quimibond.js). Si se agrega uno
# ahí con T("…"), va también aquí y en el .po.
TEXTOS_JS = [
    'Suave', 'Semirrígido', 'Rígido', 'Fusionable', 'Termofijada', 'Personalizado', 'Mercado', 'Aplicación',
    'Construcción', 'Fibra', 'Poliéster', 'Hilo', 'con elastano', 'Peso', 'Ancho', 'Color', 'Tacto', 'Acabado',
    'Presentación', 'Requisitos', 'Norma', 'Volumen', 'Lo necesita para', 'Diseño de tela (diseñador en línea)',
    'Diseño de tela', 'Enlace copiado', 'Copie el enlace de su diseño:',
    'Cargando catálogo…', 'Sin resultados. Fabricamos sobre especificación: pida una cotización.', 'Productos',
    'TORNIQUETE', 'TOBERA', 'TELA EN CUERDA', 'CALOR', 'BOMBA', 'BAÑO', 'CRUDO', 'COLOR PAREJO', 'ENTRADA',
    'ENSANCHE', 'SECADO Y TERMOFIJADO', 'ROLLO', 'CADENA DE AGUJAS', 'ANCHO PEDIDO',
]


def tabla(lang):
    """{texto en español: traducción} del idioma; vacío para el español."""
    if not lang or lang == 'en_US' or lang.startswith('es'):
        return {}
    return code_translations.get_python_translations(MODULO, lang)


def traducir(valor, lang, _tabla=None):
    """Copia de ``valor`` con sus textos traducidos (cadenas, listas, tuplas y diccionarios)."""
    t = tabla(lang) if _tabla is None else _tabla
    if not t:
        return valor
    if isinstance(valor, str):
        if valor in t:
            return t[valor]
        cantidad = RE_CANTIDAD.match(valor)
        if cantidad and cantidad.group(2) in t:
            return cantidad.group(1) + t[cantidad.group(2)]
        return valor
    if isinstance(valor, dict):
        return {k: (v if k in NO_TRADUCIR else traducir(v, lang, t)) for k, v in valor.items()}
    if isinstance(valor, list):
        return [traducir(v, lang, t) for v in valor]
    if isinstance(valor, tuple):
        return tuple(traducir(v, lang, t) for v in valor)
    return valor


def textos_js(lang):
    """JSON con los textos del script en el idioma, o '' si es español."""
    t = tabla(lang)
    if not t:
        return ''
    return json.dumps({s: t[s] for s in TEXTOS_JS if s in t}, ensure_ascii=False)
