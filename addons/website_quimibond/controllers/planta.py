"""Maquinaria de la planta para el sitio: Odoo primero, texto fijo de respaldo.

Los datos de las circulares (galgas, diámetros y salida de la tela; nunca
marcas ni cuántas máquinas hay) se leen de los centros de trabajo y de la ficha de la máquina
(`quimibond_ficha_tecnica_tela`) cuando ese módulo está instalado. Si no lo
está, o si la lectura falla, se sirve lo escrito en `contenido.MAQUINARIA`.
"""
import copy
import logging

from . import contenido

_logger = logging.getLogger(__name__)

SALIDAS = {'tubular': 'tubular', 'abierta': 'abierta'}


def _numero(valor):
    return ('%g' % valor) if valor else ''


def _lista(textos):
    """«a, b y c» (con «e» antes de i- o hi-)."""
    textos = [t for t in textos if t]
    if len(textos) < 2:
        return ''.join(textos)
    union = ' e ' if textos[-1].lower().startswith(('i', 'hi')) else ' y '
    return ', '.join(textos[:-1]) + union + textos[-1]


def _centros_por_tipo(env, empresa):
    """{clave del tipo: centros de trabajo activos de la empresa}; {} si no hay ficha de máquina."""
    if 'mrp.workcenter' not in env or 'ft_tipo_id' not in env['mrp.workcenter']._fields:
        return {}
    centros = env['mrp.workcenter'].sudo().search([
        ('ft_tipo_id', '!=', False), ('company_id', 'in', [empresa.id, False])])
    por_tipo = {}
    for centro in centros:
        por_tipo.setdefault(centro.ft_tipo_id.code, []).append(centro)
    return por_tipo


def _datos_circulares(centros, datos):
    """Sustituye galgas, diámetros y salida con lo que dicen las fichas vigentes."""
    configs = [c.ft_config_id for c in centros if c.ft_config_id]
    galgas = sorted({c.galga for c in configs if c.galga})
    diametros = sorted({c.diametro for c in configs if c.diametro})
    salidas = set()
    for config in configs:
        for valor in config.valor_ids:
            if valor.parametro_id.code == 'salida_tela' and valor.valor_texto:
                salidas.add(valor.valor_texto.strip().lower())
    nuevos = {
        'Galgas': _lista([_numero(g) for g in galgas]),
        'Diámetros': (_lista([_numero(d) for d in diametros]) + ' pulgadas') if diametros else '',
        'Salida de la tela': _lista([SALIDAS[s] for s in ('tubular', 'abierta') if s in salidas]).capitalize(),
    }
    return [(nombre, nuevos.get(nombre) or valor) for nombre, valor in datos]


def maquinaria(env, empresa):
    """Grupos de maquinaria para la página de planta."""
    grupos = copy.deepcopy(contenido.MAQUINARIA)
    try:
        por_tipo = _centros_por_tipo(env, empresa)
        for grupo in grupos:
            centros = por_tipo.get(grupo.get('tipo'))
            if not centros:
                continue
            if grupo['tipo'] == 'tejido_circular':
                grupo['datos'] = _datos_circulares(centros, grupo['datos'])
    except Exception:  # pylint: disable=broad-except
        # La página pública no se cae por un dato de planta: se sirve el texto fijo.
        _logger.exception('website_quimibond: no se pudo leer la maquinaria de Odoo')
        return copy.deepcopy(contenido.MAQUINARIA)
    return grupos


CATEGORIA_HILO = 'Materia Prima / Hilo'


def disenador(env):
    """Opciones del diseñador; los hilos, según las categorías de hilo de Odoo que tienen productos."""
    opciones = copy.deepcopy(contenido.DISENADOR)
    opciones['hilos'] = [nombre for _categoria, nombre in contenido.DISENADOR['hilos'] if _categoria != 'Bamboo']
    opciones['elastano'] = True
    try:
        Categoria = env['product.category'].sudo()
        raiz = Categoria.search([('complete_name', '=', CATEGORIA_HILO)], limit=1)
        if raiz:
            con_producto = {c.name for c in Categoria.search([('parent_id', '=', raiz.id)]) if c.product_count}
            if con_producto:
                opciones['hilos'] = [nombre for categoria, nombre in contenido.DISENADOR['hilos']
                                     if categoria in con_producto]
                opciones['elastano'] = contenido.DISENADOR['hilo_elastano'] in con_producto
    except Exception:  # pylint: disable=broad-except
        _logger.exception('website_quimibond: no se pudieron leer los hilos de Odoo')
    return opciones
