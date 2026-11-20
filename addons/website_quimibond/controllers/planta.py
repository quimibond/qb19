"""Maquinaria de la planta para el sitio: Odoo primero, texto fijo de respaldo.

Solo se lee de Odoo cuántos jets y cuántas ramas hay (centros de trabajo con
tipo de máquina de `quimibond_ficha_tecnica_tela`); si ese módulo no está o la
lectura falla, se usa la cantidad escrita en `contenido.MAQUINARIA`. Galgas,
diámetros, cargas y demás parámetros de proceso no se publican: son la receta
(Dirección, 2026-11-19).
"""
import copy
import logging

from . import contenido

_logger = logging.getLogger(__name__)

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


def _con_maquinas(grupo, cantidad):
    """Agrega el renglón «Máquinas» (jets y ramas; Dirección, 2026-11-19)."""
    _base, singular, plural = grupo['maquinas']
    cantidad = cantidad or _base
    grupo['datos'] = list(grupo['datos']) + [('Máquinas', '%d %s' % (cantidad, singular if cantidad == 1 else plural))]


def maquinaria(env, empresa):
    """Grupos de maquinaria para la página de planta."""
    grupos = copy.deepcopy(contenido.MAQUINARIA)
    por_tipo = {}
    try:
        por_tipo = _centros_por_tipo(env, empresa)
    except Exception:  # pylint: disable=broad-except
        # La página pública no se cae por un dato de planta: se sirve el texto fijo.
        _logger.exception('website_quimibond: no se pudo leer la maquinaria de Odoo')
    for grupo in grupos:
        if grupo.get('maquinas'):
            _con_maquinas(grupo, len(por_tipo.get(grupo.get('tipo')) or []))
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
