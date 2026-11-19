"""Maquinaria de la planta para el sitio: Odoo primero, texto fijo de respaldo.

Las cantidades y los datos de las circulares (galgas, diámetros, salida de la
tela, marcas) se leen de los centros de trabajo y de la ficha de la máquina
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


def _marca(texto):
    return ' '.join(p.capitalize() for p in (texto or '').upper().replace('PAILUNG', 'PAI LUNG').split())


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
    """Sustituye galgas, diámetros, salida y marcas con lo que dicen las fichas vigentes."""
    configs = [c.ft_config_id for c in centros if c.ft_config_id]
    galgas = sorted({c.galga for c in configs if c.galga})
    diametros = sorted({c.diametro for c in configs if c.diametro})
    salidas = set()
    for config in configs:
        for valor in config.valor_ids:
            if valor.parametro_id.code == 'salida_tela' and valor.valor_texto:
                salidas.add(valor.valor_texto.strip().lower())
    marcas = {}
    for centro in centros:
        if centro.ft_marca:
            marcas[_marca(centro.ft_marca)] = marcas.get(_marca(centro.ft_marca), 0) + 1
    nuevos = {
        'Galgas': _lista([_numero(g) for g in galgas]),
        'Diámetros': (_lista([_numero(d) for d in diametros]) + ' pulgadas') if diametros else '',
        'Salida de la tela': _lista([SALIDAS[s] for s in ('tubular', 'abierta') if s in salidas]).capitalize(),
        'Marcas': _lista(sorted(marcas, key=lambda m: (-marcas[m], m))),
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
            grupo['cantidad'] = len(centros)
            if grupo['tipo'] == 'tejido_circular':
                grupo['datos'] = _datos_circulares(centros, grupo['datos'])
    except Exception:  # pylint: disable=broad-except
        # La página pública no se cae por un dato de planta: se sirve el texto fijo.
        _logger.exception('website_quimibond: no se pudo leer la maquinaria de Odoo')
        return copy.deepcopy(contenido.MAQUINARIA)
    return grupos


def cifras(grupos):
    """Las cifras de la empresa, con las que salen de la maquinaria ya leída de Odoo."""
    cantidades = {g['tipo']: g['cantidad'] for g in grupos if g.get('tipo')}
    return [dict(c, valor=cantidades.get(c.get('tipo')) or c['valor']) for c in contenido.CIFRAS]
