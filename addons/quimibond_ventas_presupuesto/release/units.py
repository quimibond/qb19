# -*- coding: utf-8 -*-
"""Cantidad en la unidad del cliente → cantidad en la unidad del producto.

Python puro. Los clientes piden en metros (MT, M, MTR, LM), yardas lineales
(LY, YD), metros cuadrados (M2) o kilos; nosotros vendemos en metros o kilos.
Entre longitud y peso el puente es el rendimiento m/kg de la ficha del
producto (``qb.producto.ficha.rendimiento_m_kg``); entre longitud y área, el
ancho. Si falta el dato, la conversión devuelve None y la línea no se aplica
(nunca se inventa un factor)."""

YARD_M = 0.9144

# Unidad del cliente → (dimensión, factor a la unidad base de la dimensión).
# Bases: metro lineal ('length'), kilo ('weight'), metro cuadrado ('area').
CUSTOMER_UNITS = {
    'M': ('length', 1.0), 'MT': ('length', 1.0), 'MTR': ('length', 1.0),
    'MTS': ('length', 1.0), 'LM': ('length', 1.0), 'ML': ('length', 1.0),
    'METRO': ('length', 1.0), 'METROS': ('length', 1.0),
    'YD': ('length', YARD_M), 'YDS': ('length', YARD_M), 'LY': ('length', YARD_M),
    'YARDA': ('length', YARD_M), 'YARDAS': ('length', YARD_M),
    'KG': ('weight', 1.0), 'KGS': ('weight', 1.0), 'KILO': ('weight', 1.0),
    'KILOS': ('weight', 1.0),
    'M2': ('area', 1.0), 'MT2': ('area', 1.0), 'SQM': ('area', 1.0),
    'YD2': ('area', YARD_M * YARD_M), 'SY': ('area', YARD_M * YARD_M),
}


def customer_unit(code):
    """(dimensión, factor) de una unidad del cliente, o None si no se conoce."""
    return CUSTOMER_UNITS.get((code or '').strip().upper().replace('.', ''))


def to_base(qty, from_dim, to_dim, yield_m_kg=None, width_m=None):
    """Convierte entre metro lineal, kilo y metro cuadrado. None si falta el
    dato que hace de puente."""
    if from_dim == to_dim:
        return qty
    # Todo pasa por metros lineales.
    if from_dim == 'length':
        meters = qty
    elif from_dim == 'weight':
        if not yield_m_kg:
            return None
        meters = qty * yield_m_kg
    elif from_dim == 'area':
        if not width_m:
            return None
        meters = qty / width_m
    else:
        return None
    if to_dim == 'length':
        return meters
    if to_dim == 'weight':
        return meters / yield_m_kg if yield_m_kg else None
    if to_dim == 'area':
        return meters * width_m if width_m else None
    return None


def convert(qty, customer_uom, product_dim, product_factor=1.0,
            yield_m_kg=None, width_m=None, fixed_factor=None):
    """Cantidad del cliente → cantidad en la unidad del producto.

    customer_uom: código del cliente (MT, LY, KG, M2…).
    product_dim / product_factor: dimensión de la unidad del producto y
      cuántas unidades base vale una unidad del producto (m = 1, km = 1000,
      g = 0.001…).
    fixed_factor: si el catálogo trae un factor fijo (unidades del producto
      por unidad del cliente), manda sobre todo lo demás.
    Devuelve None si no se puede convertir."""
    if fixed_factor:
        return qty * fixed_factor
    unit = customer_unit(customer_uom)
    if not unit or not product_factor:
        return None
    from_dim, from_factor = unit
    base = to_base(qty * from_factor, from_dim, product_dim,
                   yield_m_kg=yield_m_kg, width_m=width_m)
    if base is None:
        return None
    return base / product_factor
