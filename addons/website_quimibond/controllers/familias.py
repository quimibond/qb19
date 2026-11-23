"""Catálogo por familias (/catalogo).

Los productos publicados en Odoo son solo entretelas, con nombres y categorías
del sitio disparejos, y lo automotriz (la mayor parte de la venta) son telas
desarrolladas para un cliente, que no se publican una por una. Por eso el
catálogo se organiza por familias escritas aquí, y solo las familias de
entretelas listan artículos de Odoo: los publicados, clasificados por su
nombre (no por la categoría del sitio).

Los rangos de peso, ancho y color salen de los productos vendidos en los
últimos 12 meses (Odoo, 2026-11-20). Sin precios y sin parámetros de proceso.
"""

GRUPOS = [
    {'clave': 'automotriz', 'nombre': 'Automotriz', 'icono': 'fa-car',
     'texto': 'Scrims desarrollados contra la especificación de su cliente.'},
    {'clave': 'confeccion', 'nombre': 'Confección', 'icono': 'fa-scissors',
     'texto': 'Entretelas fusionables de línea, con existencia para surtir.'},
    {'clave': 'blancos', 'nombre': 'Blancos y confección', 'icono': 'fa-bed',
     'texto': 'No tejidos y telas de punto para colchonería, blancos y prendas.'},
]

FAMILIAS = [
    {
        'clave': 'scrim-tejido',
        'grupo': 'automotriz',
        'construccion': 'una_fontura',
        'nombre': 'Scrims de tejido circular',
        'frase': 'Soporte de tejido de punto para laminar con espuma y con la tela, el vinil o la piel de la vestidura.',
        'texto': 'Tela ligera de tejido circular que da soporte y estabilidad al laminado. La desarrollamos contra '
                 'la especificación de su cliente y la entregamos termofijada, al ancho pedido.',
        'datos': [
            ('Construcción', 'Tejido de punto circular'),
            ('Composición', 'Poliéster, poliéster reciclado o poliamida (nailon)'),
            ('Peso', 'De 32 a 75 g/m², según su especificación'),
            ('Ancho', 'A la medida, hasta 210 cm'),
            ('Color', 'Natural, blanco o negro'),
            ('Acabado', 'Termofijado; retardante a la flama si su especificación lo pide'),
            ('Presentación', 'Tela abierta, en rollo'),
        ],
        'aplicaciones': ['Vestiduras de asientos', 'Descansabrazos de puerta', 'Otros componentes del interior'],
        'sobre_pedido': True,
    },
    {
        'clave': 'scrim-no-tejido',
        'grupo': 'automotriz',
        'construccion': 'no_tejido',
        'nombre': 'Scrims no tejidos',
        'frase': 'Soporte de fibra consolidada, sin tejer, para laminados del interior del vehículo.',
        'texto': 'Velo de fibra consolidado al peso, el espesor y la resistencia que pide la especificación. '
                 'Se fabrica en nuestra línea de no tejidos y se inspecciona rollo por rollo.',
        'datos': [
            ('Construcción', 'No tejido'),
            ('Composición', 'Poliéster, poliéster reciclado o polipropileno'),
            ('Peso', 'Según su especificación'),
            ('Presentación', 'En rollo, cortado o perforado'),
        ],
        'aplicaciones': ['Vestiduras de asientos', 'Otros componentes del interior'],
        'sobre_pedido': True,
    },
    {
        'clave': 'entretela-tejida',
        'grupo': 'confeccion',
        'construccion': 'una_fontura',
        'fusionable': True,
        'nombre': 'Entretelas tejidas fusionables',
        'frase': 'Entretela de tejido circular con punto de adhesivo, para prendas que piden elasticidad y caída.',
        'texto': 'La base de tejido de punto acompaña el movimiento de la prenda y el punto de adhesivo la fija '
                 'a la tela. Es nuestra línea de entretelas con más clientes.',
        'datos': [
            ('Construcción', 'Tejido de punto circular, fusionable'),
            ('Peso', 'De 46 a 150 g/m²'),
            ('Ancho', '152 y 160 cm'),
            ('Color', 'Blanco, off white, hueso, gris, negro y rojo'),
        ],
        'aplicaciones': ['Camisería y blusas', 'Uniformes', 'Prendas con elasticidad'],
        'palabras': ['tejida', 'tejido circular'],
        'excluye': ['no tejida', 'tramad', 'no fusionable'],
    },
    {
        'clave': 'entretela-no-tejida',
        'grupo': 'confeccion',
        'construccion': 'no_tejido',
        'fusionable': True,
        'nombre': 'Entretelas no tejidas fusionables',
        'frase': 'Entretela de fibra con punto de adhesivo para cuellos, puños, carteras y refuerzos.',
        'texto': 'Velo de fibra con punto de adhesivo, en tactos del suave al rígido. '
                 'Hay versiones para lavado en agua y para lavado en seco.',
        'datos': [
            ('Construcción', 'No tejido fusionable'),
            ('Peso', 'De 27 a 77 g/m²'),
            ('Ancho', '152 cm'),
            ('Color', 'Blanco y gris'),
            ('Tacto', 'Suave, semirrígido o rígido'),
        ],
        'aplicaciones': ['Cuellos y puños', 'Camisería', 'Uniformes', 'Prendas ligeras'],
        'palabras': ['no tejida'],
        'excluye': ['tramad', 'no fusionable'],
    },
    {
        'clave': 'entretela-tramada',
        'grupo': 'confeccion',
        'construccion': 'tramado',
        'fusionable': True,
        'nombre': 'Entretelas no tejidas tramadas',
        'frase': 'No tejido reforzado con hilos de trama, para frentes de saco y refuerzos.',
        'texto': 'El velo de fibra lleva hilos de trama que le dan estabilidad y recuperación. '
                 'Se usa donde la prenda debe conservar su forma.',
        'datos': [
            ('Construcción', 'No tejido tramado, fusionable'),
            ('Composición', 'Poliéster con poliamida'),
            ('Peso', 'De 34 a 56 g/m²'),
            ('Ancho', '152 cm'),
            ('Color', 'Blanco y gris'),
        ],
        'aplicaciones': ['Frentes de saco', 'Refuerzos'],
        'palabras': ['tramad'],
        'excluye': ['no fusionable'],
    },
    {
        'clave': 'no-fusionable',
        'grupo': 'blancos',
        'construccion': 'no_tejido',
        'nombre': 'No tejidos no fusionables',
        'frase': 'Velo de fibra sin adhesivo, del suave al rígido, para colchonería, blancos y confección.',
        'texto': 'No tejido de poliéster sin punto de adhesivo. Se surte en rollo y también cortado '
                 'o perforado a la medida de su línea.',
        'datos': [
            ('Construcción', 'No tejido'),
            ('Composición', 'Poliéster'),
            ('Peso', 'De 28 a 60 g/m²'),
            ('Color', 'Blanco y gris'),
            ('Tacto', 'Suave, semirrígido o rígido'),
            ('Presentación', 'En rollo, cortado o perforado'),
        ],
        'aplicaciones': ['Colchonería y blancos', 'Confección'],
        'palabras': ['no fusionable'],
        'excluye': [],
    },
    {
        'clave': 'telas-punto',
        'grupo': 'blancos',
        'construccion': 'doble_fontura',
        'nombre': 'Telas de punto',
        'frase': 'Jersey, piqué, crepé y felpa de tejido circular, crudas o teñidas a su color.',
        'texto': 'Telas de punto de una y doble fontura para prendas y para blancos, teñidas y acabadas '
                 'en la misma planta.',
        'datos': [
            ('Construcción', 'Tejido de punto circular, de una y doble fontura'),
            ('Composición', 'Poliéster, poliamida (nailon) o poliéster con algodón'),
            ('Peso', 'De 40 a 145 g/m²'),
            ('Color', 'Crudo, blanco o teñido de acuerdo a su especificación'),
            ('Presentación', 'Tela abierta o tubular'),
        ],
        'aplicaciones': ['Playeras y ropa deportiva', 'Uniformes', 'Fundas y protectores de colchón'],
        'sobre_pedido': True,
    },
]


def familia(clave):
    for f in FAMILIAS:
        if f['clave'] == clave:
            return f
    return None


def grupos():
    """Los grupos con sus familias, en el orden en que se muestran."""
    return [dict(g, familias=[f for f in FAMILIAS if f['grupo'] == g['clave']]) for g in GRUPOS]


def familia_de(nombre):
    """Familia de entretelas a la que pertenece un producto, por su nombre; None si a ninguna."""
    bajo = (nombre or '').lower()
    if 'entretela' not in bajo:
        return None
    # De la más específica a la más general: «no fusionable» y «tramada» antes que «no tejida».
    for clave in ('no-fusionable', 'entretela-tramada', 'entretela-no-tejida', 'entretela-tejida'):
        f = familia(clave)
        if any(p in bajo for p in f['palabras']) and not any(x in bajo for x in f['excluye']):
            return f
    return None
