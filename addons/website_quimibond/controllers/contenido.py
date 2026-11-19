"""Textos fijos del sitio B2B que no viven en un modelo de Odoo.

Lo que sí vive en Odoo (productos, categorías, vacantes, equipo de ventas) se
lee en el controlador. Aquí van los mercados, los pasos de la planta y las
cifras de la empresa, para tenerlos en un solo lugar.
"""

CIFRAS = [
    {'valor': 1992, 'prefijo': '', 'sufijo': '', 'etiqueta': 'Año de fundación', 'sin_miles': True},
    {'valor': 150, 'prefijo': '+', 'sufijo': '', 'etiqueta': 'Colaboradores'},
    {'valor': 37, 'prefijo': '', 'sufijo': '', 'etiqueta': 'Máquinas circulares', 'tipo': 'tejido_circular'},
    {'valor': 5, 'prefijo': '', 'sufijo': '', 'etiqueta': 'Jets de tintorería', 'tipo': 'tintoreria'},
]

# Armadoras confirmadas por Jose el 2026-11-19 («mete todas»). Se cargan al
# instalar en el campo del sitio web (qb_armadoras); después se editan ahí.
# Son las armadoras finales, nunca los clientes directos.
ARMADORAS = [
    'Toyota', 'Nissan', 'Mazda', 'Honda', 'General Motors', 'Ford',
    'Volkswagen', 'Stellantis', 'Tesla', 'Mitsubishi', 'Mercedes-Benz', 'BMW',
]

# Tipos de tela que salen de la planta. Corresponden a la maquinaria de
# MAQUINARIA; no se toman del catálogo publicado.
TEJIDOS = [
    {
        'clave': 'una_fontura',
        'nombre': 'Punto de una fontura',
        'etiqueta': 'Tejido circular',
        'texto': 'Un juego de agujas en el cilindro. Tela ligera, con cara y revés distintos y '
                 'elasticidad natural a lo ancho.',
        'construccion': 'punto',
    },
    {
        'clave': 'doble_fontura',
        'nombre': 'Punto de doble fontura',
        'etiqueta': 'Tejido circular',
        'texto': 'Dos juegos de agujas, cilindro y plato. Tela con más cuerpo y estabilidad '
                 'dimensional.',
        'construccion': 'punto',
    },
    {
        'clave': 'no_tejido',
        'nombre': 'No tejido',
        'etiqueta': 'Línea de no tejidos',
        'texto': 'Fibra abierta en carda y consolidada en velo, sin pasar por hilo. Es la base de '
                 'nuestras entretelas.',
        'construccion': 'notejido',
    },
    {
        'clave': 'tramado',
        'nombre': 'Tejido tramado',
        'etiqueta': 'Entretelas',
        'texto': 'Base con hilos de trama para entretelas que piden estabilidad y recuperación.',
        'construccion': 'tejido',
    },
]

PRESENTACIONES = [
    'Tela tubular', 'Tela abierta', 'Teñida en jet', 'Termofijada en rama', 'Fusionable por espolvoreo',
    'Cortada y perforada',
]

# Maquinaria de la planta. Fuente: centros de trabajo y fichas de máquina de
# Odoo (mrp.workcenter, ficha.tecnica.maquina.config) al 2026-11-19. Donde el
# grupo trae «tipo», la cantidad (y en las circulares galgas, diámetros y
# salida) se vuelve a leer de Odoo al servir la página (controllers/planta.py);
# Va en términos generales (Dirección, 2026-11-19): sin marcas de máquinas ni
# nombres internos de línea.
# lo de aquí es el respaldo. No se publican capacidades que Odoo no tenga.
MAQUINARIA = [
    {
        'clave': 'tejido',
        'nombre': 'Tejido circular',
        'icono': 'fa-circle-o-notch',
        'tipo': 'tejido_circular',
        'cantidad': 37,
        'unidad': 'máquinas circulares',
        'texto': 'Circulares de una fontura y de doble fontura, con salida de tela tubular o abierta.',
        'datos': [
            ('Fonturas', 'Una fontura y doble fontura'),
            ('Galgas', '16, 18, 24, 28 y 32'),
            ('Diámetros', '30, 32 y 38 pulgadas'),
            ('Salida de la tela', 'Tubular y abierta'),
        ],
    },
    {
        'clave': 'tintoreria',
        'nombre': 'Tintorería',
        'icono': 'fa-tint',
        'tipo': 'tintoreria',
        'cantidad': 5,
        'unidad': 'jets de teñido',
        'texto': 'Jets de teñido con cocina de colores propia. Cada baño lleva su número y su fórmula.',
        'datos': [
            ('Carga por jet', 'De 300 a 1,200 kg'),
            ('Relación de baño', 'Desde 1:6'),
            ('Apoyo', 'Cocina de colores'),
        ],
    },
    {
        'clave': 'acabado',
        'nombre': 'Ramas y acabado',
        'icono': 'fa-fire',
        'tipo': 'acabado',
        'cantidad': 3,
        'unidad': 'ramas',
        'texto': 'Secado, termofijado y acabado al ancho. Cada rollo se pesa y se mide al salir de la rama.',
        'datos': [
            ('Proceso', 'Secado, termofijado y acabado'),
            ('Preparación', 'Abridoras y devanadoras'),
            ('Apoyo', 'Cocina de resinas'),
        ],
    },
    {
        'clave': 'no_tejidos',
        'nombre': 'No tejidos y entretelas',
        'icono': 'fa-th',
        'cantidad': 0,
        'unidad': 'Línea completa',
        'texto': 'De la fibra a la entretela fusionable: carda, espolvoreo de adhesivo y fusionado.',
        'datos': [
            ('Formación', 'Preparación de fibra y carda'),
            ('Adhesivo', 'Espolvoreo en línea'),
            ('Acabado', 'Estiramiento, termofijado y fusionadora'),
            ('Además', 'Tejido tramado, corte y perforado'),
        ],
    },
    {
        'clave': 'inspeccion',
        'nombre': 'Inspección y laboratorio',
        'icono': 'fa-search',
        'tipo': 'inspeccion',
        'cantidad': 6,
        'unidad': 'máquinas de inspección',
        'texto': 'Inspección rollo por rollo con peso, metros, defectos y clasificación. Laboratorio '
                 'en planta y empaque.',
        'datos': [
            ('Registro', 'Etiqueta con código de barras por rollo'),
            ('Trazabilidad', 'Del baño de teñido al rollo terminado'),
        ],
    },
]

# Logotipo de cada armadora (static/src/img/armadoras/<archivo>.svg). Los que
# hay vienen de Simple Icons (CC0). Una marca sin archivo sale con su nombre.
# Para agregar uno: el SVG oficial en esa carpeta y su renglón aquí.
LOGOS_ARMADORAS = {
    'toyota': 'toyota', 'nissan': 'nissan', 'mazda': 'mazda', 'honda': 'honda',
    'general motors': 'generalmotors', 'gm': 'generalmotors', 'ford': 'ford',
    'volkswagen': 'volkswagen', 'vw': 'volkswagen', 'tesla': 'tesla', 'mitsubishi': 'mitsubishi',
    'bmw': 'bmw',
}


def logo_armadora(nombre):
    archivo = LOGOS_ARMADORAS.get((nombre or '').strip().lower())
    return archivo and '/website_quimibond/static/src/img/armadoras/%s.svg' % archivo or ''


# Opciones del diseñador de tela (/disenar). Son lo que el cliente pide, no una
# promesa de fabricación: Ventas confirma la factibilidad de cada diseño.
DISENADOR = {
    'galgas': [16, 18, 24, 28, 32],
    'fibras': ['Poliéster', 'Algodón', 'Poliamida', 'Mezcla'],
    'colores': [
        ('Blanco', '#f5f5f2'), ('Crudo', '#e9e2d0'), ('Beige', '#d6c4a4'), ('Gris', '#9a9aa2'),
        ('Negro', '#1b1b1f'), ('Azul marino', '#1f2a4a'), ('Azul', '#2f54c8'), ('Vino', '#6b1f2e'),
        ('Verde', '#1f5c48'), ('Naranja', '#e8590c'),
    ],
    'tactos': ['Suave', 'Semirrígido', 'Rígido'],
    'presentaciones': ['Tela abierta', 'Tela tubular'],
}

MERCADOS = [
    {
        'clave': 'automotriz',
        'nombre': 'Automotriz',
        'titulo': 'Textiles técnicos para la industria automotriz',
        'resumen': 'Desde 2014 abastecemos al mercado automotriz de México, Estados Unidos y Canadá '
                   'con líneas de producto desarrolladas para sus especificaciones.',
        'puntos': [
            'Desarrollo de producto contra la especificación de su cliente',
            'Lotes trazables de la materia prima al rollo terminado',
            'Calidad liberada por laboratorio antes de embarcar',
            'Programación de producción para entregas recurrentes',
        ],
        'icono': 'fa-car',
        'palabras_categoria': [],
    },
    {
        'clave': 'blancos',
        'nombre': 'Blancos',
        'titulo': 'Soportes textiles para colchonería y blancos',
        'resumen': 'Telas y no tejidos para colchones, almohadas y blancos para el hogar, '
                   'con el gramaje, el ancho y el tacto que pide su proceso.',
        'puntos': [
            'No tejidos no fusionables en distintos gramajes',
            'Anchos y presentaciones para su línea de corte',
            'Consistencia de lote a lote',
            'Muestras para validar en su planta',
        ],
        'icono': 'fa-bed',
        'palabras_categoria': ['no fusionable'],
    },
    {
        'clave': 'confeccion',
        'nombre': 'Confección',
        'titulo': 'Entretelas para la confección',
        'resumen': 'Entretelas fusionables tejidas y no tejidas para camisería, uniformes, '
                   'prendas ligeras, frentes y refuerzos, blusas y tricots.',
        'puntos': [
            'Fusionables para lavado en agua y en seco',
            'Tejido circular fusionable para prendas con elasticidad',
            'Tactos rígido, semirrígido y suave',
            'Asesoría técnica de fusionado: temperatura, presión y tiempo',
        ],
        'icono': 'fa-scissors',
        'palabras_categoria': ['fusionable', 'tricot', 'camiser', 'uniforme', 'prenda', 'frente', 'blusa'],
    },
]

PROCESO = [
    {
        'paso': '01',
        'nombre': 'Tejido',
        'texto': 'Tejido circular y formación de no tejidos a partir de fibra e hilo, con la '
                 'construcción que define el tacto y la estabilidad de la tela.',
        'icono': 'fa-circle-o-notch',
    },
    {
        'paso': '02',
        'nombre': 'Tintorería',
        'texto': 'Teñido en jets con fórmulas controladas por lote, relación de baño y '
                 'dosificación calculadas en Odoo.',
        'icono': 'fa-tint',
    },
    {
        'paso': '03',
        'nombre': 'Rama y acabado',
        'texto': 'Secado, termofijado y acabado en rama. Cada rollo se pesa y se mide al salir.',
        'icono': 'fa-fire',
    },
    {
        'paso': '04',
        'nombre': 'Inspección',
        'texto': 'Inspección rollo por rollo: defectos, metros, peso y clasificación con etiqueta y '
                 'código de barras.',
        'icono': 'fa-search',
    },
    {
        'paso': '05',
        'nombre': 'Laboratorio y embarque',
        'texto': 'Pruebas de calidad, liberación y embarque con trazabilidad completa del lote.',
        'icono': 'fa-flask',
    },
]

HISTORIA = [
    {'anio': '1992', 'titulo': 'Inicios',
     'texto': 'Nace Productora de No Tejidos Quimibond para fabricar entretelas de alta calidad.'},
    {'anio': '2014', 'titulo': 'Mercado automotriz',
     'texto': 'Nuevas líneas de producto para el mercado automotriz de México, Estados Unidos y Canadá.'},
    {'anio': '2015', 'titulo': 'ISO 9001',
     'texto': 'Certificación ISO 9001 del sistema de gestión de calidad.'},
    {'anio': '2018', 'titulo': 'Modernización',
     'texto': 'Tecnología nueva en producción para mejorar eficiencia y calidad.'},
    {'anio': '2023', 'titulo': 'ISO 14001',
     'texto': 'Certificación ISO 14001 del sistema de gestión ambiental.'},
]

SUSTENTABILIDAD = [
    {'icono': 'fa-sun-o', 'titulo': 'Energía solar', 'texto': 'Generación de energía con paneles solares en planta.'},
    {'icono': 'fa-tint', 'titulo': 'Agua pluvial', 'texto': 'Captación y aprovechamiento de agua de lluvia.'},
    {'icono': 'fa-recycle', 'titulo': 'Reciclaje', 'texto': 'Separación de residuos y reciclaje en todas las áreas.'},
]


def mercado(clave):
    for m in MERCADOS:
        if m['clave'] == clave:
            return m
    return None
