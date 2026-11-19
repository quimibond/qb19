"""Textos fijos del sitio B2B que no viven en un modelo de Odoo.

Lo que sí vive en Odoo (productos, categorías, vacantes, equipo de ventas) se
lee en el controlador. Aquí van los mercados, los pasos de la planta y las
cifras de la empresa, para tenerlos en un solo lugar.
"""

CIFRAS = [
    {'valor': 1992, 'prefijo': '', 'sufijo': '', 'etiqueta': 'Año de fundación', 'sin_miles': True},
    {'valor': 150, 'prefijo': '+', 'sufijo': '', 'etiqueta': 'Colaboradores'},
    {'valor': 60, 'prefijo': '+', 'sufijo': '', 'etiqueta': 'Máquinas de fabricación'},
    {'valor': 5, 'prefijo': '', 'sufijo': '', 'etiqueta': 'Líneas de producto'},
]

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
