"""Textos fijos del sitio B2B que no viven en un modelo de Odoo.

Lo que sí vive en Odoo (productos, categorías, vacantes, equipo de ventas) se
lee en el controlador. Aquí van los mercados, los pasos de la planta y las
cifras de la empresa, para tenerlos en un solo lugar.
"""

from urllib.parse import quote

CIFRAS = [
    {'valor': 1992, 'prefijo': '', 'sufijo': '', 'etiqueta': 'Año de fundación', 'sin_miles': True},
    {'valor': 150, 'prefijo': '+', 'sufijo': '', 'etiqueta': 'Colaboradores'},
    # Sin conteos de máquinas (Dirección, 2026-11-19): lo que le importa a quien compra.
    # Clientes: 293 con factura publicada en los últimos 12 meses (Odoo, 2026-11-20).
    {'valor': 250, 'prefijo': '+', 'sufijo': '', 'etiqueta': 'Clientes atendidos en el último año'},
    {'valor': 3, 'prefijo': '', 'sufijo': '', 'etiqueta': 'Mercados: automotriz, blancos y confección'},
]

# Armadoras confirmadas por Jose el 2026-11-19 («mete todas»). Se cargan al
# instalar en el campo del sitio web (qb_armadoras); después se editan ahí.
# Son las armadoras finales, nunca los clientes directos.
ARMADORAS = [
    'Toyota', 'Nissan', 'Mazda', 'Honda', 'General Motors', 'Ford',
    'Volkswagen', 'Stellantis', 'Tesla', 'Mitsubishi', 'Mercedes-Benz', 'BMW',
]

# Tipos de tela que salen de la planta, en dos familias: tejido (de punto, en
# máquina circular) y no tejido (de fibra, sin pasar por el telar). El tramado
# es un no tejido: un velo reforzado con hilos de trama (Dirección, 2026-11-19).
# Corresponden a la maquinaria de MAQUINARIA; no se toman del catálogo publicado.
FAMILIAS = [
    {
        'clave': 'tejido',
        'nombre': 'Tejido',
        'texto': 'Tela de punto hecha con hilo en máquinas circulares.',
    },
    {
        'clave': 'no_tejido',
        'nombre': 'No tejido',
        'texto': 'Tela hecha directamente de la fibra, sin tejer.',
    },
]

TEJIDOS = [
    {
        'clave': 'una_fontura',
        'familia': 'tejido',
        'nombre': 'Punto de una fontura',
        'etiqueta': 'Tejido circular',
        'texto': 'Un juego de agujas en el cilindro. Tela ligera, con cara y revés distintos y '
                 'elasticidad natural a lo ancho.',
    },
    {
        'clave': 'doble_fontura',
        'familia': 'tejido',
        'nombre': 'Punto de doble fontura',
        'etiqueta': 'Tejido circular',
        'texto': 'Dos juegos de agujas, cilindro y plato. Tela con más cuerpo y estabilidad '
                 'dimensional.',
    },
    {
        'clave': 'no_tejido',
        'familia': 'no_tejido',
        'nombre': 'No tejido',
        'etiqueta': 'Velo de fibra',
        'texto': 'Fibra abierta en carda y consolidada en velo, sin pasar por hilo. Es la base de '
                 'nuestras entretelas.',
    },
    {
        'clave': 'tramado',
        'familia': 'no_tejido',
        'nombre': 'No tejido tramado',
        'etiqueta': 'Velo con trama',
        'texto': 'El mismo velo de fibra, reforzado con hilos de trama para entretelas que piden '
                 'más estabilidad y recuperación.',
    },
]


def familias():
    """Las familias con sus tipos de tela, en el orden en que se muestran."""
    return [dict(f, tejidos=[t for t in TEJIDOS if t['familia'] == f['clave']]) for f in FAMILIAS]


# Dibujos de tejido de punto (ligamentos), con los nombres del catálogo de
# dibujos de la planta (claves de codificación de artículos, parte «dibujo»).
# La «clave» es la del script (DIBUJOS en static/src/js/quimibond.js), que
# anima cómo se forma cada uno. Dirección los aprobó el 2026-11-26.
DIBUJOS = [
    {
        'clave': 'una_fontura',
        'nombre': 'Una fontura',
        'texto': 'Un juego de agujas. Del jersey salen los demás dibujos, cambiando qué agujas hacen malla, '
                 'cuáles cargan el hilo y cuáles se anulan en cada pasada.',
        'dibujos': [
            {'clave': 'jersey', 'nombre': 'Jersey',
             'texto': 'Todas las agujas hacen malla en todas las pasadas. Es el dibujo base de una fontura.'},
            {'clave': 'listado', 'nombre': 'Listado',
             'texto': 'Jersey con hilos de distinto color por pasada: rayas a lo ancho de la tela.'},
            {'clave': 'vanizado', 'nombre': 'Vanizado',
             'texto': 'Dos hilos entran juntos a cada aguja: uno queda a la cara y el otro al revés.'},
            {'clave': 'pique_cargas', 'nombre': 'Piqué cargas',
             'texto': 'Agujas alternas cargan el hilo sin soltar la malla. De ahí sale el relieve del piqué.'},
            {'clave': 'pique_anulado', 'nombre': 'Piqué anulado',
             'texto': 'Agujas alternas no suben y el hilo pasa de largo por detrás.'},
            {'clave': 'crepe_cargas', 'nombre': 'Crepé cargas',
             'texto': 'Cargas repartidas sin orden aparente: superficie granulada.'},
            {'clave': 'crepe_anulado', 'nombre': 'Crepé anulado',
             'texto': 'Anulados repartidos sin orden aparente: grano más cerrado y firme.'},
            {'clave': 'french_terry', 'nombre': 'French terry',
             'texto': 'Un hilo de felpa se amarra con cargas y flota por el revés entre pasadas de jersey.'},
            {'clave': 'flece', 'nombre': 'Flece',
             'texto': 'Tres hilos: base, felpa y amarre. La felpa del revés se percha en el acabado.'},
            {'clave': 'desagujado', 'nombre': 'Desagujado',
             'texto': 'Se retiran agujas a intervalos: queda una canaleta a lo largo de la tela.'},
            {'clave': 'jacquard', 'nombre': 'Jacquard',
             'texto': 'Cada aguja elige qué color teje en cada pasada: el dibujo sale del tejido.'},
        ],
    },
    {
        'clave': 'doble_fontura',
        'nombre': 'Doble fontura',
        'texto': 'Dos juegos de agujas, cilindro y plato. La tela tiene mallas por las dos caras.',
        'dibujos': [
            {'clave': 'rib', 'nombre': 'Rib',
             'texto': 'Las agujas del cilindro y del plato tejen alternadas. Mucha elasticidad a lo ancho.'},
            {'clave': 'interlock', 'nombre': 'Interlock',
             'texto': 'Dos rib cruzados, uno por pasada. Tela lisa por las dos caras y estable.'},
            {'clave': 'punto_roma', 'nombre': 'Punto de Roma',
             'texto': 'Dos pasadas de interlock, una solo de cilindro y una solo de plato. Tela con cuerpo.'},
            {'clave': 'cardigan', 'nombre': 'Cardigan',
             'texto': 'Una fontura hace malla mientras la otra carga, y se alternan. Tela gruesa y esponjosa.'},
        ],
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
# Teléfono de la oficina de ventas (firmas de correo de Ventas, noviembre 2026).
# La ficha de la compañía en Odoo trae 5555708874, que no aparece en ningún
# correo de Quimibond; mientras no se corrija ahí, el sitio usa este.
TELEFONO = '55 5570 5874'

MAQUINARIA = [
    {
        'clave': 'tejido',
        'nombre': 'Tejido circular',
        'icono': 'fa-circle-o-notch',
        'tipo': 'tejido_circular',
        'cantidad': 0,
        'unidad': 'Máquinas circulares',
        'texto': 'Máquinas circulares de una fontura y de doble fontura.',
        'datos': [
            ('Tejidos', 'Una fontura y doble fontura'),
            ('Hilos que tejemos', 'Poliéster, poliéster reciclado, poliamida (nailon) y algodón con poliéster'),
        ],
    },
    {
        'clave': 'tintoreria',
        'nombre': 'Tintorería',
        'icono': 'fa-tint',
        'tipo': 'tintoreria',
        'cantidad': 0,
        'unidad': 'Jets de teñido',
        'texto': 'Teñido en jets, donde la tela circula por el baño hasta que el color queda parejo.',
        'maquinas': (5, 'jet de teñido', 'jets de teñido'),
        'datos': [
            ('Fibras que teñimos', 'Poliéster, poliéster reciclado, poliamida (nailon) y mezclas de poliéster con algodón'),
            ('Color', 'De acuerdo a su especificación'),
        ],
    },
    {
        'clave': 'acabado',
        'nombre': 'Ramas y acabado',
        'icono': 'fa-fire',
        'tipo': 'acabado',
        'cantidad': 0,
        'unidad': 'Ramas',
        'texto': 'Termofijado del producto de acuerdo a su especificación, al ancho pedido.',
        'maquinas': (3, 'rama', 'ramas'),
        'datos': [
            ('Telas que acabamos', 'Poliéster, poliéster reciclado, poliamida (nailon) y mezclas de poliéster con algodón'),
            ('Proceso', 'Secado y termofijado'),
            ('Acabados especiales', 'Retardante a la flama y repelentes, entre otros'),
        ],
    },
    {
        'clave': 'no_tejidos',
        'nombre': 'No tejidos y entretelas',
        'icono': 'fa-th',
        'cantidad': 0,
        'unidad': 'Línea completa',
        'texto': 'Línea completa de no tejidos, de la fibra al rollo terminado.',
        'datos': [
            ('Productos', 'Entretelas fusionables, no tejido tramado y scrims no tejidos'),
            ('Fibras', 'Poliéster, poliéster reciclado, polipropileno y poliéster con poliamida'),
            ('Presentación', 'En rollo, cortado o perforado'),
        ],
    },
    {
        'clave': 'inspeccion',
        'nombre': 'Inspección y laboratorio',
        'icono': 'fa-search',
        'tipo': 'inspeccion',
        'cantidad': 0,
        'unidad': 'Máquinas de inspección',
        'texto': 'Inspección al 100 %, rollo por rollo, y laboratorio en planta.',
        'datos': [
            ('Inspección', 'Al 100 %, rollo por rollo'),
            ('Laboratorio', 'Pruebas de acuerdo a su especificación y pruebas especiales'),
            ('Pruebas en planta', 'Peso, espesor, resistencia, elongación, estabilidad dimensional, '
                                  'flamabilidad y olor, entre otras'),
            ('Trazabilidad', 'Del lote de teñido al rollo terminado'),
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
    # Hilos: (categoría de «Materia Prima / Hilo» en Odoo, nombre en el sitio).
    # Con las categorías en la base, solo salen los hilos que tienen productos
    # (controllers/planta.py); el elastano va aparte, como acompañante.
    'hilos': [
        ('Poliéster', 'Poliéster'),
        ('PET', 'Poliéster reciclado'),
        ('Mezcla (PES/CO)', 'Mezcla poliéster y algodón'),
        ('Algodón', 'Algodón'),
        ('Nylon', 'Nylon'),
        ('Viscosa', 'Viscosa'),
        ('Bamboo', 'Bambú'),
    ],
    'hilo_elastano': 'Elastano',
    # Presentaciones del filamento de poliéster que se compran hoy.
    'hilo_tipos': ['Texturizado', 'Stretch', 'Microfibra'],
    # Fibras cortadas de la línea de no tejidos («Materia Prima / Fibra»).
    'fibras': ['Poliéster', 'Poliamida', 'Polipropileno', 'Viscosa'],
    'colores': [
        ('Blanco', '#f5f5f2'), ('Crudo', '#e9e2d0'), ('Beige', '#d6c4a4'), ('Gris', '#9a9aa2'),
        ('Negro', '#1b1b1f'), ('Azul marino', '#1f2a4a'), ('Azul', '#2f54c8'), ('Vino', '#6b1f2e'),
        ('Verde', '#1f5c48'), ('Naranja', '#e8590c'),
    ],
    'tactos': ['Suave', 'Semirrígido', 'Rígido'],
    # Aplicaciones por mercado: de lo general (el mercado) a lo particular (la
    # parte del vehículo, la prenda, el blanco). Cada una propone una tela de
    # partida que la persona ajusta. No son productos del catálogo ni promesas.
    'aplicaciones': [
        # Scrim: de los productos de Odoo (SCRIM 55, SCRIM BLACK 70 g/m², SCRIM NYLON;
        # categoría «Tejido Circular / Industrial», la de mayor venta). Dirección
        # (2026-11-19): es el producto automotriz y va al frente.
        {'nombre': 'Scrim', 'mercado': 'Automotriz',
         'texto': 'Tela ligera de tejido circular, de 55 a 70 g/m², en natural o negro.',
         'tela': {'construccion': 'una_fontura', 'galga': 28, 'hilo': 'Poliéster', 'hiloTipo': 'Texturizado',
                  'peso': 70, 'color': '#1b1b1f', 'tacto': 'Suave'}},
        {'nombre': 'Asientos', 'mercado': 'Automotriz',
         'texto': 'Tela de vestidura con cuerpo, hecha para el uso diario del asiento.',
         'tela': {'construccion': 'doble_fontura', 'galga': 18, 'hilo': 'Poliéster', 'hiloTipo': 'Texturizado',
                  'peso': 280, 'color': '#9a9aa2', 'tacto': 'Semirrígido'}},
        {'nombre': 'Toldo', 'mercado': 'Automotriz',
         'texto': 'Tela ligera y suave para el techo interior.',
         'tela': {'construccion': 'una_fontura', 'galga': 28, 'hilo': 'Poliéster', 'hiloTipo': 'Texturizado',
                  'peso': 160, 'color': '#d6c4a4', 'tacto': 'Suave'}},
        {'nombre': 'Pilares', 'mercado': 'Automotriz',
         'texto': 'La tela del toldo, continuada en los pilares.',
         'tela': {'construccion': 'una_fontura', 'galga': 28, 'hilo': 'Poliéster', 'hiloTipo': 'Texturizado',
                  'peso': 180, 'color': '#d6c4a4', 'tacto': 'Suave'}},
        {'nombre': 'Paneles de puerta', 'mercado': 'Automotriz',
         'texto': 'Tela con cuerpo para el inserto del panel de la puerta.',
         'tela': {'construccion': 'doble_fontura', 'galga': 24, 'hilo': 'Poliéster', 'hiloTipo': 'Texturizado',
                  'peso': 240, 'color': '#1b1b1f', 'tacto': 'Semirrígido'}},
        {'nombre': 'Colchón', 'mercado': 'Blancos',
         'texto': 'Tela de punto con cuerpo para la funda del colchón.',
         'tela': {'construccion': 'doble_fontura', 'galga': 18, 'hilo': 'Poliéster', 'hiloTipo': 'Texturizado',
                  'peso': 220, 'color': '#f5f5f2', 'tacto': 'Suave'}},
        {'nombre': 'Almohada', 'mercado': 'Blancos',
         'texto': 'Tela ligera y suave para la funda de la almohada.',
         'tela': {'construccion': 'una_fontura', 'galga': 24, 'hilo': 'Mezcla poliéster y algodón',
                  'peso': 130, 'color': '#f5f5f2', 'tacto': 'Suave'}},
        {'nombre': 'Protector de colchón', 'mercado': 'Blancos',
         'texto': 'Tela de punto ligera que cubre y protege el colchón.',
         'tela': {'construccion': 'una_fontura', 'galga': 24, 'hilo': 'Poliéster', 'hiloTipo': 'Texturizado',
                  'peso': 140, 'color': '#f5f5f2', 'tacto': 'Suave'}},
        {'nombre': 'Playera', 'mercado': 'Confección',
         'texto': 'Tela de punto ligera para playeras y prendas casuales.',
         'tela': {'construccion': 'una_fontura', 'galga': 24, 'hilo': 'Mezcla poliéster y algodón',
                  'peso': 160, 'color': '#f5f5f2', 'tacto': 'Suave'}},
        {'nombre': 'Ropa deportiva', 'mercado': 'Confección',
         'texto': 'Tela de punto fina y ligera para prendas deportivas.',
         'tela': {'construccion': 'una_fontura', 'galga': 28, 'hilo': 'Poliéster', 'hiloTipo': 'Microfibra',
                  'peso': 150, 'color': '#1f2a4a', 'tacto': 'Suave'}},
        {'nombre': 'Uniforme', 'mercado': 'Confección',
         'texto': 'Tela de punto con cuerpo para prendas de uso continuo.',
         'tela': {'construccion': 'doble_fontura', 'galga': 24, 'hilo': 'Poliéster', 'hiloTipo': 'Texturizado',
                  'peso': 200, 'color': '#1f2a4a', 'tacto': 'Suave'}},
        {'nombre': 'Entretela para cuello y puño', 'mercado': 'Confección',
         'texto': 'Entretela fusionable que da firmeza al cuello y al puño de la camisa.',
         'tela': {'construccion': 'no_tejido', 'fibra': 'Poliéster', 'peso': 50, 'color': '#f5f5f2',
                  'tacto': 'Rígido', 'fusionable': True}},
        {'nombre': 'Entretela para frente de saco', 'mercado': 'Confección',
         'texto': 'Entretela tramada que da forma y recuperación al frente.',
         'tela': {'construccion': 'tramado', 'fibra': 'Poliéster', 'peso': 90,
                  'color': '#1b1b1f', 'tacto': 'Semirrígido', 'fusionable': True}},
    ],
    # Requisitos que el cliente puede pedir; Ventas e Ingeniería los evalúan.
    'requisitos': [
        'Solidez del color', 'Resistencia al pilling', 'Resistencia a la abrasión', 'Estabilidad dimensional',
        'Flamabilidad', 'Lavado en agua', 'Lavado en seco',
    ],
    'unidades': ['metros al mes', 'kilos al mes'],
    'presentaciones': ['Tela abierta', 'Tela tubular'],
}

MERCADOS = [
    {
        'clave': 'automotriz',
        'frase': 'Scrims y telas para el interior del vehículo.',
        'nombre': 'Automotriz',
        'titulo': 'Scrims y textiles técnicos para la industria automotriz',
        'resumen': 'Desde 2014 abastecemos al mercado automotriz de México, Estados Unidos y Canadá '
                   'con scrims desarrollados para sus especificaciones. El scrim es el soporte textil que se '
                   'lamina con la espuma y con la tela, el vinil o la piel de las vestiduras.',
        'puntos': [
            'Scrims para vestiduras de asientos, descansabrazos y otros componentes del interior',
            'Scrims de tejido circular y no tejidos, en poliéster y nailon, en natural y negro',
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
        'frase': 'Telas para colchones, almohadas y blancos para el hogar.',
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
        'frase': 'Entretelas y telas de punto para prendas.',
        'nombre': 'Confección',
        'titulo': 'Entretelas para la confección',
        'resumen': 'Entretelas fusionables tejidas y no tejidas para camisería, uniformes, '
                   'playeras, ropa deportiva, cuellos, puños y frentes de saco.',
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
        'texto': 'Teñido en jets: la tela circula por el baño hasta que el color queda parejo '
                 'en todo el lote.',
        'icono': 'fa-tint',
    },
    {
        'paso': '03',
        'nombre': 'Rama y acabado',
        'texto': 'Termofijado del producto de acuerdo a su especificación y acabados especiales, '
                 'como retardante a la flama y repelentes.',
        'icono': 'fa-fire',
    },
    {
        'paso': '04',
        'nombre': 'Inspección',
        'texto': 'Inspección al 100 %, rollo por rollo, con pruebas de laboratorio de acuerdo a '
                 'su especificación.',
        'icono': 'fa-search',
    },
    {
        'paso': '05',
        'nombre': 'Laboratorio y embarque',
        'texto': 'Pruebas de laboratorio especiales cuando su especificación las pide, liberación '
                 'y embarque con la trazabilidad del lote.',
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
    {'anio': '2020', 'titulo': 'Energía solar',
     'texto': 'Primeros 1,315 paneles solares en la planta; en 2024 se sumaron 766 más.'},
    {'anio': '2023', 'titulo': 'ISO 14001',
     'texto': 'Certificación ISO 14001 del sistema de gestión ambiental.'},
    {'anio': '2025', 'titulo': 'Planta tratadora de agua',
     'texto': 'Entra en operación la planta tratadora de agua.'},
    {'anio': '2026', 'titulo': 'ISO 45001',
     'texto': 'Certificación ISO 45001 de seguridad y salud en el trabajo.'},
]

# Misión, visión, valores y razones: de la propuesta de Calidad para la página
# («PROPUESTA PAGINA WEB», 2026-11-19). Misión y visión van textuales.
MISION = ('Diseñar, producir y suministrar textiles técnicos de alto desempeño que excedan las expectativas '
          'de nuestros clientes industriales, automotrices y de otros sectores, integrando la innovación, la '
          'sostenibilidad y la seguridad y salud en el trabajo en cada etapa de nuestros procesos.')
VISION = ('Para el año 2030, Quimibond será reconocida como la empresa líder en soluciones textiles técnicas '
          'innovadoras y sostenibles, especialmente en la industria automotriz. Destacará por su agilidad para '
          'personalizar productos de alta calidad, su compromiso con la sostenibilidad y una cultura '
          'organizacional sólida que promueve la seguridad y la salud en el trabajo como un valor fundamental.')
VALORES = [
    {'icono': 'fa-check-circle-o', 'titulo': 'Calidad y excelencia', 'texto': 'En cada proceso y en cada detalle.'},
    {'icono': 'fa-lightbulb-o', 'titulo': 'Innovación', 'texto': 'Buscamos siempre mejores soluciones.'},
    {'icono': 'fa-leaf', 'titulo': 'Sostenibilidad', 'texto': 'Cuidamos el presente y el futuro.'},
    {'icono': 'fa-balance-scale', 'titulo': 'Integridad', 'texto': 'Actuamos con honestidad y transparencia.'},
    {'icono': 'fa-users', 'titulo': 'Respeto', 'texto': 'Valoramos a las personas y al entorno.'},
    {'icono': 'fa-handshake-o', 'titulo': 'Enfoque al cliente', 'texto': 'Su éxito es nuestro propósito.'},
]
RAZONES = [
    {'num': '01', 'titulo': 'Experiencia y especialización',
     'texto': 'Fabricamos telas desde 1992. Nuestro equipo técnico desarrolla cada tela a la medida de su aplicación.'},
    {'num': '02', 'titulo': 'Calidad y confianza',
     'texto': 'Sistema de gestión certificado en ISO 9001, ISO 14001 e ISO 45001, '
              'y trazabilidad de cada lote.'},
    {'num': '03', 'titulo': 'Compromiso con el futuro',
     'texto': 'Innovación, sustentabilidad y seguridad de nuestra gente en cada etapa del proceso.'},
]

# Cifras de la presentación de empresa de octubre de 2026 (Ventas): 1,315
# paneles en 2020 más 766 en 2024; 3.7 GWh generados de 2021 a octubre de 2026;
# planta tratadora de agua en operación desde 2025.
SUSTENTABILIDAD_CIFRAS = [
    {'valor': '2,081', 'etiqueta': 'Paneles solares en la planta'},
    {'valor': '3.7 GWh', 'etiqueta': 'Energía limpia generada desde 2021'},
    {'valor': '2025', 'etiqueta': 'Planta tratadora de agua en operación'},
]

SUSTENTABILIDAD = [
    {'icono': 'fa-sun-o', 'titulo': 'Energía solar',
     'texto': '2,081 paneles solares en el techo de la planta, que generan parte de la energía con la que producimos.'},
    {'icono': 'fa-tint', 'titulo': 'Tratamiento de agua',
     'texto': 'Planta tratadora de agua propia, en operación desde 2025, y captación de agua de lluvia.'},
    {'icono': 'fa-recycle', 'titulo': 'Reciclaje', 'texto': 'Separación de residuos y reciclaje en todas las áreas.'},
]


def aplicaciones(nombre_mercado):
    """Aplicaciones de un mercado, cada una con la tela de partida que propone
    el diseñador: de lo general (el mercado) a lo particular (la tela)."""
    tejidos = {t['clave']: t for t in TEJIDOS}
    nombres_familia = {f['clave']: f['nombre'] for f in FAMILIAS}
    lista = []
    for ap in DISENADOR['aplicaciones']:
        if ap['mercado'] != nombre_mercado:
            continue
        tejido = tejidos[ap['tela']['construccion']]
        lista.append({
            'nombre': ap['nombre'],
            'texto': ap['texto'],
            'construccion': tejido['clave'],
            'tela': tejido['nombre'],
            'familia': nombres_familia[tejido['familia']],
            'enlace': '/disenar?ap=%s' % quote(ap['nombre']),
        })
    return lista


def mercados():
    """Los mercados con sus aplicaciones."""
    return [dict(m, aplicaciones=aplicaciones(m['nombre'])) for m in MERCADOS]


def mercado(clave):
    for m in mercados():
        if m['clave'] == clave:
            return m
    return None
