# -*- coding: utf-8 -*-
from odoo import api, fields, models

# 2.29.0: cada tolerancia de medida tiene su valor hacia arriba y hacia abajo, y cada uno vale lo que dice (antes solo
# el peso; un «−» en 0 significaba «igual que el +»). Así cabe +0 / −5 y también +5 / −0.
TOL_PLUS_HELP = ('Lo más que puede pasar del valor, en la misma unidad. Se captura junto al valor en la ficha de '
                 'acabado → «Tela acabada» → Especificación (o el importador de Excel). Para ± escriba el mismo '
                 'número en las dos tolerancias.')
TOL_MINUS_HELP = ('Lo más que puede faltar del valor, en la misma unidad. Se captura junto al valor en la ficha de '
                  'acabado → «Tela acabada» → Especificación (o el importador de Excel). Puede ser distinta de la de '
                  'arriba, también 0 (ej. +5 / −0).')
UNIDAD_TEXTO_HELP = ('La unidad del valor (no solo de la tolerancia) escrita como texto. Se escribe sola al elegir la '
                     'unidad del catálogo junto al valor, o la trae el importador de Excel; la leen el SGI y el '
                     'importador.')


class FichaTecnicaAcabado(models.Model):
    _name = 'ficha.tecnica.acabado'
    _description = 'Ficha Técnica de Acabado'
    _rec_name = 'articulo'
    _order = 'articulo'

    # ------------------------------------------------------------------
    # Encabezado
    # ------------------------------------------------------------------
    articulo = fields.Char(
        string='Artículo (acabado)', required=True, index=True,
        help='Código del artículo de tela acabada, ej. WJ044Q22HNT235-NEGRO. Se escribe en el encabezado de la ficha '
             '(o lo ponen el alta de C1 en el SGI, «Crear la ficha de la tela» del producto y el importador de '
             'Excel).')
    revision = fields.Char(
        string='Revisión', default='0',
        help='Número de revisión del documento de la ficha (0, 1, 2…). Se captura en el encabezado. No es la revisión '
             'nocturna de las listas de materiales.')
    active = fields.Boolean(
        default=True,
        help='Desmarcado: la ficha está archivada. La revisión de listas deja de revisarla y el producto deja de '
             'mostrarla.')
    fecha_elaboracion = fields.Date(
        string='Fecha de elaboración',
        help='Fecha en que se elaboró esta revisión de la ficha. Se captura en el encabezado.')

    tejido_id = fields.Many2one(
        'ficha.tecnica.tejido', string='Ficha Técnica de Tejido (base)',
        required=True, ondelete='restrict',
        help='Ficha de tejido del crudo sobre el que se construye este acabado («1. Ficha de tejido» del encabezado); '
             'varias fichas de acabado (ej. distintos colores) la comparten. De ella salen la ruta de la familia y el '
             'rendimiento de tejido que decide la banda del jet.')
    product_acabado_id = fields.Many2one(
        'product.product', string='Producto — Tela acabada', required=True,
        help='La tela acabada en metros (código sin «I», ej. WJ045Q22JNT160): la que entrega la Clasificación final '
             'con los metros medidos y la que se vende en metros. Se elige en el encabezado («4. Tela acabada (m)»); '
             'un producto solo puede tener una ficha.')

    # ------------------------------------------------------------------
    # Datos de Acabado
    # ------------------------------------------------------------------
    rendimiento_tela_acabada = fields.Float(
        string='Rendimiento de producción (m/kg)', digits=(12, 4),
        help='Metros por kg que entra a acabado, con la merma (orilla, muestras y merma natural) sumada. Desde 2.30.0 '
             '(Ingeniería, 10-nov-2026) sale del peso de la ficha, el ancho de rama y la merma de la tela en la rama: '
             '1000 ÷ (g/m² × ancho m) × (1 − merma); la noche lo pasa aquí (o el botón «Pasar el rendimiento de '
             'producción a la ficha»). Sin esos datos se queda el capturado a mano o del Excel. Lo usan la conversión '
             'kg → m de la lista en metros (se pone al día al cambiar), los tiempos de rama e inspección y el costeo. '
             'No es el rendimiento de tejido del crudo.')

    peso_acabado = fields.Float(
        string='Peso',
        help='Peso de especificación de la tela acabada (normalmente g/m²). Se captura en la ficha → «Tela acabada» → '
             'Especificación (o el importador de Excel). Con el ancho bajando de rama da el rendimiento teórico '
             '(solo si su unidad es de gramaje); también lo leen el costeo y el SGI.')
    peso_acabado_tol = fields.Float(
        string='Tolerancia peso (+)',
        help=TOL_PLUS_HELP)
    peso_acabado_tol_minus = fields.Float(
        string='Tolerancia peso (−)',
        help=TOL_MINUS_HELP)
    peso_acabado_tol_unit = fields.Char(
        string='Unidad — Peso (texto)', default='g/m2',
        help=UNIDAD_TEXTO_HELP + ' El rendimiento teórico solo toma el peso si esta unidad es de gramaje (g/m²).')

    ancho_acabado = fields.Float(
        string='Ancho de venta (especificación)',
        help='El ancho de especificación, el mismo del código (…160 = 160 cm). No es el ancho de crudo relajado, el '
             'de rollo ni el que baja de rama. Se captura en la ficha → «Tela acabada» → Especificación (o el '
             'importador); el rendimiento teórico lo usa solo si falta el ancho bajando de rama.')
    ancho_acabado_tol = fields.Float(
        string='Tolerancia ancho (+)',
        help=TOL_PLUS_HELP)
    ancho_acabado_tol_minus = fields.Float(
        string='Tolerancia ancho (−)',
        help=TOL_MINUS_HELP)
    ancho_acabado_tol_unit = fields.Char(string='Unidad — Ancho (texto)', default='cm', help=UNIDAD_TEXTO_HELP)

    encogimiento_largo = fields.Float(
        string='Encogimiento a lo largo',
        help='Encogimiento de la tela acabada a lo largo (normalmente en %). Se captura en la ficha → «Tela acabada» '
             '→ Encogimiento y elongación (o el importador). Si pasa de 5 % (en valor absoluto) la ficha lo marca '
             'fuera de norma.')
    encogimiento_largo_tol = fields.Float(
        string='Tolerancia encogimiento largo (+)',
        help=TOL_PLUS_HELP)
    encogimiento_largo_tol_minus = fields.Float(
        string='Tolerancia encogimiento largo (−)',
        help=TOL_MINUS_HELP)
    encogimiento_largo_tol_unit = fields.Char(string='Unidad — Encogimiento largo (texto)', default='%',
                                              help=UNIDAD_TEXTO_HELP)

    encogimiento_ancho = fields.Float(
        string='Encogimiento a lo ancho',
        help='Encogimiento de la tela acabada a lo ancho (normalmente en %). Se captura en la ficha → «Tela acabada» '
             '→ Encogimiento y elongación (o el importador). Si pasa de 5 % (en valor absoluto) la ficha lo marca '
             'fuera de norma.')
    encogimiento_ancho_tol = fields.Float(
        string='Tolerancia encogimiento ancho (+)',
        help=TOL_PLUS_HELP)
    encogimiento_ancho_tol_minus = fields.Float(
        string='Tolerancia encogimiento ancho (−)',
        help=TOL_MINUS_HELP)
    encogimiento_ancho_tol_unit = fields.Char(string='Unidad — Encogimiento ancho (texto)', default='%',
                                              help=UNIDAD_TEXTO_HELP)

    # 2.16.2: tres decimales, como el espesor de la ficha de tejido.
    espesor_acabado = fields.Float(
        string='Espesor', digits=(16, 3),
        help='Espesor de especificación de la tela acabada (normalmente en pulgadas). Se captura en la ficha → «Tela '
             'acabada» → Especificación (o el importador). Ningún cálculo lo usa.')
    espesor_acabado_tol = fields.Float(
        string='Tolerancia espesor (+)', digits=(16, 3),
        help=TOL_PLUS_HELP)
    espesor_acabado_tol_minus = fields.Float(
        string='Tolerancia espesor (−)', digits=(16, 3),
        help=TOL_MINUS_HELP)
    espesor_acabado_tol_unit = fields.Char(string='Unidad — Espesor (texto)', default='in', help=UNIDAD_TEXTO_HELP)

    elongacion_largo_acabado = fields.Float(
        string='Elongación largo',
        help='Elongación de la tela acabada a lo largo (normalmente en %). Se captura en la ficha → «Tela acabada» → '
             'Encogimiento y elongación (o el importador). Ningún cálculo la usa.')
    elongacion_largo_acabado_tol = fields.Float(
        string='Tolerancia elongación largo (+)',
        help=TOL_PLUS_HELP)
    elongacion_largo_acabado_tol_minus = fields.Float(
        string='Tolerancia elongación largo (−)',
        help=TOL_MINUS_HELP)
    elongacion_largo_acabado_tol_unit = fields.Char(string='Unidad — Elongación largo (texto)', default='%',
                                                    help=UNIDAD_TEXTO_HELP)

    elongacion_ancho_acabado = fields.Float(
        string='Elongación ancho',
        help='Elongación de la tela acabada a lo ancho (normalmente en %). Se captura en la ficha → «Tela acabada» → '
             'Encogimiento y elongación (o el importador). Ningún cálculo la usa.')
    elongacion_ancho_acabado_tol = fields.Float(
        string='Tolerancia elongación ancho (+)',
        help=TOL_PLUS_HELP)
    elongacion_ancho_acabado_tol_minus = fields.Float(
        string='Tolerancia elongación ancho (−)',
        help=TOL_MINUS_HELP)
    elongacion_ancho_acabado_tol_unit = fields.Char(string='Unidad — Elongación ancho (texto)', default='%',
                                                    help=UNIDAD_TEXTO_HELP)

    encogimiento_largo_dentro_norma = fields.Boolean(
        string='Encogimiento largo dentro de norma (≤5%)',
        compute='_compute_dentro_norma', store=True,
        help='Se calcula: marcado si el encogimiento a lo largo, en valor absoluto, es de 5 % o menos (norma fija). '
             'Mira el valor del encogimiento, no sus tolerancias. Filtro «Encogimiento fuera de norma».')
    encogimiento_ancho_dentro_norma = fields.Boolean(
        string='Encogimiento ancho dentro de norma (≤5%)',
        compute='_compute_dentro_norma', store=True,
        help='Se calcula: marcado si el encogimiento a lo ancho, en valor absoluto, es de 5 % o menos (norma fija). '
             'Mira el valor del encogimiento, no sus tolerancias. Filtro «Encogimiento fuera de norma».')

    @api.depends('encogimiento_largo', 'encogimiento_ancho')
    def _compute_dentro_norma(self):
        for rec in self:
            rec.encogimiento_largo_dentro_norma = abs(rec.encogimiento_largo) <= 5.0
            rec.encogimiento_ancho_dentro_norma = abs(rec.encogimiento_ancho) <= 5.0

    notas = fields.Text(string='Notas', help='Texto libre. Pestaña «Notas» de la ficha de acabado (o el importador).')

    _product_acabado_uniq = models.Constraint(
        'unique(product_acabado_id)',
        'Ya existe una ficha técnica de acabado para ese producto.',
    )
