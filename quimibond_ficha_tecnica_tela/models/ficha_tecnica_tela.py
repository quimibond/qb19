# -*- coding: utf-8 -*-
from odoo import api, fields, models


class FichaTecnicaTela(models.Model):
    _name = 'ficha.tecnica.tela'
    _description = 'Ficha Técnica de Tela (Tejido + Acabado)'
    _rec_name = 'articulo'
    _order = 'articulo'

    # ------------------------------------------------------------------
    # Encabezado
    # ------------------------------------------------------------------
    articulo = fields.Char(string='Artículo', required=True, index=True,
                            help='Código del artículo, ej. WJ044Q22HNT235')
    revision = fields.Char(string='Revisión', default='0')
    active = fields.Boolean(default=True)
    fecha_elaboracion = fields.Date(string='Fecha de elaboración')
    jefe_manufactura = fields.Char(string='Jefe de manufactura')
    auxiliar_procesos = fields.Char(string='Auxiliar de procesos')

    # Vínculo con productos (arquitectura de 2 productos: Tela en Proceso / Tela Acabada)
    product_proceso_id = fields.Many2one(
        'product.product', string='Producto — Tela en Proceso (kg)',
        help='Producto semi-terminado que recorre Preparado/Devanado, Tintorería, Abridora y Rama.')
    product_acabado_id = fields.Many2many(
        'product.product', string='Productos — Tela Acabada (m)',
        help='Productos terminados que comparten esta ficha técnica '
             '(ej. distintos colores de la misma construcción de tela). '
             'Resultado de Inspección y Empaque + Control de Calidad.')

    # ------------------------------------------------------------------
    # Sección TEJIDO — datos de máquina
    # ------------------------------------------------------------------
    maquina_tejido = fields.Char(string='Máquina')
    marca_maquina = fields.Char(string='Marca de máquina')
    galga = fields.Float(string='Galga')
    diametro = fields.Char(string='Diámetro')
    no_agujas = fields.Integer(string='No. de agujas')
    no_alimentadores = fields.Integer(string='No. de alimentadores')
    velocidad = fields.Float(string='Velocidad (rpm/min)')
    vueltas_por_rollo = fields.Integer(string='Vueltas por rollo')

    # Hilos
    hilo_line_ids = fields.One2many('ficha.tecnica.tela.hilo', 'ficha_id',
                                     string='Disposición de hilos')

    # Especificaciones de tejido
    longitud_malla = fields.Float(string='Longitud de malla (cm)')
    longitud_malla_tol = fields.Float(string='Tolerancia longitud de malla (cm)')
    consumo_cm_vta = fields.Float(string='Consumo (cm/vta)')
    consumo_cm_vta_tol = fields.Float(string='Tolerancia consumo (cm)')
    polea_alimentacion = fields.Char(string='Polea de alimentación')
    tension = fields.Char(string='Tensión')
    punto_cilindro = fields.Char(string='Punto cilindro')
    punto_plato = fields.Char(string='Punto plato')
    altura_plato = fields.Char(string='Altura plato')
    ancho_bastidor = fields.Float(string='Ancho de bastidor (cm)')
    ancho_bastidor_tol = fields.Float(string='Tolerancia ancho de bastidor (cm)')
    estiraje = fields.Char(string='Estiraje')
    ancho_rollo = fields.Float(string='Ancho de rollo (cm)')
    ancho_rollo_tol = fields.Float(string='Tolerancia ancho de rollo (cm)')
    peso_promedio_rollo = fields.Float(string='Peso promedio de rollo (kg)')
    peso_promedio_rollo_tol = fields.Float(string='Tolerancia peso de rollo (kg)')

    # Datos de tela acondicionada (post-tejido, pre-acabado)
    peso_acondicionado = fields.Float(string='Peso (g/m²)')
    ancho_acondicionado = fields.Float(string='Ancho')
    espesor_acondicionado = fields.Float(string='Espesor')
    columnas = fields.Integer(string='Columnas')
    mallas = fields.Integer(string='Mallas')
    elongacion_carga_largo = fields.Float(string='Elongación bajo carga — largo (%)')
    elongacion_carga_ancho = fields.Float(string='Elongación bajo carga — ancho (%)')

    # ------------------------------------------------------------------
    # Sección ACABADO
    # ------------------------------------------------------------------
    rendimiento_tela_acabada = fields.Float(
        string='Rendimiento de tela acabada (mts/kg)', digits=(12, 4),
        help='Valor teórico fijo. Se compara contra el rendimiento real '
             'calculado en el pesaje/registro final de cada rollo.')
    peso_acabado = fields.Float(string='Peso acabado (g/m²)')
    ancho_acabado = fields.Float(string='Ancho acabado (cm)')
    espesor_acabado = fields.Float(string='Espesor acabado (mm/plg)')
    encogimiento_largo = fields.Float(string='Encogimiento a lo largo (%)')
    encogimiento_ancho = fields.Float(string='Encogimiento a lo ancho (%)')
    elongacion_largo_acabado = fields.Float(string='Elongación largo (%)')
    elongacion_ancho_acabado = fields.Float(string='Elongación ancho (%)')

    encogimiento_largo_dentro_norma = fields.Boolean(
        string='Encogimiento largo dentro de norma (≤5%)', compute='_compute_dentro_norma', store=True)
    encogimiento_ancho_dentro_norma = fields.Boolean(
        string='Encogimiento ancho dentro de norma (≤5%)', compute='_compute_dentro_norma', store=True)

    @api.depends('encogimiento_largo', 'encogimiento_ancho')
    def _compute_dentro_norma(self):
        for rec in self:
            rec.encogimiento_largo_dentro_norma = abs(rec.encogimiento_largo) <= 5.0
            rec.encogimiento_ancho_dentro_norma = abs(rec.encogimiento_ancho) <= 5.0

    notas = fields.Text(string='Notas')

    _sql_constraints = [
        ('articulo_revision_uniq', 'unique(articulo, revision)',
         'Ya existe una ficha técnica con ese artículo y esa revisión.'),
    ]


class FichaTecnicaTelaHilo(models.Model):
    _name = 'ficha.tecnica.tela.hilo'
    _description = 'Disposición de hilo — Ficha Técnica de Tela'
    _order = 'numero'

    ficha_id = fields.Many2one('ficha.tecnica.tela', string='Ficha técnica',
                                required=True, ondelete='cascade')
    numero = fields.Integer(string='No.')
    tipo_hilo = fields.Char(string='Tipo de hilo')
    titulo_hilo = fields.Char(string='Título de hilo')
    torsion = fields.Char(string='Torsión')
    porcentaje = fields.Float(string='% de hilo')
    lote = fields.Char(string='Lote')
    proveedor = fields.Char(string='Proveedor')
