# -*- coding: utf-8 -*-
from odoo import fields, models


class FichaTecnicaTejido(models.Model):
    _name = 'ficha.tecnica.tejido'
    _description = 'Ficha Técnica de Tejido'
    _rec_name = 'articulo'
    _order = 'articulo'

    # ------------------------------------------------------------------
    # Encabezado
    # ------------------------------------------------------------------
    articulo = fields.Char(string='Artículo', required=True, index=True,
                            help='Código del artículo de tejido, ej. WJ044Q22HNT235')
    revision = fields.Char(string='Revisión', default='0')
    active = fields.Boolean(default=True)
    fecha_elaboracion = fields.Date(string='Fecha de elaboración')
    jefe_manufactura = fields.Char(string='Jefe de manufactura')
    auxiliar_procesos = fields.Char(string='Auxiliar de procesos')

    # Vínculo con el producto semi-terminado (arquitectura de 2 productos)
    product_proceso_id = fields.Many2one(
        'product.product', string='Producto — Tela en Proceso (kg)',
        help='Producto semi-terminado que recorre Preparado/Devanado, Tintorería, Abridora y Rama.')

    # Fichas de acabado que usan esta ficha de tejido como base
    ficha_acabado_ids = fields.One2many(
        'ficha.tecnica.acabado', 'tejido_id',
        string='Fichas de Acabado basadas en este tejido',
        help='Productos de tela acabada (uno o varios colores/variantes) '
             'que se construyen a partir de esta ficha de tejido.')
    ficha_acabado_count = fields.Integer(
        string='No. de fichas de acabado', compute='_compute_ficha_acabado_count')

    def _compute_ficha_acabado_count(self):
        for rec in self:
            rec.ficha_acabado_count = len(rec.ficha_acabado_ids)

    # ------------------------------------------------------------------
    # Datos de máquina
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
    hilo_line_ids = fields.One2many('ficha.tecnica.tejido.hilo', 'ficha_id',
                                     string='Disposición de hilos')

    # ------------------------------------------------------------------
    # Especificaciones de tejido — Tabla 1: por polea/hilo (Polea 1 = Hilo
    # SET, Polea 2 = Hilo Stretch), con tolerancia compartida
    # ------------------------------------------------------------------
    longitud_malla_polea1 = fields.Float(string='Longitud de malla — Polea 1')
    longitud_malla_polea2 = fields.Float(string='Longitud de malla — Polea 2')
    longitud_malla_tol = fields.Float(string='Tolerancia longitud de malla')
    longitud_malla_tol_unit = fields.Char(string='Unidad', default='cm')

    consumo_cm_vta_polea1 = fields.Float(string='Consumo cm/vta — Polea 1')
    consumo_cm_vta_polea2 = fields.Float(string='Consumo cm/vta — Polea 2')
    consumo_cm_vta_tol = fields.Float(string='Tolerancia consumo')
    consumo_cm_vta_tol_unit = fields.Char(string='Unidad', default='cm')

    polea_alimentacion_polea1 = fields.Float(string='Polea de alimentación — Polea 1')
    polea_alimentacion_polea2 = fields.Float(string='Polea de alimentación — Polea 2')
    polea_alimentacion_tol = fields.Float(string='Tolerancia polea de alimentación')
    polea_alimentacion_tol_unit = fields.Char(string='Unidad', default='gr')

    # ------------------------------------------------------------------
    # Especificaciones de tejido — Tabla 2: Dato / Valor / Tolerancia
    # ------------------------------------------------------------------
    tension = fields.Char(string='Tensión')
    tension_tol = fields.Char(string='Tolerancia tensión')
    tension_tol_unit = fields.Char(string='Unidad', default='PTO')

    punto_cilindro = fields.Char(string='Punto cilindro')
    punto_cilindro_tol = fields.Char(string='Tolerancia punto cilindro')
    punto_cilindro_tol_unit = fields.Char(string='Unidad', default='PTO')

    punto_plato = fields.Char(string='Punto plato')
    punto_plato_tol = fields.Char(string='Tolerancia punto plato')
    punto_plato_tol_unit = fields.Char(string='Unidad', default='PTO')

    altura_plato = fields.Char(string='Altura plato')
    altura_plato_tol = fields.Char(string='Tolerancia altura plato')
    altura_plato_tol_unit = fields.Char(string='Unidad', default='PTO')

    ancho_bastidor = fields.Float(string='Ancho de bastidor')
    ancho_bastidor_tol = fields.Float(string='Tolerancia ancho de bastidor')
    ancho_bastidor_tol_unit = fields.Char(string='Unidad', default='cm')

    estiraje = fields.Char(string='Estiraje')
    estiraje_tol = fields.Char(string='Tolerancia estiraje')
    estiraje_tol_unit = fields.Char(string='Unidad')

    ancho_rollo = fields.Float(string='Ancho de rollo')
    ancho_rollo_tol = fields.Float(string='Tolerancia ancho de rollo')
    ancho_rollo_tol_unit = fields.Char(string='Unidad', default='cm')

    peso_promedio_rollo = fields.Float(string='Peso promedio de rollo')
    peso_promedio_rollo_tol = fields.Float(string='Tolerancia peso de rollo')
    peso_promedio_rollo_tol_unit = fields.Char(string='Unidad', default='kg')

    # ------------------------------------------------------------------
    # Datos de tela acondicionada (post-tejido, pre-acabado)
    # ------------------------------------------------------------------
    peso_acondicionado = fields.Float(string='Peso')
    peso_acondicionado_tol = fields.Char(
        string='Tolerancia peso', help='Admite formato asimétrico, ej. "+12 / -6".')
    peso_acondicionado_tol_unit = fields.Char(string='Unidad', default='g/m2')

    ancho_acondicionado = fields.Float(string='Ancho')
    ancho_acondicionado_tol = fields.Char(string='Tolerancia ancho')
    ancho_acondicionado_tol_unit = fields.Char(string='Unidad', default='cm')

    espesor_acondicionado = fields.Float(string='Espesor')
    espesor_acondicionado_tol = fields.Char(string='Tolerancia espesor')
    espesor_acondicionado_tol_unit = fields.Char(string='Unidad', default='in')

    columnas = fields.Integer(string='Columnas')
    columnas_tol = fields.Char(string='Tolerancia columnas')
    columnas_tol_unit = fields.Char(string='Unidad', default='in')

    mallas = fields.Integer(string='Mallas')
    mallas_tol = fields.Char(string='Tolerancia mallas')
    mallas_tol_unit = fields.Char(string='Unidad', default='in')

    elongacion_carga_largo = fields.Float(string='Elongación bajo carga — largo')
    elongacion_carga_largo_tol = fields.Char(string='Tolerancia elongación largo')
    elongacion_carga_largo_tol_unit = fields.Char(string='Unidad', default='%')

    elongacion_carga_ancho = fields.Float(string='Elongación bajo carga — ancho')
    elongacion_carga_ancho_tol = fields.Char(string='Tolerancia elongación ancho')
    elongacion_carga_ancho_tol_unit = fields.Char(string='Unidad', default='%')

    notas = fields.Text(string='Notas')

    _articulo_revision_uniq = models.Constraint(
        'unique(articulo, revision)',
        'Ya existe una ficha técnica de tejido con ese artículo y esa revisión.',
    )

    def action_view_fichas_acabado(self):
        self.ensure_one()
        action = {
            'type': 'ir.actions.act_window',
            'name': 'Fichas Técnicas de Acabado — %s' % self.articulo,
            'res_model': 'ficha.tecnica.acabado',
        }
        if len(self.ficha_acabado_ids) == 1:
            action.update({'view_mode': 'form', 'res_id': self.ficha_acabado_ids.id})
        else:
            action.update({
                'view_mode': 'list,form',
                'domain': [('tejido_id', '=', self.id)],
                'context': {'default_tejido_id': self.id},
            })
        return action


class FichaTecnicaTejidoHilo(models.Model):
    _name = 'ficha.tecnica.tejido.hilo'
    _description = 'Disposición de hilo — Ficha Técnica de Tejido'
    _order = 'numero'

    ficha_id = fields.Many2one('ficha.tecnica.tejido', string='Ficha técnica de tejido',
                                required=True, ondelete='cascade')
    numero = fields.Integer(string='No.')
    tipo_hilo = fields.Char(string='Tipo de hilo')
    titulo_hilo = fields.Char(string='Título de hilo')
    torsion = fields.Char(string='Torsión')
    porcentaje = fields.Float(string='% de hilo')
    lote = fields.Char(string='Lote')
    proveedor_id = fields.Many2one(
        'res.partner', string='Proveedor',
        domain=[('supplier_rank', '>', 0)],
        help='Debe ser un contacto marcado como proveedor en Odoo.')
