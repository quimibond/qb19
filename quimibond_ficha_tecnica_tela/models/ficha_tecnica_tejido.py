# -*- coding: utf-8 -*-
from odoo import api, fields, models
from odoo.exceptions import ValidationError

# 2.3.0: datos de máquina que la ficha toma de la configuración de la máquina con la que se elaboró.
MACHINE_CONFIG_FIELDS = ('galga', 'diametro', 'no_agujas', 'no_alimentadores')


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
    # 2.2.0 (Dirección General 2026-10-13): la máquina es el centro de trabajo de Odoo; lo que se
    # capturó como texto queda en maquina_tejido_texto (dato histórico).
    maquina_tejido = fields.Many2one(
        'mrp.workcenter', string='Máquina', index=True,
        help='Centro de trabajo (máquina) en el que se teje el artículo.')
    maquina_tejido_texto = fields.Char(
        string='Máquina (texto anterior)',
        help='Lo que se capturó como texto antes de la versión 2.2.0, o lo que traía el Excel cuando no '
             'coincide con ningún centro de trabajo. Solo es dato histórico.')
    # 2.3.0: la configuración de la máquina (cilindro) con la que se elaboró la ficha. De ella salen galga,
    # diámetro, agujas y alimentadores, y la marca de la máquina; un cambio de cilindro posterior no los
    # cambia aquí.
    maquina_config_id = fields.Many2one(
        'ficha.tecnica.maquina.config', string='Configuración de la máquina', index=True,
        domain="[('workcenter_id', '=', maquina_tejido)]",
        help='Configuración de la máquina vigente a la fecha de elaboración; se elige sola al poner la máquina '
             'y la fecha. Los datos de máquina de la ficha salen de aquí.')
    galga_requerida = fields.Float(
        string='Galga del artículo', digits=(16, 0),
        help='La galga que pide el artículo. Con ella la lista de máquinas solo ofrece las que hoy corren esa '
             'galga.')
    velocidad_aviso = fields.Char(string='Aviso de velocidad', compute='_compute_velocidad_aviso')
    marca_maquina = fields.Char(string='Marca de máquina')
    galga = fields.Float(string='Galga')
    diametro = fields.Float(string='Diámetro (in)', digits=(16, 2))
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
    longitud_malla_tol_unit = fields.Char(string='Unidad — Longitud de malla', default='cm')

    consumo_cm_vta_polea1 = fields.Float(string='Consumo cm/vta — Polea 1')
    consumo_cm_vta_polea2 = fields.Float(string='Consumo cm/vta — Polea 2')
    consumo_cm_vta_tol = fields.Float(string='Tolerancia consumo')
    consumo_cm_vta_tol_unit = fields.Char(string='Unidad — Consumo cm/vta', default='cm')

    polea_alimentacion_polea1 = fields.Float(string='Polea de alimentación — Polea 1')
    polea_alimentacion_polea2 = fields.Float(string='Polea de alimentación — Polea 2')
    polea_alimentacion_tol = fields.Float(string='Tolerancia polea de alimentación')
    polea_alimentacion_tol_unit = fields.Char(string='Unidad — Polea de alimentación', default='gr')

    # ------------------------------------------------------------------
    # Especificaciones de tejido — Tabla 2: Dato / Valor / Tolerancia
    # ------------------------------------------------------------------
    tension = fields.Char(string='Tensión')
    tension_tol = fields.Float(string='Tolerancia tensión')
    tension_tol_unit = fields.Char(string='Unidad — Tensión', default='PTO')

    punto_cilindro = fields.Char(string='Punto cilindro')
    punto_cilindro_tol = fields.Float(string='Tolerancia punto cilindro')
    punto_cilindro_tol_unit = fields.Char(string='Unidad — Punto cilindro', default='PTO')

    punto_plato = fields.Char(string='Punto plato')
    punto_plato_tol = fields.Float(string='Tolerancia punto plato')
    punto_plato_tol_unit = fields.Char(string='Unidad — Punto plato', default='PTO')

    altura_plato = fields.Char(string='Altura plato')
    altura_plato_tol = fields.Float(string='Tolerancia altura plato')
    altura_plato_tol_unit = fields.Char(string='Unidad — Altura plato', default='PTO')

    ancho_bastidor = fields.Float(string='Ancho de bastidor')
    ancho_bastidor_tol = fields.Float(string='Tolerancia ancho de bastidor')
    ancho_bastidor_tol_unit = fields.Char(string='Unidad — Ancho de bastidor', default='cm')

    estiraje = fields.Char(string='Estiraje')
    estiraje_tol = fields.Float(string='Tolerancia estiraje')
    estiraje_tol_unit = fields.Char(string='Unidad — Estiraje')

    ancho_rollo = fields.Float(string='Ancho de rollo')
    ancho_rollo_tol = fields.Float(string='Tolerancia ancho de rollo')
    ancho_rollo_tol_unit = fields.Char(string='Unidad — Ancho de rollo', default='cm')

    peso_promedio_rollo = fields.Float(string='Peso promedio de rollo')
    peso_promedio_rollo_tol = fields.Float(string='Tolerancia peso de rollo')
    peso_promedio_rollo_tol_unit = fields.Char(string='Unidad — Peso promedio de rollo', default='kg')

    # ------------------------------------------------------------------
    # Datos de tela acondicionada (post-tejido, pre-acabado)
    # ------------------------------------------------------------------
    peso_acondicionado = fields.Float(string='Peso')
    peso_acondicionado_tol = fields.Float(
        string='Tolerancia peso (+)',
        help='Tolerancia hacia arriba. Si la de abajo queda en 0, la tolerancia es ± este valor.')
    peso_acondicionado_tol_minus = fields.Float(
        string='Tolerancia peso (−)',
        help='Tolerancia hacia abajo cuando es distinta de la de arriba (antes «+12 / -6»). En 0: la misma '
             'que la de arriba.')
    peso_acondicionado_tol_unit = fields.Char(string='Unidad — Peso acondicionado', default='g/m2')

    ancho_acondicionado = fields.Float(string='Ancho')
    ancho_acondicionado_tol = fields.Float(string='Tolerancia ancho')
    ancho_acondicionado_tol_unit = fields.Char(string='Unidad — Ancho acondicionado', default='cm')

    espesor_acondicionado = fields.Float(string='Espesor')
    espesor_acondicionado_tol = fields.Float(string='Tolerancia espesor')
    espesor_acondicionado_tol_unit = fields.Char(string='Unidad — Espesor acondicionado', default='in')

    columnas = fields.Integer(string='Columnas')
    columnas_tol = fields.Float(string='Tolerancia columnas')
    columnas_tol_unit = fields.Char(string='Unidad — Columnas', default='in')

    mallas = fields.Integer(string='Mallas')
    mallas_tol = fields.Float(string='Tolerancia mallas')
    mallas_tol_unit = fields.Char(string='Unidad — Mallas', default='in')

    elongacion_carga_largo = fields.Float(string='Elongación bajo carga — largo')
    elongacion_carga_largo_tol = fields.Float(string='Tolerancia elongación largo')
    elongacion_carga_largo_tol_unit = fields.Char(string='Unidad — Elongación carga largo', default='%')

    elongacion_carga_ancho = fields.Float(string='Elongación bajo carga — ancho')
    elongacion_carga_ancho_tol = fields.Float(string='Tolerancia elongación ancho')
    elongacion_carga_ancho_tol_unit = fields.Char(string='Unidad — Elongación carga ancho', default='%')

    notas = fields.Text(string='Notas')

    _articulo_revision_uniq = models.Constraint(
        'unique(articulo, revision)',
        'Ya existe una ficha técnica de tejido con ese artículo y esa revisión.',
    )

    # ------------------------------------------------------------------
    # 2.3.0: datos de máquina desde la configuración de la máquina
    # ------------------------------------------------------------------
    @api.model
    def _ft_machine_vals(self, config):
        vals = {name: config[name] for name in MACHINE_CONFIG_FIELDS}
        if config.workcenter_id.ft_marca:
            vals['marca_maquina'] = config.workcenter_id.ft_marca
        return vals

    def _ft_complete_machine(self, vals):
        """Pone en ``vals`` la configuración vigente a la fecha de elaboración cuando cambia la máquina o la
        fecha (salvo que se elija una a mano) y los datos de máquina de esa configuración."""
        record = self[:1]
        vals = dict(vals)
        if 'maquina_config_id' not in vals and vals.keys() & {'maquina_tejido', 'fecha_elaboracion'}:
            wc_id = vals['maquina_tejido'] if 'maquina_tejido' in vals else record.maquina_tejido.id
            day = vals['fecha_elaboracion'] if 'fecha_elaboracion' in vals else record.fecha_elaboracion
            workcenter = self.env['mrp.workcenter'].browse(wc_id) if wc_id else self.env['mrp.workcenter']
            vals['maquina_config_id'] = workcenter._ft_config_at(day).id if workcenter else False
        if vals.get('maquina_config_id'):
            config = self.env['ficha.tecnica.maquina.config'].browse(vals['maquina_config_id'])
            vals.update(self._ft_machine_vals(config))
        return vals

    @api.model_create_multi
    def create(self, vals_list):
        return super().create([self._ft_complete_machine(vals) for vals in vals_list])

    def write(self, vals):
        if not vals.keys() & {'maquina_tejido', 'fecha_elaboracion', 'maquina_config_id'}:
            return super().write(vals)
        for ficha in self:
            super(FichaTecnicaTejido, ficha).write(ficha._ft_complete_machine(vals))
        return True

    @api.onchange('maquina_tejido', 'fecha_elaboracion')
    def _onchange_maquina_config(self):
        for ficha in self:
            ficha.maquina_config_id = ficha.maquina_tejido._ft_config_at(ficha.fecha_elaboracion) \
                if ficha.maquina_tejido else False

    @api.onchange('maquina_config_id')
    def _onchange_maquina_config_values(self):
        for ficha in self.filtered('maquina_config_id'):
            ficha.update(self._ft_machine_vals(ficha.maquina_config_id))

    @api.constrains('maquina_config_id', 'maquina_tejido')
    def _check_maquina_config(self):
        for ficha in self.filtered('maquina_config_id'):
            if ficha.maquina_config_id.workcenter_id != ficha.maquina_tejido:
                raise ValidationError('La configuración %s no es de la máquina %s.' % (
                    ficha.maquina_config_id.display_name, ficha.maquina_tejido.display_name or '(vacía)'))

    @api.depends('velocidad', 'maquina_config_id.velocidad_max')
    def _compute_velocidad_aviso(self):
        for ficha in self:
            top = ficha.maquina_config_id.velocidad_max
            ficha.velocidad_aviso = (
                'La velocidad de la ficha (%g rpm) supera la máxima de la máquina con esta configuración '
                '(%g rpm).' % (ficha.velocidad, top)) if top and ficha.velocidad > top else False

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
