# -*- coding: utf-8 -*-
import re

from odoo import api, fields, models
from odoo.exceptions import ValidationError

# 2.3.0: datos de máquina que la ficha toma de la configuración de la máquina con la que se elaboró.
MACHINE_CONFIG_FIELDS = ('galga', 'diametro', 'no_agujas', 'no_alimentadores')
# 2.9.0: diferencia máxima (puntos de %) entre el % de un hilo en la ficha y su parte en la lista de materiales.
BOM_TOLERANCIA_PCT = 1.0
HILO_TITULO_RE = re.compile(r'(\d+(?:[.,]\d+)?)\s*/\s*(\d+)')
HILO_TORSION_RE = re.compile(r'TORSI[OÓ]N\s*[“"\']?\s*([SZ])\b|[“"]([SZ])[”"]')
LIGADO_ESTADOS = [
    ('ligado', 'Ya ligado'),
    ('unico', 'Un solo candidato en su lista de materiales'),
    ('varios', 'Varios candidatos en su lista de materiales'),
    ('otra_lista', 'Solo en otra lista de materiales del crudo'),
    ('fuera', 'Solo fuera de las listas de materiales del crudo'),
    ('ninguno', 'Sin candidato'),
]


def _norm(texto):
    return re.sub(r'\s+', ' ', (texto or '').upper()).strip()


class FichaTecnicaTejido(models.Model):
    _name = 'ficha.tecnica.tejido'
    _description = 'Ficha Técnica de Tejido'
    _rec_name = 'articulo'
    _order = 'articulo, maquina_tejido'

    # ------------------------------------------------------------------
    # Encabezado
    # ------------------------------------------------------------------
    articulo = fields.Char(string='Artículo', required=True, index=True,
                            help='Código del artículo de tejido, ej. WJ044Q22HNT235')
    revision = fields.Char(string='Revisión', default='0')
    active = fields.Boolean(default=True)
    fecha_elaboracion = fields.Date(string='Fecha de elaboración')
    # 2.9.0: las personas son empleados; el texto anterior queda como dato histórico (vacío en producción).
    jefe_manufactura_id = fields.Many2one('hr.employee', string='Jefe de manufactura')
    auxiliar_procesos_id = fields.Many2one('hr.employee', string='Auxiliar de procesos')
    jefe_manufactura = fields.Char(string='Jefe de manufactura (texto anterior)')
    auxiliar_procesos = fields.Char(string='Auxiliar de procesos (texto anterior)')

    # Vínculo con el producto semi-terminado (arquitectura de 2 productos)
    product_proceso_id = fields.Many2one(
        'product.product', string='Producto — Tela en Proceso (kg)',
        help='Producto semi-terminado que recorre Preparado/Devanado, Tintorería, Abridora y Rama.')
    product_proceso_tmpl_id = fields.Many2one(related='product_proceso_id.product_tmpl_id',
                                              string='Plantilla del crudo')
    # 2.9.0: la lista de materiales con la que se cotejan los hilos (el crudo puede tener varias: alternativas
    # por hilo o por proveedor). Vacía: la principal del crudo.
    bom_id = fields.Many2one(
        'mrp.bom', string='Lista de materiales', domain="[('product_tmpl_id', '=', product_proceso_tmpl_id)]",
        help='La lista de materiales del crudo que describe esta ficha. Vacía: la principal del crudo.')
    bom_efectiva_id = fields.Many2one('mrp.bom', string='Lista de materiales que se coteja',
                                      compute='_compute_bom_aviso')
    bom_aviso = fields.Char(string='Aviso de hilos', compute='_compute_bom_aviso', search='_search_bom_aviso',
                            help='Lo que no cuadra entre los hilos de la ficha y la lista de materiales del crudo: un '
                                 'hilo sin producto, uno que falta o sobra, un %% que difiere más de %g puntos o '
                                 'unos %% que no suman 100.' % BOM_TOLERANCIA_PCT)
    hilo_pct_total = fields.Float(string='Suma de % de hilos', digits=(16, 2), compute='_compute_bom_aviso')

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
    maquina_config_aviso = fields.Char(string='Aviso de configuración', compute='_compute_maquina_config_aviso',
                                       help='Lo que impide ligar la ficha a la configuración vigente de su máquina.')
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
    # 2.16.1: tres decimales (0.205 ± 0.002 cm); sin digits la ficha mostraba 0.21 y la tolerancia +/- 0.00.
    longitud_malla_polea1 = fields.Float(string='Longitud de malla — Polea 1', digits=(16, 3))
    longitud_malla_polea2 = fields.Float(string='Longitud de malla — Polea 2', digits=(16, 3))
    longitud_malla_tol = fields.Float(string='Tolerancia longitud de malla', digits=(16, 3))
    longitud_malla_tol_unit = fields.Char(string='Unidad — Longitud de malla (texto)', default='cm')

    consumo_cm_vta_polea1 = fields.Float(string='Consumo cm/vta — Polea 1')
    consumo_cm_vta_polea2 = fields.Float(string='Consumo cm/vta — Polea 2')
    consumo_cm_vta_tol = fields.Float(string='Tolerancia consumo')
    consumo_cm_vta_tol_unit = fields.Char(string='Unidad — Consumo cm/vta (texto)', default='cm')

    polea_alimentacion_polea1 = fields.Float(string='Polea de alimentación — Polea 1')
    polea_alimentacion_polea2 = fields.Float(string='Polea de alimentación — Polea 2')
    polea_alimentacion_tol = fields.Float(string='Tolerancia polea de alimentación')
    polea_alimentacion_tol_unit = fields.Char(string='Unidad — Polea de alimentación (texto)', default='gr')

    # ------------------------------------------------------------------
    # Especificaciones de tejido — Tabla 2: Dato / Valor / Tolerancia
    # ------------------------------------------------------------------
    tension = fields.Char(string='Tensión')
    tension_tol = fields.Float(string='Tolerancia tensión')
    tension_tol_unit = fields.Char(string='Unidad — Tensión (texto)', default='PTO')

    punto_cilindro = fields.Char(string='Punto cilindro')
    punto_cilindro_tol = fields.Float(string='Tolerancia punto cilindro')
    punto_cilindro_tol_unit = fields.Char(string='Unidad — Punto cilindro (texto)', default='PTO')

    punto_plato = fields.Char(string='Punto plato')
    punto_plato_tol = fields.Float(string='Tolerancia punto plato')
    punto_plato_tol_unit = fields.Char(string='Unidad — Punto plato (texto)', default='PTO')

    altura_plato = fields.Char(string='Altura plato')
    altura_plato_tol = fields.Float(string='Tolerancia altura plato')
    altura_plato_tol_unit = fields.Char(string='Unidad — Altura plato (texto)', default='PTO')

    ancho_bastidor = fields.Float(string='Ancho de bastidor')
    ancho_bastidor_tol = fields.Float(string='Tolerancia ancho de bastidor')
    ancho_bastidor_tol_unit = fields.Char(string='Unidad — Ancho de bastidor (texto)', default='cm')

    estiraje = fields.Char(string='Estiraje')
    estiraje_tol = fields.Float(string='Tolerancia estiraje')
    estiraje_tol_unit = fields.Char(string='Unidad — Estiraje (texto)')

    ancho_rollo = fields.Float(string='Ancho de rollo')
    ancho_rollo_tol = fields.Float(string='Tolerancia ancho de rollo')
    ancho_rollo_tol_unit = fields.Char(string='Unidad — Ancho de rollo (texto)', default='cm')

    peso_promedio_rollo = fields.Float(string='Peso promedio de rollo')
    peso_promedio_rollo_tol = fields.Float(string='Tolerancia peso de rollo')
    peso_promedio_rollo_tol_unit = fields.Char(string='Unidad — Peso promedio de rollo (texto)', default='kg')

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
    peso_acondicionado_tol_unit = fields.Char(string='Unidad — Peso acondicionado (texto)', default='g/m2')

    ancho_acondicionado = fields.Float(string='Ancho')
    ancho_acondicionado_tol = fields.Float(string='Tolerancia ancho')
    ancho_acondicionado_tol_unit = fields.Char(string='Unidad — Ancho acondicionado (texto)', default='cm')

    espesor_acondicionado = fields.Float(string='Espesor')
    espesor_acondicionado_tol = fields.Float(string='Tolerancia espesor')
    espesor_acondicionado_tol_unit = fields.Char(string='Unidad — Espesor acondicionado (texto)', default='in')

    columnas = fields.Integer(string='Columnas')
    columnas_tol = fields.Float(string='Tolerancia columnas')
    columnas_tol_unit = fields.Char(string='Unidad — Columnas (texto)', default='in')

    mallas = fields.Integer(string='Mallas')
    mallas_tol = fields.Float(string='Tolerancia mallas')
    mallas_tol_unit = fields.Char(string='Unidad — Mallas (texto)', default='in')

    elongacion_carga_largo = fields.Float(string='Elongación bajo carga — largo')
    elongacion_carga_largo_tol = fields.Float(string='Tolerancia elongación largo')
    elongacion_carga_largo_tol_unit = fields.Char(string='Unidad — Elongación carga largo (texto)', default='%')

    elongacion_carga_ancho = fields.Float(string='Elongación bajo carga — ancho')
    elongacion_carga_ancho_tol = fields.Float(string='Tolerancia elongación ancho')
    elongacion_carga_ancho_tol_unit = fields.Char(string='Unidad — Elongación carga ancho (texto)', default='%')

    notas = fields.Text(string='Notas')

    # 2.7.0: una ficha por artículo, revisión y máquina, como los formatos de la planta (F-IT-P-P01-08-05 de
    # 2017 y F-IT-P-P01-08-04 de 2025 llevan una hoja por máquina: los ajustes cambian de una a otra).
    # Mismo nombre que la de 2.6.0 para que el update la reemplace.
    _articulo_revision_uniq = models.Constraint(
        'unique(articulo, revision, maquina_tejido)',
        'Ya existe una ficha técnica de tejido con ese artículo, esa revisión y esa máquina.',
    )

    @api.depends('articulo', 'revision', 'maquina_tejido')
    def _compute_display_name(self):
        for rec in self:
            name = rec.articulo or ''
            if rec.revision and rec.revision != '0':
                name = '%s rev. %s' % (name, rec.revision)
            if rec.maquina_tejido:
                name = '%s · %s' % (name, rec.maquina_tejido.name)
            rec.display_name = name

    # ------------------------------------------------------------------
    # 2.3.0: datos de máquina desde la configuración de la máquina
    # ------------------------------------------------------------------
    @api.model
    def _ft_machine_vals(self, config):
        # 2.9.0: un dato vacío de la configuración no borra el de la ficha (había configuraciones sin agujas).
        vals = {name: config[name] for name in MACHINE_CONFIG_FIELDS if config[name]}
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

    # ------------------------------------------------------------------
    # 2.9.0: configuración de la máquina para las fichas que no la tienen
    # ------------------------------------------------------------------
    def _ft_config_para_ligar(self):
        """(configuración, motivo): la vigente de la máquina a la fecha de elaboración (hoy si no hay fecha) y,
        si no se puede ligar sin perder datos, por qué. Un dato de máquina distinto entre ficha y configuración
        (los dos capturados) no se resuelve solo."""
        self.ensure_one()
        if not self.maquina_tejido:
            return self.env['ficha.tecnica.maquina.config'], 'la ficha no tiene máquina'
        config = self.maquina_tejido._ft_config_at(self.fecha_elaboracion)
        if not config:
            return config, '%s no tiene configuración vigente' % self.maquina_tejido.name
        labels = {name: self._fields[name].string for name in MACHINE_CONFIG_FIELDS}
        distintos = ['%s: ficha %g, máquina %g' % (labels[name], self[name], config[name])
                     for name in MACHINE_CONFIG_FIELDS if self[name] and config[name] and self[name] != config[name]]
        return config, ('; '.join(distintos) if distintos else False)

    @api.depends('maquina_tejido', 'maquina_config_id', 'fecha_elaboracion', 'galga', 'diametro', 'no_agujas',
                 'no_alimentadores')
    def _compute_maquina_config_aviso(self):
        for ficha in self:
            aviso = False
            if ficha.maquina_tejido and not ficha.maquina_config_id:
                config, motivo = ficha._ft_config_para_ligar()
                aviso = ('No se liga a la configuración de la máquina: %s.' % motivo) if motivo else (
                    'Sin configuración de máquina: «Ligar a la configuración vigente» toma %s.' % config.display_name)
            ficha.maquina_config_aviso = aviso

    def action_ft_ligar_configuracion(self):
        """Liga las fichas sin configuración a la vigente de su máquina cuando sus datos de máquina no la
        contradicen; las demás se reportan. Los datos vacíos de la ficha se llenan con los de la configuración."""
        ligadas, pendientes = 0, []
        for ficha in self.filtered(lambda f: not f.maquina_config_id):
            config, motivo = ficha._ft_config_para_ligar()
            if motivo:
                pendientes.append('%s: %s' % (ficha.display_name, motivo))
                continue
            ficha.maquina_config_id = config
            ligadas += 1
        mensaje = '%d ficha(s) ligadas a la configuración vigente de su máquina.' % ligadas
        if pendientes:
            mensaje += ' Sin ligar (%d): %s.' % (len(pendientes), ' · '.join(pendientes))
        return {'type': 'ir.actions.client', 'tag': 'display_notification',
                'params': {'title': 'Configuración de máquina', 'message': mensaje, 'sticky': bool(pendientes),
                           'type': 'warning' if pendientes else 'success'}}

    # ------------------------------------------------------------------
    # 2.9.0: hilos contra la lista de materiales del crudo
    # ------------------------------------------------------------------
    def _ft_bom(self):
        """La lista de materiales que se coteja: la elegida o la principal del crudo."""
        self.ensure_one()
        if self.bom_id:
            return self.bom_id
        product = self.product_proceso_id
        if not product:
            return self.env['mrp.bom']
        return self.env['mrp.bom']._bom_find(product, bom_type='normal')[product]

    @api.model
    def _ft_bom_partes(self, bom):
        """{producto: % de la cantidad de componentes} de una lista de materiales (en la unidad de la lista)."""
        qty = {}
        for line in bom.bom_line_ids:
            value = line.product_qty
            if line.product_uom_id and line.product_id.uom_id and line.product_uom_id != line.product_id.uom_id \
                    and line.product_uom_id._has_common_reference(line.product_id.uom_id):
                value = line.product_uom_id._compute_quantity(value, line.product_id.uom_id, round=False)
            qty[line.product_id] = qty.get(line.product_id, 0.0) + value
        total = sum(qty.values())
        return {product: value * 100.0 / total for product, value in qty.items()} if total else {}

    def _ft_bom_avisos(self):
        """Lo que no cuadra entre los hilos de la ficha y su lista de materiales (lista de textos)."""
        self.ensure_one()
        avisos = []
        hilos = self.hilo_line_ids
        if not hilos:
            return avisos
        total = sum(hilos.mapped('porcentaje'))
        if abs(total - 100.0) > 0.05:
            avisos.append('los %% de los hilos suman %g, no 100' % round(total, 2))
        sin_producto = hilos.filtered(lambda h: not h.product_id)
        if sin_producto:
            avisos.append('%d hilo(s) sin producto ligado' % len(sin_producto))
        bom = self._ft_bom()
        if not self.product_proceso_id:
            return avisos
        if not bom:
            avisos.append('el crudo no tiene lista de materiales')
            return avisos
        partes = self._ft_bom_partes(bom)
        ficha = {}
        for hilo in hilos.filtered('product_id'):
            ficha[hilo.product_id] = ficha.get(hilo.product_id, 0.0) + hilo.porcentaje
        for product, pct in ficha.items():
            if product not in partes:
                avisos.append('%s no está en la lista de materiales' % (product.default_code or product.name))
            elif abs(pct - partes[product]) > BOM_TOLERANCIA_PCT:
                avisos.append('%s: %g %% en la ficha y %g %% en la lista de materiales' % (
                    product.default_code or product.name, round(pct, 2), round(partes[product], 2)))
        if not sin_producto:
            for product in partes:
                if product not in ficha:
                    avisos.append('%s está en la lista de materiales y no en la ficha' % (
                        product.default_code or product.name))
        return avisos

    @api.depends('hilo_line_ids.product_id', 'hilo_line_ids.porcentaje', 'bom_id', 'product_proceso_id')
    def _compute_bom_aviso(self):
        for ficha in self:
            ficha.bom_efectiva_id = ficha._ft_bom()
            ficha.hilo_pct_total = sum(ficha.hilo_line_ids.mapped('porcentaje'))
            avisos = ficha._ft_bom_avisos()
            ficha.bom_aviso = ('Hilos: %s.' % '; '.join(avisos)) if avisos else False

    def _search_bom_aviso(self, operator, value):
        """Solo «con aviso» / «sin aviso» (son pocas fichas: se calcula sobre todas)."""
        values = list(value) if hasattr(value, '__iter__') and not isinstance(value, (str, bytes)) else [value]
        if operator in ('!=', 'not in'):
            want = not any(values)
        elif operator in ('=', 'in'):
            want = any(values)
        else:
            return NotImplemented
        con_aviso = self.with_context(active_test=False).search([]).filtered(lambda f: f._ft_bom_avisos())
        return [('id', 'in' if want else 'not in', con_aviso.ids)]

    # ------------------------------------------------------------------
    # Navegación
    # ------------------------------------------------------------------
    def action_ft_ver_maquina(self):
        self.ensure_one()
        return {'type': 'ir.actions.act_window', 'name': self.maquina_tejido.name, 'res_model': 'mrp.workcenter',
                'view_mode': 'form', 'res_id': self.maquina_tejido.id}

    def action_ft_ver_bom(self):
        self.ensure_one()
        return {'type': 'ir.actions.act_window', 'name': 'Lista de materiales', 'res_model': 'mrp.bom',
                'view_mode': 'form', 'res_id': self.bom_efectiva_id.id}

    def action_ft_reporte_ligado_ficha(self):
        self.ensure_one()
        return self.hilo_line_ids.action_ft_reporte_ligado() if self.hilo_line_ids else True

    def action_ft_ver_hilos(self):
        self.ensure_one()
        return {'type': 'ir.actions.act_window', 'name': 'Fichas de materia prima — %s' % self.display_name,
                'res_model': 'ficha.tecnica.material', 'view_mode': 'list,form',
                'domain': [('product_id', 'in', self.hilo_line_ids.product_id.ids)]}

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
    """Un hilo de la ficha de tejido.

    2.9.0: el hilo es el **producto** (``product_id``) con su ficha de materia prima; tipo, título, torsión y
    lote en texto quedan como dato anterior (los lee el importador y el ligado). Polea, alimentadores, consumo
    y longitud de malla van por hilo. Los campos guardados de máquina, crudo y galga sirven para recorrer la
    cadena al revés: de un hilo a sus telas y máquinas.
    """
    _name = 'ficha.tecnica.tejido.hilo'
    _description = 'Disposición de hilo — Ficha Técnica de Tejido'
    _order = 'numero'

    ficha_id = fields.Many2one('ficha.tecnica.tejido', string='Ficha técnica de tejido',
                                required=True, ondelete='cascade')
    numero = fields.Integer(string='No.')
    product_id = fields.Many2one('product.product', string='Hilo', index=True, ondelete='restrict',
                                 help='El producto del hilo en Inventario. Sus propiedades están en su ficha de '
                                      'materia prima.')
    material_id = fields.Many2one('ficha.tecnica.material', string='Ficha de materia prima',
                                  compute='_compute_material_id')
    material_resumen = fields.Char(string='Propiedades del hilo', compute='_compute_material_id')
    composicion = fields.Char(string='Composición', compute='_compute_material_id')
    tipo_hilo = fields.Char(string='Tipo de hilo (texto)')
    titulo_hilo = fields.Char(string='Título de hilo (texto)')
    torsion = fields.Char(string='Torsión (texto)')
    porcentaje = fields.Float(string='% de hilo')
    polea = fields.Integer(string='Polea', help='Polea (posición de alimentación) por la que entra el hilo.')
    alimentadores = fields.Integer(string='No. de alimentadores', help='Cuántos alimentadores llevan este hilo.')
    secuencia_alimentacion = fields.Char(string='Secuencia de alimentación',
                                         help='Cómo se reparte el hilo en los alimentadores (p. ej. «1 de cada 2», '
                                              '«A-B-A-B»).')
    consumo_cm_vta = fields.Float(string='Consumo (cm/vta)', help='Consumo de este hilo por vuelta de la máquina.')
    longitud_malla = fields.Float(string='Longitud de malla', digits=(16, 3),
                                  help='Longitud de malla con este hilo (misma unidad que la ficha).')
    lote = fields.Char(string='Lote (texto)')
    lote_id = fields.Many2one('stock.lot', string='Lote', domain="[('product_id', '=', product_id)]",
                              help='Lote de inventario del hilo con el que se elaboró la ficha.')
    proveedor_id = fields.Many2one(
        'res.partner', string='Proveedor',
        domain=[('supplier_rank', '>', 0)],
        help='Uno de los proveedores del hilo (pestaña Compra del producto) o, si el producto no tiene, un contacto '
             'marcado como proveedor.')
    proveedor_permitido_ids = fields.Many2many('res.partner', string='Proveedores del hilo',
                                               compute='_compute_proveedor_permitido_ids')
    # 2.9.0: guardados para recorrer la cadena al revés (hilo → telas → máquinas) y la tabla dinámica.
    articulo = fields.Char(related='ficha_id.articulo', store=True, string='Artículo')
    crudo_id = fields.Many2one(related='ficha_id.product_proceso_id', store=True, string='Crudo')
    workcenter_id = fields.Many2one(related='ficha_id.maquina_tejido', store=True, index=True, string='Máquina')
    galga = fields.Float(related='ficha_id.galga', store=True, string='Galga')
    ficha_active = fields.Boolean(related='ficha_id.active', store=True, string='Ficha activa')
    bom_pct = fields.Float(string='% en la lista de materiales', digits=(16, 2), compute='_compute_bom_pct')

    @api.depends('product_id')
    def _compute_material_id(self):
        Material = self.env['ficha.tecnica.material']
        materials = {m.product_id.id: m for m in Material.search([('product_id', 'in', self.product_id.ids)])}
        for hilo in self:
            material = materials.get(hilo.product_id.id, Material)
            hilo.material_id = material
            hilo.material_resumen = material.resumen
            hilo.composicion = material.composicion

    @api.depends('product_id')
    def _compute_proveedor_permitido_ids(self):
        for hilo in self:
            hilo.proveedor_permitido_ids = hilo.product_id._ft_proveedores() if hilo.product_id else False

    @api.depends('product_id', 'ficha_id.bom_id', 'ficha_id.product_proceso_id')
    def _compute_bom_pct(self):
        for hilo in self:
            bom = hilo.ficha_id._ft_bom() if hilo.ficha_id else False
            partes = self.env['ficha.tecnica.tejido']._ft_bom_partes(bom) if bom else {}
            hilo.bom_pct = partes.get(hilo.product_id, 0.0) if hilo.product_id else 0.0

    @api.onchange('product_id')
    def _onchange_product_id(self):
        """Con el hilo elegido: el % de la lista de materiales si no hay, y el proveedor si solo hay uno."""
        for hilo in self.filtered('product_id'):
            if not hilo.porcentaje and hilo.bom_pct:
                hilo.porcentaje = round(hilo.bom_pct, 2)
            proveedores = hilo.product_id._ft_proveedores()
            if not hilo.proveedor_id and len(proveedores) == 1:
                hilo.proveedor_id = proveedores

    # ------------------------------------------------------------------
    # 2.9.0: candidatos para ligar el texto anterior al producto
    # ------------------------------------------------------------------
    def _ft_texto(self):
        self.ensure_one()
        return ' '.join(p for p in (self.tipo_hilo, self.titulo_hilo, self.torsion) if p)

    def _ft_compatible(self, product):
        """¿El producto puede ser el hilo que dice el texto? Mismo título (75/36, 22/1), sin torsión, textura
        o color contrarios a los del texto."""
        self.ensure_one()
        titulo = HILO_TITULO_RE.search(self.titulo_hilo or '')
        if not titulo:
            return False
        nombre = _norm(' '.join(p for p in (product.name, product.default_code) if p))
        a, b = titulo.group(1).replace(',', '.'), titulo.group(2)
        if not re.search(r'(?<![\d.])%s\s*/\s*%s(?!\d)' % (re.escape(a), re.escape(b)), nombre):
            return False
        torsion = _norm(self.torsion)
        match = HILO_TORSION_RE.search(nombre)
        torsion_producto = match and (match.group(1) or match.group(2))
        if torsion in ('S', 'Z') and torsion_producto and torsion_producto != torsion:
            return False
        tipo = _norm(self.tipo_hilo)
        stretch = 'STRETCH' in nombre or 'STRECH' in nombre
        if 'STRETCH' in tipo and not stretch:
            return False
        if tipo == 'SET' and stretch:
            return False
        # El texto que dice negro solo acepta negros; el que no dice color acepta los dos (decide el %: el
        # WJ035Q22HNN200 lleva 3.2 % de negro y su ficha no lo dice).
        negro_texto = 'NEGRO' in tipo or re.search(r'\bNG\b', _norm(self.titulo_hilo))
        return 'NEGRO' in nombre or not negro_texto

    def _ft_candidatos(self):
        """(estado, candidatos) para ligar el renglón a su producto. Primero los componentes de la lista de
        materiales que se coteja; con varios, el que tenga ahí el mismo % (± tolerancia). Luego las otras listas
        del crudo y al final todo el catálogo; esos dos no se ligan solos."""
        self.ensure_one()
        Product = self.env['product.product']
        if self.product_id:
            return 'ligado', self.product_id
        ficha = self.ficha_id
        bom = ficha._ft_bom()
        propios = bom.bom_line_ids.product_id.filtered(self._ft_compatible) if bom else Product
        if len(propios) == 1:
            return 'unico', propios
        if propios:
            partes = ficha._ft_bom_partes(bom)
            mismo = propios.filtered(lambda p: abs(partes.get(p, 0.0) - self.porcentaje) <= BOM_TOLERANCIA_PCT)
            return ('unico', mismo) if len(mismo) == 1 else ('varios', propios)
        if ficha.product_proceso_id:
            otras = self.env['mrp.bom'].search([('product_tmpl_id', '=', ficha.product_proceso_id.product_tmpl_id.id)])
            en_otras = (otras - bom).bom_line_ids.product_id.filtered(self._ft_compatible)
            if en_otras:
                return 'otra_lista', en_otras
        titulo = HILO_TITULO_RE.search(self.titulo_hilo or '')
        if titulo:
            catalogo = Product.search([('name', 'ilike', '%s/%s' % (titulo.group(1), titulo.group(2)))], limit=50)
            fuera = catalogo.filtered(self._ft_compatible)
            if fuera:
                return 'fuera', fuera
        return 'ninguno', Product

    def _ft_lote_candidato(self):
        """El lote de inventario con el nombre del texto anterior, si hay uno solo del producto."""
        self.ensure_one()
        if not (self.lote and self.product_id) or self.lote_id:
            return self.env['stock.lot']
        lotes = self.env['stock.lot'].search([('name', '=', self.lote.strip()), ('product_id', '=', self.product_id.id)],
                                             limit=2)
        return lotes if len(lotes) == 1 else self.env['stock.lot']

    def _ft_ligar_unicos(self):
        """Liga los renglones sin producto que tienen un solo candidato en su lista de materiales (y su lote si
        existe). Solo escribe campos nuevos: el texto anterior se conserva. Devuelve los ligados."""
        ligados = self.browse()
        for hilo in self.filtered(lambda h: not h.product_id):
            estado, candidatos = hilo._ft_candidatos()
            if estado == 'unico':
                hilo.product_id = candidatos
                ligados |= hilo
        for hilo in self.filtered(lambda h: h.product_id and h.lote and not h.lote_id):
            lote = hilo._ft_lote_candidato()
            if lote:
                hilo.lote_id = lote
        return ligados

    def action_ft_reporte_ligado(self):
        """Reporte de ligado: un renglón por hilo de las fichas activas (o los seleccionados) con su estado y sus
        candidatos. No cambia nada; desde el reporte se ligan los de candidato único."""
        hilos = self if self else self.search([('ficha_active', '=', True)])
        return self.env['ficha.tecnica.hilo.ligado']._ft_generar(hilos)
