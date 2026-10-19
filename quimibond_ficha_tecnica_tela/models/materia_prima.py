# -*- coding: utf-8 -*-
"""Ficha de materia prima (2.9.0, Dirección General 2026-10-19).

«Hoy no puedo ligar un hilo a sus propiedades, que están dentro de un producto,
que está dentro de una máquina.» La cadena se cierra con una ficha por producto
de materia prima (``product.product``), con el mismo motor de parámetros que las
máquinas:

- El **tipo** (``ficha.tecnica.maquina.tipo`` con clase «Materia prima»: hilo de
  filamento, hilo hilado, químico, colorante…) define qué propiedades se
  capturan; un material nuevo no necesita código. Sus parámetros son de ámbito
  «De la materia prima».
- Un **valor** por parámetro del tipo (``ficha.tecnica.material.valor``, mismo
  mixin que la configuración de la máquina y el proceso). Un valor vacío es un
  dato que nadie ha dado, no un cero.
- La **composición** va en renglones (fibra del catálogo ISO 2076 + % +
  reciclado), no en un texto: así 65 % PES / 35 % CO se puede buscar y sumar.
- **Dónde se usa**: los renglones de hilo de las fichas de tejido que apuntan al
  producto, con su máquina; de ahí salen las telas y las máquinas que lo corren.

Las propiedades se pueden **proponer desde el nombre del producto** («HILO
POLIÉSTER 75/36 STRETCH TORSIÓN Z» → 75 den, 36 filamentos, torsión Z,
texturizado stretch, 100 % PES). Solo se llena lo que el nombre dice; lo demás
queda vacío.
"""
import re

from odoo import api, fields, models
from odoo.exceptions import UserError, ValidationError

from .maquina_config import _fmt

# Claves de los tipos y parámetros de hilo que siembra el módulo (data/ficha_tecnica_material_data.xml).
TIPO_FILAMENTO = 'hilo_filamento'
TIPO_HILADO = 'hilo_hilado'
# «75/36», «22/1», «75/36/2»; los pares que suman 100 son la mezcla (65/35), no el título.
TITULO_RE = re.compile(r'(\d+(?:[.,]\d+)?)\s*/\s*(\d+)(?:\s*/\s*(\d+))?')
TORSION_RE = re.compile(r'TORSI[OÓ]N\s*[“"\']?\s*([SZ])\b|[“"]([SZ])[”"]')
# Fibra (código ISO 2076) por palabra de la categoría o del nombre del producto.
FIBRA_POR_PALABRA = (('poliéster', 'PES'), ('poliester', 'PES'), ('pet', 'PES'), ('nylon', 'PA'),
                     ('poliamida', 'PA'), ('algodón', 'CO'), ('algodon', 'CO'), ('viscosa', 'CV'),
                     ('bamboo', 'CV'), ('bambú', 'CV'), ('elastano', 'EL'))
FIBRA_POR_SIGLA = {'PES': 'PES', 'PET': 'PES', 'CO': 'CO', 'PA': 'PA', 'CV': 'CV', 'EL': 'EL'}


class FichaTecnicaFibra(models.Model):
    """Fibra del catálogo de composición (siglas de la norma ISO 2076)."""
    _name = 'ficha.tecnica.fibra'
    _description = 'Fibra (composición de materias primas)'
    _order = 'sequence, name, id'

    sequence = fields.Integer(default=10)
    code = fields.Char(string='Sigla', required=True, help='Sigla de la fibra según ISO 2076 (PES, CO, PA, EL, CV…).')
    name = fields.Char(string='Fibra', required=True)
    active = fields.Boolean(default=True)

    _code_uniq = models.Constraint('unique(code)', 'La sigla de la fibra debe ser única.')

    @api.depends('code', 'name')
    def _compute_display_name(self):
        for fibra in self:
            fibra.display_name = '%s (%s)' % (fibra.name, fibra.code) if fibra.code else fibra.name


class FichaTecnicaMaterial(models.Model):
    _name = 'ficha.tecnica.material'
    _description = 'Ficha de materia prima'
    _order = 'default_code, id'

    product_id = fields.Many2one('product.product', string='Producto', required=True, index=True,
                                 ondelete='restrict', help='La materia prima en Inventario (hilo, químico, colorante…).')
    product_tmpl_id = fields.Many2one(related='product_id.product_tmpl_id', store=True, string='Plantilla')
    default_code = fields.Char(related='product_id.default_code', store=True, string='Referencia')
    categ_id = fields.Many2one(related='product_id.categ_id', store=True, string='Categoría')
    tipo_id = fields.Many2one('ficha.tecnica.maquina.tipo', string='Tipo de materia prima', required=True,
                              index=True, domain="[('clase', '=', 'material')]",
                              compute='_compute_tipo_id', store=True, readonly=False, precompute=True,
                              help='Define qué propiedades se capturan. Se propone por la categoría del producto o '
                                   'por su título (75/36 es filamento; 22/1, hilado).')
    valor_ids = fields.One2many('ficha.tecnica.material.valor', 'material_id', string='Propiedades',
                                context={'active_test': False})
    resumen = fields.Char(string='Resumen de propiedades', compute='_compute_resumen')
    composicion_ids = fields.One2many('ficha.tecnica.material.composicion', 'material_id', string='Composición')
    composicion = fields.Char(string='Composición (resumen)', compute='_compute_composicion')
    composicion_aviso = fields.Char(string='Aviso de composición', compute='_compute_composicion')
    reciclado_pct = fields.Float(string='Contenido reciclado (%)', digits=(16, 1), compute='_compute_composicion')
    proveedor_ids = fields.Many2many('res.partner', string='Proveedores', compute='_compute_proveedor_ids',
                                     help='Los proveedores del producto en Compras (pestaña Compra del producto).')
    hilo_ids = fields.Many2many('ficha.tecnica.tejido.hilo', string='Dónde se usa', compute='_compute_uso')
    tela_count = fields.Integer(string='No. de telas', compute='_compute_uso')
    maquina_count = fields.Integer(string='No. de máquinas', compute='_compute_uso')
    fecha = fields.Date(string='Fecha del dato', default=fields.Date.context_today)
    fuente = fields.Char(string='Fuente', help='De dónde salen las propiedades (ficha del proveedor, laboratorio…).')
    notas = fields.Text(string='Notas')
    active = fields.Boolean(default=True)

    _product_uniq = models.Constraint('unique(product_id)', 'Ese producto ya tiene ficha de materia prima.')

    @api.depends('product_id')
    def _compute_tipo_id(self):
        for material in self:
            if not material.tipo_id and material.product_id:
                material.tipo_id = self._ft_tipo_sugerido(material.product_id)

    @api.depends('product_id.display_name')
    def _compute_display_name(self):
        for material in self:
            material.display_name = material.product_id.display_name or ''

    @api.depends('valor_ids.valor', 'valor_ids.valor_texto', 'valor_ids.parametro_id.name',
                 'valor_ids.parametro_id.unit')
    def _compute_resumen(self):
        for material in self:
            parts = []
            for line in material.valor_ids:
                shown = line._display_value()
                if shown:
                    parts.append('%s: %s' % (line.parametro_id.name, shown))
            material.resumen = ' · '.join(parts)

    @api.depends('composicion_ids.fibra_id', 'composicion_ids.porcentaje', 'composicion_ids.reciclado')
    def _compute_composicion(self):
        for material in self:
            lines = material.composicion_ids
            material.composicion = ' · '.join('%s %% %s%s' % (_fmt(c.porcentaje), c.fibra_id.code or '',
                                                              ' reciclado' if c.reciclado else '') for c in lines)
            total = sum(lines.mapped('porcentaje'))
            material.reciclado_pct = sum(lines.filtered('reciclado').mapped('porcentaje'))
            material.composicion_aviso = ('La composición suma %s %%, no 100 %%.' % _fmt(total)) \
                if lines and abs(total - 100.0) > 0.05 else False

    @api.depends('product_id.seller_ids.partner_id')
    def _compute_proveedor_ids(self):
        for material in self:
            material.proveedor_ids = material.product_id._ft_proveedores()

    def _compute_uso(self):
        Hilo = self.env['ficha.tecnica.tejido.hilo']
        for material in self:
            hilos = Hilo.search([('product_id', '=', material.product_id.id)]) if material.product_id else Hilo
            material.hilo_ids = hilos
            material.tela_count = len(hilos.ficha_id)
            material.maquina_count = len(hilos.workcenter_id)

    # ------------------------------------------------------------------
    # Propiedades (mismo manejo que la configuración de la máquina)
    # ------------------------------------------------------------------
    def _ft_value(self, code):
        """Valor de la propiedad ``code`` (número o texto); False si no está o va vacía."""
        self.ensure_one()
        line = self.valor_ids.filtered(lambda v: v.code == code)[:1]
        if not line:
            return False
        return (line.valor_texto if line.kind == 'text' else line.valor) or False

    def _ft_set_value(self, code, value):
        """Escribe la propiedad ``code`` del tipo del material (crea el renglón si falta)."""
        self.ensure_one()
        line = self.valor_ids.filtered(lambda v: v.code == code)[:1]
        if not line:
            param = self.tipo_id._ft_parametros('material').filtered(lambda p: p.code == code)[:1]
            if not param:
                raise ValidationError('El tipo «%s» no tiene la propiedad «%s»; dele de alta en Configuración → '
                                      'Tipos de materia prima.' % (self.tipo_id.name or '(sin tipo)', code))
            line = self.env['ficha.tecnica.material.valor'].create({'material_id': self.id, 'parametro_id': param.id})
        line.write({'valor_texto': value or False} if line.kind == 'text' else {'valor': value or 0.0})
        return line

    def _ft_sync_valores(self):
        """Un renglón por propiedad activa del tipo (vacío si no se ha dado); quita los de otro tipo vacíos."""
        Valor = self.env['ficha.tecnica.material.valor']
        for material in self:
            otros = material.valor_ids.filtered(lambda v: v.parametro_id.tipo_id != material.tipo_id)
            if otros.filtered(lambda v: v.valor or v.valor_texto):
                raise UserError('La ficha %s ya tiene propiedades de otro tipo capturadas: no se cambia de tipo; '
                                'archívela y haga otra.' % material.display_name)
            otros.unlink()
            present = material.valor_ids.parametro_id
            missing = material.tipo_id._ft_parametros('material').filtered(lambda p: p not in present)
            if missing:
                Valor.create([{'material_id': material.id, 'parametro_id': p.id} for p in missing])

    @api.model_create_multi
    def create(self, vals_list):
        materials = super().create(vals_list)
        materials._ft_sync_valores()
        return materials

    def write(self, vals):
        res = super().write(vals)
        if 'tipo_id' in vals:
            self._ft_sync_valores()
        return res

    # ------------------------------------------------------------------
    # Tipo sugerido y propuesta desde el nombre del producto
    # ------------------------------------------------------------------
    @api.model
    def _ft_tipo_sugerido(self, product):
        """El tipo de materia prima que le toca a ``product``: el que tiene su categoría (o una madre); si no, por
        el título del nombre (filamento o hilado) cuando es un hilo (categoría «… Hilo …» o nombre «HILO …»)."""
        Tipo = self.env['ficha.tecnica.maquina.tipo']
        if not product:
            return Tipo
        for tipo in Tipo.search([('clase', '=', 'material'), ('categ_ids', '!=', False)]):
            if product.categ_id and product.categ_id.parent_path and any(
                    product.categ_id.parent_path.startswith(c.parent_path) for c in tipo.categ_ids if c.parent_path):
                return tipo
        es_hilo = 'hilo' in (product.categ_id.complete_name or '').lower() or \
            (product.name or '').upper().startswith('HILO')
        titulo = self._ft_titulo(product) if es_hilo else None
        if titulo:
            code = TIPO_FILAMENTO if titulo[0] == 'filamento' else TIPO_HILADO
            return Tipo.search([('clase', '=', 'material'), ('code', '=', code)], limit=1)
        return Tipo

    @api.model
    def _ft_titulo(self, product):
        """('filamento', den, filamentos, cabos) o ('hilado', Ne, cabos, None) del nombre; None si no lo dice."""
        matches = TITULO_RE.findall((product.name or '').upper()) or TITULO_RE.findall(
            (product.default_code or '').upper())
        for match in reversed(matches):
            primero, segundo, tercero = float(match[0].replace(',', '.')), int(match[1]), match[2]
            if abs(primero + segundo - 100.0) < 0.01 and not tercero:
                continue  # 65/35: es la mezcla, no el título
            if segundo <= 3:
                return 'hilado', primero, segundo, None
            cabos = int(tercero) if tercero else None
            return 'filamento', primero, segundo, cabos
        return None

    @api.model
    def _ft_propuesta(self, product):
        """({clave: valor}, [(sigla de fibra, %, reciclado)]) que dice el nombre del producto. Solo lo que dice."""
        nombre = ' '.join(p for p in (product.name, product.default_code) if p).upper()
        palabras = set(re.findall(r'[A-ZÁÉÍÓÚÑ]+', nombre))
        valores = {}
        titulo = self._ft_titulo(product)
        if titulo and titulo[0] == 'filamento':
            valores.update({'titulo_den': titulo[1], 'filamentos': titulo[2]})
            if titulo[3]:
                valores['cabos'] = titulo[3]
            if 'STRETCH' in nombre or 'STRECH' in nombre:
                valores['textura'] = 'Texturizado semistretch' if 'SEMI' in nombre else 'Texturizado stretch'
            elif 'SET' in palabras:
                valores['textura'] = 'Texturizado SET'
            elif 'RIG' in nombre:
                valores['textura'] = 'Rígido (FDY)'
            if 'OPACO' in palabras:
                valores['lustre'] = 'Opaco'
            elif 'BRILLANTE' in palabras or 'BRILL' in nombre:
                valores['lustre'] = 'Brillante'
        elif titulo:
            valores.update({'titulo_ne': titulo[1], 'cabos': titulo[2]})
            if 'PEINADO' in palabras:
                valores['hilatura'] = 'Anillo peinado'
            elif 'OE' in palabras or 'OE' in (product.default_code or '').upper():
                valores['hilatura'] = 'Open end'
            elif 'COMPACTADO' in palabras or 'COMPACTO' in palabras:
                valores['hilatura'] = 'Compacto'
        match = TORSION_RE.search(nombre)
        if match:
            valores['torsion_sentido'] = match.group(1) or match.group(2)
        if 'NEGRO' in palabras:
            valores['color'] = 'Negro'
        elif 'NATURAL' in palabras:
            valores['color'] = 'Natural'
        return valores, self._ft_composicion_propuesta(product, nombre, palabras)

    @api.model
    def _ft_composicion_propuesta(self, product, nombre, palabras):
        reciclado = bool(palabras & {'RECICLADO', 'REC', 'SIMREC'}) or 'PET' in palabras
        siglas = re.findall(r'\b(PES|PET|CO|PA|CV|EL)\s*/\s*(PES|PET|CO|PA|CV|EL)\b', nombre)
        if siglas:
            mezcla = [m for m in TITULO_RE.findall(product.name.upper() if product.name else '')
                      if abs(float(m[0].replace(',', '.')) + int(m[1]) - 100.0) < 0.01 and not m[2]]
            if not mezcla:
                return []  # «PES/CO 22/1» sin proporción: no se adivina
            a, b = FIBRA_POR_SIGLA[siglas[0][0]], FIBRA_POR_SIGLA[siglas[0][1]]
            pa, pb = float(mezcla[0][0].replace(',', '.')), float(mezcla[0][1])
            return [(a, pa, reciclado and a == 'PES'), (b, pb, reciclado and b == 'PES')]
        texto = ' '.join(p for p in (product.categ_id.name, product.name) if p).lower()
        if 'mezcla' in texto:
            return []
        for palabra, sigla in FIBRA_POR_PALABRA:
            if re.search(r'\b%s\b' % re.escape(palabra), texto):
                return [(sigla, 100.0, reciclado and sigla == 'PES')]
        return []

    def action_ft_proponer(self):
        """Llena con lo que dice el nombre del producto las propiedades y la composición que estén vacías. Lo que
        ya tiene dato no se toca."""
        Fibra = self.env['ficha.tecnica.fibra']
        for material in self:
            valores, composicion = self._ft_propuesta(material.product_id)
            for code, value in valores.items():
                line = material.valor_ids.filtered(lambda v: v.code == code)[:1]
                if line and not (line.valor or line.valor_texto):
                    material._ft_set_value(code, value)
            if composicion and not material.composicion_ids:
                lines = []
                for sigla, pct, reciclado in composicion:
                    fibra = Fibra.search([('code', '=', sigla)], limit=1)
                    if fibra:
                        lines.append((0, 0, {'fibra_id': fibra.id, 'porcentaje': pct, 'reciclado': reciclado}))
                if len(lines) == len(composicion):
                    material.composicion_ids = lines
        return True

    # ------------------------------------------------------------------
    # Navegación
    # ------------------------------------------------------------------
    def action_ft_telas(self):
        self.ensure_one()
        return {'type': 'ir.actions.act_window', 'name': 'Telas con %s' % self.display_name,
                'res_model': 'ficha.tecnica.tejido', 'view_mode': 'list,form',
                'domain': [('id', 'in', self.hilo_ids.ficha_id.ids)]}

    def action_ft_maquinas(self):
        self.ensure_one()
        return {'type': 'ir.actions.act_window', 'name': 'Máquinas que corren %s' % self.display_name,
                'res_model': 'mrp.workcenter', 'view_mode': 'list,form',
                'domain': [('id', 'in', self.hilo_ids.workcenter_id.ids)]}

    def action_ft_uso(self):
        self.ensure_one()
        return {'type': 'ir.actions.act_window', 'name': 'Dónde se usa %s' % self.display_name,
                'res_model': 'ficha.tecnica.tejido.hilo', 'view_mode': 'list,pivot',
                'domain': [('product_id', '=', self.product_id.id)]}


class FichaTecnicaMaterialValor(models.Model):
    """Valor de una propiedad en la ficha de materia prima."""
    _name = 'ficha.tecnica.material.valor'
    _inherit = ['ficha.tecnica.valor.mixin']
    _description = 'Propiedad de la materia prima'
    _order = 'material_id, sequence, id'

    material_id = fields.Many2one('ficha.tecnica.material', string='Materia prima', required=True, index=True,
                                  ondelete='cascade')
    # Guardados para la tabla dinámica (material × propiedad) y los filtros.
    product_id = fields.Many2one(related='material_id.product_id', store=True, index=True, string='Producto')
    tipo_id = fields.Many2one(related='material_id.tipo_id', store=True, string='Tipo de materia prima')
    active = fields.Boolean(related='material_id.active', store=True)

    _material_parametro_uniq = models.Constraint('unique(material_id, parametro_id)',
                                                 'La propiedad ya está en esa ficha.')

    @api.constrains('parametro_id', 'material_id')
    def _check_tipo(self):
        for line in self:
            if line.parametro_id.tipo_id != line.material_id.tipo_id:
                raise ValidationError('La propiedad «%s» es de %s y la ficha %s es de %s.' % (
                    line.parametro_id.name, line.parametro_id.tipo_id.name, line.material_id.display_name,
                    line.material_id.tipo_id.name or '(sin tipo)'))


class FichaTecnicaMaterialComposicion(models.Model):
    """Una fibra de la composición de la materia prima."""
    _name = 'ficha.tecnica.material.composicion'
    _description = 'Composición de la materia prima'
    _order = 'material_id, sequence, id'

    material_id = fields.Many2one('ficha.tecnica.material', string='Materia prima', required=True, index=True,
                                  ondelete='cascade')
    sequence = fields.Integer(default=10)
    fibra_id = fields.Many2one('ficha.tecnica.fibra', string='Fibra', required=True, ondelete='restrict')
    porcentaje = fields.Float(string='%', digits=(16, 2))
    reciclado = fields.Boolean(string='Reciclado', help='La fibra es reciclada (p. ej. PET de botella).')

    @api.constrains('porcentaje')
    def _check_porcentaje(self):
        for line in self:
            if not 0 < line.porcentaje <= 100:
                raise ValidationError('El porcentaje de %s debe estar entre 0 y 100.' % line.fibra_id.display_name)


class ProductProductMaterial(models.Model):
    _inherit = 'product.product'

    ft_material_id = fields.Many2one('ficha.tecnica.material', string='Ficha de materia prima',
                                     compute='_compute_ft_material')
    ft_material_count = fields.Integer(string='No. de fichas de materia prima', compute='_compute_ft_material')
    ft_hilo_tela_count = fields.Integer(string='No. de telas que lo usan', compute='_compute_ft_material')
    ft_material_sugerido = fields.Boolean(string='Puede tener ficha de materia prima', compute='_compute_ft_material',
                                          help='Hay un tipo de materia prima para él (por su categoría o su título).')

    def _compute_ft_material(self):
        Material = self.env['ficha.tecnica.material'].with_context(active_test=False)
        Hilo = self.env['ficha.tecnica.tejido.hilo']
        materials = {m.product_id.id: m for m in Material.search([('product_id', 'in', self.ids)])}
        data = Hilo._read_group([('product_id', 'in', self.ids)], ['product_id'], ['ficha_id:count_distinct'])
        telas = {product.id: count for product, count in data}
        for product in self:
            product.ft_material_id = materials.get(product.id, False)
            product.ft_material_count = 1 if product.ft_material_id else 0
            product.ft_hilo_tela_count = telas.get(product.id, 0)
            product.ft_material_sugerido = bool(product.ft_material_id or Material._ft_tipo_sugerido(product))

    def _ft_proveedores(self):
        """Los proveedores del producto (de la variante o de la plantilla)."""
        self.ensure_one()
        sellers = self.product_tmpl_id.seller_ids.filtered(lambda s: not s.product_id or s.product_id == self)
        return sellers.partner_id

    def action_ft_material(self):
        """Abre la ficha de materia prima del producto; si no tiene, la crea con lo que dice su nombre."""
        self.ensure_one()
        material = self.env['ficha.tecnica.material'].with_context(active_test=False).search(
            [('product_id', '=', self.id)], limit=1)
        if not material:
            tipo = self.env['ficha.tecnica.material']._ft_tipo_sugerido(self)
            if not tipo:
                return {'type': 'ir.actions.act_window', 'name': 'Ficha de materia prima',
                        'res_model': 'ficha.tecnica.material', 'view_mode': 'form', 'target': 'current',
                        'context': {'default_product_id': self.id}}
            material = self.env['ficha.tecnica.material'].create({'product_id': self.id, 'tipo_id': tipo.id})
            material.action_ft_proponer()
        return {'type': 'ir.actions.act_window', 'name': 'Ficha de materia prima',
                'res_model': 'ficha.tecnica.material', 'view_mode': 'form', 'res_id': material.id}

    def action_ft_crear_fichas_material(self):
        """Acción de la lista de productos: crea la ficha de los seleccionados que no tienen (con el tipo
        sugerido y lo que dice el nombre) y las muestra. Los que no tienen tipo sugerido se reportan."""
        Material = self.env['ficha.tecnica.material']
        nuevos, sin_tipo = Material, self.browse()
        con_ficha = Material.with_context(active_test=False).search([('product_id', 'in', self.ids)]).product_id
        for product in self - con_ficha:
            tipo = Material._ft_tipo_sugerido(product)
            if not tipo:
                sin_tipo |= product
                continue
            nuevos |= Material.create({'product_id': product.id, 'tipo_id': tipo.id})
        nuevos.action_ft_proponer()
        todos = Material.search([('product_id', 'in', self.ids)])
        action = {'type': 'ir.actions.act_window', 'name': 'Fichas de materia prima',
                  'res_model': 'ficha.tecnica.material', 'view_mode': 'list,form',
                  'domain': [('id', 'in', todos.ids)]}
        if sin_tipo:
            action['help'] = 'Sin tipo sugerido (no se crearon): %s' % ', '.join(sin_tipo.mapped('display_name'))
        return action

    def action_ft_telas_hilo(self):
        self.ensure_one()
        hilos = self.env['ficha.tecnica.tejido.hilo'].search([('product_id', '=', self.id)])
        return {'type': 'ir.actions.act_window', 'name': 'Telas con %s' % self.display_name,
                'res_model': 'ficha.tecnica.tejido', 'view_mode': 'list,form',
                'domain': [('id', 'in', hilos.ficha_id.ids)]}


class ProductTemplateMaterial(models.Model):
    _inherit = 'product.template'

    ft_material_count = fields.Integer(string='No. de fichas de materia prima', compute='_compute_ft_material')
    ft_hilo_tela_count = fields.Integer(string='No. de telas que lo usan', compute='_compute_ft_material')
    ft_material_sugerido = fields.Boolean(string='Puede tener ficha de materia prima', compute='_compute_ft_material')

    def _compute_ft_material(self):
        for tmpl in self:
            variants = tmpl.product_variant_ids
            tmpl.ft_material_count = sum(variants.mapped('ft_material_count'))
            tmpl.ft_hilo_tela_count = sum(variants.mapped('ft_hilo_tela_count'))
            tmpl.ft_material_sugerido = any(variants.mapped('ft_material_sugerido'))

    def action_ft_material(self):
        self.ensure_one()
        return self.product_variant_ids[:1].action_ft_material()

    def action_ft_telas_hilo(self):
        self.ensure_one()
        hilos = self.env['ficha.tecnica.tejido.hilo'].search([('product_id', 'in', self.product_variant_ids.ids)])
        return {'type': 'ir.actions.act_window', 'name': 'Telas con %s' % self.display_name,
                'res_model': 'ficha.tecnica.tejido', 'view_mode': 'list,form',
                'domain': [('id', 'in', hilos.ficha_id.ids)]}


class MrpWorkcenterMaterial(models.Model):
    _inherit = 'mrp.workcenter'

    ft_hilo_count = fields.Integer(string='No. de hilos que corre', compute='_compute_ft_hilo_count')

    def _compute_ft_hilo_count(self):
        data = self.env['ficha.tecnica.tejido.hilo']._read_group(
            [('workcenter_id', 'in', self.ids), ('product_id', '!=', False)], ['workcenter_id'],
            ['product_id:count_distinct'])
        counts = {wc.id: count for wc, count in data}
        for wc in self:
            wc.ft_hilo_count = counts.get(wc.id, 0)

    def action_ft_hilos(self):
        self.ensure_one()
        return {'type': 'ir.actions.act_window', 'name': 'Hilos que corre %s' % self.name,
                'res_model': 'ficha.tecnica.tejido.hilo', 'view_mode': 'list,pivot',
                'domain': [('workcenter_id', '=', self.id)]}
