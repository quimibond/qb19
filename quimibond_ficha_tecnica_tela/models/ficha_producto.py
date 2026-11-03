# -*- coding: utf-8 -*-
"""La ficha técnica en las pantallas nativas de Odoo (2.25.0, Dirección General 2026-11-03).

Decisión: en lugar de fichas aparte, la gente ve la ficha donde Odoo ya pone las cosas:

- **Producto** (la tela, su producto en kg o el crudo): pestaña «Ficha técnica» con los datos de la tela (peso,
  ancho, rendimiento, editables ahí mismo), su crudo, la ruta que sale de sus listas de materiales (operaciones con la
  duración de la lista contra la de sus parámetros), la especificación y los pendientes de la revisión (2.24.0). En un
  crudo: sus datos, las circulares donde se teje y las telas que lo usan.
- **Operación** de la lista de materiales: los parámetros del proceso de la tela en su máquina y en sus alternativas.
- **Centro de trabajo**: las telas que corren ahí.

Fase 1: los datos siguen guardados en las fichas de tejido y de acabado y en los procesos por máquina, porque los leen
el SGI, el costeo y la Programación de Tintorería; estas pantallas los muestran y editan desde lo nativo. La app
«Fichas técnicas» queda como concentrado: Telas y Crudos abren el producto.
"""
from odoo import api, fields, models

# 2.27.0: dónde va cada tela en el camino a producción (de la revisión de la noche o de «Volver a revisar»; si la lista
# en kg se activó después, ya cuenta: se lee de la lista).
FT_ESTADOS = [
    ('sin_revisar', 'Sin revisar'),
    ('pendientes', 'Con pendientes'),
    ('para_activar', 'Lista para activar'),
    ('activa_pendientes', 'Activa con pendientes'),
    ('activa', 'Activa'),
]
FT_AREAS = [
    ('ingenieria', 'Ingeniería (Innovación)'),
    ('tintoreria', 'Tintorería'),
    ('acabado', 'Acabado (ramas)'),
    ('inspeccion', 'Inspección'),
    ('manufactura', 'Manufactura (devanado y abridora)'),
]


def ft_tipo_codigo(code):
    """Qué es un producto por su código (DAT P-D02-01): 'tela' (J en la posición 9), 'kg' (la tela con «I» al
    frente), 'crudo' (H en la posición 9) o '' si el código no sigue la regla."""
    from .acabado_ruta import PREFIJO_KG, codigo_proceso
    code = (code or '').strip().upper()
    if code.startswith(PREFIJO_KG) and codigo_proceso(code[1:]):
        return 'kg'
    if codigo_proceso(code):
        return 'tela'
    if len(code) in (14, 16) and code[8:9] == 'H':
        return 'crudo'
    return ''


class ProductTemplateFichaNativa(models.Model):
    _inherit = 'product.template'

    # 2.25.0: todo calculado, nunca «related» a través de la ficha: la ficha del producto es un campo calculado sin
    # guardar y un related sobre él hace que Odoo busque por ese campo cada vez que cambia la ficha (y revienta).
    ft_tiene_ficha = fields.Boolean(string='Tiene ficha técnica', compute='_compute_ft_ficha_nativa')
    ft_es_tela = fields.Boolean(string='Es tela con ficha', compute='_compute_ft_ficha_nativa', search='_search_ft_es_tela',
                                help='Tela acabada (en metros) o su acabado en proceso (kg) con ficha de acabado.')
    ft_es_crudo = fields.Boolean(string='Es crudo con ficha', compute='_compute_ft_ficha_nativa',
                                 search='_search_ft_es_crudo', help='Crudo con ficha de tejido.')
    # La tela: se editan aquí y se guardan en su ficha de acabado.
    ft_peso = fields.Float(string='Peso de la tela (g/m²)', compute='_compute_ft_tela_datos',
                           inverse='_inverse_ft_tela_datos')
    ft_ancho = fields.Float(string='Ancho de la tela (cm)', compute='_compute_ft_tela_datos',
                            inverse='_inverse_ft_tela_datos')
    ft_rendimiento = fields.Float(string='Rendimiento acabado (m/kg)', digits=(12, 4), compute='_compute_ft_tela_datos',
                                  inverse='_inverse_ft_tela_datos')
    ft_producto_kg_id = fields.Many2one('product.product', string='Producto en kg (proceso)',
                                        compute='_compute_ft_ficha_nativa')
    ft_producto_m_id = fields.Many2one('product.product', string='Tela acabada (en metros)',
                                       compute='_compute_ft_ficha_nativa')
    ft_spec_ids = fields.Many2many('ficha.tecnica.spec', string='Especificación', compute='_compute_ft_ficha_nativa')
    # El crudo de la tela.
    ft_crudo_id = fields.Many2one('product.product', string='Crudo de la tela', compute='_compute_ft_ficha_nativa',
                                  search='_search_ft_crudo_id')
    ft_crudo_peso = fields.Float(string='Peso del crudo (g/m²)', compute='_compute_ft_ficha_nativa')
    ft_crudo_ancho = fields.Float(string='Ancho del crudo', compute='_compute_ft_ficha_nativa')
    ft_crudo_rendimiento = fields.Float(string='Rendimiento de tejido (m/kg)', digits=(12, 4),
                                        compute='_compute_ft_ficha_nativa')
    ft_crudo_telas_count = fields.Integer(string='Telas con este crudo', compute='_compute_ft_ficha_nativa')
    # La ruta y la revisión.
    ft_operacion_ids = fields.Many2many('mrp.routing.workcenter', string='Ruta (operaciones de sus listas)',
                                        compute='_compute_ft_ficha_nativa', context={'active_test': False})
    ft_bom_kg_id = fields.Many2one('mrp.bom', string='Lista en kg', compute='_compute_ft_ficha_nativa')
    ft_bom_m_id = fields.Many2one('mrp.bom', string='Lista en metros', compute='_compute_ft_ficha_nativa')
    ft_revision_id = fields.Many2one('ficha.tecnica.revision', string='Revisión de la tela',
                                     compute='_compute_ft_ficha_nativa')
    ft_revision_listo = fields.Boolean(string='Listas cuadran con la ficha', compute='_compute_ft_ficha_nativa')
    ft_revision_n = fields.Integer(string='Pendientes de la tela', compute='_compute_ft_ficha_nativa')
    ft_pendiente_ids = fields.Many2many('ficha.tecnica.revision.pendiente', string='Pendientes por área',
                                        compute='_compute_ft_ficha_nativa')
    ft_estado = fields.Selection(FT_ESTADOS, string='Estado de la ficha', compute='_compute_ft_ficha_nativa',
                                 search='_search_ft_estado',
                                 help='Sin revisar; con pendientes; lista para activar (las listas cuadran y la lista en '
                                      'kg sigue archivada: la activa Ingeniería); activa con pendientes (Odoo ya '
                                      'fabrica con ella y algo no cuadra); activa.')
    ft_revision_areas = fields.Char(string='Áreas con pendientes', compute='_compute_ft_ficha_nativa')
    ft_area_pendiente = fields.Selection(FT_AREAS, string='Área con pendientes', compute='_compute_ft_area_pendiente',
                                         search='_search_ft_area_pendiente')
    # 2.27.0: un producto con código de tela, de acabado en kg o de crudo y sin ficha: la pestaña ofrece crearla.
    ft_tipo_codigo = fields.Selection([('tela', 'Tela acabada'), ('kg', 'Acabado en kg'), ('crudo', 'Crudo')],
                                      string='Tipo por código', compute='_compute_ft_tipo_codigo')
    ft_tela_m_de_kg_id = fields.Many2one('product.product', string='Tela en metros de este acabado en kg',
                                         compute='_compute_ft_tipo_codigo')
    # El crudo visto desde su propio producto.
    ft_tejido_ids = fields.Many2many('ficha.tecnica.tejido', string='Circulares donde se teje',
                                     compute='_compute_ft_ficha_nativa')
    ft_tela_ids = fields.Many2many('ficha.tecnica.acabado', string='Telas que usan este crudo',
                                   compute='_compute_ft_ficha_nativa')

    @api.depends('product_variant_ids')
    def _compute_ft_tela_datos(self):
        for tmpl in self:
            acabado = tmpl.ficha_tecnica_acabado_id
            tmpl.ft_peso = acabado.peso_acabado
            tmpl.ft_ancho = acabado.ancho_acabado
            tmpl.ft_rendimiento = acabado.rendimiento_tela_acabada

    def _inverse_ft_tela_datos(self):
        for tmpl in self:
            acabado = tmpl.ficha_tecnica_acabado_id
            if acabado:
                acabado.write({'peso_acabado': tmpl.ft_peso, 'ancho_acabado': tmpl.ft_ancho,
                               'rendimiento_tela_acabada': tmpl.ft_rendimiento})

    def _ft_variantes(self):
        return self.with_context(active_test=False).product_variant_ids

    @api.depends('product_variant_ids')
    def _compute_ft_ficha_nativa(self):
        Acabado = self.env['ficha.tecnica.acabado']
        Tejido = self.env['ficha.tecnica.tejido']
        Revision = self.env['ficha.tecnica.revision']
        for tmpl in self:
            variantes = tmpl._ft_variantes()
            acabado = tmpl.ficha_tecnica_acabado_id
            tejidos = Tejido.search([('product_proceso_id', 'in', variantes.ids)]) if variantes else Tejido
            crudo_ficha = acabado.tejido_id
            crudo = crudo_ficha.product_proceso_id
            tmpl.ft_es_tela = bool(acabado)
            tmpl.ft_es_crudo = bool(tejidos) and not acabado
            tmpl.ft_tiene_ficha = bool(acabado or tejidos)
            tmpl.ft_crudo_id = crudo
            tmpl.ft_crudo_peso = crudo_ficha.peso_acondicionado
            tmpl.ft_crudo_ancho = crudo_ficha.ancho_acondicionado
            tmpl.ft_crudo_rendimiento = crudo_ficha.rendimiento_tela_tejida
            tmpl.ft_producto_kg_id = acabado.product_proceso_id
            tmpl.ft_producto_m_id = acabado.product_acabado_id
            tmpl.ft_spec_ids = acabado.spec_line_ids
            tmpl.ft_crudo_telas_count = Acabado.search_count(
                [('tejido_id.product_proceso_id', '=', crudo.id)]) if crudo else 0
            bom_kg = Revision._ft_bom_de(acabado.product_proceso_id) if acabado else self.env['mrp.bom']
            bom_m = Revision._ft_bom_de(acabado.product_acabado_id) if acabado else self.env['mrp.bom']
            tmpl.ft_bom_kg_id = bom_kg
            tmpl.ft_bom_m_id = bom_m
            tmpl.ft_operacion_ids = (bom_kg | bom_m).with_context(active_test=False).operation_ids
            revision = Revision.search([('acabado_id', '=', acabado.id)], limit=1) if acabado else Revision
            tmpl.ft_revision_id = revision
            tmpl.ft_revision_listo = revision.listo
            tmpl.ft_revision_n = revision.n_pendientes
            tmpl.ft_pendiente_ids = revision.pendiente_ids
            tmpl.ft_estado = tmpl._ft_estado_de(revision) if acabado else False
            tmpl.ft_revision_areas = revision.areas
            tmpl.ft_tejido_ids = tejidos
            tmpl.ft_tela_ids = Acabado.search([('tejido_id.product_proceso_id', 'in', variantes.ids)]) \
                if tejidos else Acabado

    @api.model
    def _ft_estado_de(self, revision):
        if not revision:
            return 'sin_revisar'
        # 2.27.1: «activa» solo si es la lista del flujo (con operaciones); la anterior activa no cuenta.
        bom = revision.bom_kg_id
        activa = bom.active and bool(bom.with_context(active_test=False).operation_ids)
        if revision.listo:
            return 'activa' if activa else 'para_activar'
        return 'activa_pendientes' if activa else 'pendientes'

    def _compute_ft_area_pendiente(self):
        # Solo para filtrar (una tela puede tener pendientes de varias áreas: ver «Áreas con pendientes»).
        self.ft_area_pendiente = False

    @api.depends('default_code')
    def _compute_ft_tipo_codigo(self):
        from .acabado_ruta import PREFIJO_KG
        Product = self.env['product.product']
        for tmpl in self:
            tipo = ft_tipo_codigo(tmpl.default_code)
            tmpl.ft_tipo_codigo = tipo or False
            tmpl.ft_tela_m_de_kg_id = Product.search(
                [('default_code', '=', tmpl.default_code.strip()[len(PREFIJO_KG):])], limit=1) \
                if tipo == 'kg' else Product

    @staticmethod
    def _ft_valores(value):
        # Odoo 19 pasa '=' como 'in' con un OrderedSet (iterable, no hashable).
        return {value} if isinstance(value, (str, bytes)) or not hasattr(value, '__iter__') else set(value)

    def _ft_tmpl_de_revisiones(self, revisiones):
        return set(revisiones.acabado_id.product_acabado_id.product_tmpl_id.ids)

    def _search_ft_estado(self, operator, value):
        if operator not in ('=', 'in'):
            return NotImplemented
        quiero = self._ft_valores(value)
        ids = set()
        revisiones = self.env['ficha.tecnica.revision'].search([])
        for revision in revisiones:
            if self._ft_estado_de(revision) in quiero:
                ids |= self._ft_tmpl_de_revisiones(revision)
        if 'sin_revisar' in quiero:
            telas = self.search([('ft_es_tela', '=', True)])
            ids |= set(telas.ids) - self._ft_tmpl_de_revisiones(revisiones)
        return [('id', 'in', list(ids))]

    def _search_ft_area_pendiente(self, operator, value):
        if operator not in ('=', 'in'):
            return NotImplemented
        areas = [area for area in self._ft_valores(value) if area in dict(FT_AREAS)]
        if not areas:
            return [('id', 'in', [])]
        dominio = ['|'] * (len(areas) - 1) + [('pend_%s' % area, '=', True) for area in areas]
        return [('id', 'in', list(self._ft_tmpl_de_revisiones(self.env['ficha.tecnica.revision'].search(dominio))))]

    def _search_ft_crudo_id(self, operator, value):
        """«Buscar por crudo» en el concentrado: las telas cuya ficha de acabado se basa en ese crudo."""
        acabados = self.env['ficha.tecnica.acabado'].search([('tejido_id.product_proceso_id', operator, value)])
        return [('id', 'in', acabados.product_acabado_id.product_tmpl_id.ids)]

    @api.model
    def _ft_buscar_bool(self, operator, value, tmpl_ids):
        values = list(value) if hasattr(value, '__iter__') and not isinstance(value, (str, bytes)) else [value]
        if operator in ('!=', 'not in'):
            want = not any(values)
        elif operator in ('=', 'in'):
            want = any(values)
        else:
            return NotImplemented
        return [('id', 'in' if want else 'not in', list(tmpl_ids))]

    def _search_ft_es_tela(self, operator, value):
        """Telas acabadas en metros con ficha de acabado (el concentrado de la app lista una por tela)."""
        acabados = self.env['ficha.tecnica.acabado'].search([('product_acabado_id', '!=', False)])
        return self._ft_buscar_bool(operator, value, set(acabados.product_acabado_id.product_tmpl_id.ids))

    def _search_ft_es_crudo(self, operator, value):
        tejidos = self.env['ficha.tecnica.tejido'].search([('product_proceso_id', '!=', False)])
        return self._ft_buscar_bool(operator, value, set(tejidos.product_proceso_id.product_tmpl_id.ids))

    def _ft_abrir(self, record, name):
        return {'type': 'ir.actions.act_window', 'name': name, 'res_model': record._name, 'view_mode': 'form',
                'res_id': record.id}

    def action_ft_bom_kg(self):
        self.ensure_one()
        return self._ft_abrir(self.ft_bom_kg_id, 'Lista en kg')

    def action_ft_bom_m(self):
        self.ensure_one()
        return self._ft_abrir(self.ft_bom_m_id, 'Lista en metros')

    def action_ft_crudo(self):
        self.ensure_one()
        return self._ft_abrir(self.ft_crudo_id.product_tmpl_id, 'Crudo')

    def action_ft_datos_ficha(self):
        """«Usar los datos de la ficha» en las listas de la tela y vuelve a revisarla."""
        for tmpl in self.filtered('ficha_tecnica_acabado_id'):
            (tmpl.ft_bom_kg_id | tmpl.ft_bom_m_id).action_ft_datos_ficha()
            self.env.invalidate_all()
            self.env['ficha.tecnica.revision']._ft_actualizar(tmpl.ficha_tecnica_acabado_id)
        return True

    def action_ft_revisar(self):
        self.env['ficha.tecnica.revision']._ft_actualizar(self.ficha_tecnica_acabado_id)
        return True

    def action_ft_generar_listas(self):
        """2.28.0: «Generar listas desde la ruta» desde la pestaña del producto."""
        return self.ficha_tecnica_acabado_id.action_ft_generar_listas()

    def action_ft_actualizar_revision(self):
        """2.27.0: botón «Actualizar la revisión» del concentrado (rehace la de todas las telas, como la de la
        noche). Método de registros: el botón de la cabecera llega con los renglones seleccionados."""
        self.env['ficha.tecnica.revision']._cron_ft_actualizar()
        return {'type': 'ir.actions.client', 'tag': 'reload'}

    def action_ft_crear_ficha(self):
        """2.27.0: crea la ficha desde el producto. Abre el formulario nuevo con lo que sale del producto (artículo,
        producto y, en una tela, su acabado en kg por código); lo demás (la ficha de tejido base, los datos) lo
        captura quien la crea. En un acabado en kg abre su tela en metros: la ficha va ahí."""
        self.ensure_one()
        variante = self._ft_variantes()[:1]
        code = (self.default_code or '').strip()
        if self.ft_tipo_codigo == 'kg':
            return self._ft_abrir(self.ft_tela_m_de_kg_id.product_tmpl_id, 'Tela en metros')
        if self.ft_tipo_codigo == 'crudo':
            return {'type': 'ir.actions.act_window', 'name': 'Nueva ficha de tejido', 'res_model': 'ficha.tecnica.tejido',
                    'view_mode': 'form', 'target': 'current',
                    'context': {'default_articulo': code, 'default_product_proceso_id': variante.id}}
        from .acabado_ruta import codigo_proceso
        kg = self.env['product.product'].search([('default_code', '=', codigo_proceso(code))], limit=1) \
            if codigo_proceso(code) else self.env['product.product']
        return {'type': 'ir.actions.act_window', 'name': 'Nueva ficha de la tela', 'res_model': 'ficha.tecnica.acabado',
                'view_mode': 'form', 'target': 'current',
                'context': {'default_articulo': code, 'default_product_acabado_id': variante.id,
                            'default_product_proceso_id': kg.id}}


class ProductProductFichaNativa(models.Model):
    _inherit = 'product.product'

    # _inherits delega los campos de la plantilla, no sus métodos: los botones de la pestaña los llaman aquí.
    def action_ft_bom_kg(self):
        return self.product_tmpl_id.action_ft_bom_kg()

    def action_ft_bom_m(self):
        return self.product_tmpl_id.action_ft_bom_m()

    def action_ft_crudo(self):
        return self.product_tmpl_id.action_ft_crudo()

    def action_ft_datos_ficha(self):
        return self.product_tmpl_id.action_ft_datos_ficha()

    def action_ft_revisar(self):
        return self.product_tmpl_id.action_ft_revisar()

    def action_ft_crear_ficha(self):
        return self.product_tmpl_id.action_ft_crear_ficha()

    def action_ft_generar_listas(self):
        return self.product_tmpl_id.action_ft_generar_listas()


class MrpRoutingWorkcenterParametros(models.Model):
    _inherit = 'mrp.routing.workcenter'

    ft_param_proceso_ids = fields.Many2many(
        'ficha.tecnica.proceso', string='Parámetros del proceso', compute='_compute_ft_param_proceso',
        help='Los datos de la tela en la máquina de la operación y en sus alternativas (velocidad, merma, tiempo de '
             'ciclo, kg por carga…). Cada renglón se abre para editarlo.')

    @api.depends('workcenter_id', 'workcenter_id.alternative_workcenter_ids', 'bom_id.product_id',
                 'bom_id.product_tmpl_id')
    def _compute_ft_param_proceso(self):
        Proceso = self.env['ficha.tecnica.proceso']
        for op in self:
            products = op.bom_id.product_id or op.bom_id.product_tmpl_id.product_variant_ids
            maquinas = op.workcenter_id | op.workcenter_id.alternative_workcenter_ids
            procesos = Proceso
            for wc in maquinas:
                for product in products:
                    proceso = Proceso._ft_buscar(product, wc)
                    if proceso:
                        procesos |= proceso
                        break
            op.ft_param_proceso_ids = procesos

    def action_ft_nuevo_proceso(self):
        """2.26.0: crea los datos de la tela en la máquina de la operación (con el código de la lista) y los abre."""
        self.ensure_one()
        product = self.bom_id.product_id or self.bom_id.product_tmpl_id.product_variant_ids[:1]
        proceso = self.ft_proceso_id or self.env['ficha.tecnica.proceso'].create({
            'articulo': product.default_code or product.name, 'workcenter_id': self.workcenter_id.id})
        return proceso.action_ft_editar()
