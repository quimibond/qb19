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


class ProductTemplateFichaNativa(models.Model):
    _inherit = 'product.template'

    ft_tiene_ficha = fields.Boolean(string='Tiene ficha técnica', compute='_compute_ft_ficha_nativa')
    ft_es_tela = fields.Boolean(string='Es tela con ficha', compute='_compute_ft_ficha_nativa', search='_search_ft_es_tela',
                                help='Tela acabada (en metros) o su acabado en proceso (kg) con ficha de acabado.')
    ft_es_crudo = fields.Boolean(string='Es crudo con ficha', compute='_compute_ft_ficha_nativa',
                                 search='_search_ft_es_crudo', help='Crudo con ficha de tejido.')
    # La tela: se editan aquí y se guardan en su ficha de acabado.
    ft_peso = fields.Float(related='ficha_tecnica_acabado_id.peso_acabado', string='Peso de la tela (g/m²)',
                           readonly=False)
    ft_ancho = fields.Float(related='ficha_tecnica_acabado_id.ancho_acabado', string='Ancho de la tela (cm)',
                            readonly=False)
    ft_rendimiento = fields.Float(related='ficha_tecnica_acabado_id.rendimiento_tela_acabada', digits=(12, 4),
                                  string='Rendimiento acabado (m/kg)', readonly=False)
    ft_producto_kg_id = fields.Many2one(related='ficha_tecnica_acabado_id.product_proceso_id',
                                        string='Producto en kg (proceso)')
    ft_producto_m_id = fields.Many2one(related='ficha_tecnica_acabado_id.product_acabado_id',
                                       string='Tela acabada (en metros)')
    ft_spec_ids = fields.One2many(related='ficha_tecnica_acabado_id.spec_line_ids', string='Especificación')
    # El crudo de la tela.
    ft_crudo_id = fields.Many2one('product.product', string='Crudo de la tela', compute='_compute_ft_ficha_nativa')
    ft_crudo_ficha_id = fields.Many2one('ficha.tecnica.tejido', string='Ficha del crudo',
                                        compute='_compute_ft_ficha_nativa')
    ft_crudo_peso = fields.Float(related='ft_crudo_ficha_id.peso_acondicionado', string='Peso del crudo (g/m²)')
    ft_crudo_ancho = fields.Float(related='ft_crudo_ficha_id.ancho_acondicionado', string='Ancho del crudo')
    ft_crudo_rendimiento = fields.Float(related='ft_crudo_ficha_id.rendimiento_tela_tejida', digits=(12, 4),
                                        string='Rendimiento de tejido (m/kg)')
    ft_crudo_telas_count = fields.Integer(string='Telas con este crudo', compute='_compute_ft_ficha_nativa')
    # La ruta y la revisión.
    ft_operacion_ids = fields.Many2many('mrp.routing.workcenter', string='Ruta (operaciones de sus listas)',
                                        compute='_compute_ft_ficha_nativa')
    ft_bom_kg_id = fields.Many2one('mrp.bom', string='Lista en kg', compute='_compute_ft_ficha_nativa')
    ft_bom_m_id = fields.Many2one('mrp.bom', string='Lista en metros', compute='_compute_ft_ficha_nativa')
    ft_revision_id = fields.Many2one('ficha.tecnica.revision', string='Revisión de la tela',
                                     compute='_compute_ft_ficha_nativa')
    ft_revision_listo = fields.Boolean(related='ft_revision_id.listo', string='Listas cuadran con la ficha')
    ft_revision_n = fields.Integer(related='ft_revision_id.n_pendientes', string='Pendientes de la tela')
    ft_pendiente_ids = fields.One2many(related='ft_revision_id.pendiente_ids', string='Pendientes por área')
    # El crudo visto desde su propio producto.
    ft_tejido_ids = fields.Many2many('ficha.tecnica.tejido', string='Circulares donde se teje',
                                     compute='_compute_ft_ficha_nativa')
    ft_tela_ids = fields.Many2many('ficha.tecnica.acabado', string='Telas que usan este crudo',
                                   compute='_compute_ft_ficha_nativa')

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
            tmpl.ft_crudo_ficha_id = crudo_ficha
            tmpl.ft_crudo_telas_count = Acabado.search_count(
                [('tejido_id.product_proceso_id', '=', crudo.id)]) if crudo else 0
            bom_kg = Revision._ft_bom_de(acabado.product_proceso_id) if acabado else self.env['mrp.bom']
            bom_m = Revision._ft_bom_de(acabado.product_acabado_id) if acabado else self.env['mrp.bom']
            tmpl.ft_bom_kg_id = bom_kg
            tmpl.ft_bom_m_id = bom_m
            tmpl.ft_operacion_ids = (bom_kg | bom_m).with_context(active_test=False).operation_ids
            tmpl.ft_revision_id = Revision.search([('acabado_id', '=', acabado.id)], limit=1) if acabado else Revision
            tmpl.ft_tejido_ids = tejidos
            tmpl.ft_tela_ids = Acabado.search([('tejido_id.product_proceso_id', 'in', variantes.ids)]) \
                if tejidos else Acabado

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
