# -*- coding: utf-8 -*-
"""Crudo y operaciones de la ficha de acabado (2.9.0, Dirección General 2026-10-19).

Quimibond va a pasar a **un solo producto de acabado en kg por artículo**: el
crudo en su lista de materiales y las operaciones de abridora, devanado,
tintorería y rama en su ruta, todo en el mismo código. La ficha de acabado lo
muestra así:

- **Crudo**: sale de la lista de materiales del acabado (bajando por los
  intermedios de hoy, p. ej. el teñido) hasta el primer componente que tiene
  ficha de tejido. Si la ficha de acabado se basa en otra ficha de tejido, lo
  avisa.
- **Operaciones**: las de la lista de materiales del acabado y de sus
  intermedios (no las del crudo), cada una con su máquina, el tipo de máquina y
  el proceso del artículo en esa máquina (velocidad, tiempo de ciclo, kg por
  carga…). Una operación en una máquina cuyo tipo pide datos del artículo y que
  no tiene proceso, se avisa.
"""
from odoo import api, fields, models

MAX_NIVELES = 5


class FichaTecnicaAcabadoRuta(models.Model):
    _inherit = 'ficha.tecnica.acabado'

    bom_id = fields.Many2one('mrp.bom', string='Lista de materiales del acabado', compute='_compute_ruta')
    crudo_id = fields.Many2one('product.product', string='Crudo (de la lista de materiales)', compute='_compute_ruta')
    crudo_aviso = fields.Char(string='Aviso del crudo', compute='_compute_ruta')
    operacion_ids = fields.Many2many('mrp.routing.workcenter', string='Operaciones', compute='_compute_ruta')
    operacion_aviso = fields.Char(string='Aviso de operaciones', compute='_compute_ruta')

    def _ft_cadena(self):
        """(listas de materiales del acabado y sus intermedios, crudo): baja por la lista de materiales hasta el
        primer componente con ficha de tejido."""
        self.ensure_one()
        Bom = self.env['mrp.bom']
        Tejido = self.env['ficha.tecnica.tejido']
        boms, crudo = Bom, self.env['product.product']
        nivel = self.product_acabado_id
        vistos = set()
        for _i in range(MAX_NIVELES):
            siguiente = self.env['product.product']
            for product in nivel:
                if product.id in vistos:
                    continue
                vistos.add(product.id)
                bom = Bom._bom_find(product, bom_type='normal')[product]
                if not bom:
                    continue
                boms |= bom
                for line in bom.bom_line_ids:
                    if Tejido._ft_de_producto(line.product_id):
                        crudo = crudo or line.product_id
                    else:
                        siguiente |= line.product_id
            if crudo or not siguiente:
                break
            nivel = siguiente
        return boms, crudo

    @api.depends('product_acabado_id', 'tejido_id.product_proceso_id')
    def _compute_ruta(self):
        for ficha in self:
            boms, crudo = ficha._ft_cadena() if ficha.product_acabado_id else (self.env['mrp.bom'], False)
            ficha.bom_id = boms[:1]
            ficha.crudo_id = crudo
            base = ficha.tejido_id.product_proceso_id
            aviso = False
            if ficha.product_acabado_id and not boms:
                aviso = 'El producto acabado no tiene lista de materiales.'
            elif ficha.product_acabado_id and not crudo:
                aviso = 'La lista de materiales del acabado no lleva un crudo con ficha de tejido.'
            elif crudo and base and crudo != base:
                aviso = 'La ficha se basa en el tejido de %s y la lista de materiales lleva %s.' % (
                    base.default_code or base.name, crudo.default_code or crudo.name)
            ficha.crudo_aviso = aviso
            operaciones = boms.operation_ids.sorted(lambda o: (o.bom_id.id != boms[:1].id, o.sequence, o.id))
            ficha.operacion_ids = operaciones
            sin_proceso = operaciones.filtered(lambda o: o.ft_aviso)
            ficha.operacion_aviso = ('Operaciones sin proceso del artículo en su máquina: %s.' % ', '.join(
                '%s (%s)' % (o.name, o.workcenter_id.name) for o in sin_proceso)) if sin_proceso else False

    def action_ft_ver_bom(self):
        self.ensure_one()
        return {'type': 'ir.actions.act_window', 'name': 'Lista de materiales', 'res_model': 'mrp.bom',
                'view_mode': 'form', 'res_id': self.bom_id.id}


class MrpRoutingWorkcenterFicha(models.Model):
    _inherit = 'mrp.routing.workcenter'

    ft_tipo_id = fields.Many2one(related='workcenter_id.ft_tipo_id', string='Tipo de máquina')
    ft_proceso_id = fields.Many2one('ficha.tecnica.proceso', string='Proceso del artículo',
                                    compute='_compute_ft_proceso')
    ft_proceso_resumen = fields.Char(string='Datos del artículo en la máquina', compute='_compute_ft_proceso')
    ft_aviso = fields.Char(string='Aviso de proceso', compute='_compute_ft_proceso')

    @api.depends('workcenter_id', 'bom_id.product_id', 'bom_id.product_tmpl_id')
    def _compute_ft_proceso(self):
        Proceso = self.env['ficha.tecnica.proceso']
        for op in self:
            products = op.bom_id.product_id or op.bom_id.product_tmpl_id.product_variant_ids
            proceso = Proceso
            for product in products:
                proceso = Proceso._ft_buscar(product, op.workcenter_id)
                if proceso:
                    break
            op.ft_proceso_id = proceso
            op.ft_proceso_resumen = proceso.resumen if proceso else False
            pide = op.workcenter_id.ft_tipo_id and op.workcenter_id.ft_tipo_id._ft_parametros('articulo')
            op.ft_aviso = ('%s pide datos del artículo y no hay proceso.' % op.workcenter_id.name) \
                if pide and not proceso else False
