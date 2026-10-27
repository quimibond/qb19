# -*- coding: utf-8 -*-
"""Crudo, operaciones y acabado en proceso de la ficha de acabado (2.9.0 y 2.12.0, Dirección General 2026-10-19).

Toda la producción va **en kg hasta la Clasificación final** (decisión 2026-10-19, corrige la de un solo producto en kg:
la tela acabada se vende en metros). Cada artículo tiene dos productos, ligados en la ficha y no por el código:

- **Acabado en proceso (kg)** (``product_proceso_id``): lo que sale de rama, termofijado y estiramiento.
- **Tela acabada (m)** (``product_acabado_id``): la que entrega la Clasificación final con los metros medidos, y
  la que se vende.

La ficha muestra además:

- **Crudo**: sale de la lista de materiales del acabado (bajando por los
  intermedios: acabado en proceso, teñido) hasta el primer componente que tiene
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
# DAT P-D02-01 rev. 05 (regla de codificación, documento controlado del SGI): posición 1 = unidad, «I» = kilogramos,
# sin letra = metros. El acabado en kg es el código de la tela acabada con la «I» al frente (2.13.0, decisión de
# Dirección 2026-10-19: se sigue la DAT, sin códigos nuevos). Sin inspeccionar o ya inspeccionado se distingue por la
# ubicación, no por el código: el mismo «I…» también se vende en kg.
PREFIJO_KG = 'I'
OP_ACABADO = 'J'


def codigo_proceso(code):
    """El código en kg de una tela acabada en metros (WJ045Q22JNT160 → IWJ045Q22JNT160); '' si el código no sigue
    la regla (14 o 16 posiciones con J en la 9)."""
    code = (code or '').strip()
    if len(code) not in (14, 16) or code[8:9].upper() != OP_ACABADO:
        return ''
    return PREFIJO_KG + code


class FichaTecnicaAcabadoRuta(models.Model):
    _inherit = 'ficha.tecnica.acabado'

    bom_id = fields.Many2one('mrp.bom', string='Lista de materiales del acabado', compute='_compute_ruta')
    crudo_id = fields.Many2one('product.product', string='Crudo (de la lista de materiales)', compute='_compute_ruta')
    crudo_aviso = fields.Char(string='Aviso del crudo', compute='_compute_ruta')
    operacion_ids = fields.Many2many('mrp.routing.workcenter', string='Operaciones', compute='_compute_ruta')
    operacion_aviso = fields.Char(string='Aviso de operaciones', compute='_compute_ruta')
    product_proceso_id = fields.Many2one(
        'product.product', string='Producto — Acabado en proceso (kg)', index=True,
        help='La tela en kg que sale de la orden de acabado (devanado, tintorería, abridora, rama e Inspección): el '
             'código de la tela acabada con la «I» al frente (DAT P-D02-01, posición 1 = kilogramos). La '
             'Clasificación final la pasa a metros medidos y entrega el Producto — Tela acabada; la que se vende '
             'en kg sale del mismo producto desde Producto terminado.')
    product_proceso_sugerido_id = fields.Many2one('product.product', string='Acabado en proceso sugerido',
                                                  compute='_compute_proceso_aviso')
    proceso_aviso = fields.Char(string='Aviso del acabado en proceso', compute='_compute_proceso_aviso')
    # 2.19.0: botones de la ficha a las listas de materiales de cada producto (también las archivadas: las
    # listas en kg nacen archivadas hasta que Ingeniería las revisa).
    bom_proceso_count = fields.Integer(string='Listas en kg', compute='_compute_bom_counts')
    bom_acabado_count = fields.Integer(string='Listas en metros', compute='_compute_bom_counts')

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

    def _ft_proceso_sugerido(self):
        """El producto con el código de la tela acabada y la «I» al frente (en kg), si existe."""
        self.ensure_one()
        code = codigo_proceso(self.product_acabado_id.default_code)
        if not code:
            return self.env['product.product']
        return self.env['product.product'].search([('default_code', '=', code)], limit=1)

    @api.depends('product_acabado_id', 'product_proceso_id', 'product_proceso_id.uom_id')
    def _compute_proceso_aviso(self):
        kg = self.env.ref('uom.product_uom_kgm', raise_if_not_found=False)
        Bom = self.env['mrp.bom']
        for ficha in self:
            proceso = ficha.product_proceso_id
            sugerido = ficha._ft_proceso_sugerido() if ficha.product_acabado_id and not proceso else False
            ficha.product_proceso_sugerido_id = sugerido
            aviso = False
            if not ficha.product_acabado_id:
                pass
            elif not proceso and sugerido:
                aviso = 'Falta el producto de acabado en proceso (kg): se sugiere %s.' % (
                    sugerido.default_code or sugerido.name)
            elif not proceso:
                code = codigo_proceso(ficha.product_acabado_id.default_code)
                aviso = ('Falta el producto de acabado en proceso (kg)%s.' % (
                    ': %s no existe todavía (Tela → Acabados en proceso faltantes)' % code if code else ''))
            elif proceso == ficha.product_acabado_id:
                aviso = ('El acabado en proceso y la tela acabada son el mismo producto: el primero va en kg y '
                         'el segundo en metros, después de la Clasificación final.')
            elif kg and proceso.uom_id != kg:
                aviso = 'El acabado en proceso %s está en %s: hasta la Clasificación final todo va en kg.' % (
                    proceso.default_code or proceso.name, proceso.uom_id.name)
            else:
                otra = self.search([('product_proceso_id', '=', proceso.id), ('id', '!=', ficha._origin.id)],
                                   limit=1)
                bom = Bom._bom_find(ficha.product_acabado_id, bom_type='normal')[ficha.product_acabado_id]
                if otra:
                    aviso = 'El acabado en proceso %s también es de la ficha %s.' % (
                        proceso.default_code or proceso.name, otra.display_name)
                elif bom and proceso not in bom.bom_line_ids.product_id:
                    aviso = ('La lista de materiales de la tela acabada no consume %s: la Clasificación final pasa el acabado '
                             'en proceso (kg) a metros.' % (proceso.default_code or proceso.name))
            ficha.proceso_aviso = aviso

    def _ft_ligar_proceso_sugerido(self):
        """Liga el acabado en proceso sugerido a las fichas que no tienen uno; devuelve las que ligó."""
        ligadas = self.browse()
        for ficha in self.filtered(lambda f: f.product_acabado_id and not f.product_proceso_id):
            sugerido = ficha._ft_proceso_sugerido()
            if sugerido and sugerido != ficha.product_acabado_id:
                ficha.product_proceso_id = sugerido
                ligadas |= ficha
        return ligadas

    @api.model_create_multi
    def create(self, vals_list):
        fichas = super().create(vals_list)
        fichas._ft_ligar_proceso_sugerido()
        return fichas

    def action_ft_usar_proceso_sugerido(self):
        self._ft_ligar_proceso_sugerido()
        return True

    def _ft_boms_de(self, product):
        """Las listas de materiales del producto (de la variante o de su plantilla), activas y archivadas."""
        if not product:
            return self.env['mrp.bom']
        return self.env['mrp.bom'].with_context(active_test=False).search([
            '|', ('product_id', '=', product.id),
            '&', ('product_id', '=', False), ('product_tmpl_id', '=', product.product_tmpl_id.id)])

    @api.depends('product_proceso_id', 'product_acabado_id')
    def _compute_bom_counts(self):
        for ficha in self:
            ficha.bom_proceso_count = len(ficha._ft_boms_de(ficha.product_proceso_id))
            ficha.bom_acabado_count = len(ficha._ft_boms_de(ficha.product_acabado_id))

    def _ft_accion_boms(self, product, nombre):
        self.ensure_one()
        return {'type': 'ir.actions.act_window', 'name': '%s — %s' % (nombre, product.default_code or product.name),
                'res_model': 'mrp.bom', 'view_mode': 'list,form',
                'domain': [('id', 'in', self._ft_boms_de(product).ids)],
                'context': {'active_test': False, 'default_product_tmpl_id': product.product_tmpl_id.id}}

    def action_ft_boms_proceso(self):
        return self._ft_accion_boms(self.product_proceso_id, 'Listas en kg')

    def action_ft_boms_acabado(self):
        return self._ft_accion_boms(self.product_acabado_id, 'Listas en metros')

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
