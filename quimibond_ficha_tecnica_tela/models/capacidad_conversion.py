# -*- coding: utf-8 -*-
"""Capacidad de los jets y conversión a metros vigiladas contra la ficha (2.23.0, Dirección General 2026-11-02).

**Capacidad del jet por producto.** En las listas en kg la operación de Tintorería se cuenta **por baño**: Odoo divide
la orden entre la capacidad del jet para el producto (un ciclo) y multiplica por los minutos del ciclo. Esa capacidad
es la misma que usa la Programación de Tintorería para dividir en baños, y sale de las fichas:

1. «Kg por carga» del artículo en ese jet (proceso del artículo en la máquina), si está.
2. Si no, la configuración vigente del jet: sin bandas capturadas, su capacidad general; con bandas, la de la banda
   del rendimiento de tejido (A 3–7, B 7–11, C 11–16 m/kg), topada a la capacidad general. Banda en cero o
   rendimiento fuera de las bandas: el jet no puede con el artículo y no hay capacidad (no se inventa).

Cada renglón de capacidad con producto en un jet muestra la capacidad de la ficha y avisa si es otra; «Usar la
capacidad de la ficha» la corrige (en el renglón, en varios jets a la vez o desde las listas de materiales, que
además crean el renglón que falte en el jet de la operación y en sus alternativas con proceso del artículo).

**Conversión kg → m.** La lista en metros de la tela acabada (Clasificación final) consume su acabado en proceso en
kg: por cada metro, 1 ÷ el rendimiento de tela acabada de la ficha de acabado. La lista avisa si la cantidad es otra
(más de 0.5 %) y «Usar los datos de la ficha» la corrige.
"""
from odoo import api, fields, models
from odoo.tools import float_round

from .maquina_config import _fmt
from .mrp_aviso_maquina import GRUPOS, TIPO_TINTORERIA

TOLERANCIA_REL = 0.005  # 0.5 %


class MrpWorkcenterCapacidadFicha(models.Model):
    _inherit = 'mrp.workcenter'

    def _ft_capacidad_jet(self, product, day=None):
        """(kg por baño, de dónde sale) del jet para ``product`` con la regla de la Programación de Tintorería;
        (0, motivo) si el jet no puede con el artículo o falta un dato."""
        self.ensure_one()
        proceso = self._ft_proceso_de(product) if product else False
        kg_articulo = proceso._ft_value('kg_carga') if proceso else False
        if kg_articulo:
            return kg_articulo, 'kg por carga del artículo en %s' % self.name
        config = self._ft_config_at(day)
        if not config:
            return 0.0, '%s no tiene configuración vigente' % self.name
        general = config._ft_value('capacidad') or 0.0
        bandas = {grupo: config._ft_value(code) or 0.0 for code, grupo, _desde, _hasta in GRUPOS}
        if not any(bandas.values()):
            if general:
                return general, 'capacidad general de %s (sin bandas de rendimiento)' % self.name
            return 0.0, '%s no tiene capacidad capturada' % self.name
        rendimiento = product._ft_rendimiento()[0] if product else 0.0
        if not rendimiento:
            return 0.0, 'la tela no tiene rendimiento de tejido en su ficha'
        banda = next((grupo for _code, grupo, desde, hasta in GRUPOS if desde <= rendimiento < hasta), False)
        if not banda:
            return 0.0, 'el rendimiento %s m/kg está fuera de las bandas de %s' % (_fmt(rendimiento), self.name)
        capacidad = bandas[banda]
        if not capacidad:
            return 0.0, 'la banda %s (rendimiento %s m/kg) es N/A en %s' % (banda, _fmt(rendimiento), self.name)
        if general and general < capacidad:
            return general, 'banda %s de %s, topada a su capacidad general' % (banda, self.name)
        return capacidad, 'banda %s de %s (rendimiento %s m/kg)' % (banda, self.name, _fmt(rendimiento))

    def _ft_poner_capacidad(self, product):
        """Escribe (o crea) el renglón de capacidad de ``product`` con la de la ficha. True si cambió algo."""
        self.ensure_one()
        capacidad = self._ft_capacidad_jet(product)[0]
        if not capacidad:
            return False
        line = self.capacity_ids.filtered(lambda c: c.product_id == product)[:1]
        if line:
            if abs(line.capacity - capacidad) <= capacidad * TOLERANCIA_REL:
                return False
            line.capacity = capacidad
            return True
        self.env['mrp.workcenter.capacity'].create({
            'workcenter_id': self.id, 'product_id': product.id, 'product_uom_id': product.uom_id.id,
            'capacity': capacidad})
        return True

    def action_ft_capacidades_ficha(self):
        """«Usar la capacidad de la ficha» en los renglones con producto de estos jets."""
        self.capacity_ids.filtered('product_id').action_ft_usar_capacidad_ficha()
        return True


class MrpWorkcenterCapacityFicha(models.Model):
    _inherit = 'mrp.workcenter.capacity'

    ft_capacidad_ficha = fields.Float(
        string='Capacidad de la ficha', digits=(16, 2), compute='_compute_ft_capacidad_ficha',
        help='Kg por baño que dan las fichas (kg por carga del artículo en el jet o la banda de su rendimiento en la '
             'configuración vigente), los mismos con que la Programación de Tintorería divide en baños. Solo jets.')
    ft_capacidad_motivo = fields.Char(string='De dónde sale', compute='_compute_ft_capacidad_ficha')
    ft_capacidad_difiere = fields.Boolean(string='Capacidad distinta de la ficha', compute='_compute_ft_capacidad_ficha')

    @api.depends('product_id', 'capacity', 'workcenter_id.ft_tipo_id')
    def _compute_ft_capacidad_ficha(self):
        for line in self:
            capacidad, motivo = 0.0, False
            if line.product_id and line.workcenter_id.ft_tipo_id.code == TIPO_TINTORERIA:
                capacidad, motivo = line.workcenter_id._ft_capacidad_jet(line.product_id)
            line.ft_capacidad_ficha = capacidad
            line.ft_capacidad_motivo = motivo
            line.ft_capacidad_difiere = bool(capacidad) and abs(line.capacity - capacidad) > capacidad * TOLERANCIA_REL

    def action_ft_usar_capacidad_ficha(self):
        for line in self.filtered('ft_capacidad_difiere'):
            line.capacity = line.ft_capacidad_ficha
        return True


class MrpRoutingWorkcenterCapacidadFicha(models.Model):
    _inherit = 'mrp.routing.workcenter'

    def _ft_sincronizar_capacidad(self):
        """Capacidad de la ficha en el jet de cada operación de tintorería y en sus alternativas donde el artículo
        tiene proceso (las que puede elegir el planificador, 2.20.0)."""
        for op in self.filtered(lambda o: o.workcenter_id.ft_tipo_id.code == TIPO_TINTORERIA):
            product = op._ft_producto_bom()
            if not product:
                continue
            jets = op.workcenter_id | op.workcenter_id.alternative_workcenter_ids.filtered(
                lambda wc: wc.ft_tipo_id.code == TIPO_TINTORERIA and wc._ft_proceso_de(product))
            for jet in jets:
                jet._ft_poner_capacidad(product)
        return True


class MrpBomConversionFicha(models.Model):
    _inherit = 'mrp.bom'

    ft_conversion_kg = fields.Float(
        string='Kg de la ficha', digits=(16, 4), compute='_compute_ft_conversion',
        help='Lista en metros de la tela acabada: kg de acabado en proceso que pide la ficha de acabado para la '
             'cantidad de la lista (metros ÷ rendimiento de tela acabada).')
    ft_conversion_calculo = fields.Char(string='Conversión de la ficha', compute='_compute_ft_conversion')
    ft_conversion_difiere = fields.Boolean(
        string='Conversión distinta de la ficha', compute='_compute_ft_conversion',
        search='_search_ft_conversion_difiere')

    def _ft_conversion(self):
        """(línea del acabado en proceso, kg que da la ficha, texto). Vacío si la lista no es de una tela acabada en
        metros con ficha de acabado."""
        self.ensure_one()
        Line = self.env['mrp.bom.line']
        meter = self.env.ref('uom.product_uom_meter', raise_if_not_found=False)
        kg = self.env.ref('uom.product_uom_kgm', raise_if_not_found=False)
        if not meter or not kg or not self.product_uom_id or not self.product_uom_id._has_common_reference(meter):
            return Line, 0.0, False
        products = self.product_id or self.product_tmpl_id.product_variant_ids
        ficha = self.env['ficha.tecnica.acabado'].search([('product_acabado_id', 'in', products.ids)], limit=1) \
            if products else False
        if not ficha:
            return Line, 0.0, False
        proceso = ficha.product_proceso_id
        if not proceso:
            return Line, 0.0, 'La ficha de acabado %s no tiene el acabado en proceso (kg).' % ficha.display_name
        line = self.bom_line_ids.filtered(lambda l: l.product_id == proceso)[:1]
        if not line:
            return Line, 0.0, 'La lista no consume el acabado en proceso %s.' % (proceso.default_code or proceso.name)
        rendimiento = ficha.rendimiento_tela_acabada
        if not rendimiento:
            return line, 0.0, 'Falta el rendimiento de tela acabada en la ficha %s.' % ficha.display_name
        metros = self.product_uom_id._compute_quantity(self.product_qty, meter, round=False)
        kg_ficha = metros / rendimiento
        return line, kg_ficha, '%s m ÷ %s m/kg = %s kg de %s' % (
            _fmt(metros), _fmt(rendimiento), '%.4f' % kg_ficha, proceso.default_code or proceso.name)

    def _ft_kg_guardables(self, line, kg_ficha):
        """Los kg de la ficha como los puede guardar la línea (en su unidad y con la precisión de su cantidad):
        una línea con 2 decimales no puede tener 0.125, y avisar por eso sería avisar para siempre."""
        kg = self.env.ref('uom.product_uom_kgm', raise_if_not_found=False)
        qty = kg_ficha if line.product_uom_id == kg else kg._compute_quantity(kg_ficha, line.product_uom_id, round=False)
        digits = line._fields['product_qty'].get_digits(self.env)
        return float_round(qty, precision_digits=digits[1]) if digits else round(qty, 4)

    @api.depends('product_id', 'product_tmpl_id', 'product_qty', 'product_uom_id', 'bom_line_ids.product_qty',
                 'bom_line_ids.product_id', 'bom_line_ids.product_uom_id')
    def _compute_ft_conversion(self):
        for bom in self:
            line, kg_ficha, texto = bom._ft_conversion()
            bom.ft_conversion_kg = kg_ficha
            bom.ft_conversion_calculo = texto
            if kg_ficha and line:
                esperado = bom._ft_kg_guardables(line, kg_ficha)
                bom.ft_conversion_difiere = abs(line.product_qty - esperado) > max(esperado * TOLERANCIA_REL, 1e-9)
            else:
                bom.ft_conversion_difiere = False

    def _search_ft_conversion_difiere(self, operator, value):
        """Solo «distinta» / «igual»: revisa las listas en metros (también las archivadas)."""
        values = list(value) if hasattr(value, '__iter__') and not isinstance(value, (str, bytes)) else [value]
        if operator in ('!=', 'not in'):
            want = not any(values)
        elif operator in ('=', 'in'):
            want = any(values)
        else:
            return NotImplemented
        meter = self.env.ref('uom.product_uom_meter', raise_if_not_found=False)
        boms = self.with_context(active_test=False).search([('product_uom_id', '=', meter.id)]) if meter else self
        difieren = boms.filtered('ft_conversion_difiere')
        return [('id', 'in' if want else 'not in', difieren.ids)]

    def action_ft_usar_conversion_ficha(self):
        """Pasa a la línea del acabado en proceso los kg de la ficha (en la unidad de la línea)."""
        for bom in self.filtered('ft_conversion_difiere'):
            line, kg_ficha, _texto = bom._ft_conversion()
            line.product_qty = bom._ft_kg_guardables(line, kg_ficha)
        return True

    def action_ft_datos_ficha(self):
        """«Usar los datos de la ficha» en estas listas (también las archivadas): capacidad de los jets, tiempos de
        las operaciones y conversión kg → m. La capacidad va primero: el tiempo de tintorería por baño la necesita."""
        boms = self.with_context(active_test=False)
        boms.operation_ids._ft_sincronizar_capacidad()
        boms.operation_ids.action_ft_usar_tiempo_ficha()
        boms.action_ft_usar_conversion_ficha()
        return True
