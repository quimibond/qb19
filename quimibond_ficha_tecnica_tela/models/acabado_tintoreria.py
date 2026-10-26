# -*- coding: utf-8 -*-
"""Tintorería en la ficha de acabado (2.16.0, Dirección General 2026-10-26).

La tintorería no tiene ficha propia: es una **operación de la lista de materiales del acabado** (devanado, tintorería,
abridora, rama e Inspección en la orden en kg). Lo que pedía la Ficha técnica de tintorería F-P-D01-33 queda así:

- **Datos del artículo en el jet** (proceso del artículo en la máquina, tipo Tintorería): tiempo de ciclo, kg por
  carga, relación de baño y, desde 2.16.0, temperatura, gradientes, plegador, bomba y acumulador.
- **Volumen de baño**: kg por carga × relación de baño (la del artículo en el jet o la de la máquina). Se calcula.
- **Fórmula y auxiliares**: los componentes de la lista de materiales asignados a la operación de tintorería; mientras
  el teñido siga como artículo aparte (posición 9 = I), los de la lista del teñido que no son tela.

La pestaña «Tintorería» de la ficha de acabado lo junta, leído de la lista de materiales y de los procesos: no se
recaptura.
"""
from odoo import api, fields, models

TIPO_TINTORERIA = 'tintoreria'
OP_TENIDO = 'I'


def es_tenido(code):
    """Código de teñido (posición 9 = I, DAT P-D02-01): XJ145Q21INT098. El acabado en kg (I al frente) no lo es."""
    code = (code or '').strip().upper()
    return len(code) in (14, 16) and code[8:9] == OP_TENIDO


class FichaTecnicaProcesoTintoreria(models.Model):
    _inherit = 'ficha.tecnica.proceso'

    ft_volumen_bano = fields.Float(string='Volumen de baño (L)', digits=(16, 1), compute='_compute_ft_volumen_bano',
                                   help='Kg por carga × relación de baño (la del artículo en el jet o, si no tiene, '
                                        'la de la máquina). Solo en jets.')

    @api.depends('valor_ids.valor', 'workcenter_id', 'product_id')
    def _compute_ft_volumen_bano(self):
        for proceso in self:
            volumen = 0.0
            if proceso.tipo_id.code == TIPO_TINTORERIA:
                kg = proceso._ft_value('kg_carga') or 0.0
                relacion = proceso._ft_value('relacion_bano_articulo') or 0.0
                if not relacion and proceso.workcenter_id:
                    relacion = proceso.workcenter_id._ft_relacion_bano(proceso.product_id or None)[0]
                volumen = kg * relacion if isinstance(kg, (int, float)) and isinstance(relacion, (int, float)) else 0.0
            proceso.ft_volumen_bano = volumen


class FichaTecnicaAcabadoTintoreria(models.Model):
    _inherit = 'ficha.tecnica.acabado'

    tint_operacion_ids = fields.Many2many('mrp.routing.workcenter', string='Operaciones de tintorería',
                                          compute='_compute_tintoreria')
    tint_proceso_ids = fields.Many2many('ficha.tecnica.proceso', string='El artículo en los jets',
                                        compute='_compute_tintoreria')
    tint_linea_ids = fields.Many2many('mrp.bom.line', string='Fórmula y auxiliares', compute='_compute_tintoreria')
    tint_aviso = fields.Char(string='Aviso de tintorería', compute='_compute_tintoreria')

    def _ft_tintoreria(self):
        """(operaciones de tintorería, procesos del artículo en jets, renglones de la fórmula) de la cadena."""
        self.ensure_one()
        boms = self._ft_cadena()[0] if self.product_acabado_id else self.env['mrp.bom']
        operaciones = boms.operation_ids.filtered(lambda o: o.workcenter_id.ft_tipo_id.code == TIPO_TINTORERIA)
        telas = self._ft_telas(boms)
        lineas = boms.bom_line_ids.filtered(
            lambda l: l.operation_id in operaciones
            or (es_tenido(l.bom_id.product_tmpl_id.default_code) and l.product_id not in telas))
        procesos = operaciones.ft_proceso_id
        codes = {c for c in [self.articulo, self.product_acabado_id.default_code, self.product_proceso_id.default_code]
                 + boms.product_tmpl_id.mapped('default_code') if c}
        if codes:
            procesos |= self.env['ficha.tecnica.proceso'].search(
                [('articulo', 'in', list(codes)), ('tipo_id.code', '=', TIPO_TINTORERIA)])
        return operaciones, procesos, lineas

    def _ft_telas(self, boms):
        """Los componentes que son tela (intermedios de la cadena o con ficha de tejido), no fórmula."""
        Tejido = self.env['ficha.tecnica.tejido']
        intermedios = boms.product_tmpl_id.product_variant_ids
        return boms.bom_line_ids.product_id.filtered(lambda p: p in intermedios or Tejido._ft_de_producto(p))

    @api.depends('product_acabado_id', 'product_proceso_id', 'articulo')
    def _compute_tintoreria(self):
        for ficha in self:
            operaciones, procesos, lineas = ficha._ft_tintoreria()
            ficha.tint_operacion_ids = operaciones
            ficha.tint_proceso_ids = procesos
            ficha.tint_linea_ids = lineas
            aviso = False
            if ficha.product_acabado_id and not operaciones and not lineas:
                aviso = ('La lista de materiales del acabado no tiene operación de tintorería ni fórmula de teñido: '
                         'si la tela se tiñe, agregue la operación del jet y su fórmula en la lista de materiales.')
            elif operaciones and not lineas:
                aviso = 'Hay operación de tintorería pero ningún componente asignado a ella (la fórmula).'
            elif lineas and not procesos:
                aviso = ('El artículo no tiene datos en ningún jet (temperatura, tiempo de ciclo, kg por carga…): '
                         'se capturan en Máquinas → Procesos por artículo.')
            ficha.tint_aviso = aviso
