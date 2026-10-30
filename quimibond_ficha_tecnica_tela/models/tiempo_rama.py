# -*- coding: utf-8 -*-
"""Tiempo de rama desde la ficha (2.22.0, Dirección General 2026-10-30).

El tiempo de la operación de rama en las listas en kg sale del proceso del artículo en la rama y de su ficha de
acabado, con la fórmula de Innovación (correo del 28-oct-2026) y la merma que confirmó el 29-oct («Con merma»):

    min/kg = 1000 ÷ (peso g/m² × ancho bajando de rama m) × (1 − merma %) ÷ velocidad m/min

- Peso: el de la ficha de acabado del artículo (g/m²).
- Ancho bajando de rama (``ancho_rama``), merma (``merma``) y velocidad (``velocidad``, m/min): del proceso del
  artículo en esa rama. El ancho viene de la tabla de Manufactura F-IT-P-P01-10-06.
- 1000 ÷ (peso × ancho) son los metros por kg que bajan de rama; la merma los descuenta (la columna «rendimiento
  bajando de rama» de la tabla de Manufactura es el rendimiento × 0.9) y la velocidad los vuelve minutos.

Aplica a las operaciones de listas en kg cuya máquina tiene proceso del artículo y un tipo con el parámetro
``ancho_rama`` (hoy, las tres ramas). Si falta un dato, no hay tiempo y se dice cuál falta: un dato vacío es un dato
que la planta no ha dado, nunca un cero.

El tiempo calculado no se escribe solo: la operación muestra el tiempo de la ficha y avisa si el de la lista es otro;
«Usar el tiempo de la ficha» (en la operación, o en varias listas de materiales a la vez) lo pasa a la lista. Un ciclo
de la máquina es su capacidad para el producto (1 kg si no está capturada): el tiempo del ciclo es min/kg × capacidad.
"""
from odoo import api, fields, models

from .maquina_config import _fmt

TOLERANCIA_REL = 0.005  # 0.5 %: abajo de esto el tiempo de la lista es el de la ficha


class MrpRoutingWorkcenterTiempoRama(models.Model):
    _inherit = 'mrp.routing.workcenter'

    ft_tiempo_ficha = fields.Float(
        string='Tiempo de la ficha', digits=(16, 4), compute='_compute_ft_tiempo_ficha',
        help='Minutos por ciclo de la máquina que da la ficha: 1000 ÷ (peso g/m² × ancho bajando de rama m) × '
             '(1 − merma %) ÷ velocidad m/min, por la capacidad del ciclo. Vacío si la operación no es de rama o '
             'falta un dato.')
    ft_tiempo_calculo = fields.Char(string='Cálculo del tiempo', compute='_compute_ft_tiempo_ficha')
    ft_tiempo_difiere = fields.Boolean(string='Tiempo distinto de la ficha', compute='_compute_ft_tiempo_ficha')

    def _ft_tiempo_rama(self):
        """(minutos por ciclo, texto del cálculo o del dato que falta). (0, False) si la operación no es de rama."""
        self.ensure_one()
        proceso = self.ft_proceso_id
        if not proceso or not proceso.valor_ids.filtered(lambda v: v.code == 'ancho_rama'):
            return 0.0, False
        kg = self.env.ref('uom.product_uom_kgm', raise_if_not_found=False)
        bom = self.bom_id
        if not kg or bom.product_uom_id != kg:
            return 0.0, 'La lista de materiales no está en kg.'
        faltan = []
        peso = proceso.ficha_acabado_id.peso_acabado
        if not proceso.ficha_acabado_id:
            faltan.append('la ficha de acabado')
        elif not peso:
            faltan.append('el peso en la ficha de acabado')
        ancho = proceso._ft_value('ancho_rama')
        merma = proceso._ft_value('merma')
        velocidad = proceso._ft_value('velocidad')
        if not ancho:
            faltan.append('el ancho bajando de rama')
        if not merma:
            faltan.append('la merma')
        if not velocidad:
            faltan.append('la velocidad')
        if faltan:
            return 0.0, 'Falta %s en %s.' % (', '.join(faltan), proceso.display_name)
        metros = 1000.0 / (peso * ancho)
        netos = metros * (1.0 - merma / 100.0)
        por_kg = netos / velocidad
        product = bom.product_id or bom.product_tmpl_id.product_variant_ids[:1]
        capacidad = self.workcenter_id._get_capacity(product, bom.product_uom_id, bom.product_qty or 1)[0] or 1.0
        texto = '1000 ÷ (%s g/m² × %s m) = %s m/kg × (1 − %s %%) = %s m/kg ÷ %s m/min = %s min/kg' % (
            _fmt(peso), _fmt(ancho), _fmt(round(metros, 2)), _fmt(merma), _fmt(round(netos, 2)), _fmt(velocidad),
            ('%.4f' % por_kg))
        if capacidad != 1.0:
            texto += ' × %s kg por ciclo' % _fmt(capacidad)
        return por_kg * capacidad, texto

    @api.depends('ft_proceso_id', 'time_mode', 'time_cycle_manual', 'bom_id.product_uom_id', 'bom_id.product_qty')
    def _compute_ft_tiempo_ficha(self):
        for op in self:
            tiempo, texto = op._ft_tiempo_rama()
            op.ft_tiempo_ficha = tiempo
            op.ft_tiempo_calculo = texto
            op.ft_tiempo_difiere = bool(tiempo) and (
                op.time_mode != 'manual' or abs(op.time_cycle_manual - tiempo) > tiempo * TOLERANCIA_REL)

    def action_ft_usar_tiempo_ficha(self):
        """Pasa el tiempo de la ficha a la lista (tiempo fijo) en las operaciones de rama que lo tienen."""
        for op in self:
            tiempo = op._ft_tiempo_rama()[0]
            if tiempo:
                op.write({'time_mode': 'manual', 'time_cycle_manual': round(tiempo, 4)})
        return True


class MrpBomTiempoRama(models.Model):
    _inherit = 'mrp.bom'

    def action_ft_tiempos_rama(self):
        """«Usar el tiempo de la ficha» en las operaciones de rama de estas listas (también las archivadas: las listas
        en kg nacen archivadas hasta que Ingeniería las revisa)."""
        self.with_context(active_test=False).operation_ids.action_ft_usar_tiempo_ficha()
        return True
