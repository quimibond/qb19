# -*- coding: utf-8 -*-
"""Tiempo de las operaciones desde la ficha (2.22.0 rama; 2.23.0 inspección, devanado, abridora y tintorería).

2.22.0 (Dirección General 2026-10-30): el tiempo de la operación de rama en las listas en kg sale del proceso del
artículo en la rama y de su ficha de acabado, con la fórmula de Innovación (correo del 28-oct-2026) y la merma que
confirmó el 29-oct («Con merma»):

    min/kg = 1000 ÷ (peso g/m² × ancho bajando de rama m) × (1 − merma %) ÷ velocidad m/min

- Peso: el de la ficha de acabado del artículo (g/m²).
- Ancho bajando de rama (``ancho_rama``), merma (``merma``) y velocidad (``velocidad``, m/min): del proceso del
  artículo en esa rama. El ancho viene de la tabla de Manufactura F-IT-P-P01-10-06.
- 1000 ÷ (peso × ancho) son los metros por kg que bajan de rama; la merma los descuenta (la columna «rendimiento
  bajando de rama» de la tabla de Manufactura es el rendimiento × 0.9) y la velocidad los vuelve minutos.

2.23.0 (Dirección General 2026-11-02): las demás operaciones de la lista en kg, con las mismas cuentas con que se
cargaron los tiempos en producción el 30-oct:

- **Inspección**: rendimiento de tela acabada (m/kg, ficha de acabado) × 60 ÷ velocidad de inspección de rollos con
  un empalme (``velocidad_inspeccion``, m/h, proceso del artículo en la inspeccionadora).
- **Devanado y abridora**: los metros de crudo que pasan por kg de la lista (kg de crudo por kg de la lista × el
  rendimiento de su ficha de tejido) ÷ la velocidad (m/min) del artículo en la máquina. En la abridora, la velocidad
  con tela tubular o abierta según cómo sale la tela del tejido; si no está, la velocidad general.
- **Tintorería**: minutos **por baño**, no por kg: el tiempo de ciclo del artículo en el jet (``tiempo_ciclo``, en
  horas) y, si no hay, los minutos de la fórmula de la lista en ese jet (``tintoreria.formula.tiempo``, si la
  Programación de Tintorería está instalada). Es el mismo orden que usa la Programación. Un ciclo de Odoo es la
  capacidad del jet para el producto, así que solo hay tiempo de la ficha si esa capacidad está capturada.

Si falta un dato, no hay tiempo y se dice cuál falta: un dato vacío es un dato que la planta no ha dado, nunca un cero.

El tiempo calculado no se escribe solo: la operación muestra el tiempo de la ficha y avisa si el de la lista es otro;
«Usar el tiempo de la ficha» (en la operación, o en varias listas de materiales a la vez) lo pasa a la lista. Salvo en
tintorería, un ciclo de la máquina es su capacidad para el producto (1 kg si no está capturada): el tiempo del ciclo
es min/kg × capacidad.
"""
from odoo import api, fields, models

from .maquina_config import _fmt

TOLERANCIA_REL = 0.005  # 0.5 %: abajo de esto el tiempo de la lista es el de la ficha
TIPO_TINTORERIA = 'tintoreria'
TIPO_INSPECCION = 'inspeccion'
TIPO_DEVANADORA = 'devanadora'
TIPO_ABRIDORA = 'abridora'
# Abridora: la velocidad según cómo sale la tela del tejido (opciones de «Salida de la tela» de la circular).
VELOCIDAD_ABRIDORA = {'Tubular': 'velocidad_tubular', 'Abierta': 'velocidad_abierta'}


class MrpRoutingWorkcenterTiempoFicha(models.Model):
    _inherit = 'mrp.routing.workcenter'

    ft_tiempo_ficha = fields.Float(
        string='Tiempo de la ficha', digits=(16, 4), compute='_compute_ft_tiempo_ficha',
        help='Minutos por ciclo de la máquina que dan la ficha y el proceso del artículo: rama, inspección, devanado '
             'y abridora por kg × la capacidad del ciclo; tintorería por baño. Vacío si falta un dato.')
    ft_tiempo_calculo = fields.Char(string='Cálculo del tiempo', compute='_compute_ft_tiempo_ficha')
    ft_tiempo_difiere = fields.Boolean(string='Tiempo distinto de la ficha', compute='_compute_ft_tiempo_ficha',
                                       search='_search_ft_tiempo_difiere')

    # ------------------------------------------------------------------
    # Datos comunes
    # ------------------------------------------------------------------
    def _ft_bom_kg(self):
        """True si la lista de la operación está en kg (las cuentas por kg solo aplican ahí)."""
        kg = self.env.ref('uom.product_uom_kgm', raise_if_not_found=False)
        return bool(kg) and self.bom_id.product_uom_id == kg

    def _ft_producto_bom(self):
        bom = self.bom_id
        return bom.product_id or bom.product_tmpl_id.product_variant_ids[:1]

    def _ft_por_ciclo(self, por_kg, texto):
        """min/kg × la capacidad del ciclo de la máquina para el producto (1 kg si no está capturada)."""
        bom = self.bom_id
        capacidad = self.workcenter_id._get_capacity(
            self._ft_producto_bom(), bom.product_uom_id, bom.product_qty or 1)[0] or 1.0
        if capacidad != 1.0:
            texto += ' × %s kg por ciclo' % _fmt(capacidad)
        return por_kg * capacidad, texto

    def _ft_linea_crudo(self):
        """(kg de crudo por kg de la lista, ficha de tejido del crudo, línea) de la primera línea de la lista cuyo
        producto tiene ficha de tejido; (0, ficha vacía, línea vacía) si no hay."""
        self.ensure_one()
        Tejido = self.env['ficha.tecnica.tejido']
        kg = self.env.ref('uom.product_uom_kgm', raise_if_not_found=False)
        bom = self.bom_id
        for line in bom.bom_line_ids:
            ficha = Tejido._ft_de_producto(line.product_id)
            if not ficha:
                continue
            if not kg or line.product_uom_id == kg:
                qty = line.product_qty
            elif line.product_uom_id._has_common_reference(kg):
                qty = line.product_uom_id._compute_quantity(line.product_qty, kg, round=False)
            else:
                continue
            return (qty / (bom.product_qty or 1.0)), ficha, line
        return 0.0, Tejido, self.env['mrp.bom.line']

    # ------------------------------------------------------------------
    # Cálculo por tipo de máquina
    # ------------------------------------------------------------------
    def _ft_tiempo_desde_ficha(self):
        """(minutos por ciclo, texto del cálculo o del dato que falta). (0, False) si la operación no tiene cálculo
        desde la ficha (máquina sin proceso del artículo o de un tipo sin fórmula)."""
        self.ensure_one()
        tipo = self.workcenter_id.ft_tipo_id.code
        if tipo == TIPO_TINTORERIA:
            return self._ft_tiempo_tintoreria()
        proceso = self.ft_proceso_id
        if not proceso:
            return 0.0, False
        if proceso.valor_ids.filtered(lambda v: v.code == 'ancho_rama'):
            return self._ft_tiempo_rama()
        if tipo == TIPO_INSPECCION:
            return self._ft_tiempo_inspeccion()
        if tipo in (TIPO_DEVANADORA, TIPO_ABRIDORA):
            return self._ft_tiempo_crudo()
        return 0.0, False

    def _ft_tiempo_rama(self):
        """(minutos por ciclo, texto del cálculo o del dato que falta). (0, False) si la operación no es de rama."""
        self.ensure_one()
        proceso = self.ft_proceso_id
        if not proceso or not proceso.valor_ids.filtered(lambda v: v.code == 'ancho_rama'):
            return 0.0, False
        if not self._ft_bom_kg():
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
        texto = '1000 ÷ (%s g/m² × %s m) = %s m/kg × (1 − %s %%) = %s m/kg ÷ %s m/min = %s min/kg' % (
            _fmt(peso), _fmt(ancho), _fmt(round(metros, 2)), _fmt(merma), _fmt(round(netos, 2)), _fmt(velocidad),
            ('%.4f' % por_kg))
        return self._ft_por_ciclo(por_kg, texto)

    def _ft_tiempo_inspeccion(self):
        """2.23.0: rendimiento de tela acabada (m/kg) × 60 ÷ velocidad de inspección (m/h)."""
        self.ensure_one()
        proceso = self.ft_proceso_id
        if not self._ft_bom_kg():
            return 0.0, 'La lista de materiales no está en kg.'
        faltan = []
        acabado = proceso.ficha_acabado_id
        rendimiento = acabado.rendimiento_tela_acabada
        if not acabado:
            faltan.append('la ficha de acabado')
        elif not rendimiento:
            faltan.append('el rendimiento de tela acabada en la ficha de acabado')
        velocidad = proceso._ft_value('velocidad_inspeccion')
        if not velocidad:
            faltan.append('la velocidad de inspección (rollos con 1 empalme)')
        if faltan:
            return 0.0, 'Falta %s en %s.' % (', '.join(faltan), proceso.display_name)
        por_kg = rendimiento * 60.0 / velocidad
        texto = '%s m/kg × 60 ÷ %s m/h = %s min/kg' % (_fmt(rendimiento), _fmt(velocidad), '%.4f' % por_kg)
        return self._ft_por_ciclo(por_kg, texto)

    def _ft_tiempo_crudo(self):
        """2.23.0: devanado y abridora. Metros de crudo por kg de la lista ÷ velocidad (m/min)."""
        self.ensure_one()
        proceso = self.ft_proceso_id
        if not self._ft_bom_kg():
            return 0.0, 'La lista de materiales no está en kg.'
        faltan = []
        factor, tejido, _line = self._ft_linea_crudo()
        rendimiento = tejido._ft_rendimiento() if tejido else 0.0
        if not tejido:
            faltan.append('un crudo con ficha de tejido en la lista de materiales')
        elif not factor:
            faltan.append('la cantidad del crudo en la lista de materiales')
        elif not rendimiento:
            faltan.append('el rendimiento en la ficha de tejido %s' % tejido.display_name)
        codigo, velocidad = 'velocidad', 0.0
        if self.workcenter_id.ft_tipo_id.code == TIPO_ABRIDORA:
            especifica = VELOCIDAD_ABRIDORA.get(tejido.salida_tela) if tejido else False
            velocidad = proceso._ft_value(especifica) if especifica else 0.0
            codigo = especifica if velocidad else 'velocidad'
        velocidad = velocidad or proceso._ft_value('velocidad')
        if not velocidad:
            faltan.append('la velocidad')
        if faltan:
            return 0.0, 'Falta %s en %s.' % (', '.join(faltan), proceso.display_name)
        metros = factor * rendimiento
        por_kg = metros / velocidad
        texto = '%s kg de crudo × %s m/kg = %s m/kg ÷ %s m/min%s = %s min/kg' % (
            _fmt(round(factor, 4)), _fmt(rendimiento), _fmt(round(metros, 2)), _fmt(velocidad),
            ' (tela %s)' % tejido.salida_tela.lower() if codigo != 'velocidad' else '', '%.4f' % por_kg)
        return self._ft_por_ciclo(por_kg, texto)

    def _ft_minutos_bano(self):
        """2.23.0: (minutos por baño, de dónde salen) del artículo en el jet: su tiempo de ciclo y, si no hay, los
        minutos de la fórmula de la lista en ese jet. (0, False) si no hay ninguno."""
        self.ensure_one()
        proceso = self.ft_proceso_id
        valor = proceso.valor_ids.filtered(lambda v: v.code == 'tiempo_ciclo')[:1] if proceso else False
        if valor and valor.valor:
            en_minutos = (valor.unit or 'h').strip().lower() in ('min', 'mins', 'minutos')
            minutos = valor.valor * (1.0 if en_minutos else 60.0)
            return minutos, 'tiempo de ciclo del artículo en %s: %s %s' % (
                self.workcenter_id.name, _fmt(valor.valor), 'min' if en_minutos else 'h')
        bom = self.bom_id
        if 'formula_tintoreria_id' in bom._fields and 'tintoreria.formula.tiempo' in self.env \
                and bom.formula_tintoreria_id:
            tiempo = self.env['tintoreria.formula.tiempo'].search([
                ('formula_id', '=', bom.formula_tintoreria_id.id), ('workcenter_id', '=', self.workcenter_id.id)],
                limit=1)
            if tiempo:
                return tiempo.minutos, 'fórmula %s en %s' % (bom.formula_tintoreria_id.display_name,
                                                              self.workcenter_id.name)
        return 0.0, False

    def _ft_tiempo_tintoreria(self):
        """2.23.0: minutos por baño (un ciclo = la capacidad del jet para el producto)."""
        self.ensure_one()
        minutos, origen = self._ft_minutos_bano()
        if not minutos:
            if not self.ft_proceso_id and not ('formula_tintoreria_id' in self.bom_id._fields
                                               and self.bom_id.formula_tintoreria_id):
                return 0.0, False
            return 0.0, ('Falta el tiempo de ciclo del artículo en %s o el tiempo de la fórmula de la lista en ese '
                         'jet.' % self.workcenter_id.name)
        product = self._ft_producto_bom()
        capturada = self.workcenter_id.capacity_ids.filtered(
            lambda c: c.capacity and (c.product_id == product or not c.product_id))
        if not capturada:
            return 0.0, ('%s min por baño (%s), pero falta la capacidad de %s para el producto: sin ella un ciclo no '
                         'es un baño.' % (_fmt(minutos), origen, self.workcenter_id.name))
        capacidad = self.workcenter_id._get_capacity(product, self.bom_id.product_uom_id,
                                                     self.bom_id.product_qty or 1)[0]
        return minutos, '%s min por baño (%s); un ciclo = %s %s, la capacidad del jet' % (
            _fmt(minutos), origen, _fmt(capacidad), self.bom_id.product_uom_id.name or '')

    @api.depends('ft_proceso_id', 'time_mode', 'time_cycle_manual', 'bom_id.product_uom_id', 'bom_id.product_qty',
                 'bom_id.bom_line_ids.product_qty', 'workcenter_id.capacity_ids.capacity')
    def _compute_ft_tiempo_ficha(self):
        for op in self:
            tiempo, texto = op._ft_tiempo_desde_ficha()
            op.ft_tiempo_ficha = tiempo
            op.ft_tiempo_calculo = texto
            op.ft_tiempo_difiere = bool(tiempo) and (
                op.time_mode != 'manual' or abs(op.time_cycle_manual - tiempo) > tiempo * TOLERANCIA_REL)

    def _search_ft_tiempo_difiere(self, operator, value):
        """2.23.0: «distinto» / «igual» de la ficha, sobre las operaciones en máquinas con tipo (también las de
        listas archivadas: las listas en kg nacen archivadas)."""
        # Odoo 19 normaliza «= True» a «in [True]» antes de llamar aquí: se aceptan las dos formas.
        values = list(value) if hasattr(value, '__iter__') and not isinstance(value, (str, bytes)) else [value]
        if operator in ('!=', 'not in'):
            want = not any(values)
        elif operator in ('=', 'in'):
            want = any(values)
        else:
            return NotImplemented
        ops = self.with_context(active_test=False).search([('workcenter_id.ft_tipo_id', '!=', False)])
        return [('id', 'in' if want else 'not in', ops.filtered('ft_tiempo_difiere').ids)]

    def action_ft_usar_tiempo_ficha(self):
        """Pasa el tiempo de la ficha a la lista (tiempo fijo) en las operaciones que lo tienen."""
        for op in self:
            tiempo = op._ft_tiempo_desde_ficha()[0]
            if tiempo:
                op.write({'time_mode': 'manual', 'time_cycle_manual': round(tiempo, 4)})
        return True

