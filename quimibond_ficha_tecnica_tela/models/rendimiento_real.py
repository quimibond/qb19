# -*- coding: utf-8 -*-
"""2.24.0 (Dirección General 2026-11-03): un solo rendimiento de tela acabada para planear, calculado con los kilos y
los metros fabricados, y la especificación contra lo real.

- **Rendimiento real** de una tela = metros de la tela acabada fabricados ÷ kilos de tela consumidos (crudo, teñido o
  acabado en kg) en sus órdenes terminadas de los últimos 12 meses. Los químicos (engomado, colorantes) no cuentan.
  Ya trae la merma y el peso real: es lo que de verdad sale por kilo.
- Con 3 órdenes o más, ese número **es** el rendimiento de tela acabada de la ficha (``rendimiento_tela_acabada``):
  lo usan la conversión kg → m de la lista en metros, los tiempos de rama e inspección y la ruta. Con menos, se queda
  el capturado en la ficha. Se recalcula cada noche con la revisión de listas y con el botón de la ficha. Nunca toca
  una lista de materiales: la revisión marca las que ya no cuadran y se corrigen con «Usar los datos de la ficha».
- **Especificación contra lo real:** el rendimiento de la especificación (1000 ÷ peso × ancho de venta) contra el
  real, y el peso real equivalente (1000 ÷ rendimiento real × ancho de venta) contra el peso de la especificación y su
  tolerancia. Los renglones de Características con clave «masa» o «rendimiento» muestran su valor real.
"""
from datetime import timedelta

from odoo import api, fields, models

MESES_REAL = 12
MIN_ORDENES_REAL = 3
POSICION_PROCESO = ('H', 'I', 'J')  # crudo, teñido, acabado (DAT P-D02-01, posición 9)


def es_tela_kg(code):
    """Crudo, teñido o tela (H, I o J en la posición 9), o su código en kg con «I» al frente. Lo demás
    (engomado, colorantes, auxiliares) no es tela."""
    code = (code or '').strip().upper()
    if len(code) in (15, 17) and code.startswith('I'):
        code = code[1:]
    return len(code) in (14, 16) and code[8:9] in POSICION_PROCESO


def ancho_en_m(ancho):
    """El ancho de la ficha en metros (se captura en cm: 160; si ya viene en m: 1.6)."""
    return ancho / 100.0 if ancho > 10 else ancho


class FichaTecnicaAcabadoRendimientoReal(models.Model):
    _inherit = 'ficha.tecnica.acabado'

    rendimiento_origen = fields.Selection(
        [('real', 'Lo fabricado'), ('ficha', 'Capturado en la ficha')], string='Origen del rendimiento',
        default='ficha', readonly=True,
        help='Lo fabricado: metros ÷ kilos de sus órdenes terminadas de los últimos 12 meses (3 órdenes o más). '
             'Capturado en la ficha: la tela no tiene historia suficiente.')
    rendimiento_real = fields.Float(
        string='Rendimiento real (m/kg)', digits=(12, 4), readonly=True,
        help='Metros de la tela acabada fabricados ÷ kilos de tela consumidos (crudo, teñido o acabado en kg; sin '
             'químicos) en sus órdenes terminadas de los últimos 12 meses. Trae la merma y el peso real.')
    rendimiento_real_m = fields.Float(string='Metros fabricados', digits=(16, 1), readonly=True)
    rendimiento_real_kg = fields.Float(string='Kilos de tela consumidos', digits=(16, 1), readonly=True)
    rendimiento_real_ordenes = fields.Integer(string='Órdenes terminadas', readonly=True)
    rendimiento_real_desde = fields.Date(string='Órdenes desde', readonly=True)
    rendimiento_real_fecha = fields.Datetime(string='Rendimiento calculado el', readonly=True)
    rendimiento_especificacion = fields.Float(
        string='Rendimiento de la especificación (m/kg)', digits=(12, 4), compute='_compute_ft_real',
        help='1000 ÷ (peso × ancho de venta en m) de la especificación.')
    rendimiento_diferencia = fields.Float(
        string='Rendimiento real contra especificación (%)', digits=(12, 1), compute='_compute_ft_real',
        help='Cuánto rinde de más (+) o de menos (−) lo fabricado contra la especificación.')
    peso_real = fields.Float(
        string='Peso real (g/m²)', digits=(12, 1), compute='_compute_ft_real',
        help='El peso que da lo fabricado al ancho de venta: 1000 ÷ (rendimiento real × ancho de venta en m).')
    peso_real_diferencia = fields.Float(
        string='Peso real contra especificación (%)', digits=(12, 1), compute='_compute_ft_real')
    peso_real_resultado = fields.Selection(
        [('cumple', 'Dentro de tolerancia'), ('no_conforme', 'Fuera de tolerancia')],
        string='Peso real contra la tolerancia', compute='_compute_ft_real',
        help='El peso real contra el peso de la especificación y su tolerancia (vacío si no hay tolerancia '
             'capturada).')

    @api.depends('peso_acabado', 'ancho_acabado', 'rendimiento_real', 'peso_acabado_tol', 'peso_acabado_tol_minus',
                 'peso_acabado_tol_unit_id')
    def _compute_ft_real(self):
        for ficha in self:
            peso, ancho, real = ficha.peso_acabado, ancho_en_m(ficha.ancho_acabado), ficha.rendimiento_real
            espec = 1000.0 / (peso * ancho) if peso and ancho else 0.0
            ficha.rendimiento_especificacion = espec
            ficha.rendimiento_diferencia = (real / espec - 1.0) * 100.0 if real and espec else 0.0
            peso_real = 1000.0 / (real * ancho) if real and ancho else 0.0
            ficha.peso_real = peso_real
            ficha.peso_real_diferencia = (peso_real / peso - 1.0) * 100.0 if peso_real and peso else 0.0
            ficha.peso_real_resultado = ficha._ft_resultado_peso(peso_real)

    def _ft_resultado_peso(self, valor):
        self.ensure_one()
        arriba = self.peso_acabado_tol
        abajo = self.peso_acabado_tol_minus or arriba
        if not valor or not self.peso_acabado or not (arriba or abajo):
            return False
        if self.peso_acabado_tol_unit_id.name == '%':
            arriba, abajo = self.peso_acabado * arriba / 100.0, self.peso_acabado * abajo / 100.0
        dentro = self.peso_acabado - abajo - 1e-9 <= valor <= self.peso_acabado + arriba + 1e-9
        return 'cumple' if dentro else 'no_conforme'

    # ------------------------------------------------------------------
    # Lo fabricado
    # ------------------------------------------------------------------
    @api.model
    def _ft_fabricado(self, products, desde):
        """{producto: (metros, kilos de tela, órdenes)} de las órdenes terminadas desde ``desde`` que fabricaron
        esos productos."""
        Move = self.env['stock.move'].sudo()
        metro = self.env.ref('uom.product_uom_meter', raise_if_not_found=False)
        kg = self.env.ref('uom.product_uom_kgm', raise_if_not_found=False)
        if not products or not metro or not kg:
            return {}
        metros, ordenes, kilos = {}, {}, {}
        for product, uom, qty, n in Move._read_group(
                [('state', '=', 'done'), ('product_id', 'in', products.ids), ('production_id.state', '=', 'done'),
                 ('production_id.date_finished', '>=', desde)],
                ['product_id', 'product_uom'], ['quantity:sum', 'production_id:count_distinct']):
            if uom == metro or uom._has_common_reference(metro):
                metros[product] = metros.get(product, 0.0) + uom._compute_quantity(qty, metro, round=False)
                ordenes[product] = ordenes.get(product, 0) + n
        for product, componente, uom, qty in Move._read_group(
                [('state', '=', 'done'), ('raw_material_production_id.product_id', 'in', products.ids),
                 ('raw_material_production_id.state', '=', 'done'),
                 ('raw_material_production_id.date_finished', '>=', desde)],
                ['raw_material_production_id.product_id', 'product_id', 'product_uom'], ['quantity:sum']):
            if not es_tela_kg(componente.default_code) or not (uom == kg or uom._has_common_reference(kg)):
                continue
            kilos[product] = kilos.get(product, 0.0) + uom._compute_quantity(qty, kg, round=False)
        return {p: (metros[p], kilos.get(p, 0.0), ordenes.get(p, 0)) for p in metros}

    def _ft_actualizar_rendimiento_real(self):
        """Calcula el rendimiento real de estas fichas (todas si vacío) y, con 3 órdenes o más, lo deja como el
        rendimiento de tela acabada. No toca listas de materiales."""
        fichas = self or self.search([('product_acabado_id', '!=', False)])
        ahora = fields.Datetime.now()
        desde = fields.Date.context_today(self) - timedelta(days=round(MESES_REAL * 365 / 12))
        datos = self._ft_fabricado(fichas.product_acabado_id, desde)
        for ficha in fichas:
            m, kilos, n = datos.get(ficha.product_acabado_id, (0.0, 0.0, 0))
            real = m / kilos if m and kilos else 0.0
            vals = {'rendimiento_real': real, 'rendimiento_real_m': m, 'rendimiento_real_kg': kilos,
                    'rendimiento_real_ordenes': n, 'rendimiento_real_desde': desde, 'rendimiento_real_fecha': ahora}
            if real and n >= MIN_ORDENES_REAL:
                vals.update(rendimiento_origen='real', rendimiento_tela_acabada=round(real, 4))
            else:
                vals['rendimiento_origen'] = 'ficha'
            ficha.write(vals)
        return True

    def action_ft_rendimiento_real(self):
        """Botón «Recalcular con lo fabricado»."""
        self._ft_actualizar_rendimiento_real()
        return True


class FichaTecnicaSpecReal(models.Model):
    _inherit = 'ficha.tecnica.spec'

    real_valor = fields.Float(
        string='Real', digits=(16, 3), compute='_compute_ft_real',
        help='Lo fabricado (ficha de acabado): rendimiento real en el renglón «rendimiento» y peso real equivalente '
             'en el de «masa». Vacío en las demás características: no hay medición en Odoo.')
    real_resultado = fields.Selection(
        [('cumple', 'Cumple'), ('desviacion', 'Fuera del control interno'), ('no_conforme', 'Fuera de especificación')],
        string='Real contra especificación', compute='_compute_ft_real')

    @api.depends('caracteristica_code', 'acabado_id.rendimiento_real', 'acabado_id.peso_acabado',
                 'acabado_id.ancho_acabado', 'spec_nominal', 'spec_limit', 'spec_tol_minus', 'spec_tol_plus',
                 'spec_tol_pct', 'ctrl_tol_minus', 'ctrl_tol_plus')
    def _compute_ft_real(self):
        for line in self:
            acabado = line.acabado_id
            valor = 0.0
            if acabado and line.caracteristica_code == 'rendimiento':
                valor = acabado.rendimiento_real
            elif acabado and line.caracteristica_code == 'masa':
                valor = acabado.peso_real
            line.real_valor = valor
            line.real_resultado = line._result_for(valor) if valor else False


class FichaTecnicaRevisionRendimiento(models.Model):
    _inherit = 'ficha.tecnica.revision'

    @api.model
    def _cron_ft_actualizar(self):
        # 2.24.0: primero el rendimiento real (la revisión compara las listas contra él).
        self.env['ficha.tecnica.acabado']._ft_actualizar_rendimiento_real()
        return super()._cron_ft_actualizar()
