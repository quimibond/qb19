# -*- coding: utf-8 -*-
"""Avisos de máquina en producción (2.5.0, Dirección General 2026-10-13).

Primeros usos de la ficha de la máquina en la planta:

- **Galga al programar tejido.** La orden de trabajo de tejido avisa cuando
  la circular en la que se programa corre (con su configuración vigente a la
  fecha de inicio) una galga distinta de la que pide el artículo. La galga
  del artículo sale de su ficha de tejido (galga del artículo, o la galga de
  la máquina con la que se elaboró) y, si no hay ficha, de las posiciones 7
  y 8 del código (regla DAT P-D02-01, catálogo de claves de galga). Es
  aviso, no bloqueo: hay artículos que hoy se tejen en otra galga y eso lo
  decide la planta.
- **Carga del jet en tintorería.** Las órdenes de tintorería no llevan
  orden de trabajo, así que el aviso vive en la orden de producción: una
  tabla con la capacidad que aplica en cada jet y cuántas cargas son, y si
  se elige el jet de la orden, el aviso cuando la orden no cabe en una
  carga. La capacidad sale de la configuración vigente del jet: la del grupo
  de rendimiento de la tela (A 3 a 6, B 7 a 10, C 11 a 15 m/kg; entre grupos
  se toma el de abajo: 6.5 es A) y, si la tela no tiene rendimiento, la
  capacidad general. El rendimiento sale de la ficha de tejido: 1000 /
  (peso g/m² × ancho m).

2.6.0 (Dirección General 2026-10-13): Quimibond va a pasar a **un solo
producto de acabado en kg** con todas las operaciones en su ruta (de la
recepción de tejido a la salida de inspección), sin productos de
tintorería. Por eso:

- El aviso de carga también vive en la **orden de trabajo**: si la orden de
  trabajo cae en un jet, avisa cuando la orden trae más kg de los que caben
  en una carga (igual que el aviso de galga en las circulares). La pestaña
  «Cargas por jet» aparece también en una orden que tenga una orden de
  trabajo en un jet, y ahí el jet es el de la orden de trabajo.
- El rendimiento de la tela se busca también por el camino nuevo: producto
  de acabado → su ficha de acabado → la ficha de tejido en que se basa.

2.9.0 (Dirección General 2026-10-19): el jet también tiene datos **del
artículo** (proceso del artículo en la máquina): tiempo de ciclo, kg por carga
y relación de baño. Los kg por carga del artículo ganan sobre los del grupo de
rendimiento; la relación de baño del artículo gana sobre la de la máquina.
"""
import math
import re

from markupsafe import Markup

from odoo import api, fields, models

TIPO_CIRCULAR = 'tejido_circular'
TIPO_TINTORERIA = 'tintoreria'
# Grupos de rendimiento (m/kg) de la hoja «CAP DE CARGAS» (INFORMACIÓN TINTORERIA PARA ODOO, 2026-08-08).
GRUPOS = (('capacidad_grupo_a', 'A', 3.0, 7.0), ('capacidad_grupo_b', 'B', 7.0, 11.0),
          ('capacidad_grupo_c', 'C', 11.0, 16.0))
ANCHO_A_METROS = {'m': 1.0, 'mts': 1.0, 'cm': 0.01, 'mm': 0.001, 'in': 0.0254}
# 2.9.0: «1:10» o «10» (litros por kg).
RELACION_BANO_RE = re.compile(r'^\s*(?:1\s*:\s*)?(\d+(?:[.,]\d+)?)\s*$')


def _fmt(value):
    return ('{:,.2f}'.format(value)).rstrip('0').rstrip('.')


class FichaTecnicaTejidoUso(models.Model):
    _inherit = 'ficha.tecnica.tejido'

    # 2.11.0 (Dirección General 2026-10-19, con José Sacramento): el rendimiento de la tela tejida es un campo
    # calculado y guardado (lo leen tintorería y la programación), editable para forzar un valor. Mismo nombre
    # que el campo capturado de Consolti, para que su código lo lea sin cambios.
    rendimiento_tela_tejida = fields.Float(
        string='Rendimiento de tela tejida (m/kg)', digits=(12, 4), compute='_compute_rendimiento_tela_tejida',
        store=True, readonly=False,
        help='1000 / (peso g/m² × ancho en m) de la tela acondicionada; el ancho se convierte con su unidad. 0 si '
             'falta un dato o el ancho no es creíble (fuera de 0.3 a 4 m). Si se escribe a mano, queda fijo '
             '(«Rendimiento forzado»).')
    rendimiento_manual = fields.Boolean(
        string='Rendimiento forzado',
        help='El rendimiento se escribió a mano: ya no se recalcula con el peso y el ancho. Desmárquelo para volver '
             'al cálculo.')

    def _ft_rendimiento_calculado(self):
        """Rendimiento (m/kg) con el peso y el ancho acondicionados; 0 si falta alguno o el ancho no es creíble
        (fuera de 0.3 a 4 m, p. ej. 1.92 capturado en cm)."""
        self.ensure_one()
        # 2.10.0: la unidad del catálogo convierte con su unidad de Inventario; si no, el texto.
        unidad = self.ancho_acondicionado_tol_unit_id
        metro = self.env.ref('uom.product_uom_meter', raise_if_not_found=False)
        factor = unidad._ft_factor_a(metro) if unidad and metro else 0.0
        factor = factor or ANCHO_A_METROS.get((self.ancho_acondicionado_tol_unit or 'cm').strip().lower())
        if not (self.peso_acondicionado and self.ancho_acondicionado and factor):
            return 0.0
        ancho_m = self.ancho_acondicionado * factor
        if not 0.3 <= ancho_m <= 4.0:
            return 0.0
        return 1000.0 / (self.peso_acondicionado * ancho_m)

    @api.depends('peso_acondicionado', 'ancho_acondicionado', 'ancho_acondicionado_tol_unit_id',
                 'ancho_acondicionado_tol_unit', 'rendimiento_manual')
    def _compute_rendimiento_tela_tejida(self):
        for ficha in self:
            if not ficha.rendimiento_manual:
                ficha.rendimiento_tela_tejida = ficha._ft_rendimiento_calculado()

    def write(self, vals):
        # Escribir el rendimiento a mano lo deja fijo (salvo que se diga lo contrario en la misma escritura).
        if 'rendimiento_tela_tejida' in vals and 'rendimiento_manual' not in vals:
            vals = dict(vals, rendimiento_manual=True)
        return super().write(vals)

    @api.model
    def _ft_de_producto(self, product):
        """La ficha de tejido de referencia de un producto: la que lo tiene como tela en proceso o la que lleva
        su código como artículo. 2.11.0: la de revisión más alta (activa); entre máquinas de la misma revisión,
        la de elaboración más reciente. En producción el peso y el ancho no cambian de una máquina a otra."""
        if not product:
            return self.browse()
        domain = [('product_proceso_id', '=', product.id)]
        if product.default_code:
            domain = ['|', ('articulo', '=', product.default_code)] + domain
        fichas = self.search(domain)
        if not fichas:
            return fichas

        def clave(ficha):
            rev = (ficha.revision or '0').strip()
            return (int(rev) if rev.isdigit() else -1, ficha.fecha_elaboracion or fields.Date.to_date('1900-01-01'),
                    ficha.id)
        return max(fichas, key=clave)

    def _ft_rendimiento(self):
        """Rendimiento de la tela (m/kg): el guardado en la ficha (calculado o forzado)."""
        self.ensure_one()
        return self.rendimiento_tela_tejida or 0.0


class ProductProductFicha(models.Model):
    _inherit = 'product.product'

    def _ft_galga_requerida(self):
        """(galga, de dónde sale) que pide el artículo; (0, '') si no se sabe."""
        self.ensure_one()
        ficha = self.env['ficha.tecnica.tejido']._ft_de_producto(self)
        if ficha and (ficha.galga_requerida or ficha.galga):
            return ficha.galga_requerida or ficha.galga, 'ficha de tejido %s' % ficha.display_name
        code = (self.default_code or '').strip()
        if len(code) >= 8 and code[6:8].isdigit():
            number = int(code[6:8])
            clave = self.env['ficha.tecnica.clave.codigo'].search(
                [('kind', '=', 'galga'), ('range_from', '<=', number), ('range_to', '>=', number)], limit=1)
            if clave and clave.gauge:
                return float(clave.gauge), 'código del artículo (posiciones 7 y 8: %s)' % code[6:8]
        return 0.0, ''

    def _ft_rendimiento(self):
        """(rendimiento m/kg, de dónde sale) de la tela; (0, '') si no hay ficha con peso y ancho.

        Primero la ficha de tejido del producto (tela en proceso o mismo código); si no hay, la del
        producto de acabado: su ficha de acabado → la ficha de tejido en que se basa (2.6.0, producto único
        de acabado en kg). El rendimiento de tintorería es el de tejido (regla de los dos rendimientos)."""
        self.ensure_one()
        ficha = self.env['ficha.tecnica.tejido']._ft_de_producto(self)
        rendimiento = ficha._ft_rendimiento() if ficha else 0.0
        if rendimiento:
            return rendimiento, 'ficha de tejido %s' % ficha.display_name
        domain = [('product_acabado_id', '=', self.id)]
        if self.default_code:
            domain = ['|', ('articulo', '=', self.default_code)] + domain
        for acabado in self.env['ficha.tecnica.acabado'].search(domain, order='id desc'):
            rendimiento = acabado.tejido_id._ft_rendimiento() if acabado.tejido_id else 0.0
            if rendimiento:
                return rendimiento, 'ficha de tejido %s (base de la ficha de acabado %s)' % (
                    acabado.tejido_id.display_name, acabado.display_name)
        return 0.0, ''


class MrpWorkcenterCarga(models.Model):
    _inherit = 'mrp.workcenter'

    @api.model
    def _ft_circulares_con_galga(self, galga, day=None):
        """Circulares cuya configuración vigente en ``day`` corre esa galga (checklist de factibilidad y ficha
        de proceso del desarrollo)."""
        if not galga:
            return self.browse()
        circulares = self.search([('ft_tipo_id.code', '=', TIPO_CIRCULAR)], order='name')
        return circulares.filtered(lambda wc: (wc._ft_config_at(day).galga or 0.0) == galga)

    def _ft_aviso_para(self, product, qty_kg=0.0, day=None):
        """Aviso de esta máquina para fabricar ``product``: galga distinta en una circular, o más kg de los
        que caben en una carga de un jet. False si no hay nada que avisar. Lo usan la orden de trabajo y la
        corrida de muestra del desarrollo antes de crear las órdenes."""
        self.ensure_one()
        tipo = self.ft_tipo_id.code
        if tipo == TIPO_CIRCULAR and product:
            config = self._ft_config_at(day)
            galga_maquina = config.galga if config else 0.0
            galga_articulo, origen = product._ft_galga_requerida()
            if galga_maquina and galga_articulo and galga_maquina != galga_articulo:
                return '%s corre galga %s y el artículo %s pide galga %s (%s).' % (
                    self.name, _fmt(galga_maquina), product.default_code or product.display_name,
                    _fmt(galga_articulo), origen)
        elif tipo == TIPO_TINTORERIA and qty_kg:
            rendimiento = product._ft_rendimiento()[0] if product else 0.0
            capacidad, motivo = self._ft_capacidad_carga(rendimiento, day, product)
            if not capacidad:
                return '%s: %s.' % (self.name, motivo)
            if qty_kg > capacidad:
                return 'La orden lleva %s kg y %s carga %s kg (%s): son %s cargas.' % (
                    _fmt(qty_kg), self.name, _fmt(capacidad), motivo, math.ceil(qty_kg / capacidad))
        return False

    def _ft_proceso_de(self, product):
        """2.9.0: el proceso del producto en esta máquina; vacío si no hay."""
        self.ensure_one()
        return self.env['ficha.tecnica.proceso']._ft_buscar(product, self) if product else \
            self.env['ficha.tecnica.proceso']

    def _ft_capacidad_carga(self, rendimiento, day=None, product=None):
        """(kg por carga, explicación) del jet para una tela de ese rendimiento; (0, motivo) si no aplica.
        2.9.0: si el artículo tiene «kg por carga» en este jet, ese manda."""
        self.ensure_one()
        proceso = self._ft_proceso_de(product) if product else False
        kg_articulo = proceso._ft_value('kg_carga') if proceso else False
        if kg_articulo:
            return kg_articulo, 'kg por carga del artículo en este jet'
        config = self._ft_config_at(day)
        if not config:
            return 0.0, 'sin configuración vigente'
        general = config._ft_value('capacidad') or 0.0
        if rendimiento:
            for code, grupo, desde, hasta in GRUPOS:
                if desde <= rendimiento < hasta:
                    capacidad = config._ft_value(code) or 0.0
                    if capacidad:
                        return capacidad, 'grupo %s' % grupo
                    return 0.0, 'sin capacidad para el grupo %s' % grupo
            if general:
                return general, 'rendimiento fuera de los grupos: capacidad general'
            return 0.0, 'rendimiento fuera de los grupos y sin capacidad general'
        if general:
            return general, 'capacidad general (la tela no tiene rendimiento en su ficha)'
        return 0.0, 'sin capacidad capturada'

    def _ft_relacion_bano(self, product=None, day=None):
        """2.9.0: (litros por kg, de dónde sale) para teñir ``product`` en este jet: la del artículo en el jet
        y, si no hay, la de la configuración vigente del jet («1:10» o 10). (0, motivo) si no hay ninguna."""
        self.ensure_one()
        proceso = self._ft_proceso_de(product) if product else False
        articulo = proceso._ft_value('relacion_bano_articulo') if proceso else False
        if articulo:
            return articulo, 'relación de baño del artículo en este jet'
        config = self._ft_config_at(day)
        valor = config._ft_value('relacion_bano') if config else False
        if isinstance(valor, str):
            match = RELACION_BANO_RE.match(valor)
            valor = float(match.group(1).replace(',', '.')) if match else 0.0
        if valor:
            return valor, 'relación de baño de la máquina'
        return 0.0, 'sin relación de baño'


class MrpWorkorderAviso(models.Model):
    _inherit = 'mrp.workorder'

    ft_aviso_maquina = fields.Char(
        string='Aviso de máquina', compute='_compute_ft_aviso_maquina', search='_search_ft_aviso_maquina',
        help='La circular corre una galga distinta de la que pide el artículo (según su ficha de tejido o su '
             'código), o la orden no cabe en una carga del jet. Es aviso: la decisión es de la planta.')

    def _ft_aviso(self):
        """Aviso de la máquina de la orden de trabajo (galga en circular, carga en jet)."""
        self.ensure_one()
        if not self.workcenter_id:
            return False
        qty = self.production_id._ft_qty_kg() if self.production_id else 0.0
        return self.workcenter_id._ft_aviso_para(self.product_id, qty, self.date_start or None)

    @api.depends('workcenter_id', 'product_id', 'date_start', 'production_id.product_qty',
                 'production_id.product_uom_id')
    def _compute_ft_aviso_maquina(self):
        for workorder in self:
            workorder.ft_aviso_maquina = workorder._ft_aviso()

    def _search_ft_aviso_maquina(self, operator, value):
        """Solo «con aviso» / «sin aviso» sobre órdenes abiertas en circulares y jets."""
        open_wo = self.search([('state', 'not in', ('done', 'cancel')),
                               ('workcenter_id.ft_tipo_id.code', 'in', (TIPO_CIRCULAR, TIPO_TINTORERIA))])
        with_aviso = open_wo.filtered(lambda w: w._ft_aviso())
        # Odoo 19 normaliza «!= False» a «not in [False]» antes de llamar aquí: se aceptan las dos formas.
        # El valor llega como colección (OrderedSet en Odoo 19) o suelto.
        values = list(value) if hasattr(value, '__iter__') and not isinstance(value, (str, bytes)) else [value]
        if operator in ('!=', 'not in'):
            want = not any(values)
        elif operator in ('=', 'in'):
            want = any(values)
        else:
            return NotImplemented
        return [('id', 'in' if want else 'not in', with_aviso.ids)]

    @api.onchange('workcenter_id')
    def _onchange_ft_aviso_maquina(self):
        aviso = self._ft_aviso() if len(self) == 1 else False
        if aviso:
            return {'warning': {'title': 'Aviso de máquina', 'message': aviso}}
        return None


class StockPickingTypeTintoreria(models.Model):
    _inherit = 'stock.picking.type'

    ft_cargas_jet = fields.Boolean(
        string='Calcular cargas por jet',
        help='Las órdenes de fabricación de este tipo de operación son de tintorería: muestran la capacidad de '
             'cada jet, cuántas cargas son y el aviso de carga del jet elegido.')


class MrpProductionAviso(models.Model):
    _inherit = 'mrp.production'

    ft_es_tintoreria = fields.Boolean(
        string='Orden con teñido', compute='_compute_ft_es_tintoreria',
        help='Orden de tintorería (su tipo de operación calcula cargas por jet) u orden con una orden de '
             'trabajo en un jet (producto único de acabado, 2.6.0).')
    ft_jet_wo_id = fields.Many2one('mrp.workcenter', string='Jet de la orden de trabajo',
                                   compute='_compute_ft_es_tintoreria')
    ft_jet_id = fields.Many2one(
        'mrp.workcenter', string='Jet', tracking=True, domain="[('ft_tipo_id.code', '=', 'tintoreria')]",
        help='Jet en el que se tiñe la orden. Con él Odoo avisa si la orden no cabe en una carga.')
    ft_rendimiento = fields.Float(string='Rendimiento de la tela (m/kg)', digits=(16, 2),
                                  compute='_compute_ft_cargas')
    ft_cargas_html = fields.Html(string='Cargas por jet', compute='_compute_ft_cargas', sanitize=False)
    ft_aviso_maquina = fields.Char(string='Aviso de máquina', compute='_compute_ft_cargas')

    @api.depends('picking_type_id.ft_cargas_jet', 'workorder_ids.workcenter_id')
    def _compute_ft_es_tintoreria(self):
        for production in self:
            jets = production.workorder_ids.workcenter_id.filtered(
                lambda wc: wc.ft_tipo_id.code == TIPO_TINTORERIA)
            production.ft_jet_wo_id = jets[:1]
            production.ft_es_tintoreria = bool(production.picking_type_id.ft_cargas_jet or jets)

    def _ft_qty_kg(self):
        self.ensure_one()
        kg = self.env.ref('uom.product_uom_kgm', raise_if_not_found=False)
        if not kg or not self.product_uom_id:
            return 0.0
        if self.product_uom_id == kg:
            return self.product_qty
        try:
            return self.product_uom_id._compute_quantity(self.product_qty, kg)
        except Exception:  # unidades que no son de peso (m, unidades): no se puede comparar contra kg
            return 0.0

    def _ft_jets(self):
        return self.env['mrp.workcenter'].search([('ft_tipo_id.code', '=', TIPO_TINTORERIA),
                                                  ('company_id', 'in', (self.company_id.id, False))],
                                                 order='name')

    @api.depends('product_id', 'product_qty', 'product_uom_id', 'ft_jet_id', 'picking_type_id',
                 'workorder_ids.workcenter_id', 'date_start')
    def _compute_ft_cargas(self):
        for production in self:
            avisos = [w.ft_aviso_maquina for w in production.workorder_ids if w.ft_aviso_maquina]
            rendimiento, origen = production.product_id._ft_rendimiento() if production.product_id else (0.0, '')
            production.ft_rendimiento = rendimiento
            html = False
            if production.ft_es_tintoreria:
                qty = production._ft_qty_kg()
                day = production.date_start or None
                rows = []
                elegido = production.ft_jet_wo_id or production.ft_jet_id
                for jet in production._ft_jets():
                    capacidad, motivo = jet._ft_capacidad_carga(rendimiento, day, production.product_id)
                    cargas = math.ceil(qty / capacidad) if capacidad and qty else 0
                    rows.append(Markup('<tr%s><td>%s</td><td class="text-end">%s</td>'
                                       '<td class="text-end">%s</td><td>%s</td></tr>') % (
                        Markup(' class="fw-bold"') if jet == elegido else '',
                        jet.name, _fmt(capacidad) + ' kg' if capacidad else '—', cargas or '—', motivo))
                    if jet == production.ft_jet_id and not production.ft_jet_wo_id and qty:
                        if not capacidad:
                            avisos.append('%s: %s.' % (jet.name, motivo))
                        elif qty > capacidad:
                            avisos.append('La orden lleva %s kg y %s carga %s kg (%s): son %s cargas.' % (
                                _fmt(qty), jet.name, _fmt(capacidad), motivo, cargas))
                nota = (Markup('Rendimiento %s m/kg (%s).') % (_fmt(rendimiento), origen) if rendimiento
                        else Markup('La tela no tiene rendimiento en su ficha de tejido: se usa la capacidad '
                                    'general de cada jet.'))
                if not qty:
                    nota += Markup(' La cantidad de la orden no está en kg: no se calculan cargas.')
                html = (Markup('<p class="text-muted mb-2">%s</p><table class="table table-sm o_ft_cargas">'
                               '<thead><tr><th>Jet</th><th class="text-end">Capacidad por carga</th>'
                               '<th class="text-end">Cargas</th><th>Capacidad que aplica</th></tr></thead>'
                               '<tbody>%s</tbody></table>') % (nota, Markup('').join(rows))) if rows else (
                    Markup('<p class="text-muted">No hay jets con tipo de máquina «Tintorería».</p>'))
            production.ft_cargas_html = html
            production.ft_aviso_maquina = ' '.join(avisos) or False

    @api.onchange('ft_jet_id')
    def _onchange_ft_jet_id(self):
        if len(self) == 1 and self.ft_jet_id and self.ft_aviso_maquina:
            return {'warning': {'title': 'Carga del jet', 'message': self.ft_aviso_maquina}}
        return None
