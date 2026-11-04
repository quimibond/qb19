# -*- coding: utf-8 -*-
"""57.198.0 (Dirección General 2026-11-04): la «Capacidad a utilizar» toma la ruta de la app de fichas técnicas.

Jessica Francisco reportó que la tabla no traía las máquinas en las que se hace el artículo. La ruta salía de las listas
de materiales del desarrollo, que hasta Muestra solo tienen el tejido. Ahora:

- **Ruta**: la de la familia del crudo (``ficha.tecnica.ruta``, lista en kg: devanado → tintorería → abridora → rama →
  inspección). Si el desarrollo ya tiene su ficha de acabado en esa familia, la de la ficha, con su jet y su rama
  propios. Tintorería: lavado o teñido según el color del código (tabla de Innovación).
- **Telas de color al jet de colores** (``dev_capacity_color_jet_id``; vacío: el centro «TINTORERIA 4»): teñido y no
  blanco (BL). Datos de Manufactura, correo del 4-nov-2026: los colores van al jet 4. Un jet propio en la ficha manda.
- **Capacidad teórica** de cada máquina, en kg al mes: horas de su calendario en 30 días × eficiencia ÷ minutos por kg
  de la ficha (las mismas cuentas de «Usar el tiempo de la ficha»). Jets: kg por baño (regla única de las fichas) ×
  baños al mes (horas ÷ minutos por baño). El tiempo sale del artículo en kg del desarrollo, si no del de la tela base
  y si no de las telas de la familia; en los jets, solo de una tela que se trate igual (lavado o teñido) y con tiempo de
  ciclo en su ficha, y si ninguna lo tiene, la mediana de los tiempos de ciclo capturados en ese jet. Sin dato no hay capacidad: la fila dice «Sin capacidad capturada».
- Con ruta de fichas, de las listas de materiales solo cuentan las operaciones del crudo (tejido); las circulares
  siguen con su producción real (57.158.0 / 57.196.0).
"""
from datetime import timedelta
from statistics import median

from odoo import fields, models

PARAM_COLOR_JET = 'quimibond_sgi.dev_capacity_color_jet_id'
COLOR_JET_NAME = 'TINTORERIA 4'
# Color del código (posiciones 10 y 11) que no es «de color» aunque se tiña: el blanco óptico va a su jet de familia.
COLORES_NO_COLOR = ('BL',)
CAPACITY_DAYS = 30
STEP_LABELS = {
    'es': {'devanado': "Devanado", 'tintoreria': "Tintorería", 'abridora_volteado': "Abridora volteado",
           'abridora': "Abridora", 'rama': "Rama", 'inspeccion': "Inspección", 'theoretical': "capacidad teórica",
           'no_machine': "sin máquina"},
    'en': {'devanado': "Winding", 'tintoreria': "Dyeing", 'abridora_volteado': "Opening (turning)",
           'abridora': "Opening", 'rama': "Stenter", 'inspeccion': "Inspection", 'theoretical': "theoretical capacity",
           'no_machine': "no machine"},
}


class ProjectProjectDevCapacityFichas(models.Model):
    _inherit = 'project.project'

    # ------------------------------------------------------------------
    # Ruta de la app de fichas
    # ------------------------------------------------------------------
    def _sgi_dev_capacity_ficha(self):
        """Ficha de acabado del desarrollo (por proyecto o por su artículo); vacía si no hay."""
        self.ensure_one()
        Acabado = self.env['ficha.tecnica.acabado'].sudo()
        ficha = Acabado.search([('project_id', '=', self.id)], order='id desc', limit=1)
        if not ficha and self.sgi_dev_product_id:
            ficha = Acabado.search([('product_acabado_id', '=', self.sgi_dev_product_id.id)], order='id desc', limit=1)
        return ficha

    def _sgi_dev_capacity_color_jet(self):
        """Jet de las telas de color: el parámetro o, si está vacío, el centro llamado «TINTORERIA 4»."""
        Workcenter = self.env['mrp.workcenter'].sudo()
        value = self.env['ir.config_parameter'].sudo().get_param(PARAM_COLOR_JET)
        jet = Workcenter.browse(int(value)).exists() if value and str(value).isdigit() else Workcenter
        return jet or Workcenter.search([('name', '=', COLOR_JET_NAME)], limit=1)

    def _sgi_dev_capacity_route_steps(self):
        """[(paso, máquina)] de la lista en kg según la ruta de la familia del crudo; [] si el crudo no tiene ruta."""
        self.ensure_one()
        from odoo.addons.quimibond_ficha_tecnica_tela.models.ruta_familia import tintoreria_por_codigo
        crudo = self.sgi_dev_product_crudo_id
        Ruta = self.env['ficha.tecnica.ruta'].sudo()
        ruta = Ruta.search([('crudo_id', '=', crudo.id)], limit=1) if crudo else Ruta
        if not ruta:
            return []
        code = self.sgi_dev_product_id.default_code or ''
        ficha = self._sgi_dev_capacity_ficha()
        if ficha and ficha.ruta_familia_id == ruta:
            pasos = ficha._ft_pasos('kg')
            tintoreria, jet_propio = ficha.ruta_tintoreria, ficha.ruta_jet_id
        else:
            tintoreria, jet_propio = tintoreria_por_codigo(code), False
            pasos = [(p, p.workcenter_id) for p in ruta.paso_ids.sorted(lambda p: (p.sequence, p.id))
                     if p.paso != 'clasificacion' and p.condicion in ('siempre', tintoreria)]
        bare = code.upper()[1:] if code.upper().startswith('I') else code.upper()
        color = bare[9:11]
        color_jet = self._sgi_dev_capacity_color_jet() \
            if tintoreria == 'tenido' and not jet_propio and color not in COLORES_NO_COLOR else False
        steps = []
        for paso, wc in pasos:
            if paso.paso == 'tintoreria' and color_jet:
                wc = color_jet
            steps.append((paso.paso, wc))
        return steps

    def _sgi_dev_capacity_operations(self):
        """Operaciones de las listas de materiales que van a «Capacidad a utilizar»: todas, o solo las del crudo cuando
        la ruta sale de la app de fichas (el acabado lo pone la ruta)."""
        self.ensure_one()
        ops = self._sgi_dev_route_operations()
        if self._sgi_dev_capacity_route_steps():
            ops = [(product, op) for product, op in ops if product == self.sgi_dev_product_crudo_id]
        return ops

    # ------------------------------------------------------------------
    # Capacidad teórica
    # ------------------------------------------------------------------
    def _sgi_dev_capacity_hours(self, workcenter):
        """Horas de trabajo de la máquina en los próximos 30 días según su calendario, por su eficiencia."""
        calendar = workcenter.resource_calendar_id
        if not calendar:
            return 0.0
        start = fields.Datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        hours = calendar.get_work_hours_count(start, start + timedelta(days=CAPACITY_DAYS), compute_leaves=False)
        return hours * (workcenter.time_efficiency or 100.0) / 100.0

    def _sgi_dev_capacity_ref_products(self):
        """[(artículo en kg, su lista en kg)] de donde se toman los tiempos, en orden: el acabado en kg del desarrollo,
        el de la tela base y los de las telas de la familia del crudo."""
        self.ensure_one()
        Acabado = self.env['ficha.tecnica.acabado'].sudo()
        Revision = self.env['ficha.tecnica.revision'].sudo()
        kg = self.env.ref('uom.product_uom_kgm', raise_if_not_found=False)
        products = self.sgi_dev_product_proceso_id
        if self.sgi_dev_base_product_id:
            products |= Acabado.search([('product_acabado_id', '=', self.sgi_dev_base_product_id.id)]).product_proceso_id
        if self.sgi_dev_product_crudo_id:
            products |= Acabado.search([('tejido_id.product_proceso_id', '=', self.sgi_dev_product_crudo_id.id)],
                                       order='id').product_proceso_id
        refs = []
        for product in products:
            bom = Revision._ft_bom_de(product)
            if bom and (not kg or bom.product_uom_id == kg):
                refs.append((product, bom))
        return refs

    def _sgi_dev_capacity_theoretical(self, step, workcenter, refs):
        """(kg al mes, de dónde sale) de la máquina para el artículo; (False, motivo) si falta un dato."""
        hours = self._sgi_dev_capacity_hours(workcenter)
        if not hours:
            return False, "%s no tiene calendario" % workcenter.name
        minutes = hours * 60.0
        from odoo.addons.quimibond_ficha_tecnica_tela.models.ruta_familia import tintoreria_por_codigo
        kind = tintoreria_por_codigo(self.sgi_dev_product_id.default_code or '')
        Op = self.env['mrp.routing.workcenter'].sudo()
        for product, bom in refs:
            op = Op.new({'name': workcenter.name, 'bom_id': bom.id, 'workcenter_id': workcenter.id})
            if step == 'tintoreria':
                # El tiempo de baño de otra tela solo sirve si se trata igual (lavado o teñido) y es de su ficha: el
                # tiempo de la fórmula de una tela natural no es el de una de color.
                if product != self.sgi_dev_product_proceso_id and (
                        not op.ft_proceso_id or tintoreria_por_codigo(product.default_code or '') != kind):
                    continue
                per_bath, origin = op._ft_minutos_bano()
                if not per_bath:
                    continue
                load, _why = workcenter._ft_capacidad_carga(product._ft_rendimiento()[0], product=product)
                if not load:
                    return False, "%s: falta su capacidad por baño" % workcenter.name
                return minutes / per_bath * load, "%s; %s kg por baño" % (origin, load)
            per_cycle, text = op._ft_tiempo_desde_ficha()
            if not per_cycle:
                continue
            cycle_kg = workcenter._get_capacity(product, bom.product_uom_id, bom.product_qty or 1)[0] or 1.0
            return minutes / per_cycle * cycle_kg, "%s (%s)" % (text, product.default_code or product.name)
        if step == 'tintoreria':
            return self._sgi_dev_capacity_jet_median(workcenter, minutes, refs)
        return False, "%s: ninguna tela de la familia tiene tiempo en la ficha" % workcenter.name

    def _sgi_dev_capacity_jet_median(self, workcenter, minutes, refs):
        """Jet sin tiempo del artículo ni de su familia: la mediana de los tiempos de ciclo capturados en ese jet."""
        Proceso = self.env['ficha.tecnica.proceso'].sudo()
        times = []
        for proceso in Proceso.search([('workcenter_id', '=', workcenter.id)]):
            valor = proceso.valor_ids.filtered(lambda v: v.code == 'tiempo_ciclo' and v.valor)[:1]
            if valor:
                en_minutos = (valor.unit or 'h').strip().lower() in ('min', 'mins', 'minutos')
                times.append(valor.valor * (1.0 if en_minutos else 60.0))
        if not times:
            return False, "%s: sin tiempos de ciclo capturados" % workcenter.name
        product = refs[0][0] if refs else self.sgi_dev_product_proceso_id
        load, _why = workcenter._ft_capacidad_carga(product._ft_rendimiento()[0] if product else 0.0,
                                                   product=product or None)
        if not load:
            return False, "%s: falta su capacidad por baño" % workcenter.name
        per_bath = median(times)
        return minutes / per_bath * load, "mediana de %d tiempos de ciclo en %s: %s min; %s kg por baño" % (
            len(times), workcenter.name, round(per_bath), load)

    def _sgi_dev_capacity_fichas(self, labels):
        """Filas de «Capacidad a utilizar» de la ruta de la app de fichas: una por máquina, con su capacidad teórica."""
        self.ensure_one()
        steps = self._sgi_dev_capacity_route_steps()
        if not steps:
            return []
        names = STEP_LABELS['en' if labels.get('capacity') == "Capacity to be used" else 'es']
        refs = self._sgi_dev_capacity_ref_products()
        rows = []
        for step, workcenter in steps:
            label = names.get(step, step)
            if not workcenter:
                rows.append({'name': "%s (%s)" % (label, names['no_machine']), 'capacity_kg': False,
                             'operations': [], 'origin': ''})
                continue
            capacity, origin = self._sgi_dev_capacity_theoretical(step, workcenter, refs)
            rows.append({'name': "%s: %s (%s)" % (label, workcenter.name, names['theoretical']),
                         'capacity_kg': capacity, 'operations': [], 'origin': origin})
        return rows
