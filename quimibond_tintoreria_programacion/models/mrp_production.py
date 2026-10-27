# -*- coding: utf-8 -*-
import math

from odoo import _, api, fields, models
from odoo.exceptions import UserError
from odoo.tools import float_round

from odoo.addons.quimibond_ficha_tecnica_tela.models.mrp_aviso_maquina import GRUPOS


def banda_rendimiento(rendimiento):
    """Banda A/B/C del rendimiento de tejido (m/kg), con los mismos grupos que las fichas técnicas
    (GRUPOS): el límite inferior pertenece a la banda y el superior a la siguiente.
    Fuera de todas las bandas devuelve False."""
    rendimiento = rendimiento or 0.0
    for _codigo, grupo, desde, hasta in GRUPOS:
        if desde <= rendimiento < hasta:
            return grupo
    return False


class MrpProduction(models.Model):
    _inherit = 'mrp.production'

    tintoreria_programacion_id = fields.Many2one(
        'tintoreria.programacion', 'Programación de Tintorería', copy=False, readonly=True,
        index=True, ondelete='set null')
    tintoreria_programada = fields.Boolean(
        'Tintorería programada', copy=False, readonly=True, tracking=True,
        help="Tintorería ya asignó el HTJ de esta orden y se calculó el tamaño de baño.")
    requiere_programacion_tintoreria = fields.Boolean(
        'Requiere programación de Tintorería', compute='_compute_requiere_programacion_tintoreria',
        store=True,
        help="Producto en kg (ligado como «Acabado en proceso» en una ficha técnica de acabado) "
             "y una operación de Tintorería en su BoM.")
    tintoreria_pendiente = fields.Boolean(
        'Pendiente de programar Tintorería', compute='_compute_tintoreria_pendiente',
        store=True, index=True)

    # ------------------------------------------------------------------
    # Campos calculados
    # ------------------------------------------------------------------
    def _es_producto_acabado_kg(self):
        """True si el producto es el «Acabado en proceso» (kg) de alguna ficha técnica de acabado.
        El vínculo kg <-> metros vive en la ficha (product_proceso_id), no en el código del artículo."""
        self.ensure_one()
        if not self.product_id:
            return False
        return bool(self.env['ficha.tecnica.acabado'].search_count(
            [('product_proceso_id', '=', self.product_id.id)], limit=1))

    @api.depends('product_id', 'bom_id', 'bom_id.operation_ids.workcenter_id',
                 'bom_id.operation_ids.name')
    def _compute_requiere_programacion_tintoreria(self):
        # Nota: se calcula al crear la orden; si la ficha de acabado se liga después, las órdenes ya
        # creadas no se recalculan solas.
        for orden in self:
            orden.requiere_programacion_tintoreria = bool(
                orden.product_id and orden._es_producto_acabado_kg()
                and orden.bom_id and orden.bom_id._operacion_tintoreria())

    @api.depends('requiere_programacion_tintoreria', 'tintoreria_programada', 'state')
    def _compute_tintoreria_pendiente(self):
        for orden in self:
            orden.tintoreria_pendiente = (
                orden.requiere_programacion_tintoreria
                and not orden.tintoreria_programada
                and orden.state not in ('done', 'cancel'))

    # ------------------------------------------------------------------
    # Datos para el cálculo
    # ------------------------------------------------------------------
    def _workorder_tintoreria(self):
        self.ensure_one()
        operacion = self.bom_id._operacion_tintoreria()
        return self.workorder_ids.filtered(lambda wo: wo.operation_id == operacion)[:1]

    def _rendimiento_tejido(self):
        """Rendimiento de tejido (m/kg) del producto en kg: el que fija la banda y la carga del jet."""
        self.ensure_one()
        rendimiento, _origen = self.product_id._ft_rendimiento()
        if not rendimiento:
            raise UserError(_(
                "No se encontró el rendimiento de tejido de %s. Revisa que tenga ficha de tejido con peso "
                "y ancho, o que su ficha de acabado esté ligada a ella como «Acabado en proceso».",
                self.product_id.display_name))
        return rendimiento

    def _capacidad_jet(self, workcenter, rendimiento):
        """(capacidad por baño en kg, banda, aviso) del jet para este producto.

        Fuente única: la configuración vigente del jet en las fichas técnicas.
        * Si el artículo tiene «kg por carga» en este jet, ese manda.
        * Jet con bandas capturadas: bloquea si la banda es N/A o el rendimiento cae fuera de las bandas.
        * Jet sin ninguna banda capturada (p. ej. HTJ5): su capacidad general, con aviso.
        """
        self.ensure_one()
        banda = banda_rendimiento(rendimiento)
        cap_articulo, motivo = workcenter._ft_capacidad_carga(rendimiento, product=self.product_id)
        if cap_articulo and 'artículo' in (motivo or ''):
            return cap_articulo, banda, False
        config = workcenter._ft_config_at()
        if not config:
            raise UserError(_(
                "El centro de trabajo %s no tiene configuración vigente en las fichas técnicas de máquina.",
                workcenter.display_name))
        general = config._ft_value('capacidad') or 0.0
        capacidades = {grupo: config._ft_value(codigo) or 0.0 for codigo, grupo, _d, _h in GRUPOS}
        if not any(capacidades.values()):
            if general <= 0:
                raise UserError(_(
                    "%s no tiene capacidad capturada (ni general ni por banda).", workcenter.display_name))
            aviso = _(
                "%(wc)s no tiene bandas de rendimiento capturadas: se usó su capacidad general de %(cap)s kg.",
                wc=workcenter.display_name, cap=general)
            return general, banda, aviso
        if not banda:
            raise UserError(_(
                "El rendimiento de tejido de %(prod)s (%(r)s m/kg) está fuera de las bandas de rendimiento "
                "de %(wc)s. Corrige el rendimiento en la ficha técnica.",
                prod=self.product_id.display_name, r=rendimiento, wc=workcenter.display_name))
        capacidad = capacidades[banda]
        if capacidad <= 0:
            raise UserError(_(
                "%(wc)s no puede procesar este artículo: la banda %(b)s (rendimiento de %(r)s m/kg) "
                "es N/A en ese jet.", wc=workcenter.display_name, b=banda, r=rendimiento))
        if general > 0:
            capacidad = min(capacidad, general)
        return capacidad, banda, False

    def _minutos_tintoreria(self, workcenter, kg_bano=None):
        """(minutos por baño, origen, aviso) de la operación de Tintorería en el HTJ dado.

        El tiempo de la ficha y el de la fórmula son por ORDEN (un solo ciclo), no por kg. Orden de
        prioridad (diseño §3.1):
        1. «Tiempo de ciclo» del artículo en ese jet (proceso del artículo en la ficha técnica; la
           ficha lo guarda en horas).
        2. Minutos de la fórmula de la BoM en ese HTJ (``tintoreria.formula.tiempo``).
        3. La operación de Tintorería de la BoM (con aviso). 1.5.0: su tiempo es el de Odoo, por
           unidad de la BoM (en las listas en kg, minutos por kg), así que se multiplica por los kg
           del baño (``kg_bano``; si no se da, los de la orden), como lo calcula Odoo.
        Si no existe ninguno la programación se bloquea (no se asume un valor).
        """
        self.ensure_one()
        productos = self.product_id
        acabados = self.env['ficha.tecnica.acabado'].search([('product_proceso_id', '=', self.product_id.id)])
        productos |= acabados.mapped('product_acabado_id')
        codigos = [c for c in productos.mapped('default_code') if c]
        valor = self.env['ficha.tecnica.proceso.valor'].search([
            ('code', '=', 'tiempo_ciclo'),
            ('workcenter_id', '=', workcenter.id),
            ('valor', '>', 0),
            '|', ('proceso_id.product_id', 'in', productos.ids), ('proceso_id.articulo', 'in', codigos),
        ], limit=1)
        if valor:
            minutos = valor.valor * (1.0 if (valor.unit or 'h').lower() in ('min', 'mins', 'minutos') else 60.0)
            return minutos, 'ficha', False
        formula = self.bom_id.formula_tintoreria_id
        if formula:
            tiempo = self.env['tintoreria.formula.tiempo'].search([
                ('formula_id', '=', formula.id), ('workcenter_id', '=', workcenter.id)], limit=1)
            if tiempo:
                return tiempo.minutos, 'formula', False
        operacion = self.bom_id._operacion_tintoreria()
        if operacion and operacion.time_mode == 'manual' and operacion.time_cycle_manual > 0:
            kg = self.product_qty if kg_bano is None else kg_bano
            minutos = self._minutos_operacion_bom(operacion, workcenter, kg)
            aviso = _(
                "No hay tiempo de ciclo del artículo en %(wc)s ni tiempo de la fórmula %(f)s en ese HTJ: "
                "se calcularon %(m)s min por baño con la operación de Tintorería de la BoM (%(t)s min por "
                "%(q)s %(u)s, %(kg)s kg por baño).",
                wc=workcenter.display_name, f=formula.display_name or '-', m=round(minutos, 1),
                t=operacion.time_cycle_manual, q=operacion.bom_id.product_qty or 1,
                u=operacion.bom_id.product_uom_id.name, kg=kg)
            return minutos, 'bom', aviso
        raise UserError(_(
            "No hay tiempo de Tintorería para %(prod)s en %(wc)s: captura el tiempo de ciclo del artículo "
            "en el jet (ficha técnica) o el tiempo de su fórmula en ese HTJ.",
            prod=self.product_id.display_name, wc=workcenter.display_name))

    def _minutos_operacion_bom(self, operacion, workcenter, kg):
        """1.5.0: minutos de la operación de la BoM para ``kg`` en el HTJ, con la misma cuenta que Odoo
        (``mrp.workorder._get_duration_expected``): preparación + limpieza + ciclos × tiempo del ciclo,
        entre la eficiencia de la máquina; un ciclo es la cantidad de la BoM (o la capacidad del HTJ
        para el producto, si está capturada)."""
        self.ensure_one()
        bom = operacion.bom_id
        capacidad, preparacion, limpieza = workcenter._get_capacity(
            self.product_id, bom.product_uom_id, bom.product_qty or 1)
        cantidad = self.product_uom_id._compute_quantity(kg, bom.product_uom_id)
        ciclos = float_round(cantidad / (capacidad or 1), precision_digits=0, rounding_method='UP')
        return preparacion + limpieza + ciclos * operacion.time_cycle_manual * 100.0 / (
            workcenter.time_efficiency or 100.0)

    def _calcular_plan_tintoreria(self, workcenter):
        """Capacidad por baño, número de baños y kg por baño (reparto simétrico) en el HTJ dado."""
        self.ensure_one()
        rendimiento = self._rendimiento_tejido()
        capacidad, banda, aviso = self._capacidad_jet(workcenter, rendimiento)
        kg_total = self.product_qty
        n_banos = max(1, math.ceil(round(kg_total / capacidad, 6)))
        uom = self.product_uom_id
        kg_bano = uom.round(kg_total / n_banos)
        montos = [kg_bano] * (n_banos - 1)
        montos.append(uom.round(kg_total - sum(montos)))  # el redondeo se absorbe en el último baño
        # 1.5.0: después de repartir, porque el tiempo de la operación de la BoM va por kg del baño.
        minutos, origen_tiempo, aviso_tiempo = self._minutos_tintoreria(workcenter, kg_bano=max(montos))
        return {
            'rendimiento': rendimiento,
            'banda': banda,
            'capacidad_kg': capacidad,
            'n_banos': n_banos,
            'kg_bano': kg_bano,
            'montos': montos,
            'aviso': aviso,
            'minutos': minutos,
            'origen_tiempo': origen_tiempo,
            'aviso_tiempo': aviso_tiempo,
        }

    # ------------------------------------------------------------------
    # Programación
    # ------------------------------------------------------------------
    def _check_programable(self):
        self.ensure_one()
        if not self.requiere_programacion_tintoreria:
            raise UserError(_(
                "La orden %s no requiere programación de Tintorería (el producto no está ligado como "
                "«Acabado en proceso» en una ficha de acabado, o la BoM no tiene operación de Tintorería).",
                self.name))
        if self.tintoreria_programada:
            raise UserError(_("La orden %s ya está programada.", self.name))
        if self.state not in ('draft', 'confirmed'):
            raise UserError(_("La orden %s ya inició o está cerrada; no se puede programar.", self.name))
        # Odoo 19: estados de la workorder = blocked / ready / progress / done / cancel
        if any(wo.state not in ('blocked', 'ready') for wo in self.workorder_ids) \
                or any(self.move_raw_ids.mapped('picked')):
            raise UserError(_("La orden %s ya tiene operaciones iniciadas; no se puede programar.", self.name))
        if not self._workorder_tintoreria():
            raise UserError(_("La orden %s no tiene operación de Tintorería.", self.name))

    def _programar_tintoreria(self, workcenter):
        """Asigna el HTJ, divide la MO en baños simétricos si hace falta y marca las órdenes como
        programadas. Devuelve el registro tintoreria.programacion."""
        self.ensure_one()
        self._check_programable()
        plan = self._calcular_plan_tintoreria(workcenter)
        nombre_origen = self.name
        kg_total = self.product_qty
        producto = self.product_id

        ordenes = self
        if plan['n_banos'] > 1:
            # División nativa de Odoo 19: conserva los enlaces MTO (move_dest_ids) hacia la MO de B
            # y reparte los consumos de manera proporcional.
            ordenes = self._split_productions({self: plan['montos']})

        programacion = self.env['tintoreria.programacion'].create({
            'name': nombre_origen,
            'product_id': producto.id,
            'workcenter_id': workcenter.id,
            'kg_total': kg_total,
            'rendimiento': plan['rendimiento'],
            'banda': plan['banda'],
            'capacidad_kg': plan['capacidad_kg'],
            'n_banos': plan['n_banos'],
            'kg_bano': plan['kg_bano'],
            'minutos_bano': plan['minutos'],
            'origen_tiempo': plan['origen_tiempo'],
        })
        for orden in ordenes:
            # Asignar el HTJ dispara el recálculo de la dosificación (quimibond_tintoreria_dosificacion).
            workorder = orden._workorder_tintoreria()
            workorder.workcenter_id = workcenter
            # Tintorería es tiempo por baño (cada orden es un baño). Se escribe después de asignar el
            # HTJ para que ese cambio no lo reemplace.
            workorder.duration_expected = plan['minutos']
            orden.write({
                'tintoreria_programada': True,
                'tintoreria_programacion_id': programacion.id,
            })
            orden.message_post(body=_(
                "Tintorería programada en %(wc)s: %(n)s baño(s) de %(kg)s kg (orden de origen %(o)s).",
                wc=workcenter.display_name, n=plan['n_banos'], kg=orden.product_qty, o=nombre_origen))
            if plan['aviso']:
                orden.message_post(body=plan['aviso'])
            if plan['aviso_tiempo']:
                orden.message_post(body=plan['aviso_tiempo'])
        return programacion

    # ------------------------------------------------------------------
    # Validaciones de arranque
    # ------------------------------------------------------------------
    def _check_tintoreria_programada(self):
        for orden in self:
            if orden.requiere_programacion_tintoreria and not orden.tintoreria_programada:
                raise UserError(_(
                    "La orden %s no tiene la Programación de Tintorería confirmada. Tintorería debe asignar "
                    "el HTJ antes de iniciar cualquier operación.", orden.name))

    def action_start(self):
        self._check_tintoreria_programada()
        return super().action_start()

    def button_mark_done(self):
        self._check_tintoreria_programada()
        return super().button_mark_done()
