# -*- coding: utf-8 -*-
import math

from odoo import _, api, fields, models
from odoo.exceptions import UserError

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
        return {
            'rendimiento': rendimiento,
            'banda': banda,
            'capacidad_kg': capacidad,
            'n_banos': n_banos,
            'kg_bano': kg_bano,
            'montos': montos,
            'aviso': aviso,
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
        if any(wo.state not in ('pending', 'waiting', 'ready') for wo in self.workorder_ids) \
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
        })
        for orden in ordenes:
            # Asignar el HTJ dispara el recálculo de la dosificación (quimibond_tintoreria_dosificacion).
            orden._workorder_tintoreria().workcenter_id = workcenter
            orden.write({
                'tintoreria_programada': True,
                'tintoreria_programacion_id': programacion.id,
            })
            orden.message_post(body=_(
                "Tintorería programada en %(wc)s: %(n)s baño(s) de %(kg)s kg (orden de origen %(o)s).",
                wc=workcenter.display_name, n=plan['n_banos'], kg=orden.product_qty, o=nombre_origen))
            if plan['aviso']:
                orden.message_post(body=plan['aviso'])
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
