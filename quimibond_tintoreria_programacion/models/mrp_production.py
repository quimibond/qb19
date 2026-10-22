# -*- coding: utf-8 -*-
import math

from odoo import _, api, fields, models
from odoo.exceptions import UserError

# Posición 9 del código (DAT P-D02-01) = Operación; "K" = Acabado en Kilos.
INDICE_OPERACION = 8
CLAVE_ACABADO_KG = 'K'
CLAVE_ACABADO_METROS = 'J'


def banda_rendimiento(rendimiento):
    """Banda A/B/C según las mismas bandas fijas de tintoreria.capacidad.rendimiento."""
    if 3 <= rendimiento <= 6:
        return 'A'
    if 7 <= rendimiento <= 10:
        return 'B'
    if 11 <= rendimiento <= 15:
        return 'C'
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
        help="Producto con 'K' en la posición 9 del código y una operación de Tintorería en su BoM.")
    tintoreria_pendiente = fields.Boolean(
        'Pendiente de programar Tintorería', compute='_compute_tintoreria_pendiente',
        store=True, index=True)

    # ------------------------------------------------------------------
    # Campos calculados
    # ------------------------------------------------------------------
    def _es_producto_acabado_kg(self):
        """True si el código del producto tiene 'K' en la posición 9 (acabado en kilos)."""
        self.ensure_one()
        codigo = (self.product_id.default_code or '').strip()
        return len(codigo) > INDICE_OPERACION and codigo[INDICE_OPERACION].upper() == CLAVE_ACABADO_KG

    @api.depends('product_id.default_code', 'bom_id', 'bom_id.operation_ids.workcenter_id',
                 'bom_id.operation_ids.name')
    def _compute_requiere_programacion_tintoreria(self):
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

    def _producto_acabado_metros(self):
        """Producto B (metros) que consume esta MO de A: primero por el enlace MTO; si no existe,
        derivándolo del código (posición 9: K -> J)."""
        self.ensure_one()
        destinos = self.move_finished_ids.filtered(
            lambda m: m.product_id == self.product_id
        ).move_dest_ids.raw_material_production_id.product_id
        if destinos:
            return destinos[:1]
        codigo = (self.product_id.default_code or '').strip()
        if len(codigo) > INDICE_OPERACION:
            candidato = codigo[:INDICE_OPERACION] + CLAVE_ACABADO_METROS + codigo[INDICE_OPERACION + 1:]
            return self.env['product.product'].search([('default_code', '=', candidato)], limit=1)
        return self.env['product.product']

    def _rendimiento_acabado(self):
        """Rendimiento teórico (m/kg) de la ficha técnica de acabado del producto B."""
        self.ensure_one()
        producto = self._producto_acabado_metros()
        ficha = self.env['ficha.tecnica.acabado'].search(
            [('product_acabado_id', '=', producto.id)], limit=1) if producto else False
        if not ficha:
            raise UserError(_(
                "No se encontró la ficha técnica de acabado del producto en metros de la orden %s. "
                "Revisa que exista el producto con 'J' en la posición 9 y su ficha técnica.", self.name))
        if ficha.rendimiento_tela_acabada <= 0:
            raise UserError(_(
                "La ficha técnica de acabado de %s no tiene rendimiento (m/kg).", producto.display_name))
        return ficha.rendimiento_tela_acabada

    def _calcular_plan_tintoreria(self, workcenter):
        """Capacidad por baño, número de baños y kg por baño (reparto simétrico) en el HTJ dado."""
        self.ensure_one()
        capacidad_cfg = self.env['tintoreria.capacidad.rendimiento'].search(
            [('workcenter_id', '=', workcenter.id)], limit=1)
        if not capacidad_cfg:
            raise UserError(_(
                "El centro de trabajo %s no está en la Tabla de Rendimientos y RB de Tintorería.",
                workcenter.display_name))
        rendimiento = self._rendimiento_acabado()
        capacidad = capacidad_cfg.capacidad_para_rendimiento(rendimiento)
        if capacidad_cfg.capacidad_kg > 0:
            capacidad = min(capacidad, capacidad_cfg.capacidad_kg) if capacidad > 0 else 0.0
        if capacidad <= 0:
            raise UserError(_(
                "%(wc)s no puede procesar este artículo: su rendimiento de %(r)s m/kg no tiene capacidad "
                "definida (N/A o fuera de las bandas 3-6, 7-10 y 11-15).",
                wc=workcenter.display_name, r=rendimiento))
        kg_total = self.product_qty
        n_banos = max(1, math.ceil(round(kg_total / capacidad, 6)))
        uom = self.product_uom_id
        kg_bano = uom.round(kg_total / n_banos)
        montos = [kg_bano] * (n_banos - 1)
        montos.append(uom.round(kg_total - sum(montos)))  # el redondeo se absorbe en el último baño
        return {
            'rendimiento': rendimiento,
            'banda': banda_rendimiento(rendimiento),
            'capacidad_kg': capacidad,
            'n_banos': n_banos,
            'kg_bano': kg_bano,
            'montos': montos,
        }

    # ------------------------------------------------------------------
    # Programación
    # ------------------------------------------------------------------
    def _check_programable(self):
        self.ensure_one()
        if not self.requiere_programacion_tintoreria:
            raise UserError(_(
                "La orden %s no requiere programación de Tintorería (producto sin 'K' en la posición 9 "
                "o BoM sin operación de Tintorería).", self.name))
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
