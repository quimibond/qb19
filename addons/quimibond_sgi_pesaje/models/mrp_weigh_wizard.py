# -*- coding: utf-8 -*-
from odoo import models

TOLERANCE_KG = 3.0


class MrpWeighRollWizard(models.TransientModel):
    _inherit = 'mrp.weigh.roll.wizard'

    def confirm_weighing(self):
        """Gancho hacia el SGI: si el operador confirma un rollo PESE a estar
        fuera de tolerancia, se levanta una alerta de calidad ligada a la MO.

        No se altera el flujo original: cuando el peso está fuera de rango y aún
        no se ha advertido, el módulo base activa `confirm_threshold` y recarga
        el wizard sin registrar (solo advierte). La alerta se crea únicamente en
        la confirmación forzada (confirm_threshold ya activo y peso fuera de
        rango), es decir, una sola vez por rollo."""
        forcing = self.confirm_threshold
        out_of_tolerance = forcing and self._sgi_weight_out_of_tolerance()
        result = super().confirm_weighing()
        if out_of_tolerance:
            self._sgi_create_weight_alert()
        return result

    def _sgi_weight_out_of_tolerance(self):
        self.ensure_one()
        if not self.production_id:
            return False
        config = self.env['mrp.rollo.estandar'].search(
            [('product_id', '=', self.production_id.product_id.id)], limit=1)
        if not config:
            return False
        return (self.net_weight < config.rollo_teorico - TOLERANCE_KG
                or self.net_weight > config.rollo_teorico + TOLERANCE_KG)

    def _sgi_create_weight_alert(self):
        """Crea (idempotente por rollo) la alerta de calidad del rollo fuera de
        tolerancia en el equipo «Revisado de Tela» (o NC Internas de respaldo)."""
        self.ensure_one()
        production = self.production_id
        lot_name = "%s-%04d" % (production.name.split('/')[-1], production.roll_count)
        title = "Rollo fuera de tolerancia de peso (%s)" % lot_name
        Alert = self.env['quality.alert'].sudo()
        existing = Alert.search([
            ('production_id', '=', production.id),
            ('title', '=', title),
        ], limit=1)
        if existing:
            return existing
        team = self.env['quality.alert.team'].sudo().search(
            [('name', '=', 'Revisado de Tela')], limit=1)
        if not team:
            team = self.env.ref('quimibond_sgi.sgi_quality_team_internal',
                                raise_if_not_found=False)
        vals = {
            'title': title,
            'product_id': production.product_id.id,
            'production_id': production.id,
            'sgi_origin_type': 'proceso',
            'sgi_deviation': (
                "El rollo %s se registró con peso neto %.3f kg, fuera de la "
                "tolerancia ±%.0f kg del Tamaño de Rollo Estándar." % (
                    lot_name, self.net_weight, TOLERANCE_KG)),
        }
        if self.workorder_id:
            vals['workorder_id'] = self.workorder_id.id
        if team:
            vals['team_id'] = team.id
        return Alert.create(vals)
