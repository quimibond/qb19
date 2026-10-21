# -*- coding: utf-8 -*-
"""57.176.0 (Jose, 2026-10-21): la medición se valida sola cuando no hay nada
que una persona tenga que mirar.

Antes cada medición capturada pedía «Validar» a su dueño (I-006), una por
periodo: con los indicadores semanales eran cientos de renglones en Mis
pendientes para confirmar que el número que calculó Odoo es el que calculó
Odoo. Lo que sí necesita a una persona ya tiene su lugar:

- el cálculo se revisa una vez, al pasar el indicador de prueba a oficial;
- un rojo pide causa y plan (y su NC automática sale al validarlo);
- una muestra chica o una captura a mano de otra persona la confirma el dueño.

Regla: una medición «capturado» de un indicador **oficial** que no sea de
salud del SGI, sin rojo y sin muestra chica, pasa sola a «validado». Si el
indicador es manual, solo cuando la capturó su responsable (su captura es la
confirmación). Lo demás se queda para el dueño, como antes."""
from odoo import api, models

from .sgi_health_const import HEALTH_MODES

NO_AUTO_CTX = 'sgi_no_auto_validate'


class SgiIndicatorMeasureAutoValidate(models.Model):
    _inherit = 'sgi.indicator.measure'

    def _sgi_auto_validates(self, capturer):
        """True si esta medición capturada no necesita que nadie la mire."""
        self.ensure_one()
        indicator = self.indicator_id
        if self.state != 'capturado' or indicator.status != 'oficial' \
                or indicator.calc_mode in HEALTH_MODES:
            return False
        if self.small_sample or self.semaphore == 'rojo':
            return False
        if indicator.calc_mode == 'manual':
            return bool(capturer) and capturer == indicator.responsible_id
        return True

    def _sgi_auto_validate(self, capturer=None):
        """Valida lo que no necesita a una persona; devuelve lo validado."""
        if self.env.context.get(NO_AUTO_CTX):
            return self.browse()
        todo = self.filtered(lambda m: m._sgi_auto_validates(capturer))
        if todo:
            todo.sudo().with_context(**{NO_AUTO_CTX: True}).write({'state': 'validado'})
        return todo

    def _sgi_capturer(self):
        """Quién captura en este momento: nadie si escribe el sistema."""
        return self.env['res.users'] if self.env.su else self.env.user

    @api.model_create_multi
    def create(self, vals_list):
        records = super().create(vals_list)
        records._sgi_auto_validate(records._sgi_capturer())
        return records

    def write(self, vals):
        res = super().write(vals)
        if vals.get('state') == 'capturado' or (
                'value' in vals and vals.get('state') in (None, 'capturado')):
            self._sgi_auto_validate(self._sgi_capturer())
        return res


class SgiIndicatorAutoValidate(models.Model):
    _inherit = 'sgi.indicator'

    def write(self, vals):
        newly = self.browse()
        if vals.get('status') == 'oficial':
            newly = self.filtered(lambda i: i.status != 'oficial')
        res = super().write(vals)
        if newly:
            # Al pasar a oficial, lo capturado en prueba sigue la misma regla
            # (las manuales: no se sabe quién las capturó; las valida el dueño).
            measures = self.env['sgi.indicator.measure'].sudo().search(
                [('indicator_id', 'in', newly.ids), ('state', '=', 'capturado')])
            measures._sgi_auto_validate()
        return res
