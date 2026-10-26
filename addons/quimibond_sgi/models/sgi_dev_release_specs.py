# -*- coding: utf-8 -*-
"""57.192.0 (Dirección General 2026-10-26): las características del desarrollo pasan a la ficha de acabado.

El proyecto y la ficha técnica comparten catálogo y límites (``ficha.tecnica.caracteristica.mixin``) para que al
liberar el artículo los renglones pasen a su ficha sin recaptura (decisión de Jose, 2026-10-06), pero nada los
copiaba: la especificación aprobada se quedaba en el proyecto y la ficha de acabado se volvía a capturar.

- Al pasar el proyecto a **Liberado**, y con el botón «Pasar características a la ficha» (pestaña Muestra), cada
  renglón de la tabla con especificación del cliente se copia a la ficha de acabado del artículo (``spec_line_ids``):
  especificación, control interno, método, «va a la especificación del cliente» y «va al certificado».
- Solo se agregan los que la ficha no tiene (misma característica, dirección y posición): nunca se pisa un renglón
  de la ficha.
- Las características del producto terminado van a la ficha de **acabado**; la de tejido tiene sus propios datos.
- Sin ficha de acabado no se copia nada y el chatter lo dice (la liberación no se detiene).
"""
from odoo import models
from odoo.exceptions import UserError


class ProjectProjectDevReleaseSpecs(models.Model):
    _inherit = 'project.project'

    @staticmethod
    def _sgi_dev_spec_key(line):
        return (line.caracteristica_id.id or (line.name or '').strip().lower(), line.direction or 'na',
                line.position or 'na')

    @staticmethod
    def _sgi_dev_line_has_spec(line):
        if line.kind == 'text':
            return bool((line.spec_text or '').strip())
        if line.kind == 'bool':
            return bool(line.spec_bool)
        return line._has_spec()

    def _sgi_dev_release_sheet(self):
        """La ficha de acabado del artículo del proyecto (por el acabado o por el acabado en kg)."""
        self.ensure_one()
        for product in (self.sgi_dev_product_id, self.sgi_dev_product_proceso_id):
            sheet = self._sgi_dev_find_sheet('acabado', product) if product else None
            if sheet:
                return sheet
        return self.env['ficha.tecnica.acabado']

    def _sgi_dev_specs_to_sheet(self):
        """Copia a la ficha de acabado los renglones con especificación que le falten. Devuelve
        {proyecto: (ficha, copiados)}; la ficha vacía si no hay."""
        Spec = self.env['ficha.tecnica.spec'].sudo()
        result = {}
        for project in self.filtered('sgi_is_ft'):
            sheet = project._sgi_dev_release_sheet()
            if not sheet:
                result[project] = (sheet, 0)
                continue
            have = {self._sgi_dev_spec_key(line) for line in sheet.spec_line_ids}
            vals = []
            for line in project.sgi_dev_line_ids.filtered(self._sgi_dev_line_has_spec):
                key = self._sgi_dev_spec_key(line)
                if key in have:
                    continue
                have.add(key)
                vals.append(dict(line._limit_vals(), acabado_id=sheet.id))
            if vals:
                Spec.create(vals)
            result[project] = (sheet, len(vals))
        return result

    def _sgi_dev_post_specs_result(self, result):
        for project, (sheet, count) in result.items():
            if not sheet:
                body = ("Las características del desarrollo no pasaron a ninguna ficha: %s no tiene ficha de acabado. "
                        "Créela en la pestaña Muestra → «Fichas de proceso» y use «Pasar características a la ficha»."
                        % (project.sgi_dev_product_id.display_name or "el artículo"))
            elif count:
                body = "%d característica(s) del desarrollo pasaron a la ficha de acabado %s." % (count, sheet.display_name)
            else:
                body = "La ficha de acabado %s ya tenía todas las características del desarrollo." % sheet.display_name
            project.message_post(body=body)

    def action_sgi_dev_specs_to_sheet(self):
        result = self._sgi_dev_specs_to_sheet()
        missing = [p for p, (sheet, _count) in result.items() if not sheet]
        if missing and len(self) == 1:
            raise UserError("%s no tiene ficha de acabado: créela en la pestaña Muestra → «Fichas de proceso»."
                            % (self.sgi_dev_product_id.display_name or "El artículo del proyecto"))
        self._sgi_dev_post_specs_result(result)
        return True

    def write(self, vals):
        res = super().write(vals)
        if 'stage_id' in vals and not self.env.context.get('sgi_dev_migration'):
            released = self.filtered(lambda p: p.sgi_is_ft and p.sgi_dev_stage_key == 'liberado')
            if released:
                released._sgi_dev_post_specs_result(released._sgi_dev_specs_to_sheet())
        return res
