# -*- coding: utf-8 -*-
"""57.190.0 (Dirección General 2026-10-23): «Regenerar artículos» en la pestaña Cotización de C1.

«Generar artículos» se oculta en cuanto el proyecto tiene artículos, y aunque se vuelva a correr no toca los
niveles ya ligados. Si se eligió mal el artículo base (hermano), cambiarlo en Análisis no corregía nada: los
códigos y las listas de materiales seguían saliendo del base anterior.

«Regenerar artículos»:

1. Solo antes de que el cliente apruebe y mientras el proyecto esté en Solicitud, Análisis o Cotización
   (después, un cambio de artículo es un cambio de diseño).
2. Suelta los artículos ligados (crudo, teñido, acabado en kg, acabado). Los que el proyecto **no** creó (los del
   artículo base o de catálogo) solo se desligan: no se tocan.
3. Los que **sí** creó el proyecto se borran con sus listas de materiales, siempre que nada más los use (órdenes,
   movimientos, pedidos, facturas, fichas técnicas, listas de otros artículos…). Si algo los usa, no se cambia
   nada y se dice qué los usa.
4. Vuelve a correr «Generar artículos» con el artículo base y las claves de hoy. Todo queda en el chatter.
"""
from odoo import models
from odoo.exceptions import UserError

from .sgi_dev_product_base import ROLE_FIELD, ROLE_LABEL, ROLE_ORDER

# Etapas en las que todavía se pueden rehacer los artículos.
REGEN_STAGES = ('solicitud', 'analisis', 'cotizacion')
# Modelos cuyas referencias al producto no cuentan como «uso»: los propios productos y el proyecto (sus campos
# de artículo se limpian antes de revisar).
REGEN_SKIP_MODELS = ('product.product', 'product.template', 'project.project')


class ProjectProjectDevRegen(models.Model):
    _inherit = 'project.project'

    def _sgi_dev_regen_check(self):
        """Razón por la que no se pueden regenerar los artículos, o ''."""
        self.ensure_one()
        if self.sgi_dev_analysis_result == 'linea':
            return "el resultado del análisis es «Producto de línea»: no se generan artículos."
        if self.sgi_dev_customer_approved_at:
            return ("el cliente ya aprobó para iniciar: un cambio de artículo ahora es un cambio de diseño, no se "
                    "regenera.")
        if self.sgi_dev_stage_key and self.sgi_dev_stage_key not in REGEN_STAGES:
            return "el proyecto ya pasó de Cotización (etapa «%s»)." % self.stage_id.name
        if not any(self[ROLE_FIELD[role]] for role in ROLE_ORDER):
            return "el proyecto todavía no tiene artículos: use «Generar artículos»."
        return ''

    def _sgi_dev_regen_usage(self, products):
        """[texto] de lo que usa a `products` fuera de sus propias listas de materiales. Solo cuentan las
        referencias que impedirían borrarlos o que quedarían vacías al borrarlos; las que se borran en
        cascada con el producto (costeo, proveedores…) no."""
        usage = []
        if not products:
            return usage
        ids_by_comodel = {'product.product': products.ids, 'product.template': products.product_tmpl_id.ids}
        for model_name in sorted(self.env.registry):
            if model_name in REGEN_SKIP_MODELS:
                continue
            Model = self.env[model_name]
            if Model._abstract or Model._transient or not Model._auto:
                continue
            for fname, field in Model._fields.items():
                ids = ids_by_comodel.get(getattr(field, 'comodel_name', None))
                if (not ids or field.type != 'many2one' or not field.store or field.related
                        or field.ondelete == 'cascade'):
                    continue
                try:
                    count = Model.sudo().with_context(active_test=False).search_count([(fname, 'in', ids)])
                except Exception:  # noqa: BLE001 - un modelo raro no detiene la revisión; borrar lo dirá
                    continue
                if count:
                    usage.append("%s (%s): %d" % (Model._description or model_name, field.string, count))
        return usage

    def action_sgi_dev_regenerate_products(self):
        for project in self.filtered('sgi_is_ft'):
            reason = project._sgi_dev_regen_check()
            if reason:
                raise UserError("No se pueden regenerar los artículos: %s" % reason)
            linked = [(role, project[ROLE_FIELD[role]]) for role in ROLE_ORDER if project[ROLE_FIELD[role]]]
            mine = self.env['product.product'].browse(
                [p.id for _role, p in linked if p.sgi_dev_project_id == project]).with_context(active_test=False)
            # 1. Soltar todos los artículos del proyecto.
            project.write({ROLE_FIELD[role]: False for role, _p in linked})
            # 2. Borrar las listas de materiales de los que creó el proyecto (se copiaron del base anterior).
            boms = self.env['mrp.bom'].sudo().with_context(active_test=False).search(
                ['|', ('product_tmpl_id', 'in', mine.product_tmpl_id.ids), ('product_id', 'in', mine.ids)])
            boms.unlink()
            # 3. Si algo más los usa, no se cambia nada (la transacción se deshace con el error).
            usage = project._sgi_dev_regen_usage(mine)
            if usage:
                raise UserError(
                    "No se pueden regenerar los artículos: estos artículos del proyecto ya se usan en otros "
                    "documentos y no se pueden borrar (%s).\n\n%s\n\nCorrija esos documentos o pida un cambio de "
                    "diseño." % (", ".join(mine.mapped('default_code')), "\n".join(usage)))
            codes = {p.id: p.default_code for p in mine}
            mine.sudo().product_tmpl_id.unlink()
            log = []
            for role, product in linked:
                if product.id in codes:
                    log.append("<li>%s: <b>%s</b> borrado con su lista de materiales.</li>" % (
                        ROLE_LABEL[role].capitalize(), codes[product.id]))
                else:
                    log.append("<li>%s: <b>%s</b> desligado (no lo creó el proyecto; no se toca).</li>" % (
                        ROLE_LABEL[role].capitalize(), product.default_code))
            project.message_post(body="Regenerar artículos con el artículo base %s:<ul>%s</ul>" % (
                project.sgi_dev_base_product_id.default_code or "(sin base: por claves)", "".join(log)))
        return self.action_sgi_dev_generate_products()
