# -*- coding: utf-8 -*-
"""57.160.0 (Dirección General 2026-10-19): el acabado en kg en C1.

Toda la producción va **en kg hasta la Clasificación final** (57.172.1): la orden de acabado (devanado → tintorería
→ abridora → rama → Inspección) se cierra con los rollos pesados e inspeccionados y produce el **acabado en kg**; la
Clasificación final (empaque) lo pasa a metros y entrega el **acabado (J)** que se vende en metros. El teñido (I en la operación) desaparece cuando la ruta del
acabado en kg lleva todas sus operaciones.

El código del acabado en kg es el del acabado con la **«I» al frente**: la DAT P-D02-01 rev. 05 (regla de
codificación) define la posición 1 como unidad, I = kilogramos. El mismo artículo se vende en kg; sin inspeccionar o
ya inspeccionado se distingue por ubicación, no por código.

- **Generar artículos** (por claves o desde el artículo base) crea o liga también el acabado en kg: kg, la categoría
  y el seguimiento del acabado, sin venta. Sus listas de materiales (la del acabado en kg y la de la Clasificación final, que lo
  consume y produce el J) se capturan a mano.
- El artículo sigue el estado del desarrollo como los demás; la venta en kg la habilita Ventas.
- **Validar corrida** (C1.10): la orden del acabado en kg es de la etapa de acabado y se valida contra la
  ficha de acabado que lo tiene ligado (``product_proceso_id`` en ``quimibond_ficha_tecnica_tela`` 2.13.0).
- La ruta del proyecto muestra sus operaciones entre las del teñido y las del acabado.
"""
from markupsafe import Markup

from odoo import api, fields, models

from .sgi_dev_product_base import UNIT_KG, parse_code


def proceso_code(acabado_code):
    """El código en kg de un acabado (WJ053Q22JNT160 → IWJ053Q22JNT160); '' si el código no sigue la regla."""
    parts = parse_code(acabado_code)
    if not parts or parts['role'] != 'acabado':
        return ''
    return UNIT_KG + acabado_code.strip().upper()


class ProjectProjectDevProceso(models.Model):
    _inherit = 'project.project'

    sgi_dev_code_proceso_code = fields.Char(string="Código acabado en kg", compute='_compute_sgi_dev_code_proceso',
                                            help="Código propuesto del acabado en kg: el del acabado con la «I» al "
                                                 "frente (DAT P-D02-01, posición 1 = kilogramos).")
    sgi_dev_product_proceso_id = fields.Many2one('product.product', string="Artículo acabado en kg", copy=False,
                                                 help="Acabado en kg del desarrollo: sale de rama pesado y entra a "
                                                      "la Clasificación final, que lo pasa a metros; también es el que se vende "
                                                      "en kg.")

    @api.depends('sgi_dev_code_acabado_code', 'sgi_dev_product_id.default_code')
    def _compute_sgi_dev_code_proceso(self):
        for project in self:
            project.sgi_dev_code_proceso_code = proceso_code(
                project.sgi_dev_product_id.default_code or project.sgi_dev_code_acabado_code)

    def _sgi_dev_ensure_proceso(self):
        """Crea o liga el acabado en kg de los proyectos que ya tienen acabado y no lo tienen."""
        Product = self.env['product.product']
        kg = self.env.ref('uom.product_uom_kgm')
        for project in self.filtered(lambda p: p.sgi_is_ft and p.sgi_dev_product_id
                                     and not p.sgi_dev_product_proceso_id):
            acabado = project.sgi_dev_product_id
            code = proceso_code(acabado.default_code)
            if not code:
                project.message_post(body="El código del acabado %s no sigue la regla: capture a mano el acabado en "
                                          "kg." % (acabado.default_code or acabado.display_name))
                continue
            product = Product.with_context(active_test=False).search([('default_code', '=', code)], limit=1)
            if product:
                body = Markup("Acabado en kg: se liga el existente <b>%s</b>.") % code
            else:
                product = Product.create({
                    'name': acabado.name, 'default_code': code, 'type': 'consu', 'is_storable': True,
                    'uom_id': kg.id, 'categ_id': acabado.categ_id.id,
                    'sale_ok': False, 'purchase_ok': False, 'tracking': acabado.tracking,
                    'company_id': project.company_id.id or False,
                    'sgi_dev_state': 'desarrollo', 'sgi_dev_project_id': project.id, 'sgi_dev_role': 'proceso',
                })
                body = Markup("Acabado en kg <b>%s</b> creado. Capture su lista de materiales (abridora, tintorería, "
                              "rama, Inspección) y la de la Clasificación final del acabado (consume %s y produce %s en "
                              "metros).") % (code, code, acabado.default_code)
            project.write({'sgi_dev_product_proceso_id': product.id})
            project.message_post(body=body)
        return True

    def action_sgi_dev_generate_products(self):
        res = super().action_sgi_dev_generate_products()
        self._sgi_dev_ensure_proceso()
        return res

    def _sgi_dev_products(self):
        templates = super()._sgi_dev_products()
        proceso = self.sgi_dev_product_proceso_id
        if proceso and proceso.sgi_dev_project_id == self:
            templates |= proceso.product_tmpl_id
        return templates

    def _sgi_dev_bom_templates(self):
        return super()._sgi_dev_bom_templates() | self.sgi_dev_product_proceso_id.product_tmpl_id

    def _sgi_dev_route_operations(self):
        """Las operaciones del acabado en kg van antes que las del acabado (Clasificación final)."""
        ops = super()._sgi_dev_route_operations()
        proceso = self.sgi_dev_product_proceso_id
        if not proceso or proceso in (self.sgi_dev_product_crudo_id | self.sgi_dev_product_tenido_id
                                      | self.sgi_dev_product_id):
            return ops
        bom = self.env['mrp.bom'].sudo()._bom_find(proceso, company_id=self.company_id.id).get(proceso)
        if not bom:
            return ops
        mine = [(proceso, op) for op in bom.operation_ids.sorted(lambda o: (o.sequence, o.id))]
        at = next((i for i, (product, _op) in enumerate(ops) if product == self.sgi_dev_product_id), len(ops))
        return ops[:at] + mine + ops[at:]

    def _sgi_dev_find_sheet(self, stage, product):
        """La ficha de acabado también se encuentra por su acabado en kg."""
        sheet = super()._sgi_dev_find_sheet(stage, product)
        if sheet or stage != 'acabado' or not product:
            return sheet
        sheets = self.env['ficha.tecnica.acabado'].search([('product_proceso_id', '=', product.id)])
        return sheets.sorted(lambda s: (s.project_id == self, s.id), reverse=True)[:1]


class MrpProductionDevProceso(models.Model):
    _inherit = 'mrp.production'

    def _sgi_dev_run_sheet(self):
        """La orden del acabado en kg es de la etapa de acabado."""
        self.ensure_one()
        project = self.sgi_dev_project_id
        if project and self.product_id and self.product_id == project.sgi_dev_product_proceso_id:
            return 'acabado', project._sgi_dev_find_sheet('acabado', self.product_id)
        return super()._sgi_dev_run_sheet()
