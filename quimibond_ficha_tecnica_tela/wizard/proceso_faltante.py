# -*- coding: utf-8 -*-
"""Acabados en proceso faltantes (2.13.0, Dirección General 2026-10-19).

Toda la producción va en kg hasta la Clasificación final: cada tela acabada (J, en metros) necesita su artículo en kg, que por
la DAT P-D02-01 es el mismo código con la «I» al frente (posición 1 = unidad). Los artículos nuevos los da de alta C1
(proyecto de desarrollo, SGI); para los que ya existen, este reporte lista las telas acabadas en metros cuyo «I…» no
existe y lo crea para las seleccionadas: mismo nombre, kg, la categoría, el seguimiento y las rutas de la tela
acabada, sin venta ni compra (Ventas lo habilita si se vende en kg). El reporte no cambia nada por sí mismo.
"""
from odoo import api, fields, models

from ..models.acabado_ruta import codigo_proceso


class FichaTecnicaProcesoFaltante(models.TransientModel):
    _name = 'ficha.tecnica.proceso.faltante'
    _description = 'Acabado en proceso faltante'
    _order = 'codigo, id'

    acabado_id = fields.Many2one('product.product', string='Tela acabada (m)', required=True, ondelete='cascade')
    codigo = fields.Char(string='Código en kg')
    categ_id = fields.Many2one(related='acabado_id.categ_id', string='Categoría de la tela acabada')
    sale_ok = fields.Boolean(related='acabado_id.sale_ok', string='Se vende')

    @api.model
    def _ft_acabados(self):
        """Telas acabadas en metros de la compañía con código de la regla (J en la posición 9)."""
        meter = self.env.ref('uom.product_uom_meter')
        return self.env['product.product'].search([
            ('default_code', '=like', '________J%'), ('uom_id', '=', meter.id),
            ('company_id', 'in', [self.env.company.id, False])])

    @api.model
    def _ft_generar(self):
        """Rehace el reporte del usuario y lo abre."""
        self.search([('create_uid', '=', self.env.uid)]).unlink()
        acabados = self._ft_acabados()
        codigos = {p: codigo_proceso(p.default_code) for p in acabados}
        existentes = set(self.env['product.product'].with_context(active_test=False).search(
            [('default_code', 'in', [c for c in codigos.values() if c])]).mapped('default_code'))
        rows = self.create([{'acabado_id': p.id, 'codigo': c} for p, c in codigos.items()
                            if c and c not in existentes])
        return {'type': 'ir.actions.act_window', 'name': 'Acabados en proceso faltantes', 'res_model': self._name,
                'view_mode': 'list', 'domain': [('id', 'in', rows.ids)]}

    def action_crear(self):
        """Crea el acabado en proceso de los renglones seleccionados, lo liga a sus fichas de acabado y rehace el
        reporte."""
        Product = self.env['product.product']
        kg = self.env.ref('uom.product_uom_kgm')
        existentes = set(Product.with_context(active_test=False).search(
            [('default_code', 'in', self.mapped('codigo'))]).mapped('default_code'))
        vals_list = []
        for row in self.filtered(lambda r: r.codigo and r.codigo not in existentes):
            acabado = row.acabado_id
            vals_list.append({
                'name': acabado.name, 'default_code': row.codigo, 'type': 'consu', 'is_storable': True,
                'uom_id': kg.id, 'categ_id': acabado.categ_id.id,
                'sale_ok': False, 'purchase_ok': False, 'tracking': acabado.tracking,
                'company_id': acabado.company_id.id, 'route_ids': [(6, 0, acabado.route_ids.ids)],
            })
            existentes.add(row.codigo)
        if vals_list:
            Product.create(vals_list)
        self.env['ficha.tecnica.acabado'].search([
            ('product_acabado_id', 'in', self.acabado_id.ids), ('product_proceso_id', '=', False),
        ])._ft_ligar_proceso_sugerido()
        return self._ft_generar()
