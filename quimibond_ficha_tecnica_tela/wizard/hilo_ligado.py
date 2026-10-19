# -*- coding: utf-8 -*-
"""Reporte de ligado de hilos (2.9.0, Dirección General 2026-10-19).

Antes de 2.9.0 los hilos de la ficha de tejido eran texto (tipo, título,
torsión, lote). El reporte dice, por cada renglón, qué producto le toca según
la lista de materiales de su crudo:

- **Un solo candidato** en su lista de materiales (o varios y uno con el mismo
  %): se puede ligar desde aquí («Ligar los de candidato único»).
- **Varios**, **solo en otra lista** del crudo, **solo fuera** de sus listas o
  **ninguno**: los decide una persona en la ficha.

El reporte no cambia nada por sí mismo; ligar solo escribe el producto (y el
lote, si existe en inventario) y el texto anterior se conserva.
"""
from odoo import api, fields, models

from ..models.ficha_tecnica_tejido import LIGADO_ESTADOS


class FichaTecnicaHiloLigado(models.TransientModel):
    _name = 'ficha.tecnica.hilo.ligado'
    _description = 'Reporte de ligado de hilos'
    _order = 'articulo, workcenter_id, numero, id'

    hilo_id = fields.Many2one('ficha.tecnica.tejido.hilo', string='Renglón de hilo', required=True,
                              ondelete='cascade')
    ficha_id = fields.Many2one(related='hilo_id.ficha_id', string='Ficha de tejido')
    articulo = fields.Char(string='Artículo')
    workcenter_id = fields.Many2one('mrp.workcenter', string='Máquina')
    numero = fields.Integer(string='No.')
    texto = fields.Char(string='Hilo (texto)')
    porcentaje = fields.Float(string='% en la ficha', digits=(16, 2))
    estado = fields.Selection(LIGADO_ESTADOS, string='Estado')
    candidato_id = fields.Many2one('product.product', string='Candidato')
    candidatos = fields.Char(string='Candidatos')
    bom_pct = fields.Float(string='% del candidato en la lista', digits=(16, 2))
    lote = fields.Char(string='Lote (texto)')
    lote_id = fields.Many2one('stock.lot', string='Lote encontrado')

    @api.model
    def _ft_generar(self, hilos):
        """Rehace el reporte del usuario para esos renglones y lo abre."""
        self.search([('create_uid', '=', self.env.uid)]).unlink()
        Tejido = self.env['ficha.tecnica.tejido']
        vals_list = []
        for hilo in hilos:
            estado, candidatos = hilo._ft_candidatos()
            unico = candidatos if len(candidatos) == 1 else self.env['product.product']
            bom = hilo.ficha_id._ft_bom()
            partes = Tejido._ft_bom_partes(bom) if bom else {}
            lote = hilo._ft_lote_candidato() if hilo.product_id else self.env['stock.lot']
            vals_list.append({
                'hilo_id': hilo.id, 'articulo': hilo.ficha_id.articulo, 'workcenter_id': hilo.workcenter_id.id,
                'numero': hilo.numero, 'texto': hilo._ft_texto(), 'porcentaje': hilo.porcentaje,
                'estado': estado, 'candidato_id': unico.id,
                'candidatos': ', '.join(p.default_code or p.name for p in candidatos[:5]),
                'bom_pct': partes.get(unico, 0.0) if unico else 0.0,
                'lote': hilo.lote, 'lote_id': lote.id,
            })
        rows = self.create(vals_list)
        return {'type': 'ir.actions.act_window', 'name': 'Ligado de hilos', 'res_model': self._name,
                'view_mode': 'list', 'domain': [('id', 'in', rows.ids)],
                'context': {'search_default_group_estado': 1}}

    def action_ligar_unicos(self):
        """Liga los renglones seleccionados que tienen un solo candidato y rehace el reporte."""
        hilos = self.filtered(lambda r: r.estado == 'unico').hilo_id
        hilos._ft_ligar_unicos()
        self.filtered(lambda r: r.estado == 'ligado' and r.lote_id).hilo_id._ft_ligar_unicos()
        todos = self.search([('create_uid', '=', self.env.uid)]).hilo_id
        return self._ft_generar(todos)
