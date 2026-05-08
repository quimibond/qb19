from odoo import models, api

class MoReportSummary(models.AbstractModel):
    _name = 'report.reporte_orden_fabricacion.mo_report_summary'
    _description = 'Reporte Integral de Producción y Calidad'

    @api.model
    def _get_report_values(self, docids, data=None):
        docs = self.env['mrp.production'].browse(docids)
        calcs = {}
        
        for doc in docs:
            # Tiempos en horas
            exp = sum(doc.workorder_ids.mapped('duration_expected')) / 60.0
            real = sum(doc.workorder_ids.mapped('duration')) / 60.0
            
            # Costos desde valoración
            costo_total = 0.0
            try:
                finished_moves = doc.move_finished_ids.filtered(lambda m: m.state == 'done')
                costo_total = sum(finished_moves.mapped('stock_valuation_layer_ids.value'))
            except:
                costo_total = 0.0

            calcs[doc.id] = {
                'total_cost': abs(costo_total),
                'unit_cost': abs(costo_total / doc.qty_producing) if doc.qty_producing > 0 else 0.0,
                'time_exp': exp,
                'time_real': real,
                'time_diff': real - exp,
                'subproductos': doc.move_byproduct_ids.filtered(lambda m: m.state == 'done'),
                'scraps': self.env['stock.scrap'].search([('production_id', '=', doc.id)])
            }

        return {
            'doc_ids': docids,
            'docs': docs,
            'calcs': calcs,
        }