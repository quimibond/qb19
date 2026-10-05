# -*- coding: utf-8 -*-
"""57.101.0: impresos con formato controlado sobre datos que ya calcula el
SGI. Diagramas (C3) con los datos de ``sgi.diagram.data``; programa de
auditorías contra lo realizado (C5); mapa de calor de riesgos por
instrumento (C7)."""
from odoo import api, fields, models
from odoo.exceptions import UserError

# Diagramas con impresión controlada (pestañas «bands», «columns» y
# «matrix»). Carriles y PDCA no: sus flechas solo existen en el navegador.
PRINT_KINDS = ('process_map', 'interaction_matrix', 'sipoc', 'roles_map', 'context_map')
PRINT_LAYOUTS = ('bands', 'columns', 'matrix')
DG_COLORS = {'success': '#198754', 'warning': '#ffc107', 'danger': '#dc3545',
             'info': '#0dcaf0', 'muted': '#adb5bd'}
DG_LEVEL_BG = {3: '#a3cfbb', 2: '#ffe69c', 1: '#cff4fc'}
# Instrucciones de pantalla que no van en el papel («Pase el mouse…», «clic…»).
SCREEN_HINTS = ('mouse', 'clic')


class SgiDiagramPrint(models.AbstractModel):
    """57.101.0 (C3): el diagrama en PDF con el pie del formato controlado
    (botón de la barra del diagrama)."""
    _inherit = 'sgi.diagram'

    @api.model
    def _sgi_printable(self, kind, params=None):
        """¿El diagrama tiene impresión en formato controlado?"""
        return kind in PRINT_KINDS

    @api.model
    def data(self, kind, res_id=None, params=None):
        result = super().data(kind, res_id=res_id, params=params)
        result['print_report'] = bool(self._sgi_printable(kind, params)
                                      and result.get('layout') in PRINT_LAYOUTS)
        return result

    @api.model
    def _sgi_print_action(self, kind, res_id, params):
        return self.env.ref('quimibond_sgi.action_report_sgi_diagram').report_action(
            None, data={'kind': kind, 'res_id': res_id, 'params': params}, config=False)

    @api.model
    def action_print_controlled(self, kind, res_id=None, params=None):
        """Acción del botón «Imprimir en formato controlado» de la barra del
        diagrama: el mismo diagrama en PDF con el pie del SGI."""
        params = dict(params or {})
        if not self._sgi_printable(kind, params):
            raise UserError("Este diagrama no tiene impresión en formato controlado; "
                            "use el botón de la impresora.")
        res_id = int(res_id) if res_id else False
        return self._sgi_print_action(kind, res_id, params)

    @api.model
    def _sgi_print_subtitle(self, subtitle):
        """El subtítulo sin las instrucciones de pantalla (mouse, clic)."""
        parts = [p.strip() for chunk in (subtitle or '').split('·') for p in chunk.split(';')]
        return " · ".join(p for p in parts if p and not any(h in p.lower() for h in SCREEN_HINTS))

    @api.model
    def _sgi_print_edges(self, dg):
        """[(de, qué, a)] de las flechas, con el nombre de cada caja."""
        names = {}
        for lane in dg.get('lanes') or []:
            for item in lane.get('items') or []:
                names[item['key']] = " ".join(x for x in (item.get('code'), item.get('name')) if x)
        matrix = dg.get('matrix') or {}
        for node in (matrix.get('rows') or []) + (matrix.get('cols') or []):
            names.setdefault(node['key'], node.get('label') or '')
        seen, rows = set(), []
        for edge in dg.get('edges') or []:
            row = (names.get(edge['from'], ''), edge.get('label') or '', names.get(edge['to'], ''))
            if row[0] and row[2] and row not in seen:
                seen.add(row)
                rows.append(row)
        return rows


class ReportSgiDiagram(models.AbstractModel):
    """57.101.0 (C3): valores del PDF de un diagrama. Se imprime con ``data``
    (kind, res_id, params), sin registros; los datos son los de pantalla
    (``sgi.diagram.data``, con los permisos del usuario)."""
    _name = 'report.quimibond_sgi.report_sgi_diagram_document'
    _description = "Diagrama del SGI en formato controlado (PDF)"

    @api.model
    def _get_report_values(self, docids, data=None):
        data = data or {}
        kind = data.get('kind')
        if kind not in PRINT_KINDS:
            raise UserError("Este diagrama no tiene impresión en formato controlado.")
        Diagram = self.env['sgi.diagram']
        dg = Diagram.data(kind, data.get('res_id') or None, data.get('params') or {})
        process = self.env['sgi.process'].browse(dg.get('process_id') or []).exists()
        now = fields.Datetime.context_timestamp(self, fields.Datetime.now())
        return {
            'doc_ids': [], 'doc_model': 'sgi.diagram', 'docs': process,
            'dg': dg, 'dg_subtitle': Diagram._sgi_print_subtitle(dg.get('subtitle')),
            'dg_edges': Diagram._sgi_print_edges(dg), 'dg_colors': DG_COLORS,
            'dg_level_bg': DG_LEVEL_BG, 'dg_anchor': process or self.env.company,
            'dg_printed': now.strftime('%d/%m/%Y %H:%M'),
        }
