# -*- coding: utf-8 -*-
"""Diagramas con la vista nativa «hierarchy» de Odoo 19 (la misma del
organigrama de Empleados, módulo web_hierarchy).

La vista solo necesita un Many2one al mismo modelo (padre) y un One2many
(hijos). Los procesos ya los tienen (parent_id / child_ids: macroproceso →
subprocesos). Las actividades se encadenan por eslabones (sgi.activity.link),
que es un grafo; para dibujarlo como árbol cada actividad toma como «padre de
flujo» al paso anterior de su mismo proceso que va antes en la secuencia. Así
nunca hay ciclos y el diagrama arranca en los pasos que no reciben nada."""
from odoo import api, fields, models

from .sgi_diagram import PROCESS_KINDS


class SgiProcessActivityHierarchy(models.Model):
    _inherit = 'sgi.process.activity'

    flow_parent_id = fields.Many2one(
        'sgi.process.activity', string="Paso anterior", compute='_compute_flow_parent_id',
        store=True, index=True, ondelete='set null',
        help="El paso del mismo proceso que entrega a esta actividad (el primero "
             "en la secuencia). Es lo que dibuja el diagrama; se calcula de los "
             "eslabones «Recibe de».")
    flow_child_ids = fields.One2many(
        'sgi.process.activity', 'flow_parent_id', string="Pasos siguientes (diagrama)")
    flow_child_count = fields.Integer(compute='_compute_flow_child_count', string="Siguientes")
    flow_executor = fields.Char(compute='_compute_flow_executor', string="Quién ejecuta")

    @api.depends('in_link_ids.from_activity_id', 'in_link_ids.from_activity_id.sequence',
                 'in_link_ids.from_activity_id.active', 'process_id', 'sequence')
    def _compute_flow_parent_id(self):
        for activity in self:
            own = (activity.sequence or 0, activity.id or 0)
            candidates = activity.in_link_ids.from_activity_id.filtered(
                lambda a: a.active and a != activity and a.process_id == activity.process_id
                and (a.sequence or 0, a.id or 0) < own)
            activity.flow_parent_id = candidates.sorted(
                lambda a: (a.sequence or 0, a.id or 0))[:1]

    @api.depends('flow_child_ids')
    def _compute_flow_child_count(self):
        for activity in self:
            activity.flow_child_count = len(activity.flow_child_ids)

    @api.depends('responsible_job_ids.name')
    def _compute_flow_executor(self):
        for activity in self:
            activity.flow_executor = ", ".join(activity.responsible_job_ids.mapped('name')) or ''


class SgiProcessHierarchy(models.Model):
    _inherit = 'sgi.process'

    child_count = fields.Integer(compute='_compute_child_count', string="Subprocesos")

    @api.depends('child_ids')
    def _compute_child_count(self):
        for process in self:
            process.child_count = len(process.child_ids)

    def _sgi_diagram_action(self, kind, name):
        self.ensure_one()
        return {
            'type': 'ir.actions.client',
            'tag': 'sgi_diagram',
            'name': name,
            'context': {'sgi_diagram_kind': kind, 'sgi_diagram_res_id': self.id,
                        'sgi_diagram_kinds': ','.join(k for k, _l in PROCESS_KINDS),
                        'sgi_diagram_selected': 'sgi.process,%d' % self.id},
        }

    def action_sgi_view_diagram(self):
        """«Ver en diagrama»: el flujo de actividades del proceso por etapa,
        con los eslabones como flechas (componente sgi_diagram)."""
        return self._sgi_diagram_action('process_flow', "Flujo del proceso")

    def action_sgi_view_process_map(self):
        """«Ver en el mapa»: el mapa con conexiones, con este proceso
        seleccionado (sus flechas de entrada y salida resaltadas)."""
        self.ensure_one()
        return {
            'type': 'ir.actions.client',
            'tag': 'sgi_diagram',
            'name': "Mapa de procesos",
            'context': {'sgi_diagram_kind': 'process_map', 'sgi_diagram_res_id': self.id,
                        'sgi_diagram_selected': 'sgi.process,%d' % self.id},
        }

    def action_sgi_view_sipoc(self):
        return self._sgi_diagram_action('sipoc', "Tortuga (SIPOC)")

    def action_sgi_view_doc_tree(self):
        return self._sgi_diagram_action('doc_tree', "Árbol documental")

    @api.model
    def sgi_map_data(self):
        """Compatibilidad 53.3.x: los datos del mapa viven en sgi.diagram."""
        data = self.env['sgi.diagram'].data('process_map')
        return {'bands': data['lanes'], 'flows': data['edges']}
