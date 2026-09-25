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

    def action_sgi_view_diagram(self):
        """«Ver en diagrama»: las actividades del proceso encadenadas paso a
        paso, con la vista nativa de organigrama."""
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'name': "Diagrama — %s" % self.display_name,
            'res_model': 'sgi.process.activity',
            'view_mode': 'hierarchy,list,form',
            'views': [(self.env.ref('quimibond_sgi.sgi_process_activity_view_hierarchy').id, 'hierarchy'),
                      (False, 'list'), (False, 'form')],
            'domain': [('process_id', '=', self.id)],
            'context': {'default_process_id': self.id},
        }

    def action_sgi_view_process_map(self):
        """«Ver en el mapa»: el mapa con conexiones, con este proceso
        seleccionado (sus flechas de entrada y salida resaltadas)."""
        self.ensure_one()
        return {
            'type': 'ir.actions.client',
            'tag': 'sgi_process_map',
            'name': "Mapa de procesos",
            'context': {'sgi_map_process_id': self.id},
        }

    @api.model
    def sgi_map_data(self):
        """Datos del mapa de procesos con conexiones: procesos activos por
        banda (estratégicos, cadena de valor, soporte) y los flujos entre
        ellos (sgi.process.flow). Lo consume el componente sgi_process_map."""
        labels = dict(self._fields['process_type'].selection)
        order = ['estrategico', 'cop', 'soporte']
        processes = self.search([('active', '=', True)], order='process_type, code')
        flows = self.env['sgi.process.flow'].search([
            ('from_process_id', 'in', processes.ids), ('to_process_id', 'in', processes.ids)])
        in_count, out_count = {}, {}
        for flow in flows:
            out_count[flow.from_process_id.id] = out_count.get(flow.from_process_id.id, 0) + 1
            in_count[flow.to_process_id.id] = in_count.get(flow.to_process_id.id, 0) + 1
        by_type = {}
        for process in processes:
            by_type.setdefault(process.process_type or 'soporte', []).append({
                'id': process.id,
                'code': process.code or '',
                'name': process.name or '',
                'owner': process.owner_id.name or '',
                'health': process.health or 'amarillo',
                'in_count': in_count.get(process.id, 0),
                'out_count': out_count.get(process.id, 0),
            })
        keys = [k for k in order if k in by_type] + sorted(k for k in by_type if k not in order)
        return {
            'bands': [{'key': k, 'label': labels.get(k, k), 'processes': by_type[k]} for k in keys],
            'flows': [{
                'id': flow.id, 'name': flow.name or '',
                'from_id': flow.from_process_id.id, 'to_id': flow.to_process_id.id,
                'from_code': flow.from_process_id.code or '', 'to_code': flow.to_process_id.code or '',
            } for flow in flows],
        }
