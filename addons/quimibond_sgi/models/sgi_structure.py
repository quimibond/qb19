# -*- coding: utf-8 -*-
"""Estructura del SGI (CEO, 2026-09-24), pasos 3 y 4: la ficha del proceso
(nivel 2) y la ficha de la actividad (nivel 3) como pantallas de un solo
lugar. Solo presentación y navegación; el modelo de datos no cambia.

Ficha de proceso: arriba dueño, estado, semáforo, actividades atrasadas,
KPIs en rojo y NC abiertas; pestañas Actividades · Indicadores · Riesgos ·
Documentos · No conformidades · Con quién se conecta; botones Imprimir
procedimiento · Pedir un cambio · Ver en diagrama.

Ficha de actividad: «Ir a hacerlo», «Ver instructivo» y «Ver registros
recientes».
"""
from odoo import api, fields, models
from odoo.exceptions import UserError


class SgiProcessStructure(models.Model):
    _inherit = 'sgi.process'

    nc_open_ids = fields.Many2many(
        'quality.alert', string="No conformidades abiertas",
        compute='_compute_nc_open_ids')
    structure_status = fields.Char(
        string="Estado del proceso", compute='_compute_structure_status',
        help="Una línea: dueño, estado, semáforo, actividades atrasadas, KPIs "
             "en rojo y NC abiertas.")

    def _compute_nc_open_ids(self):
        Alert = self.env['quality.alert'].sudo()
        for process in self:
            if not process.id:
                process.nc_open_ids = Alert
                continue
            domain = [('sgi_process_id', '=', process.id)]
            if 'sgi_stage_is_closing' in Alert._fields:
                domain += [('sgi_stage_is_closing', '=', False), ('sgi_stage_is_cancel', '=', False)]
            process.nc_open_ids = Alert.search(domain, order='create_date desc')

    @api.depends('owner_id', 'state', 'health', 'activity_red_count', 'red_kpi_count', 'nc_count',
                 'overdue_action_count')
    def _compute_structure_status(self):
        states = dict(self._fields['state'].selection)
        healths = {'verde': "en verde", 'amarillo': "en amarillo", 'rojo': "en rojo"}
        for process in self:
            parts = []
            parts.append("Dueño: %s" % (process.owner_id.name or "sin dueño"))
            parts.append(states.get(process.state, process.state or ''))
            if process.health:
                parts.append(healths.get(process.health, process.health))
            if process.activity_red_count:
                parts.append("%d actividad(es) atrasada(s)" % process.activity_red_count)
            if process.red_kpi_count:
                parts.append("%d indicador(es) en rojo" % process.red_kpi_count)
            if process.nc_count:
                parts.append("%d NC abierta(s)" % process.nc_count)
            if process.overdue_action_count:
                parts.append("%d acción(es) vencida(s)" % process.overdue_action_count)
            process.structure_status = " · ".join(p for p in parts if p)

    def action_sgi_request_change(self):
        """«Pedir un cambio»: abre una solicitud de cambio documental (F-P-G01-06)
        ya apuntando al procedimiento vigente del proceso. La aprueba MAST y
        edita la actividad; ficha, Mi procedimiento y PDF se regeneran solos."""
        self.ensure_one()
        category = self.env.ref('quimibond_sgi.sgi_approval_category_doc_change',
                                raise_if_not_found=False)
        if not category:
            raise UserError("No está configurada la categoría de cambio documental del SGI.")
        doc = self._sgi_procedure_document()
        context = {
            'default_category_id': category.id,
            'default_name': "Cambio al proceso %s %s" % (self.code or '', self.name or ''),
            'default_sgi_change_kind': 'modificacion' if doc else 'alta',
            'default_sgi_what_changes': 'contenido',
            'default_sgi_affected_process_ids': [(6, 0, self.ids)],
            'default_request_owner_id': self.env.user.id,
        }
        if doc:
            context['default_sgi_document_id'] = doc.id
        return {
            'type': 'ir.actions.act_window',
            'name': "Pedir un cambio — %s" % self.display_name,
            'res_model': 'approval.request',
            'view_mode': 'form',
            'target': 'current',
            'context': context,
        }

    def action_sgi_view_diagram(self):
        """«Ver en diagrama»: las flechas del mapa que entran y salen de este
        proceso, agrupadas por quién entrega."""
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'name': "Con quién se conecta — %s" % self.display_name,
            'res_model': 'sgi.process.flow',
            'view_mode': 'list,form',
            'domain': ['|', ('from_process_id', '=', self.id), ('to_process_id', '=', self.id)],
            'context': {'search_default_group_from': 1},
        }


class SgiProcessActivityStructure(models.Model):
    _inherit = 'sgi.process.activity'

    def action_open_instruction(self):
        """«Ver instructivo»: el archivo o enlace del IT de la actividad."""
        self.ensure_one()
        if not self.instruction_id:
            raise UserError("Esta actividad no tiene instructivo ligado.")
        return self.instruction_id.action_sgi_view_file()

    def action_view_recent_records(self):
        """«Ver registros recientes»: la evidencia real, la más nueva primero."""
        action = self.action_view_measure_records()
        action['name'] = "%s — registros recientes" % (self.name or self.number or '')
        action['context'] = dict(action.get('context') or {}, search_default_recent=1)
        return action
