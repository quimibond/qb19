# -*- coding: utf-8 -*-
from odoo import models, fields
from odoo.exceptions import UserError


class ProjectProject(models.Model):
    _inherit = 'project.project'

    sgi_is_improvement = fields.Boolean(string="Proyecto de mejora SGI")


class ProjectTaskType(models.Model):
    _inherit = 'project.task.type'

    sgi_is_done_stage = fields.Boolean(string="Etapa de cierre (Mejora SGI)")


class ProjectTask(models.Model):
    _inherit = 'project.task'

    sgi_is_improvement = fields.Boolean(related='project_id.sgi_is_improvement', store=True)
    sgi_improvement_type = fields.Selection([
        ('ambiental', "Ambiental"),
        ('proceso', "Proceso"),
        ('recurso', "Recurso"),
        ('otros', "Otros"),
    ], string="Tipo de mejora")
    sgi_area_id = fields.Many2one('sgi.area', string="Área SGI")
    sgi_process_id = fields.Many2one('sgi.process', string="Proceso SGI")

    def _sgi_check_can_close_improvement(self):
        for task in self:
            if not task.sgi_is_improvement:
                continue
            problems = []
            if not task.date_deadline:
                problems.append("• Falta la fecha límite (date_deadline).")
            att_count = self.env['ir.attachment'].search_count([
                ('res_model', '=', 'project.task'),
                ('res_id', '=', task.id),
            ])
            if att_count == 0:
                problems.append("• Falta al menos un adjunto como evidencia.")
            if problems:
                raise UserError(
                    "No se puede cerrar la mejora '%s':\n%s" % (task.name, "\n".join(problems)))

    def write(self, vals):
        if 'stage_id' in vals:
            new_stage = self.env['project.task.type'].browse(vals['stage_id'])
            if new_stage.sgi_is_done_stage:
                for task in self:
                    if task.stage_id != new_stage:
                        task._sgi_check_can_close_improvement()
        return super().write(vals)
