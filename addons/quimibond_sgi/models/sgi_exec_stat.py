# -*- coding: utf-8 -*-
"""Quién ejecutó cada actividad, por semana (adelanto de la fase 2).

Una fila por actividad, semana (lunes), usuario y clase de ejecución. La
escribe el cron de medición (reemplaza las semanas que recalcula); nadie la
edita. Existe como modelo, y no como JSON en la actividad, para poder
filtrar, agrupar, pivotear y graficar «quién hace qué» en todas las
actividades a la vez.
"""
from odoo import models, fields, api

SGI_EXEC_CLASSES = [
    ('correcto', "Correcto"),
    ('otro_puesto', "Otro puesto"),
    ('generico', "Cuenta genérica"),
    ('sin_empleado', "Sin empleado"),
    ('sistema', "Sistema"),
]


class SgiActivityExecStat(models.Model):
    _name = 'sgi.activity.exec.stat'
    _description = "Ejecuciones de una actividad SGI por semana y usuario"
    _order = 'period_start desc, activity_id, count desc'

    activity_id = fields.Many2one(
        'sgi.process.activity', string="Actividad", required=True,
        ondelete='cascade', index=True, readonly=True)
    process_id = fields.Many2one(
        related='activity_id.process_id', string="Proceso", store=True,
        index=True)
    period_start = fields.Date(
        string="Semana", required=True, index=True, readonly=True,
        help="Lunes de la semana medida.")
    user_id = fields.Many2one(
        'res.users', string="Usuario", index=True, readonly=True)
    employee_id = fields.Many2one(
        'hr.employee', string="Empleado", readonly=True,
        help="Empleado del usuario al momento de medir.")
    job_id = fields.Many2one(
        'hr.job', string="Puesto", readonly=True,
        help="Puesto del empleado al momento de medir.")
    family_id = fields.Many2one(
        'sgi.job.family', string="Familia", readonly=True)
    exec_class = fields.Selection(
        SGI_EXEC_CLASSES, string="Clase", readonly=True,
        help="Vacía cuando el ejecutor de la actividad es un rol relativo "
             "(solicitante, quien detecta…): no hay contra quién comparar.")
    count = fields.Integer(string="Ejecuciones", aggregator='sum', readonly=True)
    company_id = fields.Many2one(
        related='activity_id.company_id', string="Empresa", store=True,
        index=True)

    @api.depends('activity_id', 'period_start', 'user_id')
    def _compute_display_name(self):
        for stat in self:
            stat.display_name = "%s · %s · %s" % (
                stat.activity_id.display_name or '', stat.period_start or '',
                stat.user_id.name or "Sin usuario")
