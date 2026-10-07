# -*- coding: utf-8 -*-
"""57.121.0 (C1, medición): las fichas de C1 se miden con los campos nuevos del
proyecto de desarrollo y no con las tareas de la plantilla vieja ni con el AMEF.

La medición de una actividad cuelga de su entregable (``measure_method =
entregable``): el entregable dice modelo, filtro, fecha y usuario, y la
actividad los copia. Aquí se re-apuntan los entregables de C1 (por código) a
``project.project``, ``sgi.dev.lab.request``, ``sgi.dev.mp.wait`` y
``sgi.dev.stage.log``. Jose, 2026-10-07 (contexto post-despliegue, punto 2).

Campos que se agregan para poder medir con fecha y usuario reales:

- ``project.project.sgi_dev_analysis_date`` / ``sgi_dev_analysis_by_id``:
  cuándo y quién capturó el resultado del análisis (C1.02).
- ``project.project.sgi_dev_approved_date``: cuándo se firmó «Aprobó» en la
  solicitud (C1.07; el usuario ya estaba en ``sgi_dev_approved_by_id``).
- ``sgi.dev.stage.log.stage_key``: clave de la etapa de avance guardada en el
  reloj, para filtrar «pasó por Respuesta del cliente / Pilotaje» sin ids.
"""
import logging

from odoo import api, fields, models

_logger = logging.getLogger(__name__)

DEV_PROJECT_DOMAIN = "('sgi_is_ft', '=', True), ('is_template', '=', False)"
DEV_RELATED_DOMAIN = "('project_id.sgi_is_ft', '=', True), ('project_id.is_template', '=', False)"

# Código del entregable → cómo se mide ahora. ``activity_number`` solo en los
# entregables nuevos: la actividad que los entrega y se mide con ellos.
C1_MEASURES = {
    'C1-ANALISIS': {
        'name': "Análisis de la muestra o especificación registrado en el proyecto",
        'model': 'project.project',
        'domain': "[%s, ('sgi_dev_analysis_result', '!=', False)]" % DEV_PROJECT_DOMAIN,
        'date_field': 'sgi_dev_analysis_date',
        'user_field': 'sgi_dev_analysis_by_id',
        'complete_domain': "[('sgi_dev_line_ids', '!=', False)]",
        'complete_criteria': "Resultado del análisis capturado y tabla de características con renglones.",
    },
    'C1-AMEF': {
        'name': "Factibilidad revisada y aprobada por Ventas (checklist)",
        'model': 'project.project',
        'domain': "[%s, ('sgi_dev_review_state', '=', 'aprobado')]" % DEV_PROJECT_DOMAIN,
        'date_field': 'sgi_dev_review_date',
        'user_field': 'sgi_dev_reviewed_by_id',
        'complete_domain': "[('sgi_dev_feasibility_ids', '!=', False)]",
        'complete_criteria': "Revisión única de Ventas aprobada con el checklist de factibilidad contestado.",
    },
    'C1-PLAN': {
        'name': "Solicitud de desarrollo aprobada con folio FT",
        'model': 'project.project',
        'domain': "[%s, ('sgi_ft_folio', '!=', False), ('sgi_dev_approved_by_id', '!=', False)]" % DEV_PROJECT_DOMAIN,
        'date_field': 'sgi_dev_approved_date',
        'user_field': 'sgi_dev_approved_by_id',
        'complete_domain': "[('sgi_dev_prepared_by_id', '!=', False)]",
        'complete_criteria': "Folio FT asignado, «Elaboró» y «Aprobó» firmados en la solicitud.",
    },
    'C1-MP-ESPERA': {
        'name': "Espera de materia prima del desarrollo registrada",
        'model': 'sgi.dev.mp.wait',
        'domain': "[%s]" % DEV_RELATED_DOMAIN,
        'date_field': 'date_start',
        'user_field': 'create_uid',
        'complete_domain': "[('date_end', '!=', False)]",
        'complete_criteria': "La espera tiene fecha de llegada de la materia prima.",
        'activity_number': 'C1.08',
    },
    'C1-PRUEBAS': {
        'name': "Resultados de laboratorio de la muestra (solicitud medida)",
        'model': 'sgi.dev.lab.request',
        'domain': "[%s, ('state', '=', 'medida')]" % DEV_RELATED_DOMAIN,
        'date_field': 'date_measured',
        'user_field': 'write_uid',
        'complete_domain': "[('pending_count', '=', 0)]",
        'complete_criteria': "Todos los renglones pedidos tienen resultado.",
    },
    'C1-RESPUESTA': {
        'name': "Respuesta del cliente a la muestra (paso por la etapa)",
        'model': 'sgi.dev.stage.log',
        'domain': "[%s, ('stage_key', '=', 'respuesta_cliente')]" % DEV_RELATED_DOMAIN,
        'date_field': 'date_start',
        'user_field': 'create_uid',
        'complete_domain': "[('date_end', '!=', False)]",
        'complete_criteria': "El proyecto salió de «Respuesta del cliente» hacia la etapa siguiente.",
    },
    'C1-APROBADO': {
        'name': "Producto y proceso en pilotaje (paso por la etapa)",
        'model': 'sgi.dev.stage.log',
        'domain': "[%s, ('stage_key', '=', 'pilotaje')]" % DEV_RELATED_DOMAIN,
        'date_field': 'date_start',
        'user_field': 'create_uid',
        'complete_domain': "[('date_end', '!=', False)]",
        'complete_criteria': "El proyecto salió de «Pilotaje» hacia Liberado.",
    },
}


class SgiDevStageLogKey(models.Model):
    _inherit = 'sgi.dev.stage.log'

    stage_key = fields.Char(string="Clave de la etapa", compute='_compute_stage_key', store=True, index=True,
                            help="Clave de la etapa de avance (solicitud, analisis, …, liberado); vacía si la "
                                 "etapa no es de desarrollo. Sirve para medir sin depender de ids.")

    @api.depends('stage_id')
    def _compute_stage_key(self):
        keys = self.env['project.project']._sgi_dev_stage_keys()
        for log in self:
            log.stage_key = keys.get(log.stage_id.id, ('', 0))[0] or False


class ProjectProjectDevMeasure(models.Model):
    _inherit = 'project.project'

    sgi_dev_analysis_date = fields.Datetime(string="Análisis capturado el", readonly=True, copy=False,
                                            help="Cuándo se capturó el resultado del análisis (se llena solo).")
    sgi_dev_analysis_by_id = fields.Many2one('res.users', string="Análisis capturado por", readonly=True, copy=False,
                                             help="Quién capturó el resultado del análisis (se llena solo).")
    sgi_dev_approved_date = fields.Datetime(string="Aprobada el", readonly=True, copy=False,
                                            help="Cuándo se firmó «Aprobó» en la solicitud de desarrollo (se llena solo).")

    @api.model
    def _sgi_dev_measure_stamps(self, vals):
        """Sellos de fecha y usuario que acompañan a los campos que la medición de C1 lee."""
        now = fields.Datetime.now()
        stamps = {}
        if 'sgi_dev_analysis_result' in vals:
            if vals['sgi_dev_analysis_result']:
                stamps.update({'sgi_dev_analysis_date': now, 'sgi_dev_analysis_by_id': self.env.uid})
            else:
                stamps.update({'sgi_dev_analysis_date': False, 'sgi_dev_analysis_by_id': False})
        if 'sgi_dev_approved_by_id' in vals:
            stamps['sgi_dev_approved_date'] = now if vals['sgi_dev_approved_by_id'] else False
        return stamps

    @api.model_create_multi
    def create(self, vals_list):
        if not self.env.context.get('sgi_dev_migration'):
            for vals in vals_list:
                vals.update(self._sgi_dev_measure_stamps(vals))
        return super().create(vals_list)

    def write(self, vals):
        if not self.env.context.get('sgi_dev_migration'):
            vals = dict(vals, **self._sgi_dev_measure_stamps(vals))
        return super().write(vals)

    # ------------------------------------------------------------------
    # Migración 57.121.0
    # ------------------------------------------------------------------
    @api.model
    def _sgi_dev_backfill_measure_dates(self):
        """Fechas de análisis y de aprobación de los proyectos que ya las tenían capturadas, tomadas
        del seguimiento del chatter (dato real). Lo que no tiene rastro se queda vacío: no se
        inventa una fecha."""
        Tracking = self.env['mail.tracking.value'].sudo()
        Project = self.sudo().with_context(active_test=False, sgi_dev_migration=True)
        filled = Project.browse()
        for field_name, date_field, user_field in (
                ('sgi_dev_analysis_result', 'sgi_dev_analysis_date', 'sgi_dev_analysis_by_id'),
                ('sgi_dev_approved_by_id', 'sgi_dev_approved_date', False)):
            projects = Project.search([('sgi_is_ft', '=', True), (field_name, '!=', False), (date_field, '=', False)])
            for project in projects:
                tracking = Tracking.search([
                    ('mail_message_id.model', '=', 'project.project'), ('mail_message_id.res_id', '=', project.id),
                    ('field_id.name', '=', field_name)], order='id desc', limit=1)
                if not tracking:
                    continue
                message = tracking.mail_message_id
                vals = {date_field: message.date}
                if user_field:
                    vals[user_field] = message.author_id.user_ids[:1].id or message.create_uid.id
                project.write(vals)
                filled |= project
        return filled

    @api.model
    def _sgi_dev_apply_c1_measures(self):
        """Re-apunta los entregables de C1 (por código) a los modelos nuevos y crea los que faltan.
        Idempotente. Devuelve {'updated': [códigos], 'created': [códigos], 'missing': [códigos]}."""
        Deliverable = self.env['sgi.deliverable'].sudo().with_context(active_test=False)
        Activity = self.env['sgi.process.activity'].sudo().with_context(active_test=False)
        IrModel = self.env['ir.model'].sudo()
        report = {'updated': [], 'created': [], 'missing': []}
        touched = Deliverable.browse()
        for code, spec in C1_MEASURES.items():
            if spec['model'] not in self.env:
                report['missing'].append(code)
                continue
            vals = {
                'name': spec['name'],
                'odoo_model_id': IrModel._get(spec['model']).id,
                'measure_domain': spec['domain'],
                'measure_date_field': spec['date_field'],
                'measure_user_field': spec['user_field'] or False,
            }
            if 'complete_domain' in Deliverable._fields:
                vals.update({'complete_domain': spec.get('complete_domain') or False,
                             'complete_criteria': spec.get('complete_criteria') or False})
            deliverable = Deliverable.search([('code', '=', code)], limit=1)
            if deliverable:
                changed = {k: v for k, v in vals.items()
                           if (deliverable[k].id if isinstance(deliverable[k], models.BaseModel) else deliverable[k]) != v}
                if changed:
                    deliverable.write(changed)
                    report['updated'].append(code)
                touched |= deliverable
                continue
            activity = Activity.search([('number', '=', spec.get('activity_number') or '')], limit=1)
            if not activity:
                report['missing'].append(code)
                continue
            deliverable = Deliverable.create(dict(vals, code=code, company_id=activity.company_id.id))
            activity.write({'output_deliverable_ids': [(4, deliverable.id)], 'measure_deliverable_id': deliverable.id,
                            'measure_method': 'entregable'})
            report['created'].append(code)
            touched |= deliverable
        # La actividad copia modelo, filtro, fecha y usuario del entregable: se vuelve a copiar.
        touched.measured_activity_ids._sgi_apply_deliverable_measure()
        return report
