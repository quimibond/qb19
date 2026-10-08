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
    'C1-RUTA': {
        'name': "Ruta preliminar del artículo de desarrollo (operaciones de la lista de materiales)",
        'model': 'mrp.bom',
        # 57.127.0 (Jose, 1a): atribuida a quien asignó la ruta, no al último que editó la lista.
        'domain': "[('product_tmpl_id.sgi_dev_project_id', '!=', False), ('operation_ids', '!=', False), "
                  "('sgi_route_assigned_by_id', '!=', False)]",
        'date_field': 'sgi_route_assigned_date',
        'user_field': 'sgi_route_assigned_by_id',
        'complete_domain': "[('operation_ids.workcenter_id', '!=', False)]",
        'complete_criteria': "Cada operación de la lista de materiales tiene centro de trabajo.",
        'activity_number': 'C1.04b',
    },
    # 57.127.0 (Jose, 1c y 3.2): la orden de muestra nace del proyecto; se mide por proyecto, no por tipo 86.
    'C1-OP-MUESTRA': {
        'name': "Orden de muestra en Tejido Desarrollo",
        'model': 'mrp.production',
        'domain': "[('sgi_dev_project_id', '!=', False), ('sgi_dev_issued_by_id', '!=', False), ('state', '!=', 'cancel')]",
        'date_field': 'sgi_dev_issued_date',
        'user_field': 'sgi_dev_issued_by_id',
        'complete_domain': "[('bom_id', '!=', False), ('origin', '!=', False)]",
        'complete_criteria': "Orden confirmada por Planeación con el artículo del proyecto, su lista de materiales y el folio FT en el origen.",
        'activity_number': 'C1.09',
    },
    'C1-MUESTRA': {
        'name': "Muestra corrida con sus parámetros por área",
        'model': 'mrp.production',
        'domain': "[('sgi_dev_project_id', '!=', False), ('sgi_dev_run_validated_by_id', '!=', False)]",
        'date_field': 'sgi_dev_run_validated_date',
        'user_field': 'sgi_dev_run_validated_by_id',
        'complete_domain': "[('state', '=', 'done')]",
        'complete_criteria': "Orden terminada y corrida validada por el supervisor del área.",
        'activity_number': 'C1.10',
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
    # 57.131.0 (Jose 2026-10-08, punto 4): C1.11 se atribuye a quien dio el dictamen
    # (Diseño de Producto), no al último que editó la solicitud.
    'C1-PRUEBAS': {
        'name': "Resultados de laboratorio de la muestra dictaminados por Diseño de Producto",
        'model': 'sgi.dev.lab.request',
        'domain': "[%s, ('state', '=', 'medida'), ('verdict_by_id', '!=', False)]" % DEV_RELATED_DOMAIN,
        'date_field': 'date_verdict',
        'user_field': 'verdict_by_id',
        'complete_domain': "[('pending_count', '=', 0)]",
        'complete_criteria': "Todos los renglones pedidos tienen resultado y Diseño de Producto dictaminó.",
    },
    # 57.129.0 (Jose, 3.3): el envío y la respuesta del cliente son registros propios; C1.12 y C1.13
    # se miden por ellos (antes C1.13 por el paso del proyecto por la etapa).
    'C1-ENVIO': {
        'name': "Muestra enviada al cliente (registro del envío)",
        'model': 'sgi.dev.shipment',
        'domain': "[%s, ('state', 'in', ('enviada', 'respondida'))]" % DEV_RELATED_DOMAIN,
        'date_field': 'shipped_at',
        'user_field': 'shipped_by_id',
        'complete_domain': "[('roll_ids', '!=', False), ('notified_date', '!=', False)]",
        'complete_criteria': "Rollos con sus dimensiones registrados y aviso al cliente enviado.",
        'activity_number': 'C1.12',
    },
    'C1-RESPUESTA': {
        'name': "Respuesta del cliente a la muestra (registro con evidencia)",
        'model': 'sgi.dev.shipment',
        'domain': "[%s, ('state', '=', 'respondida')]" % DEV_RELATED_DOMAIN,
        'date_field': 'response_registered_at',
        'user_field': 'response_registered_by_id',
        'complete_domain': "[('response_medium', '!=', False), ('response_date', '!=', False)]",
        'complete_criteria': "Respuesta (aprueba, pide cambios o rechaza) con medio, fecha y evidencia adjunta.",
    },
    'C1-APROBADO': {
        'name': "Producto y proceso en pilotaje (paso por la etapa)",
        'model': 'sgi.dev.stage.log',
        'domain': "[%s, ('stage_key', '=', 'pilotaje')]" % DEV_RELATED_DOMAIN,
        'date_field': 'date_start',
        'user_field': 'user_id',  # 57.127.0 (Jose, 1b): quién lo pasó a la etapa
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
    def _sgi_dev_c1_deliverable_vals(self, code):
        """Valores de medición del entregable de C1 ``code`` (None si su modelo no está instalado)."""
        spec = C1_MEASURES[code]
        if spec['model'] not in self.env:
            return None
        Deliverable = self.env['sgi.deliverable']
        vals = {
            'name': spec['name'],
            'odoo_model_id': self.env['ir.model'].sudo()._get(spec['model']).id,
            'measure_domain': spec['domain'],
            'measure_date_field': spec['date_field'],
            'measure_user_field': spec['user_field'] or False,
        }
        if 'complete_domain' in Deliverable._fields:
            vals.update({'complete_domain': spec.get('complete_domain') or False,
                         'complete_criteria': spec.get('complete_criteria') or False})
        if 'measure_user_history' in Deliverable._fields:
            # 57.127.0 (Jose, 1b): el usuario sale de un campo, no del historial de estado.
            vals['measure_user_history'] = False
        return vals

    @api.model
    def _sgi_dev_ensure_c1_deliverable(self, code, company=None):
        """El entregable de C1 ``code``, creado si no existe (57.123.1: antes que la actividad que
        lo entrega, porque un procedimiento vigente no admite una actividad «por su entregable»
        sin entregable con modelo). Vacío si su modelo no está instalado."""
        Deliverable = self.env['sgi.deliverable'].sudo().with_context(active_test=False)
        deliverable = Deliverable.search([('code', '=', code)], limit=1)
        if deliverable:
            return deliverable
        vals = self._sgi_dev_c1_deliverable_vals(code)
        if vals is None:
            return Deliverable.browse()
        return Deliverable.create(dict(vals, code=code, company_id=(company or self.env.company).id))

    @api.model
    def _sgi_dev_apply_c1_measures(self):
        """Re-apunta los entregables de C1 (por código) a los modelos nuevos y crea los que faltan.
        Idempotente. Devuelve {'updated': [códigos], 'created': [códigos], 'missing': [códigos]}."""
        Deliverable = self.env['sgi.deliverable'].sudo().with_context(active_test=False)
        Activity = self.env['sgi.process.activity'].sudo().with_context(active_test=False)
        report = {'updated': [], 'created': [], 'missing': []}
        touched = Deliverable.browse()
        for code, spec in C1_MEASURES.items():
            vals = self._sgi_dev_c1_deliverable_vals(code)
            if vals is None:
                report['missing'].append(code)
                continue
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
