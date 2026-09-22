# -*- coding: utf-8 -*-
"""Procedimiento vivo: el Desarrollo del procedimiento como datos del proceso.

El procedimiento deja de ser un PDF adjunto — su alcance, responsabilidades y
actividades viven como datos estructurados del proceso, y el PDF se genera
desde Odoo con el layout del F-P-G01-02 (ver report/report_procedure.xml).

Son LÍNEAS (no evidencia), así que NO usan el mixin de folio/inmutabilidad.

Medición (fase 10): una actividad puede declarar el modelo de Odoo cuyos
registros son la EVIDENCIA de que se ejecutó (cotización creada, OC enviada,
MO cerrada). Un cron evalúa cada actividad contra su cadencia esperada y
pinta el semáforo de cumplimiento — el procedimiento se mide con acciones
reales, no con texto.
"""
import logging

from datetime import datetime, timedelta

from odoo import models, fields, api, Command, SUPERUSER_ID
from odoo.exceptions import UserError, ValidationError
from odoo.tools.safe_eval import safe_eval

from .sgi_base import sgi_bypass_allowed

SGI_AUTOMATION_LEVELS = [
    ('manual', "Manual"),
    ('asistido', "Asistido"),
    ('automatico', "Automático"),
    ('agente_ia', "Agente de IA"),
]

_logger = logging.getLogger(__name__)


class SgiProcessProcedure(models.Model):
    """Extiende el proceso con el cuerpo del procedimiento controlado."""
    _inherit = 'sgi.process'

    scope = fields.Text(
        string="Alcance",
        help="A qué áreas/actividades aplica el procedimiento (sección 2).")
    env_aspects = fields.Text(
        string="Descripción de aspectos ambientales",
        help="Aspectos ambientales del proceso (sección 5 del procedimiento).")
    norm_ids = fields.Many2many(
        'sgi.norm', 'sgi_process_norm_rel', 'process_id', 'norm_id',
        string="Marco normativo",
        help="Normas ISO que rigen el proceso (sección 7).")
    job_responsibility_ids = fields.One2many(
        'sgi.process.responsibility', 'process_id',
        string="Responsabilidades de las áreas")
    # OJO con el nombre: mail.activity.mixin (heredado por sgi.process) ya
    # define activity_ids para las actividades de mail — llamar igual a este
    # one2many pisaba el campo del mixin y el registro NO CARGABA
    # (KeyError: activity_type_id ... does not exist). De ahí el prefijo.
    procedure_activity_ids = fields.One2many(
        'sgi.process.activity', 'process_id',
        string="Actividades del procedimiento")
    activity_count = fields.Integer(
        string="# Actividades", compute='_compute_activity_count')

    # Cumplimiento del procedimiento: agregado de las actividades medibles.
    measurable_activity_count = fields.Integer(
        string="Actividades medibles", compute='_compute_measure_stats')
    measure_red_count = fields.Integer(
        string="Sin evidencia en su periodo", compute='_compute_measure_stats')
    procedure_compliance = fields.Integer(
        string="% Cumplimiento del procedimiento",
        compute='_compute_measure_stats',
        help="Porcentaje de actividades medibles con evidencia dentro de su "
             "cadencia esperada (lo calcula el cron de medición).")
    # Cuánto del proceso está a la vista: actividades medidas de verdad
    # (odoo + consecuencia) entre el total.
    measure_real_pct = fields.Integer(
        string="% medido de verdad", compute='_compute_measure_methods',
        help="Actividades con método «Registro en Odoo» o «Por consecuencia» "
             "entre el total de actividades activas del proceso.")
    measure_method_summary = fields.Char(
        string="Actividades por método", compute='_compute_measure_methods')
    activity_green_count = fields.Integer(
        string="En verde", compute='_compute_activity_board')
    activity_red_count = fields.Integer(
        string="En rojo", compute='_compute_activity_board')
    activity_grey_count = fields.Integer(
        string="Sin evidencia aún", compute='_compute_activity_board',
        help="Pendientes de conector o registro, no aplica o sin medir.")
    activity_no_method_count = fields.Integer(
        string="Sin método", compute='_compute_activity_board')
    measure_adherence_avg = fields.Float(
        string="Adherencia promedio (%)", compute='_compute_activity_board',
        digits=(5, 1),
        help="Promedio de adherencia de las actividades con campo de usuario.")
    # Guardado para la barra del kanban (<progressbar> agrupa por él).
    measure_status = fields.Selection([
        ('verde', "Todo con evidencia"),
        ('gris', "Hay actividades sin evidencia aún"),
        ('rojo', "Hay actividades en rojo"),
    ], string="Estado de medición", compute='_compute_measure_status', store=True)
    chain_link_ids = fields.Many2many(
        'sgi.activity.link', string="Ligas entre actividades",
        compute='_compute_chain_link_ids')

    # Firmas del procedimiento (bloque del F-P-G01-02). Se imprimen como
    # nombre + cargo; el PDF generado es copia NO controlada, sin imagen de firma.
    doc_owner_id = fields.Many2one(
        'res.users', string="Responsable del documento",
        help="Elabora / es dueño del procedimiento (bloque de firmas).")
    doc_approver_id = fields.Many2one(
        'res.users', string="Aprueba")
    doc_vobo_id = fields.Many2one(
        'res.users', string="Vo.Bo.")

    # Campos del cuerpo del procedimiento cuya edición diverge del PDF
    # controlado. 'purpose' (sección 1, OBJETIVO) también se imprime en el
    # F-P-G01-02: editarlo sin nueva revisión ES una divergencia G14.
    _SGI_PROCEDURE_BODY_FIELDS = {
        'purpose', 'scope', 'env_aspects', 'norm_ids',
        'doc_owner_id', 'doc_approver_id', 'doc_vobo_id'}

    @api.depends('procedure_activity_ids')
    def _compute_activity_count(self):
        for process in self:
            process.activity_count = len(process.procedure_activity_ids)

    chain_link_count = fields.Integer(
        string="Ligas de la cadena", compute='_compute_chain_link_count',
        help="Entregas entre actividades que tocan este proceso (entran o salen).")

    # Referencias entrantes: actividades de OTROS procesos que citan mis
    # procedimientos (related_procedure_id) o usan mis formatos. Se calculan
    # solas — nadie captura dos veces: quien menciona, ya avisó.
    inbound_reference_ids = fields.Many2many(
        'sgi.process.activity', string="Actividades que me referencian",
        compute='_compute_inbound_references')
    inbound_reference_count = fields.Integer(
        string="Me referencian", compute='_compute_inbound_references')

    def _compute_inbound_references(self):
        """Por lote: una search de documentos y una de actividades para el
        recordset completo (antes eran 2 searches POR proceso)."""
        Activity = self.env['sgi.process.activity']
        Document = self.env['documents.document']
        processes = self.filtered('id')
        for process in (self - processes):
            process.inbound_reference_ids = False
            process.inbound_reference_count = 0
        if not processes:
            return
        doc_process = {}  # doc_id -> process_id dueño
        for doc in Document.search_read(
                [('sgi_process_id', 'in', processes.ids)], ['sgi_process_id']):
            doc_process[doc['id']] = doc['sgi_process_id'][0]
        acts = Activity.search([
            '|', ('related_procedure_id', 'in', list(doc_process)),
            ('format_document_ids', 'in', list(doc_process)),
        ]) if doc_process else Activity.browse()
        by_process = {}  # process_id -> [activity_id]
        for act in acts:
            owner_pids = set()
            related_id = act.related_procedure_id.id
            if related_id in doc_process:
                owner_pids.add(doc_process[related_id])
            for doc_id in act.format_document_ids.ids:
                if doc_id in doc_process:
                    owner_pids.add(doc_process[doc_id])
            for pid in owner_pids:
                if act.process_id.id != pid:
                    by_process.setdefault(pid, []).append(act.id)
        for process in processes:
            act_ids = by_process.get(process.id, [])
            process.inbound_reference_ids = Activity.browse(act_ids)
            process.inbound_reference_count = len(act_ids)

    def action_view_inbound_references(self):
        """Quién me menciona: las actividades ajenas que citan mis
        procedimientos o usan mis formatos."""
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'name': "Me referencian — %s" % self.name,
            'res_model': 'sgi.process.activity',
            'view_mode': 'list,form',
            'domain': [('id', 'in', self.inbound_reference_ids.ids)],
        }

    chain_stuck_count = fields.Integer(
        string="Eslabones atorados", compute='_compute_chain_link_count',
        help="Ligas de este proceso donde el paso origen entregó pero el "
             "destino no tiene evidencia en su periodo.")

    def _compute_chain_link_count(self):
        """Por lote: una sola search_read para el recordset (antes 2
        search_count por proceso). El set de pids deduplica los eslabones cuyo
        origen y destino caen en el mismo proceso."""
        Link = self.env['sgi.activity.link']
        processes = self.filtered('id')
        for process in (self - processes):
            process.chain_link_count = 0
            process.chain_stuck_count = 0
        if not processes:
            return
        wanted = set(processes.ids)
        totals, stuck = {}, {}
        for link in Link.search_read(
                ['|', ('from_process_id', 'in', list(wanted)),
                 ('to_process_id', 'in', list(wanted))],
                ['from_process_id', 'to_process_id', 'chain_state']):
            pids = {value[0] for value in (
                link['from_process_id'], link['to_process_id']) if value}
            for pid in pids & wanted:
                totals[pid] = totals.get(pid, 0) + 1
                if link['chain_state'] == 'atorado':
                    stuck[pid] = stuck.get(pid, 0) + 1
        for process in processes:
            process.chain_link_count = totals.get(process.id, 0)
            process.chain_stuck_count = stuck.get(process.id, 0)

    def action_view_chain(self):
        """La cadena del proceso: qué entregables entran y salen, paso a paso."""
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'name': "Cadena — %s" % self.name,
            'res_model': 'sgi.activity.link',
            'view_mode': 'list',
            'domain': ['|', ('from_process_id', '=', self.id),
                       ('to_process_id', '=', self.id)],
        }

    @api.depends('procedure_activity_ids.measure_state',
                 'procedure_activity_ids.measure_model_id')
    def _compute_measure_stats(self):
        for process in self:
            acts = process.procedure_activity_ids.filtered(
                lambda a: a.measure_model_id and a.measure_state)
            reds = acts.filtered(lambda a: a.measure_state == 'rojo')
            process.measurable_activity_count = len(acts)
            process.measure_red_count = len(reds)
            process.procedure_compliance = (
                int(round((len(acts) - len(reds)) * 100.0 / len(acts)))
                if acts else 0)

    def _compute_measure_methods(self):
        Activity = self.env['sgi.process.activity']
        processes = self.filtered('id')
        counts = {}
        if processes:
            for process, method, count in Activity._read_group(
                    [('process_id', 'in', processes.ids)],
                    ['process_id', 'measure_method'], ['__count']):
                counts.setdefault(process.id, {})[method or False] = count
        labels = dict(Activity._fields['measure_method'].selection)
        labels[False] = "Sin medir"
        for process in self:
            by_method = counts.get(process.id, {})
            total = sum(by_method.values())
            real = by_method.get('odoo', 0) + by_method.get('consecuencia', 0)
            process.measure_real_pct = int(round(real * 100.0 / total)) if total else 0
            process.measure_method_summary = ' · '.join(
                "%s %d" % (labels.get(method, method), count)
                for method, count in sorted(by_method.items(), key=lambda kv: -kv[1]))

    def _compute_activity_board(self):
        Activity = self.env['sgi.process.activity']
        processes = self.filtered('id')
        states, methods, adherence = {}, {}, {}
        if processes:
            for process, state, count in Activity._read_group(
                    [('process_id', 'in', processes.ids)],
                    ['process_id', 'measure_state'], ['__count']):
                states.setdefault(process.id, {})[state or False] = count
            for process, count in Activity._read_group(
                    [('process_id', 'in', processes.ids), ('measure_method', '=', False)],
                    ['process_id'], ['__count']):
                methods[process.id] = count
            for process, avg in Activity._read_group(
                    [('process_id', 'in', processes.ids),
                     ('measure_user_field', '!=', False),
                     ('measure_count_30d', '>', 0)],
                    ['process_id'], ['measure_adherence_pct:avg']):
                adherence[process.id] = avg or 0.0
        for process in self:
            by_state = states.get(process.id, {})
            process.activity_green_count = by_state.get('verde', 0)
            process.activity_red_count = by_state.get('rojo', 0)
            process.activity_grey_count = sum(by_state.values()) \
                - process.activity_green_count - process.activity_red_count
            process.activity_no_method_count = methods.get(process.id, 0)
            process.measure_adherence_avg = round(adherence.get(process.id, 0.0), 1)

    @api.depends('procedure_activity_ids.measure_state',
                 'procedure_activity_ids.measure_method')
    def _compute_measure_status(self):
        for process in self:
            acts = process.procedure_activity_ids
            if any(a.measure_state == 'rojo' for a in acts):
                process.measure_status = 'rojo'
            elif not acts or any(a.measure_state != 'verde' for a in acts):
                process.measure_status = 'gris'
            else:
                process.measure_status = 'verde'

    def _compute_chain_link_ids(self):
        Link = self.env['sgi.activity.link']
        for process in self:
            process.chain_link_ids = Link.search([
                '|', ('from_process_id', '=', process.id),
                ('to_process_id', '=', process.id)]) if process.id else Link

    def action_view_activities(self, extra_domain=None, name=None):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'name': "%s — %s" % (name or "Actividades", self.name),
            'res_model': 'sgi.process.activity',
            'view_mode': 'list,kanban,form',
            'domain': [('process_id', '=', self.id)] + (extra_domain or []),
            'context': {'default_process_id': self.id},
        }

    def action_view_red_activities(self):
        return self.action_view_activities([('measure_state', '=', 'rojo')], "En rojo")

    def action_view_no_method_activities(self):
        return self.action_view_activities([('measure_method', '=', False)], "Sin método")

    def _sgi_measure_strict(self):
        """¿El procedimiento del proceso está en piloto o vigente? Entonces
        ninguna actividad puede quedar «sin medir»."""
        self.ensure_one()
        return bool(self.env['documents.document'].sudo().search_count([
            ('sgi_process_id', '=', self.id),
            ('sgi_doc_type', '=', 'procedimiento'),
            ('sgi_is_controlled', '=', True),
            ('sgi_state', 'in', ('piloto', 'vigente')),
        ], limit=1))

    def _sgi_flag_procedure_dirty(self):
        """Marca el procedimiento controlado VIGENTE como 'pendiente de revisión'
        cuando su procedimiento vivo cambió tras la revisión aprobada (G14).

        Idempotente: sólo actúa en la transición limpio→divergente, así el aviso
        al dueño se agenda una vez por ciclo de revisión. Se omite durante la
        carga de módulo (semillas), cuando el registro aún no está listo, y
        cuando el contexto pide saltarlo (sgi_bypass_dirty): las capturas de
        contenido que SON la revisión vigente (seed_procedure_ventas y similares)
        no son una divergencia y no deben disparar G14.
        """
        if not self.env.registry.ready or (
                self.env.context.get('sgi_bypass_dirty')
                and sgi_bypass_allowed(self.env)):
            return
        for process in self:
            doc = process._sgi_procedure_document()
            if not doc or doc.sgi_procedure_dirty:
                continue
            doc.sudo().write({
                'sgi_procedure_dirty': True,
                'sgi_procedure_dirty_since': fields.Datetime.now(),
                'sgi_procedure_dirty_by': self.env.uid,
            })
            doc.message_post(
                body="⚠ El procedimiento vivo se modificó después de la revisión "
                     "vigente %s. Queda <b>pendiente de revisión documental</b>: "
                     "el PDF impreso ya no coincide con la revisión aprobada." % (
                         doc.sgi_revision_label or ''))
            user_id = doc.sgi_owner_id.id or self.env['sgi.cron']._sgi_manager_user_id()
            if user_id:
                doc.activity_schedule(
                    'mail.mail_activity_data_todo',
                    summary="Procedimiento vivo cambió: revisar %s" % (
                        doc.sgi_code or doc.name),
                    note="Genere una nueva revisión controlada del procedimiento o "
                         "confirme que el cambio no la amerita.",
                    user_id=user_id)

    def write(self, vals):
        res = super().write(vals)
        if self._SGI_PROCEDURE_BODY_FIELDS & set(vals):
            self._sgi_flag_procedure_dirty()
        return res

    # --- Helpers del reporte "Imprimir procedimiento (F-P-G01-02)" -----------
    def _sgi_procedure_document(self):
        """Documento controlado vigente (P-Xnn) que encabeza el procedimiento.
        La clave, fecha de emisión, área y revisión del encabezado se leen EN
        VIVO de aquí (única fuente de verdad)."""
        self.ensure_one()
        docs = self.procedure_ids.filtered(
            lambda d: d.sgi_doc_type == 'procedimiento' and d.sgi_state == 'vigente')
        return docs[:1]

    def _sgi_format_revision(self, code):
        """Revisión viva del documento controlado vigente con esa clave (para el
        pie F-P-G01-02), o False."""
        return self.env['sgi.format.map'].sudo()._revision_of(code)

    def _sgi_document_by_code(self, code):
        """Documento vigente con esa clave (ref. a F-P-S01-01, etc.)."""
        return self.env['documents.document'].sudo().search([
            ('sgi_code', '=', code), ('sgi_state', '=', 'vigente'),
        ], limit=1)

    def _sgi_env_risks(self):
        """Riesgos ambientales ligados al proceso (sección 5)."""
        self.ensure_one()
        return self.risk_ids.filtered(lambda r: r.instrument == 'ambiental')

    def _sgi_iper_risks(self):
        """Riesgos IPER (SST) ligados al proceso (sección 6)."""
        self.ensure_one()
        return self.risk_ids.filtered(lambda r: r.instrument == 'iper')

    def _sgi_documented_info(self):
        """Sección 8, GENERADA: unión sin duplicados de los formatos referenciados
        en las actividades + la familia FK del procedimiento + las referencias
        cruzadas, ordenada por clave."""
        self.ensure_one()
        docs = self.procedure_activity_ids.mapped('format_document_ids')
        proc = self._sgi_procedure_document()
        if proc:
            docs |= proc.sgi_family_document_ids
            docs |= proc.sgi_reference_ids
        return docs.sorted(lambda d: (d.sgi_code or '￿', d.name or ''))

    def action_print_procedure(self):
        self.ensure_one()
        return self.env.ref(
            'quimibond_sgi.action_report_procedure').report_action(self)


class SgiProcessResponsibility(models.Model):
    """Responsabilidad de un rol/puesto dentro del procedimiento (sección 3)."""
    _name = 'sgi.process.responsibility'
    _description = "Responsabilidad de área en el procedimiento"
    _order = 'process_id, sequence, id'

    process_id = fields.Many2one(
        'sgi.process', string="Proceso", required=True, ondelete='cascade',
        index=True)
    sequence = fields.Integer(string="Secuencia", default=10)
    company_id = fields.Many2one(
        related='process_id.company_id', string="Empresa", store=True,
        index=True)
    job_id = fields.Many2one(
        'hr.job', string="Puesto", required=True,
        help="Puesto de hr.job al que corresponde el rol.")
    name = fields.Char(
        string="Rol en el procedimiento", required=True,
        help="Nombre del rol tal como aparece en el procedimiento "
             "(no siempre mapea 1:1 al puesto de hr.job).")
    responsibilities = fields.Text(string="Responsabilidades")

    @api.onchange('job_id')
    def _onchange_job_id(self):
        if self.job_id and not self.name:
            self.name = self.job_id.name

    @api.model_create_multi
    def create(self, vals_list):
        records = super().create(vals_list)
        records.process_id._sgi_flag_procedure_dirty()
        return records

    def write(self, vals):
        res = super().write(vals)
        self.process_id._sgi_flag_procedure_dirty()
        return res

    def unlink(self):
        processes = self.process_id
        res = super().unlink()
        processes._sgi_flag_procedure_dirty()
        return res


class SgiProcessActivity(models.Model):
    """Actividad (numeral) del Desarrollo del procedimiento (sección 4)."""
    _name = 'sgi.process.activity'
    _description = "Actividad del procedimiento"
    _order = 'process_id, sequence, step, id'

    process_id = fields.Many2one(
        'sgi.process', string="Proceso", required=True, ondelete='cascade',
        index=True)
    company_id = fields.Many2one(
        related='process_id.company_id', string="Empresa", store=True,
        index=True)
    active = fields.Boolean(
        default=True,
        help="La carga por API archiva las actividades que ya no vienen en "
             "el catálogo del proceso; nunca las borra.")
    sequence = fields.Integer(string="Secuencia", default=10)
    # El numeral ya no se escribe: es la clave del proceso + un paso entero,
    # único dentro del proceso (C6 + 22 → «C6.22»). Si el proceso cambia de
    # clave, sus numerales cambian solos; el paso nunca se reutiliza. El
    # numeral viejo en texto («4.2.3.1», «P-C16», «3.4-3.6») queda en
    # «Numeral anterior» para buscarlo.
    step = fields.Integer(
        string="Paso", index=True, copy=False,
        help="Número del paso dentro del proceso. Vacío = el siguiente libre.")
    number = fields.Char(
        string="Numeral", compute='_compute_number', store=True, index=True,
        help="Clave del proceso + paso, ej. C6.22. Se calcula.")
    legacy_number = fields.Char(
        string="Numeral anterior", readonly=True, copy=False, index=True,
        help="Numeral en texto de la versión anterior del procedimiento.")
    # Etapa del proceso (A. Recepción, D. Inventario…): antes, texto libre
    # «sección» y un bloque fijo inicial/desarrollo/final.
    stage_id = fields.Many2one(
        'sgi.process.stage', string="Etapa", index=True, ondelete='set null',
        domain="[('process_id', '=', process_id)]")
    block = fields.Selection([
        ('inicial', "Actividades iniciales"),
        ('desarrollo', "Desarrollo"),
        ('final', "Actividades finales"),
    ], string="Bloque (anterior)", default='desarrollo', required=True,
        help="Agrupación fija de la versión anterior; hoy mandan las etapas.")
    section = fields.Char(
        string="Sección", compute='_compute_section', store=True,
        help="Nombre de la etapa (se calcula de «Etapa»).")
    name = fields.Char(string="Resumen", help="Resumen corto de la actividad.")
    description = fields.Text(
        string="Descripción", help="Texto completo del numeral del procedimiento.")
    # Quién ejecuta, aprueba, participa o se entera: filas de
    # sgi.activity.role. «Puestos responsables» se conserva (vistas, filtros
    # «Mis actividades», reportes) calculado desde los roles «ejecuta», sobre
    # la misma tabla de relación de antes; escribirlo crea o quita roles
    # «ejecuta» para no romper a quien todavía lo escribe.
    role_ids = fields.One2many(
        'sgi.activity.role', 'activity_id', string="Roles")
    responsible_job_ids = fields.Many2many(
        'hr.job', 'sgi_activity_job_rel', 'activity_id', 'job_id',
        string="Puestos que ejecutan", compute='_compute_responsible_job_ids',
        inverse='_inverse_responsible_job_ids', store=True)
    instruction_id = fields.Many2one(
        'documents.document', string="Instructivo",
        domain=[('sgi_doc_type', '=', 'instructivo')],
        help="Instructivo (IT) que explica cómo se hace el paso. El "
             "«Procedimiento relacionado» es otra cosa: el procedimiento que "
             "rige la actividad.")
    value_class = fields.Selection([
        ('va', "Agrega valor"),
        ('nva_n', "No agrega valor, necesaria"),
        ('nva', "No agrega valor (desperdicio)"),
    ], string="Clase de valor")
    # Nivel de automatización (fase 5 completa el resto: minutos, volumen,
    # horas liberables). El nivel actual se necesita ya: una actividad
    # automática no lleva rol «ejecuta».
    automation_level_current = fields.Selection(
        SGI_AUTOMATION_LEVELS, string="Automatización actual",
        default='manual', required=True)
    automation_level_target = fields.Selection(
        SGI_AUTOMATION_LEVELS, string="Automatización meta")
    automation_method = fields.Selection([
        ('estandar_odoo', "Estándar de Odoo"),
        ('accion_automatizada', "Acción automatizada"),
        ('cron', "Acción planificada"),
        ('integracion', "Integración"),
        ('agente_ia', "Agente de IA"),
        ('otro', "Otro"),
    ], string="Método de automatización")
    responsible_role = fields.Char(
        string="Rol responsable",
        help="Nombre del rol en negritas del procedimiento (no siempre mapea a "
             "un puesto de hr.job).")
    format_document_ids = fields.Many2many(
        'documents.document', 'sgi_activity_format_rel', 'activity_id', 'document_id',
        string="Formatos referenciados",
        domain=[('sgi_is_controlled', '=', True)],
        help="Claves de formato en rojo que la actividad genera o usa.")
    related_procedure_id = fields.Many2one(
        'documents.document', string="Procedimiento relacionado",
        domain=[('sgi_doc_type', '=', 'procedimiento')],
        help="Otro procedimiento que rige esta actividad (ej. marca P-A22).")
    odoo_ref = fields.Char(
        string="Dónde se ejecuta en Odoo",
        help="Ej. 'Ventas > Pedidos', 'Helpdesk Servicio Técnico'.")
    odoo_menu_id = fields.Many2one(
        'ir.ui.menu', string="Menú de Odoo",
        help="Menú real donde se ejecuta la actividad; el texto impreso se toma "
             "de la ruta.")
    note = fields.Text(
        string="Nota", help="Notas resaltadas del procedimiento.")

    # --- Encadenamiento: de qué actividad viene y a cuál sigue ---
    out_link_ids = fields.One2many(
        'sgi.activity.link', 'from_activity_id', string="Entrega a")
    in_link_ids = fields.One2many(
        'sgi.activity.link', 'to_activity_id', string="Recibe de")
    next_activity_ids = fields.Many2many(
        'sgi.process.activity', string="Siguientes pasos",
        compute='_compute_chain')
    prev_activity_ids = fields.Many2many(
        'sgi.process.activity', string="Pasos anteriores",
        compute='_compute_chain')

    @api.depends('role_ids.role', 'role_ids.target_type', 'role_ids.job_id',
                 'role_ids.family_id.job_ids')
    def _compute_responsible_job_ids(self):
        # Las familias se expanden a todos sus puestos; los roles relativos no
        # tienen puesto fijo y no aparecen aquí.
        for activity in self:
            executors = activity.role_ids.filtered(lambda r: r.role == 'ejecuta')
            activity.responsible_job_ids = executors.job_id | executors.family_id.job_ids

    def _inverse_responsible_job_ids(self):
        for activity in self:
            wanted = activity.responsible_job_ids
            executors = activity.role_ids.filtered(lambda r: r.role == 'ejecuta')
            current = executors.filtered(lambda r: r.target_type == 'job')
            # Lo que ya cubre una familia no se duplica como puesto suelto.
            covered = current.job_id | executors.family_id.job_ids
            commands = [Command.delete(role.id) for role in current
                        if role.job_id not in wanted]
            commands += [Command.create({'role': 'ejecuta', 'target_type': 'job',
                                         'job_id': job.id})
                         for job in wanted - covered]
            if commands:
                activity.write({'role_ids': commands})

    executor_role_ids = fields.Many2many(
        'sgi.activity.role', string="Ejecuta", compute='_compute_role_views')
    approver_role_ids = fields.Many2many(
        'sgi.activity.role', string="Aprueba", compute='_compute_role_views')
    informed_role_ids = fields.Many2many(
        'sgi.activity.role', string="Se entera", compute='_compute_role_views')

    @api.depends('role_ids.role')
    def _compute_role_views(self):
        for activity in self:
            roles = activity.role_ids
            activity.executor_role_ids = roles.filtered(lambda r: r.role == 'ejecuta')
            activity.approver_role_ids = roles.filtered(lambda r: r.role == 'aprueba')
            activity.informed_role_ids = roles.filtered(lambda r: r.role == 'informa')

    def _compute_recent_exec_stat_ids(self):
        start = self._sgi_exec_window_start()
        Stat = self.env['sgi.activity.exec.stat']
        stats = Stat.search([('activity_id', 'in', self.filtered('id').ids),
                             ('period_start', '>=', start)]) if self.filtered('id') else Stat
        for activity in self:
            activity.recent_exec_stat_ids = stats.filtered(
                lambda s, a=activity: s.activity_id == a)

    def _sgi_executor_jobs(self):
        """Puestos que DEBEN ejecutar la actividad (familias expandidas, dueño
        del proceso resuelto). None si el ejecutor es un rol relativo sin
        puesto (solicitante, quien detecta…) o si no hay ejecutor."""
        self.ensure_one()
        executors = self.role_ids.filtered(lambda r: r.role == 'ejecuta')
        if not executors:
            return None
        return executors._sgi_jobs()

    def _sgi_check_roles(self):
        """Exactamente un «ejecuta» (cero si la actividad es automática) y a
        lo más un «aprueba» sin condición. Se puede saltar solo desde código
        de sistema o un Jefe MAST con ``sgi_skip_role_check`` (semillas de
        procedimientos heredados)."""
        if self.env.context.get('sgi_skip_role_check') and sgi_bypass_allowed(self.env):
            return
        for activity in self:
            executors = activity.role_ids.filtered(lambda r: r.role == 'ejecuta')
            label = activity.display_name or activity.number or ''
            if activity.automation_level_current == 'automatico':
                if executors:
                    raise ValidationError(
                        "La actividad %s es automática: no lleva rol «Ejecuta» "
                        "(tiene %s)." % (label, ', '.join(
                            role._sgi_target_label() for role in executors)))
            elif len(executors) != 1:
                raise ValidationError(
                    "La actividad %s debe tener exactamente un puesto que la "
                    "ejecuta (tiene %d). Si nadie la ejecuta porque es "
                    "automática, márcala así en «Automatización actual»." % (
                        label, len(executors)))
            approvers = activity.role_ids.filtered(
                lambda r: r.role == 'aprueba' and not (r.condition or '').strip())
            if len(approvers) > 1:
                raise ValidationError(
                    "La actividad %s tiene %d puestos que aprueban sin "
                    "condición; solo puede haber uno (los demás deben decir "
                    "cuándo aplican)." % (label, len(approvers)))

    @api.constrains('role_ids', 'automation_level_current')
    def _check_roles(self):
        self._sgi_check_roles()

    def _sgi_measure_problems(self, full=True):
        """Lo que impide que la actividad cuente como medida. ``full`` = las
        cuatro revisiones del paso a vigente; si no, solo «sin método»."""
        self.ensure_one()
        if not self.measure_method:
            return ["sin método de medición"]
        if not full:
            return []
        if self.measure_method == 'no_aplica' and not (self.measure_justification or '').strip():
            return ["«No aplica» sin justificación"]
        if self.measure_method == 'consecuencia' and not self.measure_proxy_activity_id:
            return ["«Por consecuencia» sin la actividad que la prueba"]
        if self.measure_method == 'odoo' and not self.measure_model_id:
            return ["«Registro en Odoo» sin modelo"]
        return []

    def _sgi_check_measure_strict(self):
        """Con el procedimiento del proceso en piloto o vigente, ninguna
        actividad queda sin medir ni con un método incompleto."""
        if self.env.context.get('sgi_defer_measure_check') or (
                self.env.context.get('sgi_skip_role_check') and sgi_bypass_allowed(self.env)):
            return
        strict = {}
        for activity in self.filtered('active'):
            process = activity.process_id
            if process.id not in strict:
                strict[process.id] = process._sgi_measure_strict()
            if not strict[process.id]:
                continue
            problems = activity._sgi_measure_problems()
            if problems:
                raise ValidationError(
                    "El procedimiento de %s está en piloto o vigente: la actividad "
                    "%s no puede quedar así (%s)." % (
                        process.display_name, activity.display_name, problems[0]))

    @api.constrains('measure_method', 'measure_justification',
                    'measure_proxy_activity_id', 'measure_model_id', 'process_id')
    def _check_measure_method(self):
        self._sgi_check_measure_strict()

    @api.constrains('measure_proxy_activity_id')
    def _check_proxy_cycle(self):
        """Una actividad no se prueba con otra que (directa o indirectamente)
        se prueba con ella."""
        for activity in self.filtered('measure_proxy_activity_id'):
            seen = activity
            current = activity.measure_proxy_activity_id
            while current:
                if current in seen:
                    raise ValidationError(
                        "Ciclo de «se prueba con»: %s termina probándose con "
                        "ella misma." % activity.display_name)
                seen |= current
                current = current.measure_proxy_activity_id

    def _sgi_proxy_root(self):
        """La actividad que realmente deja evidencia al final de la cadena de
        consecuencias."""
        self.ensure_one()
        root = self
        while root.measure_method == 'consecuencia' and root.measure_proxy_activity_id:
            root = root.measure_proxy_activity_id
        return root

    _process_step_uniq = models.Constraint(
        'unique(process_id, step)',
        "El paso ya existe en el proceso: cada actividad tiene su propio número.",
    )

    @api.depends('process_id.code', 'step')
    def _compute_number(self):
        for activity in self:
            activity.number = "%s.%02d" % (activity.process_id.code, activity.step) \
                if activity.process_id.code and activity.step else False

    @api.depends('stage_id.name', 'stage_id.code')
    def _compute_section(self):
        for activity in self:
            activity.section = activity.stage_id.display_name or False

    @api.constrains('stage_id', 'process_id')
    def _check_stage_process(self):
        for activity in self.filtered('stage_id'):
            if activity.stage_id.process_id != activity.process_id:
                raise ValidationError(
                    "La etapa «%s» es de otro proceso." % activity.stage_id.display_name)

    @api.model
    def _sgi_parse_number(self, process, number):
        """«C6.22» → 22 si la clave es la del proceso; un entero → ese paso;
        cualquier otra cosa → None (numeral en texto de la versión anterior)."""
        if isinstance(number, int) and not isinstance(number, bool):
            return number
        text = (number or '').strip()
        if text.isdigit():
            return int(text)
        prefix = '%s.' % (process.code or '')
        if process.code and text.startswith(prefix) and text[len(prefix):].isdigit():
            return int(text[len(prefix):])
        return None

    def _sgi_structure_vals(self, vals, process=None):
        """Traduce lo que llega como texto (numeral, sección) a estructura
        (paso, etapa). Así la carga y el código viejo que escriben «number» y
        «section» siguen funcionando."""
        vals = dict(vals)
        process = process or self.env['sgi.process'].browse(vals.get('process_id'))
        if 'number' in vals:
            number = vals.pop('number')
            step = self._sgi_parse_number(process, number) if process else None
            if step is not None:
                vals.setdefault('step', step)
            elif number:
                vals.setdefault('legacy_number', number)
        if 'section' in vals:
            section = vals.pop('section')
            if section and process and 'stage_id' not in vals:
                vals['stage_id'] = self.env['sgi.process.stage']._sgi_get_or_create(process, section).id
        return vals

    def _sgi_next_step(self, process, taken=()):
        self.env.cr.execute(
            "SELECT COALESCE(MAX(step), 0) FROM sgi_process_activity WHERE process_id = %s",
            (process.id,))
        return max([self.env.cr.fetchone()[0], *taken]) + 1

    @api.depends('out_link_ids.to_activity_id', 'in_link_ids.from_activity_id')
    def _compute_chain(self):
        for activity in self:
            activity.next_activity_ids = activity.out_link_ids.to_activity_id
            activity.prev_activity_ids = activity.in_link_ids.from_activity_id

    def action_open_next(self):
        """Navega al siguiente paso de la cadena (o a la lista si hay varios)."""
        self.ensure_one()
        nxt = self.next_activity_ids
        if not nxt:
            raise UserError("Esta actividad no tiene un siguiente paso ligado.")
        action = {
            'type': 'ir.actions.act_window',
            'res_model': 'sgi.process.activity',
            'name': "Siguiente paso",
        }
        if len(nxt) == 1:
            action.update({'view_mode': 'form', 'res_id': nxt.id})
        else:
            action.update({'view_mode': 'list,form',
                           'domain': [('id', 'in', nxt.ids)]})
        return action

    # --- Medición: la actividad ligada a las acciones reales de Odoo ---
    measure_model_id = fields.Many2one(
        'ir.model', string="Modelo que la materializa", ondelete='set null',
        help="Modelo de Odoo cuyos registros son la evidencia de que la "
             "actividad se ejecutó (sale.order para cotizar, mrp.production "
             "para cerrar una orden, quality.check para inspeccionar…).")
    measure_model_name = fields.Char(
        string="Modelo técnico", compute='_compute_measure_model_name',
        inverse='_inverse_measure_model_name', store=True,
        help="Nombre técnico (sale.order, mrp.production…). Escribirlo "
             "resuelve solo el modelo — útil para capturas masivas.")
    measure_domain = fields.Char(
        string="Filtro de evidencia", default='[]',
        help="Dominio sobre el modelo para acotar qué registros cuentan, "
             "ej. [('state', '=', 'done')].")
    measure_date_field = fields.Char(
        string="Campo de fecha", default='create_date',
        help="Campo del modelo que fecha la ejecución (create_date, "
             "date_done, date_approve…). Si no existe, se usa create_date.")
    measure_cadence = fields.Selection([
        ('evento', "Por evento (solo conteo)"),
        ('diaria', "Diaria"),
        ('semanal', "Semanal"),
        ('quincenal', "Quincenal"),
        ('mensual', "Mensual"),
        ('trimestral', "Trimestral"),
        ('semestral', "Semestral"),
        ('anual', "Anual"),
    ], string="Cadencia esperada", default='evento',
        help="Cada cuánto DEBE haber evidencia. «Por evento» solo cuenta, "
             "sin juzgar cumplimiento (actividades que dependen de demanda).")
    measure_last_date = fields.Datetime("Última ejecución", readonly=True)
    measure_count_30d = fields.Integer("Ejecuciones (30 días)", readonly=True)
    measure_state = fields.Selection([
        ('verde', "En cumplimiento"),
        ('rojo', "Sin evidencia en su periodo"),
        ('pendiente', "Pendiente de conector/registro"),
        ('no_aplica', "No se mide"),
    ], string="Cumplimiento", readonly=True)

    # --- Cómo se mide (toda actividad tiene un método) ---
    measure_method = fields.Selection([
        ('odoo', "Registro en Odoo"),
        ('consecuencia', "Por consecuencia"),
        ('correo', "Por correo"),
        ('manual', "Registro manual"),
        ('muestreo', "Por muestreo"),
        ('no_aplica', "No aplica"),
    ], string="Método de medición", index=True,
        help="De mejor a peor. Odoo: deja registro (modelo, dominio, fecha, "
             "usuario). Consecuencia: no deja rastro pero la actividad que la "
             "prueba sí (se copia su conteo). Correo: conector de la fase 2. "
             "Manual: registro de un toque (fase 2). Muestreo: se verifica de "
             "vez en cuando. No aplica: su resultado se mide en otra parte "
             "(exige justificación). Vacío = sin medir, solo con el "
             "procedimiento en borrador.")
    measure_proxy_activity_id = fields.Many2one(
        'sgi.process.activity', string="Se prueba con", ondelete='restrict',
        index=True,
        help="Actividad (de este proceso o de otro) cuya evidencia prueba que "
             "esta se hizo: si se validó la recepción, se descargó el camión.")
    sample_cadence = fields.Selection([
        ('semanal', "Semanal"),
        ('mensual', "Mensual"),
    ], string="Cadencia de muestreo")
    measure_justification = fields.Text(
        string="Por qué no se mide",
        help="Obligatoria con «No aplica»: dónde se mide su resultado.")

    # --- Quién la ejecutó (adelanto de la fase 2) ---
    # TODO(fase 2): measure_user_field pasa a sgi.activity.evidence.user_field
    # (una por fuente de evidencia) y el detalle a sgi.activity.measure.user.
    measure_user_field = fields.Char(
        string="Campo de usuario",
        help="Campo del modelo de evidencia que dice QUÉ USUARIO ejecutó la "
             "actividad (create_uid, user_id…). Con él se mide si la hizo el "
             "puesto que debía.")
    # El detalle por semana, usuario y clase vive en sgi.activity.exec.stat
    # (filtrable, agrupable, graficable); aquí quedan los totales de las
    # últimas 4 semanas que escribe el cron desde ese detalle.
    exec_stat_ids = fields.One2many(
        'sgi.activity.exec.stat', 'activity_id', string="Ejecuciones por semana")
    recent_exec_stat_ids = fields.Many2many(
        'sgi.activity.exec.stat', string="Últimas 4 semanas",
        compute='_compute_recent_exec_stat_ids')
    measure_adherence_pct = fields.Float(
        string="Adherencia (%)", readonly=True, digits=(5, 1),
        aggregator='avg',
        help="Ejecuciones de las últimas 4 semanas hechas por el puesto "
             "asignado, entre todas las que no son del sistema. 0 si no aplica "
             "(rol relativo o sin campo de usuario).")
    measure_top_users = fields.Char(
        string="Quién la ejecuta", readonly=True,
        help="Usuarios con más ejecuciones en las últimas 4 semanas (✓ = puesto asignado).")
    measure_count_generic = fields.Integer(
        string="Por cuenta genérica (4 sem.)", readonly=True,
        help="Ejecuciones con una cuenta compartida (quimibond_sgi.generic_user_ids): "
             "no se pueden atribuir a nadie.")
    measure_count_no_employee = fields.Integer(
        string="Sin empleado (4 sem.)", readonly=True,
        help="Ejecuciones de usuarios sin empleado activo.")
    measure_count_system = fields.Integer(
        string="Del sistema (4 sem.)", readonly=True,
        help="Ejecuciones de OdooBot o procesos automáticos: no cuentan en la "
             "adherencia.")
    measure_count_other_job = fields.Integer(
        string="Por otro puesto (4 sem.)", readonly=True,
        help="Ejecuciones de empleados de un puesto al que no le toca.")
    measure_warning = fields.Text(
        string="Avisos de medición", readonly=True)

    # Ventana de tolerancia por cadencia (días naturales): holgura para fines
    # de semana y cierres sin falsos rojos.
    _SGI_CADENCE_DAYS = {
        'diaria': 2, 'semanal': 9, 'quincenal': 18, 'mensual': 35,
        'trimestral': 100, 'semestral': 190, 'anual': 380,
    }
    # Campos de medición: configurarlos o que el cron los actualice NO es un
    # cambio al cuerpo del procedimiento (no dispara el candado G14).
    # La clase de valor y la automatización tampoco: describen la actividad
    # para mejorarla, no cambian lo que dice el procedimiento.
    _SGI_MEASURE_FIELDS = {
        'measure_model_id', 'measure_model_name', 'measure_domain',
        'measure_date_field', 'measure_cadence', 'measure_last_date',
        'measure_count_30d', 'measure_state', 'value_class',
        'automation_level_target', 'automation_method', 'measure_user_field',
        'measure_adherence_pct', 'measure_top_users',
        'measure_method', 'measure_proxy_activity_id', 'sample_cadence',
        'measure_justification', 'measure_count_generic',
        'measure_count_no_employee', 'measure_count_system',
        'measure_count_other_job', 'measure_warning'}

    @api.depends('measure_model_id')
    def _compute_measure_model_name(self):
        for activity in self:
            activity.measure_model_name = activity.measure_model_id.model

    def _inverse_measure_model_name(self):
        IrModel = self.env['ir.model'].sudo()
        for activity in self:
            name = (activity.measure_model_name or '').strip()
            model = IrModel.search([('model', '=', name)], limit=1) \
                if name and name in self.env else IrModel.browse()
            activity.measure_model_id = model

    def _sgi_measure_domain(self):
        self.ensure_one()
        try:
            domain = safe_eval(self.measure_domain or '[]')
            return domain if isinstance(domain, list) else []
        except Exception:
            return []

    _SGI_EXECUTOR_RESET = {
        'measure_adherence_pct': 0.0,
        'measure_top_users': False, 'measure_count_generic': 0,
        'measure_count_no_employee': 0, 'measure_count_system': 0,
        'measure_count_other_job': 0, 'measure_warning': False,
    }

    def _sgi_measure(self):
        """Mide según el método: Odoo (evidencia real), consecuencia (copia de
        la actividad que la prueba, al final), y los que aún no tienen fuente
        (correo, manual, muestreo) quedan «pendiente»; «no aplica» no se mide.
        Sin método pero con modelo (heredadas) se mide como Odoo."""
        odoo = self.filtered(lambda a: a.measure_method in (False, 'odoo'))
        consequence = self.filtered(lambda a: a.measure_method == 'consecuencia')
        others = self - odoo - consequence
        odoo._sgi_measure_odoo()
        start = self._sgi_exec_window_start()
        for activity in others | consequence:
            activity._sgi_replace_exec_stats(start, [])
        for activity in others:
            vals = dict(self._SGI_EXECUTOR_RESET, measure_last_date=False,
                        measure_count_30d=0,
                        measure_state='no_aplica' if activity.measure_method == 'no_aplica'
                        else 'pendiente')
            activity._sgi_write_if_changed(vals)
        for activity in consequence:
            root = activity._sgi_proxy_root()
            if root not in odoo and root.measure_method in (False, 'odoo') \
                    and root.measure_model_id:
                root._sgi_measure_odoo()
            vals = dict(self._SGI_EXECUTOR_RESET,
                        measure_last_date=root.measure_last_date,
                        measure_count_30d=root.measure_count_30d,
                        measure_state=root.measure_state)
            activity._sgi_write_if_changed(vals)

    def _sgi_write_if_changed(self, vals):
        self.ensure_one()
        # Solo se escribe lo que cambió: el cron diario re-mide TODO y la
        # mayoría de los valores no se mueven.
        if any(self[key] != value for key, value in vals.items()):
            self.write(vals)

    def _sgi_measure_odoo(self):
        """Recalcula la evidencia de cada actividad medible. Una actividad con
        dominio o modelo inválido queda sin semáforo, sin tumbar al resto."""
        now = fields.Datetime.now()
        for activity in self:
            vals = dict(self._SGI_EXECUTOR_RESET, measure_last_date=False,
                        measure_count_30d=0, measure_state=False)
            try:
                model_name = activity.measure_model_id.model
                Model = self.env.get(model_name) if model_name else None
                if Model is None or Model._transient or Model._abstract:
                    activity.write(vals)
                    continue
                Model = Model.sudo()
                date_field = activity.measure_date_field or 'create_date'
                if date_field not in Model._fields:
                    date_field = 'create_date'
                domain = activity._sgi_measure_domain()
                last = Model.search(
                    domain, order='%s desc, id desc' % date_field, limit=1)
                last_date = last and last[date_field] or False
                if last_date and not isinstance(last_date, datetime):
                    last_date = fields.Datetime.to_datetime(last_date)
                vals['measure_last_date'] = last_date
                window = domain + [(date_field, '>=', now - timedelta(days=30))]
                vals['measure_count_30d'] = Model.search_count(window)
                vals.update(activity._sgi_measure_executors(Model, domain, date_field))
                days = self._SGI_CADENCE_DAYS.get(activity.measure_cadence)
                if days:
                    in_window = Model.search_count(
                        domain
                        + [(date_field, '>=', now - timedelta(days=days))])
                    vals['measure_state'] = 'verde' if in_window else 'rojo'
                elif last_date:
                    vals['measure_state'] = 'verde'
            except Exception:
                pass
            # Solo se escribe lo que cambió: el cron diario re-mide TODO y la
            # mayoría de los valores no se mueven — escribir igual infla el
            # write_date y el WAL sin aportar nada.
            if any(activity[key] != value for key, value in vals.items()):
                activity.write(vals)

    @api.model
    def _sgi_generic_user_ids(self):
        """Cuentas compartidas (quimibond_sgi.generic_user_ids, ids separados
        por coma): lo que hacen no se puede atribuir a nadie."""
        raw = self.env['ir.config_parameter'].sudo().get_param(
            'quimibond_sgi.generic_user_ids') or ''
        return {int(x) for x in raw.replace(';', ',').split(',') if x.strip().isdigit()}

    _SGI_EXEC_WEEKS = 4

    @api.model
    def _sgi_exec_window_start(self):
        """Lunes de hace 3 semanas: la ventana de 4 semanas que el cron
        recalcula y reemplaza en sgi.activity.exec.stat."""
        today = fields.Date.context_today(self)
        monday = today - timedelta(days=today.weekday())
        return monday - timedelta(weeks=self._SGI_EXEC_WEEKS - 1)

    def _sgi_replace_exec_stats(self, start, rows):
        """Reemplaza las semanas recalculadas (desde ``start``) con ``rows``;
        si no cambió nada, no escribe."""
        self.ensure_one()
        Stat = self.env['sgi.activity.exec.stat'].sudo()
        current = Stat.search([('activity_id', '=', self.id), ('period_start', '>=', start)])

        def key(r):
            return (r['period_start'], r['user_id'] or False, r['exec_class'] or False,
                    r['employee_id'] or False, r['job_id'] or False,
                    r['family_id'] or False, r['count'])
        before = sorted((key({
            'period_start': s.period_start, 'user_id': s.user_id.id,
            'exec_class': s.exec_class, 'employee_id': s.employee_id.id,
            'job_id': s.job_id.id, 'family_id': s.family_id.id, 'count': s.count,
        }) for s in current), key=str)
        after = sorted((key(r) for r in rows), key=str)
        if before == after:
            return
        current.unlink()
        if rows:
            Stat.create([dict(r, activity_id=self.id) for r in rows])

    def _sgi_measure_executors(self, Model, domain, date_field):
        """Quién ejecutó la actividad en las últimas 4 semanas: un read_group
        por campo de usuario y semana (sin recorrer registros) y, por usuario,
        su empleado, puesto y familia al momento de medir. Cada ejecución cae
        en una clase: correcto (su puesto está entre los que ejecutan,
        familias incluidas), otro_puesto, generico (cuenta compartida),
        sin_empleado o sistema (OdooBot). El detalle va a
        sgi.activity.exec.stat (una fila por semana, usuario y clase) y de ahí
        salen la adherencia (correcto entre todo lo que no es sistema), los
        contadores y los avisos. Con un ejecutor relativo sin puesto no hay
        clase ni adherencia: solo el conteo."""
        self.ensure_one()
        start = self._sgi_exec_window_start()
        user_field = (self.measure_user_field or '').strip()
        field = Model._fields.get(user_field) if user_field else None
        if not field or field.type != 'many2one' or field.comodel_name != 'res.users' \
                or not field.store:
            self._sgi_replace_exec_stats(start, [])
            return {}
        since = start if Model._fields[date_field].type == 'date' \
            else datetime.combine(start, datetime.min.time())
        groups = Model._read_group(
            domain + [(date_field, '>=', since)],
            [user_field, '%s:week' % date_field], ['__count'])
        expected = self._sgi_executor_jobs()
        generic_ids = self._sgi_generic_user_ids()
        system_ids = {SUPERUSER_ID}
        root = self.env.ref('base.user_root', raise_if_not_found=False)
        if root:
            system_ids.add(root.id)
        company = self.company_id or self.env.company
        user_ids = list({u.id for u, _w, _c in groups if u})
        employees = self.env['hr.employee'].sudo().search([
            ('user_id', 'in', user_ids), ('company_id', '=', company.id)])
        emp_by_user = {emp.user_id.id: emp for emp in employees}
        merged = {}
        for user, week, count in groups:
            emp = emp_by_user.get(user.id)
            job = emp.job_id if emp else self.env['hr.job']
            if not user or user.id in system_ids:
                klass = 'sistema'
            elif user.id in generic_ids:
                klass = 'generico'
            elif not emp:
                klass = 'sin_empleado'
            elif expected is not None and job and job in expected:
                klass = 'correcto'
            else:
                klass = 'otro_puesto'
            week = week.date() if isinstance(week, datetime) else week
            row_key = (week, user.id or False, klass)
            if row_key in merged:
                merged[row_key]['count'] += count
                continue
            merged[row_key] = {
                'period_start': week,
                'user_id': user.id or False,
                'employee_id': emp.id if emp else False,
                'job_id': job.id or False,
                'family_id': job.sgi_family_id.id or False,
                # Ejecutor relativo (solicitante, quien detecta…): no hay a
                # quién comparar; solo se cuenta.
                'exec_class': False if expected is None else klass,
                'count': count,
                '_class': klass,
            }
        rows = list(merged.values())
        counts = dict.fromkeys(
            ('correcto', 'otro_puesto', 'generico', 'sin_empleado', 'sistema'), 0)
        per_user = {}
        for row in rows:
            counts[row['_class']] += row['count']
            per_user.setdefault(row['user_id'], [0, row['exec_class']])
            per_user[row['user_id']][0] += row['count']
        self._sgi_replace_exec_stats(start, [
            {k: v for k, v in row.items() if k != '_class'} for row in rows])
        if not rows:
            return {}
        total = sum(counts.values())
        attributable = total - counts['sistema']
        adherence = round(counts['correcto'] * 100.0 / attributable, 1) \
            if expected is not None and attributable else 0.0
        warnings = []
        if expected is not None and attributable and adherence < 80:
            warnings.append("Adherencia de %.0f%%: la hacen puestos que no la tienen "
                            "asignada." % adherence)
        if counts['generico'] or counts['sin_empleado']:
            warnings.append(
                "%d ejecución(es) con cuenta genérica y %d sin empleado activo: no "
                "hay forma de saber quién hizo el movimiento." % (
                    counts['generico'], counts['sin_empleado']))
        if self.automation_level_current == 'manual' and total \
                and counts['sistema'] * 2 > total:
            warnings.append("Parece automática (%d de %d ejecuciones son del "
                            "sistema): revisar nivel de automatización." % (
                                counts['sistema'], total))
        Users = self.env['res.users'].sudo()
        top = ', '.join("%s%s (%d)" % (
            Users.browse(user_id).name if user_id else "Sin usuario",
            " ✓" if klass == 'correcto' else "", count)
            for user_id, (count, klass) in sorted(
                per_user.items(), key=lambda kv: -kv[1][0])[:3])
        return {
            'measure_adherence_pct': adherence,
            'measure_top_users': top,
            'measure_count_generic': counts['generico'],
            'measure_count_no_employee': counts['sin_empleado'],
            'measure_count_system': counts['sistema'],
            'measure_count_other_job': counts['otro_puesto'] if expected is not None else 0,
            'measure_warning': '\n'.join(warnings) or False,
        }

    def _sgi_resolve_menu(self):
        """Resuelve odoo_menu_id desde el texto de odoo_ref: convierte
        «Compras → Órdenes de compra» en el menú real. Solo menús con acción.
        Con varias rutas separadas por «·» se usa la primera."""
        from .sgi_base import sgi_find_menu
        for activity in self:
            ref = (activity.odoo_ref or '').split('·')[0].strip()
            if not ref or activity.odoo_menu_id:
                continue
            path = '/'.join(p.strip() for p in ref.replace('→', '/')
                            .replace('>', '/').split('/') if p.strip())
            menu = sgi_find_menu(self.env, path)
            if menu:
                activity.odoo_menu_id = menu

    @api.model
    def cron_measure_activities(self):
        # Primero intenta resolver menús pendientes desde su texto; después
        # mide, y con la medición fresca evalúa el flujo de la cadena. Cada
        # paso es independiente: un tropiezo en uno no debe dejar sin medir
        # a los demás (ya pasó en producción con el aviso de eslabón).
        steps = (
            lambda: self.search([('odoo_menu_id', '=', False),
                                 ('odoo_ref', '!=', False)])._sgi_resolve_menu(),
            # Los «Formularios de Odoo» del control documental también
            # resuelven su menú desde el texto del destino de migración.
            lambda: self.env['documents.document'].search([
                ('sgi_doc_type', '=', 'formulario_odoo'),
                ('sgi_odoo_menu_id', '=', False),
                ('sgi_migration_target', '!=', False),
            ]).action_sgi_resolve_odoo_menu(),
            lambda: self.search(
                ['|', ('measure_model_id', '!=', False),
                 ('measure_method', '!=', False)])._sgi_measure(),
            lambda: self.env['sgi.activity.link'].search(
                [])._sgi_evaluate_chain(),
        )
        for step in steps:
            try:
                step()
            except Exception:
                _logger.exception(
                    "SGI: falló un paso del cron de medición; continúo.")
        return True

    def _sgi_checked_measure_domain(self):
        """Dominio de evidencia VALIDADO contra el modelo real. El dominio lo
        captura una persona: uno con un campo inexistente o no almacenado pasa
        el safe_eval y truena hasta la vista del usuario («Cannot convert to
        SQL» — misma familia del bug de complete_name). El cron ya se protege
        con try/except; aquí se valida ANTES de devolver la acción, con un
        error accionable en vez de un traceback."""
        self.ensure_one()
        domain = self._sgi_measure_domain()
        try:
            self.env[self.measure_model_id.model].sudo().search_count(domain, limit=1)
        except Exception as exc:
            raise UserError(
                "El «Filtro de evidencia» de la actividad %s es inválido para "
                "el modelo %s:\n%s\n\nCorrige el dominio en la pestaña de "
                "medición (solo campos reales y almacenados del modelo)." % (
                    self.display_name, self.measure_model_id.model, exc))
        return domain

    def action_view_measure_records(self):
        """Abre los registros reales que son la evidencia de la actividad."""
        self.ensure_one()
        if not self.measure_model_id:
            raise UserError(
                "Esta actividad no tiene modelo de medición ligado.")
        domain = self._sgi_checked_measure_domain()
        return {
            'type': 'ir.actions.act_window',
            'name': "%s — evidencia" % (
                self.name or self.number or self.section or 'Actividad'),
            'res_model': self.measure_model_id.model,
            'view_mode': 'list,form',
            'domain': domain,
        }

    @api.depends('number', 'name', 'section')
    def _compute_display_name(self):
        for activity in self:
            label = activity.name or activity.section or ''
            activity.display_name = (
                "%s %s" % (activity.number, label)).strip() if activity.number else label

    @api.onchange('odoo_menu_id')
    def _onchange_odoo_menu_id(self):
        """Si hay menú y el texto está vacío, toma la ruta legible del menú."""
        if self.odoo_menu_id and not self.odoo_ref:
            self.odoo_ref = self.odoo_menu_id.complete_name

    def action_open_odoo(self):
        """Abre el menú real de Odoo donde se ejecuta la actividad, respetando
        el dominio/contexto/vistas de su acción original. Sin menú ligado
        intenta resolverlo del texto y, si tampoco, abre la evidencia — el
        paso siempre es navegable, nunca texto plano."""
        self.ensure_one()
        if not self.odoo_menu_id and self.odoo_ref:
            self._sgi_resolve_menu()
        action = self.odoo_menu_id.action if self.odoo_menu_id else False
        if action and action._name == 'ir.actions.act_window':
            return action.read()[0]
        if self.measure_model_id:
            return self.action_view_measure_records()
        raise UserError("Esta actividad no tiene menú de Odoo ni medición ligada.")

    @api.model_create_multi
    def create(self, vals_list):
        # Numeral y sección llegan como texto desde código viejo o la carga:
        # se traducen a paso y etapa; sin paso, el siguiente libre.
        vals_list = [self._sgi_structure_vals(vals) for vals in vals_list]
        taken = {}
        for vals in vals_list:
            if not vals.get('step') and vals.get('process_id'):
                process = self.env['sgi.process'].browse(vals['process_id'])
                vals['step'] = self._sgi_next_step(process, taken.get(process.id, ()))
            if vals.get('step') and vals.get('process_id'):
                taken.setdefault(vals['process_id'], []).append(vals['step'])
        # Los roles que vienen en el alta se validan juntos al final, con la
        # restricción de la actividad.
        records = super(SgiProcessActivity, self.with_context(
            sgi_roles_via_activity=True)).create(vals_list).with_env(self.env)
        # Una actividad nueva sin método no entra a un procedimiento en
        # piloto o vigente (la restricción solo corre con el campo presente).
        records._sgi_check_measure_strict()
        records.process_id._sgi_flag_procedure_dirty()
        return records

    def write(self, vals):
        if 'number' in vals or 'section' in vals:
            if len(self.process_id) > 1:
                raise UserError("Cambia el numeral o la sección de una actividad a la vez.")
            vals = self._sgi_structure_vals(vals, self.process_id)
        # Los roles que llegan por el one2many se validan juntos al final
        # (restricción de la actividad), no uno por uno a medio camino.
        records = self.with_context(sgi_roles_via_activity=True) \
            if 'role_ids' in vals or 'responsible_job_ids' in vals else self
        res = super(SgiProcessActivity, records).write(vals)
        # La medición (configuración o refresco del cron) no es un cambio al
        # cuerpo del procedimiento: no dispara revisión documental (G14).
        if set(vals) - self._SGI_MEASURE_FIELDS:
            self.process_id._sgi_flag_procedure_dirty()
        return res

    def unlink(self):
        processes = self.process_id
        res = super().unlink()
        processes._sgi_flag_procedure_dirty()
        return res


class SgiActivityLink(models.Model):
    """Liga entre dos actividades de procedimiento: qué ENTREGABLE pasa de un
    paso al siguiente. Puede cruzar procesos (el pedido de Ventas alimenta el
    programa de Planeación): es el hilo conductor de la operación al nivel de
    paso, no solo entre procesos (sgi.process.flow)."""
    _name = 'sgi.activity.link'
    _description = "Encadenamiento entre actividades"
    _order = 'from_activity_id, id'

    from_activity_id = fields.Many2one(
        'sgi.process.activity', string="Actividad origen", required=True,
        ondelete='cascade', index=True)
    to_activity_id = fields.Many2one(
        'sgi.process.activity', string="Actividad destino", required=True,
        ondelete='cascade', index=True)
    name = fields.Char(
        string="Entregable / condición", required=True,
        help="Qué pasa de un paso al otro: el pedido confirmado, el programa "
             "semanal, el lote liberado…")
    from_process_id = fields.Many2one(
        related='from_activity_id.process_id', string="Proceso origen",
        store=True)
    to_process_id = fields.Many2one(
        related='to_activity_id.process_id', string="Proceso destino",
        store=True)
    company_id = fields.Many2one(
        related='from_activity_id.company_id', string="Empresa", store=True,
        index=True)
    is_cross_process = fields.Boolean(
        string="Cruza procesos", compute='_compute_cross', store=True)

    # --- Flujo del eslabón (fase 3): ¿el paso siguiente sigue al anterior? ---
    chain_state = fields.Selection([
        ('fluye', "Fluye"),
        ('atorado', "Atorado"),
    ], string="Flujo", readonly=True,
        help="Fluye: ambos pasos con evidencia en su periodo. Atorado: el paso "
             "origen tiene evidencia pero el destino no — el entregable entró "
             "y no salió. Lo calcula el cron de medición.")
    lag_days = fields.Float(
        string="Rezago (días)", readonly=True, digits=(6, 1),
        help="Días que la última evidencia del paso destino va detrás de la "
             "del paso origen. 0 = el eslabón está al día.")
    atorado_since = fields.Datetime("Atorado desde", readonly=True)
    nc_alert_id = fields.Many2one(
        'quality.alert', string="NC generada", readonly=True, copy=False)

    # Campos que escribe el cron: no son contenido del procedimiento.
    _SGI_MEASURE_FIELDS = {
        'chain_state', 'lag_days', 'atorado_since', 'nc_alert_id'}

    _SGI_NC_AFTER_DAYS = 7

    @api.depends('from_process_id', 'to_process_id')
    def _compute_cross(self):
        for link in self:
            link.is_cross_process = (
                link.from_process_id != link.to_process_id)

    def _sgi_evaluate_chain(self):
        """Evalúa cada eslabón tras la medición: estado, rezago, aviso al
        dueño al atorarse y NC automática si persiste."""
        now = fields.Datetime.now()
        Cron = self.env['sgi.cron']
        for link in self:
            frm, to = link.from_activity_id, link.to_activity_id
            vals = {'chain_state': False, 'lag_days': 0.0}
            if frm.measure_state == 'verde' and to.measure_state == 'rojo':
                vals['chain_state'] = 'atorado'
            elif frm.measure_state and to.measure_state:
                vals['chain_state'] = 'fluye'
            if frm.measure_last_date and to.measure_last_date \
                    and frm.measure_last_date > to.measure_last_date:
                vals['lag_days'] = round(
                    (frm.measure_last_date - to.measure_last_date
                     ).total_seconds() / 86400.0, 1)
            if vals['chain_state'] == 'atorado':
                since = link.atorado_since or now
                vals['atorado_since'] = since
                owner_user = (to.process_id.owner_id.user_id.id
                              or Cron._sgi_manager_user_id())
                if not link.atorado_since:
                    Cron._sgi_schedule(
                        to.process_id,
                        "Eslabón atorado: %s" % (link.name or ''),
                        "«%s» entregó (%s) pero «%s» no tiene evidencia en su "
                        "periodo. Revise el paso o su medición." % (
                            frm.display_name, link.name or '',
                            to.display_name),
                        owner_user)
                elif (now - link.atorado_since).days >= self._SGI_NC_AFTER_DAYS \
                        and not link._sgi_chain_nc_open():
                    # Sin NC ligada, o la ligada ya cerró/canceló: este
                    # atoramiento persistente amerita su propia NC (antes,
                    # nc_alert_id nunca se soltaba y el SEGUNDO episodio ya no
                    # generaba NC jamás).
                    alert = link._sgi_create_chain_nc()
                    if alert:
                        vals['nc_alert_id'] = alert.id
            else:
                vals['atorado_since'] = False
            # Solo-diferencia: el cron evalúa todos los eslabones a diario y
            # casi siempre nada cambió (un UPDATE por link por día, de balde).
            if any(
                    (link[key].id if key == 'nc_alert_id' else link[key]) != value
                    for key, value in vals.items()):
                link.write(vals)

    def _sgi_chain_nc_open(self):
        """¿La NC ligada al eslabón sigue abierta (bloquea crear otra)?"""
        self.ensure_one()
        nc = self.nc_alert_id
        return bool(nc) and not (nc.stage_id.sgi_is_closing_stage
                                 or nc.stage_id.sgi_is_cancel_stage)

    def _sgi_create_chain_nc(self):
        """NC automática por ruptura de secuencia persistente.

        Va por sgi_auto_create (punto ÚNICO de entrada de NCs automáticas):
        así MAST puede apagar la fuente «eslabon_atorado» desde Configuración
        y la NC queda estampada con su sgi_source_id — antes se creaba con
        create() directo y no había forma de apagarla sin tocar código.
        Devuelve un recordset vacío si la fuente está apagada o si no está
        configurado el equipo NC Internas (una NC sin equipo queda sin folio,
        fuera de los candados del SGI: mejor no crearla y avisar al log)."""
        self.ensure_one()
        team = self.env.ref('quimibond_sgi.sgi_quality_team_internal',
                            raise_if_not_found=False)
        if not team:
            _logger.warning(
                "SGI: eslabón atorado %s sin NC — falta el equipo NC Internas "
                "(quimibond_sgi.sgi_quality_team_internal).", self.id)
            return self.env['quality.alert']
        stage = self.env.ref('quimibond_sgi.sgi_nc_int_stage_open',
                             raise_if_not_found=False)
        vals = {
            'title': "Ruptura de secuencia: %s" % (self.name or ''),
            'description':
                "El procedimiento se rompió en este eslabón por más de %d "
                "días: «%s» tiene evidencia pero «%s» no ejecutó su paso "
                "(%s). Detectado por la medición de la cadena." % (
                    self._SGI_NC_AFTER_DAYS,
                    self.from_activity_id.display_name,
                    self.to_activity_id.display_name, self.name or ''),
            'sgi_origin_type': 'proceso',
            'sgi_process_id': self.to_process_id.id,
            'team_id': team.id,
        }
        if stage:
            vals['stage_id'] = stage.id
        # count_suppression=False: el cron reevalúa el mismo eslabón cada día;
        # una omisión ya contada no es un evento nuevo.
        return self.env['quality.alert'].sudo().sgi_auto_create(
            'eslabon_atorado', vals, count_suppression=False)

    @api.constrains('from_activity_id', 'to_activity_id')
    def _check_not_self(self):
        for link in self:
            if link.from_activity_id == link.to_activity_id:
                raise ValidationError(
                    "Una actividad no puede encadenarse consigo misma.")

    @api.model_create_multi
    def create(self, vals_list):
        records = super().create(vals_list)
        (records.from_activity_id.process_id
         | records.to_activity_id.process_id)._sgi_flag_procedure_dirty()
        return records

    def write(self, vals):
        res = super().write(vals)
        # Lo que escribe el cron (flujo/rezago) no es contenido documental.
        if set(vals) - self._SGI_MEASURE_FIELDS:
            (self.from_activity_id.process_id
             | self.to_activity_id.process_id)._sgi_flag_procedure_dirty()
        return res

    def unlink(self):
        processes = (self.from_activity_id.process_id
                     | self.to_activity_id.process_id)
        res = super().unlink()
        processes._sgi_flag_procedure_dirty()
        return res
