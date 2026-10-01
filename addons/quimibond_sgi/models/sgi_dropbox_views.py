# -*- coding: utf-8 -*-
"""«Del Dropbox a Odoo»: buscador por clave anterior y avance de la
transición (19.0.57.0.0; L-006, L-009, L-019; docs/audit/12-transicion.md
§3.4 y §3.6).

Los dos son de solo lectura sobre SQL (``_table_query``, como
``sgi.competence.gap``). RIESGO conocido: los nombres de columna salen del
código de los modelos (``documents.document``: sgi_previous_code,
sgi_doc_type_id, sgi_title, name, sgi_process_id, sgi_replaced_by_process_id,
sgi_migration_state/_class, sgi_odoo_menu_id, sgi_migration_point_id,
sgi_parent_document_id, sgi_state, sgi_is_controlled, active;
``sgi_document_type.code``; ``sgi_legacy_routine``: procedure_id,
procedure_code, n, name, state, process_id, active;
``sgi_legacy_routine_activity_rel (routine_id, activity_id)``;
``sgi_process_activity``: process_id, legacy_number, name, related_procedure_id,
odoo_menu_id, active; ``sgi_process``: code, company_id, active) y no se pueden
probar fuera del build. tests/test_dropbox_key.py los ejercita.
"""
import re

from odoo import api, fields, models
from odoo.exceptions import AccessError, UserError

from .sgi_legacy_routine import ROUTINE_STATES, sgi_can_read_legacy_routines

KEY_KINDS = [
    ('procedimiento', "Procedimiento"),
    ('formato', "Formato"),
    ('formato_it', "Formato de instructivo"),
    ('formulario_odoo', "Formulario de Odoo"),
    ('instructivo', "Instructivo"),
    ('dat', "DAT"),
    ('anexo', "Anexo"),
    ('protocolo', "Protocolo"),
    ('reglamento', "Reglamento"),
    ('miid', "Manual"),
    ('diagrama', "Diagrama"),
    ('control_operacional', "Control operacional"),
    ('metodo_anexo', "Método / anexo"),
    ('descripcion_puesto', "Descripción de puesto"),
    ('rutina', "Rutina"),
    ('numeral_archivado', "Numeral archivado"),
]
# Dominio de «Procedimientos anteriores» (igual que sgi_dropbox_procedure_action).
PROCEDURE_DOMAIN = [('sgi_is_controlled', '=', True), ('sgi_doc_type', '=', 'procedimiento'),
                    ('sgi_legacy_family', 'not in', ['P-I01'])]
MIGRATION_STATES = [
    ('pendiente', "Pendiente"),
    ('en_curso', "En curso"),
    ('migrado', "Migrado a Odoo"),
    ('baja', "Baja tramitada"),
    ('na', "No aplica (se queda)"),
]


def _sql_codes(codes):
    """Lista SQL literal de claves saneadas (solo A-Z, 0-9, guion y espacio)."""
    clean = [re.sub(r'[^A-Z0-9 -]', '', (c or '').upper()) for c in codes]
    return ', '.join("'%s'" % c for c in clean if c) or "''"


class SgiDropboxKey(models.Model):
    """Buscador por clave anterior: cada renglón es una clave vieja y dice qué
    es hoy y dónde vive."""
    _name = 'sgi.dropbox.key'
    _description = "Clave anterior del Dropbox"
    _auto = False
    _order = 'key, id'
    _rec_name = 'key'

    key = fields.Char(string="Clave anterior", readonly=True)
    kind = fields.Selection(KEY_KINDS, string="Qué es", readonly=True)
    title = fields.Char(string="Título", readonly=True)
    document_id = fields.Many2one('documents.document', string="Documento", readonly=True)
    routine_id = fields.Many2one('sgi.legacy.routine', string="Rutina", readonly=True)
    activity_id = fields.Many2one('sgi.process.activity', string="Actividad archivada", readonly=True)
    process_id = fields.Many2one('sgi.process', string="Proceso", readonly=True)
    migration_state = fields.Selection(MIGRATION_STATES, string="Estado de migración", readonly=True)
    routine_state = fields.Selection(ROUTINE_STATES, string="Estado de la rutina", readonly=True)
    doc_state = fields.Selection([
        ('borrador', "Borrador"), ('piloto', "Prueba piloto"),
        ('vigente', "Vigente"), ('obsoleto', "Obsoleto"),
    ], string="Estado del documento", readonly=True)
    odoo_menu_id = fields.Many2one('ir.ui.menu', string="Menú de Odoo", readonly=True)
    point_id = fields.Many2one('quality.point', string="Worksheet", readonly=True)
    activity_numbers = fields.Char(string="Numerales", readonly=True)
    destination = fields.Char(string="Dónde vive en Odoo", compute='_compute_destination')

    def _sgi_key_filters(self):
        """(filtro de documentos, filtro de procedimiento de la rutina, filtro
        del procedimiento del numeral) en SQL. L-006/P-L6: nadie ve un
        documento que no puede abrir (carpeta Dirección, P-I01): se calcula
        con el usuario real, porque una regla ``any`` se evalúa en modo
        sistema y no filtraría. Las claves excluidas no salen nunca.

        57.0.0 (decisión de Jose): las rutinas solo salen para Auditor, Jefe
        MAST, Dirección y dueños de proceso; el Usuario SGI normal busca
        procedimientos, formatos, instructivos y numerales, sin rutinas."""
        excluded = _sql_codes(self.env['documents.document']._sgi_dropbox_excluded_codes())
        doc_filter = "d.sgi_previous_code NOT IN (%s)" % excluded
        routine_filter = "coalesce(r.procedure_code, '') NOT IN (%s)" % excluded
        numeral_filter = "TRUE"
        if not sgi_can_read_legacy_routines(self.env):
            routine_filter = "FALSE"
        if not self.env.su:
            Doc = self.env['documents.document'].with_context(active_test=False)
            try:
                Doc.check_access('read')
                ids = Doc.search([('sgi_is_controlled', '=', True)]).ids
            except AccessError:
                ids = []
            id_list = ', '.join(str(int(i)) for i in ids) or 'NULL'
            doc_filter += " AND d.id IN (%s)" % id_list
            routine_filter += " AND r.procedure_id IN (%s)" % id_list
            numeral_filter = "(a.related_procedure_id IS NULL OR a.related_procedure_id IN (%s))" % id_list
        return doc_filter, routine_filter, numeral_filter

    @property
    def _table_query(self):
        doc_filter, routine_filter, numeral_filter = self._sgi_key_filters()
        return """
            SELECT d.id * 10 + 1 AS id,
                   d.sgi_previous_code AS key,
                   t.code AS kind,
                   coalesce(d.sgi_title, d.name->>'es_MX', d.name->>'en_US')::varchar AS title,
                   d.id AS document_id,
                   NULL::integer AS routine_id,
                   NULL::integer AS activity_id,
                   coalesce(d.sgi_replaced_by_process_id, d.sgi_process_id) AS process_id,
                   d.sgi_migration_state AS migration_state,
                   NULL::varchar AS routine_state,
                   d.sgi_state AS doc_state,
                   d.sgi_odoo_menu_id AS odoo_menu_id,
                   d.sgi_migration_point_id AS point_id,
                   NULL::varchar AS activity_numbers
              FROM documents_document d
              JOIN sgi_document_type t ON t.id = d.sgi_doc_type_id
             WHERE d.sgi_is_controlled IS TRUE AND d.active IS TRUE
               AND d.sgi_previous_code IS NOT NULL
               AND t.code NOT IN ('mi_procedimiento', 'externo')
               AND %(doc)s
            UNION ALL
            SELECT r.id * 10 + 2,
                   r.procedure_code || ' · ' || r.n,
                   'rutina',
                   r.name,
                   r.procedure_id,
                   r.id,
                   NULL::integer,
                   r.process_id,
                   NULL::varchar,
                   r.state,
                   NULL::varchar,
                   NULL::integer,
                   NULL::integer,
                   r.activity_numbers
              FROM sgi_legacy_routine r
             WHERE r.active IS TRUE AND %(routine)s
            UNION ALL
            SELECT a.id * 10 + 3,
                   p.code || ' ' || a.legacy_number,
                   'numeral_archivado',
                   a.name,
                   a.related_procedure_id,
                   NULL::integer,
                   a.id,
                   a.process_id,
                   NULL::varchar,
                   NULL::varchar,
                   NULL::varchar,
                   a.odoo_menu_id,
                   NULL::integer,
                   NULL::varchar
              FROM sgi_process_activity a
              JOIN sgi_process p ON p.id = a.process_id
             WHERE a.active IS NOT TRUE AND a.legacy_number IS NOT NULL AND %(numeral)s
        """ % {'doc': doc_filter, 'routine': routine_filter, 'numeral': numeral_filter}

    @api.model
    def _search(self, *args, **kwargs):
        """Solo lectura sobre SQL: flush antes de leer (la vista no sabe de
        los writes pendientes) y la caché fuera (como qb.sql.view)."""
        self.env.flush_all()
        self.invalidate_model()
        return super()._search(*args, **kwargs)

    @api.depends('odoo_menu_id', 'point_id', 'document_id', 'activity_numbers', 'kind')
    def _compute_destination(self):
        for row in self:
            if row.kind == 'rutina':
                row.destination = row.activity_numbers or dict(ROUTINE_STATES).get(row.routine_state)
            elif row.document_id:
                row.destination = row.document_id.sgi_destination_label or (
                    row.process_id.display_name if row.kind == 'procedimiento' else False)
            elif row.odoo_menu_id:
                row.destination = row.odoo_menu_id.sudo().complete_name
            else:
                row.destination = False

    def action_open_target(self):
        """«Abrir en Odoo»: menú → su acción; worksheet → el punto de calidad;
        rutina → sus actividades; procedimiento → el proceso que lo sustituye
        (o su proceso actual)."""
        self.ensure_one()
        if self.kind == 'rutina' and self.routine_id:
            if not self.routine_id.with_context(active_test=False).activity_ids:
                raise UserError("Esta rutina todavía no la cubre ninguna actividad de Odoo.")
            return self.routine_id.action_open_activities()
        if self.document_id and (self.document_id.sgi_odoo_menu_id
                                 or self.document_id.sgi_migration_point_id):
            return self.document_id.action_sgi_open_odoo_form()
        action = self.odoo_menu_id.sudo().action if self.odoo_menu_id else False
        if action and action._name == 'ir.actions.act_window':
            return action.read()[0]
        if self.point_id and self.document_id:
            return self.document_id.action_sgi_open_migration_point()
        if self.kind == 'procedimiento' and self.process_id:
            return {'type': 'ir.actions.act_window', 'res_model': 'sgi.process',
                    'res_id': self.process_id.id, 'view_mode': 'form',
                    'name': self.process_id.display_name}
        raise UserError("Todavía no tiene destino en Odoo.")

    def action_open_previous(self):
        """«Ver el anterior»: el documento (o la actividad archivada)."""
        self.ensure_one()
        if self.kind == 'numeral_archivado' and self.activity_id:
            return {'type': 'ir.actions.act_window', 'res_model': 'sgi.process.activity',
                    'res_id': self.activity_id.id, 'view_mode': 'form',
                    'context': {'active_test': False}}
        if not self.document_id:
            raise UserError("No hay documento anterior que abrir.")
        # La ficha de procedimiento anterior trae las rutinas: solo para
        # quien las puede leer (57.0.0); el resto ve la ficha del documento.
        view = 'sgi_dropbox_procedure_view_form' if (
            self.document_id.sgi_doc_type == 'procedimiento'
            and sgi_can_read_legacy_routines(self.env)) else 'sgi_dropbox_document_view_form'
        return {'type': 'ir.actions.act_window', 'res_model': 'documents.document',
                'res_id': self.document_id.id, 'view_mode': 'form',
                'views': [(self.env.ref('quimibond_sgi.%s' % view).id, 'form')]}


class SgiDropboxProgress(models.Model):
    """Avance de la transición: un renglón por proceso activo."""
    _name = 'sgi.dropbox.progress'
    _description = "Avance de la transición del Dropbox"
    _auto = False
    _order = 'process_code, id'
    _rec_name = 'process_id'

    process_id = fields.Many2one('sgi.process', string="Proceso", readonly=True)
    process_code = fields.Char(string="Clave", readonly=True)
    company_id = fields.Many2one('res.company', string="Empresa", readonly=True)
    procedures_replaced = fields.Integer(string="Procedimientos sustituidos", readonly=True)
    procedures_pending = fields.Integer(string="Procedimientos con pendientes", readonly=True)
    procedures_control = fields.Integer(string="Control operacional", readonly=True)
    routines_total = fields.Integer(string="Rutinas", readonly=True)
    routines_covered = fields.Integer(string="Cubiertas", readonly=True)
    routines_replaced = fields.Integer(string="La hace Odoo", readonly=True)
    routines_eliminated = fields.Integer(string="Eliminadas", readonly=True)
    routines_pending = fields.Integer(string="Rutinas pendientes", readonly=True)
    routines_undecided = fields.Integer(string="Pendientes sin decisión", readonly=True)
    routines_resolved_pct = fields.Float(string="% rutinas resuelto", readonly=True, aggregator='avg')
    docs_total = fields.Integer(string="Documentos", readonly=True)
    docs_migrated = fields.Integer(string="Migrados con liga", readonly=True)
    docs_paper = fields.Integer(string="Siguen como documento", readonly=True)
    docs_pending = fields.Integer(string="Documentos pendientes", readonly=True)
    docs_incomplete = fields.Integer(string="Documentos incompletos", readonly=True)
    activities_new = fields.Integer(string="Actividades nuevas (sin antecedente)", readonly=True)
    progress_pct = fields.Float(string="% migrado", readonly=True, aggregator='avg')

    @property
    def _table_query(self):
        excluded = _sql_codes(self.env['documents.document']._sgi_dropbox_excluded_codes())
        # 57.84.0: los procedimientos «No aplica (se queda)» pasaron a control
        # operacional (D-02); siguen contando como documento del Dropbox.
        dropbox_types = ("'formato', 'formato_it', 'formulario_odoo', 'instructivo', 'dat', 'anexo', "
                         "'protocolo', 'reglamento', 'miid', 'diagrama', 'control_operacional'")
        return """
            WITH proc AS (
                SELECT d.id, d.sgi_process_id, d.sgi_replaced_by_process_id, d.sgi_migration_state
                  FROM documents_document d
                  JOIN sgi_document_type t ON t.id = d.sgi_doc_type_id
                 WHERE t.code = 'procedimiento' AND d.sgi_is_controlled IS TRUE AND d.active IS TRUE
                   AND coalesce(d.sgi_previous_code, d.sgi_code, '') NOT IN (%(excluded)s)
            ), doc AS (
                SELECT d.id, d.sgi_process_id, d.sgi_migration_state AS st, d.sgi_migration_class AS cls,
                       (d.sgi_odoo_menu_id IS NOT NULL OR d.sgi_migration_point_id IS NOT NULL) AS linked
                  FROM documents_document d
                  JOIN sgi_document_type t ON t.id = d.sgi_doc_type_id
                 WHERE t.code IN (%(types)s) AND d.sgi_is_controlled IS TRUE AND d.active IS TRUE
                   AND coalesce(d.sgi_state, '') <> 'obsoleto'
            ), rut AS (
                SELECT r.process_id, r.state, r.decision
                  FROM sgi_legacy_routine r
                 WHERE r.active IS TRUE
            ), agg AS (
                SELECT p.id AS process_id, p.code AS process_code, p.company_id,
                       (SELECT count(*) FROM proc WHERE proc.sgi_replaced_by_process_id = p.id)
                           AS procedures_replaced,
                       (SELECT count(*) FROM proc WHERE proc.sgi_replaced_by_process_id IS NULL
                           AND proc.sgi_process_id = p.id AND proc.sgi_migration_state = 'en_curso')
                           AS procedures_pending,
                       (SELECT count(*) FROM proc WHERE proc.sgi_replaced_by_process_id IS NULL
                           AND proc.sgi_process_id = p.id AND proc.sgi_migration_state = 'na')
                           AS procedures_control,
                       (SELECT count(*) FROM rut WHERE rut.process_id = p.id) AS routines_total,
                       (SELECT count(*) FROM rut WHERE rut.process_id = p.id AND rut.state = 'cubierta')
                           AS routines_covered,
                       (SELECT count(*) FROM rut WHERE rut.process_id = p.id AND rut.state = 'reemplazada')
                           AS routines_replaced,
                       (SELECT count(*) FROM rut WHERE rut.process_id = p.id AND rut.state = 'eliminada')
                           AS routines_eliminated,
                       (SELECT count(*) FROM rut WHERE rut.process_id = p.id AND rut.state = 'pendiente')
                           AS routines_pending,
                       (SELECT count(*) FROM rut WHERE rut.process_id = p.id AND rut.state = 'pendiente'
                           AND rut.decision IS NULL) AS routines_undecided,
                       (SELECT count(*) FROM doc WHERE doc.sgi_process_id = p.id) AS docs_total,
                       (SELECT count(*) FROM doc WHERE doc.sgi_process_id = p.id
                           AND doc.st IN ('migrado', 'baja') AND doc.linked) AS docs_migrated,
                       (SELECT count(*) FROM doc WHERE doc.sgi_process_id = p.id
                           AND (doc.cls = 'd' OR doc.st = 'na')) AS docs_paper,
                       (SELECT count(*) FROM doc WHERE doc.sgi_process_id = p.id
                           AND doc.st IN ('pendiente', 'en_curso')) AS docs_pending,
                       (SELECT count(*) FROM doc WHERE doc.sgi_process_id = p.id
                           AND (coalesce(doc.cls, 'x') = 'x'
                                OR (doc.st IN ('migrado', 'en_curso') AND doc.cls IN ('a', 'b')
                                    AND NOT doc.linked)
                                OR (doc.cls = 'd' AND doc.st <> 'na')
                                OR (doc.cls IN ('a', 'b', 'c') AND doc.st = 'na'))) AS docs_incomplete,
                       (SELECT count(*) FROM sgi_process_activity a
                         WHERE a.process_id = p.id AND a.active IS TRUE
                           AND NOT EXISTS (SELECT 1 FROM sgi_legacy_routine_activity_rel rel
                                             JOIN sgi_legacy_routine lr ON lr.id = rel.routine_id
                                            WHERE rel.activity_id = a.id AND lr.active IS TRUE))
                           AS activities_new
                  FROM sgi_process p
                 WHERE p.active IS TRUE
            )
            SELECT agg.process_id AS id, agg.*,
                   CASE WHEN routines_total > 0
                        THEN round(100.0 * (routines_total - routines_pending) / routines_total, 1)
                        ELSE 0 END AS routines_resolved_pct,
                   CASE WHEN routines_total + docs_total > 0
                        THEN round(100.0 * (routines_total - routines_pending + docs_migrated + docs_paper)
                                   / (routines_total + docs_total), 1)
                        ELSE 0 END AS progress_pct
              FROM agg
        """ % {'excluded': excluded, 'types': dropbox_types}

    @api.model
    def _search(self, *args, **kwargs):
        self.env.flush_all()
        self.invalidate_model()
        return super()._search(*args, **kwargs)

    # Cada número abre su lista (submenús 20, 30 y 40).
    def _routines_action(self, domain, name):
        self.ensure_one()
        action = self.env['ir.actions.act_window']._for_xml_id('quimibond_sgi.sgi_dropbox_routine_action')
        action['domain'] = [('process_id', '=', self.process_id.id)] + domain
        action['name'] = "%s — %s" % (name, self.process_code)
        action['context'] = {}
        return action

    def action_open_routines(self):
        return self._routines_action([], "Rutinas")

    def action_open_pending_routines(self):
        return self._routines_action([('state', '=', 'pendiente')], "Rutinas pendientes")

    def action_open_procedures(self):
        self.ensure_one()
        action = self.env['ir.actions.act_window']._for_xml_id('quimibond_sgi.sgi_dropbox_procedure_action')
        action['domain'] = PROCEDURE_DOMAIN + [
            '|', ('sgi_replaced_by_process_id', '=', self.process_id.id),
            ('sgi_process_id', '=', self.process_id.id)]
        action['name'] = "Procedimientos anteriores — %s" % self.process_code
        return action

    def action_open_documents(self):
        self.ensure_one()
        action = self.env['ir.actions.act_window']._for_xml_id('quimibond_sgi.sgi_migration_action')
        action['domain'] = [('sgi_is_controlled', '=', True), ('sgi_process_id', '=', self.process_id.id),
                            ('sgi_doc_type', 'in', ('formato', 'formato_it', 'formulario_odoo', 'instructivo',
                                                    'dat', 'anexo', 'protocolo', 'reglamento', 'miid',
                                                    'diagrama', 'control_operacional'))]
        action['name'] = "Formatos y documentos anteriores — %s" % self.process_code
        return action

    def action_open_new_activities(self):
        self.ensure_one()
        activities = self.env['sgi.process.activity'].search([
            ('process_id', '=', self.process_id.id),
            ('id', 'not in', self.env['sgi.legacy.routine'].search([]).mapped('activity_ids').ids)])
        return {'type': 'ir.actions.act_window', 'res_model': 'sgi.process.activity',
                'name': "Actividades nuevas — %s" % self.process_code,
                'view_mode': 'list,form', 'domain': [('id', 'in', activities.ids)]}
