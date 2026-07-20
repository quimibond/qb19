# -*- coding: utf-8 -*-
import re
from dateutil.relativedelta import relativedelta

from odoo import models, fields, api, Command
from odoo.exceptions import ValidationError, UserError

# Nomenclatura documental real de PNTQ (áreas G,A,C,D,E,I,M,P,S,V)
SGI_CODE_REGEX = re.compile(
    r'^(MIID'
    r'|P-[AGCDEIMPSV]\d{2}'
    r'|IT-P-[AGCDEIMPSV]\d{2}-\d{2}'
    r'|F-P-[AGCDEIMPSV]\d{2}-\d{2}'
    r'|F-IT-P-[AGCDEIMPSV]\d{2}-\d{2}-\d{2}'
    r'|DAT.*'
    r'|PROT-\d{2}'
    r'|DF-.*'
    r'|R-.*'
    r'|ANEXO \d{1,2})$'
)


class DocumentsDocument(models.Model):
    _inherit = 'documents.document'

    sgi_is_controlled = fields.Boolean(string="Documento controlado SGI", tracking=True)
    sgi_code = fields.Char(string="Clave SGI", index=True, tracking=True)
    sgi_doc_type = fields.Selection([
        ('miid', "Manual (MIID)"),
        ('procedimiento', "Procedimiento (P)"),
        ('instructivo', "Instructivo (IT)"),
        ('formato', "Formato (F)"),
        ('formato_it', "Formato de instructivo (F-IT)"),
        ('dat', "DAT"),
        ('protocolo', "Protocolo (PROT)"),
        ('diagrama', "Diagrama de flujo (DF)"),
        ('reglamento', "Reglamento (R)"),
        ('anexo', "Anexo"),
        ('externo', "Documento externo"),
    ], string="Tipo de documento")
    sgi_area_id = fields.Many2one('sgi.area', string="Área SGI")
    sgi_process_id = fields.Many2one('sgi.process', string="Proceso SGI")
    sgi_revision = fields.Char(string="Revisión", default="00")
    sgi_issue_date = fields.Date(string="Fecha de emisión")
    sgi_state = fields.Selection([
        ('borrador', "Borrador"),
        ('piloto', "Prueba piloto"),
        ('vigente', "Vigente"),
        ('obsoleto', "Obsoleto"),
    ], string="Estado SGI", default='borrador', tracking=True)
    sgi_owner_id = fields.Many2one('res.users', string="Responsable SGI")
    sgi_job_ids = fields.Many2many('hr.job', 'sgi_document_job_rel', 'document_id', 'job_id',
                                   string="Puestos a los que aplica")
    sgi_next_review_date = fields.Date(string="Próxima revisión")
    sgi_pilot_end_date = fields.Date(string="Fin de prueba piloto")

    # --- Seguimiento de migración del formato a Odoo ---
    sgi_migration_class = fields.Selection([
        ('a', "A - Transacción Odoo"),
        ('b', "B - Hoja de trabajo Calidad"),
        ('c', "C - Salida impresa (reporte)"),
        ('d', "D - Sigue como documento"),
        ('x', "Por definir"),
    ], string="Clase de migración", tracking=True,
        help="A: el registro de Odoo sustituye al formato. B: se configura como "
             "punto de control con hoja de trabajo. C: Odoo lo genera como reporte. "
             "D: permanece como documento controlado.")
    sgi_migration_state = fields.Selection([
        ('pendiente', "Pendiente"),
        ('en_curso', "En curso"),
        ('migrado', "Migrado a Odoo"),
        ('baja', "Baja tramitada"),
        ('na', "No aplica (se queda)"),
    ], string="Estado de migración", default='pendiente', tracking=True)
    sgi_migration_target = fields.Char(string="Destino en Odoo",
        help="Objeto/menú de Odoo que sustituye a este formato (p.ej. 'SGI > No Conformidades').")

    sgi_ack_ids = fields.One2many('sgi.document.ack', 'document_id', string="Acuses de lectura")
    sgi_ack_count = fields.Integer(string="# Acuses", compute='_compute_sgi_ack_stats')
    sgi_ack_read_pct = fields.Float(string="% Difusión", compute='_compute_sgi_ack_stats')

    # --- Relación documental por nomenclatura (P-A28 -> IT/F/F-IT/DAT P-A28-*) ---
    sgi_parent_document_id = fields.Many2one(
        'documents.document', string="Procedimiento padre",
        compute='_compute_sgi_family',
        help="El procedimiento vigente de la familia de la clave (P-Xnn).")
    sgi_family_document_ids = fields.Many2many(
        'documents.document', string="Documentos de la familia",
        compute='_compute_sgi_family',
        help="Instructivos, formatos y DATs de la misma familia de clave.")
    sgi_reference_ids = fields.Many2many(
        'documents.document', 'sgi_doc_reference_rel', 'doc_id', 'ref_id',
        string="Referencias cruzadas",
        help="Documentos de OTRAS familias que este documento menciona "
             "(ej. P-A28 referencia P-A22, P-C01, P-D01). Captura de MAST.")

    def _compute_sgi_family(self):
        Doc = self.env['documents.document']
        family_re = re.compile(r'(P-[AGCDEIMPSV]\d{2})')
        for doc in self:
            family = Doc
            parent = Doc
            code = (doc.sgi_code or '').strip().upper()
            match = family_re.search(code)
            if doc.sgi_is_controlled and match and doc.id:
                base = match.group(1)
                family = Doc.search([
                    ('sgi_is_controlled', '=', True),
                    ('sgi_code', 'like', base + '-'),
                    ('id', '!=', doc.id),
                ], order='sgi_code')
                if code != base:
                    parent = Doc.search([
                        ('sgi_code', '=', base),
                        ('sgi_state', '=', 'vigente'),
                    ], limit=1)
            doc.sgi_family_document_ids = family
            doc.sgi_parent_document_id = parent[:1]

    @api.depends('sgi_ack_ids', 'sgi_ack_ids.state')
    def _compute_sgi_ack_stats(self):
        for doc in self:
            acks = doc.sgi_ack_ids
            total = len(acks)
            read = len(acks.filtered(lambda a: a.state == 'leido'))
            doc.sgi_ack_count = total
            doc.sgi_ack_read_pct = (read / total * 100.0) if total else 0.0

    @api.onchange('sgi_issue_date')
    def _onchange_sgi_issue_date(self):
        for doc in self:
            if doc.sgi_issue_date and not doc.sgi_next_review_date:
                doc.sgi_next_review_date = doc.sgi_issue_date + relativedelta(years=2)

    def init(self):
        """Un solo VIGENTE por clave, garantizado en BD (la validación Python
        sola permite condición de carrera)."""
        super().init()
        self.env.cr.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS documents_document_sgi_unique_vigente
            ON documents_document (sgi_code)
            WHERE sgi_state = 'vigente' AND sgi_is_controlled IS TRUE
                  AND sgi_code IS NOT NULL
        """)

    @api.constrains('sgi_is_controlled', 'sgi_code', 'sgi_doc_type')
    def _check_sgi_code(self):
        for doc in self:
            if not doc.sgi_is_controlled or doc.sgi_doc_type == 'externo':
                continue
            if not doc.sgi_code or not SGI_CODE_REGEX.match(doc.sgi_code.strip()):
                raise ValidationError(
                    "La clave SGI '%s' no cumple la nomenclatura de PNTQ.\n"
                    "Formatos válidos: MIID, P-Xnn, IT-P-Xnn-nn, F-P-Xnn-nn, "
                    "F-IT-P-Xnn-nn-nn, DAT..., PROT-nn, DF-..., R-..., ANEXO n "
                    "(X = área G/A/C/D/E/I/M/P/S/V)." % (doc.sgi_code or '')
                )

    @api.constrains('sgi_is_controlled', 'sgi_code', 'sgi_state')
    def _check_unique_vigente(self):
        for doc in self:
            if doc.sgi_state == 'vigente' and doc.sgi_code:
                dup = self.search_count([
                    ('id', '!=', doc.id),
                    ('sgi_code', '=', doc.sgi_code),
                    ('sgi_state', '=', 'vigente'),
                ])
                if dup:
                    raise ValidationError(
                        "Ya existe un documento vigente con la clave '%s'." % doc.sgi_code)

    def _obsolete_code(self, code, exclude=None):
        """Obsoleta cualquier versión vigente del mismo código (excepto `exclude`)."""
        if not code:
            return
        domain = [('sgi_code', '=', code), ('sgi_state', '=', 'vigente')]
        if exclude:
            domain.append(('id', 'not in', exclude.ids))
        previous = self.search(domain)
        if previous:
            previous.write({'sgi_state': 'obsoleto'})
            # Vuelca el cambio a la BD ANTES de insertar la nueva versión: el
            # índice único parcial (vigente + controlado) mira la tabla, no la
            # caché, así que sin este flush el INSERT de la nueva vigente choca
            # con la anterior aún marcada como vigente en la BD.
            previous.flush_recordset(['sgi_state'])
            for prev in previous:
                prev.message_post(
                    body="Obsoletado automáticamente: entró en vigor una nueva "
                         "revisión del documento %s." % code)

    @api.model_create_multi
    def create(self, vals_list):
        # Obsoleta versiones previas ANTES de crear la nueva vigente (evita el candado de unicidad)
        for vals in vals_list:
            if vals.get('sgi_state') == 'vigente' and vals.get('sgi_code'):
                self._obsolete_code(vals['sgi_code'])
        return super().create(vals_list)

    def write(self, vals):
        if vals.get('sgi_state') == 'vigente':
            for doc in self:
                code = vals.get('sgi_code', doc.sgi_code)
                self._obsolete_code(code, exclude=doc)
        return super().write(vals)

    def action_generate_acks(self):
        """Crea acuses pendientes para los empleados de los puestos aplicables (idempotente)."""
        Ack = self.env['sgi.document.ack']
        for doc in self:
            if not doc.sgi_job_ids:
                continue
            employees = self.env['hr.employee'].search([('job_id', 'in', doc.sgi_job_ids.ids)])
            existing = doc.sgi_ack_ids.mapped('employee_id')
            to_create = [{
                'document_id': doc.id,
                'employee_id': emp.id,
            } for emp in employees if emp not in existing]
            if to_create:
                Ack.create(to_create)
                doc.message_post(body="Se generaron %d acuse(s) de lectura." % len(to_create))
        return True

    def action_open_acks(self):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'name': "Acuses — %s" % (self.sgi_code or self.name),
            'res_model': 'sgi.document.ack',
            'view_mode': 'list,form',
            'domain': [('document_id', '=', self.id)],
            'context': {'default_document_id': self.id},
        }


class SgiDocumentAck(models.Model):
    _name = 'sgi.document.ack'
    _description = "Acuse de lectura de documento SGI"
    _order = 'document_id, employee_id'
    _rec_name = 'document_id'

    document_id = fields.Many2one('documents.document', string="Documento", required=True, ondelete='cascade')
    sgi_code = fields.Char(related='document_id.sgi_code', string="Clave", store=True)
    employee_id = fields.Many2one('hr.employee', string="Empleado", required=True, ondelete='cascade')
    user_id = fields.Many2one('res.users', related='employee_id.user_id', string="Usuario", store=True)
    state = fields.Selection([
        ('pendiente', "Pendiente"),
        ('leido', "Leído y entendido"),
    ], string="Estado", default='pendiente', required=True)
    ack_date = fields.Datetime(string="Fecha de acuse", readonly=True)

    _doc_employee_uniq = models.Constraint(
        'unique(document_id, employee_id)',
        "Ya existe un acuse para este empleado y documento.",
    )

    def action_mark_read(self):
        for ack in self:
            if ack.user_id and ack.user_id != self.env.user and not self.env.user.has_group('quimibond_sgi.group_sgi_manager'):
                raise UserError("Solo el propio empleado puede marcar su acuse como leído.")
            ack.write({'state': 'leido', 'ack_date': fields.Datetime.now()})
        return True
