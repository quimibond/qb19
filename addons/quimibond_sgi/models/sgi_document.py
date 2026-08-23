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
        ('formulario_odoo', "Formulario de Odoo (vista)"),
    ], string="Tipo de documento")
    # El «documento» que ya no es un archivo: el formato migrado vive como
    # vista/transacción de Odoo y este registro solo lo controla (clave,
    # revisión, difusión) y lo abre con un clic.
    sgi_odoo_menu_id = fields.Many2one(
        'ir.ui.menu', string="Menú de Odoo",
        help="Menú donde vive el formulario que sustituye a este documento. "
             "El botón «Abrir en Odoo» salta directo a él.")
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

    # --- Detección de divergencia "Procedimiento vivo" vs PDF controlado (G14) ---
    # Los datos vivos del procedimiento (sgi.process.activity/responsibility y el
    # cuerpo del proceso) pueden editarse fuera del flujo de cambio documental.
    # Cuando eso pasa sobre un procedimiento con revisión VIGENTE, el documento
    # queda "pendiente de revisión": el PDF impreso ya no coincide con la revisión
    # aprobada. La bandera se limpia al aprobar una nueva revisión.
    sgi_procedure_dirty = fields.Boolean(
        string="Procedimiento vivo pendiente de revisión", readonly=True, copy=False,
        help="Las actividades/responsabilidades del procedimiento vivo cambiaron "
             "después de esta revisión vigente. Genere una nueva revisión "
             "controlada o confirme que el cambio no la amerita.")
    sgi_procedure_dirty_since = fields.Datetime(
        string="Divergencia desde", readonly=True, copy=False)
    sgi_procedure_dirty_by = fields.Many2one(
        'res.users', string="Divergencia registrada por", readonly=True, copy=False)

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
    # Liga REAL al destino (H-migración): del formato al worksheet con un
    # clic. Para hojas de piso/laboratorio apunta al punto de calidad; los
    # formatos-transacción siguen describiendo su menú en sgi_migration_target.
    sgi_migration_point_id = fields.Many2one(
        'quality.point', string="Worksheet destino",
        help="Punto de calidad (worksheet) que sustituye a este formato. "
             "El botón «Abrir worksheet» salta directo a él.")

    def action_sgi_open_migration_point(self):
        """Del formato a su worksheet en un clic (y desde ahí, a sus checks)."""
        self.ensure_one()
        if not self.sgi_migration_point_id:
            raise UserError(
                "Este formato no tiene ligado su worksheet destino. "
                "Selecciónalo en la pestaña de migración (campo "
                "«Worksheet destino»).")
        return {
            'type': 'ir.actions.act_window',
            'name': self.sgi_migration_point_id.title or "Punto de calidad",
            'res_model': 'quality.point',
            'res_id': self.sgi_migration_point_id.id,
            'view_mode': 'form',
        }

    def action_sgi_resolve_odoo_menu(self):
        """Resuelve el «Menú de Odoo» desde el texto de «Destino en Odoo»:
        convierte «SGI > No Conformidades» en el menú real, igual que las
        actividades del procedimiento (los ir.* no se exponen por MCP, así
        que la liga se resuelve aquí, en el servidor). Ignora los
        paréntesis aclaratorios («Fabricación > Plan Maestro (MPS)»)."""
        Menu = self.env['ir.ui.menu'].sudo()
        resolved = self.env['documents.document']
        for doc in self:
            target = (doc.sgi_migration_target or '').split('·')[0]
            target = re.sub(r'\([^)]*\)', '', target).strip()
            if not target or doc.sgi_odoo_menu_id:
                continue
            path = '/'.join(p.strip() for p in target.replace('→', '/')
                            .replace('>', '/').split('/') if p.strip())
            menu = Menu.search([
                ('complete_name', '=ilike', path),
                ('action', '!=', False)], limit=1)
            if not menu and '/' in path:
                first, last = path.split('/')[0], path.split('/')[-1]
                menu = Menu.search([
                    ('name', '=ilike', last),
                    ('complete_name', '=ilike', first + '/%'),
                    ('action', '!=', False)], limit=1)
            if not menu and '/' not in path:
                menu = Menu.search([
                    ('name', '=ilike', path),
                    ('action', '!=', False)], limit=1)
            if menu:
                doc.sgi_odoo_menu_id = menu
                resolved |= doc
        return {
            'type': 'ir.actions.client',
            'tag': 'display_notification',
            'params': {
                'type': 'success' if resolved else 'warning',
                'message': "Menú resuelto en %d de %d documento(s)." % (
                    len(resolved), len(self)),
            },
        }

    def action_sgi_open_odoo_form(self):
        """Abre el formulario de Odoo que sustituye al documento: el menú
        ligado o, en su defecto, el worksheet destino de la migración."""
        self.ensure_one()
        action = self.sgi_odoo_menu_id.action if self.sgi_odoo_menu_id else False
        if action and action._name == 'ir.actions.act_window':
            return action.read()[0]
        if self.sgi_migration_point_id:
            return self.action_sgi_open_migration_point()
        raise UserError(
            "Este documento no tiene ligado su formulario de Odoo. "
            "Selecciona el «Menú de Odoo» (o el worksheet destino) en la ficha.")

    sgi_ack_ids = fields.One2many('sgi.document.ack', 'document_id', string="Acuses de lectura")
    sgi_ack_count = fields.Integer(string="# Acuses", compute='_compute_sgi_ack_stats')
    sgi_ack_read_pct = fields.Float(string="% Difusión", compute='_compute_sgi_ack_stats')

    # --- Relación documental por FK real (P-A28 -> IT/F/F-IT/DAT P-A28-*) ---
    # H21: la familia se define por un enlace explícito y editable, no por regex.
    # La nomenclatura queda solo como SUGERENCIA (onchange) y como semilla de la
    # migración idempotente (sgi.config.migrate_document_families).
    sgi_parent_document_id = fields.Many2one(
        'documents.document', string="Procedimiento padre",
        domain=[('sgi_is_controlled', '=', True)], index=True, ondelete='set null',
        help="Procedimiento del que depende este documento (familia documental).")
    sgi_child_document_ids = fields.One2many(
        'documents.document', 'sgi_parent_document_id', string="Documentos hijos")
    sgi_family_document_ids = fields.Many2many(
        'documents.document', string="Documentos de la familia",
        compute='_compute_sgi_family',
        help="Hermanos (hijos del mismo padre) más los hijos propios.")
    sgi_reference_ids = fields.Many2many(
        'documents.document', 'sgi_doc_reference_rel', 'doc_id', 'ref_id',
        string="Referencias cruzadas",
        help="Documentos de OTRAS familias que este documento menciona "
             "(ej. P-A28 referencia P-A22, P-C01, P-D01). Captura de MAST.")

    @api.depends('sgi_parent_document_id',
                 'sgi_parent_document_id.sgi_child_document_ids',
                 'sgi_child_document_ids')
    def _compute_sgi_family(self):
        for doc in self:
            own_children = doc.sgi_child_document_ids
            if doc.sgi_parent_document_id:
                siblings = doc.sgi_parent_document_id.sgi_child_document_ids - doc
                doc.sgi_family_document_ids = siblings | own_children
            else:
                doc.sgi_family_document_ids = own_children

    @api.onchange('sgi_code')
    def _onchange_sgi_code_parent(self):
        """Sugerencia (no obliga): propone el procedimiento padre P-Xnn vigente
        por la nomenclatura, solo si el enlace está vacío."""
        if self.sgi_parent_document_id or not self.sgi_is_controlled:
            return
        code = (self.sgi_code or '').strip().upper()
        match = re.compile(r'(P-[AGCDEIMPSV]\d{2})').search(code)
        if not match or code == match.group(1):
            return
        parent = self.env['documents.document'].search([
            ('sgi_code', '=', match.group(1)), ('sgi_state', '=', 'vigente'),
        ], limit=1)
        if parent:
            self.sgi_parent_document_id = parent

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
            # Externos y formularios de Odoo pueden no tener clave PNTQ (el
            # formulario nativo de Odoo no siempre sustituye a un formato F-).
            if not doc.sgi_is_controlled or doc.sgi_doc_type in ('externo', 'formulario_odoo'):
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
        # Mismo alcance que el índice único parcial de BD (init): solo aplica
        # a documentos CONTROLADOS — antes Python rechazaba duplicados que la
        # BD sí permitía en documentos no controlados.
        for doc in self:
            if doc.sgi_state == 'vigente' and doc.sgi_code and doc.sgi_is_controlled:
                dup = self.search_count([
                    ('id', '!=', doc.id),
                    ('sgi_code', '=', doc.sgi_code),
                    ('sgi_state', '=', 'vigente'),
                    ('sgi_is_controlled', '=', True),
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

    def _sgi_reparent_family(self):
        """H21: al entrar en vigor una nueva revisión de un procedimiento, sus
        hijos (que colgaban de la revisión anterior, ahora obsoleta) se re-apuntan
        al registro vigente. El ciclo documental crea un REGISTRO NUEVO por
        revisión, así que sin esto la familia del procedimiento nuevo se vería
        vacía y los hijos quedarían colgados de un documento obsoleto."""
        for doc in self:
            if doc.sgi_state != 'vigente' or not doc.sgi_code:
                continue
            prior_revisions = self.search([
                ('sgi_code', '=', doc.sgi_code),
                ('id', '!=', doc.id),
            ])
            if not prior_revisions:
                continue
            orphans = self.search([
                ('sgi_parent_document_id', 'in', prior_revisions.ids)])
            if orphans:
                orphans.write({'sgi_parent_document_id': doc.id})

    @api.model_create_multi
    def create(self, vals_list):
        # Obsoleta versiones previas ANTES de crear la nueva vigente (evita el candado de unicidad)
        for vals in vals_list:
            if vals.get('sgi_state') == 'vigente' and vals.get('sgi_code'):
                self._obsolete_code(vals['sgi_code'])
        docs = super().create(vals_list)
        docs.filtered(
            lambda d: d.sgi_state == 'vigente' and d.sgi_code)._sgi_reparent_family()
        # Trazabilidad del alta documental: el documento creado desde la
        # solicitud aprobada (botón «Crear documento») queda ligado a ella.
        request_id = self.env.context.get('sgi_alta_request_id')
        if request_id and docs:
            request = self.env['approval.request'].browse(request_id).exists()
            if request and not request.sgi_document_id:
                doc = docs[0]
                request.sudo().write({'sgi_document_id': doc.id})
                doc.message_post(
                    body="Documento creado desde la solicitud de alta aprobada "
                         "<b>%s</b>." % (request.name or ''))
                request.message_post(
                    body="Documento del alta creado: <b>%s</b>."
                         % (doc.sgi_code or doc.name))
        return docs

    def write(self, vals):
        if vals.get('sgi_state') == 'vigente':
            for doc in self:
                code = vals.get('sgi_code', doc.sgi_code)
                self._obsolete_code(code, exclude=doc)
        res = super().write(vals)
        if vals.get('sgi_state') == 'vigente':
            self._sgi_reparent_family()
        # Una nueva revisión aprobada (bump de revisión o entrada en vigor)
        # realinea el documento con el procedimiento vivo: limpia la divergencia.
        if 'sgi_revision' in vals or vals.get('sgi_state') == 'vigente':
            dirty = self.filtered('sgi_procedure_dirty')
            if dirty:
                dirty.write({
                    'sgi_procedure_dirty': False,
                    'sgi_procedure_dirty_since': False,
                    'sgi_procedure_dirty_by': False,
                })
        return res

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

    def action_sgi_view_file(self):
        """Abre el archivo del documento para previsualizarlo en el navegador.

        Si el documento tiene adjunto binario, sirve su contenido inline (sin
        download=true, para que el visor del navegador lo muestre); si es de tipo
        enlace (URL), abre la URL; si no tiene nada, avisa amablemente. Un
        «Formulario de Odoo» no tiene archivo: abre la vista real."""
        self.ensure_one()
        if self.sgi_doc_type == 'formulario_odoo':
            return self.action_sgi_open_odoo_form()
        if self.attachment_id:
            return {
                'type': 'ir.actions.act_url',
                'url': '/web/content/%d?filename=%s' % (
                    self.attachment_id.id,
                    self.name or self.attachment_id.name or ''),
                'target': 'new',
            }
        if self.type == 'url' and self.url:
            return {
                'type': 'ir.actions.act_url',
                'url': self.url,
                'target': 'new',
            }
        raise UserError(
            "Este documento no tiene archivo ni enlace para abrir. "
            "Sube el PDF en «Archivo adjunto» o captura la URL.")

    def action_sgi_open_in_documents(self):
        """Abre el documento en la app nativa de Documentos (visor completo con
        carpetas), para quien quiera el explorador en vez de la ficha SGI."""
        self.ensure_one()
        action = self.env.ref('documents.document_action',
                              raise_if_not_found=False)
        if not action:
            raise UserError("La app de Documentos no está disponible.")
        result = action.sudo().read()[0]
        result['res_id'] = self.id
        result.setdefault('context', {})
        return result


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

    def action_view_file(self):
        """Leer antes de firmar: abre el PDF/enlace del documento del acuse."""
        self.ensure_one()
        return self.document_id.action_sgi_view_file()
