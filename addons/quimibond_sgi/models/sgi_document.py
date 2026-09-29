# -*- coding: utf-8 -*-
import logging
import re
from dateutil.relativedelta import relativedelta

from odoo import models, fields, api
from odoo.exceptions import AccessError, ValidationError, UserError

from .sgi_base import sgi_bypass_allowed

_logger = logging.getLogger(__name__)

# Nomenclatura documental del Dropbox de PNTQ (áreas G,A,C,D,E,I,M,P,S,V).
# C-007 (56.32.0): solo la usa la herramienta histórica de carga
# (docs/historico/quimibond_sgi_tools/carga_documental.py). La nomenclatura vigente vive en los tipos de
# documento (sgi.document.type: patrón nuevo D-02 + clave heredada).
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

# «Del Dropbox a Odoo» (entrega 6, 56.39.0) -----------------------------------
# L-010 / E-005: campos de la transición. Los consultan todos; solo los escribe
# el Jefe MAST (o el sistema con sudo: el cron que resuelve menús, la
# aplicación de cambios documentales, la baja al entrar en vigor el proceso).
# No hay contexto de excepción: el contexto lo controla el cliente por RPC.
SGI_TRANSITION_FIELDS = frozenset((
    'sgi_previous_code', 'sgi_previous_code_date',
    'sgi_migration_class', 'sgi_migration_state', 'sgi_migration_target',
    'sgi_migration_point_id', 'sgi_odoo_menu_id', 'sgi_replaced_by_process_id',
))
# Tipos del Dropbox que no son procedimiento (la sección «Formatos y documentos
# anteriores», E-004).
SGI_DROPBOX_DOC_TYPES = (
    'formato', 'formato_it', 'formulario_odoo', 'instructivo', 'dat', 'anexo',
    'protocolo', 'reglamento', 'miid', 'diagrama',
)
# L-001: P-I01 contiene credenciales. Queda fuera de la sección, del cron de
# menús, de las migraciones y del importador SIEMPRE, aunque el parámetro
# quimibond_sgi.dropbox_excluded_codes se vacíe.
SGI_DROPBOX_ALWAYS_EXCLUDED = ('P-I01',)
# Clave de procedimiento del Dropbox dentro de cualquier clave anterior
# (F-P-A23-04 → P-A23; DAT P-C10-01 → P-C10; F-IT-P-P07-01-02 → P-P07).
SGI_LEGACY_FAMILY_RE = re.compile(r'(?<![A-Z])(P-[A-Z]\d{2})(?!\d)')


def sgi_legacy_family(code):
    """Familia (clave del procedimiento del Dropbox) de una clave anterior."""
    match = SGI_LEGACY_FAMILY_RE.search((code or '').strip().upper())
    return match.group(1) if match else False


class DocumentsDocument(models.Model):
    _inherit = 'documents.document'

    sgi_is_controlled = fields.Boolean(string="Documento controlado SGI", tracking=True, index=True)
    sgi_code = fields.Char(string="Clave SGI", index=True, tracking=True)
    # C-004/C-005 (56.32.0): la clave anterior es DEFINITIVA (la del Dropbox,
    # copiada por la migración 56.32.0) y se busca siempre (búsqueda «Clave
    # SGI», «Del Dropbox a Odoo» y _sgi_find_by_code): nadie pierde un formato
    # porque cambió la nomenclatura. Un renombre posterior no la pisa; queda
    # en el seguimiento de «Clave SGI».
    sgi_previous_code = fields.Char(
        string="Clave anterior", index=True, copy=False, tracking=True,
        help="Clave con la que se conocía el documento antes de la clave nueva "
             "(la del Dropbox). Se busca siempre y no se sobrescribe.")
    sgi_previous_code_date = fields.Date(
        string="Cambio de clave", copy=False,
        help="Cuándo se cambió la clave en Odoo. Vacío = clave anterior del "
             "Dropbox, copiada por la migración.")
    # C-009 / D-002: título limpio para mostrar (el nombre del archivo sin la
    # extensión ni la clave inicial). El archivo no se renombra: es evidencia.
    sgi_title = fields.Char(
        string="Título", compute='_compute_sgi_title', store=True, index=True,
        help="Nombre del documento sin la clave ni la extensión del archivo. "
             "El archivo conserva su nombre original.")
    # Tipo de documento como dato (sgi.document.type). El campo de selección
    # de antes se conserva calculado para las vistas, dominios y reportes
    # que lo usan; escribirlo resuelve el tipo por su código.
    sgi_doc_type_id = fields.Many2one(
        'sgi.document.type', string="Tipo de documento", index=True,
        tracking=True, ondelete='restrict')
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
        ('control_operacional', "Control operacional (CO)"),
        ('metodo_anexo', "Método / anexo (MA)"),
        ('descripcion_puesto', "Descripción de puesto (DP)"),
    ], string="Tipo (código)", compute='_compute_sgi_doc_type',
        inverse='_inverse_sgi_doc_type', store=True, readonly=False,
        help="Código del tipo de documento (compatibilidad). Un tipo nuevo "
             "creado en Configuración que no esté en esta lista deja este "
             "campo vacío; usa «Tipo de documento».")
    # El «documento» que ya no es un archivo: el formato migrado vive como
    # vista/transacción de Odoo y este registro solo lo controla (clave,
    # revisión, difusión) y lo abre con un clic.
    sgi_odoo_menu_id = fields.Many2one(
        'ir.ui.menu', string="Menú de Odoo",
        help="Menú donde vive el formulario que sustituye a este documento. "
             "El botón «Abrir en Odoo» salta directo a él.")
    sgi_area_id = fields.Many2one('sgi.area', string="Área SGI", ondelete='restrict')
    sgi_process_id = fields.Many2one('sgi.process', string="Proceso SGI", ondelete='restrict')
    # P-3: el documento apunta al cambio documental que lo dejó así (alta,
    # modificación o baja aprobada). Es la liga con la que E2.02 «Publicar el
    # documento vigente» se mide contra su entrada (match: sgi_doc_change_id).
    sgi_doc_change_id = fields.Many2one(
        'approval.request', string="Último cambio documental", copy=False,
        readonly=True, index=True, ondelete='set null',
        help="Solicitud de cambio documental aprobada que produjo esta versión "
             "(la de alta, o la última modificación o baja aplicada).")
    # Revisión como número: se compara, se ordena y no se captura «A» ni
    # «00» por omisión. La etiqueta de dos dígitos es para imprimir.
    sgi_revision = fields.Integer(string="Revisión", tracking=True)
    sgi_revision_label = fields.Char(
        string="Rev.", compute='_compute_sgi_revision_label')
    sgi_issue_date = fields.Date(string="Fecha de emisión")
    sgi_state = fields.Selection([
        ('borrador', "Borrador"),
        ('piloto', "Prueba piloto"),
        ('vigente', "Vigente"),
        ('obsoleto', "Obsoleto"),
    ], string="Estado SGI", tracking=True, index=True,
        help="Solo los documentos controlados del SGI llevan estado; los demás "
             "archivos de Documentos quedan sin él (2026-09-25).")
    sgi_owner_id = fields.Many2one('res.users', string="Responsable SGI")
    sgi_job_ids = fields.Many2many('hr.job', 'sgi_document_job_rel', 'document_id', 'job_id',
                                   string="Puestos a los que aplica")
    sgi_next_review_date = fields.Date(string="Próxima revisión")
    # 5.2 DOC-2 (56.11.0): cuándo y por qué quedó obsoleto.
    sgi_obsolete_date = fields.Date(string="Obsoleto desde", readonly=True, copy=False)
    sgi_obsolete_reason = fields.Char(string="Motivo de obsolescencia", readonly=True, copy=False)
    # C-001/C-014/C-015 (56.31.0, decisión 3 de Jose): ÚNICA fuente de verdad
    # de «qué proceso sustituye a este procedimiento». El proceso solo lee el
    # inverso (sgi.process.replaced_document_ids). restrict, nunca cascade: un
    # One2many cuyo inverso es cascade borra las filas que se quitan (C-014).
    sgi_replaced_by_process_id = fields.Many2one(
        'sgi.process', string="Lo sustituye el proceso", copy=False, index=True,
        ondelete='restrict', tracking=True,
        domain="[('active', '=', True)]",
        help="Proceso de Odoo que sustituye a este procedimiento del Dropbox. El "
             "procedimiento sigue vigente mientras el proceso esté en borrador o "
             "piloto; cuando el proceso entra en vigor pasa a obsoleto y a «Baja "
             "tramitada». Lo captura el Jefe MAST.")
    sgi_pilot_end_date = fields.Date(string="Fin de prueba piloto")

    # --- Retención y disposición de registros (ISO 7.5.3; clientes IATF
    # suelen imponer retenciones largas). 0 = sin definir: el filtro «Sin
    # retención definida» y el diagnóstico lo señalan; la tabla de retención
    # se imprime con el reporte del mismo nombre. ---
    sgi_retention_years = fields.Integer(
        string="Retención (años)",
        help="Años que el registro/documento se conserva tras quedar obsoleto "
             "o cerrado. 0 = sin definir. Clientes automotrices suelen exigir "
             "vida del programa + años: captúralo por documento o familia.")
    sgi_disposition = fields.Selection([
        ('archivo', "Archivo muerto"),
        ('destruccion', "Destrucción controlada"),
        ('devolucion', "Devolución al cliente"),
    ], string="Disposición final",
        help="Qué se hace con el registro al cumplirse la retención.")

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

    # --- «Del Dropbox a Odoo» (entrega 6, 56.39.0) ---------------------------
    # C-011: el destino se guarda de tres formas; para leerlo hay una sola
    # etiqueta con prioridad menú > worksheet > texto.
    sgi_destination_label = fields.Char(
        string="Dónde vive en Odoo", compute='_compute_sgi_destination_label',
        help="Menú de Odoo ligado; si no hay, el worksheet de Calidad; si no, el "
             "texto «Destino en Odoo». Vacío = todavía sin destino.")
    # L-014: familia del Dropbox por la clave anterior (P-A23 para F-P-A23-04),
    # para agrupar los documentos cuyo procedimiento no está cargado sin
    # inventar procedimientos.
    sgi_legacy_family = fields.Char(
        string="Familia del Dropbox", compute='_compute_sgi_legacy_family',
        store=True, index=True,
        help="Clave del procedimiento del Dropbox al que pertenece el documento, "
             "sacada de su clave anterior (o de su clave si todavía no la tiene).")
    # E-005: las vistas de la sección dejan los campos de transición en solo
    # lectura para quien no es Jefe MAST (la guarda del servidor es L-010).
    sgi_can_edit_transition = fields.Boolean(
        compute='_compute_sgi_can_edit_transition')

    @api.depends('sgi_odoo_menu_id', 'sgi_migration_point_id', 'sgi_migration_target')
    def _compute_sgi_destination_label(self):
        for doc in self:
            if doc.sgi_odoo_menu_id:
                label = doc.sgi_odoo_menu_id.sudo().complete_name
            elif doc.sgi_migration_point_id:
                point = doc.sgi_migration_point_id.sudo()
                label = "Worksheet: %s" % (point.title or point.name or point.id)
            else:
                label = (doc.sgi_migration_target or '').strip()
            doc.sgi_destination_label = label or False

    @api.depends('sgi_previous_code', 'sgi_code', 'sgi_is_controlled')
    def _compute_sgi_legacy_family(self):
        for doc in self:
            doc.sgi_legacy_family = sgi_legacy_family(
                doc.sgi_previous_code or doc.sgi_code) if doc.sgi_is_controlled else False

    @api.depends_context('uid')
    def _compute_sgi_can_edit_transition(self):
        allowed = sgi_bypass_allowed(self.env)
        for doc in self:
            doc.sgi_can_edit_transition = allowed

    @api.model
    def _sgi_dropbox_excluded_codes(self):
        """Claves del Dropbox fuera de la sección (L-001): el parámetro
        ``quimibond_sgi.dropbox_excluded_codes`` (separadas por coma) más
        P-I01, que no se puede quitar."""
        raw = self.env['ir.config_parameter'].sudo().get_param(
            'quimibond_sgi.dropbox_excluded_codes', '') or ''
        codes = {c.strip().upper() for c in raw.split(',') if c.strip()}
        return tuple(sorted(codes | set(SGI_DROPBOX_ALWAYS_EXCLUDED)))

    @api.model
    def _sgi_dropbox_excluded_domain(self):
        """Dominio que deja fuera el procedimiento excluido y su familia."""
        codes = list(self._sgi_dropbox_excluded_codes())
        return [('sgi_legacy_family', 'not in', codes),
                ('sgi_parent_document_id', 'not any', [('sgi_legacy_family', 'in', codes)])]

    def _sgi_is_dropbox_excluded(self):
        self.ensure_one()
        codes = self._sgi_dropbox_excluded_codes()
        return (self.sgi_legacy_family in codes
                or self.sgi_parent_document_id.sgi_legacy_family in codes)

    @api.constrains('sgi_migration_class', 'sgi_migration_state')
    def _check_sgi_dropbox_class(self):
        """L-012 / L-013 (solo al escribir clase o estado; las 13
        contradicciones de hoy las corrige MAST): en los documentos del
        Dropbox que no son procedimiento, «Migrado» es de clase A, B o C y con
        destino; la clase D («Sigue como documento») es «No aplica (se
        queda)», y A, B o C nunca son «No aplica»."""
        for doc in self.filtered(lambda d: d.sgi_is_controlled
                                 and d.sgi_doc_type in SGI_DROPBOX_DOC_TYPES):
            name = doc.sgi_previous_code or doc.sgi_code or doc.name
            cls, state = doc.sgi_migration_class, doc.sgi_migration_state
            if state == 'migrado' and (cls not in ('a', 'b', 'c') or not (
                    doc.sgi_odoo_menu_id or doc.sgi_migration_point_id or doc.sgi_migration_target)):
                raise ValidationError(
                    "%s no puede quedar «Migrado a Odoo» sin clase A, B o C y sin "
                    "destino (menú, worksheet o texto)." % name)
            if cls == 'd' and state != 'na':
                raise ValidationError(
                    "%s es clase D («Sigue como documento»): su estado de migración es "
                    "«No aplica (se queda)»." % name)
            if cls in ('a', 'b', 'c') and state == 'na':
                raise ValidationError(
                    "%s es clase %s: Odoo lo sustituye, no puede ser «No aplica (se "
                    "queda)»." % (name, cls.upper()))

    @api.model
    def _sgi_classify_legacy_documents(self, exclude_codes=None, ids=None):
        """Respuesta 3 de Jose a L (migración 56.39.0): instructivos, DAT,
        anexos, protocolos y reglamentos controlados y activos SIN clase pasan
        a clase D y «No aplica (se queda)». Los que ya pasaron a actividades de
        Odoo los marca MAST uno por uno. Deja fuera el procedimiento excluido
        (P-I01) y su familia: por la clave anterior o la clave, y por el
        procedimiento padre. Por SQL, sin chatter; solo toca los que no tienen
        clase, así que la segunda vez devuelve {}. ``ids`` limita el alcance
        (pruebas). Devuelve {tipo: n}."""
        codes = list(exclude_codes if exclude_codes is not None
                     else self._sgi_dropbox_excluded_codes())
        codes = [re.sub(r'[^A-Z0-9-]', '', c.upper()) for c in codes if c]
        patterns = ['(^|[^A-Z])%s([^0-9]|$)' % c for c in codes if c] or ['a^']
        self.env.flush_all()
        query = """
            UPDATE documents_document d
               SET sgi_migration_class = 'd', sgi_migration_state = 'na'
              FROM sgi_document_type t
             WHERE t.id = d.sgi_doc_type_id
               AND t.code IN ('instructivo', 'dat', 'anexo', 'protocolo', 'reglamento')
               AND d.sgi_is_controlled IS TRUE AND d.active IS TRUE
               AND d.sgi_migration_class IS NULL
               AND NOT (upper(coalesce(d.sgi_previous_code, '')) ~ ANY(%(patterns)s))
               AND NOT (upper(coalesce(d.sgi_code, '')) ~ ANY(%(patterns)s))
               AND NOT EXISTS (
                   SELECT 1 FROM documents_document p
                    WHERE p.id = d.sgi_parent_document_id
                      AND (upper(coalesce(p.sgi_previous_code, '')) ~ ANY(%(patterns)s)
                           OR upper(coalesce(p.sgi_code, '')) ~ ANY(%(patterns)s)))
        """
        params = {'patterns': patterns}
        if ids is not None:
            query += " AND d.id = ANY(%(ids)s)"
            params['ids'] = list(ids)
        query += " RETURNING t.code"
        self.env.cr.execute(query, params)
        result = {}
        for (code,) in self.env.cr.fetchall():
            result[code] = result.get(code, 0) + 1
        self.invalidate_model(['sgi_migration_class', 'sgi_migration_state'])
        return result

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
        from .sgi_base import sgi_find_menu
        resolved = self.env['documents.document']
        for doc in self:
            target = (doc.sgi_migration_target or '').split('·')[0]
            target = re.sub(r'\([^)]*\)', '', target).strip()
            if not target or doc.sgi_odoo_menu_id:
                continue
            path = '/'.join(p.strip() for p in target.replace('→', '/')
                            .replace('>', '/').split('/') if p.strip())
            menu = sgi_find_menu(self.env, path)
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

    @api.model
    def _sgi_cron_resolve_menus(self):
        """Paso del cron de medición: resuelve el «Menú de Odoo» desde el
        texto del destino. Los «Formularios de Odoo» y, desde 56.39.0 (L-011),
        cualquier documento de clase A o B (48 formatos «migrado» solo tenían
        el destino en texto). Con sudo: el menú es dato de transición (L-010)
        y esto lo hace el sistema. P-I01 y su familia quedan fuera (L-001)."""
        docs = self.sudo().search([
            ('sgi_odoo_menu_id', '=', False),
            ('sgi_migration_target', '!=', False),
            '|', ('sgi_doc_type_id.code', '=', 'formulario_odoo'),
            ('sgi_migration_class', 'in', ('a', 'b')),
        ] + self._sgi_dropbox_excluded_domain())
        return docs.action_sgi_resolve_odoo_menu()

    def action_sgi_open_odoo_form(self):
        """«Abrir en Odoo» (57.0.0: lo usa todo Usuario SGI desde «Formatos y
        documentos anteriores»). Abre lo que sustituye al documento: el menú
        ligado (su acción; si no es una ventana, el menú mismo), si no el
        worksheet destino y, si no hay ninguno, un aviso. No escribe nada.
        Si el usuario no tiene acceso al menú destino, lo dice en vez de
        abrir una pantalla que Odoo le negaría."""
        self.ensure_one()
        menu = self.sgi_odoo_menu_id
        action = menu.sudo().action if menu else False
        if action:
            if menu.id not in self.env['ir.ui.menu']._visible_menu_ids():
                raise UserError(
                    "Este documento vive en Odoo en «%s», pero tu usuario no tiene acceso a ese "
                    "menú. Pide el acceso a tu jefe o al Jefe MAST." % menu.sudo().complete_name)
            if action._name == 'ir.actions.act_window':
                return action.read()[0]
            return {'type': 'ir.actions.client', 'tag': 'reload', 'params': {'menu_id': menu.id}}
        if self.sgi_migration_point_id:
            return self.action_sgi_open_migration_point()
        raise UserError(
            "Este documento todavía no tiene destino en Odoo (ni menú ni worksheet ligados). "
            "Si ya se hace en Odoo, avísale al Jefe MAST para que lo ligue.")

    sgi_ack_ids = fields.One2many('sgi.document.ack', 'document_id', string="Acuses de lectura")
    # 56.7.0 (1.8): guardados para filtrar y reportar la difusión.
    sgi_ack_count = fields.Integer(string="# Acuses", compute='_compute_sgi_ack_stats', store=True)
    sgi_ack_read_pct = fields.Float(string="% Difusión", compute='_compute_sgi_ack_stats', store=True)

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
        sola permite condición de carrera).

        Con datos legados duplicados (BD restaurada, SQL directo), el CREATE
        UNIQUE INDEX abortaba el update completo del módulo con un
        IntegrityError críptico. Ahora se detectan primero: se loggea la lista
        accionable y se OMITE el índice (el constraint Python sigue
        protegiendo) en vez de bloquear el update."""
        super().init()
        cr = self.env.cr
        cr.execute("""
            SELECT sgi_code, array_agg(id ORDER BY id)
            FROM documents_document
            WHERE sgi_state = 'vigente' AND sgi_is_controlled IS TRUE
                  AND sgi_code IS NOT NULL
            GROUP BY sgi_code HAVING count(*) > 1
        """)
        duplicated = cr.fetchall()
        if duplicated:
            _logger.error(
                "SGI: NO se creó el índice único de documentos vigentes: hay "
                "claves con más de un vigente controlado. Obsoleta los "
                "sobrantes y vuelve a actualizar. Duplicados (clave, ids): %s",
                duplicated)
            return
        cr.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS documents_document_sgi_unique_vigente
            ON documents_document (sgi_code)
            WHERE sgi_state = 'vigente' AND sgi_is_controlled IS TRUE
                  AND sgi_code IS NOT NULL
        """)

    @api.depends('sgi_doc_type_id.code')
    def _compute_sgi_doc_type(self):
        valid = dict(self._fields['sgi_doc_type'].selection)
        for doc in self:
            code = doc.sgi_doc_type_id.code
            doc.sgi_doc_type = code if code in valid else False

    def _inverse_sgi_doc_type(self):
        Type = self.env['sgi.document.type'].sudo()
        for doc in self:
            if not doc.sgi_doc_type:
                if doc.sgi_doc_type_id.code in dict(self._fields['sgi_doc_type'].selection):
                    doc.sgi_doc_type_id = False
                continue
            if doc.sgi_doc_type_id.code == doc.sgi_doc_type:
                continue
            company = doc.company_id or self.env.company
            dtype = Type.search([('code', '=', doc.sgi_doc_type),
                                 ('company_id', 'in', [company.id, False])],
                                order='company_id', limit=1)
            doc.sgi_doc_type_id = dtype

    @staticmethod
    def _sgi_strip_code(text, code):
        """El texto sin la clave inicial (tolerante a separadores: «F-P-A-16-02»
        contra la clave F-P-A16-02). None si el texto no empieza con ella."""
        target = re.sub(r'[^0-9A-Z]', '', (code or '').upper())
        if not target:
            return None
        acc = ''
        for index, char in enumerate(text):
            if char.isalnum():
                acc += char.upper()
            if acc == target:
                rest = text[index + 1:]
                if rest[:1].isalnum():
                    return None
                return rest.lstrip(' -_–—·.:')
            if not target.startswith(acc):
                return None
        return None

    @api.depends('name', 'sgi_code', 'sgi_previous_code', 'sgi_is_controlled')
    def _compute_sgi_title(self):
        for doc in self:
            base = (doc.name or '').strip()
            base = re.sub(r'\.[A-Za-z0-9]{2,5}$', '', base).strip()
            title = base
            if doc.sgi_is_controlled:
                for code in (doc.sgi_code, doc.sgi_previous_code):
                    stripped = doc._sgi_strip_code(base, code)
                    if stripped:
                        title = stripped
                        break
            doc.sgi_title = title or base or False

    @api.depends('sgi_revision')
    def _compute_sgi_revision_label(self):
        for doc in self:
            doc.sgi_revision_label = "%02d" % (doc.sgi_revision or 0)

    def _sgi_share_controlled(self):
        """Un documento controlado en piloto o vigente lo LEE cualquier usuario
        interno (es lo que cada puesto debe leer y firmar), sin depender de la
        carpeta. Documents 18+: `access_internal`.

        56.7.0: y solo lo lee. Antes no se bajaba un «editor» y 486 documentos
        vigentes los podía editar o reemplazar cualquier usuario interno; los
        cambios van por Cambios documentales y MAST edita como gerente de
        Documentos."""
        if 'access_internal' not in self._fields:
            return
        docs = self.sudo().filtered(
            lambda d: d.sgi_is_controlled and d.sgi_state in ('piloto', 'vigente')
            and (d.access_internal or 'none') != 'view')
        if docs:
            super(DocumentsDocument, docs).write({'access_internal': 'view'})

    @api.constrains('sgi_is_controlled', 'sgi_code', 'sgi_doc_type_id', 'sgi_process_id')
    def _check_sgi_code(self):
        """La clave cumple la nomenclatura de su TIPO (patrón nuevo o clave
        heredada, ambos configurables en Configuración → Tipos de documento).
        Los tipos sin clave propia (externos, formularios de Odoo) no se
        revisan. Si el tipo exige proceso, un documento con la nomenclatura
        nueva debe tenerlo."""
        Type = self.env['sgi.document.type'].sudo()
        for doc in self:
            dtype = doc.sgi_doc_type_id
            if not dtype and doc.sgi_doc_type:
                # Al crear, la restricción corre antes del inverso que liga el
                # tipo a partir de la selección heredada: se resuelve aquí.
                company = doc.company_id or self.env.company
                dtype = Type.search([('code', '=', doc.sgi_doc_type),
                                     ('company_id', 'in', [company.id, False])],
                                    order='company_id', limit=1)
            if not doc.sgi_is_controlled or (dtype and not dtype.code_required):
                continue
            code = (doc.sgi_code or '').strip()
            if not dtype:
                # Sin tipo ni clave ('' cuenta como sin clave) no hay
                # nomenclatura contra la cual revisar. Sin tipo pero con clave:
                # basta con que alguna nomenclatura la acepte.
                if not code or Type._sgi_any_match(code):
                    continue
                raise ValidationError(
                    "La clave SGI '%s' no corresponde a ningún tipo de "
                    "documento. Elige el tipo o corrige la clave." % code)
            if not dtype._sgi_code_ok(code, doc.sgi_process_id):
                raise ValidationError(
                    "La clave SGI '%s' no cumple la nomenclatura del tipo «%s» "
                    "(%s%s)." % (
                        code, dtype.name,
                        dtype.prefix_pattern or 'sin patrón',
                        ", con el proceso %s" % doc.sgi_process_id.code
                        if doc.sgi_process_id and '{process}' in (dtype.prefix_pattern or '')
                        else ''))
            if dtype.requires_process and not doc.sgi_process_id \
                    and not dtype._sgi_legacy_match(code):
                raise ValidationError(
                    "Un documento de tipo «%s» debe estar ligado a su proceso "
                    "(%s)." % (dtype.name, code))

    @api.constrains('sgi_replaced_by_process_id', 'sgi_is_controlled', 'sgi_doc_type_id')
    def _check_sgi_replaced_by_process(self):
        """C-015: solo un procedimiento controlado lo sustituye un proceso, y
        ese proceso está activo y es de la misma empresa."""
        for doc in self.filtered('sgi_replaced_by_process_id'):
            process = doc.sgi_replaced_by_process_id
            if not doc.sgi_is_controlled or doc.sgi_doc_type != 'procedimiento':
                raise ValidationError(
                    "Solo un procedimiento controlado puede tener «Lo sustituye el "
                    "proceso» (%s es %s)." % (
                        doc.sgi_code or doc.name,
                        doc.sgi_doc_type_id.name or 'sin tipo'
                        if doc.sgi_is_controlled else 'no controlado'))
            if not process.active:
                raise ValidationError(
                    "El proceso %s está archivado: no puede sustituir a %s." % (
                        process.display_name, doc.sgi_code or doc.name))
            if doc.company_id and process.company_id and doc.company_id != process.company_id:
                raise ValidationError(
                    "El proceso %s es de otra empresa que %s." % (
                        process.display_name, doc.sgi_code or doc.name))

    @api.constrains('sgi_migration_state', 'sgi_replaced_by_process_id')
    def _check_sgi_procedure_migration_state(self):
        """L-005 (decisión C-003): un procedimiento no queda «Migrado a Odoo»
        (o sigue en curso, o su proceso entró en vigor y quedó en baja), y
        «No aplica» (control operacional) es un procedimiento que ningún
        proceso sustituye. Solo corre al escribir esos campos."""
        for doc in self.filtered(lambda d: d.sgi_is_controlled
                                 and d.sgi_doc_type == 'procedimiento'):
            if doc.sgi_migration_state == 'migrado':
                raise ValidationError(
                    "Un procedimiento no queda «Migrado a Odoo»: está «En curso» "
                    "hasta que su proceso entre en vigor, y entonces pasa a «Baja "
                    "tramitada» (%s)." % (doc.sgi_code or doc.name))
            if doc.sgi_migration_state == 'na' and doc.sgi_replaced_by_process_id:
                raise ValidationError(
                    "%s lo sustituye el proceso %s: no puede ser «No aplica (se "
                    "queda)»." % (doc.sgi_code or doc.name,
                                  doc.sgi_replaced_by_process_id.display_name))

    def _sgi_same_code_docs(self):
        """Otros documentos (activos o archivados) con la misma clave y
        empresa."""
        self.ensure_one()
        return self.with_context(active_test=False).search([
            ('id', '!=', self.id),
            ('sgi_code', '=', self.sgi_code),
            ('company_id', '=', self.company_id.id),
        ])

    @api.constrains('sgi_code', 'sgi_revision', 'sgi_is_controlled', 'active', 'company_id')
    def _check_sgi_revision_unique(self):
        """Una sola combinación clave + revisión por empresa entre los
        documentos activos."""
        for doc in self.filtered(lambda d: d.sgi_is_controlled and d.sgi_code and d.active):
            dup = doc._sgi_same_code_docs().filtered(
                lambda d: d.active and d.sgi_is_controlled
                and d.sgi_revision == doc.sgi_revision)
            if dup:
                raise ValidationError(
                    "Ya existe el documento %s con la clave %s y la revisión "
                    "%s. Cada revisión de una clave es única." % (
                        dup[0].display_name, doc.sgi_code, doc.sgi_revision_label))

    @api.constrains('sgi_code', 'sgi_doc_type_id', 'sgi_process_id', 'sgi_is_controlled')
    def _check_sgi_code_family(self):
        """Una clave, aun dada de baja, pertenece a su familia (tipo y
        proceso): no se reutiliza para otro documento."""
        for doc in self.filtered(lambda d: d.sgi_is_controlled and d.sgi_code):
            for other in doc._sgi_same_code_docs().filtered('sgi_is_controlled'):
                different_type = (other.sgi_doc_type_id and doc.sgi_doc_type_id
                                  and other.sgi_doc_type_id != doc.sgi_doc_type_id)
                different_process = (other.sgi_process_id and doc.sgi_process_id
                                     and other.sgi_process_id != doc.sgi_process_id)
                if different_type or different_process:
                    raise ValidationError(
                        "La clave %s ya la usó %s (%s, %s). Una clave, aunque "
                        "esté dada de baja, no se reutiliza en otro tipo de "
                        "documento ni en otro proceso." % (
                            doc.sgi_code, other.display_name,
                            other.sgi_doc_type_id.name or 'sin tipo',
                            other.sgi_process_id.display_name or 'sin proceso'))

    def _sgi_check_revision_increases(self, old_revisions=None):
        """La revisión nueva es mayor que la última de la misma clave (y que
        la que el documento tenía). Corregir una revisión mal capturada hacia
        abajo requiere ``sgi_revision_correction`` (sistema o Jefe MAST)."""
        if self.env.context.get('sgi_revision_correction') and sgi_bypass_allowed(self.env):
            return
        old_revisions = old_revisions or {}
        for doc in self.filtered(lambda d: d.sgi_is_controlled and d.sgi_code):
            previous = old_revisions.get(doc.id)
            if previous is not None and doc.sgi_revision < previous:
                raise ValidationError(
                    "La revisión de %s no puede bajar de %02d a %02d." % (
                        doc.sgi_code, previous, doc.sgi_revision))
            others = doc._sgi_same_code_docs().filtered('sgi_is_controlled')
            if not others:
                continue
            last = max(others.mapped('sgi_revision'))
            if doc.sgi_revision <= last:
                raise ValidationError(
                    "La revisión %02d de %s debe ser mayor que la última "
                    "registrada para esa clave (%02d)." % (
                        doc.sgi_revision or 0, doc.sgi_code, last))

    def _sgi_check_procedure_measures(self, new_state, created=False):
        """Un procedimiento no pasa a piloto con actividades sin método de
        medición, ni a vigente con alguna sin método, «no aplica» sin
        justificación, «consecuencia» sin la actividad que la prueba u «Odoo»
        sin modelo. El error las lista todas."""
        if new_state not in ('piloto', 'vigente'):
            return
        full = new_state == 'vigente'
        for doc in self:
            if doc.sgi_doc_type != 'procedimiento' or not doc.sgi_is_controlled \
                    or not doc.sgi_process_id or (doc.sgi_state == new_state and not created):
                continue
            problems = []
            for activity in doc.sgi_process_id.procedure_activity_ids:
                for problem in activity._sgi_measure_problems(full=full):
                    problems.append("• %s: %s" % (activity.display_name, problem))
            if problems:
                raise UserError(
                    "El procedimiento %s no puede pasar a %s: estas actividades de "
                    "%s no tienen cómo medirse.\n%s" % (
                        doc.sgi_code or doc.name, new_state,
                        doc.sgi_process_id.display_name, "\n".join(problems)))

    @api.model
    def _sgi_find_by_code(self, code, states=('vigente',)):
        """Documento por clave; si no hay, por clave anterior (sin límite de
        tiempo desde 56.32.0, C-005)."""
        code = (code or '').strip()
        if not code:
            return self.browse()
        domain = [('sgi_state', 'in', list(states))] if states else []
        doc = self.search([('sgi_code', '=', code)] + domain,
                          order='sgi_revision desc, id desc', limit=1)
        if doc:
            return doc
        return self.search([('sgi_previous_code', '=', code)] + domain,
                           order='sgi_revision desc, id desc', limit=1)

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

    @api.model
    def _sgi_migrate_procedure_states(self, na_codes=(), skip_codes=()):
        """L-005 / C-003 (migración 56.31.0): los procedimientos controlados,
        activos y en vigor que siguen en «Migrado a Odoo» pasan a «En curso»,
        salvo los de ``na_codes`` (control operacional) que pasan a «No
        aplica (se queda)» si ningún proceso los sustituye. Los de
        ``skip_codes`` no se tocan (P-I01, que se retira aparte). Las claves se
        comparan con la clave del Dropbox (clave anterior, o la clave si aún
        no se copió). Por SQL, sin chatter; solo toca «migrado», así que no
        pisa lo que MAST cambie a mano. Devuelve {estado: n}."""
        self.env.flush_all()
        cr = self.env.cr
        cr.execute("""
            UPDATE documents_document d
               SET sgi_migration_state = CASE
                       WHEN coalesce(d.sgi_previous_code, d.sgi_code) = ANY(%(na)s)
                            AND d.sgi_replaced_by_process_id IS NULL
                       THEN 'na' ELSE 'en_curso' END
              FROM sgi_document_type t
             WHERE t.id = d.sgi_doc_type_id AND t.code = 'procedimiento'
               AND d.sgi_is_controlled IS TRUE AND d.active IS TRUE
               AND d.sgi_state IN ('vigente', 'piloto')
               AND d.sgi_migration_state = 'migrado'
               AND NOT (coalesce(d.sgi_previous_code, d.sgi_code, '') = ANY(%(skip)s))
         RETURNING d.sgi_migration_state
        """, {'na': list(na_codes), 'skip': list(skip_codes)})
        result = {}
        for (state,) in cr.fetchall():
            result[state] = result.get(state, 0) + 1
        self.invalidate_model(['sgi_migration_state'])
        return result

    @api.model
    def _sgi_migrate_previous_codes(self, exclude_ids=(), ids=None):
        """C-004 + L-004 (migración 56.32.0): la clave del Dropbox pasa a ser
        la clave anterior definitiva. Copia ``sgi_code`` → ``sgi_previous_code``
        (sin fecha = clave del Dropbox) en los documentos controlados, activos
        o archivados, salvo «Mi procedimiento» y externos, solo donde la clave
        anterior está vacía. No toca ``sgi_code`` ni el nombre. Por SQL: sin
        write(), sin seguimiento ni chatter. ``ids`` limita el alcance (pruebas).
        Devuelve cuántos copió (0 la segunda vez)."""
        self.env.flush_all()
        query = """
            UPDATE documents_document d
               SET sgi_previous_code = d.sgi_code
              FROM sgi_document_type t
             WHERE t.id = d.sgi_doc_type_id
               AND t.code NOT IN ('mi_procedimiento', 'externo')
               AND d.sgi_is_controlled IS TRUE
               AND d.sgi_previous_code IS NULL
               AND d.sgi_code IS NOT NULL AND btrim(d.sgi_code) <> ''
               AND NOT (d.id = ANY(%(exclude)s))
        """
        params = {'exclude': list(exclude_ids)}
        if ids is not None:
            query += " AND d.id = ANY(%(ids)s)"
            params['ids'] = list(ids)
        self.env.cr.execute(query, params)
        count = self.env.cr.rowcount
        self.invalidate_model(['sgi_previous_code'])
        # El título limpio también quita la clave anterior.
        if count:
            docs = self.with_context(active_test=False).search(
                [('sgi_previous_code', '!=', False)] + ([('id', 'in', list(ids))] if ids is not None else []))
            self.env.add_to_compute(self._fields['sgi_title'], docs)
        return count

    # --- Clave nueva (C-004, D-02) -------------------------------------------
    def _sgi_new_code_blockers(self):
        """Por qué este documento no recibe clave nueva (texto) o False."""
        self.ensure_one()
        if not self.sgi_is_controlled:
            return "no es controlado"
        if self.sgi_state == 'obsoleto':
            return "está obsoleto"
        if self.sgi_doc_type in ('formulario_odoo', 'externo', 'mi_procedimiento'):
            return "es %s: conserva su clave" % (self.sgi_doc_type_id.name or self.sgi_doc_type)
        if self.sgi_replaced_by_process_id:
            return "lo sustituye el proceso %s: se da de baja con su clave" % (
                self.sgi_replaced_by_process_id.code)
        if not self.sgi_doc_type_id.prefix_pattern:
            return "su tipo (%s) no tiene patrón de clave nueva" % (self.sgi_doc_type_id.name or '—')
        if self.sgi_doc_type_id._sgi_new_code_ok(self.sgi_code, self.sgi_process_id):
            return "ya tiene clave nueva"
        return False

    def _sgi_assign_new_code(self, new_code=None):
        """Pone la clave nueva a este documento y a TODAS las revisiones de su
        clave (activas o archivadas), o a ninguna. Sin ``new_code`` la arma el
        tipo (``sgi_next_code`` con el proceso). La clave anterior (Dropbox)
        no se toca: ya quedó en ``sgi_previous_code``. Solo Jefe MAST."""
        self.ensure_one()
        if not (self.env.su or self.env.user.has_group('quimibond_sgi.group_sgi_manager')):
            raise AccessError("Solo el Jefe MAST asigna claves nuevas.")
        blocker = self._sgi_new_code_blockers()
        if blocker:
            raise UserError("%s no recibe clave nueva: %s." % (self.sgi_code or self.name, blocker))
        dtype = self.sgi_doc_type_id
        if not self.sgi_process_id and '{process}' in dtype.prefix_pattern:
            raise UserError("%s necesita su proceso para armar la clave nueva." % self.sgi_code)
        new_code = (new_code or dtype.sgi_next_code(self.sgi_process_id)).strip()
        if not dtype._sgi_new_code_ok(new_code, self.sgi_process_id):
            raise UserError(
                "«%s» no es una clave nueva de %s (%s, proceso %s). La clave del "
                "Dropbox no se reutiliza como clave nueva." % (
                    new_code, dtype.name, dtype.prefix_pattern,
                    self.sgi_process_id.code or '—'))
        family = self | self._sgi_same_code_docs()
        taken = self.with_context(active_test=False).search_count([
            ('sgi_code', '=', new_code), ('id', 'not in', family.ids)])
        if taken:
            raise UserError("La clave %s ya la usa otro documento." % new_code)
        old_code = self.sgi_code
        # Cambia la clave, no la revisión: la regla «cada revisión nueva va
        # por arriba» no aplica a revisiones que ya existían.
        family.with_context(sgi_revision_correction=True).write({'sgi_code': new_code})
        for doc in family:
            doc.message_post(body="Clave nueva %s (antes %s)." % (new_code, old_code))
        return new_code

    def action_sgi_assign_new_code(self):
        """Acción «Asignar clave nueva» (lista de documentos, Jefe MAST): a
        cada documento seleccionado le arma la siguiente clave del patrón de
        su tipo (D-02) con todas sus revisiones. Los que no aplican se
        reportan y se saltan."""
        done, skipped, seen = [], [], set()
        for doc in self.sorted(lambda d: (d.sgi_process_id.code or '', d.sgi_code or '')):
            if doc.sgi_code in seen:
                continue
            seen.add(doc.sgi_code)
            try:
                with self.env.cr.savepoint():
                    old = doc.sgi_code
                    done.append("%s → %s" % (old, doc._sgi_assign_new_code()))
            except (UserError, ValidationError) as exc:
                skipped.append(str(exc.args[0] if exc.args else exc))
        message = "Clave nueva en %d clave(s)." % len(done)
        if done:
            message += " " + "; ".join(done[:20])
        if skipped:
            message += " Sin cambio (%d): %s" % (len(skipped), "; ".join(skipped[:20]))
        return {
            'type': 'ir.actions.client', 'tag': 'display_notification',
            'params': {'type': 'success' if done and not skipped else 'warning',
                       'message': message, 'sticky': bool(skipped)},
        }

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
            # C-006: los formatos ligados a la revisión anterior pasan a la nueva.
            self.env['sgi.format.map']._sgi_repoint(prior_revisions, doc)

    # --- L-010 / E-005 (56.39.0): el usuario no escribe la transición -------
    @api.model
    def _sgi_check_transition_create(self, vals_list):
        """Crear un documento con datos de transición (clave anterior, clase,
        estado o destino de migración, menú, sustitución) es de Jefe MAST. Los
        valores por defecto (vacío, estado «Pendiente») sí pasan: el formulario
        los manda al crear."""
        if sgi_bypass_allowed(self.env):
            return
        for vals in vals_list:
            given = sorted(
                name for name in SGI_TRANSITION_FIELDS & set(vals)
                if vals[name] and not (name == 'sgi_migration_state' and vals[name] == 'pendiente'))
            if given:
                raise AccessError(
                    "Solo el Jefe MAST captura los datos de «Del Dropbox a Odoo» (%s)."
                    % ", ".join(self._fields[name].string for name in given))

    def _sgi_check_transition_write(self, vals):
        """Escribir un campo de transición con un valor DISTINTO al que ya
        tiene es de Jefe MAST (reescribir el mismo valor, como hace un
        formulario, no cuenta)."""
        touched = SGI_TRANSITION_FIELDS & set(vals)
        if not touched or sgi_bypass_allowed(self.env):
            return
        changed = sorted(name for name in touched
                         if any(doc._sgi_value_differs(name, vals[name]) for doc in self))
        if not changed:
            return
        if changed == ['sgi_replaced_by_process_id']:
            # D-017: el documento es la fuente de verdad y lo captura el Jefe MAST.
            raise AccessError(
                "Solo el Jefe MAST captura qué proceso sustituye a un procedimiento.")
        raise AccessError(
            "Solo el Jefe MAST cambia los datos de «Del Dropbox a Odoo» (%s)."
            % ", ".join(self._fields[name].string for name in changed))

    def _sgi_value_differs(self, name, value):
        self.ensure_one()
        field = self._fields[name]
        current = self[name]
        if field.type == 'many2one':
            if isinstance(value, models.BaseModel):
                value = value.id
            return (value or False) != (current.id or False)
        if field.type == 'date':
            return fields.Date.to_date(value or None) != (current or None)
        return (value or False) != (current or False)

    @api.model_create_multi
    def create(self, vals_list):
        self._sgi_check_transition_create(vals_list)
        # Obsoleta versiones previas ANTES de crear la nueva vigente (evita el candado de unicidad)
        for vals in vals_list:
            if vals.get('sgi_is_controlled') and not vals.get('sgi_state'):
                vals['sgi_state'] = 'borrador'
            if vals.get('sgi_state') == 'vigente' and vals.get('sgi_code'):
                self._obsolete_code(vals['sgi_code'])
        docs = super().create(vals_list)
        docs._sgi_share_controlled()
        for state in ('piloto', 'vigente'):
            docs.filtered(lambda d, state=state: d.sgi_state == state)\
                ._sgi_check_procedure_measures(state, created=True)
        # Una revisión nueva de una clave existente va por arriba de la última.
        docs._sgi_check_revision_increases()
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
                doc.sudo().write({'sgi_doc_change_id': request.id})
                doc.message_post(
                    body="Documento creado desde la solicitud de alta aprobada "
                         "<b>%s</b>." % (request.name or ''))
                request.message_post(
                    body="Documento del alta creado: <b>%s</b>."
                         % (doc.sgi_code or doc.name))
        return docs

    def write(self, vals):
        self._sgi_check_transition_write(vals)
        if vals.get('sgi_state') == 'obsoleto' and 'sgi_obsolete_date' not in vals:
            vals = dict(vals, sgi_obsolete_date=fields.Date.context_today(self))
        if vals.get('sgi_state') in ('piloto', 'vigente'):
            self._sgi_check_procedure_measures(vals['sgi_state'])
        if vals.get('sgi_state') == 'vigente' and len(self) > 1:
            # Selección múltiple con la MISMA clave: el obsoletado excluye solo
            # al doc en turno, ambos quedarían vigentes y el índice único
            # reventaría en el flush con un IntegrityError ilegible. Mejor un
            # error claro antes de escribir.
            seen = {}
            for doc in self:
                code = vals.get('sgi_code', doc.sgi_code)
                controlled = vals.get('sgi_is_controlled', doc.sgi_is_controlled)
                if not code or not controlled:
                    continue
                if code in seen:
                    raise UserError(
                        "No se puede poner en vigor más de un documento "
                        "controlado con la clave '%s' a la vez (%s y %s): solo "
                        "puede haber un vigente por clave. Hazlo de uno en uno." % (
                            code, seen[code].display_name, doc.display_name))
                seen[code] = doc
        if vals.get('sgi_state') == 'vigente':
            for doc in self:
                code = vals.get('sgi_code', doc.sgi_code)
                self._obsolete_code(code, exclude=doc)
        old_revisions = {doc.id: doc.sgi_revision for doc in self} \
            if 'sgi_revision' in vals else {}
        if 'sgi_code' in vals and 'sgi_previous_code' not in vals:
            # Cambio de clave: la anterior se guarda y sigue encontrando el
            # documento, sin límite. C-005 (56.32.0): solo si no tenía clave
            # anterior; la del Dropbox (o la primera) es definitiva y los
            # renombres posteriores quedan en el seguimiento de «Clave SGI».
            new_code = (vals.get('sgi_code') or '').strip()
            today = fields.Date.context_today(self)
            for doc in self.filtered(lambda d: d.sgi_code and d.sgi_code != new_code
                                     and not d.sgi_previous_code):
                super(DocumentsDocument, doc).write({
                    'sgi_previous_code': doc.sgi_code,
                    'sgi_previous_code_date': today,
                })
        res = super().write(vals)
        if vals.get('sgi_is_controlled') and 'sgi_state' not in vals:
            # Al volverse controlado sin estado, arranca en borrador.
            fresh = self.filtered(lambda d: not d.sgi_state)
            if fresh:
                super(DocumentsDocument, fresh).write({'sgi_state': 'borrador'})
        if 'sgi_state' in vals or 'sgi_is_controlled' in vals:
            self._sgi_share_controlled()
        if 'sgi_revision' in vals or 'sgi_code' in vals:
            self._sgi_check_revision_increases(old_revisions)
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
            employees = self.env['hr.employee'].sudo().search([('job_id', 'in', doc.sgi_job_ids.ids)])
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

    # El acuse es evidencia de difusión (ISO 7.5): la firma vive en write(),
    # no solo en el botón — un write directo (lista editable, import, RPC)
    # podía firmar acuses ajenos sin dejar rastro. Un acuse cuyo empleado no
    # tiene usuario ligado solo lo firma MAST (no hay forma de saber que fue
    # «el propio empleado»).
    _SGI_ACK_SIGN_FIELDS = {'state', 'ack_date'}

    def _sgi_check_can_sign(self):
        if self.env.su or self.env.user.has_group('quimibond_sgi.group_sgi_manager'):
            return
        for ack in self:
            if not ack.user_id or ack.user_id != self.env.user:
                raise UserError(
                    "Solo el propio empleado (o el Jefe de MAST) puede firmar o "
                    "modificar el acuse de lectura de %s." % ack.employee_id.name)

    @api.model_create_multi
    def create(self, vals_list):
        acks = super().create(vals_list)
        # Crear un acuse ya «firmado» equivale a firmarlo: mismo candado.
        pre_signed = acks.filtered(lambda a: a.state != 'pendiente' or a.ack_date)
        pre_signed._sgi_check_can_sign()
        return acks

    def write(self, vals):
        if self._SGI_ACK_SIGN_FIELDS & set(vals):
            self._sgi_check_can_sign()
        # 56.7.0: un acuse firmado no se «mueve» a otra persona ni a otro
        # documento (sería evidencia falsa de difusión).
        if {'employee_id', 'document_id'} & set(vals) and any(a.state == 'leido' for a in self) \
                and not (self.env.su or self.env.user.has_group('quimibond_sgi.group_sgi_manager')):
            raise UserError("Un acuse firmado no se puede pasar a otro empleado ni a otro documento.")
        return super().write(vals)

    def action_mark_read(self):
        # La validación de identidad vive en write(); aquí solo se sella.
        for ack in self:
            ack.write({'state': 'leido', 'ack_date': fields.Datetime.now()})
        return True

    def action_view_file(self):
        """Leer antes de firmar: abre el PDF/enlace del documento del acuse."""
        self.ensure_one()
        return self.document_id.action_sgi_view_file()
