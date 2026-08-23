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
from datetime import datetime, timedelta

from odoo import models, fields, api
from odoo.exceptions import UserError, ValidationError
from odoo.tools.safe_eval import safe_eval


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
    activity_ids = fields.One2many(
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

    # Firmas del procedimiento (bloque del F-P-G01-02). Se imprimen como
    # nombre + cargo; el PDF generado es copia NO controlada, sin imagen de firma.
    doc_owner_id = fields.Many2one(
        'res.users', string="Responsable del documento",
        help="Elabora / es dueño del procedimiento (bloque de firmas).")
    doc_approver_id = fields.Many2one(
        'res.users', string="Aprueba")
    doc_vobo_id = fields.Many2one(
        'res.users', string="Vo.Bo.")

    # Campos del cuerpo del procedimiento cuya edición diverge del PDF controlado.
    _SGI_PROCEDURE_BODY_FIELDS = {
        'scope', 'env_aspects', 'norm_ids',
        'doc_owner_id', 'doc_approver_id', 'doc_vobo_id'}

    @api.depends('activity_ids')
    def _compute_activity_count(self):
        for process in self:
            process.activity_count = len(process.activity_ids)

    @api.depends('activity_ids.measure_state', 'activity_ids.measure_model_id')
    def _compute_measure_stats(self):
        for process in self:
            acts = process.activity_ids.filtered(
                lambda a: a.measure_model_id and a.measure_state)
            reds = acts.filtered(lambda a: a.measure_state == 'rojo')
            process.measurable_activity_count = len(acts)
            process.measure_red_count = len(reds)
            process.procedure_compliance = (
                int(round((len(acts) - len(reds)) * 100.0 / len(acts)))
                if acts else 0)

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
        if not self.env.registry.ready or self.env.context.get('sgi_bypass_dirty'):
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
                         doc.sgi_revision or ''))
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
        docs = self.activity_ids.mapped('format_document_ids')
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
    _order = 'process_id, sequence, number, id'

    process_id = fields.Many2one(
        'sgi.process', string="Proceso", required=True, ondelete='cascade',
        index=True)
    sequence = fields.Integer(string="Secuencia", default=10)
    number = fields.Char(string="Numeral", help="Ej. 4.2.3.1")
    block = fields.Selection([
        ('inicial', "Actividades iniciales"),
        ('desarrollo', "Desarrollo"),
        ('final', "Actividades finales"),
    ], string="Bloque", default='desarrollo', required=True)
    section = fields.Char(
        string="Sección", help="Título del apartado, ej. '4.2.3 Cotización de productos'.")
    name = fields.Char(string="Resumen", help="Resumen corto de la actividad.")
    description = fields.Text(
        string="Descripción", help="Texto completo del numeral del procedimiento.")
    responsible_job_ids = fields.Many2many(
        'hr.job', 'sgi_activity_job_rel', 'activity_id', 'job_id',
        string="Puestos responsables")
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
    ], string="Cumplimiento", readonly=True)

    # Ventana de tolerancia por cadencia (días naturales): holgura para fines
    # de semana y cierres sin falsos rojos.
    _SGI_CADENCE_DAYS = {
        'diaria': 2, 'semanal': 9, 'quincenal': 18, 'mensual': 35,
        'trimestral': 100, 'semestral': 190, 'anual': 380,
    }
    # Campos de medición: configurarlos o que el cron los actualice NO es un
    # cambio al cuerpo del procedimiento (no dispara el candado G14).
    _SGI_MEASURE_FIELDS = {
        'measure_model_id', 'measure_model_name', 'measure_domain',
        'measure_date_field', 'measure_cadence', 'measure_last_date',
        'measure_count_30d', 'measure_state'}

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

    def _sgi_measure(self):
        """Recalcula la evidencia de cada actividad medible. Una actividad con
        dominio o modelo inválido queda sin semáforo, sin tumbar al resto."""
        now = fields.Datetime.now()
        for activity in self:
            vals = {'measure_last_date': False, 'measure_count_30d': 0,
                    'measure_state': False}
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
                vals['measure_count_30d'] = Model.search_count(
                    domain + [(date_field, '>=', now - timedelta(days=30))])
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
            activity.write(vals)

    def _sgi_resolve_menu(self):
        """Resuelve odoo_menu_id desde el texto de odoo_ref: convierte
        «Compras → Órdenes de compra» en el menú real. Solo menús con acción.
        Con varias rutas separadas por «·» se usa la primera."""
        Menu = self.env['ir.ui.menu'].sudo()
        for activity in self:
            ref = (activity.odoo_ref or '').split('·')[0].strip()
            if not ref or activity.odoo_menu_id:
                continue
            path = '/'.join(p.strip() for p in ref.replace('→', '/')
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
            if menu:
                activity.odoo_menu_id = menu

    @api.model
    def cron_measure_activities(self):
        # Primero intenta resolver menús pendientes desde su texto; después
        # mide. Así el paso queda navegable sin captura manual.
        self.search([('odoo_menu_id', '=', False),
                     ('odoo_ref', '!=', False)])._sgi_resolve_menu()
        self.search([('measure_model_id', '!=', False)])._sgi_measure()
        return True

    def action_view_measure_records(self):
        """Abre los registros reales que son la evidencia de la actividad."""
        self.ensure_one()
        if not self.measure_model_id:
            raise UserError(
                "Esta actividad no tiene modelo de medición ligado.")
        return {
            'type': 'ir.actions.act_window',
            'name': "%s — evidencia" % (
                self.name or self.number or self.section or 'Actividad'),
            'res_model': self.measure_model_id.model,
            'view_mode': 'list,form',
            'domain': self._sgi_measure_domain(),
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
        records = super().create(vals_list)
        records.process_id._sgi_flag_procedure_dirty()
        return records

    def write(self, vals):
        res = super().write(vals)
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
    is_cross_process = fields.Boolean(
        string="Cruza procesos", compute='_compute_cross', store=True)

    @api.depends('from_process_id', 'to_process_id')
    def _compute_cross(self):
        for link in self:
            link.is_cross_process = (
                link.from_process_id != link.to_process_id)

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
        (self.from_activity_id.process_id
         | self.to_activity_id.process_id)._sgi_flag_procedure_dirty()
        return res

    def unlink(self):
        processes = (self.from_activity_id.process_id
                     | self.to_activity_id.process_id)
        res = super().unlink()
        processes._sgi_flag_procedure_dirty()
        return res
