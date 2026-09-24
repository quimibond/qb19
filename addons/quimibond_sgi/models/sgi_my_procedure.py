# -*- coding: utf-8 -*-
"""«Mi procedimiento» por puesto.

Un solo documento imprimible por puesto (y por empleado, vía su puesto) con
todas sus actividades de todos los procesos, tomadas de los roles del puesto
y de su familia (``hr.job.sgi_all_role_ids``):

- Una sección por cadencia real (diario, semanal, quincenal, mensual,
  trimestral, semestral, anual, «cuando ocurre»), solo las que tengan
  actividades.
- Lo que el puesto ejecuta o aprueba va completo: cuándo (día de la semana o
  día hábil del mes), rol, proceso y numeral, y todas las piezas de la frase
  del procedimiento (dónde, cómo, criterio de terminado, si falla, entradas,
  salidas, instructivo, a quién escala).
- Lo que solo participa o de lo que solo se entera va en una lista corta al
  final.
- Escalamientos que recibe de actividades de otros puestos.
- Portada: puesto, personas, jefe inmediato, procesos y conteo por rol.

El PDF se archiva como documento controlado del puesto (tipo «Mi
procedimiento (MP)», clave ``MP-<puesto>``) SOLO cuando cambia el contenido
(hash de las piezas, no del PDF) y cada revisión nueva deja pendiente el
acuse «leído y entendido» de todos los empleados del puesto con el mecanismo
de siempre (``sgi.document.ack``): la firma queda contra la versión exacta
que la persona leyó.
"""
import base64
import hashlib
import json
import logging

from markupsafe import Markup

from odoo import api, fields, models
from odoo.exceptions import UserError

from .sgi_catalog import SGI_ROLE_SELECTION, sgi_normalize_name

_logger = logging.getLogger(__name__)

# Orden fijo de las secciones y su título en la hoja.
SGI_MP_CADENCES = [
    ('diaria', "Diario"),
    ('semanal', "Semanal"),
    ('quincenal', "Quincenal"),
    ('mensual', "Mensual"),
    ('trimestral', "Trimestral"),
    ('semestral', "Semestral"),
    ('anual', "Anual"),
    ('evento', "Cuando ocurre"),
]
_CADENCE_RANK = {code: i for i, (code, _l) in enumerate(SGI_MP_CADENCES)}
_DETAIL_ROLES = ('ejecuta', 'aprueba')
_SHORT_ROLES = ('participa', 'informa')


class DocumentsDocumentMyProcedure(models.Model):
    _inherit = 'documents.document'

    sgi_doc_type = fields.Selection(
        selection_add=[('mi_procedimiento', "Mi procedimiento (MP)")],
        ondelete={'mi_procedimiento': 'set null'})
    # Huella del contenido con el que se generó el PDF: una revisión nueva
    # solo cuando cambia (no cada vez que alguien imprime).
    sgi_content_hash = fields.Char(
        string="Huella del contenido", readonly=True, copy=False, index=True)


class SgiActivityRoleCadence(models.Model):
    _inherit = 'sgi.activity.role'

    # Para agrupar la vista «Mi procedimiento» por cadencia.
    cadence = fields.Selection(
        related='activity_id.measure_cadence', string="Cadencia", store=True)
    activity_active = fields.Boolean(
        related='activity_id.active', string="Actividad activa", store=True)
    activity_menu_id = fields.Many2one(
        related='activity_id.odoo_menu_id', string="Menú de Odoo")
    activity_when = fields.Char(string="Cuándo", compute='_compute_activity_when')

    @api.depends('activity_id.measure_cadence', 'activity_id.due_weekday',
                 'activity_id.due_business_day')
    def _compute_activity_when(self):
        Job = self.env['hr.job']
        for role in self:
            role.activity_when = Job._sgi_mp_when(role.activity_id)[1] if role.activity_id else ''

    def action_open_activity(self):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window', 'res_model': 'sgi.process.activity',
            'res_id': self.activity_id.id, 'view_mode': 'form',
            'name': self.activity_id.display_name,
        }


class HrJobMyProcedure(models.Model):
    _inherit = 'hr.job'

    sgi_my_procedure_doc_id = fields.Many2one(
        'documents.document', string="Mi procedimiento (vigente)",
        compute='_compute_sgi_my_procedure_doc')
    sgi_my_procedure_stale = fields.Boolean(
        string="Mi procedimiento desactualizado",
        compute='_compute_sgi_my_procedure_doc',
        help="Las actividades del puesto cambiaron desde la última revisión "
             "publicada de «Mi procedimiento».")

    # ------------------------------------------------------------------
    # Documento
    # ------------------------------------------------------------------
    def _sgi_my_procedure_code(self):
        self.ensure_one()
        return "MP-%03d" % self.id

    def _sgi_my_procedure_current_doc(self):
        self.ensure_one()
        if not self.id:
            return self.env['documents.document']
        return self.env['documents.document'].sudo().search([
            ('sgi_code', '=', self._sgi_my_procedure_code()),
            ('sgi_state', '=', 'vigente'),
        ], order='sgi_revision desc, id desc', limit=1)

    def _compute_sgi_my_procedure_doc(self):
        for job in self:
            doc = job._sgi_my_procedure_current_doc()
            job.sgi_my_procedure_doc_id = doc
            job.sgi_my_procedure_stale = bool(
                doc and job.id and doc.sgi_content_hash != job._sgi_my_procedure_data()['hash'])

    # ------------------------------------------------------------------
    # Datos
    # ------------------------------------------------------------------
    @api.model
    def _sgi_mp_when(self, activity):
        """(clave de orden, texto) del «cuándo» concreto de una actividad."""
        cadence = activity.measure_cadence or 'evento'
        if cadence == 'semanal' and activity.due_weekday:
            label = dict(activity._fields['due_weekday'].selection)[activity.due_weekday]
            return ((0, int(activity.due_weekday)), "Cada %s" % label.lower())
        if cadence in ('mensual', 'quincenal', 'trimestral', 'semestral', 'anual') \
                and activity.due_business_day:
            return ((1, activity.due_business_day),
                    "Día hábil %d del mes" % activity.due_business_day)
        return ((9, 0), "")

    def _sgi_mp_role_label(self, role):
        return dict(SGI_ROLE_SELECTION).get(role, role)

    def _sgi_my_procedure_data(self):
        """Todo lo que imprime y lo que se compara: secciones por cadencia,
        lista corta, escalamientos, portada y huella."""
        self.ensure_one()
        Role = self.env['sgi.activity.role'].sudo()
        roles = Role.search(self._sgi_roles_domain()) if self.id else Role
        roles = roles.filtered(lambda r: r.activity_id.active)
        role_labels = dict(SGI_ROLE_SELECTION)

        by_activity = {}
        for role in roles:
            by_activity.setdefault(role.activity_id, set()).add(role.role)

        detailed, short = [], []
        status_map = self._sgi_mp_status_map(
            self.env['sgi.process.activity'].sudo().browse([a.id for a in by_activity]))
        for activity, role_codes in by_activity.items():
            mine = sorted(role_codes & set(_DETAIL_ROLES),
                          key=lambda r: _DETAIL_ROLES.index(r))
            if mine:
                when_key, when = self._sgi_mp_when(activity)
                parts = [(label, text) for label, text in activity._sgi_sentence_parts()
                         if not (label is None and text.startswith("Vence"))]
                escalates = [
                    "%s%s" % (r._sgi_target_label(),
                              " (a los %d días hábiles)" % r.after_days if r.after_days else "")
                    for r in activity.role_ids.filtered(lambda r: r.role == 'escala')]
                detailed.append(dict({
                    'activity': activity,
                    'cadence': activity.measure_cadence or 'evento',
                    'roles': [role_labels[r] for r in mine],
                    'role_codes': mine,
                    'when_key': when_key,
                    'when': when,
                    'process': activity.process_id,
                    'number': activity.number or activity.legacy_number or '',
                    'name': activity.name,
                    'parts': parts,
                    'escalates_to': escalates,
                }, **self._sgi_mp_entry_extra(activity, status_map.get(activity.sudo()))))
            else:
                only = sorted(role_codes & set(_SHORT_ROLES),
                              key=lambda r: _SHORT_ROLES.index(r))
                if only:
                    short.append({
                        'activity': activity,
                        'roles': [role_labels[r] for r in only],
                        'process': activity.process_id,
                        'number': activity.number or activity.legacy_number or '',
                        'name': activity.name,
                    })

        def sort_key(entry):
            return (entry['when_key'], entry['process'].code or '', entry['number'], entry['name'])

        sections = []
        for code, label in SGI_MP_CADENCES:
            entries = sorted([e for e in detailed if e['cadence'] == code], key=sort_key)
            if entries:
                sections.append({'code': code, 'label': label, 'entries': entries})
        short.sort(key=lambda e: (e['process'].code or '', e['number'], e['name']))

        # Escalamientos que recibe: roles «escala» de otras actividades que
        # apuntan a este puesto o a su familia.
        received = []
        for role in roles.filtered(lambda r: r.role == 'escala'):
            activity = role.activity_id
            executors = activity.role_ids.filtered(lambda r: r.role == 'ejecuta')
            received.append({
                'activity': activity,
                'process': activity.process_id,
                'number': activity.number or activity.legacy_number or '',
                'name': activity.name,
                'from': ", ".join(r._sgi_target_label() for r in executors),
                'after_days': role.after_days,
            })
        received.sort(key=lambda e: (e['process'].code or '', e['number']))

        # Portada.
        employees = self.env['hr.employee'].sudo().search(
            [('job_id', '=', self.id)], order='name') if self.id else self.env['hr.employee']
        counts = {code: 0 for code, _l in SGI_ROLE_SELECTION}
        for role in roles:
            counts[role.role] = counts.get(role.role, 0) + 1
        processes = sorted(set(roles.mapped('activity_id.process_id')),
                           key=lambda p: (p.code or '', p.name or ''))
        cover = {
            'job': self,
            'department': self.department_id,
            'manager': self.department_id.manager_id,
            'employees': employees,
            'family': self.sgi_family_id,
            'processes': processes,
            'counts': [(role_labels[c], counts[c]) for c, _l in SGI_ROLE_SELECTION if counts[c]],
        }

        data = {
            'sections': sections,
            'short': short,
            'received': received,
            'cover': cover,
            'total': len(detailed) + len(short),
        }
        data['hash'] = self._sgi_my_procedure_hash(data)
        return data

    @api.model
    def _sgi_mp_status_map(self, activities):
        """Estado de ejecución de varias actividades en UNA consulta: la última
        semana medida de cada una (`sgi.activity.week.stat`, entradas con plazo
        vencido sin salida) más el cumplimiento que escribe el cron
        (`measure_state`). Devuelve {actividad: (código, etiqueta, detalle)}."""
        late_open = {}
        if activities:
            for stat in self.env['sgi.activity.week.stat'].sudo().search(
                    [('activity_id', 'in', activities.ids)], order='period_start desc'):
                late_open.setdefault(stat.activity_id.id, stat.late_open_count)
        result = {}
        for activity in activities:
            late = late_open.get(activity.id, 0)
            last = activity.measure_last_date
            last_txt = fields.Date.to_string(fields.Datetime.context_timestamp(
                self, last).date()) if last else ''
            if late:
                result[activity] = ('atrasada', "Atrasada",
                                    "%d entrada(s) con plazo vencido sin salida" % late)
            elif activity.measure_state == 'rojo':
                result[activity] = ('atrasada', "Atrasada", "Sin evidencia en su periodo")
            elif activity.measure_state == 'verde':
                result[activity] = ('al_dia', "Al día",
                                    "Última ejecución %s" % last_txt if last_txt else "")
            else:
                # Neutro: muchas actividades no tienen medición automática y
                # eso no es una falla de quien las hace.
                result[activity] = ('sin_medir', "Sin medición automática", "")
        return result

    @api.model
    def _sgi_mp_status(self, activity):
        return self._sgi_mp_status_map(activity)[activity]

    @api.model
    def _sgi_mp_entry_extra(self, activity, status=None):
        """Piezas sueltas para la pantalla (la frase armada es para el PDF):
        estado, dónde ir a hacerlo, instructivo, entradas con plazo y salidas."""
        status, status_label, status_detail = status or self._sgi_mp_status(activity)
        action = activity.odoo_action_id
        action_url = '/odoo/action-%d' % action.id if action else ''
        instruction = activity.instruction_id
        instruction_url = ''
        if instruction:
            if instruction.attachment_id:
                instruction_url = '/web/content/%d?filename=%s' % (
                    instruction.attachment_id.id, instruction.name or '')
            elif instruction.url:
                instruction_url = instruction.url
        where = []
        if activity.exec_channel:
            where.append(dict(activity._fields['exec_channel'].selection)[activity.exec_channel])
        if activity.odoo_menu_id:
            where.append(activity.odoo_menu_id.complete_name)
        if activity.external_system:
            where.append(activity.external_system)
        if activity.location_id:
            where.append(activity.location_id.complete_name)
        if activity.workcenter_id:
            where.append(activity.workcenter_id.display_name)
        if activity.place_note:
            where.append(activity.place_note)
        return {
            'status': status, 'status_label': status_label, 'status_detail': status_detail,
            'action_url': action_url,
            'external': activity.external_system or '',
            'activity_url': '/odoo/sgi.process.activity/%d' % activity.id,
            'instruction': instruction.sgi_code or instruction.name if instruction else '',
            'instruction_url': instruction_url,
            'where': " — ".join(where),
            'how': " ".join((activity.how_steps or '').split()),
            'check_against': activity.check_against or '',
            'done': (activity.done_criteria or '').strip(),
            'on_fail': (activity.on_fail or '').strip(),
            'inputs': [(line.deliverable_id.name, line.max_days) for line in activity.input_ids],
            'outputs': activity.output_deliverable_ids.mapped('name'),
            'related': (activity.related_procedure_id.sgi_code
                        or activity.related_procedure_id.name) if activity.related_procedure_id else '',
        }

    @api.model
    def _sgi_my_procedure_hash(self, data):
        """Huella del contenido que se lee (no del PDF, que cambia con la
        fecha): solo las piezas que cambian lo que la persona debe hacer."""
        payload = {
            'sections': [[
                s['code'],
                [[e['activity'].id, e['role_codes'], e['when'], e['number'], e['name'],
                  [[label, text] for label, text in e['parts']], e['escalates_to']]
                 for e in s['entries']]] for s in data['sections']],
            'short': [[e['activity'].id, e['roles'], e['number'], e['name']] for e in data['short']],
            'received': [[e['activity'].id, e['from'], e['after_days']] for e in data['received']],
        }
        raw = json.dumps(payload, sort_keys=True, ensure_ascii=False, default=str)
        return hashlib.sha256(raw.encode('utf-8')).hexdigest()

    @api.model
    def _sgi_mp_parts_html(self, parts):
        """Las piezas con la etiqueta en negritas (misma forma que el F-P-G01-02)."""
        return Markup(" ").join(
            Markup("<b>%s:</b> %s.") % (label, text) if label else Markup("%s.") % text
            for label, text in parts)

    def _sgi_my_procedure_header(self):
        """Clave, revisión y fecha que imprime la caja de control. Al publicar
        vienen por contexto (el documento aún no existe); al imprimir a mano
        salen del vigente, marcado «sin publicar» si el contenido ya cambió."""
        self.ensure_one()
        ctx = self.env.context
        if ctx.get('sgi_mp_revision') is not None:
            return {'code': self._sgi_my_procedure_code(),
                    'revision': "%02d" % ctx['sgi_mp_revision'],
                    'issue_date': ctx.get('sgi_mp_issue_date') or fields.Date.context_today(self),
                    'stale': False, 'doc': self.env['documents.document']}
        doc = self._sgi_my_procedure_current_doc()
        if doc:
            return {'code': doc.sgi_code, 'revision': doc.sgi_revision_label,
                    'issue_date': doc.sgi_issue_date, 'doc': doc,
                    'stale': doc.sgi_content_hash != self._sgi_my_procedure_data()['hash']}
        return {'code': self._sgi_my_procedure_code(), 'revision': "—",
                'issue_date': False, 'stale': True, 'doc': doc}

    # ------------------------------------------------------------------
    # Acciones
    # ------------------------------------------------------------------
    def action_sgi_print_my_procedure(self):
        return self.env.ref('quimibond_sgi.action_report_my_procedure').report_action(self)

    def action_sgi_publish_my_procedure(self):
        """Archiva el PDF como documento controlado del puesto (revisión nueva
        solo si cambió el contenido) y deja pendiente el acuse de todos los
        empleados del puesto."""
        if not self.env.user.has_group('quimibond_sgi.group_sgi_manager'):
            raise UserError("Solo el Jefe MAST publica «Mi procedimiento».")
        Doc = self.env['documents.document'].sudo()
        published, unchanged, empty = self.env['documents.document'], [], []
        today = fields.Date.context_today(self)
        for job in self:
            data = job._sgi_my_procedure_data()
            if not data['total']:
                empty.append(job.name)
                continue
            code = job._sgi_my_procedure_code()
            current = job._sgi_my_procedure_current_doc()
            if current and current.sgi_content_hash == data['hash']:
                unchanged.append(job.name)
                continue
            previous = Doc.with_context(active_test=False).search([('sgi_code', '=', code)])
            revision = (max(previous.mapped('sgi_revision')) + 1) if previous else 0
            report = self.env.ref('quimibond_sgi.action_report_my_procedure')
            pdf, _ = self.env['ir.actions.report'].with_context(
                sgi_mp_revision=revision, sgi_mp_issue_date=today,
            )._render_qweb_pdf(report.report_name, job.ids)
            doc = Doc.create({
                'name': "Mi procedimiento — %s (Rev. %02d).pdf" % (job.name, revision),
                'type': 'binary',
                'datas': base64.b64encode(pdf),
                'mimetype': 'application/pdf',
                'sgi_is_controlled': True,
                'sgi_doc_type': 'mi_procedimiento',
                'sgi_code': code,
                'sgi_state': 'vigente',
                'sgi_revision': revision,
                'sgi_issue_date': today,
                'sgi_job_ids': [(6, 0, [job.id])],
                'sgi_content_hash': data['hash'],
                'sgi_owner_id': self.env.user.id,
                'company_id': job.company_id.id or self.env.company.id,
            })
            doc.action_generate_acks()
            doc.message_post(body=Markup(
                "«Mi procedimiento» de <b>%s</b>, revisión %02d: %d actividades "
                "(%d con detalle). Acuse pendiente para %d persona(s).") % (
                job.name, revision, data['total'], sum(len(s['entries']) for s in data['sections']),
                len(doc.sgi_ack_ids)))
            published |= doc
            _logger.info("SGI Mi procedimiento: %s rev %02d publicado (%s).",
                         code, revision, job.name)
        parts = []
        if published:
            parts.append("%d publicado(s)" % len(published))
        if unchanged:
            parts.append("sin cambios: %s" % ", ".join(unchanged))
        if empty:
            parts.append("sin actividades: %s" % ", ".join(empty))
        if len(self) == 1 and published:
            return {
                'type': 'ir.actions.act_window', 'res_model': 'documents.document',
                'res_id': published.id, 'view_mode': 'form',
                'views': [(self.env.ref('quimibond_sgi.sgi_document_view_form').id, 'form')],
                'name': published.name,
            }
        return {
            'type': 'ir.actions.client', 'tag': 'display_notification',
            'params': {'type': 'success' if published else 'warning',
                       'message': "Mi procedimiento: %s." % "; ".join(parts)},
        }

    @api.model
    def _sgi_my_procedure_jobs(self):
        """Puestos que tienen «Mi procedimiento» que publicar: con roles
        (propios o de su familia) y con personas."""
        Role = self.env['sgi.activity.role'].sudo()
        jobs = self.env['hr.job'].sudo().search([])
        families = jobs.sgi_family_id
        roles = Role.search(['|', ('job_id', 'in', jobs.ids), ('family_id', 'in', families.ids)])
        roles = roles.filtered(lambda r: r.activity_id.active)
        with_roles = roles.job_id | roles.family_id.job_ids
        staffed = self.env['hr.employee'].sudo().search([('job_id', 'in', with_roles.ids)]).job_id
        return with_roles & staffed

    @api.model
    def action_sgi_publish_all_my_procedures(self):
        """Publica (o deja igual) «Mi procedimiento» de todos los puestos con
        roles y personas. Una revisión nueva solo donde cambió el contenido."""
        if not self.env.user.has_group('quimibond_sgi.group_sgi_manager'):
            raise UserError("Solo el Jefe MAST publica «Mi procedimiento».")
        jobs = self._sgi_my_procedure_jobs()
        published = unchanged = 0
        for job in jobs:
            before = job._sgi_my_procedure_current_doc()
            job.action_sgi_publish_my_procedure()
            after = job._sgi_my_procedure_current_doc()
            if after and after != before:
                published += 1
            else:
                unchanged += 1
        _logger.info("SGI Mi procedimiento: publicación masiva, %d nuevas, %d sin cambio.",
                     published, unchanged)
        return {
            'type': 'ir.actions.client', 'tag': 'display_notification',
            'params': {'type': 'success', 'sticky': True,
                       'message': "Mi procedimiento: %d puesto(s) con revisión nueva, %d sin "
                                  "cambios, de %d con roles y personas." % (
                                      published, unchanged, len(jobs))},
        }

    @api.model
    def _sgi_my_procedure_stale_jobs(self):
        """Puestos con personas y roles cuya revisión publicada no existe o ya
        no coincide con sus actividades."""
        return self._sgi_my_procedure_jobs().filtered(
            lambda j: not j._sgi_my_procedure_current_doc()
            or j._sgi_my_procedure_current_doc().sgi_content_hash != j._sgi_my_procedure_data()['hash'])

    @api.model
    def _sgi_my_procedure_precheck(self):
        """Lo que hay que limpiar ANTES de publicar para toda la planta:
        puestos duplicados (mismo nombre normalizado), empleados sin puesto o
        en un puesto sin roles, y puestos con roles pero sin personas."""
        Job = self.env['hr.job'].sudo()
        Employee = self.env['hr.employee'].sudo()
        Role = self.env['sgi.activity.role'].sudo()
        jobs = Job.search([])
        roles = Role.search([('activity_id.active', '=', True)])
        with_roles = roles.job_id | roles.family_id.job_ids
        staffed_jobs = Employee.search([('job_id', '!=', False)]).job_id

        by_name = {}
        for job in jobs:
            by_name.setdefault((job.company_id.id, sgi_normalize_name(job.name)), Job)
            by_name[(job.company_id.id, sgi_normalize_name(job.name))] |= job
        duplicates = [group for group in by_name.values() if len(group) > 1]
        duplicates.sort(key=lambda g: sgi_normalize_name(g[0].name))

        no_job = Employee.search([('job_id', '=', False)], order='name')
        job_without_roles = Employee.search(
            [('job_id', '!=', False), ('job_id', 'not in', with_roles.ids)], order='job_id, name')
        roles_without_people = (with_roles - staffed_jobs).sorted('name')
        return {
            'duplicates': duplicates,
            'no_job': no_job,
            'job_without_roles': job_without_roles,
            'roles_without_people': roles_without_people,
            'ready': len(with_roles & staffed_jobs),
        }

    def action_sgi_open_my_procedure_doc(self):
        self.ensure_one()
        doc = self._sgi_my_procedure_current_doc()
        if not doc:
            raise UserError("Este puesto aún no tiene publicado «Mi procedimiento».")
        return {
            'type': 'ir.actions.act_window', 'res_model': 'documents.document',
            'res_id': doc.id, 'view_mode': 'form',
            'views': [(self.env.ref('quimibond_sgi.sgi_document_view_form').id, 'form')],
            'name': doc.name,
        }

    def _sgi_my_procedure_view_action(self):
        """La misma información como vista de Odoo: roles del puesto agrupados
        por cadencia, con la actividad a un clic."""
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'name': "Mi procedimiento — %s" % self.display_name,
            'res_model': 'sgi.activity.role',
            'view_mode': 'list,form',
            'views': [(self.env.ref('quimibond_sgi.sgi_activity_role_view_list_my_procedure').id, 'list'),
                      (False, 'form')],
            'domain': self._sgi_roles_domain() + [('activity_active', '=', True)],
            'context': {'group_by': ['cadence', 'role'], 'create': False, 'edit': False},
            'help': "<p class='o_view_nocontent_smiling_face'>Sin actividades para "
                    "este puesto</p><p>Pide a MAST que capture los roles del "
                    "puesto en las actividades de procedimiento.</p>",
        }


class HrEmployeeMyProcedure(models.Model):
    _inherit = 'hr.employee'

    sgi_my_procedure_ack_state = fields.Selection([
        ('sin_publicar', "Sin publicar"),
        ('pendiente', "Acuse pendiente"),
        ('leido', "Leído y entendido"),
    ], string="Mi procedimiento", compute='_compute_sgi_my_procedure_ack')

    def _compute_sgi_my_procedure_ack(self):
        Ack = self.env['sgi.document.ack'].sudo()
        for emp in self:
            doc = emp.job_id._sgi_my_procedure_current_doc() if emp.job_id else False
            if not doc:
                emp.sgi_my_procedure_ack_state = 'sin_publicar'
                continue
            ack = Ack.search([('document_id', '=', doc.id), ('employee_id', '=', emp.id)], limit=1)
            emp.sgi_my_procedure_ack_state = 'leido' if ack and ack.state == 'leido' else 'pendiente'

    def _sgi_require_job(self):
        self.ensure_one()
        if not self.job_id:
            raise UserError(
                "%s no tiene puesto asignado. Pide a RH que lo capture en la "
                "ficha del empleado." % self.name)
        return self.job_id

    def action_sgi_print_my_procedure(self):
        return self._sgi_require_job().with_context(
            sgi_mp_employee_id=self.id).action_sgi_print_my_procedure()

    def action_sgi_my_procedure_view(self):
        return self._sgi_require_job()._sgi_my_procedure_view_action()


class SgiCronMyProcedure(models.AbstractModel):
    _inherit = 'sgi.cron'

    @api.model
    def cron_my_procedure_stale(self):
        """Semanal: avisa al Jefe MAST qué puestos con personas tienen «Mi
        procedimiento» sin publicar o desactualizado. Una sola actividad,
        sobre la revisión vigente del primer puesto desactualizado
        (documents.document lleva actividades); si nadie ha publicado nada,
        solo queda en el log."""
        Job = self.env['hr.job']
        stale = Job._sgi_my_procedure_stale_jobs()
        if not stale:
            return True
        summary = "Mi procedimiento: %d puesto(s) por publicar" % len(stale)
        note = "Puestos con personas cuya revisión no existe o ya no coincide con sus " \
               "actividades: %s. Publícalos desde SGI → Inicio → Mi procedimiento " \
               "(«Publicar todos los puestos»)." % ", ".join(stale.mapped('name'))
        # La actividad cuelga de la revisión vigente del primer puesto
        # desactualizado (ahí va a trabajar MAST); si ninguno tiene revisión,
        # de la más reciente publicada.
        anchor = self.env['documents.document'].sudo()
        for job in stale:
            anchor = job._sgi_my_procedure_current_doc()
            if anchor:
                break
        if not anchor:
            anchor = self.env['documents.document'].sudo().search(
                [('sgi_doc_type', '=', 'mi_procedimiento'), ('sgi_state', '=', 'vigente')],
                order='sgi_issue_date desc, id desc', limit=1)
        if anchor:
            self._sgi_schedule(anchor, summary, note, self._sgi_manager_user_id())
        _logger.info("SGI Mi procedimiento: %s", note)
        return True
