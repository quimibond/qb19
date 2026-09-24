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

from .sgi_catalog import SGI_ROLE_SELECTION

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
                detailed.append({
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
                })
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
