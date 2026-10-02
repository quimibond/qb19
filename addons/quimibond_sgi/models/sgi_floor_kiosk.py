# -*- coding: utf-8 -*-
"""57.94.0 «SGI en planta» (auditoría 2026-10: U-01, U-08).

La planta firma en la tableta compartida a su nombre:

- ``sgi.floor.tablet``: qué cuenta compartida es tableta (supervisor@,
  manufactura@…), de qué departamentos y qué checklists se llenan ahí. La da
  de alta el Jefe MAST; la cuenta recibe el grupo «Tableta de planta (SGI)».
- ``sgi.floor.kiosk``: lo que llama la pantalla (acción cliente
  ``sgi_floor_kiosk``). CADA método vuelve a validar la tableta, que la
  persona sea de sus departamentos y su PIN (``sgi.pin``), y escribe con sudo
  poniendo al empleado y la tableta. La cuenta de la tableta no tiene ACL de
  acuses, incidentes, EPP ni checklist: nada se firma a su nombre.
- RH (U-08): «Le falta» en el empleado, la lista «Empleados sin puesto, sin
  PIN o sin correo» y un aviso semanal por departamento en Mis pendientes.
"""
import base64
import textwrap
from datetime import timedelta

from odoo import api, fields, models
from odoo.exceptions import AccessError, UserError, ValidationError
from odoo.tools.mimetypes import guess_mimetype

from .sgi_calendar import sgi_today
from .sgi_menu_paths import sgi_menu_path

KIOSK_IDLE_SECONDS = 90
KIOSK_FILE_LIMIT = 15 * 1024 * 1024
# Solo PDF e imágenes se abren en la tableta (ver kiosk_document_file).
KIOSK_MIMETYPES = ('application/pdf', 'image/png', 'image/jpeg')
CHECKLIST_DAYS_BACK = 6


def _kiosk_int(value):
    """Id que manda la pantalla: entero o 0 (nunca un error de servidor)."""
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


class SgiFloorTablet(models.Model):
    """Tableta de planta: una cuenta compartida, sus departamentos y sus checklists."""
    _name = 'sgi.floor.tablet'
    _description = "Tableta de planta (SGI en planta)"
    _order = 'name'

    name = fields.Char(string="Tableta", required=True,
                       help="Nombre que ve la gente y que queda en lo firmado: «Tableta Tejido», "
                            "«Tableta Mantenimiento».")
    user_id = fields.Many2one(
        'res.users', string="Cuenta de la tableta", required=True, ondelete='restrict',
        domain="[('share', '=', False)]",
        help="Usuario compartido con el que se abre la tableta (por ejemplo supervisor@). No debe "
             "estar ligado a un empleado: lo firmado queda a nombre de quien teclea su PIN.")
    department_ids = fields.Many2many(
        'hr.department', 'sgi_floor_tablet_department_rel', 'tablet_id', 'department_id',
        string="Departamentos", required=True,
        help="Las personas de estos departamentos (y de sus subdepartamentos) salen en la pantalla "
             "y pueden firmar en esta tableta.")
    checklist_template_ids = fields.Many2many(
        'sgi.checklist.template', 'sgi_floor_tablet_checklist_rel', 'tablet_id', 'template_id',
        string="Checklists que se llenan aquí",
        help="Las hojas de estas plantillas salen en «Checklist de mi equipo» de la tableta.")
    company_id = fields.Many2one('res.company', string="Empresa", required=True,
                                 default=lambda self: self.env['sgi.config']._sgi_company(),
                                 help="Empresa del SGI: solo su gente sale en la tableta.")
    active = fields.Boolean(default=True,
                            help="Archive la tableta para que deje de abrir SGI en planta sin borrarla "
                                 "(lo firmado en ella la sigue citando).")
    note = fields.Text(string="Dónde está", help="Lugar de la tableta y quién la cuida.")

    _user_uniq = models.Constraint('unique(user_id)', "Esa cuenta ya es de otra tableta.")

    @api.constrains('user_id')
    def _check_shared_user(self):
        for tablet in self:
            # El dominio share=False es solo de la vista: un usuario de portal
            # no puede recibir el grupo (implica Usuario interno).
            if tablet.user_id.share:
                raise ValidationError("La cuenta %s es de portal: una tableta usa un usuario interno."
                                      % tablet.user_id.login)
            if tablet.user_id.sudo().employee_ids:
                raise ValidationError(
                    "La cuenta %s está ligada a un empleado. Una tableta usa una cuenta compartida, "
                    "sin empleado: lo firmado queda a nombre de quien teclea su PIN." % tablet.user_id.login)

    @api.model_create_multi
    def create(self, vals_list):
        tablets = super().create(vals_list)
        tablets._sgi_grant_group()
        return tablets

    def write(self, vals):
        res = super().write(vals)
        if 'user_id' in vals or vals.get('active'):
            self._sgi_grant_group()
        return res

    def _sgi_grant_group(self):
        """La cuenta de una tableta activa recibe «Tableta de planta (SGI)».
        No quita grupos: eso lo decide Jose y lo hace Sistemas (Q8)."""
        group = self.env.ref('quimibond_sgi.group_sgi_floor_tablet').sudo()
        users = self.filtered('active').user_id - group.user_ids
        if users:
            group.write({'user_ids': [(4, user.id) for user in users]})


class SgiFloorKiosk(models.AbstractModel):
    """Servicios de la pantalla «SGI en planta». Cada método público valida la tableta, a la
    persona y su PIN, y escribe con sudo a nombre de la persona."""
    _name = 'sgi.floor.kiosk'
    _description = "SGI en planta: servicios de la pantalla de la tableta"

    # ------------------------------------------------------------------
    # Identidad: tableta, persona y PIN (en cada llamada)
    # ------------------------------------------------------------------
    @api.model
    def _kiosk_tablet(self):
        user = self.env.user
        if not (user.has_group('quimibond_sgi.group_sgi_floor_tablet')
                or user.has_group('quimibond_sgi.group_sgi_manager')):
            raise AccessError("SGI en planta es solo para las tabletas de planta.")
        tablet = self.env['sgi.floor.tablet'].sudo().search(
            [('user_id', '=', user.id), ('active', '=', True)], limit=1)
        if not tablet:
            raise AccessError(
                "Esta cuenta no está dada de alta como tableta de planta. Pida al Jefe MAST que la "
                "registre en %s." % sgi_menu_path('tabletas_planta'))
        return tablet

    @api.model
    def _kiosk_domain(self, tablet):
        return [('active', '=', True), ('company_id', '=', tablet.company_id.id),
                ('department_id', 'child_of', tablet.department_ids.ids)]

    @api.model
    def _kiosk_person(self, employee_id, pin):
        tablet = self._kiosk_tablet()
        Employee = self.env['hr.employee'].sudo()
        employee = Employee.browse(_kiosk_int(employee_id)).exists()
        if not employee or not Employee.search_count(
                self._kiosk_domain(tablet) + [('id', '=', employee.id)]):
            raise UserError("Esa persona no está en los departamentos de esta tableta.")
        self.env['sgi.pin']._sgi_check_pin(employee, pin, required=True, purpose="firmar en la tableta")
        return tablet, employee

    @api.model
    def _kiosk_card(self, employee):
        avatar = employee.avatar_128 or b''
        if isinstance(avatar, str):
            avatar = avatar.encode()
        src = False
        if avatar:
            mimetype = guess_mimetype(base64.b64decode(avatar), default='image/png')
            src = 'data:%s;base64,%s' % (mimetype, avatar.decode())
        return {'id': employee.id, 'name': employee.name, 'job': employee.job_id.name or '',
                'department': employee.department_id.name or '', 'has_pin': bool(employee.pin),
                'avatar': src}

    # ------------------------------------------------------------------
    # Mosaico y menú de la persona
    # ------------------------------------------------------------------
    @api.model
    def kiosk_employees(self, department_id=None):
        tablet = self._kiosk_tablet()
        domain = self._kiosk_domain(tablet)
        if department_id:
            allowed = self.env['hr.department'].sudo().search(
                [('id', 'child_of', tablet.department_ids.ids)])
            department_id = _kiosk_int(department_id)
            if department_id not in allowed.ids:
                raise UserError("Ese departamento no es de esta tableta.")
            domain = domain + [('department_id', 'child_of', department_id)]
        employees = self.env['hr.employee'].sudo().search(domain, order='name')
        return {
            'tablet': {'id': tablet.id, 'name': tablet.name},
            'departments': [{'id': d.id, 'name': d.name} for d in tablet.department_ids.sudo()],
            'idle_seconds': KIOSK_IDLE_SECONDS,
            'employees': [self._kiosk_card(emp) for emp in employees],
        }

    @api.model
    def kiosk_check_pin(self, employee_id, pin):
        tablet, employee = self._kiosk_person(employee_id, pin)
        return {
            'employee': {'id': employee.id, 'name': employee.name, 'job': employee.job_id.name or ''},
            'counts': {'docs': len(self._kiosk_acks(employee)),
                       'epp': len(self._kiosk_epp_records(employee)),
                       'checklists': len(self._kiosk_checklist_records(tablet, employee))},
        }

    # ------------------------------------------------------------------
    # Documentos por leer (acuses)
    # ------------------------------------------------------------------
    @api.model
    def _kiosk_acks(self, employee):
        # Mismo dominio que Mis pendientes (sgi_my_pending.py, acuses).
        return self.env['sgi.document.ack'].sudo().search(
            [('employee_id', '=', employee.id), ('state', '=', 'pendiente'),
             ('document_id.active', '=', True)], order='create_date')

    @api.model
    def _kiosk_own_ack(self, employee, ack_id):
        ack = self.env['sgi.document.ack'].sudo().browse(_kiosk_int(ack_id)).exists()
        if not ack or ack.employee_id != employee:
            raise UserError("Ese acuse no es suyo.")
        return ack

    @api.model
    def kiosk_pending_docs(self, employee_id, pin):
        __, employee = self._kiosk_person(employee_id, pin)
        rows = []
        for ack in self._kiosk_acks(employee):
            doc = ack.document_id
            title = doc.sgi_title if 'sgi_title' in doc._fields else False
            revision = doc.sgi_revision_label if 'sgi_revision_label' in doc._fields else False
            rows.append({'ack_id': ack.id, 'title': title or doc.name or '', 'revision': revision or '',
                         'has_file': bool(doc.attachment_id or (doc.type == 'url' and doc.url))})
        return rows

    @api.model
    def kiosk_document_file(self, employee_id, pin, ack_id):
        __, employee = self._kiosk_person(employee_id, pin)
        doc = self._kiosk_own_ack(employee, ack_id).document_id
        if doc.attachment_id:
            # Solo PDF e imágenes: la pantalla arma un blob: con el tipo que
            # dice el servidor y lo pone en un <iframe> del mismo origen; un
            # HTML o SVG subido como documento correría su JavaScript con la
            # sesión de la tableta.
            mimetype = doc.attachment_id.mimetype or ''
            if mimetype not in KIOSK_MIMETYPES:
                raise UserError("Este documento no se puede abrir en la tableta (solo PDF o imagen). "
                                "Pida a su jefe que se lo muestre en una computadora.")
            raw = doc.attachment_id.raw or b''
            if len(raw) > KIOSK_FILE_LIMIT:
                raise UserError("El archivo es demasiado grande para la tableta. Pida a su jefe que se "
                                "lo muestre en una computadora.")
            return {'name': doc.attachment_id.name or doc.name, 'mimetype': mimetype,
                    'data': base64.b64encode(raw).decode()}
        # Un enlace solo si es web: «javascript:» o «data:» en un documento
        # de tipo URL correrían en la sesión de la tableta.
        if doc.type == 'url' and (doc.url or '').strip().lower().startswith(('https://', 'http://')):
            return {'url': doc.url.strip()}
        raise UserError("Este documento no tiene archivo para leer en la tableta. Avise al Jefe MAST.")

    @api.model
    def kiosk_ack_document(self, employee_id, pin, ack_id):
        tablet, employee = self._kiosk_person(employee_id, pin)
        ack = self._kiosk_own_ack(employee, ack_id)
        ack._sgi_sign_with_pin(tablet)
        return {'message': "Listo: su acuse de «%s» quedó firmado." % (ack.document_id.name or '')}

    # ------------------------------------------------------------------
    # Casi accidente
    # ------------------------------------------------------------------
    @api.model
    def kiosk_report_near_miss(self, employee_id, pin, values=None):
        tablet, employee = self._kiosk_person(employee_id, pin)
        values = values if isinstance(values, dict) else {}
        description = values.get('description')
        description = description.strip() if isinstance(description, str) else ''
        if len(description) < 10:
            raise UserError("Describa qué pasó (al menos una frase) para que Seguridad lo pueda investigar.")
        location = values.get('location')
        location = location.strip()[:200] if isinstance(location, str) else ''
        now = fields.Datetime.now()
        incident = self.env['sgi.incident'].sudo().create({
            'name': "Casi accidente: %s" % textwrap.shorten(description, 60, placeholder="…"),
            'incident_type': 'casi_accidente', 'severity': 'leve', 'date': now,
            'description': description, 'location': location or tablet.name,
            # El usuario de la persona (vacío si no tiene), nunca la cuenta
            # de la tableta.
            'reporter_id': employee.user_id.id or False,
            'reporter_employee_id': employee.id,
            'sgi_pin_tablet_id': tablet.id, 'sgi_pin_signed_at': now,
        })
        incident.message_post(body="Reportado en SGI en planta por %s con su PIN, en la tableta %s." % (
            employee.name, tablet.name))
        self._kiosk_notify_incident(incident)
        return {'folio': incident.folio or '',
                'message': "Gracias: su reporte %s quedó registrado. Seguridad lo va a revisar." % (
                    incident.folio or '')}

    @api.model
    def _kiosk_notify_incident(self, incident):
        """Que el reporte no se pierda: Salud ocupacional (primer miembro
        activo) o, sin miembros, el Jefe MAST."""
        Cron = self.env['sgi.cron'].sudo()
        user_id = Cron._sgi_first_user_id(self.env.ref('quimibond_sgi.group_sgi_health').sudo()) \
            or Cron._sgi_manager_user_id()
        Cron._sgi_schedule(
            incident, "Revisar casi accidente %s" % (incident.folio or incident.name),
            "Reportado en la tableta %s por %s. Clasifíquelo (severidad) e investíguelo." % (
                incident.sgi_pin_tablet_id.name, incident.reporter_employee_id.name),
            user_id, date_deadline=sgi_today(self.env) + timedelta(days=2), key='incidente_planta')

    # ------------------------------------------------------------------
    # Mi EPP
    # ------------------------------------------------------------------
    @api.model
    def _kiosk_epp_records(self, employee):
        return self.env['sgi.epp.delivery'].sudo().search(
            [('employee_id', '=', employee.id), ('state', '=', 'entregada')], order='date desc, id desc')

    @api.model
    def kiosk_epp(self, employee_id, pin):
        __, employee = self._kiosk_person(employee_id, pin)
        return [{'id': rec.id, 'name': rec.name, 'date': fields.Date.to_string(rec.date),
                 'items': rec.items or ''} for rec in self._kiosk_epp_records(employee)]

    @api.model
    def kiosk_sign_epp(self, employee_id, pin, delivery_id):
        tablet, employee = self._kiosk_person(employee_id, pin)
        delivery = self.env['sgi.epp.delivery'].sudo().browse(_kiosk_int(delivery_id)).exists()
        if not delivery or delivery.employee_id != employee:
            raise UserError("Esa responsiva no es suya.")
        delivery._sgi_sign_with_pin(tablet)
        return {'message': "Listo: su responsiva %s quedó firmada." % delivery.name}

    # ------------------------------------------------------------------
    # Checklist de mi equipo
    # ------------------------------------------------------------------
    @api.model
    def _kiosk_checklist_records(self, tablet, employee):
        today = sgi_today(self.env)
        sheets = self.env['maintenance.request'].sudo().search([
            ('sgi_checklist_template_id', 'in', tablet.checklist_template_ids.ids),
            ('sgi_checklist_employee_id', '=', False),
            ('sgi_checklist_date', '>=', today - timedelta(days=CHECKLIST_DAYS_BACK)),
            ('sgi_checklist_date', '<=', today)], order='sgi_checklist_date desc, id')
        return sheets.filtered(lambda s: not s.sgi_checklist_template_id.employee_ids
                               or employee in s.sgi_checklist_template_id.employee_ids)

    @api.model
    def _kiosk_sheet(self, sheet):
        return {'id': sheet.id, 'equipment': sheet.equipment_id.name or '',
                'template': sheet.sgi_checklist_template_id.name or '',
                'date': fields.Date.to_string(sheet.sgi_checklist_date),
                'signed': bool(sheet.sgi_checklist_employee_id),
                'lines': [{'id': line.id, 'name': line.name, 'hint': line.hint or '',
                           'answer': line.answer or False, 'note': line.note or ''}
                          for line in sheet.sgi_checklist_line_ids.sorted('sequence')]}

    @api.model
    def kiosk_checklists(self, employee_id, pin):
        tablet, employee = self._kiosk_person(employee_id, pin)
        return [self._kiosk_sheet(sheet) for sheet in self._kiosk_checklist_records(tablet, employee)]

    @api.model
    def kiosk_checklist_save(self, employee_id, pin, request_id, answers=None, rest_ok=False, finish=False):
        """Guarda respuestas y notas, «Marcar el resto como Bien» y, con
        ``finish``, firma la hoja a nombre de la persona. ``sheet`` sale de una
        búsqueda con sudo: escribe el sistema; la hoja nunca está firmada aquí
        (el dominio la excluye)."""
        tablet, employee = self._kiosk_person(employee_id, pin)
        request_id = _kiosk_int(request_id)
        sheet = self._kiosk_checklist_records(tablet, employee).filtered(lambda s: s.id == request_id)
        if not sheet:
            raise UserError("Esa hoja de checklist no está pendiente en esta tableta o no le toca a usted.")
        if answers and not isinstance(answers, dict):
            raise UserError("Respuesta no válida para el checklist.")
        valid = {key for key, __ in self.env['sgi.checklist.line']._fields['answer'].selection}
        lines = sheet.sgi_checklist_line_ids
        for line_id, value in (answers or {}).items():
            line_id = _kiosk_int(line_id)
            line = lines.filtered(lambda l: l.id == line_id)
            if not line:
                raise UserError("Ese punto no es de esta hoja.")
            if not isinstance(value, dict):
                raise UserError("Respuesta no válida para el checklist.")
            vals = {}
            if 'answer' in value:
                if value['answer'] not in valid:
                    raise UserError("Respuesta no válida para el checklist.")
                vals['answer'] = value['answer']
            if 'note' in value:
                note = value.get('note')
                vals['note'] = (note.strip()[:250] if isinstance(note, str) else '') or False
            if vals:
                line.write(vals)
        if rest_ok:
            sheet._sgi_checklist_fill_ok()
        if finish:
            sheet._sgi_checklist_sign(employee, True, tablet=tablet)
        return self._kiosk_sheet(sheet)
