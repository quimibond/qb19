# -*- coding: utf-8 -*-
"""57.13.0 (entrega 8, primer bloque; decisión de Jose 2026-09-29): los roles
relativos se resuelven a personas.

Antes, «Dueño del proceso» se resolvía siempre con el proceso de la
ACTIVIDAD y los demás relativos (solicitante, jefe del que pide, quien
detecta, área responsable) no se resolvían a nadie. Los 41 escalamientos a
«Dueño del proceso» no le llegaban a nadie en Mis pendientes, y S6.07
resolvía el aprobador a la misma persona que ejecuta.

Reglas (``_sgi_resolve_employees(record)``; el registro es el documento que
se pide, aprueba o detecta; sin registro, o si el registro es la propia
actividad, solo «Dueño del proceso» se resuelve):

- **Dueño del proceso**: el dueño del proceso DEL REGISTRO. El proceso se
  busca, en este orden: (1) ``SGI_RECORD_PROCESS_FIELDS`` (``sgi_process_id``,
  ``process_id``, many2one a ``sgi.process``); (2) cualquier otro campo
  guardado many2one o many2many a ``sgi.process`` (campos de Studio y
  ``sgi_affected_process_ids`` de un cambio incluidos: con varios procesos,
  aprueban todos sus dueños); (3) un salto por ``SGI_RECORD_PROCESS_VIA``
  (el documento o la actividad ligados, p. ej. la solicitud de cambio de un
  documento toma el proceso del documento). Si el registro es un proceso, él
  mismo. Sin proceso en el registro, el de la actividad.
- **Solicitante**: quien pide. Primer campo con valor de
  ``SGI_REQUESTER_FIELDS`` (many2one a ``res.users`` o ``hr.employee``); si
  no hay, quien creó el registro (``create_uid``), salvo que sea del
  sistema. Se resuelve a su empleado de la empresa del SGI.
- **Jefe del área que pide**: el jefe directo del solicitante
  (``hr.employee.parent_id``).
- **Quien lo detecta**: primer campo con valor de ``SGI_DETECTOR_FIELDS``; si
  no hay, ``create_uid`` (salvo sistema).
- **Área responsable**: el responsable (``manager_id``) del departamento del
  registro: ``sgi_area_id.department_id``, ``department_id`` o, si no, el
  primer many2one guardado a ``hr.department`` con valor (Studio incluido).

Lo que no se puede resolver regresa vacío con el motivo, que queda en el log
(``_logger.info``) y en el reporte ``sgi_relative_roles_report``.

**Aprobador ≠ quien ejecuta o pide** (``_sgi_not_self``): en «Aprueba» y
«Escala», las personas resueltas que también ejecutan la actividad (o piden el
registro) se quitan. Si con eso no queda nadie, el aprobador o el
escalamiento sube al jefe directo de cada persona quitada
(``hr.employee.parent_id``, el mismo camino que el escalamiento de las
actividades), saltando a quien también ejecute o pida; si no hay jefe, queda
vacío con el motivo («avisa»): nunca se asigna en silencio a la misma persona.
"""
import logging

from odoo import SUPERUSER_ID, api, models

from .sgi_catalog import SGI_RECORD_RELATIVES

_logger = logging.getLogger(__name__)

# Campos del registro, en orden de preferencia.
SGI_RECORD_PROCESS_FIELDS = ('sgi_process_id', 'process_id')
SGI_REQUESTER_FIELDS = ('request_owner_id', 'sgi_requester_id', 'requester_id',
                        'requested_by', 'request_user_id')
SGI_DETECTOR_FIELDS = ('sgi_detected_by_id', 'detected_by_id', 'reporter_id',
                       'sgi_requester_id')
# Registros ligados cuyo proceso vale para el registro (un salto): la
# solicitud de cambio de un documento (E2.01) toma el proceso del documento.
SGI_RECORD_PROCESS_VIA = ('sgi_document_id', 'sgi_new_document_id', 'document_id',
                          'sgi_activity_id', 'activity_id')
SGI_AREA_FIELDS = ('sgi_area_id', 'department_id')
# Tope al subir por la cadena de jefes (evita ciclos en hr.employee).
_MAX_CLIMB = 10


class SgiActivityRoleRelative(models.Model):
    _inherit = 'sgi.activity.role'

    # ------------------------------------------------------------------
    # Lectura del registro
    # ------------------------------------------------------------------
    @api.model
    def _sgi_company_employee(self, users):
        """Empleado de la empresa del SGI de cada usuario (o sin empresa)."""
        users = users.filtered(lambda u: u.id != SUPERUSER_ID and not u.share)
        if not users:
            return self.env['hr.employee']
        company = self.env['sgi.config']._sgi_company()
        employees = self.env['hr.employee'].sudo().search(
            [('user_id', 'in', users.ids), ('company_id', 'in', (company.id, False))])
        found = self.env['hr.employee']
        for user in users:
            found |= employees.filtered(lambda e, u=user: e.user_id == u)[:1]
        return found

    @api.model
    def _sgi_record_people(self, record, names):
        """Empleados del primer campo con valor de ``names`` (many2one a
        res.users o hr.employee). (empleados, campo) o (vacío, False)."""
        Employee = self.env['hr.employee']
        for name in names:
            field = record._fields.get(name)
            if not field or field.type != 'many2one':
                continue
            value = record[name]
            if not value:
                continue
            if field.comodel_name == 'hr.employee':
                return value.sudo(), name
            if field.comodel_name == 'res.users':
                return self._sgi_company_employee(value), name
        return Employee, False

    @api.model
    def _sgi_record_creator(self, record):
        root = self.env.ref('base.user_root', raise_if_not_found=False)
        user = record.create_uid if 'create_uid' in record._fields else self.env['res.users']
        if not user or user.id == SUPERUSER_ID or (root and user == root):
            return self.env['hr.employee']
        return self._sgi_company_employee(user)

    @api.model
    def _sgi_record_process(self, record, hop=True):
        """Procesos del registro (ver docstring del módulo) o vacío."""
        Process = self.env['sgi.process']
        if not record:
            return Process
        if record._name == 'sgi.process':
            return record
        record_fields = record._fields
        for name in SGI_RECORD_PROCESS_FIELDS:
            field = record_fields.get(name)
            if field and field.type == 'many2one' and field.comodel_name == 'sgi.process' \
                    and record[name]:
                return record[name]
        found = Process
        for field in record_fields.values():
            if field.store and field.comodel_name == 'sgi.process' \
                    and field.type in ('many2one', 'many2many') \
                    and field.name not in SGI_RECORD_PROCESS_FIELDS:
                found |= record[field.name]
        if found or not hop:
            return found
        for name in SGI_RECORD_PROCESS_VIA:
            field = record_fields.get(name)
            if field and field.type == 'many2one' and record[name]:
                found = self._sgi_record_process(record[name].sudo(), hop=False)
                if found:
                    return found
        return Process

    @api.model
    def _sgi_record_area_manager(self, record):
        """(responsable del área del registro, campo) o (vacío, False)."""
        record_fields = record._fields
        names = list(SGI_AREA_FIELDS) + [
            f.name for f in record_fields.values()
            if f.store and f.type == 'many2one' and f.comodel_name == 'hr.department'
            and f.name not in SGI_AREA_FIELDS]
        for name in names:
            field = record_fields.get(name)
            if not field or field.type != 'many2one' or not record[name]:
                continue
            value = record[name].sudo()
            if field.comodel_name == 'sgi.area':
                department = value.department_id
            elif field.comodel_name == 'hr.department':
                department = value
            else:
                continue
            if department:
                return department.manager_id, name
        return self.env['hr.employee'], False

    # ------------------------------------------------------------------
    # Resolución
    # ------------------------------------------------------------------
    def _sgi_requester(self, record):
        """(empleado que pide, motivo si no hay)."""
        people, _field = self._sgi_record_people(record, SGI_REQUESTER_FIELDS)
        if not people:
            people = self._sgi_record_creator(record)
        if not people:
            return people, "el registro no dice quién pide (o lo creó el sistema)"
        return people, ''

    def _sgi_resolve_employees(self, record=None):
        """(empleados, motivo): a quién toca este rol para ``record``. Motivo
        vacío = resuelto. Sin la regla aprobador ≠ ejecutor (ver
        ``_sgi_target_employees``)."""
        self.ensure_one()
        Employee = self.env['hr.employee'].sudo()
        if self.target_type == 'job':
            return Employee.search([('job_id', '=', self.job_id.id)]) if self.job_id else Employee, ''
        if self.target_type == 'family':
            jobs = self.family_id.sudo().job_ids
            return Employee.search([('job_id', 'in', jobs.ids)]) if jobs else Employee, ''
        relative = self.relative_role
        record = record.sudo() if record else None
        if record is not None and record._name == 'sgi.process.activity':
            record = None   # la actividad no es un registro que se pide
        if relative == 'dueno_proceso':
            process = self._sgi_record_process(record) if record is not None else False
            process = (process or self.activity_id.process_id).sudo()
            owner = process.owner_id
            if not owner:
                return Employee, "el proceso %s no tiene dueño" % ', '.join(
                    p.code or '' for p in process)
            return owner, ''
        if record is None:
            return Employee, "depende del registro (sin registro no hay a quién)"
        if relative == 'solicitante':
            return self._sgi_requester(record)
        if relative == 'jefe_del_solicitante':
            requester, reason = self._sgi_requester(record)
            if reason:
                return Employee, reason
            boss = requester.parent_id
            if not boss:
                return Employee, "%s no tiene jefe en Empleados" % requester[:1].name
            return boss, ''
        if relative == 'quien_detecta':
            people, _field = self._sgi_record_people(record, SGI_DETECTOR_FIELDS)
            people = people or self._sgi_record_creator(record)
            if not people:
                return Employee, "el registro no dice quién lo detectó (o lo creó el sistema)"
            return people, ''
        if relative == 'area_responsable':
            manager, field = self._sgi_record_area_manager(record)
            if not field:
                return Employee, "el registro no tiene área ni departamento"
            if not manager:
                return Employee, "el departamento del registro no tiene responsable"
            return manager, ''
        return Employee, "rol relativo desconocido"

    def _sgi_executor_employees(self, record=None):
        """Quienes ejecutan o piden: los empleados de los puestos que
        ejecutan la actividad (familias y dueño del proceso resueltos) y, con
        registro, quien lo pide."""
        self.ensure_one()
        Employee = self.env['hr.employee'].sudo()
        people = Employee
        for executor in self.activity_id.role_ids.filtered(lambda r: r.role == 'ejecuta'):
            if executor.target_type == 'relative' and executor.relative_role != 'dueno_proceso' \
                    and not record:
                continue
            people |= executor._sgi_resolve_employees(record)[0]
        if record and record._name != 'sgi.process.activity':
            people |= self._sgi_requester(record.sudo())[0]
        return people

    @api.model
    def _sgi_not_self(self, employees, excluded):
        """Regla aprobador ≠ quien ejecuta o pide: (empleados, aviso). Quita
        a los excluidos; si no queda nadie, sube al jefe directo de cada
        quitado, saltando excluidos. Sin jefe: vacío y aviso."""
        clash = employees & excluded
        if not clash:
            return employees, ''
        rest = employees - excluded
        if rest:
            return rest, "se quitó a %s porque también ejecuta o pide" % ', '.join(
                clash.mapped('name'))
        bosses = self.env['hr.employee'].sudo()
        for emp in clash:
            boss, hops = emp.parent_id, 0
            while boss and boss in excluded and hops < _MAX_CLIMB:
                boss, hops = boss.parent_id, hops + 1
            if boss and boss not in excluded:
                bosses |= boss
        names = ', '.join(clash.mapped('name'))
        if not bosses:
            return bosses, "%s también ejecuta o pide y no tiene jefe: nadie aprueba" % names
        return bosses, "%s también ejecuta o pide: sube a su jefe (%s)" % (
            names, ', '.join(bosses.mapped('name')))

    def _sgi_target_employees(self, record=None, log=True):
        """(empleados, aviso) de este rol para ``record`` con la regla
        aprobador ≠ ejecutor en «Aprueba» y «Escala». Lo que no se resuelve o
        se mueve queda en el log (``log=False`` en los cálculos repetidos de
        Mis pendientes; el reporte y la sincronización sí registran)."""
        self.ensure_one()
        employees, reason = self._sgi_resolve_employees(record)
        note = reason
        if employees and self.role in ('aprueba', 'escala'):
            employees, note = self._sgi_not_self(employees, self._sgi_executor_employees(record))
        if note and log:
            _logger.info("SGI rol %s (%s %s): %s", self.id,
                         self.activity_id.number or self.activity_id.legacy_number or '',
                         self.display_name, note)
        return employees, note

    # ------------------------------------------------------------------
    # Escalamientos a roles relativos (Mis pendientes)
    # ------------------------------------------------------------------
    @api.model
    def _sgi_relative_escalations(self, employees):
        """{empleado.id: roles «Escala» relativos que le tocan}. El
        escalamiento es del atraso de la actividad (no hay registro), así que
        solo «Dueño del proceso» se resuelve: el dueño del proceso de la
        actividad, o su jefe si el dueño también la ejecuta."""
        result = {}
        if not employees:
            return result
        company = self.env['sgi.config']._sgi_company()
        roles = self.sudo().search([
            ('role', '=', 'escala'), ('target_type', '=', 'relative'),
            ('activity_id.active', '=', True), ('activity_id.process_id.active', '=', True),
            ('company_id', 'in', (company.id, False))])
        wanted = set(employees.ids)
        for role in roles:
            targets, _note = role._sgi_target_employees(log=False)
            for emp in targets:
                if emp.id in wanted:
                    result.setdefault(emp.id, self.browse())
                    result[emp.id] |= role
        return result

    # ------------------------------------------------------------------
    # Reporte (lectura; se puede llamar por MCP)
    # ------------------------------------------------------------------
    @api.model
    def sgi_relative_roles_report(self):
        """Por cada rol relativo de una actividad activa: a quién se resuelve
        sin registro (catálogo, escalamientos, aprobaciones nativas) y el
        aviso de la regla aprobador ≠ ejecutor. Solo lectura."""
        roles = self.sudo().search([('target_type', '=', 'relative'),
                                    ('activity_id.active', '=', True)])
        rows = []
        for role in roles:
            employees, note = role._sgi_target_employees()
            activity = role.activity_id
            rows.append({
                'role_id': role.id,
                'activity': activity.number or activity.legacy_number or '',
                'role': role.role,
                'relative': role.relative_role,
                'employees': employees.mapped('name'),
                'note': note,
                'record_dependent': role.relative_role in SGI_RECORD_RELATIVES,
            })
        return rows
