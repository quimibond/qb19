# -*- coding: utf-8 -*-
"""Catálogo único de actividades (fase 1 del rediseño del SGI).

- ``sgi.activity.role``: quién ejecuta, aprueba, participa o se entera de cada
  actividad. Los roles son FILAS, no columnas: agregar un rol o una condición
  no toca el esquema.
- ``hr.job``: sus roles en el catálogo, contadores por rol, la búsqueda por
  nombre normalizado que usa la carga por API y la fusión de puestos
  duplicados.
- ``sgi.document.type``: los tipos de documento como datos (prefijo de clave,
  si exige proceso, claves heredadas aceptadas). Nada de la nomenclatura queda
  cerrado en código.
"""
import logging
import re
import unicodedata

from odoo import models, fields, api
from odoo.exceptions import ValidationError, UserError

_logger = logging.getLogger(__name__)

SGI_ROLE_SELECTION = [
    ('ejecuta', "Ejecuta"),
    ('aprueba', "Aprueba"),
    ('participa', "Participa"),
    ('informa', "Se entera"),
]


SGI_ROLE_TARGETS = [
    ('job', "Puesto"),
    ('family', "Familia de puestos"),
    ('relative', "Rol relativo"),
]

# Roles que no son de un puesto fijo. La lógica vive en código (pocos y
# estables); si hacen falta más, se agregan aquí.
SGI_RELATIVE_ROLES = [
    ('solicitante', "Solicitante"),
    ('jefe_del_solicitante', "Jefe del área que pide"),
    ('quien_detecta', "Quien lo detecta"),
    ('area_responsable', "Área responsable"),
    ('dueno_proceso', "Dueño del proceso"),
]


def sgi_normalize_name(name):
    """Nombre comparable de un puesto: sin distinguir mayúsculas ni acentos y
    con los espacios y saltos de línea colapsados («JEFE DE INVENTARIOS Y
    \\nALMACENES» == «Jefe de inventarios y almacenes», «Diseño» == «diseno»)."""
    text = unicodedata.normalize('NFKD', ' '.join((name or '').split()))
    return ''.join(c for c in text if not unicodedata.combining(c)).casefold()


class SgiJobFamily(models.Model):
    """Familia de puestos: el mismo rol repartido en puestos que solo cambian
    por nivel o letra (Operador de tejido circular A…J). El nivel se queda en
    hr.job; el SGI asigna actividades a la familia."""
    _name = 'sgi.job.family'
    _description = "Familia de puestos SGI"
    _order = 'code'

    code = fields.Char(string="Código", required=True, index=True)
    name = fields.Char(string="Familia", required=True)
    job_ids = fields.Many2many(
        'hr.job', 'sgi_job_family_rel', 'family_id', 'job_id',
        string="Puestos")
    company_id = fields.Many2one(
        'res.company', string="Empresa", required=True, index=True,
        default=lambda self: self.env.company)
    active = fields.Boolean(default=True)
    employee_count = fields.Integer(
        string="Empleados activos", compute='_compute_employee_count')

    _code_company_uniq = models.Constraint(
        'unique(code, company_id)',
        "El código de la familia debe ser único por empresa.",
    )

    @api.depends('code', 'name')
    def _compute_display_name(self):
        for family in self:
            family.display_name = "%s - %s" % (family.code, family.name) \
                if family.code else family.name

    def _compute_employee_count(self):
        counts = {}
        jobs = self.job_ids
        if jobs:
            counts = {job.id: count for job, count in self.env['hr.employee'].sudo()._read_group(
                [('job_id', 'in', jobs.ids)], ['job_id'], ['__count'])}
        for family in self:
            family.employee_count = sum(counts.get(j.id, 0) for j in family.job_ids)

    @api.constrains('job_ids', 'company_id', 'active')
    def _check_job_single_family(self):
        """Un puesto pertenece a una sola familia por empresa."""
        for family in self.filtered('active'):
            others = self.search([
                ('id', '!=', family.id),
                ('company_id', '=', family.company_id.id),
                ('job_ids', 'in', family.job_ids.ids),
            ]) if family.job_ids else self.browse()
            for other in others:
                shared = family.job_ids & other.job_ids
                raise ValidationError(
                    "El puesto %s ya pertenece a la familia %s; un puesto solo "
                    "puede estar en una familia por empresa." % (
                        ', '.join(shared.mapped('name')), other.display_name))


class SgiActivityRole(models.Model):
    _name = 'sgi.activity.role'
    _description = "Rol de un puesto en una actividad SGI"
    _order = 'activity_id, sequence, id'

    activity_id = fields.Many2one(
        'sgi.process.activity', string="Actividad", required=True,
        ondelete='cascade', index=True)
    role = fields.Selection(
        SGI_ROLE_SELECTION, string="Rol", required=True, default='ejecuta')
    # A quién toca: un puesto, una familia de puestos o un rol relativo
    # (el solicitante, quien detecta…). Exactamente uno según target_type.
    target_type = fields.Selection(
        SGI_ROLE_TARGETS, string="Asignado a", required=True, default='job')
    job_id = fields.Many2one(
        'hr.job', string="Puesto", ondelete='restrict', index=True)
    family_id = fields.Many2one(
        'sgi.job.family', string="Familia de puestos", ondelete='restrict',
        index=True)
    relative_role = fields.Selection(
        SGI_RELATIVE_ROLES, string="Rol relativo",
        help="Rol que no es de un puesto fijo. «Dueño del proceso» se resuelve "
             "al dueño del proceso; los demás no se resuelven a un puesto (no "
             "cuentan para «puesto sin persona» ni para adherencia).")
    condition = fields.Char(
        string="Condición",
        help="Solo para quien aprueba o se entera: cuándo aplica, ej. «arriba "
             "del monto que se fije». Vacío = siempre. Si según el caso la "
             "ejecuta otro puesto, son dos actividades.")
    sequence = fields.Integer(string="Secuencia", default=10)
    process_id = fields.Many2one(
        related='activity_id.process_id', string="Proceso", store=True,
        index=True)
    company_id = fields.Many2one(
        related='activity_id.company_id', string="Empresa", store=True,
        index=True)

    _activity_role_job_uniq = models.Constraint(
        'unique(activity_id, role, job_id)',
        "El mismo puesto no puede tener dos veces el mismo rol en una actividad.",
    )
    _activity_role_family_uniq = models.Constraint(
        'unique(activity_id, role, family_id)',
        "La misma familia no puede tener dos veces el mismo rol en una actividad.",
    )
    _activity_role_relative_uniq = models.Constraint(
        'unique(activity_id, role, relative_role)',
        "El mismo rol relativo no puede repetirse en una actividad.",
    )

    @api.constrains('target_type', 'job_id', 'family_id', 'relative_role')
    def _check_target(self):
        for role in self:
            filled = {
                'job': bool(role.job_id),
                'family': bool(role.family_id),
                'relative': bool(role.relative_role),
            }
            if not filled[role.target_type] or sum(filled.values()) != 1:
                raise ValidationError(
                    "Cada rol se asigna a exactamente una cosa: un puesto, una "
                    "familia o un rol relativo, según «Asignado a» (%s)." % (
                        dict(SGI_ROLE_TARGETS)[role.target_type]))

    @api.constrains('role', 'condition')
    def _check_condition(self):
        """Un ejecutor (o participante) condicionado es otra actividad: así
        cada actividad tiene un solo ejecutor y se mide limpio."""
        for role in self:
            if (role.condition or '').strip() and role.role not in ('aprueba', 'informa'):
                raise ValidationError(
                    "«%s» tiene condición («%s»). La condición solo va en quien "
                    "aprueba o se entera; si según el caso lo hace otro puesto, "
                    "parte la actividad en dos." % (role.display_name, role.condition))

    def _sgi_target_label(self):
        self.ensure_one()
        if self.target_type == 'family':
            return self.family_id.display_name or ''
        if self.target_type == 'relative':
            return dict(SGI_RELATIVE_ROLES).get(self.relative_role, '')
        return ' '.join((self.job_id.name or '').split())

    @api.depends('role', 'target_type', 'job_id', 'family_id', 'relative_role')
    def _compute_display_name(self):
        labels = dict(SGI_ROLE_SELECTION)
        for role in self:
            role.display_name = "%s · %s" % (
                labels.get(role.role, ''), role._sgi_target_label())

    def _sgi_target_key(self):
        """Llave natural del destino (para la carga idempotente)."""
        self.ensure_one()
        if self.target_type == 'family':
            return ('family', self.family_id.id)
        if self.target_type == 'relative':
            return ('relative', self.relative_role)
        return ('job', self.job_id.id)

    def _sgi_jobs(self):
        """Puestos a los que se resuelven los roles: el puesto, los de la
        familia, o el del dueño del proceso. Si alguno es un relativo que no
        se resuelve a un puesto (solicitante, quien detecta…): None."""
        jobs = self.env['hr.job']
        for role in self:
            if role.target_type == 'job':
                jobs |= role.job_id
            elif role.target_type == 'family':
                jobs |= role.family_id.job_ids
            elif role.relative_role == 'dueno_proceso':
                jobs |= role.activity_id.process_id.owner_id.job_id
            else:
                return None
        return jobs

    def _sgi_staffing_state(self):
        """Para la regla «puesto sin persona»: 'ok', 'vacante' (vacante
        aprobada y vigente), 'sin_persona', o 'na' (rol relativo que no se
        resuelve a un puesto). Una familia está vacía solo si TODOS sus
        puestos tienen cero empleados activos. El dueño del proceso cuenta
        como el puesto de ese empleado."""
        self.ensure_one()
        if self.target_type == 'relative':
            if self.relative_role != 'dueno_proceso':
                return 'na'
            owner = self.activity_id.process_id.owner_id
            if not owner.active:
                return 'sin_persona'
            return owner.job_id._sgi_staffing_state() if owner.job_id else 'ok'
        jobs = self.job_id if self.target_type == 'job' else self.family_id.job_ids
        return jobs._sgi_staffing_state()

    # Un rol es parte del cuerpo del procedimiento (quién hace qué): moverlo
    # lo marca pendiente de revisión, igual que una responsabilidad. Y como
    # un rol puede crearse sin pasar por el one2many de la actividad (por RPC,
    # desde el puesto), la regla de «exactamente un ejecutor» se revisa aquí
    # también.
    #
    # Cuando el cambio llega por el one2many de la actividad (formulario,
    # carga por API), la actividad lo valida al final con su propia
    # restricción: revisar aquí a medio camino rechazaría un «quita el
    # ejecutor y pon otro» legítimo.
    def _sgi_after_change(self, activities):
        activities = activities.exists()
        if not self.env.context.get('sgi_roles_via_activity'):
            activities._sgi_check_roles()
        activities.process_id._sgi_flag_procedure_dirty()

    @api.model_create_multi
    def create(self, vals_list):
        roles = super().create(vals_list)
        roles._sgi_after_change(roles.activity_id)
        return roles

    def write(self, vals):
        before = self.activity_id
        res = super().write(vals)
        self._sgi_after_change(before | self.activity_id)
        return res

    def unlink(self):
        activities = self.activity_id
        res = super().unlink()
        self._sgi_after_change(activities)
        return res


class HrJob(models.Model):
    _inherit = 'hr.job'

    sgi_role_ids = fields.One2many(
        'sgi.activity.role', 'job_id', string="Roles en actividades SGI")
    sgi_family_ids = fields.Many2many(
        'sgi.job.family', 'sgi_job_family_rel', 'job_id', 'family_id',
        string="Familias SGI", readonly=True)
    sgi_family_id = fields.Many2one(
        'sgi.job.family', string="Familia SGI", compute='_compute_sgi_family_id',
        store=True, readonly=True, index=True,
        help="Familia de puestos del SGI (se define en la familia). El puesto "
             "hereda las actividades de su familia.")
    # Vacante aprobada: un puesto con actividades y cero personas no es
    # alarma si ya se autorizó contratar.
    sgi_vacancy_approved = fields.Boolean(
        string="Vacante aprobada (SGI)",
        help="El puesto no tiene personas porque se va a contratar. Mientras "
             "esté vigente, el SGI lo acepta como responsable con advertencia.")
    sgi_vacancy_until = fields.Date(
        string="Vacante vigente hasta",
        help="Vacío = sin fecha límite.")

    @api.depends('sgi_family_ids', 'sgi_family_ids.active')
    def _compute_sgi_family_id(self):
        for job in self:
            job.sgi_family_id = job.sgi_family_ids.filtered('active')[:1]

    def _sgi_vacancy_valid(self):
        self.ensure_one()
        return bool(self.sgi_vacancy_approved) and (
            not self.sgi_vacancy_until
            or self.sgi_vacancy_until >= fields.Date.context_today(self))

    def _sgi_staffing_state(self):
        """Estado de un conjunto de puestos (un puesto o una familia): 'ok' si
        alguno tiene empleados activos, 'vacante' si ninguno pero alguno tiene
        vacante vigente, 'sin_persona' si no."""
        if not self:
            return 'sin_persona'
        count = self.env['hr.employee'].sudo().search_count([('job_id', 'in', self.ids)])
        if count:
            return 'ok'
        if any(job._sgi_vacancy_valid() for job in self):
            return 'vacante'
        return 'sin_persona'

    def _sgi_roles_domain(self):
        """Roles del puesto: los suyos y los de su familia (el puesto hereda
        las actividades de su familia)."""
        families = self.sgi_family_id
        domain = [('job_id', 'in', self.ids)]
        if families:
            domain = ['|'] + domain + [('family_id', 'in', families.ids)]
        return domain
    sgi_execute_count = fields.Integer(
        string="Ejecuta", compute='_compute_sgi_role_counts')
    sgi_approve_count = fields.Integer(
        string="Aprueba", compute='_compute_sgi_role_counts')
    sgi_participate_count = fields.Integer(
        string="Participa", compute='_compute_sgi_role_counts')
    sgi_inform_count = fields.Integer(
        string="Se entera", compute='_compute_sgi_role_counts')
    sgi_role_count = fields.Integer(
        string="Actividades SGI", compute='_compute_sgi_role_counts')
    sgi_all_role_ids = fields.Many2many(
        'sgi.activity.role', string="Actividades SGI (propias y de su familia)",
        compute='_compute_sgi_all_role_ids')

    def _compute_sgi_all_role_ids(self):
        Role = self.env['sgi.activity.role']
        for job in self:
            job.sgi_all_role_ids = Role.search(job._sgi_roles_domain()) if job.id else Role

    def _compute_sgi_role_counts(self):
        """Roles propios más los de su familia."""
        jobs = self.filtered('id')
        by_job, by_family = {}, {}
        if jobs:
            Role = self.env['sgi.activity.role']
            for job, role, count in Role._read_group(
                    [('job_id', 'in', jobs.ids)], ['job_id', 'role'], ['__count']):
                by_job[(job.id, role)] = count
            if jobs.sgi_family_id:
                for family, role, count in Role._read_group(
                        [('family_id', 'in', jobs.sgi_family_id.ids)],
                        ['family_id', 'role'], ['__count']):
                    by_family[(family.id, role)] = count
        for job in self:
            def total(role, job=job):
                return (by_job.get((job.id, role), 0)
                        + by_family.get((job.sgi_family_id.id, role), 0))
            job.sgi_execute_count = total('ejecuta')
            job.sgi_approve_count = total('aprueba')
            job.sgi_participate_count = total('participa')
            job.sgi_inform_count = total('informa')
            job.sgi_role_count = (job.sgi_execute_count + job.sgi_approve_count
                                  + job.sgi_participate_count + job.sgi_inform_count)

    def action_sgi_view_roles(self):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'name': "Actividades SGI — %s" % self.display_name,
            'res_model': 'sgi.activity.role',
            'view_mode': 'list,form',
            'domain': self._sgi_roles_domain(),
            'context': {'default_job_id': self.id,
                        'search_default_group_role': 1},
        }

    @api.model
    def _sgi_find_job(self, ref, company):
        """Resuelve un puesto por id o por nombre normalizado dentro de la
        empresa (o compartido). Nunca crea: devuelve (puesto, error)."""
        Job = self.with_context(active_test=True)
        if isinstance(ref, int) and not isinstance(ref, bool):
            job = Job.browse(ref).exists()
            if not job:
                return Job.browse(), "El puesto con id %s no existe." % ref
            if job.company_id and job.company_id != company:
                return Job.browse(), "El puesto %s (id %s) es de otra empresa." % (
                    job.display_name, ref)
            return job, None
        wanted = sgi_normalize_name(ref if isinstance(ref, str) else '')
        if not wanted:
            return Job.browse(), "Falta el puesto."
        candidates = Job.search([('company_id', 'in', [company.id, False])])
        matches = candidates.filtered(lambda j: sgi_normalize_name(j.name) == wanted)
        if not matches:
            return Job.browse(), "No existe el puesto «%s» en %s." % (ref, company.name)
        if len(matches) > 1:
            return Job.browse(), (
                "El puesto «%s» es ambiguo: hay %d con ese nombre (ids %s). "
                "Fusiona los duplicados (hr.job.sgi_merge_duplicate_jobs) o "
                "usa el id." % (ref, len(matches), matches.ids))
        return matches, None

    # ------------------------------------------------------------------
    # Limpieza de puestos: nombres con saltos de línea y duplicados
    # ------------------------------------------------------------------
    def _sgi_job_references(self):
        """Campos almacenados que apuntan a hr.job: [(modelo, campo, field)]."""
        refs = []
        for model_name in self.env.registry:
            Model = self.env[model_name]
            if Model._abstract or Model._transient or not Model._auto:
                continue
            for name, field in Model._fields.items():
                if (field.type in ('many2one', 'many2many')
                        and field.comodel_name == 'hr.job' and field.store
                        and not field.compute and not field.related):
                    refs.append((model_name, name, field))
        return refs

    @api.model
    def sgi_merge_duplicate_jobs(self, dry_run=True, company_id=None):
        """Fusiona puestos duplicados (mismo nombre normalizado y empresa) y
        limpia los saltos de línea de los nombres.

        Por cada grupo se conserva el puesto con más empleados activos (a
        igualdad, el de id menor); las referencias de los demás (empleados,
        roles, documentos, responsabilidades… cualquier campo que apunte a
        hr.job) se reasignan al que se queda y los duplicados se archivan.
        Con ``dry_run`` (por omisión) solo reporta. Se corre a mano (MCP o
        shell) y solo un Administrador SGI."""
        if not (self.env.su or self.env.user.has_group('quimibond_sgi.group_sgi_admin')):
            raise UserError("Solo un Administrador SGI puede fusionar puestos.")
        Job = self.sudo().with_context(active_test=True)
        domain = [('company_id', '=', company_id)] if company_id else []
        jobs = Job.search(domain)
        employees = {job.id: count for job, count in self.env['hr.employee'].sudo()._read_group(
            [('job_id', 'in', jobs.ids)], ['job_id'], ['__count'])}
        groups = {}
        for job in jobs:
            key = (job.company_id.id, sgi_normalize_name(job.name))
            groups.setdefault(key, Job.browse())
            groups[key] |= job
        refs = self._sgi_job_references()
        cr = self.env.cr
        report = {'dry_run': bool(dry_run), 'merged': [], 'renamed': []}
        for dup_group in groups.values():
            if len(dup_group) < 2:
                continue
            keep = dup_group.sorted(lambda j: (-employees.get(j.id, 0), j.id))[:1]
            others = dup_group - keep
            moved = {}
            for model_name, fname, field in refs:
                Model = self.env[model_name]
                if field.type == 'many2one':
                    cr.execute('SELECT count(*) FROM "%s" WHERE "%s" IN %%s' % (
                        Model._table, fname), [tuple(others.ids)])
                    count = cr.fetchone()[0]
                    if count and not dry_run:
                        cr.execute('UPDATE "%s" SET "%s" = %%s WHERE "%s" IN %%s' % (
                            Model._table, fname, fname), [keep.id, tuple(others.ids)])
                else:
                    rel, col_self, col_job = field.relation, field.column1, field.column2
                    cr.execute('SELECT count(*) FROM "%s" WHERE "%s" IN %%s' % (
                        rel, col_job), [tuple(others.ids)])
                    count = cr.fetchone()[0]
                    if count and not dry_run:
                        cr.execute(
                            'INSERT INTO "{rel}" ("{a}", "{b}") '
                            'SELECT DISTINCT "{a}", %s FROM "{rel}" WHERE "{b}" IN %s '
                            'ON CONFLICT DO NOTHING'.format(rel=rel, a=col_self, b=col_job),
                            [keep.id, tuple(others.ids)])
                        cr.execute('DELETE FROM "%s" WHERE "%s" IN %%s' % (
                            rel, col_job), [tuple(others.ids)])
                if count:
                    moved['%s.%s' % (model_name, fname)] = count
            report['merged'].append({
                'name': ' '.join(keep.name.split()),
                'keep_id': keep.id,
                'merged_ids': others.ids,
                'references': moved,
            })
            if not dry_run:
                self.env.invalidate_all()
                others.write({'active': False})
                for other in others:
                    other.message_post(
                        body="Puesto duplicado fusionado en «%s» (id %d) por el "
                             "SGI; sus referencias se movieron allá." % (
                                 keep.name, keep.id))
        # Nombres con saltos de línea o espacios dobles (solo los que quedan
        # activos; los archivados conservan su nombre para no chocar con la
        # restricción de unicidad de hr.job).
        for job in Job.search(domain):
            clean = ' '.join((job.name or '').split())
            if clean and clean != job.name:
                report['renamed'].append({'id': job.id, 'from': job.name, 'to': clean})
                if not dry_run:
                    try:
                        with cr.savepoint():
                            job.name = clean
                    except Exception as exc:  # noqa: BLE001 - se reporta y sigue
                        report['renamed'][-1]['error'] = str(exc)
        if not dry_run:
            _logger.info("SGI: fusión de puestos aplicada: %s", report)
        return report


class SgiDocumentType(models.Model):
    """Tipo de documento controlado. Los prefijos de clave viven aquí como
    datos: agregar un tipo o cambiar su nomenclatura no requiere programar."""
    _name = 'sgi.document.type'
    _description = "Tipo de documento SGI"
    _order = 'sequence, code'

    code = fields.Char(
        string="Código", required=True, index=True,
        help="Identificador estable (procedimiento, instructivo, formato…). "
             "El campo heredado «Tipo de documento» se calcula desde él.")
    name = fields.Char(string="Nombre", required=True, translate=True)
    sequence = fields.Integer(default=10)
    active = fields.Boolean(default=True)
    prefix_pattern = fields.Char(
        string="Patrón de clave",
        help="Cómo se arma la clave. Marcadores: {process} = clave del proceso "
             "(C6, S2…), {seq} o {seq:02d} = consecutivo. Ej. PR-{process}, "
             "IT-{process}-{seq:02d}, CO-{seq:02d}. Vacío = clave libre.")
    requires_process = fields.Boolean(
        string="Exige proceso",
        help="Un documento controlado de este tipo debe estar ligado a un "
             "proceso (salvo que conserve una clave heredada).")
    code_required = fields.Boolean(
        string="Exige clave", default=True,
        help="Apágalo para tipos sin clave propia (documentos externos, "
             "formularios de Odoo).")
    legacy_code_regex = fields.Char(
        string="Claves heredadas aceptadas (regex)",
        help="Expresión regular de la nomenclatura anterior que se sigue "
             "aceptando mientras se migra (ej. ^P-[AGCDEIMPSV]\\d{2}$).")
    company_id = fields.Many2one(
        'res.company', string="Empresa", index=True,
        help="Vacío = tipo compartido por todas las empresas.")

    _code_company_uniq = models.Constraint(
        'unique(code, company_id)',
        "El código del tipo de documento debe ser único por empresa.",
    )

    @api.constrains('legacy_code_regex')
    def _check_legacy_regex(self):
        for dtype in self.filtered('legacy_code_regex'):
            try:
                re.compile(dtype.legacy_code_regex)
            except re.error as exc:
                raise ValidationError(
                    "La expresión de claves heredadas de «%s» no es válida: %s"
                    % (dtype.name, exc))

    @api.constrains('prefix_pattern')
    def _check_prefix_pattern(self):
        for dtype in self.filtered('prefix_pattern'):
            try:
                dtype._sgi_pattern_regex()
                dtype._sgi_format_code('X', 1)
            except (KeyError, ValueError, IndexError, re.error) as exc:
                raise ValidationError(
                    "El patrón de clave «%s» no es válido (%s). Usa solo "
                    "{process} y {seq} / {seq:02d}." % (dtype.prefix_pattern, exc))

    _TOKEN_RE = re.compile(r'\{(process|seq)(?::0?(\d+)d)?\}')

    def _sgi_pattern_regex(self, process_code=None):
        """Regex anclada del patrón. Sin proceso, {process} acepta cualquier
        clave de proceso."""
        self.ensure_one()
        pattern = self.prefix_pattern or ''
        out, pos = [], 0
        for match in self._TOKEN_RE.finditer(pattern):
            out.append(re.escape(pattern[pos:match.start()]))
            if match.group(1) == 'process':
                out.append(re.escape(process_code) if process_code
                           else r'[A-Za-z0-9]+')
            else:
                width = int(match.group(2) or 0)
                out.append(r'\d{%d,}' % width if width else r'\d+')
            pos = match.end()
        rest = pattern[pos:]
        if '{' in rest or '}' in rest:
            raise ValueError("marcador desconocido en «%s»" % rest)
        out.append(re.escape(rest))
        return re.compile('^%s$' % ''.join(out))

    def _sgi_format_code(self, process_code, seq):
        self.ensure_one()
        return (self.prefix_pattern or '').format(process=process_code or '', seq=seq)

    def _sgi_legacy_match(self, code):
        self.ensure_one()
        if not self.legacy_code_regex or not code:
            return False
        try:
            return bool(re.match(self.legacy_code_regex, code.strip()))
        except re.error:
            return False

    def _sgi_code_ok(self, code, process=None):
        """¿La clave cumple el patrón del tipo (con el proceso, si lo hay) o
        la nomenclatura heredada?"""
        self.ensure_one()
        if not self.code_required:
            return True
        code = (code or '').strip()
        if not code:
            return False
        if not self.prefix_pattern and not self.legacy_code_regex:
            return True
        if self.prefix_pattern:
            if self._sgi_pattern_regex(process.code if process else None).match(code):
                return True
        return self._sgi_legacy_match(code)

    @api.model
    def _sgi_any_match(self, code):
        """¿Alguna nomenclatura activa (nueva o heredada) acepta la clave?"""
        return any(dtype._sgi_code_ok(code) for dtype in self.search(
            [('code_required', '=', True)]))

    def sgi_next_code(self, process=None):
        """Siguiente clave libre del tipo para el proceso: arma el patrón con
        el consecutivo más alto usado + 1."""
        self.ensure_one()
        if not self.prefix_pattern:
            raise UserError("El tipo «%s» no tiene patrón de clave." % self.name)
        if '{process}' in self.prefix_pattern and not process:
            raise UserError("El tipo «%s» necesita el proceso para armar la clave." % self.name)
        process_code = process.code if process else None
        if '{seq' not in self.prefix_pattern:
            return self._sgi_format_code(process_code, 0)
        regex = self._sgi_pattern_regex(process_code)
        # Parte fija de la clave antes del consecutivo («IT-C6-»): acota la
        # búsqueda de claves usadas.
        prefix = self.prefix_pattern.split('{seq')[0].format(
            process=process_code or '')
        used = self.env['documents.document'].with_context(active_test=False).search(
            [('sgi_code', '=like', prefix + '%')])
        seq = 0
        for code in used.mapped('sgi_code'):
            if code and regex.match(code):
                numbers = re.findall(r'\d+', code)
                if numbers:
                    seq = max(seq, int(numbers[-1]))
        return self._sgi_format_code(process_code, seq + 1)


class SgiConfigStudioCleanup(models.AbstractModel):
    _inherit = 'sgi.config'

    # Modelos de Studio que el SGI sustituye (actividades por empleado, NC,
    # obligaciones). Se borran SOLO si siguen vacíos.
    _SGI_STUDIO_MODELS = (
        'x_emp_activity', 'x_no_conformidades', 'x_actividades_obligato',
        'x_calendario_de_obliga')

    @api.model
    def sgi_drop_empty_studio_models(self, dry_run=True):
        """Borra los modelos de Studio vacíos que el SGI sustituye, con sus
        vistas, acciones y menús. Un modelo con al menos un registro no se
        toca. Con ``dry_run`` (por omisión) solo reporta. Se corre a mano
        fuera de un update: borrar un modelo recarga el registro."""
        if not (self.env.su or self.env.user.has_group('quimibond_sgi.group_sgi_admin')):
            raise UserError("Solo un Administrador SGI puede borrar modelos de Studio.")
        IrModel = self.env['ir.model'].sudo()
        report = {'dry_run': bool(dry_run), 'dropped': [], 'kept': [], 'missing': []}
        for name in self._SGI_STUDIO_MODELS:
            model = IrModel.search([('model', '=', name)], limit=1)
            if not model:
                report['missing'].append(name)
                continue
            if model.state != 'manual':
                report['kept'].append({'model': name, 'reason': "no es de Studio"})
                continue
            count = self.env[name].sudo().with_context(active_test=False).search_count([]) \
                if name in self.env else 0
            if count:
                report['kept'].append({'model': name, 'reason': "%d registro(s)" % count})
                continue
            report['dropped'].append(name)
            if dry_run:
                continue
            actions = self.env['ir.actions.act_window'].sudo().search([('res_model', '=', name)])
            menus = self.env['ir.ui.menu'].sudo().with_context(active_test=False).search(
                [('action', 'in', ['ir.actions.act_window,%d' % a for a in actions.ids])])
            views = self.env['ir.ui.view'].sudo().with_context(active_test=False).search(
                [('model', '=', name)])
            menus.unlink()
            actions.unlink()
            views.unlink()
            model.unlink()
            _logger.info("SGI: modelo de Studio vacío %s eliminado.", name)
        return report
