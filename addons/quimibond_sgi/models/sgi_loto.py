# -*- coding: utf-8 -*-
"""Bloqueo y etiquetado de energías, LOTO (ISO 45001 8.1, NOM-004-STPS;
S5.14, procedimiento P-A20).

Antes de reparar, limpiar, desatascar o cambiar herramental de una máquina:
se avisa a los afectados, se identifican las fuentes de energía y su punto
de bloqueo, **cada trabajador pone su candado y su tarjeta**, y se comprueba
la energía cero. Al terminar, cada quien retira el suyo y se avisa al
responsable del área.

Flujo: borrador → bloqueado → retirado (o cancelado). Retirado o cancelado
es evidencia: solo el Jefe MAST lo reabre.
"""
from odoo import api, fields, models
from odoo.exceptions import UserError

ENERGY_TYPES = [
    ('electrica', "Eléctrica"),
    ('neumatica', "Neumática"),
    ('hidraulica', "Hidráulica"),
    ('mecanica', "Mecánica (partes en movimiento, resortes)"),
    ('termica', "Térmica (vapor, aceite caliente)"),
    ('quimica', "Química"),
    ('gravitacional', "Gravitacional"),
    ('otra', "Otra"),
]


class SgiLoto(models.Model):
    _name = 'sgi.loto'
    _description = "Bloqueo y etiquetado de energías (LOTO)"
    _inherit = ['sgi.base.mixin', 'sgi.format.mixin']
    _order = 'date_applied desc, folio desc'
    _sgi_sequence_code = 'sgi.loto'
    _sgi_locked_states = ('retirado', 'cancelado')

    _folio_uniq = models.Constraint(
        'unique(folio)', "Ya existe un bloqueo con ese folio.")

    company_id = fields.Many2one(
        'res.company', string="Empresa", required=True, index=True,
        default=lambda self: self.env.company)
    equipment_id = fields.Many2one('maintenance.equipment', string="Equipo", required=True,
                                   tracking=True)
    maintenance_request_id = fields.Many2one('maintenance.request', string="Orden de mantenimiento")
    work_permit_id = fields.Many2one('sgi.work.permit', string="Permiso de trabajo",
                                     help="Si el trabajo además requiere permiso de alto riesgo.")
    name = fields.Char(string="Trabajo a realizar", required=True, tracking=True,
                       help="Reparar, limpiar, desatascar, cambiar herramental…")
    responsible_id = fields.Many2one('res.users', string="Responsable del bloqueo", required=True,
                                     default=lambda self: self.env.user, tracking=True)
    affected_notified = fields.Boolean(
        string="Se avisó a los afectados y a su jefe", tracking=True,
        help="Operadores de la máquina y su jefe saben que se va a bloquear.")
    energy_ids = fields.One2many('sgi.loto.energy', 'loto_id', string="Fuentes de energía")
    lock_ids = fields.One2many('sgi.loto.lock', 'loto_id', string="Candados y tarjetas")
    zero_energy_verified = fields.Boolean(string="Energía cero comprobada", tracking=True)
    zero_energy_method = fields.Text(
        string="Cómo se comprobó",
        help="Intento de arranque, medición con probador, manómetro en cero, purga…")
    verified_by_id = fields.Many2one('res.users', string="Comprobó", readonly=True, copy=False)
    date_applied = fields.Datetime(string="Bloqueado desde", readonly=True, copy=False)
    date_removed = fields.Datetime(string="Retirado", readonly=True, copy=False)
    area_notified_end = fields.Boolean(
        string="Se avisó al responsable del área al terminar", tracking=True)
    removal_note = fields.Text(string="Condiciones al retirar",
                               help="Guardas colocadas, herramienta recogida, personal fuera "
                                    "de la zona.")
    active_locks = fields.Integer(string="Candados puestos", compute='_compute_active_locks')
    state = fields.Selection([
        ('borrador', "Borrador"),
        ('bloqueado', "Bloqueado"),
        ('retirado', "Retirado"),
        ('cancelado', "Cancelado"),
    ], string="Estado", default='borrador', required=True, tracking=True)

    @api.depends('lock_ids.date_removed')
    def _compute_active_locks(self):
        for loto in self:
            loto.active_locks = len(loto.lock_ids.filtered(lambda lock: not lock.date_removed))

    @api.depends('folio', 'equipment_id.name')
    def _compute_display_name(self):
        for loto in self:
            label = loto.equipment_id.name or loto.name or ''
            loto.display_name = "%s - %s" % (loto.folio, label) if loto.folio else label

    def action_apply(self):
        """Bloqueo puesto: fuentes con su punto de bloqueo, un candado y
        tarjeta por trabajador, aviso a los afectados y energía cero."""
        for loto in self:
            if loto.state != 'borrador':
                raise UserError("Solo se aplica un bloqueo en borrador.")
            problems = []
            if not loto.affected_notified:
                problems.append("• Avise a los afectados y a su jefe antes de bloquear.")
            if not loto.energy_ids:
                problems.append("• Registre al menos una fuente de energía.")
            elif loto.energy_ids.filtered(lambda e: not (e.isolation_point or '').strip()):
                problems.append("• Cada fuente de energía necesita su punto de bloqueo.")
            if not loto.lock_ids:
                problems.append("• Cada trabajador que interviene pone su candado y su tarjeta.")
            elif loto.lock_ids.filtered(lambda lock: not (lock.lock_number and lock.tag_number)):
                problems.append("• Cada candado lleva su número y su tarjeta.")
            if not loto.zero_energy_verified or not (loto.zero_energy_method or '').strip():
                problems.append("• Compruebe la energía cero y anote cómo lo hizo.")
            if problems:
                raise UserError("No se puede aplicar el bloqueo %s:\n%s" % (
                    loto.folio or loto.name, "\n".join(problems)))
            now = fields.Datetime.now()
            loto.lock_ids.filtered(lambda lock: not lock.date_placed).write({'date_placed': now})
            loto.write({'state': 'bloqueado', 'date_applied': now,
                        'verified_by_id': self.env.user.id})
        return True

    def action_remove(self):
        """Retiro: ya no queda ningún candado y se avisó al área."""
        for loto in self:
            if loto.state != 'bloqueado':
                raise UserError("Solo se retira un bloqueo aplicado.")
            problems = []
            pending = loto.lock_ids.filtered(lambda lock: not lock.date_removed)
            if pending:
                problems.append("• Cada trabajador retira su propio candado. Faltan: %s." % (
                    ", ".join(pending.sudo().mapped('employee_id.name'))))
            if not loto.area_notified_end:
                problems.append("• Avise al responsable del área que el equipo queda liberado.")
            if not (loto.removal_note or '').strip():
                problems.append("• Anote las condiciones al retirar.")
            if problems:
                raise UserError("No se puede retirar el bloqueo %s:\n%s" % (
                    loto.folio, "\n".join(problems)))
            loto.write({'state': 'retirado', 'date_removed': fields.Datetime.now()})
        return True

    def action_cancel(self):
        for loto in self:
            if loto.state == 'bloqueado':
                raise UserError("Un bloqueo aplicado no se cancela: se retira candado por candado.")
        self.write({'state': 'cancelado'})
        return True

    def action_reset(self):
        """Reabrir (solo desde retirado o cancelado, candado de evidencia:
        Jefe MAST)."""
        self.write({'state': 'borrador'})
        return True


class SgiLotoEnergy(models.Model):
    _name = 'sgi.loto.energy'
    _description = "Fuente de energía del bloqueo"
    _order = 'loto_id, sequence, id'

    loto_id = fields.Many2one('sgi.loto', string="Bloqueo", required=True, ondelete='cascade',
                              index=True)
    sequence = fields.Integer(default=10)
    energy_type = fields.Selection(ENERGY_TYPES, string="Energía", required=True,
                                   default='electrica')
    isolation_point = fields.Char(string="Punto de bloqueo",
                                  help="Interruptor, válvula, tablero, candado de palanca…")
    method = fields.Char(string="Cómo se aísla", help="Abrir y bloquear, purgar, calzar…")

    def _sgi_check_parent_open(self):
        if self.env.su or self.env.user.has_group('quimibond_sgi.group_sgi_manager'):
            return
        if self.loto_id.filtered(lambda loto: loto.state != 'borrador'):
            raise UserError("Las fuentes de energía se registran antes de aplicar el bloqueo.")

    @api.model_create_multi
    def create(self, vals_list):
        records = super().create(vals_list)
        records._sgi_check_parent_open()
        return records

    def write(self, vals):
        self._sgi_check_parent_open()
        return super().write(vals)

    def unlink(self):
        self._sgi_check_parent_open()
        return super().unlink()


class SgiLotoLock(models.Model):
    _name = 'sgi.loto.lock'
    _description = "Candado y tarjeta de un trabajador"
    _order = 'loto_id, id'

    loto_id = fields.Many2one('sgi.loto', string="Bloqueo", required=True, ondelete='cascade',
                              index=True)
    employee_id = fields.Many2one('hr.employee', string="Trabajador", required=True)
    lock_number = fields.Char(string="Candado")
    tag_number = fields.Char(string="Tarjeta")
    date_placed = fields.Datetime(string="Puesto", readonly=True)
    date_removed = fields.Datetime(string="Retirado", readonly=True)
    removed_by_id = fields.Many2one('res.users', string="Retiró", readonly=True)
    state = fields.Selection(related='loto_id.state')

    _employee_uniq = models.Constraint(
        'unique(loto_id, employee_id)', "Cada trabajador pone un solo candado por bloqueo.")

    def _sgi_check_parent_open(self):
        if self.env.su or self.env.user.has_group('quimibond_sgi.group_sgi_manager'):
            return
        if self.loto_id.filtered(lambda loto: loto.state != 'borrador'):
            raise UserError("Los candados se registran antes de aplicar el bloqueo; después solo "
                            "se retiran con su botón.")

    @api.model_create_multi
    def create(self, vals_list):
        records = super().create(vals_list)
        records._sgi_check_parent_open()
        return records

    def write(self, vals):
        self._sgi_check_parent_open()
        return super().write(vals)

    def unlink(self):
        self._sgi_check_parent_open()
        return super().unlink()

    def action_remove_lock(self):
        """Cada trabajador retira su propio candado (P-A20). El Jefe MAST
        puede retirarlo por él, y queda anotado quién lo hizo."""
        is_mast = self.env.su or self.env.user.has_group('quimibond_sgi.group_sgi_manager')
        for lock in self:
            if lock.loto_id.state != 'bloqueado':
                raise UserError("Solo se retira un candado de un bloqueo aplicado.")
            if lock.date_removed:
                raise UserError("Este candado ya se retiró.")
            owner = lock.sudo().employee_id.user_id
            if not is_mast and owner != self.env.user:
                raise UserError("Cada trabajador retira su propio candado. El de %s lo retira "
                                "él o el Jefe MAST." % lock.sudo().employee_id.name)
            # sudo: ya validado quién retira; el renglón de un bloqueo aplicado
            # no se edita de otro modo.
            lock.sudo().write({
                'date_removed': fields.Datetime.now(), 'removed_by_id': self.env.user.id})
            lock.loto_id.message_post(body="Candado %s retirado por %s." % (
                lock.lock_number or '', self.env.user.name))
        return True
