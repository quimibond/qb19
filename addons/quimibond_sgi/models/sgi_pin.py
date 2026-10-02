# -*- coding: utf-8 -*-
"""57.94.0 (U-01): el PIN del empleado, una sola validación para el checklist
y para «SGI en planta».

- ``sgi.pin._sgi_check_pin``: la persona está activa, es de la empresa del
  SGI (D-03) y, si se pide, de la lista permitida; su PIN (``hr.employee.pin``,
  el mismo de Asistencias) coincide. Compara en tiempo constante. Sin límite
  de intentos (D-08 / F-018). Método privado: no se llama por RPC; el único
  punto de entrada desde la tableta es ``sgi.floor.kiosk.kiosk_check_pin``.
- ``sgi.pin.signature.mixin``: lo que queda en un registro firmado con PIN:
  la tableta (cuenta compartida) y la hora. Solo el sistema lo escribe (el
  kiosco valida y escribe con sudo); ni el Jefe MAST lo cambia.

Este archivo va justo después de ``sgi_base`` en ``models/__init__.py``: el
acuse (``sgi_document``) y los demás heredan el mixin.
"""
import hmac
import logging

from odoo import api, fields, models
from odoo.exceptions import UserError

_logger = logging.getLogger(__name__)

SGI_PIN_FIELDS = frozenset({'sgi_pin_tablet_id', 'sgi_pin_signed_at'})


class SgiPin(models.AbstractModel):
    """Validación del PIN del empleado (checklist y SGI en planta). No guarda registros."""
    _name = 'sgi.pin'
    _description = "Validación del PIN del empleado (checklist y SGI en planta)"

    @api.model
    def _sgi_check_pin(self, employee, pin, required=True, allowed=None, purpose="firmar"):
        """True: firmó con su PIN. False: no tiene PIN y no se exige (solo el
        checklist con ``quimibond_sgi.checklist_pin_required`` apagado). Todo
        lo demás detiene con UserError."""
        employee = employee.sudo().exists()
        if not employee or not employee.active:
            raise UserError("La persona elegida ya no está activa. Pida a RH que revise su ficha.")
        company = self.env['sgi.config']._sgi_company()
        if employee.company_id and employee.company_id != company:
            raise UserError("%s no es de %s: no puede %s aquí." % (employee.name, company.name, purpose))
        if allowed is not None and employee not in allowed:
            raise UserError("%s no está en la lista de quién puede %s." % (employee.name, purpose))
        real_pin = employee.pin or ''
        if not real_pin:
            if required:
                raise UserError(
                    "%s no tiene PIN registrado y sin PIN no puede %s. Pida a RH que lo capture en "
                    "su ficha de empleado (el mismo del quiosco de asistencia)." % (employee.name, purpose))
            return False
        if not hmac.compare_digest(str(pin or '').strip().encode(), real_pin.encode()):
            _logger.info("SGI PIN: PIN incorrecto para el empleado %s (usuario %s).",
                         employee.id, self.env.uid)
            raise UserError("PIN incorrecto para %s." % employee.name)
        return True


class SgiPinSignatureMixin(models.AbstractModel):
    """Firma con PIN en una tableta de planta: la tableta y la hora. Cada modelo dice quién
    firmó con ``_sgi_pin_employee``."""
    _name = 'sgi.pin.signature.mixin'
    _description = "Firma con PIN en una tableta de planta"

    sgi_pin_tablet_id = fields.Many2one(
        'sgi.floor.tablet', string="Firmado con PIN en la tableta", readonly=True, copy=False,
        ondelete='restrict', index='btree_not_null',
        help="Tableta de planta (cuenta compartida) donde la persona firmó con su PIN.")
    sgi_pin_signed_at = fields.Datetime(
        string="Firmado con PIN el", readonly=True, copy=False,
        help="Fecha y hora en que la persona firmó con su PIN.")
    sgi_pin_signature = fields.Char(
        string="Firma con PIN", compute='_compute_sgi_pin_signature',
        help="Quién firmó con su PIN, en qué tableta y cuándo.")

    def _sgi_pin_employee(self):
        """Empleado que firmó (cada modelo lo sobrescribe)."""
        return self.env['hr.employee']

    # Sin store: se calcula al leer, así que no hace falta depender del campo
    # del empleado que firma (distinto en cada modelo, ``_sgi_pin_employee``).
    @api.depends('sgi_pin_tablet_id', 'sgi_pin_signed_at')
    def _compute_sgi_pin_signature(self):
        for rec in self:
            if not rec.sgi_pin_signed_at:
                rec.sgi_pin_signature = False
                continue
            employee = rec.sudo()._sgi_pin_employee()
            when = fields.Datetime.context_timestamp(rec, rec.sgi_pin_signed_at)
            text = "Firmado con PIN por %s" % (employee.name or "?")
            if rec.sgi_pin_tablet_id:
                text += " en la tableta %s" % rec.sudo().sgi_pin_tablet_id.name
            rec.sgi_pin_signature = "%s el %s." % (text, when.strftime('%d/%m/%Y %H:%M'))

    def _sgi_check_pin_fields(self, keys):
        if SGI_PIN_FIELDS & set(keys) and not self.env.su:
            raise UserError("La firma con PIN solo la registra el sistema, desde la tableta.")

    @api.model_create_multi
    def create(self, vals_list):
        for vals in vals_list:
            self._sgi_check_pin_fields(vals)
        records = super().create(vals_list)
        # Después del alta: un ``default_sgi_pin_*`` en el contexto no pasa
        # por ``vals_list`` y también firmaría.
        if not self.env.su and any(rec.sgi_pin_tablet_id or rec.sgi_pin_signed_at
                                   for rec in records.sudo()):
            self._sgi_check_pin_fields(SGI_PIN_FIELDS)
        return records

    def write(self, vals):
        self._sgi_check_pin_fields(vals)
        return super().write(vals)
