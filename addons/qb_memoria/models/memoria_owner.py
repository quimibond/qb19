# -*- coding: utf-8 -*-
"""Dueños aprendidos de la memoria: quién atiende a cada empresa.

La memoria (Supabase, vista ``memoria_encargados``) cuenta qué buzón interno
le escribe más a cada empresa (180 días) y qué buzón atiende cada tipo de
pendiente por empresa (365 días). En Quimibond los buzones son funcionales
(logistica@, cxcobrar@, innovacion@, comprasplanta@), así que aquí se resuelve
la persona detrás de cada buzón: primero el catálogo ``qb.memoria.mailbox``
(editable) y, si no, el usuario cuyo login o correo es ese buzón.

Cada noche el cron escribe en el contacto comercial el encargado general y el
encargado por área, con la evidencia (correos y %). Un contacto marcado como
fijado a mano no se sobrescribe. ``memoria_owner_for(area)`` es la API que
consumen las obligaciones.
"""
import logging

from odoo import _, api, fields, models

_logger = logging.getLogger(__name__)

AREAS = [
    ('comercial', 'Comercial'), ('operaciones', 'Operaciones'), ('compras', 'Compras'),
    ('finanzas', 'Finanzas'), ('sgi', 'SGI'), ('rh', 'RH'), ('otro', 'Otro'),
]
# Umbral para creer una señal: mínimo de correos/pendientes y % del total.
MIN_N = 3
MIN_SHARE = 40
PAGE = 1000


class QbMemoriaMailbox(models.Model):
    _name = 'qb.memoria.mailbox'
    _description = 'Buzón de la memoria (quién está detrás)'
    _order = 'email'
    _rec_name = 'email'

    email = fields.Char(required=True, index=True)
    user_id = fields.Many2one('res.users', string='Persona', domain=[('share', '=', False)],
                              help='Usuario de Odoo que atiende este buzón. Vacío: no se propone dueño.')
    area = fields.Selection(AREAS, string='Área', help='Solo informativo: qué área atiende el buzón.')
    note = fields.Char(string='Nota')
    active = fields.Boolean(default=True)

    _email_uniq = models.Constraint('unique(email)', 'Ese buzón ya está en el catálogo.')

    @api.model_create_multi
    def create(self, vals_list):
        for vals in vals_list:
            if vals.get('email'):
                vals['email'] = vals['email'].strip().lower()
        return super().create(vals_list)

    def write(self, vals):
        if vals.get('email'):
            vals['email'] = vals['email'].strip().lower()
        return super().write(vals)

    @api.model
    def user_for(self, mailbox):
        """Persona detrás de un buzón: catálogo, si no login/correo de un usuario interno activo."""
        mailbox = (mailbox or '').strip().lower()
        Users = self.env['res.users'].sudo()
        if not mailbox:
            return Users
        row = self.sudo().search([('email', '=', mailbox)], limit=1)
        if row:
            return row.user_id if (row.user_id and row.user_id.active) else Users
        return Users.search(['|', ('login', '=ilike', mailbox), ('email', '=ilike', mailbox),
                             ('share', '=', False), ('active', '=', True)], limit=1)


class ResPartnerMemoriaOwner(models.Model):
    _inherit = 'res.partner'

    memoria_owner_user_id = fields.Many2one(
        'res.users', string='Encargado (memoria)', copy=False, index=True,
        help='Quién atiende a esta empresa según el correo. Lo propone la memoria cada noche; '
             'marca "Fijado a mano" para que no lo cambie.')
    memoria_owner_evidence = fields.Char(string='Evidencia', copy=False, readonly=True)
    memoria_owner_at = fields.Datetime(string='Aprendido el', copy=False, readonly=True)
    memoria_owner_areas = fields.Json(string='Encargados por área', copy=False)
    memoria_owner_locked = fields.Boolean(string='Fijado a mano', copy=False)

    def memoria_owner_for(self, area=None):
        """Dueño propuesto para un área (o general). Recordset vacío si no hay."""
        self.ensure_one()
        partner = (self.commercial_partner_id or self).sudo()
        Users = self.env['res.users'].sudo()
        data = partner.memoria_owner_areas or {}
        if area and (data.get(area) or {}).get('user_id'):
            user = Users.browse(data[area]['user_id']).exists()
            if user and user.active:
                return user
        user = partner.memoria_owner_user_id
        return user if (user and user.active) else Users

    @api.model
    def _memoria_owner_rows(self, client):
        """Filas rank 1 de la vista, paginadas."""
        rows, offset = [], 0
        while True:
            page = client.get('memoria_encargados', {
                'select': 'odoo_partner_id,area,mailbox,n,share,last_at', 'rank': 'eq.1',
                'order': 'odoo_partner_id.asc,area.asc.nullsfirst', 'limit': PAGE, 'offset': offset})
            rows += page
            if len(page) < PAGE:
                return rows
            offset += PAGE

    @api.model
    def _cron_memoria_owners(self):
        """Cada noche: encargado general y por área en cada contacto comercial."""
        client = self.env['qb.memoria.client']
        Mailbox = self.env['qb.memoria.mailbox']
        rows = self._memoria_owner_rows(client)
        by_partner = {}
        for row in rows:
            if (row.get('n') or 0) < MIN_N or (row.get('share') or 0) < MIN_SHARE:
                continue
            by_partner.setdefault(int(row['odoo_partner_id']), []).append(row)
        Partner = self.env['res.partner'].sudo()
        now = fields.Datetime.now()
        users_cache = {}
        updated = unresolved = 0
        for pid, prows in by_partner.items():
            partner = Partner.browse(pid).exists()
            if not partner:
                continue
            partner = partner.commercial_partner_id or partner
            areas, general, evidence = {}, None, None
            for row in prows:
                mailbox = (row.get('mailbox') or '').lower()
                if mailbox not in users_cache:
                    users_cache[mailbox] = Mailbox.user_for(mailbox)
                user = users_cache[mailbox]
                entry = {'mailbox': mailbox, 'user_id': user.id or False, 'n': row.get('n'),
                         'share': row.get('share'), 'last_at': row.get('last_at')}
                if row.get('area'):
                    areas[row['area']] = entry
                else:
                    general = entry
                    evidence = _('%(mb)s: %(n)s correos en 180 días (%(share)s %%)') % {
                        'mb': mailbox, 'n': row.get('n'), 'share': row.get('share')}
                    if not user:
                        evidence += _(' — buzón sin persona: asignarla en Contactos → Configuración → Buzones')
                        unresolved += 1
            vals = {'memoria_owner_areas': areas or False, 'memoria_owner_at': now}
            if general:
                vals['memoria_owner_evidence'] = evidence
                if not partner.memoria_owner_locked and general['user_id']:
                    vals['memoria_owner_user_id'] = general['user_id']
            partner.write(vals)
            updated += 1
        _logger.info('qb_memoria: encargados aprendidos en %s contactos (%s buzones sin persona)', updated, unresolved)
        return updated
