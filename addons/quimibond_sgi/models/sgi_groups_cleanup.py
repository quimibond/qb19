# -*- coding: utf-8 -*-
"""Bloque 2 (56.8.0): grupos correctos del SGI.

- PERM-2: «Jefe MAST y SGI» solo con quien administra el SGI (Areli). Los
  demás miembros DIRECTOS pasan a Usuario SGI; Dirección y Administrador lo
  siguen teniendo por herencia.
- Grupos heredados «SGI» y «SGI admin» (creados a mano o con Studio, sin
  privilegio): sus miembros ya tienen los grupos nuevos y lo único que daban
  era lectura de un modelo de Studio vacío. Se retiran con sus permisos.

Los corre la migración 19.0.56.8.0 y se pueden volver a correr (idempotentes).
"""
import logging

from odoo import api, models

_logger = logging.getLogger(__name__)

SGI_MAST_LOGINS = ('mas@quimibond.com',)
SGI_LEGACY_GROUP_NAMES = ('SGI', 'SGI admin')
# Módulos «de mano»: un grupo con xmlid de otro módulo es de ese módulo y no
# se toca.
_HAND_MODULES = ('__export__', '__custom__', 'studio_customization')


class ResGroupsSgiCleanup(models.Model):
    _inherit = 'res.groups'

    @api.model
    def _sgi_enforce_mast_members(self, logins=SGI_MAST_LOGINS):
        """Deja como miembros directos de Jefe MAST y SGI solo a `logins`; el
        resto pasa a Usuario SGI. Si ninguno de `logins` está en el grupo no
        hace nada (no deja al SGI sin jefe)."""
        mast = self.env.ref('quimibond_sgi.group_sgi_manager')
        user_group = self.env.ref('quimibond_sgi.group_sgi_user')
        direct = mast.sudo().user_ids
        keep = direct.filtered(lambda u: u.login in logins)
        extra = direct - keep
        if not keep or not extra:
            return self.env['res.users']
        extra.sudo().write({'group_ids': [(3, mast.id), (4, user_group.id)]})
        _logger.info("SGI: Jefe MAST y SGI solo con %s; pasan a Usuario SGI: %s",
                     keep.mapped('login'), extra.mapped('login'))
        return extra

    @api.model
    def _sgi_legacy_groups(self):
        Data = self.env['ir.model.data'].sudo()
        groups = self.sudo().search([('name', 'in', list(SGI_LEGACY_GROUP_NAMES)),
                                     ('privilege_id', '=', False)])
        return groups.filtered(lambda g: not Data.search_count([
            ('model', '=', 'res.groups'), ('res_id', '=', g.id),
            ('module', 'not in', _HAND_MODULES)]))

    @api.model
    def _sgi_retire_legacy_groups(self):
        """Retira los grupos heredados «SGI» y «SGI admin»: quien solo los
        tenía recibe Usuario SGI; se quitan sus permisos, reglas y menús."""
        legacy = self._sgi_legacy_groups()
        if not legacy:
            return []
        user_group = self.env.ref('quimibond_sgi.group_sgi_user')
        sgi_groups = user_group | self.env.ref('quimibond_sgi.group_sgi_auditor')
        names = []
        for group in legacy:
            orphans = group.user_ids.filtered(lambda u: not (u.all_group_ids & sgi_groups))
            if orphans:
                orphans.write({'group_ids': [(4, user_group.id)]})
            self.env['ir.model.access'].sudo().search([('group_id', '=', group.id)]).unlink()
            # Una regla que solo era de este grupo se ARCHIVA (sin grupos
            # quedaría global); si tiene otros grupos, solo se le quita este.
            for rule in self.env['ir.rule'].sudo().search([('groups', 'in', group.id)]):
                if rule.groups == group:
                    rule.active = False
                else:
                    rule.write({'groups': [(3, group.id)]})
            self.env['ir.model.data'].sudo().search(
                [('model', '=', 'res.groups'), ('res_id', '=', group.id)]).unlink()
            names.append(group.name)
            _logger.info("SGI: grupo heredado «%s» retirado (%d usuarios, %d con Usuario SGI nuevo).",
                         group.name, len(group.user_ids), len(orphans))
        legacy.unlink()
        return names
