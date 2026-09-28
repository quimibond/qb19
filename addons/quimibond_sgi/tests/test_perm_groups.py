# -*- coding: utf-8 -*-
"""Bloque 2 (56.8.0): Jefe MAST y SGI con un solo miembro directo y retiro de
los grupos heredados «SGI» / «SGI admin». Idempotente."""
from odoo.tests import TransactionCase, tagged, new_test_user


@tagged('post_install', '-at_install')
class TestPermGroups(TransactionCase):

    def test_01_jefe_mast_solo_con_areli(self):
        Groups = self.env['res.groups']
        mast = self.env.ref('quimibond_sgi.group_sgi_manager')
        user_group = self.env.ref('quimibond_sgi.group_sgi_user')
        areli = new_test_user(self.env, login='perm_areli', groups='base.group_user,quimibond_sgi.group_sgi_manager')
        logistica = new_test_user(self.env, login='perm_logistica',
                                  groups='base.group_user,quimibond_sgi.group_sgi_manager')
        moved = Groups._sgi_enforce_mast_members(logins=('perm_areli',))
        self.assertIn(logistica, moved)
        self.assertEqual(mast.user_ids, areli, "Un solo miembro directo.")
        self.assertIn(user_group, logistica.group_ids, "Queda como Usuario SGI.")
        self.assertFalse(logistica.has_group('quimibond_sgi.group_sgi_manager'))
        # Idempotente, y sin Areli en el grupo no deja al SGI sin jefe.
        self.assertFalse(Groups._sgi_enforce_mast_members(logins=('perm_areli',)))
        self.assertFalse(Groups._sgi_enforce_mast_members(logins=('nadie@quimibond.com',)))
        self.assertEqual(mast.user_ids, areli)
        # Como Usuario SGI sigue abriendo su procedimiento sin error de acceso.
        screen = self.env['sgi.my.procedure'].with_user(logistica).create({})
        self.assertTrue(screen.no_employee)

    def test_02_grupos_heredados_retirados(self):
        Groups = self.env['res.groups']
        legacy = Groups.create({'name': 'SGI admin'})
        solo = new_test_user(self.env, login='perm_solo_legacy', groups='base.group_user')
        solo.write({'group_ids': [(4, legacy.id)]})
        model = self.env['ir.model']._get('sgi.process')
        self.env['ir.model.access'].create({'name': 'legacy', 'model_id': model.id, 'group_id': legacy.id,
                                            'perm_read': True})
        rule = self.env['ir.rule'].create({'name': 'legacy', 'model_id': model.id,
                                           'groups': [(6, 0, legacy.ids)], 'domain_force': "[(1, '=', 1)]"})
        self.assertIn(legacy, Groups._sgi_legacy_groups())
        self.assertIn('SGI admin', Groups._sgi_retire_legacy_groups())
        self.assertFalse(legacy.exists())
        self.assertFalse(rule.active, "La regla que solo era suya se archiva, no queda global.")
        self.assertTrue(solo.has_group('quimibond_sgi.group_sgi_user'), "Recibe Usuario SGI.")
        self.assertEqual(Groups._sgi_retire_legacy_groups(), [], "Idempotente.")
        # Un grupo con el mismo nombre pero de un módulo (con privilegio) no se toca.
        self.assertNotIn(self.env.ref('quimibond_sgi.group_sgi_user'), Groups._sgi_legacy_groups())
