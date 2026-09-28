# -*- coding: utf-8 -*-
"""19.0.56.8.0 (Bloque 2 · permisos):

- 2.2 PERM-2: Jefe MAST y SGI con un solo miembro directo (Areli,
  mas@quimibond.com); los demás miembros directos pasan a Usuario SGI.
- 2.3: retira los grupos heredados «SGI» y «SGI admin» (sin privilegio, sin
  xmlid de un módulo). Sus miembros ya tienen los grupos nuevos; quien no,
  recibe Usuario SGI. Sus permisos se quitan y sus reglas se archivan.

Idempotente: la lógica vive en res.groups (models/sgi_groups_cleanup.py)."""
from odoo import SUPERUSER_ID, api


def migrate(cr, version):
    env = api.Environment(cr, SUPERUSER_ID, {})
    Groups = env['res.groups']
    Groups._sgi_enforce_mast_members()
    Groups._sgi_retire_legacy_groups()
