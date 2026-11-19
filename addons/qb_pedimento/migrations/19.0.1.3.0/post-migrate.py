# -*- coding: utf-8 -*-
"""1.3.0 agrega «Tiene vigencia» a los tipos de documento. Los tipos sembrados
son noupdate: aquí se marca la carta encomienda y los certificados de origen
en las bases que ya tenían el módulo."""
from odoo import SUPERUSER_ID, api


def migrate(cr, version):
    env = api.Environment(cr, SUPERUSER_ID, {})
    for xmlid in ('doc_carta_encomienda', 'doc_certificado_origen_imp', 'doc_certificado_origen_exp'):
        tipo = env.ref('qb_pedimento.%s' % xmlid, raise_if_not_found=False)
        if tipo:
            tipo.pide_vigencia = True
