# -*- coding: utf-8 -*-
"""1.2.0: la cuenta de IVA de importación (118.02.01) queda configurada en
las compañías que la tienen, para que el expediente se compare con
contabilidad desde el primer día."""
from odoo import SUPERUSER_ID, api


def migrate(cr, version):
    env = api.Environment(cr, SUPERUSER_ID, {})
    for company in env['res.company'].search([('qb_pedimento_iva_account_id', '=', False)]):
        account = env['account.account'].with_company(company).search(
            [('code', '=', '118.02.01'), ('company_ids', 'in', company.id)], limit=1)
        if account:
            company.qb_pedimento_iva_account_id = account
