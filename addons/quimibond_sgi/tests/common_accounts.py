# -*- coding: utf-8 -*-
"""Cuentas propias para las pruebas que contabilizan facturas de proveedor.

En la copia de producción la cuenta por pagar por defecto de los proveedores
(201.01.01) está archivada, y una factura de proveedor no se puede publicar
contra ella. La prueba no debe depender de esa configuración: crea su propia
cuenta por pagar y se la asigna al proveedor de prueba.
"""


def sgi_test_payable(env, partner):
    account = env['account.account'].create({
        'name': 'Proveedores (prueba SGI)',
        'code': 'SGIT201%s' % partner.id,
        'account_type': 'liability_payable',
        'reconcile': True,
    })
    partner.property_account_payable_id = account
    return account


def sgi_test_sales_accounts(env, *accounts):
    """57.90.0: los KPI de ventas solo cuentan líneas en las cuentas de
    ventas (``quimibond_sgi.sales_account_prefixes``, 401/402 en producción).
    La prueba declara como cuenta de ventas la de ingresos que usa, para no
    depender del plan de cuentas de la base."""
    env['ir.config_parameter'].sudo().set_param(
        'quimibond_sgi.sales_account_prefixes',
        ','.join(account.code for account in accounts))
