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
