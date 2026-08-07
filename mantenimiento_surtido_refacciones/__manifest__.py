# -*- coding: utf-8 -*-
{
    'name': 'Surtido de Refacciones a Solicitudes de Mantenimiento',
    'version': '19.0.1.0.0',
    'category': 'Manufacturing/Maintenance',
    'summary': 'Alta de refacciones en solicitudes de mantenimiento con generación '
               'automática de traslado interno hacia ubicación de consumo costeada.',
    'description': """
Integra el módulo de Mantenimiento con Inventario y Contabilidad para:
  * Registrar refacciones y cantidades requeridas dentro de la solicitud de mantenimiento.
  * Mostrar disponibilidad y costo (unitario y total) desde la ubicación de refacciones (MANTTO).
  * Generar el traslado interno (requisición) de la ubicación MANTTO hacia la ubicación
    virtual "Consumo Mantenimiento".
  * El consumo se contabiliza automáticamente contra la cuenta de gasto de mantenimiento
    definida en la(s) categoría(s) de producto de las refacciones (campo "Cuenta de
    producción"), usando el mismo mecanismo nativo que ya usa Quimibond para el consumo
    de materia prima en manufactura. No requiere código de contabilidad adicional.
""",
    'author': 'Jose - Consultoría Odoo',
    'depends': ['maintenance', 'stock_account', 'account'],
    'data': [
        'security/ir.model.access.csv',
        'data/stock_data.xml',
        'views/stock_picking_views.xml',
        'views/maintenance_request_views.xml',
    ],
    'installable': True,
    'application': False,
    'license': 'LGPL-3',
}
