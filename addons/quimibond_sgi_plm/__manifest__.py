# -*- coding: utf-8 -*-
{
    'name': "Quimibond SGI - Puente PLM",
    'summary': "Enlaza los cambios de ingeniería (ECO) con PPAP/AMEF/planes de control del SGI",
    'description': """
Puente entre la app de Gestión del Ciclo de Vida del Producto (mrp_plm) y el SGI.

Marca "Requiere PPAP" en el cambio de ingeniería (ECO) cuando el producto se vende
a clientes que exigen PPAP ante cambios, y al aplicarlo genera un PPAP por cliente
(motivo: cambio de ingeniería). Si el cambio implica aviso al cliente, agenda una
actividad al equipo de ventas.

Se instala automáticamente cuando conviven quimibond_sgi y mrp_plm.
    """,
    'author': "Quimibond",
    'website': "https://www.quimibond.com",
    'category': 'Services/SGI',
    'version': '19.0.3.2.0',
    'license': 'OPL-1',
    'depends': [
        'quimibond_sgi',
        'mrp_plm',
    ],
    'data': [
        'views/mrp_eco_views.xml',
    ],
    'auto_install': True,
    'installable': True,
}
