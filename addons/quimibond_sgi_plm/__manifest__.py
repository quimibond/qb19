# -*- coding: utf-8 -*-
{
    'name': "Quimibond SGI - Puente PLM",
    'summary': "Enlaza los cambios de ingeniería (ECO) con PPAP/AMEF/planes de control del SGI",
    'description': """
Puente entre la app de Gestión del Ciclo de Vida del Producto (mrp_plm) y el SGI.

Al aplicar un cambio de ingeniería (ECO) marcado como "Requiere PPAP", genera
automáticamente un expediente PPAP (motivo: cambio de ingeniería) y, si el cambio
implica aviso al cliente, agenda una actividad al equipo de ventas.

Se instala automáticamente cuando conviven quimibond_sgi y mrp_plm.
    """,
    'author': "Quimibond",
    'website': "https://www.quimibond.com",
    'category': 'Services/SGI',
    'version': '19.0.3.0.0',
    'license': 'LGPL-3',
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
