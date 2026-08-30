# -*- coding: utf-8 -*-
{
    'name': 'Quimibond Capacidad & Costeo',
    'version': '19.0.1.36.0',
    'license': 'LGPL-3',
    'category': 'Manufacturing',
    'summary': 'Capacidad, costo real, ociosidad y cotizador — read-only sobre datos nativos de Odoo.',
    'description': """
Módulo read-only y no invasivo que calcula, en vivo desde los registros
nativos de Odoo (mrp / hr / account / stock / sale / resource):

- Capacidad por máquina y por centro (calendarios reales × eficiencia).
- Balance de línea / cuello de botella en unidad común (metros-equivalentes).
- Costo real por producto: MP a último costo de compra (explosión recursiva
  de BOM), energía variable $/kg, fabricación absorbida híbrida peso/largo,
  operación % sobre ventas.
- Costo de capacidad ociosa (costeo normal, IAS 2) y escenarios de turno.
- Cotizador con desglose por capa, pisos de precio, contribución por
  hora-máquina y chequeo de capacidad.
- Snapshot mensual para histórico y tendencia.

No escribe ni extiende ningún modelo nativo: todo dato propio vive en
tablas de configuración del módulo, editables desde la UI.
    """,
    'author': 'Quimibond',
    'website': 'https://quimibond.com',
    'depends': [
        'base',
        'mail',
        'mrp',
        'hr',
        'account',
        'stock',
        'sale',
        'purchase',
        'uom',
    ],
    'data': [
        'security/ir.model.access.csv',
        'data/seed_config.xml',
        'data/ir_cron.xml',
        'views/config_views.xml',
        'views/panel_views.xml',
        'report/cotizacion_report.xml',
        'views/capacidad_views.xml',
        'views/costeo_views.xml',
        'views/conciliacion_views.xml',
        'views/workorder_excepcion_views.xml',
        'views/cotizacion_views.xml',
        # DESPUÉS de cotizacion_views: su botón referencia
        # cotizador_orden_action, que se define ahí
        'views/sale_order_views.xml',
        'views/ficha_views.xml',
        'data/mail_template.xml',
        'views/cliente_views.xml',
        'views/producto_reportes_views.xml',
        'views/snapshot_views.xml',
        'views/recalculo_views.xml',
        'views/menus.xml',
    ],
    'post_init_hook': 'post_init_hook',
    'installable': True,
    'application': True,
    'auto_install': False,
}
