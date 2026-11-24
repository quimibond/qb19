# -*- coding: utf-8 -*-
{
    'name': 'Quimibond - Flujo de efectivo a 13 semanas',
    'version': '19.0.1.1.0',
    'license': 'LGPL-3',
    'category': 'Accounting/Accounting',
    'summary': 'Proyección semanal de caja (método directo) en Contabilidad → Reportes: caja, cobranza y '
               'pagos por vencimiento, pedidos por facturar, nómina, IMSS y SAT.',
    'description': """
Flujo de efectivo a 13 semanas
==============================

Reporte del motor de reportes de Contabilidad: una columna por semana (la
semana en curso y 12 más) y el total, con PDF y Excel nativos.

* Saldo inicial: las cuentas de caja que se elijan (último estado de cuenta o
  saldo contable), en pesos al tipo de cambio del día.
* Comprometido: facturas de clientes y proveedores abiertas, en su fecha
  esperada (la que se capture en la factura, o el vencimiento más el atraso
  histórico del contacto; lo vencido de más se reparte en las primeras
  semanas).
* Estimado: pedidos de venta y órdenes de compra confirmados por facturar,
  ventas sin pedido (promedio facturado o presupuesto) y sus compras en la
  proporción histórica, compromisos recurrentes (nómina, aguinaldo, IMSS,
  SAT, rentas…) estimados con lo pagado, y ajustes manuales.
* Foto semanal de la proyección y comparación contra la caja real.
* Lo que no suma (cartera incobrable, proveedores por revisar, lo que vence
  después de la semana 13) se muestra aparte.

Los supuestos viven en Contabilidad → Configuración → Flujo a 13 semanas.
    """,
    'author': 'Quimibond',
    'website': 'https://www.quimibond.com',
    'depends': ['account_reports', 'sale', 'purchase'],
    'data': [
        'security/ir.model.access.csv',
        'data/account_report_data.xml',
        'views/flujo_config_views.xml',
        'data/ir_cron_data.xml',
    ],
    'installable': True,
    'application': False,
    'auto_install': False,
}
