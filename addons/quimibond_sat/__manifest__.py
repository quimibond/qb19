# -*- coding: utf-8 -*-
{
    'name': 'Quimibond - SAT (Syntage) en Odoo',
    'version': '19.0.1.15.0',
    'license': 'LGPL-3',
    'category': 'Accounting/Localizations',
    'summary': 'CFDI del SAT (vía Syntage) dentro de Odoo y comparación contra las facturas registradas.',
    'description': """
SAT (Syntage) en Odoo
=====================

Trae a Odoo los CFDI que el SAT tiene para cada compañía (emitidos y
recibidos) usando Syntage como proveedor de descarga masiva, y los cruza por
UUID contra las facturas de Odoo.

- Ingesta: webhook de Syntage (firma HMAC) + descarga paginada por API +
  solicitud diaria de extracción incremental.
- Cruce: cada CFDI se liga a su factura por el folio fiscal del documento CFDI
  de Odoo (l10n_mx_edi.document) o del asiento; queda "solo en el SAT" si no
  hay factura.
- Comparación SAT vs Odoo: vista con tres cubetas (en ambos, solo SAT, solo
  Odoo) y hallazgos (monto distinto, cancelado en el SAT pero publicado en
  Odoo, cancelado en Odoo pero vigente en el SAT).

Parámetros del sistema (Ajustes > Técnico > Parámetros del sistema):
  quimibond_sat.api_key         API key de Syntage
  quimibond_sat.webhook_secret  Secreto de firma del webhook de Syntage
  quimibond_sat.api_base        Opcional, default https://api.syntage.com
  quimibond_sat.alert_email     Destinatarios (coma) de la alerta diaria de hallazgos
  quimibond_sat.queue_budget_seconds  Presupuesto por corrida de la cola (default 600)

Endpoint del webhook: POST /quimibond_sat/webhook
    """,
    'author': 'Quimibond',
    'website': 'https://quimibond.com',
    # l10n_mx_edi NO es dependencia dura: en producción está (Odoo.sh
    # Enterprise) y el cruce por UUID lo usa si existe; en la imagen community
    # del CI no está y el módulo debe instalar igual (cruce manual solamente).
    'depends': [
        'account',
    ],
    'data': [
        'security/ir.model.access.csv',
        'data/ir_cron_data.xml',
        'views/sat_cfdi_views.xml',
        'views/sat_compare_views.xml',
        'views/sat_event_views.xml',
        'views/sat_pull_wizard_views.xml',
        'views/sat_reconcile_wizard_views.xml',
        'views/res_company_views.xml',
        'views/res_partner_views.xml',
        'views/sat_pago_views.xml',
        'views/account_move_views.xml',
        'views/menus.xml',
    ],
    'post_init_hook': 'post_init_hook',
    'installable': True,
    'application': False,
    'auto_install': False,
}
