{
    'name': 'Quimibond Intelligence',
    # NO subir la versión: el módulo está en tools/no_bump.txt (el update
    # automático destapa errores pre-existentes de Odoo Studio y pinta el
    # build en rojo). Se despliega a mano con `odoo-update quimibond_intelligence`.
    'version': '19.0.30.0.0',
    'license': 'LGPL-3',
    'category': 'Productivity',
    'summary': 'Puente mínimo Odoo ↔ Supabase: contactos, empresas y usuarios para la memoria de correo.',
    'description': """
        Desde el 2026-09-18 Supabase guarda solo la memoria de correo; todo lo
        demás (ventas, finanzas, inventario, SAT, costeo) vive en Odoo. Este
        módulo empuja únicamente lo que esa memoria necesita para ligar correos
        con Odoo:

          * res.partner            → contacts + companies   (cada hora)
          * res.users + hr.employee → odoo_users            (cada hora)

        y trae de vuelta (cada 5 min) los comandos de sync_commands y los
        contactos nuevos que aparecen en el correo y no existen en Odoo.

        Se quitó el push de los otros 20+ modelos (facturas, órdenes, stock,
        manufactura, contabilidad, CFDI), el backfill histórico, la auditoría
        Odoo↔Supabase (quimibond.sync.audit) y el registro de fallos de
        ingesta (esquema `ingestion`, que ya no existe en Supabase).

        Tablas huérfanas en la base de Odoo: `quimibond_sync_audit` (modelo
        transitorio eliminado). Odoo no borra tablas de modelos que
        desaparecen del código; queda ahí hasta que alguien la tire a mano.
    """,
    'author': 'Quimibond',
    'website': 'https://quimibond.com',
    'depends': ['base', 'sale', 'purchase', 'account', 'stock', 'crm', 'mail'],
    'external_dependencies': {
        'python': ['httpx'],
    },
    'data': [
        'security/ir.model.access.csv',
        'data/ir_cron_data.xml',
        'data/cleanup_2026_09_18.xml',
        'views/sync_status_views.xml',
    ],
    'installable': True,
    'application': True,
    'auto_install': False,
}
