# -*- coding: utf-8 -*-
{
    'name': 'Quimibond - Memoria del contacto',
    'version': '19.0.1.3.0',
    'license': 'LGPL-3',
    'category': 'Sales/CRM',
    'summary': 'Pestaña Memoria en cada contacto: hilos de correo, pendientes detectados, demanda y contactos, desde la memoria en Supabase.',
    'description': """
Memoria del contacto
====================

La memoria de Quimibond (correo de 52 buzones, adjuntos, pendientes y señales
de demanda extraídas) vive en Supabase. Este módulo la muestra dentro de Odoo:
al abrir un contacto, la pestaña **Memoria** trae lo que la memoria sabe de esa
empresa (por el partner comercial o por RFC): hilos recientes con quién habló
por última vez y si esperan respuesta nuestra, pendientes detectados en el
correo, demanda mencionada, contactos con su ritmo de respuesta y las notas de
relación. Se cachea por contacto y se refresca con un botón o al caducar.

No copia correos a Odoo: consulta la API REST de Supabase con la llave que ya
tiene ``quimibond_intelligence`` (``quimibond_intelligence.supabase_url`` /
``supabase_service_key``).

Ficha consolidada (v1.2): la memoria resume cada conversación con Claude
(tema, estado, quién debe responder, acuerdos, pendientes) y extrae hechos con
vigencia (condiciones de pago, precios, contactos clave, logística, calidad,
riesgos). La pestaña los muestra en "Quién la atiende", "Lo que sabemos" y
"Conversaciones (resumen de la memoria)" (RPC ``memoria_brief``).

Dueños aprendidos: cada noche la memoria cuenta qué buzón interno atiende a
cada empresa (y cada tipo de pendiente) y lo escribe en el contacto como
"Encargado (memoria)", con la evidencia. La persona detrás de cada buzón
compartido se define en Contactos → Configuración → Buzones (memoria). Las
obligaciones usan ese encargado como dueño por defecto.

Desde 1.3.0 la pestaña Memoria está oculta: solo la ve el grupo "Ver pestaña
Memoria" (privilegio Memoria), que nace sin miembros. El cron de dueños
aprendidos y el resto del módulo siguen funcionando sin la pestaña.
    """,
    'author': 'Quimibond',
    'website': 'https://www.quimibond.com',
    'depends': ['base', 'mail', 'contacts'],
    'data': [
        'security/qb_memoria_security.xml',
        'security/ir.model.access.csv',
        'data/ir_cron_data.xml',
        'views/res_partner_views.xml',
        'views/memoria_mailbox_views.xml',
    ],
    'installable': True,
    'application': False,
    'auto_install': False,
}
