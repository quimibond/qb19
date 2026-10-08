# -*- coding: utf-8 -*-
{
    'name': "Quimibond SGI - Aprobaciones de Studio",
    'summary': "El rol «Aprueba» del SGI bloquea el botón del documento con la regla de aprobación de Studio",
    'description': """
Puente entre el SGI y las reglas de aprobación de Studio (web_studio).

Un renglón «Aprueba» de tipo «Botón de Odoo» crea y mantiene la regla de
aprobación nativa (studio.approval.rule) del botón del documento con las
personas del puesto; «Adoptar regla» toma una regla hecha a mano en el mismo
botón. Archivar cualquier regla de aprobación cierra sus avisos abiertos sin
aprobar ni rechazar.

Salió del núcleo en quimibond_sgi 57.9.0 (auditoría A-010, decisión D-10):
el SGI ya no depende de Studio. Se instala solo cuando conviven ambos
módulos; en una base que venía de una versión anterior, el update del SGI lo
instala y le pasa los campos existentes sin borrar datos.
    """,
    'author': "Quimibond",
    'website': "https://www.quimibond.com",
    'category': 'Services/SGI',
    'version': '19.0.1.0.5',
    'license': 'OPL-1',
    'depends': [
        'quimibond_sgi',
        'web_studio',
    ],
    'data': [
        'views/sgi_approval_studio_views.xml',
    ],
    'auto_install': True,
    'installable': True,
}
