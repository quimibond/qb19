# -*- coding: utf-8 -*-
{
    'name': "Quimibond SGI - Instructivos en Conocimiento",
    'summary': "El instructivo de una actividad del SGI se escribe en Conocimiento y se publica como revisión del IT",
    'description': """
Puente entre el SGI y Conocimiento (knowledge), DOC-5.

El instructivo de una actividad se escribe en un artículo de Conocimiento
(con fotos) y «Publicar como instructivo» lo congela como revisión del
documento controlado con su clave IT: PDF del artículo en Documentos, ligado
a la actividad, con acuses para los puestos y la huella del contenido para
saber si el artículo cambió después.

Salió del núcleo en quimibond_sgi 57.9.0 (auditoría A-014): el SGI ya no
depende de Conocimiento. Se instala solo cuando conviven ambos módulos; en
una base que venía de una versión anterior, el update del SGI lo instala y le
pasa los campos existentes sin borrar datos.
    """,
    'author': "Quimibond",
    'website': "https://www.quimibond.com",
    'category': 'Services/SGI',
    'version': '19.0.1.0.0',
    'license': 'LGPL-3',
    'depends': [
        'quimibond_sgi',
        'knowledge',
    ],
    'data': [
        'security/ir.model.access.csv',
        'views/sgi_instruction_knowledge_views.xml',
        'report/report_knowledge_instruction.xml',
    ],
    'auto_install': True,
    'installable': True,
}
