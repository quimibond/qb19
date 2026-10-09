# -*- coding: utf-8 -*-
{
    'name': "Quimibond SGI - Instructivos en Conocimiento",
    'summary': "La documentación del SGI en Conocimiento: manuales por rol, instructivos, controles "
               "operacionales, protocolos y reglamentos, y su publicación como revisión controlada",
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

1.2.0 (2026-10-06): espacio «SGI» en Conocimiento sembrado desde el código
(manuales por rol, un artículo por proceso, «Reglamentos»; un artículo que
alguien editó no se pisa), «Ayuda» en SGI → Inicio, importador por lotes de
los instructivos, controles operacionales, protocolos y reglamentos vigentes
(borradores que solo ven el dueño del proceso y el Jefe MAST, sin tocar el
documento controlado) y la publicación de DOC-5 extendida a los cuatro tipos.
No cambia la huella de Mi procedimiento ni la del MIID.
    """,
    'author': "Quimibond",
    'website': "https://www.quimibond.com",
    'category': 'Services/SGI',
    'version': '19.0.1.2.2',
    'license': 'OPL-1',
    'depends': [
        'quimibond_sgi',
        'knowledge',
    ],
    'data': [
        'security/ir.model.access.csv',
        'views/sgi_instruction_knowledge_views.xml',
        'views/sgi_knowledge_views.xml',
        'report/report_knowledge_instruction.xml',
        'data/sgi_knowledge_data.xml',
        # Al final: las acciones de arriba y los padres del SGI ya existen.
        'views/sgi_knowledge_menus.xml',
    ],
    'auto_install': True,
    'installable': True,
}
