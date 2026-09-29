# -*- coding: utf-8 -*-
{
    'name': "Quimibond SGI - Puente Revisado",
    'summary': "Pareto de defectos del revisado de tela (etiquetas TEJIDO-*) para el SGI",
    'description': """
Puente entre el revisado de tela (mrp_revisado_telas) y el SGI.

Aporta las vistas pivot/graph del registro de revisado (mrp.revision.log) para
armar el Pareto de defectos por causa (etiquetas de calidad ``TEJIDO-*``), fuente
del tablero de calidad del piso. No modifica mrp_revisado_telas. Se instala
automáticamente cuando conviven ambos módulos.

Desde 4.2.0 (quimibond_sgi 57.10.0, auditoría A-019) trae también el indicador
MA-03 «Calidad PQ»: el modo de cálculo ``calidad_pq`` (rollos revisados sin
defecto ÷ revisados), su detalle, su evidencia y los avisos del Diagnóstico.
Antes vivía en el núcleo protegido con ``if 'mrp.revision.log' in env``.
    """,
    'author': "Quimibond",
    'website': "https://www.quimibond.com",
    'category': 'Manufacturing/SGI',
    'version': '19.0.4.2.0',
    'license': 'LGPL-3',
    'depends': [
        'quimibond_sgi',
        'mrp_revisado_telas',
    ],
    'data': [
        'views/mrp_revision_log_views.xml',
    ],
    'post_init_hook': 'post_init_hook',
    'auto_install': True,
    'installable': True,
}
