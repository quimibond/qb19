# -*- coding: utf-8 -*-
{
    'name': "Quimibond SGI - Puente Pesaje",
    'summary': "Escala a NC del SGI los rollos pesados fuera de tolerancia",
    'description': """
Puente entre el pesaje de rollos de tejido (pesaje_rollos_tejido) y el SGI.

Cuando el operador confirma el pesaje de un rollo PESE a estar fuera de la
tolerancia (±3 kg vs el Tamaño de Rollo Estándar), se genera una alerta de
calidad ligada a la orden de fabricación con el peso registrado, para que el
SGI la trate (y, si es sistémico, se escale a NC).

NO modifica el comportamiento de pesaje_rollos_tejido: solo agrega el gancho
hacia el SGI. Se instala automáticamente cuando conviven ambos módulos.

Desde 5.2.0 (quimibond_sgi 57.10.0, auditoría A-020) es dueño de la tolerancia
``quimibond_sgi.pesaje_tolerance_kg``: la siembra al instalarse y la muestra en
Ajustes → SGI → Piso. Antes lo hacía el núcleo.
    """,
    'author': "Quimibond",
    'website': "https://www.quimibond.com",
    'category': 'Manufacturing/SGI',
    'version': '19.0.5.2.0',
    'license': 'LGPL-3',
    'depends': [
        'quimibond_sgi',
        'pesaje_rollos_tejido',
    ],
    'data': [
        'data/pesaje_data.xml',
        'views/res_config_settings_views.xml',
    ],
    'post_init_hook': 'post_init_hook',
    'auto_install': True,
    'installable': True,
}
