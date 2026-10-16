# -*- coding: utf-8 -*-
{
    'name': 'Quimibond - Ficha Técnica de Tela',
    'version': '19.0.2.8.1',
    'summary': 'Fichas técnicas de Tejido y de Acabado, e importación masiva desde Excel',
    'description': """
Fichas Técnicas de Tela (Tejido y Acabado)
===========================================
Módulo para capturar, importar y administrar las fichas técnicas de tela,
separadas en 2 modelos:

- **Ficha Técnica de Tejido**: máquina, hilos, especificaciones de tejido,
  tela acondicionada (peso, ancho, espesor, elongación). Se vincula al
  producto "Tela en Proceso" (kg).
- **Ficha Técnica de Acabado**: rendimiento de tela acabada (mts/kg), peso,
  ancho, espesor, encogimiento (largo/ancho) y elongación (largo/ancho).
  Cada producto de "Tela Acabada" (m) — por ejemplo cada color — tiene su
  propia ficha de acabado, vinculada a la ficha de tejido que le sirve de
  base. Una misma ficha de tejido puede ser la base de varias fichas de
  acabado (varios colores/variantes).

Incluye:

- Importación masiva desde Excel (tabular, fila por fila, muchos
  artículos a la vez) para ambos modelos, con encabezados flexibles.
- Validaciones de tolerancia (ej. encogimiento máximo 5%).
- Botones inteligentes en la ficha de producto para acceder directo a la
  ficha de tejido y/o de acabado vinculada.
- 2.1.0: catálogo de características (unidad, método o norma, tipo de dato),
  claves de la regla de codificación de artículos (DAT P-D02-01) y renglones
  de característica en las fichas de tejido y acabado con **dos juegos de
  límites** (especificación del cliente y control interno) y la marca «va a
  la especificación del cliente». El SGI (proyectos de desarrollo) usa el
  mismo catálogo y los mismos límites.
- 2.1.1: se retira el código del modelo viejo `ficha.tecnica.tela` y su
  importador (sin uso; la tabla queda en la base hasta que se borre a
  mano). Las columnas fijas de las fichas de tejido y acabado no se tocan:
  los renglones de características conviven con ellas. Hay dos
  rendimientos y los dos se conservan: el de tejido alimenta el cálculo de
  tintorería; el de acabado valida los metros finales.
- 2.3.0: ficha de la máquina con configuraciones por fecha (cambio de
  cilindro); la ficha de tejido toma la vigente a su fecha.
- 2.4.0: la ficha de la máquina es para cualquier tipo de máquina (tejido,
  tintorería, acabado, inspección, abridora, devanadora…): cada tipo tiene
  sus parámetros y cada configuración un valor por parámetro.
- 2.5.0: avisos de máquina en producción: galga distinta al programar
  tejido y cargas por jet (con aviso de carga) en las órdenes de tintorería.
- 2.6.0: listo para el producto único de acabado en kg: el aviso de carga
  también en la orden de trabajo que cae en un jet, y el rendimiento se
  busca también por la ficha de acabado → su ficha de tejido.
- 2.7.0: una ficha de tejido por artículo, revisión y máquina (como los
  formatos de la planta); menús reordenados en la app «Fichas técnicas»
  (Tela, Máquinas, Importar, Configuración) con ícono.
- 2.8.0: proceso del artículo en la máquina: los parámetros tienen ámbito
  (de la máquina / del artículo en la máquina) y cada artículo guarda sus
  datos en cada máquina en que corre (velocidad en la rama, y lo que den
  las demás áreas), con importación desde Excel, tabla artículo × máquina
  y promedio por máquina.
""",
    'category': 'Manufacturing',
    'author': 'Jose Sacramento Consolti',
    'depends': ['mrp', 'product', 'account'],
    'data': [
        'security/ir.model.access.csv',
        'data/ficha_tecnica_caracteristica_data.xml',
        'data/ficha_tecnica_maquina_tipo_data.xml',  # 2.4.0
        'views/ficha_tecnica_tejido_views.xml',
        'views/ficha_tecnica_acabado_views.xml',
        'views/ficha_tecnica_caracteristica_views.xml',  # 2.1.0: después del menú raíz que usa
        'views/maquina_config_views.xml',  # 2.3.0: después del menú «Configuración» que usa
        'views/mrp_aviso_maquina_views.xml',  # 2.5.0
        'views/proceso_views.xml',  # 2.8.0
        'views/product_template_views.xml',
        'wizard/ficha_tecnica_tejido_import_wizard_views.xml',
        'wizard/ficha_tecnica_acabado_import_wizard_views.xml',
        'wizard/proceso_import_wizard_views.xml',  # 2.8.0
        'views/ficha_tecnica_menus.xml',  # 2.7.0: todos los menús, al final (las acciones ya existen)
    ],
    'installable': True,
    'application': True,
    'license': 'LGPL-3',
}
