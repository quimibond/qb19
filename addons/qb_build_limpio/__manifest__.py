# -*- coding: utf-8 -*-
{
    'name': 'Quimibond - Build limpio',
    'version': '19.0.1.1.0',
    'license': 'LGPL-3',
    'category': 'Technical',
    'summary': 'Corrige en la base lo que pinta de naranja el build de Odoo.sh: campos y vistas de Studio con etiquetas duplicadas, dependencias no buscables, vistas inválidas, grupos inexistentes y columnas requeridas sin NOT NULL.',
    'description': """
Build limpio
============

Los avisos (WARNING) del build de Odoo.sh casi nunca vienen del código del
repo: vienen de la base de datos, de campos y vistas creados con Odoo Studio
a lo largo de los años. Como la base de cada rama es copia de producción, el
mismo aviso se repite en ``quimibond``, ``main`` y ``qbtesting`` hasta que
alguien lo corrige en la base.

Este módulo lo corrige solo. Al terminar cualquier instalación o
actualización de módulos (cuando ya está cargado todo el registro) revisa:

* **Etiquetas duplicadas** — dos campos del mismo modelo con el mismo nombre
  visible. Renombra el campo de Studio más nuevo agregando ``(2)``, ``(3)``…
  Nunca toca campos definidos en código.
* **Dependencias no buscables** — campos relacionados de Studio que pasan por
  un campo calculado sin búsqueda (``product_variant_id``, ``group_id``…).
  Los convierte en campos calculados equivalentes con dependencias sanas.
* **Vistas por defecto de Studio inválidas** — las repara si el arreglo es
  trivial (``quick_add`` → ``quick_create``) y si no, las archiva: Odoo
  vuelve a generar la vista por defecto.
* **Grupos que ya no existen** en vistas personalizadas — quita la referencia
  al grupo. Si era el único grupo del nodo, quita el nodo: es lo que ya pasaba
  (nadie pertenece a un grupo inexistente).
* **Columnas requeridas sin NOT NULL** — rellena los nulos con el valor por
  defecto del campo y pone la restricción.

También se puede correr a mano: Ajustes → Técnico → Build limpio.
    """,
    'author': 'Quimibond',
    'website': 'https://www.quimibond.com',
    'depends': ['base'],
    'data': [
        'data/actions.xml',
    ],
    'installable': True,
    'application': False,
    'auto_install': False,
}
