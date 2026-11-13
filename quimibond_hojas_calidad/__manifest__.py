# -*- coding: utf-8 -*-
{
    'name': 'Quimibond - Libros de calidad (Rama e Inspección)',
    'version': '19.0.1.0.0',
    'category': 'Manufacturing',
    'summary': 'Botones REGISTRO RAMA y REVISIÓN TELA ACABADA: abren el libro de hoja de cálculo del artículo en Documentos',
    'description': """
Libros de calidad — tela acabada
================================
Rama y Laboratorio llevan un libro de hoja de cálculo por artículo y por proceso (una columna por rollo, de varias
órdenes; se cierra a los 700 registros y se abre uno nuevo). Este módulo no usa el módulo de Calidad: un punto de control
genera una hoja por orden y aquí el libro es continuo.

* **Plantillas:** una por artículo y proceso (Rama, Inspección). Se sube el .xlsx a Documentos, se abre con Odoo
  Spreadsheet, se deja en blanco y se elige en Manufactura → Configuración → Plantillas de libros de calidad.
* **REGISTRO RAMA:** botón en la operación de Rama de la orden en kg. Abre el libro vigente del artículo; si no existe lo
  crea copiando la plantilla.
* **REVISIÓN TELA ACABADA:** botón en la operación CLASIFICACION de la orden en metros (laboratorio revisa los rollos PQ).
  Abre el libro de Inspección del artículo. La liberación para Empaque vive en ``quimibond_rollos_bitacora`` (3.20.0).
* **Cerrar libro y abrir uno nuevo:** archiva el libro lleno (queda en el historial) y copia otra vez la plantilla.

Primera versión: el operador captura a mano en el libro. Llenar la fila de datos de cada rollo desde Odoo queda para una
segunda etapa.
""",
    'author': 'Jose Sacramento Consolti',
    'license': 'LGPL-3',
    'depends': [
        'quimibond_rollos_bitacora',
        'documents_spreadsheet',  # Enterprise: documents.document con handler «spreadsheet»
    ],
    'data': [
        'security/ir.model.access.csv',
        'views/mrp_hoja_calidad_views.xml',
        'views/mrp_workorder_views.xml',
    ],
    'installable': True,
    'application': False,
}
