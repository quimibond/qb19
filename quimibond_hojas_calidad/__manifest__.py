# -*- coding: utf-8 -*-
{
    'name': 'Quimibond - Libros de calidad (Rama e Inspección)',
    'version': '19.0.3.0.0',
    'category': 'Manufacturing',
    'summary': 'Calidad → Laboratorio tela acabada: Registro Rama y Revisión tela acabada abren el libro del artículo en Documentos',
    'description': """
Libros de calidad — tela acabada
================================
Rama y Laboratorio llevan un libro de hoja de cálculo por artículo y por proceso (una columna por rollo, de varias
órdenes; se cierra a los 700 registros y se abre uno nuevo). Este módulo no usa el módulo de Calidad: un punto de control
genera una hoja por orden y aquí el libro es continuo.

* **Plantillas:** una por artículo y proceso (Rama, Inspección). Se sube el .xlsx a Documentos, se abre con Odoo
  Spreadsheet, se deja en blanco y se elige en Manufactura → Configuración → Plantillas de libros de calidad.
* **Calidad → Laboratorio tela acabada → Registro Rama:** lo hace control de calidad, no el operador de Rama. Pide la orden
  en kg (abierta y con la operación de Rama en proceso o terminada) y abre el libro vigente del artículo; si no existe lo
  crea copiando la plantilla.
* **Calidad → Laboratorio tela acabada → Revisión tela acabada:** pide la orden en metros (abierta y aún no liberada) y
  abre el libro de Inspección del artículo.
* **Calidad → Laboratorio tela acabada → Liberar para Empaque:** pide la orden y el número de empleado (wizard de
  ``quimibond_rollos_bitacora``, 3.20.x).
* **Cerrar libro y abrir uno nuevo:** archiva el libro lleno (queda en el historial) y copia otra vez la plantilla.

Primera versión: el operador captura a mano en el libro. Llenar la fila de datos de cada rollo desde Odoo queda para una
segunda etapa.
""",
    'author': 'Jose Sacramento Consolti',
    'license': 'LGPL-3',
    'depends': [
        'quimibond_rollos_bitacora',
        'quality_control',  # menú Calidad → Laboratorio tela acabada
        'documents_spreadsheet',  # Enterprise: documents.document con handler «spreadsheet»
    ],
    'data': [
        'security/ir.model.access.csv',
        'views/mrp_hoja_calidad_views.xml',
        'wizard/mrp_hoja_revision_wizard_views.xml',
        'wizard/mrp_hoja_rama_wizard_views.xml',
        'views/menus.xml',
    ],
    'installable': True,
    'application': False,
}
