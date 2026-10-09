# -*- coding: utf-8 -*-
{
    'name': "Quimibond SGI - Mapa de procesos",
    'summary': "El mapa de 14 procesos de producción, para cargarlo a mano en staging o en una recuperación",
    'description': """
Módulo de DATOS del SGI (decisión 6 de la auditoría: el SGI se instala
vacío). Trae el mapa de procesos de producción exportado el 2026-09-29 en
``data/mapa.json``: 14 procesos, 60 etapas y 310 actividades con numeral
congelado (con sus huecos), 1,072 roles por puesto, familia o rol relativo,
319 entregables, 246 entradas con su plazo, 13 familias de puestos, 93
indicadores con 50 términos de fórmula, 10 objetivos y 10 planes de control
(D-01).

Instalarlo NO carga nada (decisión 13): la carga es manual desde SGI →
Administración → Transición → Cargar mapa de procesos, siempre con el
modo de prueba primero, y solo la puede hacer un Administrador SGI.

Todo va por llave natural (clave de proceso, numeral, código de entregable
y de familia, nombre de puesto, clave de documento, XML ID de menú) y los
filtros con IDs de producción viajan con referencias portables (H-007). Lo
que no exista en la base que carga se reporta en la prueba.

Para regenerar el mapa: el mismo asistente descarga el de la base
(``sgi.process.export_payload``). 1.1.0: E1-02 va en ``acuerdos_rxd`` (D-13)
y sin la familia archivada OP-PTAR (D-15), editado a mano (ver
``meta.review``). ``tools/generar_mapa.py`` es el script con
el que se armó el primero desde lecturas de producción por MCP.
    """,
    'author': "Quimibond",
    'website': "https://www.quimibond.com",
    'category': 'Services/SGI',
    'version': '19.0.1.2.0',
    'license': 'OPL-1',
    'depends': ['quimibond_sgi'],
    'data': [
        'security/ir.model.access.csv',
        'views/sgi_mapa_views.xml',
    ],
    'installable': True,
}
