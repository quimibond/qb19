# -*- coding: utf-8 -*-
"""Rutas de menú que se le dicen al usuario (I-009, entrega 4).

Los textos del Diagnóstico, de los avisos y de los errores decían rutas del
árbol viejo («SGI → Medición → Indicadores», «SGI → Panel → …») que ya no
existen. Todas salen de aquí, con los nombres del árbol decidido
(views/sgi_menus.xml y tools/sgi_menu_tree.txt).

Cada entrada es (xmlid del menú, nombres de la ruta). La prueba
tests/test_menu_tree.py sube por los padres del menú y compara los nombres de
los menús del módulo con esta ruta: si alguien renombra o mueve un menú sin
cambiar aquí, la prueba falla.

Módulo sin modelos: se puede importar a nivel de módulo desde cualquier
archivo de models/.
"""

SGI_MENU_PATHS = {
    'mapa_procesos': ('quimibond_sgi.menu_sgi_process_map',
                      ('SGI', 'Procesos', 'Mapa de procesos')),
    'mi_procedimiento': ('quimibond_sgi.menu_sgi_my_procedure',
                         ('SGI', 'Inicio', 'Mi procedimiento')),
    'documentos_vigentes': ('quimibond_sgi.menu_sgi_current_documents',
                            ('SGI', 'Inicio', 'Documentos vigentes')),
    'indicadores': ('quimibond_sgi.menu_sgi_indicators',
                    ('SGI', 'Administración SGI', 'Indicadores', 'Indicadores')),
    'mediciones': ('quimibond_sgi.menu_sgi_measures',
                   ('SGI', 'Administración SGI', 'Indicadores', 'Mediciones')),
    'solicitudes_cambio': ('quimibond_sgi.menu_sgi_doc_changes',
                           ('SGI', 'Administración SGI', 'Documentos', 'Solicitudes de cambio')),
    'politica': ('quimibond_sgi.menu_sgi_policy',
                 ('SGI', 'Dirección', 'Política integral')),
    'riesgos': ('quimibond_sgi.menu_sgi_risks',
                ('SGI', 'Dirección', 'Riesgos y oportunidades')),
    'requisitos_legales': ('quimibond_sgi.menu_sgi_legal',
                           ('SGI', 'Dirección', 'Requisitos legales')),
    'partes_interesadas': ('quimibond_sgi.menu_sgi_interested_parties',
                           ('SGI', 'Dirección', 'Partes interesadas')),
    'planes_emergencia': ('quimibond_sgi.menu_sgi_emergency_plans',
                          ('SGI', 'Seguridad y ambiente', 'Planes de emergencia')),
    'aspectos_ambientales': ('quimibond_sgi.menu_sgi_env_aspects',
                             ('SGI', 'Seguridad y ambiente', 'Aspectos ambientales')),
    'no_conformidades': ('quimibond_sgi.menu_sgi_nc',
                         ('SGI', 'Mejora', 'No conformidades')),
    'formatos_odoo': ('quimibond_sgi.menu_sgi_config_format_map',
                      ('SGI', 'Administración SGI', 'Configuración',
                       'Formatos en documentos de Odoo')),
    'fuentes_nc': ('quimibond_sgi.menu_sgi_config_alert_sources',
                   ('SGI', 'Administración SGI', 'Configuración', 'Fuentes de NC automáticas')),
    'ajustes': ('quimibond_sgi.menu_sgi_config_settings',
                ('SGI', 'Administración SGI', 'Configuración', 'Ajustes')),
    'publicar_mi_procedimiento': ('quimibond_sgi.menu_sgi_my_procedure_publish',
                                  ('SGI', 'Administración SGI', 'Firmas de lectura',
                                   'Publicar Mi procedimiento')),
    'tabletas_planta': ('quimibond_sgi.menu_sgi_floor_tablets',
                        ('SGI', 'Administración SGI', 'Configuración', 'Tabletas de planta')),
    # Fuera del SGI: la raíz es de otra app («Empleados»); la prueba solo
    # compara los menús del módulo.
    'rh_faltantes': ('quimibond_sgi.menu_hr_sgi_employee_gaps',
                     ('Empleados', 'Empleados', 'Empleados sin puesto o sin correo')),
    'brechas_competencia': ('quimibond_sgi.menu_sgi_competences',
                            ('Empleados', 'Competencias SGI', 'Brechas de competencia (DNC)')),
}


def sgi_menu_path(key, suffix=None):
    """«SGI → Dirección → Política integral» (con «, suffix» si se da)."""
    text = " → ".join(SGI_MENU_PATHS[key][1])
    return "%s, %s" % (text, suffix) if suffix else text
