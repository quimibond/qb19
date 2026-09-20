# -*- coding: utf-8 -*-
"""Los 6 ir.config_parameter que se declaraban como <record> en data (además
de sembrarse en sgi.config.seed_parameters) dejan de estar en los XML. Sin
esta migración, Odoo borraría el parámetro al limpiar los xmlids huérfanos
del módulo — perdiendo el valor que MAST haya configurado. Se desliga el
xmlid (se borra solo la fila de ir_model_data); el parámetro sobrevive y
seed_parameters() sigue garantizando el default en instalaciones limpias.
"""


def migrate(cr, version):
    cr.execute("""
        DELETE FROM ir_model_data
        WHERE module = 'quimibond_sgi'
          AND model = 'ir.config_parameter'
          AND name IN (
              'sgi_param_nc_escalation_days',
              'sgi_param_fmea_npr_action',
              'sgi_param_risk_inmediata',
              'sgi_param_risk_media',
              'sgi_param_risk_intermedia',
              'sgi_param_waste_category'
          )
    """)
