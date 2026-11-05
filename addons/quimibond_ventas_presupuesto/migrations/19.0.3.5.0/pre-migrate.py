# -*- coding: utf-8 -*-
"""3.5.0: el presupuesto ya no es por equipo. La columna team_id de la cabecera se queda en la
base (Odoo no borra columnas de campos que desaparecen) y deja de ser obligatoria, para que los
documentos nuevos se puedan crear. La migración posterior la lee para poner el mercado."""


def migrate(cr, version):
    if not version:
        return
    cr.execute("""SELECT 1 FROM information_schema.columns
                   WHERE table_name = 'sgi_sales_budget' AND column_name = 'team_id'""")
    if cr.fetchone():
        cr.execute("ALTER TABLE sgi_sales_budget ALTER COLUMN team_id DROP NOT NULL")
