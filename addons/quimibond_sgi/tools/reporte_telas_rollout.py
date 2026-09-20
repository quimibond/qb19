# -*- coding: utf-8 -*-
"""Reporte de rollout de telas — Fase 4.3.2 (quimibond_sgi).

Lista de trabajo para Producción: telas SIN operación TEJIDO / BoM completa,
frente a las que ya están configuradas, agrupadas por familia (categoría).

NO configura nada: solo reporta. El criterio de configuración es:
  «una tela está configurada si tiene una lista de materiales (mrp.bom) con al
   menos una operación cuyo nombre empieza por TEJIDO».

Uso (shell de Odoo.sh):
    odoo-bin shell --no-http < addons/quimibond_sgi/tools/reporte_telas_rollout.py

Salida: imprime un resumen por familia y escribe un CSV en /tmp/telas_rollout.csv
(pendientes y configuradas). Parámetros ajustables abajo (universo de telas).
"""
import csv

# --- Parámetros ajustables por Producción -----------------------------------
# Prefijo del nombre de la operación de tejido.
OP_PREFIX = "TEJIDO"
# Universo de telas: se consideran telas los productos fabricables (con ruta de
# fabricación). Si prefieren acotar por familia, ponga aquí nombres de categoría
# (p.ej. ["Telas", "Tejido de punto"]); vacío = todas las fabricables.
FAMILY_NAMES = []
# ----------------------------------------------------------------------------


def _is_tejido_configured(template):
    for bom in template.bom_ids:
        for op in bom.operation_ids:
            if (op.name or "").upper().startswith(OP_PREFIX):
                return True
    return False


def _tela_templates(env):
    Template = env['product.template']
    domain = [('type', '=', 'consu')]
    manufacture_route = env.ref('mrp.route_warehouse0_manufacture',
                                raise_if_not_found=False)
    templates = Template.search(domain)
    # Solo fabricables (ruta de fabricación) si la ruta existe.
    if manufacture_route:
        templates = templates.filtered(
            lambda t: manufacture_route in t.route_ids or t.bom_ids)
    if FAMILY_NAMES:
        templates = templates.filtered(lambda t: t.categ_id.name in FAMILY_NAMES)
    return templates


def generar_reporte(env):
    templates = _tela_templates(env)
    pendientes, configuradas = [], []
    for tmpl in templates:
        row = {
            'familia': tmpl.categ_id.complete_name or tmpl.categ_id.name or '',
            'referencia': tmpl.default_code or '',
            'tela': tmpl.name,
        }
        if _is_tejido_configured(tmpl):
            configuradas.append(row)
        else:
            pendientes.append(row)

    # Resumen por familia
    por_familia = {}
    for row in pendientes:
        por_familia.setdefault(row['familia'], 0)
        por_familia[row['familia']] += 1

    print("=== Rollout de telas (Fase 4.3.2) ===")
    print("Configuradas (BoM con operación %s*): %d" % (OP_PREFIX, len(configuradas)))
    print("Pendientes de configurar: %d" % len(pendientes))
    print("--- Pendientes por familia ---")
    for familia, n in sorted(por_familia.items(), key=lambda x: -x[1]):
        print("  %-40s %d" % (familia or '(sin familia)', n))

    path = '/tmp/telas_rollout.csv'
    with open(path, 'w', newline='') as fh:
        writer = csv.DictWriter(fh, fieldnames=['estado', 'familia', 'referencia', 'tela'])
        writer.writeheader()
        for row in pendientes:
            writer.writerow(dict(row, estado='PENDIENTE'))
        for row in configuradas:
            writer.writerow(dict(row, estado='CONFIGURADA'))
    print("CSV escrito en: %s" % path)
    return pendientes, configuradas


# Auto-ejecución en `odoo-bin shell` (la variable `env` está disponible).
if 'env' in dir():
    generar_reporte(env)  # noqa: F821
