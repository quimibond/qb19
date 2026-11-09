# -*- coding: utf-8 -*-
"""19.0.3.16.0 — A3.5: se retira el esquema de clasificación anterior (wizard «rollo.clasificacion.wizard», orden en metros
ligada al rollo de la bitácora y etiqueta final de la bitácora) y el interruptor ``clasificacion_empaque``: la
Clasificación final en Empaque queda siempre activa. Idempotente."""


def migrate(cr, version):
    if not version:
        return
    cr.execute("DELETE FROM ir_config_parameter WHERE key = 'quimibond_rollos_bitacora.clasificacion_empaque'")
    # Columnas huérfanas de los campos retirados (el ORM no las borra).
    cr.execute("ALTER TABLE mrp_rollo_bitacora DROP COLUMN IF EXISTS production_b_id")
    cr.execute("ALTER TABLE mrp_rollo_bitacora DROP COLUMN IF EXISTS zpl_etiqueta_final")
