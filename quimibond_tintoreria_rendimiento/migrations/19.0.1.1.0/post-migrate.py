# -*- coding: utf-8 -*-
"""Migración 19.0.1.1.0

Repara los registros de `tintoreria.capacidad.rendimiento` creados con la
versión anterior del módulo, donde `codigo` y `nombre` eran campos de
texto libre capturados a mano (no vinculados a ningún `mrp.workcenter`).

Como esos registros se cargaron originalmente vía datos con
`noupdate="1"`, Odoo no los vuelve a tocar en una actualización normal de
módulo — por eso, tras el cambio a campos `related` sobre `workcenter_id`,
quedaron con `workcenter_id` vacío y sus valores viejos de `codigo`/
`nombre` como texto suelto en la base de datos.

Esta migración busca, para cada registro sin `workcenter_id`, el centro
de trabajo real que coincide por código (o por nombre, como respaldo) y
lo vincula. Al escribir `workcenter_id`, Odoo recalcula automáticamente
`codigo` y `nombre` a partir del centro de trabajo real.
"""
from odoo import api, SUPERUSER_ID


def migrate(cr, version):
    env = api.Environment(cr, SUPERUSER_ID, {})
    Config = env['tintoreria.capacidad.rendimiento']
    Workcenter = env['mrp.workcenter']

    huerfanos = Config.search([('workcenter_id', '=', False)])
    for rec in huerfanos:
        # En este punto, rec.codigo / rec.nombre todavía reflejan el valor
        # viejo almacenado en la columna (texto suelto de la versión
        # anterior), ya que nada ha disparado aún el recálculo del campo
        # related. Los usamos para encontrar el centro de trabajo real.
        codigo_viejo = (rec.codigo or '').strip()
        # 'nombre' pudo haber sido eliminado del modelo en una versión
        # posterior a esta migración — se accede de forma defensiva para
        # que este script siga funcionando igual si el servidor salta
        # directo a una versión más nueva sin pasar por 19.0.1.1.0 primero.
        nombre_viejo = (getattr(rec, 'nombre', '') or '').strip()

        workcenter = env['mrp.workcenter']
        if codigo_viejo:
            workcenter = Workcenter.search([('code', '=', codigo_viejo)], limit=1)
        if not workcenter and nombre_viejo:
            workcenter = Workcenter.search([('name', '=', nombre_viejo)], limit=1)

        if workcenter:
            rec.workcenter_id = workcenter.id
