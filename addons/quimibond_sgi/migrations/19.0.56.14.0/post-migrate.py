# -*- coding: utf-8 -*-
"""19.0.56.14.0 (9.1 evaluación de proveedores): «Sin datos» no es «Baja».

Antes, un proveedor sin recepciones con fecha compromiso tenía OTD 0 y
calificación 30 → «Baja» (44 de 87 evaluaciones con OTD 0; 84 de 87
proveedores en «Baja»). Se recalculan con el ORM las evaluaciones con OTD 0
(las que tienen datos reales quedan igual; las que no, pasan a «Sin datos» o
a la clase que dé su calidad) y, si esa evaluación es la más reciente del
proveedor, se vuelve a aplicar la clase al contacto. Idempotente; no borra.
"""
from odoo import api, SUPERUSER_ID


def migrate(cr, version):
    env = api.Environment(cr, SUPERUSER_ID, {})
    Eval = env['sgi.supplier.eval']
    evals = Eval.search([('otd_pct', '=', 0)])
    if not evals:
        return
    evals.action_recompute()
    no_data = evals.filtered(lambda ev: not ev.otd_has_data)
    for partner in no_data.mapped('partner_id'):
        latest = Eval.search([('partner_id', '=', partner.id)],
                             order='date_to desc, id desc', limit=1)
        if latest in no_data:
            latest.action_apply_to_partner()
