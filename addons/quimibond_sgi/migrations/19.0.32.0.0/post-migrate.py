# -*- coding: utf-8 -*-
"""COA fase 1 (19.0.32.0.0).

- Marca «Requiere COA en cada embarque» en los 12 clientes que lo piden
  (compañía principal). Las salidas abiertas toman el requisito; las ya
  validadas no se marcan retroactivamente. WOODBRIDGE (3924) y MACLIN (7096)
  quedan fuera hasta que lo confirme el Jefe de Calidad.
- Crea el alias de correo «coa» hacia el buzón de COA si no existe.
"""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)

COA_PARTNERS = {
    5796: "SALTILLO LAMINATION",
    1780: "SHAWMUT LLC",
    8095: "LEAR MEXICAN SEATING CORP.",
    8101: "LEAR MEXICAN SEATING CORP. ZARAGOZA PLANT",
    1265: "CONTITECH MEXICANA",
    5357: "ZWISSTEX MEXICO",
    1779: "FXI INC",
    5455: "FXI DE JUÁREZ",
    1433: "TQ-1 DE MEXICO",
    1768: "SEIREN VISCOTEC MEXICO",
    1762: "PIELES SINTETICAS",
    1733: "COPO TEXTILE MEXICO",
}


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    _mark_partners(env)
    _create_alias(env)


def _mark_partners(env):
    company = env.ref('base.main_company', raise_if_not_found=False)
    if not company:
        return
    partners = env['res.partner'].browse(list(COA_PARTNERS)).exists()
    # Solo si el id sigue siendo el cliente esperado (la migración no debe
    # marcar a otro en una base distinta a producción).
    ok = partners.filtered(lambda p: (p.name or '').upper().startswith(
        COA_PARTNERS[p.id].upper()[:12]))
    for missing in set(COA_PARTNERS) - set(ok.ids):
        _logger.warning("SGI COA: el cliente %s (%s) no se marcó: no existe o cambió.",
                        missing, COA_PARTNERS[missing])
    if ok:
        ok.with_company(company).write({'sgi_requires_coa': True})
        _logger.info("SGI COA: %d clientes marcados con «Requiere COA».", len(ok))


def _create_alias(env):
    Alias = env['mail.alias'].sudo()
    if Alias.search_count([('alias_name', '=', 'coa')]):
        _logger.info("SGI COA: ya existe un alias «coa»; no se crea otro.")
        return
    model = env['ir.model']._get('sgi.coa.inbox')
    try:
        with env.cr.savepoint():
            Alias.create({'alias_name': 'coa', 'alias_model_id': model.id,
                          'alias_contact': 'everyone'})
        _logger.info("SGI COA: alias «coa» creado hacia el buzón de COA.")
    except Exception:  # noqa: BLE001 - el alias es opcional; no frena el update
        _logger.exception("SGI COA: no se pudo crear el alias «coa».")
