# -*- coding: utf-8 -*-
"""Re-vincula los textos antiguos con los nuevos Many2one:
- maquina_tejido   -> mrp.workcenter (por code, luego por name)
- jefe_manufactura -> hr.employee con puesto JEFE DE MANUFACTURA (por nombre)
- auxiliar_procesos-> hr.employee con puesto AUXILIAR DE PROCESOS (por nombre)
Lo que no se encuentre se deja vacío y se registra en el log."""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)
TABLE = 'ficha_tecnica_tejido'


def _has_old(cr, col):
    cr.execute("""SELECT 1 FROM information_schema.columns
                   WHERE table_name = %s AND column_name = %s""", (TABLE, col + '_old'))
    return bool(cr.fetchone())


def migrate(cr, version):
    env = api.Environment(cr, SUPERUSER_ID, {})
    Workcenter = env['mrp.workcenter'].with_context(active_test=False)
    Employee = env['hr.employee'].with_context(active_test=False)
    jobs = {'jefe_manufactura': 'JEFE DE MANUFACTURA',
            'auxiliar_procesos': 'AUXILIAR DE PROCESOS'}

    def _clean(v):
        v = (v or '').strip()
        return v[:-2] if v.endswith('.0') else v

    if _has_old(cr, 'maquina_tejido'):
        cr.execute('SELECT id, maquina_tejido_old FROM "%s" '
                   'WHERE maquina_tejido_old IS NOT NULL' % TABLE)
        for rec_id, old in cr.fetchall():
            txt = _clean(old)
            if not txt:
                continue
            wc = Workcenter.search([('code', '=', txt)], limit=1) \
                or Workcenter.search([('name', '=ilike', txt)], limit=1)
            if wc:
                cr.execute('UPDATE "%s" SET maquina_tejido = %%s WHERE id = %%s' % TABLE,
                           (wc.id, rec_id))
            else:
                _logger.warning('Ficha tejido %s: máquina "%s" sin centro de trabajo', rec_id, txt)
        cr.execute('ALTER TABLE "%s" DROP COLUMN maquina_tejido_old' % TABLE)

    for col, job in jobs.items():
        if not _has_old(cr, col):
            continue
        cr.execute('SELECT id, %s_old FROM "%s" WHERE %s_old IS NOT NULL' % (col, TABLE, col))
        for rec_id, old in cr.fetchall():
            txt = _clean(old)
            if not txt:
                continue
            emp = Employee.search([('name', '=ilike', txt),
                                   ('job_id.name', '=ilike', job)], limit=1)
            if emp:
                cr.execute('UPDATE "%s" SET %s = %%s WHERE id = %%s' % (TABLE, col),
                           (emp.id, rec_id))
            else:
                _logger.warning('Ficha tejido %s: "%s" no encontrado como %s', rec_id, txt, job)
        cr.execute('ALTER TABLE "%s" DROP COLUMN %s_old' % (TABLE, col))
