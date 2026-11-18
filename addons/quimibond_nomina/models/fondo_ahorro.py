# -*- coding: utf-8 -*-
"""Empleados sin fondo de ahorro.

Las reglas ``FAP``, ``FAE`` y ``FA_EMPRESA`` de «Paga regular» cobraban el
fondo a todo el que no es semanal, sin forma de excluir a nadie. RH confirmó
(Quincena 22 CDMX, 18-nov-2026) que hay empleados que no tienen fondo de
ahorro. Desde entonces las tres reglas tienen esta condición, escrita en la
base y vigilada por el centinela:

    result = 'SIN FONDO DE AHORRO' not in employee.category_ids.mapped('name')

La etiqueta de empleado se busca **por nombre** (en ``safe_eval`` no hay
``env.ref``), así que no se renombra. Aquí sólo se garantiza que exista y que
tenga xmlid del módulo; el módulo no escribe en ``hr.salary.rule``."""
import logging

from odoo import api, models

_logger = logging.getLogger(__name__)

TAG_SIN_FONDO_AHORRO = 'SIN FONDO DE AHORRO'
XMLID_SIN_FONDO_AHORRO = 'category_sin_fondo_ahorro'
FONDO_AHORRO_CODES = ('FAP', 'FAE', 'FA_EMPRESA')


class HrEmployeeCategory(models.Model):
    _inherit = 'hr.employee.category'

    @api.model
    def qb_nomina_ensure_sin_fondo_ahorro(self):
        """Deja la etiqueta «SIN FONDO DE AHORRO» con el xmlid
        ``quimibond_nomina.category_sin_fondo_ahorro``. Si ya existe con ese
        nombre (producción la tiene desde el 18-nov-2026, creada a mano) la
        adopta en lugar de crear otra: el nombre de la etiqueta es único.
        Idempotente; se llama en cada instalación/actualización."""
        tag = self.env.ref('quimibond_nomina.%s' % XMLID_SIN_FONDO_AHORRO, raise_if_not_found=False)
        if tag:
            if tag.name != TAG_SIN_FONDO_AHORRO:
                _logger.warning('quimibond_nomina: la etiqueta %s se llama «%s»; las reglas del fondo de '
                                'ahorro la buscan como «%s» y no la van a encontrar',
                                XMLID_SIN_FONDO_AHORRO, tag.name, TAG_SIN_FONDO_AHORRO)
            return tag
        Category = self.sudo()
        tag = Category.search([('name', '=', TAG_SIN_FONDO_AHORRO)], limit=1)
        if not tag:
            tag = Category.create({'name': TAG_SIN_FONDO_AHORRO})
        self.env['ir.model.data'].sudo().create({
            'module': 'quimibond_nomina',
            'name': XMLID_SIN_FONDO_AHORRO,
            'model': tag._name,
            'res_id': tag.id,
            'noupdate': True,
        })
        return tag
