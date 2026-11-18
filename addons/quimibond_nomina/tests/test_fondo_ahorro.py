# -*- coding: utf-8 -*-
"""Etiqueta «SIN FONDO DE AHORRO»: se crea si falta, se adopta si ya existe
con ese nombre (producción la tiene creada a mano), es idempotente y la
condición de las reglas del fondo la reconoce.

No corre en el CI (depende de Enterprise); se corre en el shell de Odoo.sh:
``odoo-bin ... --test-tags /quimibond_nomina --stop-after-init``."""
from odoo.tests.common import TransactionCase
from odoo.tools.safe_eval import safe_eval

from odoo.addons.quimibond_nomina.models.fondo_ahorro import TAG_SIN_FONDO_AHORRO

CONDICION = "result = 'SIN FONDO DE AHORRO' not in employee.category_ids.mapped('name')"


class TestFondoAhorro(TransactionCase):

    def _xmlid(self):
        return self.env['ir.model.data'].search([
            ('module', '=', 'quimibond_nomina'), ('name', '=', 'category_sin_fondo_ahorro')])

    def test_adopta_la_existente_e_idempotente(self):
        Category = self.env['hr.employee.category']
        # Quitar el xmlid que dejó la instalación, como en producción antes de 1.19.4.
        self._xmlid().unlink()
        existente = Category.search([('name', '=', TAG_SIN_FONDO_AHORRO)]) or \
            Category.create({'name': TAG_SIN_FONDO_AHORRO})
        tag = Category.qb_nomina_ensure_sin_fondo_ahorro()
        self.assertEqual(tag, existente)
        self.assertEqual(Category.qb_nomina_ensure_sin_fondo_ahorro(), existente)
        self.assertEqual(Category.search_count([('name', '=', TAG_SIN_FONDO_AHORRO)]), 1)
        self.assertEqual(self._xmlid().res_id, existente.id)

    def test_crea_si_falta(self):
        Category = self.env['hr.employee.category']
        self._xmlid().unlink()
        Category.search([('name', '=', TAG_SIN_FONDO_AHORRO)]).unlink()
        tag = Category.qb_nomina_ensure_sin_fondo_ahorro()
        self.assertEqual(tag.name, TAG_SIN_FONDO_AHORRO)
        self.assertEqual(self._xmlid().res_id, tag.id)

    def test_condicion_de_las_reglas(self):
        tag = self.env['hr.employee.category'].qb_nomina_ensure_sin_fondo_ahorro()
        otra = self.env['hr.employee.category'].create({'name': 'QB prueba fondo'})
        con = self.env['hr.employee'].create({'name': 'Con fondo', 'category_ids': [(6, 0, otra.ids)]})
        sin = self.env['hr.employee'].create({'name': 'Sin fondo', 'category_ids': [(6, 0, (tag | otra).ids)]})
        for employee, esperado in ((con, True), (sin, False)):
            ctx = {'employee': employee}
            safe_eval(CONDICION, ctx, mode='exec', nocopy=True)
            self.assertIs(ctx['result'], esperado)
