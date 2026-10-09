# -*- coding: utf-8 -*-
"""57.122.0: uso diario del proyecto de desarrollo (tarjeta, características solas, asistente)."""
from odoo.tests import TransactionCase, tagged


@tagged('post_install', '-at_install')
class TestDevBoard(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.Project = cls.env['project.project']
        cls.partner = cls.env['res.partner'].create({'name': 'CLIENTE TABLERO PRUEBA', 'is_company': True})

    def _dev(self, **vals):
        return self.Project.create(dict({'name': 'x', 'sgi_is_ft': True, 'partner_id': self.partner.id,
                                         'sgi_dev_product_name': 'Jersey tablero'}, **vals))

    def test_01_card_opens_the_form_and_tasks_stay_reachable(self):
        dev = self._dev()
        plain = self.Project.create({'name': 'Proyecto normal prueba'})
        action = dev.action_view_tasks()
        self.assertIn('context', action, "sale_project escribe action['context'] sobre lo que devuelve super().")
        self.assertIn('domain', action)
        self.assertEqual((action['res_model'], action.get('res_id'), action['view_mode']),
                         ('project.project', dev.id, 'form'), "La tarjeta de un desarrollo abre su ficha.")
        tasks = dev.action_sgi_dev_view_tasks()
        self.assertEqual(tasks['res_model'], 'project.task', "El botón y los enlaces «Tareas» siguen abriendo tareas.")
        self.assertEqual(plain.action_view_tasks()['res_model'], 'project.task', "Un proyecto normal no cambia.")
        tpl = self.Project.create({'name': 'PLANTILLA tablero', 'is_template': True, 'sgi_is_ft': True})
        self.assertEqual(tpl.action_view_tasks()['res_model'], 'project.task', "Las plantillas no cambian.")

    def test_02_lines_load_alone_when_the_table_is_empty(self):
        Template = self.env['sgi.dev.characteristic.template']
        general = Template.search_count([('dev_type', '=', 'general')])
        carda = Template.search_count([('dev_type', '=', 'carda')])
        self.assertTrue(general and carda, "Los catálogos del módulo están cargados.")
        dev = self._dev()
        self.assertEqual(len(dev.sgi_dev_line_ids), general, "Al crear el desarrollo se proponen las del tipo.")
        dev.write({'sgi_dev_type': 'carda'})
        self.assertEqual(len(dev.sgi_dev_line_ids), general, "Con la tabla llena, cambiar el tipo no agrega nada solo.")
        dev.sgi_dev_line_ids.unlink()
        dev.write({'sgi_dev_type': 'tramado'})
        self.assertEqual(len(dev.sgi_dev_line_ids), Template.search_count([('dev_type', '=', 'tramado')]),
                         "Con la tabla vacía, elegir el tipo la llena.")
        quiet = self.Project.with_context(sgi_dev_no_autoload=True).create(
            {'name': 'y', 'sgi_is_ft': True, 'partner_id': self.partner.id})
        self.assertFalse(quiet.sgi_dev_line_ids, "Con sgi_dev_no_autoload no se propone nada.")
        plain = self.Project.create({'name': 'Proyecto normal 2'})
        self.assertFalse(plain.sgi_dev_line_ids)

    def test_03_mark_wizard_lists_candidates_and_marks_only_the_chosen(self):
        Stage = self.env['project.project.stage']
        review = Stage.create({'name': 'Por revisar', 'sequence': 24})
        coded = self.Project.create({'name': 'WJ053Q21JNT178'})
        reviewing = self.Project.create({'name': 'Proyecto viejo sin código', 'stage_id': review.id})
        other = self.Project.create({'name': 'Mantenimiento planta'})
        already = self._dev()
        tpl = self.Project.create({'name': 'WK140Q48JNT165', 'is_template': True})
        candidates = self.Project._sgi_dev_mark_candidates()
        self.assertIn(coded, candidates)
        self.assertIn(reviewing, candidates)
        self.assertNotIn(other, candidates)
        self.assertNotIn(already, candidates, "Lo que ya es desarrollo no se propone.")
        self.assertNotIn(tpl, candidates, "Las plantillas no se proponen.")
        wizard = self.env['sgi.dev.mark.wizard'].create({})
        self.assertIn(coded, wizard.project_ids)
        wizard.write({'project_ids': [(3, reviewing.id)]})
        action = wizard.action_mark()
        self.assertTrue(coded.sgi_is_ft)
        self.assertFalse(reviewing.sgi_is_ft, "Solo se marca lo que quedó en la lista.")
        self.assertFalse(coded.sgi_dev_line_ids, "Sin la casilla no se proponen características.")
        self.assertFalse(coded.sgi_dev_stage_key, "Marcar no mueve de etapa.")
        self.assertEqual(action['res_model'], 'project.project')
        self.assertNotIn(coded, self.Project._sgi_dev_mark_candidates(), "Idempotente: ya marcado no se vuelve a proponer.")
        wizard2 = self.env['sgi.dev.mark.wizard'].create({'project_ids': [(6, 0, reviewing.ids)], 'load_lines': True})
        wizard2.action_mark()
        self.assertTrue(reviewing.sgi_is_ft)
        self.assertTrue(reviewing.sgi_dev_line_ids, "Con la casilla se proponen las del tipo general.")

    def test_04_missing_partner_flag_and_views_load(self):
        dev = self._dev(partner_id=False)
        self.assertTrue(dev.sgi_dev_missing_partner)
        dev.write({'partner_id': self.partner.id})
        self.assertFalse(dev.sgi_dev_missing_partner)
        dev.write({'sgi_dev_origin': 'interno', 'partner_id': False})
        self.assertFalse(dev.sgi_dev_missing_partner, "Un desarrollo interno no necesita cliente.")
        for view_type, view_id in (('kanban', False), ('form', False),
                                   ('list', self.env.ref('quimibond_sgi.sgi_dev_project_view_list_board').id)):
            arch = self.Project.get_view(view_id=view_id, view_type=view_type)['arch']
            self.assertIn('sgi_dev_missing_partner', arch, view_type)
        self.assertTrue(self.env['sgi.dev.mark.wizard'].get_view(view_type='form')['arch'])
