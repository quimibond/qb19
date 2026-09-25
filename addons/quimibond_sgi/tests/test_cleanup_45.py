# -*- coding: utf-8 -*-
"""Limpieza 19.0.45.0.0: el árbol de cinco entradas, los xmlids retirados,
el religado de los procesos viejos y el retiro automático de lo sustituido."""
from odoo.tests import TransactionCase, tagged

from ..models.sgi_cleanup import SGI_MENU_ENTRIES, SGI_REMOVED_XMLIDS
from .common_documents import sgi_hide_real_documents


@tagged('post_install', '-at_install')
class TestCleanup45(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        sgi_hide_real_documents(cls.env)
        cls.Process = cls.env['sgi.process']
        cls.Document = cls.env['documents.document']

    def _doc(self, code, process, state='vigente', parent=None, doc_type='procedimiento',
             revision=0):
        vals = {
            'name': code, 'type': 'binary', 'sgi_is_controlled': True,
            'sgi_doc_type': doc_type, 'sgi_code': code, 'sgi_state': state,
            'sgi_revision': revision, 'sgi_process_id': process.id}
        if parent:
            vals['sgi_parent_document_id'] = parent.id
        return self.Document.create(vals)

    def test_01_menu_tree_has_five_entries(self):
        """Todo menú bajo el raíz del SGI desciende de una de las cinco
        entradas. Falla si alguien cuelga un menú suelto del raíz."""
        Menu = self.env['ir.ui.menu']
        self.assertFalse(
            Menu._sgi_menu_tree_offenders().mapped('complete_name'),
            "Hay menús fuera de las cinco entradas del SGI.")
        root = self.env.ref('quimibond_sgi.menu_sgi_root')
        for xmlid in SGI_MENU_ENTRIES:
            self.assertEqual(self.env.ref(xmlid).parent_id, root, "%s debe colgar del raíz." % xmlid)
        # Un menú suelto nuevo sí se detecta.
        stray = Menu.create({'name': 'Suelto de prueba', 'parent_id': root.id})
        self.assertIn(stray, Menu._sgi_menu_tree_offenders())

    def test_05_no_menu_points_to_a_deleted_action(self):
        """Inicio quedó apuntando al Panel de procesos borrado (producción,
        46.0.0). Ningún menú del SGI puede abrir una acción que no existe."""
        Menu = self.env['ir.ui.menu']
        self.assertFalse(Menu._sgi_menu_dangling_actions().mapped('complete_name'))
        self.assertFalse(self.env.ref('quimibond_sgi.menu_sgi_panel').action,
                         "Inicio es agrupador: sin acción.")

    def test_02_removed_xmlids_do_not_come_back(self):
        for xmlid in SGI_REMOVED_XMLIDS:
            self.assertFalse(
                self.env.ref(xmlid, raise_if_not_found=False),
                "%s fue retirado en 19.0.45.0.0 y no debe volver." % xmlid)
        # Lo que el CEO pidió conservar sigue en su lugar.
        improvement = self.env.ref('quimibond_sgi.menu_sgi_improvement_group')
        for xmlid in ('quimibond_sgi.menu_sgi_improvements', 'quimibond_sgi.menu_sgi_lessons'):
            self.assertEqual(self.env.ref(xmlid).parent_id, improvement)
        diagnostics = self.env.ref('quimibond_sgi.menu_sgi_analysis')
        for xmlid in ('quimibond_sgi.menu_sgi_diagnostic', 'quimibond_sgi.menu_sgi_week_stats',
                      'quimibond_sgi.menu_sgi_activity_methods', 'quimibond_sgi.menu_sgi_spec_gaps'):
            self.assertEqual(self.env.ref(xmlid).parent_id, diagnostics)

    def test_03_relink_follows_replaced_rule_then_map(self):
        old_a = self.Process.create({'code': 'XOLDA', 'name': 'Viejo A'})
        old_b = self.Process.create({'code': 'XOLDB', 'name': 'Viejo B'})
        new_1 = self.Process.create({'code': 'XNEW1', 'name': 'Nuevo 1'})
        new_2 = self.Process.create({'code': 'XNEW2', 'name': 'Nuevo 2'})
        proc_a = self._doc('P-A91', old_a)                       # sustituido por new_2
        fmt_a = self._doc('F-P-A91-01', old_a, parent=proc_a, doc_type='formato')
        # DAT-XA: una revisión vieja (0, obsoleta) y la vigente (1). Las dos
        # viajan juntas: la restricción de familia no deja separarlas. La
        # vieja se crea primero porque cada revisión nueva va por arriba.
        old_rev = self._doc('DAT-XA', old_a, state='obsoleto', doc_type='dat', revision=0)
        other_a = self._doc('DAT-XA', old_a, doc_type='dat', revision=1)  # mapa → new_1
        proc_b = self._doc('P-A92', old_b)                        # mapa → new_2
        risk = self.env['sgi.risk'].create({'name': 'Riesgo viejo', 'process_id': old_a.id})
        new_2.replaced_document_ids = [(6, 0, proc_a.ids)]
        (old_a | old_b).write({'active': False})

        summary = self.Process._sgi_relink_from_archived(
            mapping={'XOLDA': 'XNEW1', 'XOLDB': 'XNEW2'}, review_codes=('XOLDA',))

        self.assertEqual(proc_a.sgi_process_id, new_2, "Regla 1: lo sustituido va a quien lo sustituye.")
        self.assertEqual(fmt_a.sgi_process_id, new_2, "…con su familia.")
        self.assertEqual(other_a.sgi_process_id, new_1, "El resto sigue el mapa.")
        self.assertEqual(proc_b.sgi_process_id, new_2)
        self.assertEqual(old_rev.sgi_process_id, new_1, "La revisión vieja viaja con su clave.")
        self.assertEqual(risk.process_id, new_1)
        self.assertEqual(summary['documentos'], 5)
        self.assertEqual(summary['documentos_regla_sustituidos'], 2)
        self.assertEqual(summary['riesgos'], 1)
        for doc in (proc_a, fmt_a, other_a, proc_b):
            self.assertEqual(doc.sgi_state, 'vigente', "Religar no archiva ni obsoleta.")
        # Idempotente: una segunda corrida no mueve nada.
        again = self.Process._sgi_relink_from_archived(
            mapping={'XOLDA': 'XNEW1', 'XOLDB': 'XNEW2'}, review_codes=())
        self.assertEqual(again['documentos'], 0)

    def test_04_process_vigente_obsoletes_replaced_documents(self):
        old = self.Process.create({'code': 'XOLDC', 'name': 'Viejo C'})
        new = self.Process.create({'code': 'XNEWC', 'name': 'Nuevo C'})
        replaced = self._doc('P-A93', old)
        fmt = self._doc('F-P-A93-01', old, parent=replaced, doc_type='formato')
        new.replaced_document_ids = [(6, 0, replaced.ids)]
        new.write({'state': 'piloto'})
        self.assertEqual(replaced.sgi_state, 'vigente', "En piloto conviven.")
        new.write({'state': 'vigente'})
        self.assertEqual(replaced.sgi_state, 'obsoleto', "Vigente retira lo que sustituye.")
        self.assertEqual(fmt.sgi_state, 'vigente', "Los formatos de la familia siguen vigentes.")
        self.assertTrue(any('obsoleto' in (m.body or '').lower() for m in new.message_ids))
