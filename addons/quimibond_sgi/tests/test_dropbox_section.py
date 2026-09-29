# -*- coding: utf-8 -*-
"""«Del Dropbox a Odoo», bloque 1 (19.0.56.39.0; entrega 6): la sección de
formatos y documentos anteriores. P-L7 de 12-transicion.md §3.11 (el usuario
no escribe los campos de transición) más C-011, L-011, L-012 y L-014."""
from odoo.exceptions import AccessError, ValidationError
from odoo.tests import TransactionCase, tagged

from .common_documents import sgi_hide_real_documents
from .common_users import sgi_test_user


@tagged('post_install', '-at_install')
class TestDropboxSection(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        sgi_hide_real_documents(cls.env)
        cls.env = cls.env(context=dict(cls.env.context, sgi_skip_role_check=True))
        cls.Doc = cls.env['documents.document']
        cls.process = cls.env['sgi.process'].create({'code': 'XDBX', 'name': 'Proceso Dropbox'})
        cls.user = sgi_test_user(cls.env, login='e6_usuario', groups='quimibond_sgi.group_sgi_user')
        cls.mast = sgi_test_user(cls.env, login='e6_mast')

    def _doc(self, code, doc_type='formato', state='vigente', **extra):
        vals = {'name': '%s Documento de prueba.pdf' % code, 'type': 'binary',
                'sgi_is_controlled': True, 'sgi_doc_type': doc_type, 'sgi_code': code,
                'sgi_state': state, 'sgi_process_id': self.process.id}
        vals.update(extra)
        return self.Doc.create(vals)

    # P-L7 (L-010 / E-005)
    def test_l7_el_usuario_no_escribe_la_transicion(self):
        doc = self._doc('F-P-A61-01', state='borrador', owner_id=self.user.id)
        if 'access_internal' in doc._fields:
            doc.access_internal = 'edit'
        mine = doc.with_user(self.user)
        self.assertFalse(mine.sgi_can_edit_transition)
        mine.write({'sgi_revision': 1})  # sí puede editar su documento
        for vals in ({'sgi_migration_state': 'en_curso'}, {'sgi_migration_class': 'a'},
                     {'sgi_migration_target': 'Ventas > Pedidos'},
                     {'sgi_previous_code': 'F-P-VIEJA-01'},
                     {'sgi_replaced_by_process_id': self.process.id}):
            with self.assertRaises(AccessError, msg=str(vals)), self.cr.savepoint():
                mine.write(vals)
        # Reescribir el mismo valor (lo que manda un formulario) no cuenta.
        mine.write({'sgi_migration_state': 'pendiente', 'sgi_migration_class': False})
        # Crear con datos de transición tampoco; con los valores por defecto sí.
        with self.assertRaises(AccessError):
            self.Doc.with_user(self.user)._sgi_check_transition_create(
                [{'name': 'x', 'sgi_migration_class': 'a'}])
        self.Doc.with_user(self.user)._sgi_check_transition_create(
            [{'name': 'x', 'sgi_migration_state': 'pendiente', 'sgi_migration_class': False}])
        # El Jefe MAST sí, y el sistema (sudo) también.
        doc.with_user(self.mast).write({'sgi_migration_class': 'a',
                                        'sgi_migration_target': 'Ventas > Pedidos'})
        self.assertTrue(doc.with_user(self.mast).sgi_can_edit_transition)
        doc.with_user(self.user).sudo().write({'sgi_migration_state': 'en_curso'})
        self.assertEqual(doc.sgi_migration_state, 'en_curso')

    # C-011
    def test_destino_menu_worksheet_texto(self):
        act = self.env['ir.actions.act_window'].create({'name': 'Prueba E6', 'res_model': 'res.partner'})
        root = self.env['ir.ui.menu'].create({'name': 'AppE6'})
        menu = self.env['ir.ui.menu'].create({'name': 'MenuE6', 'parent_id': root.id,
                                              'action': 'ir.actions.act_window,%d' % act.id})
        doc = self._doc('F-P-A61-02', sgi_migration_target='Texto libre')
        self.assertEqual(doc.sgi_destination_label, 'Texto libre')
        doc.sgi_odoo_menu_id = menu
        self.assertEqual(doc.sgi_destination_label, 'AppE6/MenuE6')
        self.assertFalse(self._doc('F-P-A61-03').sgi_destination_label)

    # L-014
    def test_familia_por_la_clave_anterior(self):
        for code, family in (('F-P-A23-04', 'P-A23'), ('DAT P-C10-01', 'P-C10'),
                             ('IT-P-P07-02', 'P-P07'), ('F-IT-P-P07-01-02', 'P-P07')):
            doc_type = {'F': 'formato', 'D': 'dat', 'I': 'instructivo'}[code[0]]
            if code.startswith('F-IT'):
                doc_type = 'formato_it'
            doc = self._doc(code, doc_type=doc_type)
            self.assertEqual(doc.sgi_legacy_family, family, code)
        loose = self.Doc.create({'name': 'F-P-A23-09 suelto.pdf', 'type': 'binary'})
        self.assertFalse(loose.sgi_legacy_family, "Solo los controlados llevan familia.")

    # L-012 / L-013
    def test_clase_y_estado_cuadran_al_escribir(self):
        doc = self._doc('F-P-A61-04')
        with self.assertRaises(ValidationError), self.cr.savepoint():
            doc.write({'sgi_migration_class': 'd', 'sgi_migration_state': 'pendiente'})
        with self.assertRaises(ValidationError), self.cr.savepoint():
            doc.write({'sgi_migration_class': 'a', 'sgi_migration_state': 'na'})
        with self.assertRaises(ValidationError), self.cr.savepoint():
            doc.write({'sgi_migration_class': 'a', 'sgi_migration_state': 'migrado'})
        doc.write({'sgi_migration_class': 'a', 'sgi_migration_state': 'migrado',
                   'sgi_migration_target': 'Ventas > Pedidos'})
        doc.write({'sgi_migration_class': 'd', 'sgi_migration_state': 'na'})

    # Respuesta 3 de Jose a L (migración 56.39.0)
    def test_sin_clase_pasan_a_d_y_no_aplica(self):
        it = self._doc('IT-P-A61-01', doc_type='instructivo')
        dat = self._doc('DAT P-A61-01', doc_type='dat')
        # Así hay 25 en producción: «migrado» sin clase (anterior a la regla).
        self.env.flush_all()
        self.env.cr.execute("UPDATE documents_document SET sgi_migration_state = 'migrado' "
                            "WHERE id = %s", (dat.id,))
        self.env.invalidate_all()
        classified = self._doc('IT-P-A61-02', doc_type='instructivo')
        classified.write({'sgi_migration_class': 'd', 'sgi_migration_state': 'na'})
        fmt = self._doc('F-P-A61-05')
        excluded_parent = self._doc('P-S71', doc_type='procedimiento')
        child = self._doc('IT-P-A61-03', doc_type='instructivo',
                          sgi_parent_document_id=excluded_parent.id)
        by_code = self._doc('DAT P-S71-02', doc_type='dat')
        ids = (it | dat | classified | fmt | child | by_code).ids
        result = self.Doc._sgi_classify_legacy_documents(exclude_codes=('P-S71',), ids=ids)
        self.assertEqual(result, {'instructivo': 1, 'dat': 1})
        for doc in (it, dat):
            self.assertEqual((doc.sgi_migration_class, doc.sgi_migration_state), ('d', 'na'))
        self.assertFalse(fmt.sgi_migration_class, "Los formatos no entran.")
        self.assertFalse(child.sgi_migration_class, "La familia del excluido no se toca (padre).")
        self.assertFalse(by_code.sgi_migration_class, "La familia del excluido no se toca (clave).")
        self.assertEqual(self.Doc._sgi_classify_legacy_documents(
            exclude_codes=('P-S71',), ids=ids), {}, "Idempotente.")
        self.assertIn('P-I01', self.Doc._sgi_dropbox_excluded_codes(), "P-I01 siempre fuera.")

    # L-011
    def test_cron_resuelve_menu_de_clase_a_o_b(self):
        act = self.env['ir.actions.act_window'].create({'name': 'Prueba E6b', 'res_model': 'res.partner'})
        root = self.env['ir.ui.menu'].create({'name': 'AppE6b'})
        menu = self.env['ir.ui.menu'].create({'name': 'MenuE6b', 'parent_id': root.id,
                                              'action': 'ir.actions.act_window,%d' % act.id})
        doc = self._doc('F-P-A61-06')
        doc.write({'sgi_migration_class': 'b', 'sgi_migration_target': 'AppE6b > MenuE6b'})
        no_class = self._doc('F-P-A61-07', sgi_migration_target='AppE6b > MenuE6b')
        self.Doc._sgi_cron_resolve_menus()
        self.assertEqual(doc.sgi_odoo_menu_id, menu)
        self.assertFalse(no_class.sgi_odoo_menu_id, "Sin clase A o B no se resuelve solo.")

    def test_menu_en_la_seccion(self):
        menu = self.env.ref('quimibond_sgi.menu_sgi_migration')
        self.assertEqual(menu.parent_id, self.env.ref('quimibond_sgi.menu_sgi_dropbox'))
        self.assertFalse(menu.group_ids, "Todos consultan: hereda del raíz.")
        visible = self.env['ir.ui.menu'].with_user(self.user)._visible_menu_ids()
        self.assertIn(menu.id, visible)
