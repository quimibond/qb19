# -*- coding: utf-8 -*-
"""«Del Dropbox a Odoo», bloque 1 (19.0.56.39.0; entrega 6): la sección de
formatos y documentos anteriores. P-L7 de 12-transicion.md §3.11 (el usuario
no escribe los campos de transición) más C-011, L-011, L-012 y L-014.

57.0.0 (decisión de Jose): qué ve cada grupo en la sección y el grupo «Dueño
de proceso (SGI)» sincronizado desde sgi.process."""
from odoo.exceptions import AccessError, UserError, ValidationError
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
        cls.auditor = sgi_test_user(cls.env, login='e6_auditor', groups='quimibond_sgi.group_sgi_auditor')
        cls.director = sgi_test_user(cls.env, login='e6_director', groups='quimibond_sgi.group_sgi_director')
        cls.owner = sgi_test_user(cls.env, login='e6_duenio', groups='quimibond_sgi.group_sgi_user')
        cls.owner_employee = cls.env['hr.employee'].create({'name': 'Dueño E6', 'user_id': cls.owner.id})
        cls.owner_group = cls.env.ref('quimibond_sgi.group_sgi_process_owner')

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

    # ------------------------------------------------------------------
    # 57.0.0 (decisión de Jose): qué ve cada grupo en «Del Dropbox a Odoo».
    # ------------------------------------------------------------------
    RESTRICTED_MENUS = ('menu_sgi_dropbox_procedures', 'menu_sgi_dropbox_routines',
                        'menu_sgi_dropbox_progress')
    OPEN_MENUS = ('menu_sgi_dropbox_search', 'menu_sgi_migration')

    def _routine(self):
        proc = self._doc('P-V61', doc_type='procedimiento')
        return self.env['sgi.legacy.routine'].create({
            'procedure_id': proc.id, 'n': 1, 'name': 'Rutina de prueba', 'state': 'pendiente',
            'reason': 'Nadie la cubre todavía'})

    def _assert_reads_transition(self, user, routine):
        self.assertEqual(self.env['sgi.legacy.routine'].with_user(user).search(
            [('id', '=', routine.id)]), routine)
        self.assertEqual(routine.with_user(user).read(['name'])[0]['name'], 'Rutina de prueba')
        self.env['sgi.dropbox.progress'].with_user(user).search([('process_id', '=', self.process.id)])
        visible = self.env['ir.ui.menu'].with_user(user)._visible_menu_ids()
        for xmlid in self.RESTRICTED_MENUS + self.OPEN_MENUS:
            self.assertIn(self.env.ref('quimibond_sgi.' + xmlid).id, visible, (user.login, xmlid))

    def test_usuario_sgi_buscador_y_formatos_sin_rutinas_ni_avance(self):
        routine = self._routine()
        visible = self.env['ir.ui.menu'].with_user(self.user)._visible_menu_ids()
        for xmlid in self.OPEN_MENUS:
            self.assertIn(self.env.ref('quimibond_sgi.' + xmlid).id, visible, xmlid)
        for xmlid in self.RESTRICTED_MENUS:
            self.assertNotIn(self.env.ref('quimibond_sgi.' + xmlid).id, visible, xmlid)
        # La restricción es real: por RPC tampoco lee rutinas ni avance.
        with self.assertRaises(AccessError):
            self.env['sgi.legacy.routine'].with_user(self.user).search([])
        with self.assertRaises(AccessError):
            routine.with_user(self.user).read(['name'])
        with self.assertRaises(AccessError):
            self.env['sgi.dropbox.progress'].with_user(self.user).search([])
        # Las tres acciones llevan los mismos grupos que sus menús.
        restricted = (self.env.ref('quimibond_sgi.group_sgi_auditor')
                      | self.env.ref('quimibond_sgi.group_sgi_manager')
                      | self.env.ref('quimibond_sgi.group_sgi_director') | self.owner_group)
        for xmlid in ('sgi_dropbox_routine_action', 'sgi_dropbox_procedure_action',
                      'sgi_dropbox_progress_action'):
            self.assertEqual(self.env.ref('quimibond_sgi.' + xmlid).group_ids, restricted, xmlid)
        self.assertFalse(self.env.ref('quimibond_sgi.sgi_migration_action').group_ids)
        self.assertFalse(self.env.ref('quimibond_sgi.sgi_dropbox_key_action').group_ids)
        # «Formatos y documentos anteriores» sigue leyéndose (el conteo de
        # rutinas del documento es un campo guardado, no lee el modelo).
        self.Doc.with_user(self.user).search_read(
            [('sgi_is_controlled', '=', True), ('sgi_doc_type', 'in', ('formato', 'instructivo', 'dat'))],
            ['sgi_previous_code', 'sgi_destination_label', 'sgi_migration_state'], limit=5)
        self.assertEqual(routine.procedure_id.with_user(self.user).sgi_routine_pending_count, 1)

    def test_usuario_sgi_abre_en_odoo_sin_escribir(self):
        act = self.env['ir.actions.act_window'].create({'name': 'Destino E6c', 'res_model': 'res.partner'})
        root = self.env['ir.ui.menu'].create({'name': 'AppE6c'})
        menu = self.env['ir.ui.menu'].create({'name': 'MenuE6c', 'parent_id': root.id,
                                              'action': 'ir.actions.act_window,%d' % act.id})
        doc = self._doc('F-P-A61-08')
        doc.write({'sgi_migration_class': 'a', 'sgi_migration_state': 'migrado',
                   'sgi_odoo_menu_id': menu.id})
        self.env.flush_all()
        before = doc.write_date
        action = doc.with_user(self.user).action_sgi_open_odoo_form()
        self.assertEqual(action['res_model'], 'res.partner', "Abre la acción del menú ligado.")
        self.env.flush_all()
        doc.invalidate_recordset()
        self.assertEqual(doc.write_date, before, "«Abrir en Odoo» no escribe.")
        # Menú al que el usuario no tiene acceso: aviso claro, no pantalla rota.
        locked = self.env['ir.ui.menu'].create({
            'name': 'MenuE6d', 'parent_id': root.id, 'action': 'ir.actions.act_window,%d' % act.id,
            'group_ids': [(6, 0, self.env.ref('base.group_system').ids)]})
        doc_locked = self._doc('F-P-A61-09')
        doc_locked.write({'sgi_migration_class': 'a', 'sgi_migration_state': 'migrado',
                          'sgi_odoo_menu_id': locked.id})
        with self.assertRaises(UserError):
            doc_locked.with_user(self.user).action_sgi_open_odoo_form()
        # Sin menú ni worksheet: aviso.
        with self.assertRaises(UserError):
            self._doc('F-P-A61-10').with_user(self.user).action_sgi_open_odoo_form()

    def test_auditor_direccion_y_mast_leen_rutinas_y_avance(self):
        routine = self._routine()
        for user in (self.auditor, self.director, self.mast):
            self._assert_reads_transition(user, routine)

    def test_duenio_de_proceso_sincronizado(self):
        routine = self._routine()
        Process = self.env['sgi.process']
        self.assertNotIn(self.owner, self.owner_group.user_ids)
        with self.assertRaises(AccessError):
            self.env['sgi.legacy.routine'].with_user(self.owner).search([])
        # Ser dueño (al escribir owner_id) lo mete al grupo, y ya lee.
        self.process.write({'owner_id': self.owner_employee.id})
        self.assertIn(self.owner, self.owner_group.user_ids)
        self._assert_reads_transition(self.owner, routine)
        # El método es idempotente.
        self.assertEqual(Process._sgi_sync_process_owner_group(), {'added': [], 'removed': []})
        # Otro proceso con el mismo dueño; deja de serlo de uno: sigue dentro.
        other = Process.create({'code': 'XDBY', 'name': 'Otro proceso',
                                'owner_id': self.owner_employee.id})
        self.process.write({'owner_id': False})
        self.assertIn(self.owner, self.owner_group.user_ids)
        # Archivar el último proceso que tenía: sale del grupo y deja de leer.
        other.write({'active': False})
        self.assertNotIn(self.owner, self.owner_group.user_ids)
        with self.assertRaises(AccessError):
            self.env['sgi.legacy.routine'].with_user(self.owner).search([])
        # Alguien metido a mano sin ser dueño sale con el cron.
        self.owner_group.write({'user_ids': [(4, self.user.id)]})
        result = Process._sgi_sync_process_owner_group()
        self.assertIn(self.user.id, result['removed'])
        self.assertNotIn(self.user, self.owner_group.user_ids)
