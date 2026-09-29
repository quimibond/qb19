# -*- coding: utf-8 -*-
"""Buscador por clave anterior y avance de la transición (19.0.57.0.0;
entrega 6). P-L6 de docs/audit/12-transicion.md §3.11. También ejercita las
columnas de las dos vistas SQL, que no se pueden probar fuera del build."""
from odoo.exceptions import AccessError
from odoo.tests import TransactionCase, tagged

from .common_documents import sgi_hide_real_documents, sgi_neutralize_dropbox_contradictions
from .common_users import sgi_test_user


@tagged('post_install', '-at_install')
class TestDropboxKey(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        sgi_hide_real_documents(cls.env)
        sgi_neutralize_dropbox_contradictions(cls.env)
        cls.env = cls.env(context=dict(cls.env.context, sgi_skip_role_check=True))
        cls.Doc = cls.env['documents.document']
        cls.Key = cls.env['sgi.dropbox.key']
        cls.process = cls.env['sgi.process'].create({'code': 'E8', 'name': 'Proceso buscador'})
        cls.old_process = cls.env['sgi.process'].create({'code': 'XDBK', 'name': 'Proceso viejo'})
        Activity = cls.env['sgi.process.activity']
        cls.activity = Activity.create({'process_id': cls.process.id, 'name': 'Capturar', 'step': 1})
        cls.archived = Activity.create({'process_id': cls.old_process.id, 'name': 'Numeral viejo',
                                        'step': 1})
        cls.env.flush_all()
        cls.env.cr.execute("UPDATE sgi_process_activity SET legacy_number = '4.1.2', active = FALSE "
                           "WHERE id = %s", (cls.archived.id,))
        cls.env.invalidate_all()
        act = cls.env['ir.actions.act_window'].create({'name': 'Destino E6', 'res_model': 'res.partner'})
        root = cls.env['ir.ui.menu'].create({'name': 'AppClaveE6'})
        cls.menu = cls.env['ir.ui.menu'].create({'name': 'MenuClaveE6', 'parent_id': root.id,
                                                 'action': 'ir.actions.act_window,%d' % act.id})
        cls.procedure = cls._doc('P-V81', 'procedimiento')
        cls.form = cls._doc('F-P-V81-01', 'formato', sgi_parent_document_id=cls.procedure.id)
        cls.form.write({'sgi_migration_class': 'a', 'sgi_migration_state': 'migrado',
                        'sgi_odoo_menu_id': cls.menu.id})
        cls.hidden = cls._doc('F-P-V81-02', 'formato', state='borrador')
        if 'access_internal' in cls.hidden._fields:
            cls.hidden.write({'access_internal': 'none'})
        if 'access_via_link' in cls.hidden._fields:
            cls.hidden.write({'access_via_link': 'none'})
        # La clave anterior la pone la migración 56.32.0 (clave del Dropbox).
        cls.Doc._sgi_migrate_previous_codes(ids=(cls.procedure | cls.form | cls.hidden).ids)
        cls.routine = cls.env['sgi.legacy.routine'].create({
            'procedure_id': cls.procedure.id, 'n': 3, 'name': 'Revisar la captura',
            'state': 'cubierta', 'activity_ids': [(6, 0, cls.activity.ids)]})
        cls.user = sgi_test_user(cls.env, login='e6k_user', groups='quimibond_sgi.group_sgi_user')
        # 57.0.0: las rutinas del buscador solo las ve Auditor, MAST, Dirección
        # o dueño de proceso; Dirección implica Usuario SGI (lee documentos).
        cls.director = sgi_test_user(cls.env, login='e6k_director',
                                     groups='quimibond_sgi.group_sgi_director')

    @classmethod
    def _doc(cls, code, doc_type, state='vigente', **extra):
        vals = {'name': '%s Documento buscador.pdf' % code, 'type': 'binary', 'sgi_is_controlled': True,
                'sgi_doc_type': doc_type, 'sgi_code': code, 'sgi_state': state,
                'sgi_process_id': cls.process.id}
        vals.update(extra)
        return cls.Doc.create(vals)

    def _find(self, key, user=None):
        Key = self.Key.with_user(user) if user else self.Key
        return Key.search([('key', '=', key)])

    # P-L6
    def test_l6_clave_rutina_y_numeral_archivado(self):
        doc_row = self._find('F-P-V81-01', self.user)
        self.assertEqual(len(doc_row), 1)
        self.assertEqual(doc_row.kind, 'formato')
        self.assertEqual(doc_row.document_id, self.form)
        self.assertEqual(doc_row.odoo_menu_id, self.menu)
        self.assertEqual(doc_row.destination, 'AppClaveE6/MenuClaveE6')
        action = doc_row.action_open_target()
        self.assertEqual(action['res_model'], 'res.partner', "«Abrir en Odoo» abre la acción del menú.")
        routine_row = self._find('P-V81 · 3', self.director)
        self.assertEqual(routine_row.routine_id, self.routine)
        self.assertEqual(routine_row.destination, 'E8.01')
        self.assertEqual(routine_row.action_open_target()['res_model'], 'sgi.process.activity')
        numeral = self._find('XDBK 4.1.2', self.user)
        self.assertEqual(numeral.activity_id, self.archived)
        self.assertEqual(self.Key.with_user(self.director).search([('title', 'ilike', 'Revisar la captura')]),
                         routine_row, "Se busca también por título.")

    def test_usuario_sgi_busca_sin_rutinas(self):
        """57.0.0 (decisión de Jose): el Usuario SGI usa el buscador, pero las
        rutinas no le salen, y «Ver el anterior» de un procedimiento le abre
        la ficha del documento (la de procedimiento anterior trae rutinas)."""
        Key = self.Key.with_user(self.user)
        self.assertFalse(self._find('P-V81 · 3', self.user), "Sin rutinas para el Usuario SGI.")
        self.assertFalse(Key.search([('kind', '=', 'rutina')]))
        proc_row = self._find('P-V81', self.user)
        self.assertEqual(len(proc_row), 1)
        view = proc_row.action_open_previous()['views'][0][0]
        self.assertEqual(view, self.env.ref('quimibond_sgi.sgi_dropbox_document_view_form').id)
        director_row = self._find('P-V81', self.director)
        self.assertEqual(director_row.action_open_previous()['views'][0][0],
                         self.env.ref('quimibond_sgi.sgi_dropbox_procedure_view_form').id)

    def test_l6_no_muestra_lo_que_no_puede_abrir(self):
        self.assertTrue(self._find('F-P-V81-02'), "El sistema lo ve.")
        self.assertFalse(self._find('F-P-V81-02', self.user),
                         "Un documento que el usuario no puede abrir no sale en el buscador.")
        self.assertFalse(self._find('P-I01', self.user), "P-I01 nunca sale.")

    def test_avance_por_proceso(self):
        with self.assertRaises(AccessError):
            self.env['sgi.dropbox.progress'].with_user(self.user).search(
                [('process_id', '=', self.process.id)])
        row = self.env['sgi.dropbox.progress'].with_user(self.director).search(
            [('process_id', '=', self.process.id)])
        self.assertEqual(len(row), 1)
        self.assertEqual(row.routines_total, 1)
        self.assertEqual(row.routines_covered, 1)
        self.assertEqual(row.routines_resolved_pct, 100.0)
        self.assertEqual(row.docs_total, 2)
        self.assertEqual(row.docs_migrated, 1)
        self.assertEqual(row.activities_new, 0, "La única actividad viene de una rutina.")
        self.assertEqual(row.action_open_routines()['res_model'], 'sgi.legacy.routine')
        self.assertTrue(row.action_open_procedures()['domain'])
