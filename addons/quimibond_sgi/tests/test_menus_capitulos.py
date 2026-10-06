# -*- coding: utf-8 -*-
"""57.113.0 — Menús por capítulos del MIID (plan
docs/superpowers/plans/2026-10-06-sgi-57-113-0-menus.md).

Ocho entradas en el orden de la norma (Inicio, Reportar, Sistema, Planeación,
Seguridad y ambiente, Desempeño, Mejora, Administración), mismos xmlids,
cuatro menús archivados en su lugar (nada se borra), nadie gana ni pierde
una pantalla, el Diagnóstico de 8 a 4 entradas sin perder lo que salió del
menú, y el re-sello de «Mi procedimiento» cuando solo cambió la ruta.

La base del build es copia de producción: nada cuenta registros globales."""
import ast
import importlib.util
import os
from unittest.mock import patch

from lxml import etree

from odoo.exceptions import AccessError
from odoo.tests import TransactionCase, new_test_user, tagged
from odoo.tools.convert import convert_file
from odoo.tools.misc import file_open

from ..models.sgi_cleanup import SGI_MENU_ARCHIVED, SGI_MENU_ENTRIES, SGI_REMOVED_XMLIDS
from ..models.sgi_menu_paths import sgi_menu_path
from .common_documents import sgi_hide_real_documents
from .common_users import sgi_set_mast

_MODULE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# xmlid → padre nuevo de los 15 menús que cambian de carpeta.
MOVED = {
    'menu_sgi_documental': 'menu_sgi_processes',
    'menu_sgi_config_acks': 'menu_sgi_documental',
    'menu_sgi_miid': 'menu_sgi_processes',
    'menu_sgi_env_aspects': 'menu_sgi_direction',
    'menu_sgi_dashboard_health': 'menu_sgi_performance',
    'menu_sgi_admin_indicators': 'menu_sgi_performance',
    'menu_sgi_measure_reviews': 'menu_sgi_admin_indicators',
    'menu_sgi_satisfaction': 'menu_sgi_performance',
    'menu_sgi_audits': 'menu_sgi_performance',
    'menu_sgi_mgmt_review': 'menu_sgi_performance',
    'menu_sgi_my_procedure_publish': 'menu_sgi_admin',
    'menu_sgi_config_load': 'menu_sgi_transition',
    'menu_sgi_config_company_fix': 'menu_sgi_transition',
    'menu_sgi_config_env_aspect_transfer': 'menu_sgi_transition',
    'menu_sgi_config_doc_types': 'menu_sgi_config',
}
# Archivados sin cambiar de padre, con la acción que se queda viva.
ARCHIVED = {
    'menu_sgi_admin_signatures': ('menu_sgi_admin', None),
    'menu_sgi_activity_methods': ('menu_sgi_analysis', 'sgi_activity_method_action'),
    'menu_sgi_week_stats': ('menu_sgi_analysis', 'sgi_activity_week_stat_action'),
    'menu_sgi_menu_mismatch': ('menu_sgi_analysis', 'sgi_activity_menu_mismatch_action'),
}
OLD_PATHS = ('SGI → Dirección', 'SGI → Procesos', 'Administración SGI →', 'Mejora → Auditorías',
             'Seguridad y ambiente → Aspectos', 'Firmas de lectura →')


def _ref(env, xmlid):
    return env.ref('quimibond_sgi.' + xmlid)


@tagged('post_install', '-at_install')
class TestMenusCapitulos(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        env = cls.env

        def user(login, groups):
            return new_test_user(env, login=login, groups='base.group_user,' + groups)

        cls.plain = user('zc_plain', 'quimibond_sgi.group_sgi_user')
        cls.owner = user('zc_owner', 'quimibond_sgi.group_sgi_user,quimibond_sgi.group_sgi_process_owner')
        cls.director = user('zc_director', 'quimibond_sgi.group_sgi_director')
        cls.auditor = user('zc_auditor', 'quimibond_sgi.group_sgi_auditor')
        cls.mast = user('zc_mast', 'quimibond_sgi.group_sgi_manager')

    # ------------------------------------------------------------------
    def _reachable(self, user, xmlid):
        """El menú y todos sus padres están activos y el usuario está en los
        grupos de cada uno (o no tienen grupos): lo que deciden los grupos,
        sin depender de los permisos de cada modelo."""
        node = _ref(self.env, xmlid).sudo()
        while node:
            if not node.active:
                return False
            if node.group_ids and not any(
                    user.has_group(xid) for xid in node.group_ids.get_external_id().values() if xid):
                return False
            node = node.parent_id
        return True

    def _assert_sees(self, user, yes=(), no=()):
        for xmlid in yes:
            self.assertTrue(self._reachable(user, xmlid), "%s debe ver %s." % (user.login, xmlid))
        for xmlid in no:
            self.assertFalse(self._reachable(user, xmlid), "%s no debe ver %s." % (user.login, xmlid))

    # ------------------------------------------------------------------
    def test_01_ocho_entradas_en_orden(self):
        root = _ref(self.env, 'menu_sgi_root')
        children = self.env['ir.ui.menu'].sudo().with_context(lang='en_US').search(
            [('parent_id', '=', root.id)], order='sequence, id')
        expected = ['menu_sgi_panel', 'menu_sgi_report', 'menu_sgi_processes', 'menu_sgi_direction',
                    'menu_sgi_safety', 'menu_sgi_performance', 'menu_sgi_improvement_group', 'menu_sgi_admin']
        own = [m for m in children if (m.get_external_id().get(m.id) or '').startswith('quimibond_sgi.')]
        self.assertEqual([m.get_external_id()[m.id] for m in own], ['quimibond_sgi.' + x for x in expected])
        self.assertEqual(own.mapped('name'), ['Inicio', 'Reportar', 'Sistema', 'Planeación',
                                              'Seguridad y ambiente', 'Desempeño', 'Mejora', 'Administración'])
        self.assertEqual(SGI_MENU_ENTRIES, tuple('quimibond_sgi.' + x for x in expected))

    def test_02_padres(self):
        for xmlid, parent in MOVED.items():
            self.assertEqual(_ref(self.env, xmlid).parent_id, _ref(self.env, parent), xmlid)

    def test_03_archivados_en_su_lugar(self):
        self.assertEqual(set(SGI_MENU_ARCHIVED), {'quimibond_sgi.' + x for x in ARCHIVED})
        Menu = self.env['ir.ui.menu'].sudo().with_context(active_test=False)
        for xmlid, (parent, action) in ARCHIVED.items():
            menu = Menu.browse(_ref(self.env, xmlid).id)
            self.assertTrue(menu.exists(), "%s se archiva, no se borra." % xmlid)
            self.assertFalse(menu.active, xmlid)
            self.assertEqual(menu.parent_id, _ref(self.env, parent), "%s conserva su padre." % xmlid)
            self.assertNotIn('quimibond_sgi.' + xmlid, SGI_REMOVED_XMLIDS)
            if action:
                self.assertTrue(_ref(self.env, action).exists(), "La acción de %s sigue viva." % xmlid)
                self.assertEqual(menu.action, _ref(self.env, action))
        signatures = _ref(self.env, 'menu_sgi_admin_signatures')
        self.assertFalse(self.env['ir.ui.menu'].sudo().search([('parent_id', '=', signatures.id)]),
                         "Firmas de lectura quedó sin hijos activos.")
        for xmlid in ('menu_sgi_performance', 'menu_sgi_transition'):
            self.assertNotIn('quimibond_sgi.' + xmlid, SGI_REMOVED_XMLIDS)

    def test_04_lo_archivado_sigue_archivado_tras_cargar_el_xml(self):
        """VERIFICAR 1 del plan: volver a cargar views/sgi_menus.xml (lo que
        hace cada actualización) no reactiva lo archivado."""
        convert_file(self.env, 'quimibond_sgi', 'views/sgi_menus.xml', {}, mode='update', noupdate=False)
        self.env.invalidate_all()
        Menu = self.env['ir.ui.menu'].sudo().with_context(active_test=False)
        for xmlid in ARCHIVED:
            self.assertFalse(Menu.browse(_ref(self.env, xmlid).id).active, xmlid)
        for xmlid, parent in MOVED.items():
            self.assertEqual(_ref(self.env, xmlid).parent_id, _ref(self.env, parent), xmlid)

    def test_05_cada_quien_ve_lo_mismo_que_antes(self):
        """Q7: cada menú que cambió de carpeta conserva quién lo ve."""
        everyone = ('menu_sgi_processes', 'menu_sgi_process_map', 'menu_sgi_activities', 'menu_sgi_miid',
                    'menu_sgi_dropbox_search', 'menu_sgi_migration', 'menu_sgi_direction', 'menu_sgi_policy',
                    'menu_sgi_objectives', 'menu_sgi_risks', 'menu_sgi_env_aspects', 'menu_sgi_legal',
                    'menu_sgi_legal_evaluations', 'menu_sgi_performance', 'menu_sgi_audits',
                    'menu_sgi_audit_programs', 'menu_sgi_audit_list', 'menu_sgi_audit_findings',
                    'menu_sgi_safety', 'menu_sgi_incidents', 'menu_sgi_work_permits', 'menu_sgi_loto',
                    'menu_sgi_epp_deliveries', 'menu_sgi_checklist_requests', 'menu_sgi_improvement_group',
                    'menu_sgi_nc')
        catalogs = ('menu_sgi_deliverable_list', 'menu_sgi_process_flows', 'menu_sgi_analysis_who',
                    'menu_sgi_jobs_roles', 'menu_sgi_machine_sheets', 'menu_sgi_dropbox_procedures',
                    'menu_sgi_dropbox_routines', 'menu_sgi_dropbox_progress')
        reading = ('menu_sgi_dashboard_health', 'menu_sgi_mgmt_review', 'menu_sgi_satisfaction',
                   'menu_sgi_interested_parties', 'menu_sgi_documental', 'menu_sgi_documents',
                   'menu_sgi_master_list', 'menu_sgi_external_docs', 'menu_sgi_doc_changes',
                   'menu_sgi_config_acks', 'menu_sgi_admin_indicators', 'menu_sgi_indicators',
                   'menu_sgi_measures', 'menu_sgi_measures_split', 'menu_sgi_measure_reviews',
                   'menu_sgi_admin', 'menu_sgi_analysis', 'menu_sgi_diagnostic', 'menu_sgi_compliance',
                   'menu_sgi_activity_executions', 'menu_sgi_spec_gaps', 'menu_sgi_approvals_native',
                   'menu_sgi_nc_all_actions')
        mast_only = ('menu_sgi_config', 'menu_sgi_config_doc_types', 'menu_sgi_transition',
                     'menu_sgi_config_company_fix', 'menu_sgi_config_env_aspect_transfer',
                     'menu_sgi_my_procedure_publish', 'menu_sgi_indicator_wizard')
        self._assert_sees(self.plain, yes=everyone, no=catalogs + reading + mast_only)
        self._assert_sees(self.owner, yes=everyone + catalogs, no=reading + mast_only)
        for user in (self.director, self.auditor):
            self._assert_sees(user, yes=everyone + catalogs + reading, no=mast_only)
        self._assert_sees(self.mast, yes=everyone + catalogs + reading + mast_only,
                          no=('menu_sgi_config_load',))
        # Desempeño no tiene grupos propios: el personal la ve por Auditorías.
        self.assertFalse(_ref(self.env, 'menu_sgi_performance').group_ids)
        visible = self.env['ir.ui.menu'].with_user(self.plain)._visible_menu_ids()
        for xmlid in ('menu_sgi_performance', 'menu_sgi_audits', 'menu_sgi_processes', 'menu_sgi_direction'):
            self.assertIn(_ref(self.env, xmlid).id, visible, xmlid)
        for xmlid in ('menu_sgi_documental', 'menu_sgi_admin_indicators', 'menu_sgi_admin'):
            self.assertNotIn(_ref(self.env, xmlid).id, visible, xmlid)
        visible = self.env['ir.ui.menu'].with_user(self.director)._visible_menu_ids()
        for xmlid in ('menu_sgi_performance', 'menu_sgi_dashboard_health', 'menu_sgi_mgmt_review',
                      'menu_sgi_documental', 'menu_sgi_admin_indicators', 'menu_sgi_admin',
                      'menu_sgi_analysis', 'menu_sgi_approvals_native'):
            self.assertIn(_ref(self.env, xmlid).id, visible, xmlid)
        for xmlid in ('menu_sgi_config', 'menu_sgi_transition', 'menu_sgi_my_procedure_publish'):
            self.assertNotIn(_ref(self.env, xmlid).id, visible, xmlid)

    def test_06_diagnostico_absorbe(self):
        """Q5: lo que salió del menú se alcanza con un filtro, una agrupación
        o un botón (VERIFICAR 2: el botón de encabezado de la lista)."""
        compliance = _ref(self.env, 'sgi_activity_compliance_action')
        self.assertIn(compliance.domain or '[]', ('[]', False), "Sin dominio fijo.")
        self.assertIn('search_default_measurable', compliance.context or '')
        search = etree.fromstring(_ref(self.env, 'sgi_process_activity_view_search').arch)
        self.assertTrue(search.xpath("//filter[@name='measurable']"))
        self.assertTrue(search.xpath("//filter[@name='group_method']"))

        gap_search = etree.fromstring(_ref(self.env, 'sgi_activity_spec_gap_view_search').arch)
        filters = gap_search.xpath("//filter[@name='por_revisar']")
        self.assertEqual(len(filters), 1)
        mismatch = _ref(self.env, 'sgi_activity_menu_mismatch_action')
        self.assertEqual(ast.literal_eval(filters[0].get('domain')), ast.literal_eval(mismatch.domain))

        views = self.env['sgi.activity.execution'].get_views(
            [(_ref(self.env, 'sgi_activity_execution_view_list').id, 'list')])
        arch = etree.fromstring(views['views']['list']['arch'])
        week = str(_ref(self.env, 'sgi_activity_week_stat_action').id)
        buttons = arch.xpath("//header/button[@name='%s']" % week)
        self.assertEqual(len(buttons), 1, "Botón «Por semana» en el encabezado de la lista.")
        self.assertEqual(buttons[0].get('type'), 'action')

    def test_07_rutas_viejas_no_quedan(self):
        """Ningún texto que ve el usuario en vistas, reportes o datos dice una
        ruta del menú viejo (los comentarios no cuentan)."""
        parser = etree.XMLParser(remove_comments=True)
        found = []
        for folder in ('views', 'report', 'data'):
            base = os.path.join(_MODULE_DIR, folder)
            for name in sorted(os.listdir(base)):
                if not name.endswith('.xml'):
                    continue
                with file_open('quimibond_sgi/%s/%s' % (folder, name), 'rb') as handle:
                    tree = etree.parse(handle, parser)
                for node in tree.iter():
                    texts = [node.text or '', node.tail or ''] + list(node.attrib.values())
                    for text in texts:
                        for old in OLD_PATHS:
                            if old in text:
                                found.append("%s/%s: %s" % (folder, name, old))
        self.assertFalse(found, "Rutas del menú viejo:\n%s" % "\n".join(found))

    def test_08_reseal_mi_procedimiento(self):
        """Q8: si lo único que cambió es la ruta del menú, el documento
        vigente recibe la huella nueva (nadie vuelve a firmar); si cambió
        algo más, sigue desactualizado. Idempotente."""
        sgi_hide_real_documents(self.env)
        self.env['ir.config_parameter'].sudo().set_param('quimibond_sgi.mp_sign_required', 'False')
        Menu = self.env['ir.ui.menu'].sudo()
        admin = _ref(self.env, 'menu_sgi_admin')
        folder_a = Menu.create({'name': 'Z57113 Carpeta A', 'parent_id': admin.id})
        folder_b = Menu.create({'name': 'Z57113 Carpeta B', 'parent_id': admin.id})
        screen = Menu.create({
            'name': 'Z57113 Pantalla', 'parent_id': folder_a.id,
            'action': 'ir.actions.act_window,%d' % _ref(self.env, 'sgi_indicator_action').id})
        job = self.env['hr.job'].create({'name': 'Z57113 PUESTO'})
        self.env['hr.employee'].create({'name': 'Z57113 Persona', 'job_id': job.id})
        process = self.env['sgi.process'].create({'code': 'Z57113', 'name': 'Proceso Z57113'})
        activity = self.env['sgi.process.activity'].create({
            'process_id': process.id, 'name': 'Capturar Z57113', 'odoo_menu_id': screen.id,
            'measure_cadence': 'mensual',
            'role_ids': [(0, 0, {'role': 'ejecuta', 'job_id': job.id})]})
        job.with_user(self.mast).action_sgi_publish_my_procedure()
        doc = job._sgi_my_procedure_current_doc()
        self.assertTrue(doc.sgi_content_hash)
        old_path = screen.complete_name
        self.assertIn(old_path, str(job._sgi_my_procedure_data()['sections']))

        def stale():
            job._sgi_mp_mark_dirty()
            job.invalidate_recordset(['sgi_my_procedure_doc_id', 'sgi_my_procedure_stale'])
            return job.sgi_my_procedure_stale

        def notes():
            return self.env['mail.message'].search_count([
                ('model', '=', 'documents.document'), ('res_id', '=', doc.id),
                ('body', 'ilike', '57.113.0: la huella se actualizó')])

        screen.write({'parent_id': folder_b.id})
        self.assertNotEqual(screen.complete_name, old_path)
        self.assertTrue(stale(), "Mover el menú cambia la huella.")
        with self.assertRaises(AccessError):
            self.env['hr.job'].with_user(self.mast)._sgi_mp_reseal_menu_moves({screen: old_path}, jobs=job)
        result = self.env['hr.job']._sgi_mp_reseal_menu_moves({screen: old_path}, jobs=job)
        self.assertEqual(result['reselladas'], [doc.sgi_code])
        self.assertFalse(stale(), "Solo cambió la ruta: ya no está desactualizado.")
        self.assertEqual(notes(), 1)
        self.assertEqual(job._sgi_my_procedure_current_doc(), doc, "Sin revisión nueva.")
        # Segunda corrida: nada que escribir.
        again = self.env['hr.job']._sgi_mp_reseal_menu_moves({screen: old_path}, jobs=job)
        self.assertFalse(again['reselladas'])
        self.assertEqual(notes(), 1)

        # Cambió la ruta Y algo más: no se re-sella.
        path_b = screen.complete_name
        screen.write({'parent_id': folder_a.id})
        activity.write({'name': 'Capturar Z57113 con otro nombre'})
        hash_before = doc.sgi_content_hash
        result = self.env['hr.job']._sgi_mp_reseal_menu_moves({screen: path_b}, jobs=job)
        self.assertFalse(result['reselladas'])
        self.assertEqual(result['siguen_desactualizadas'], [doc.sgi_code])
        self.assertEqual(doc.sgi_content_hash, hash_before)
        self.assertTrue(stale())
        self.assertEqual(notes(), 1)

    def test_09_migracion_idempotente(self):
        """La migración corre dos veces sin duplicar el aviso al Jefe MAST
        ni tocar dos veces una sección del MIID. El re-sello se prueba en
        test_08; aquí se revisa que reciba los menús con su ruta vieja."""
        mast = sgi_set_mast(self.env, login='zc_mig_mast')
        miid = self.env['sgi.miid']._sgi_get()
        self.assertTrue(miid, "La empresa del SGI tiene su Manual.")
        path = os.path.join(_MODULE_DIR, 'migrations', '19.0.57.113.0', 'post-migrate.py')
        spec = importlib.util.spec_from_file_location('sgi_mig_57_111_0', path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        calls = []

        def fake_reseal(self_job, old_paths, tag="57.113.0", jobs=None):
            calls.append((old_paths, tag))
            return {'reselladas': [], 'sin_cambio': 0, 'siguen_desactualizadas': []}

        section = self.env.ref('quimibond_sgi.sgi_miid_section_35_auditoria')
        with patch.object(type(self.env['hr.job']), '_sgi_mp_reseal_menu_moves', fake_reseal):
            module.migrate(self.env.cr, '19.0.57.110.0')
            module.migrate(self.env.cr, '19.0.57.110.0')
        Activity = self.env['mail.activity'].sudo().with_context(active_test=False)
        notices = Activity.search([('sgi_cron_key', '=', 'menus_capitulos_57113')])
        self.assertEqual(len(notices), 1, "Un solo aviso al Jefe MAST.")
        self.assertEqual(notices.user_id, mast)
        self.assertEqual(len(calls), 2)
        old_paths, tag = calls[0]
        self.assertEqual(tag, "57.113.0")
        by_menu = {menu: p for menu, p in old_paths.items()}
        self.assertEqual(by_menu[_ref(self.env, 'menu_sgi_indicators')],
                         "SGI/Administración SGI/Indicadores/Indicadores")
        self.assertEqual(by_menu[_ref(self.env, 'menu_sgi_policy')], "SGI/Dirección/Política integral")
        self.assertNotIn(_ref(self.env, 'menu_sgi_nc'), by_menu, "No cambió de ruta.")
        for menu, old in by_menu.items():
            self.assertNotEqual(menu.with_context(lang='en_US').complete_name, old, old)
        # Sección sin editar: con la ruta nueva y un solo mensaje de la siembra.
        if not self.env['mail.message'].search_count([
                ('model', '=', 'sgi.miid.section'), ('res_id', '=', section.id),
                ('body', 'ilike', 'Texto de la sección editado')]):
            self.assertIn('SGI → Desempeño → Auditorías', section.body)
            self.assertLessEqual(self.env['mail.message'].search_count([
                ('model', '=', 'sgi.miid.section'), ('res_id', '=', section.id),
                ('body', 'ilike', '57.113.0: texto actualizado')]), 1)

    def test_10_rutas_que_se_le_dicen(self):
        self.assertEqual(sgi_menu_path('aprobaciones'), "SGI → Administración → Aprobaciones del SGI")
        self.assertEqual(sgi_menu_path('politica'), "SGI → Planeación → Política integral")
        self.assertEqual(sgi_menu_path('indicadores'), "SGI → Desempeño → Indicadores → Indicadores")
        self.assertEqual(sgi_menu_path('publicar_mi_procedimiento'),
                         "SGI → Administración → Publicar Mi procedimiento")

    def test_11_mensaje_de_la_siembra_con_genero_y_numero(self):
        """57.110.0 decía «nota de «Por confirmar» actualizado»."""
        Section = self.env['sgi.miid.section']
        self.assertEqual(Section._sgi_seed_message('ZT', {'body'}, 'X'), "ZT: texto actualizado con X.")
        self.assertEqual(Section._sgi_seed_message('ZT', {'to_confirm_note'}, 'X'),
                         "ZT: nota de «Por confirmar» actualizada con X.")
        self.assertEqual(Section._sgi_seed_message('ZT', {'body', 'to_confirm_note'}, 'X'),
                         "ZT: texto y nota de «Por confirmar» actualizados con X.")
        self.assertIn("revisión 02", Section._sgi_seed_message('ZT', {'body'}))
