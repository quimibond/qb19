# -*- coding: utf-8 -*-
"""57.98.0 — Interfaz (auditoría 2026-10: I-01, I-02, I-04, I-06, U-07; Q20).

Pie del formato controlado en cada hoja (un solo ``div.footer`` por artículo,
con clave, revisión, emisión y página), Documentos vigentes con clave, tipo y
proceso, un solo filtro «Míos», filtros por omisión, tabla de colores, menús
por rol, «Reportar», «Checklists de hoy» en Inicio y la app que abre en el
Tablero para Dirección. La base del build es copia de producción: nada cuenta
registros globales; los documentos de prueba esconden los reales."""
import re
from datetime import date

from lxml import etree

from odoo.tests import TransactionCase, tagged
from odoo.tests.common import new_test_user
from odoo.tools.safe_eval import safe_eval

from .common_documents import sgi_hide_real_documents

USER = 'base.group_user,quimibond_sgi.group_sgi_user'

# I-06: la misma tabla que el README («Colores»).
COLORS = {
    'muted': ('borrador', 'cancelado', 'cancelada', 'obsoleto'),
    'info': ('abierto', 'abierta', 'en_curso'),
    'warning': ('pendiente', 'por_vencer'),
    'danger': ('vencido', 'vencida', 'rechazado', 'rechazada'),
    'success': ('cerrado', 'cerrada', 'vigente', 'validado', 'validada'),
}
EXPECTED = {value: color for color, values in COLORS.items() for value in values}
# (modelo de la vista, campo de la expresión, valor, campo pintado o None =
# cualquiera): color permitido y por qué.
EXCEPTIONS = {
    # «Cerrada, por recibir»: paso intermedio hacia RH, no el cierre.
    ('sgi.staff.efficiency', 'state', 'cerrado', None): 'info',
    # Actividad sin método de medición: gris, no un pendiente de alguien.
    ('sgi.process.activity', 'measure_state', 'pendiente', None): 'muted',
    # Rutina del Dropbox pendiente: la fecha límite de la decisión sigue en
    # rojo (como el renglón «pendiente y sin decisión»); la pastilla, amarilla.
    ('sgi.legacy.routine', 'state', 'pendiente', 'decision_deadline'): 'danger',
}
SIMPLE = re.compile(r"^\s*(\w+)\s*(==|in)\s*(\(.*\)|\[.*\]|'[^']*')\s*$")
STATE_FIELD = re.compile(r'^(state|sgi_state|\w+_state|\w+_status)$')

# I-04
FORBIDDEN_MINE = ('Mías', 'Mis acciones', 'Mis NC', 'Mis bloqueos', 'Mis solicitudes',
                  'Mis indicadores', 'Mis mediciones', 'Mis actividades', 'Mis procesos',
                  'Los que me tocan')
DEFAULT_FILTERS = {
    'sgi_work_permit_action': ['open'],
    'sgi_process_activity_action': ['mine'],
    'sgi_fmea_action': ['vigente'],
    'sgi_ppap_action': ['en_proceso'],
    'sgi_objective_action': ['group_policy_id'],
    'sgi_env_aspect_action': ['significant'],
    'sgi_management_review_action': ['open'],
    'sgi_audit_program_action': ['this_year'],
    'sgi_current_document_action': ['mine', 'group_process'],
    'sgi_supplier_eval_action': ['group_class'],
}
CATALOG_MENUS = ('menu_sgi_deliverable_list', 'menu_sgi_process_flows', 'menu_sgi_analysis_who',
                 'menu_sgi_jobs_roles', 'menu_sgi_machine_sheets')


def _own_views(env, view_type=None):
    ids = env['ir.model.data'].sudo().search([
        ('module', '=', 'quimibond_sgi'), ('model', '=', 'ir.ui.view')]).mapped('res_id')
    views = env['ir.ui.view'].sudo().browse(ids).exists()
    return views.filtered(lambda v: v.type == view_type) if view_type else views


def _has_class(name):
    return "//div[contains(concat(' ', normalize-space(@class), ' '), ' %s ')]" % name


class _Case(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        sgi_hide_real_documents(cls.env)
        cls.Map = cls.env['sgi.format.map']
        cls.Doc = cls.env['documents.document']
        cls.plain = new_test_user(cls.env, login='zif_usuario', groups=USER)
        cls.owner = new_test_user(cls.env, login='zif_duenio',
                                  groups=USER + ',quimibond_sgi.group_sgi_process_owner')
        cls.director = new_test_user(cls.env, login='zif_direccion',
                                     groups=USER + ',quimibond_sgi.group_sgi_director')

    @classmethod
    def _doc(cls, code, revision=4, issued=date(2026, 9, 15), dtype='formato', **vals):
        base = {
            'name': '%s prueba interfaz.xlsx' % code, 'type': 'binary',
            'sgi_is_controlled': True, 'sgi_code': code,
            'sgi_revision': revision, 'sgi_state': 'vigente', 'sgi_issue_date': issued}
        if dtype:
            base['sgi_doc_type'] = dtype
        base.update(vals)
        return cls.Doc.create(base)


@tagged('post_install', '-at_install')
class TestPieFormato(_Case):
    """I-01."""

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.news_doc = cls._doc('F-P-G01-16')
        cls.env.ref('quimibond_sgi.format_ref_news').write(
            {'document_id': cls.news_doc.id, 'sgi_code': 'F-P-G01-16', 'active': True})

    def _review(self, **vals):
        return self.env['sgi.management.review'].create(dict(
            {'period_from': date(2061, 1, 1), 'period_to': date(2061, 6, 30)}, **vals))

    def _page_footer(self, values):
        return str(self.env['ir.qweb']._render('quimibond_sgi.sgi_format_page_footer', values))

    def test_01_info_por_referencia(self):
        info = self.Map.sgi_footer_info(self.env['sgi.management.review'], 'format_ref_news')
        self.assertEqual(info['label'], 'F-P-G01-16 · Rev. 04')
        self.assertEqual(info['issue_date'], date(2026, 9, 15))

    def test_02_info_por_registro_y_sin_mapeo(self):
        review = self._review()
        fmap = self.Map._sgi_map_for(review)
        doc = self._doc('F-P-A10-01', issued=False)
        if fmap:
            fmap.write({'document_id': doc.id, 'sgi_code': 'F-P-A10-01'})
        else:
            fmap = self.Map.create({
                'model_id': self.env['ir.model']._get('sgi.management.review').id,
                'document_id': doc.id, 'sgi_code': 'F-P-A10-01'})
        info = self.Map.sgi_footer_info(review)
        self.assertTrue(info['label'].startswith('F-P-A10-01'))
        self.assertFalse(info['issue_date'], "Sin fecha de emisión, el pie no la inventa.")
        self.assertFalse(self.Map.sgi_footer_info(self.env['sgi.management.review']))
        # El pie dentro de la hoja (reportes nativos, etiquetas) dice lo mismo.
        inline = str(self.env['ir.qweb']._render('quimibond_sgi.sgi_format_footer',
                                                 {'sgi_rec': review}))
        self.assertIn('F-P-A10-01', inline)
        self.assertNotIn('Emisión', inline)

    def test_03_pie_con_pagina_y_emision(self):
        review = self._review()
        html = self._page_footer({'sgi_rec': review, 'sgi_fmt_ref': 'format_ref_news'})
        self.assertIn('class="footer', html)
        for text in ('Formato controlado del SGI', 'F-P-G01-16', 'Emisión', 'Página',
                     'class="page"', 'class="topage"'):
            self.assertIn(text, html)
        self.assertRegex(html, r'(?s)Formato controlado del SGI:.*?F-P-G01-16.*?PNTQ')
        # Sin mapeo: un solo div.footer con la página, sin clave.
        bare = self._page_footer({'sgi_rec': False})
        self.assertIn('class="topage"', bare)
        self.assertNotIn('Formato controlado', bare)
        # El pie dentro de la hoja también trae la emisión.
        inline = str(self.env['ir.qweb']._render('quimibond_sgi.sgi_format_footer', {
            'sgi_rec': review, 'sgi_fmt_ref': 'format_ref_news'}))
        self.assertIn('Emisión', inline)
        self.assertNotIn('class="topage"', inline)

    def test_04_un_pie_por_articulo(self):
        """Dos revisiones en un mismo PDF: dos artículos, dos encabezados y
        dos pies, ninguno de ``web.external_layout`` (que desplazaría los
        índices con los que wkhtmltopdf elige el pie de cada hoja)."""
        reviews = self._review() | self._review(period_from=date(2061, 7, 1),
                                                 period_to=date(2061, 12, 31))
        html = self.env['ir.actions.report']._render_qweb_html(
            'quimibond_sgi.report_mgmt_review_document', reviews.ids)[0]
        root = etree.fromstring(html, etree.HTMLParser())
        self.assertEqual(len(root.xpath(_has_class('article'))), 2)
        self.assertEqual(len(root.xpath(_has_class('footer'))), 2)
        self.assertEqual(len(root.xpath(_has_class('header'))), 2)
        ids = {int(node.get('data-oe-id')) for node in root.xpath(_has_class('article'))}
        self.assertEqual(ids, set(reviews.ids))

    def test_05_ninguna_plantilla_mezcla_layouts(self):
        """Los reportes con pie controlado usan el layout propio; el pie de
        página (div.footer) solo lo llama ese layout; los nativos y las
        etiquetas conservan el pie dentro de la hoja."""
        from ..models.sgi_format_map import SGI_FORMAT_REPORTS
        View = self.env['ir.ui.view'].sudo()
        for report_xmlid, _fmt_ref in SGI_FORMAT_REPORTS:
            report = self.env.ref(report_xmlid)
            arch = View.search([('key', '=', report.report_name)], limit=1).arch_db or ''
            self.assertIn('quimibond_sgi.sgi_report_layout', arch, report_xmlid)
        for view in _own_views(self.env, 'qweb'):
            arch = view.arch_db or ''
            if 'quimibond_sgi.sgi_report_layout"' in arch:
                self.assertNotIn('web.external_layout', arch, view.key)
                self.assertNotIn('quimibond_sgi.sgi_format_footer"', arch,
                                 "%s: el pie saldría dos veces." % view.key)
            if 'quimibond_sgi.sgi_format_page_footer"' in arch:
                self.assertEqual(view.key, 'quimibond_sgi.sgi_report_layout')
        for xmlid in ('report_saleorder_document_sgi', 'report_purchaseorder_document_sgi',
                      'report_delivery_document_sgi', 'report_mrporder_sgi'):
            arch = self.env.ref('quimibond_sgi.' + xmlid).arch_db
            self.assertIn('quimibond_sgi.sgi_format_footer"', arch, xmlid)

    def test_06_diagnostico_lista_reportes_sin_mapeo(self):
        news = self.env.ref('quimibond_sgi.action_report_news').name
        self.assertNotIn(news, self.Map._sgi_unmapped_reports())
        self.env.ref('quimibond_sgi.format_ref_news').active = False
        self.assertIn(news, self.Map._sgi_unmapped_reports())
        lines = self.env['sgi.diagnostic']._sgi_unmapped_report_lines()
        self.assertEqual(len(lines), 1)
        self.assertEqual(lines[0]['level'], 'warn')
        self.assertIn('sin formato controlado', lines[0]['text'])
        self.assertIn(news, lines[0]['text'])
        self.assertIn('Formatos en documentos de Odoo', lines[0]['fix'])


@tagged('post_install', '-at_install')
class TestDocumentosVigentes(_Case):
    """I-02."""

    def test_07_lista_busqueda_y_sin_mi_procedimiento(self):
        action = self.env.ref('quimibond_sgi.sgi_current_document_action')
        domain = safe_eval(action.domain)
        process = self.env['sgi.process'].create({'code': 'Z97', 'name': 'Proceso interfaz Z97'})
        fmt = self._doc('F-Z97-01', sgi_previous_code='F-P-Z97-01', sgi_process_id=process.id)
        self.assertIn(fmt, self.Doc.search(domain + [('sgi_code', '=', 'F-Z97-01')]))
        mp_type = self.env['sgi.document.type'].search([('code', '=', 'mi_procedimiento')], limit=1)
        if mp_type:
            mp = self._doc('MP-997', dtype='mi_procedimiento')
            self.assertEqual(mp.sgi_doc_type_id, mp_type)
            self.assertNotIn(mp, self.Doc.search(domain + [('id', '=', mp.id)]),
                             "Mi procedimiento tiene su propia entrada en Inicio.")
        search = self.env.ref('quimibond_sgi.sgi_current_document_view_search')
        search_arch = etree.fromstring(search.arch_db)
        code_field = search_arch.xpath("//field[@name='sgi_code']")[0]
        by_old = self.Doc.search(domain + safe_eval(code_field.get('filter_domain'),
                                                    {'self': 'F-P-Z97-01'}))
        self.assertIn(fmt, by_old, "La clave anterior se busca aunque no se muestre.")
        names = {f.get('name') for f in search_arch.iter('filter')}
        self.assertTrue({'procedures', 'instructions', 'formats', 'mine', 'group_process',
                         'my_pending_acks'} <= names, names)
        formats = search_arch.xpath("//filter[@name='formats']")[0]
        self.assertIn(fmt, self.Doc.search(domain + safe_eval(formats.get('domain'))))
        arch = self.Doc.get_view(
            self.env.ref('quimibond_sgi.sgi_current_document_view_list').id, 'list')['arch']
        for field in ('sgi_code', 'sgi_title', 'sgi_doc_type_id', 'sgi_process_id'):
            self.assertIn('name="%s"' % field, arch)
        self.assertNotIn('sgi_previous_code', arch)


@tagged('post_install', '-at_install')
class TestFiltros(TransactionCase):
    """I-04."""

    def test_08_un_solo_mios(self):
        for view in _own_views(self.env, 'search'):
            for flt in etree.fromstring(view.arch_db).iter('filter'):
                name, label = flt.get('name'), flt.get('string')
                self.assertNotIn(label, FORBIDDEN_MINE, "%s: «%s» se llama «Míos»." % (view.key, label))
                if name == 'mine':
                    self.assertEqual(label, 'Míos', view.key)
                if label == 'De mis procesos':
                    self.assertEqual(name, 'my_processes', view.key)

    def _search_names(self, action):
        """Filtros y campos de la búsqueda que usa la acción (combinada, con
        las herencias de otros módulos)."""
        arch = self.env[action.res_model].get_view(
            action.search_view_id.id or None, 'search')['arch']
        root = etree.fromstring(arch)
        return ({f.get('name') for f in root.iter('filter')},
                {f.get('name') for f in root.iter('field')})

    def test_09_filtros_por_omision(self):
        for xmlid, names in DEFAULT_FILTERS.items():
            action = self.env.ref('quimibond_sgi.' + xmlid)
            context = safe_eval(action.context or '{}', {'uid': self.env.uid})
            filters, _fields = self._search_names(action)
            for name in names:
                self.assertEqual(context.get('search_default_' + name), 1, xmlid)
                self.assertIn(name, filters, "%s: no existe el filtro %s." % (xmlid, name))
        # Ninguna acción propia pide un filtro que su búsqueda no tiene
        # (search_default_<campo> también vale).
        ids = self.env['ir.model.data'].sudo().search([
            ('module', '=', 'quimibond_sgi'), ('model', '=', 'ir.actions.act_window')]).mapped('res_id')
        for action in self.env['ir.actions.act_window'].sudo().browse(ids).exists():
            wanted = re.findall(r"search_default_(\w+)", action.context or '')
            if not wanted or action.res_model not in self.env:
                continue
            filters, fields_ = self._search_names(action)
            for name in wanted:
                self.assertIn(name, filters | fields_, "%s pide search_default_%s." % (action.xml_id, name))


@tagged('post_install', '-at_install')
class TestColores(TransactionCase):
    """I-06: la misma palabra, el mismo color."""

    def test_10_tabla_de_colores(self):
        problems = []
        for view in _own_views(self.env):
            if view.type not in ('list', 'form', 'kanban'):
                continue
            for el in etree.fromstring(view.arch_db).iter():
                if not isinstance(el.tag, str):
                    continue
                for attr, expr in el.attrib.items():
                    if not attr.startswith('decoration-'):
                        continue
                    match = SIMPLE.match(expr)
                    if not match or not STATE_FIELD.match(match.group(1)):
                        continue
                    color = attr.split('-', 1)[1]
                    field = match.group(1)
                    for value in re.findall(r"'([a-z_]+)'", match.group(3)):
                        want = EXCEPTIONS.get(
                            (view.model, field, value, el.get('name')),
                            EXCEPTIONS.get((view.model, field, value, None), EXPECTED.get(value)))
                        if want and want != color:
                            problems.append('%s: %s %s = %s (debe ser %s)' % (
                                view.key or view.name, field, value, color, want))
        self.assertFalse(problems, "Colores fuera de la tabla del README:\n" + "\n".join(problems))


@tagged('post_install', '-at_install')
class TestMenusPorRol(_Case):
    """U-07 y Q20."""

    def test_11_reportar_abre_la_ficha_nueva(self):
        root = self.env.ref('quimibond_sgi.menu_sgi_root')
        folder = self.env.ref('quimibond_sgi.menu_sgi_report')
        self.assertEqual(folder.parent_id, root)
        self.assertLess(self.env.ref('quimibond_sgi.menu_sgi_panel').sequence, folder.sequence)
        self.assertLess(folder.sequence, self.env.ref('quimibond_sgi.menu_sgi_processes').sequence)
        Pending = self.env['sgi.my.pending'].with_user(self.plain)
        for kind, model in (('nc', 'quality.alert'), ('incident', 'sgi.incident'),
                            ('voice', 'helpdesk.ticket')):
            action = Pending.action_sgi_report(kind)
            self.assertEqual((action['res_model'], action['view_mode'], action['target']),
                             (model, 'form', 'current'), kind)
            self.assertFalse(action.get('res_id'), "%s: abre la ficha nueva." % kind)
            self.env[model].with_user(self.plain).check_access('create')
        context = Pending.action_sgi_report('nc')['context']
        self.assertEqual(context['default_team_id'], self.env.ref('quimibond_sgi.sgi_quality_team_internal').id)
        self.assertEqual(Pending.action_sgi_report('incident')['context']['default_incident_type'],
                         'casi_accidente')
        self.assertEqual(Pending.action_sgi_report('voice')['context']['default_team_id'],
                         self.env.ref('quimibond_sgi.sgi_helpdesk_team_voice').id)
        visible = self.env['ir.ui.menu'].with_user(self.plain)._visible_menu_ids()
        for xmlid in ('menu_sgi_report', 'menu_sgi_report_nc', 'menu_sgi_report_incident',
                      'menu_sgi_report_voice'):
            self.assertIn(self.env.ref('quimibond_sgi.' + xmlid).id, visible, xmlid)

    def test_12_catalogos_por_rol(self):
        plain = self.env['ir.ui.menu'].with_user(self.plain)._visible_menu_ids()
        owner = self.env['ir.ui.menu'].with_user(self.owner)._visible_menu_ids()
        for xmlid in CATALOG_MENUS:
            menu = self.env.ref('quimibond_sgi.' + xmlid)
            self.assertNotIn(menu.id, plain, xmlid)
            self.assertIn(menu.id, owner, xmlid)
        for xmlid in ('menu_sgi_process_map', 'menu_sgi_activities'):
            self.assertIn(self.env.ref('quimibond_sgi.' + xmlid).id, plain, xmlid)

    def test_13_checklists_de_hoy_en_inicio_y_en_mantenimiento(self):
        action = self.env.ref('quimibond_sgi.sgi_checklist_today_action')
        home = self.env.ref('quimibond_sgi.menu_sgi_home_checklist_today')
        self.assertEqual(home.parent_id, self.env.ref('quimibond_sgi.menu_sgi_panel'))
        self.assertEqual(home.action, action)
        self.assertEqual(self.env.ref('quimibond_sgi.menu_sgi_checklist_today').action, action)
        self.assertIn(home.id, self.env['ir.ui.menu'].with_user(self.plain)._visible_menu_ids())

    def test_14_direccion_arranca_en_el_tablero(self):
        root = self.env.ref('quimibond_sgi.menu_sgi_root')
        self.assertEqual(root.action, self.env.ref('quimibond_sgi.sgi_home_action'))
        self.assertEqual(
            self.env['sgi.my.pending'].with_user(self.director).action_open_home()['res_model'],
            'sgi.direction.board')
        self.assertEqual(
            self.env['sgi.my.pending'].with_user(self.plain).action_open_home()['res_model'],
            'sgi.my.pending')
