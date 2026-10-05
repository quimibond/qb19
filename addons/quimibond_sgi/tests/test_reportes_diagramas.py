# -*- coding: utf-8 -*-
"""57.101.0 — Reportes y diagramas.

A: la tendencia promedia (no suma) y agrupa por semana a los semanales; el
pivote de riesgos separa los instrumentos. B: nombre de archivo con folio,
8D / solicitud de desarrollo / responsiva / eficiencias con el pie
controlado en cada hoja, copia guardada del acta cerrada. C: ficha del
indicador con gráfica SVG y metas del periodo (K-04), diagramas, programa de
auditorías contra lo realizado y mapa de calor por instrumento.

La base del build es copia de producción: todo se crea aquí (claves ZR01-*,
periodos 2046, procesos ZR1/ZR2) y se renderiza HTML, nunca PDF."""
import time
from datetime import date

from lxml import etree
from markupsafe import Markup

from odoo.exceptions import UserError
from odoo.tests import TransactionCase, tagged
from odoo.tools.safe_eval import safe_eval

from .common_documents import sgi_hide_real_documents
from .common_users import sgi_set_mast


def _html(env, report_name, ids, data=None):
    return env['ir.actions.report']._render_qweb_html(report_name, ids, data=data)[0].decode()


class _Case(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        sgi_hide_real_documents(cls.env)
        cls.env = cls.env(context=dict(cls.env.context, sgi_skip_role_check=True))
        cls.mast = sgi_set_mast(cls.env, login='sgi_mast_zr01')
        Process = cls.env['sgi.process']
        cls.proc = Process.create({'code': 'ZR1', 'name': 'Proceso reportes'})
        cls.other = Process.create({'code': 'ZR2', 'name': 'Otro reportes'})

    def _ind(self, code, frequency='monthly', direction='higher_better', obj=90.0, acc=80.0):
        return self.env['sgi.indicator'].create({
            'code': code, 'name': 'Indicador %s' % code, 'calc_mode': 'manual',
            'frequency': frequency, 'direction': direction, 'target_objective': obj,
            'target_acceptable': acc, 'process_id': self.proc.id,
            'responsible_id': self.mast.id})

    def _measure(self, ind, period, value, state='capturado', **vals):
        return self.env['sgi.indicator.measure'].create(dict(
            indicator_id=ind.id, period_date=period, value=value, state=state, **vals))


@tagged('post_install', '-at_install')
class TestCorrecciones(_Case):
    """A1 y A2."""

    def test_01_valor_promedia(self):
        self.assertEqual(self.env['sgi.indicator.measure']._fields['value'].aggregator, 'avg',
                         "Agrupar por mes promedia las mediciones (no las suma).")

    def test_02_tendencia_semanal_por_semana_y_con_dato(self):
        weekly = self._ind('ZR01-W', frequency='weekly')
        action = weekly.action_view_trend()
        graph = self.env.ref('quimibond_sgi.sgi_measure_view_graph_weekly')
        self.assertIn((graph.id, 'graph'), [tuple(v) for v in action['views']])
        self.assertIn('interval="week"', graph.arch_db)
        self.assertEqual(action['context'].get('search_default_con_dato'), 1)
        monthly = self._ind('ZR01-M').action_view_trend()
        self.assertEqual(monthly['context'].get('search_default_con_dato'), 1)
        self.assertIn('graph', monthly['view_mode'])
        search = self.env.ref('quimibond_sgi.sgi_measure_view_search').arch_db
        self.assertIn('name="con_dato"', search)

    def test_03_pivote_de_riesgos_por_instrumento(self):
        arch = etree.fromstring(self.env.ref('quimibond_sgi.sgi_risk_view_pivot').arch_db)
        rows = [f.get('name') for f in arch.iter('field') if f.get('type') == 'row']
        self.assertEqual(rows[:2], ['instrument', 'process_id'])


@tagged('post_install', '-at_install')
class TestImpresos(_Case):
    """B1, B2 y B3."""

    def test_04_nombre_de_archivo_con_folio(self):
        for xmlid in ('action_report_nc', 'action_report_8d', 'action_report_audit_plan',
                      'action_report_audit_report', 'action_report_mgmt_review',
                      'action_report_incident', 'action_report_fmea'):
            report = self.env.ref('quimibond_sgi.' + xmlid)
            self.assertIn('folio', report.print_report_name or '', xmlid)
        review = self.env['sgi.management.review'].create({
            'period_from': date(2046, 1, 1), 'period_to': date(2046, 6, 30)})
        name = safe_eval(self.env.ref('quimibond_sgi.action_report_mgmt_review').print_report_name,
                         {'object': review, 'time': time})
        self.assertIn(review.folio, name)

    def test_05_cuatro_reportes_al_layout_controlado(self):
        View = self.env['ir.ui.view'].sudo()
        for name in ('report_8d_document', 'report_dev_request_document',
                     'report_epp_delivery_document', 'report_staff_efficiency_document',
                     'report_machine_sheet_document'):
            arch = View.search([('key', '=', 'quimibond_sgi.' + name)], limit=1).arch_db
            self.assertIn('quimibond_sgi.sgi_report_layout', arch, name)
            self.assertNotIn('web.external_layout', arch, name)
            self.assertNotIn('Formato controlado del SGI', arch,
                             "%s: el pie va en el layout, no escrito en la hoja." % name)

    def test_06_8d_sin_la_clave_de_la_nc(self):
        team = self.env.ref('quimibond_sgi.sgi_quality_team_internal')
        alert = self.env['quality.alert'].create({'title': 'ZR01 alerta 8D', 'team_id': team.id})
        html = _html(self.env, 'quimibond_sgi.report_8d_document', alert.ids)
        self.assertIn('class="topage"', html)
        self.assertNotIn('F-P-G05-01', html, "El 8D no imprime la clave del reporte de NC (57.44.0).")
        unmapped = self.env['sgi.format.map']._sgi_unmapped_reports()
        self.assertTrue(any(n.startswith('Reporte 8D') and 'sin clave del SGI' in n for n in unmapped))

    def test_07_copia_del_acta_solo_cerrada_y_se_renombra_al_reabrir(self):
        report = self.env.ref('quimibond_sgi.action_report_mgmt_review')
        self.assertTrue(report.attachment_use)
        review = self.env['sgi.management.review'].create({
            'period_from': date(2046, 1, 1), 'period_to': date(2046, 6, 30)})
        ctx = {'object': review, 'time': time}
        self.assertFalse(safe_eval(report.attachment, ctx), "En borrador no se guarda copia.")
        review.sudo().write({'state': 'cerrada'})
        name = safe_eval(report.attachment, ctx)
        self.assertEqual(name, review.sgi_minutes_filename())
        download = safe_eval(report.print_report_name, ctx)
        self.assertEqual(name, download + '.pdf', "La copia y la descarga se llaman igual.")
        stored = self.env['ir.attachment'].create({
            'name': name, 'res_model': review._name, 'res_id': review.id, 'raw': b'%PDF-1.4'})
        review.with_user(self.mast).action_draft()
        self.assertTrue(stored.exists(), "Reabrir no borra la copia.")
        self.assertNotEqual(stored.name, name)
        self.assertIn('reabierta', stored.name)

    def test_08_informe_de_auditoria_sin_copia_duplicada(self):
        """AU-3 ya archiva el informe al cerrar (Documentos y chatter)."""
        self.assertFalse(self.env.ref('quimibond_sgi.action_report_audit_report').attachment)


@tagged('post_install', '-at_install')
class TestFichaIndicador(_Case):
    """C1."""

    def test_09_svg_puro_escapa_y_dibuja_franjas(self):
        from ..models.sgi_indicator_sheet import sgi_trend_svg, sgi_zones
        zones = sgi_zones('higher_better', 90.0, 80.0)
        points = [{'label': '<b>01/46</b>', 'value': 85.0, 'semaphore': 'amarillo', 'zones': zones},
                  {'label': '02/46', 'value': None, 'semaphore': False, 'zones': zones},
                  {'label': '03/46', 'value': 95.0, 'semaphore': 'verde', 'zones': zones}]
        svg = sgi_trend_svg(points, uom='%')
        self.assertIsInstance(svg, Markup)
        self.assertIn('<svg', svg)
        self.assertIn('&lt;b&gt;01/46', svg)
        self.assertNotIn('<b>01/46', svg)
        self.assertIn('s/d', svg)
        self.assertGreaterEqual(svg.count('<rect'), 3)
        # «Más bajo es mejor»: verde por debajo del objetivo.
        low = dict((c, (a, b)) for a, b, c in sgi_zones('lower_better', 10.0, 20.0))
        self.assertEqual(low['verde'][1], 10.0)
        self.assertEqual(sgi_trend_svg([]), Markup(''))

    def test_10_metas_del_periodo_respetan_las_guardadas(self):
        ind = self._ind('ZR01-K')
        validated = self._measure(ind, date(2046, 1, 1), 85.0)
        validated.with_user(self.mast).action_validate()
        self._measure(ind, date(2046, 2, 1), 60.0, cause='Paro de línea ZR01')
        self._measure(ind, date(2046, 3, 1), 0.0, state='sin_dato')
        ind.write({'target_objective': 80.0, 'target_acceptable': 70.0})
        rows = ind._sgi_sheet_rows()
        self.assertEqual([r['label'] for r in rows], ['01/2046', '02/2046', '03/2046'])
        self.assertEqual((rows[0]['objective'], rows[0]['acceptable'], rows[0]['frozen']),
                         (90.0, 80.0, True), "La validada se juzga con sus metas guardadas.")
        self.assertEqual(rows[1]['objective'], 80.0)
        self.assertEqual(rows[1]['semaphore'], 'rojo', "60 contra un aceptable de 70.")
        self.assertIsNone(rows[2]['value'], "Sin dato no es cero.")
        html = _html(self.env, 'quimibond_sgi.report_indicator_sheet_document', ind.ids)
        self.assertIn('<svg', html)
        self.assertIn('Paro de línea ZR01', html)
        self.assertIn('class="topage"', html)

    def test_11_semanal_y_ficha_por_proceso(self):
        weekly = self._ind('ZR01-S', frequency='weekly', direction='lower_better', obj=5.0, acc=8.0)
        self._measure(weekly, date(2046, 1, 1), 4.0)   # lunes
        self._measure(weekly, date(2046, 1, 8), 9.0)
        rows = weekly._sgi_sheet_rows()
        self.assertTrue(rows[0]['label'].startswith('Sem '))
        self.assertEqual([r['semaphore'] for r in rows], ['verde', 'rojo'])
        self._ind('ZR01-P')
        html = _html(self.env, 'quimibond_sgi.report_indicator_sheet_process_document', self.proc.ids)
        root = etree.fromstring(html, etree.HTMLParser())
        articles = root.xpath("//div[contains(concat(' ', normalize-space(@class), ' '), ' article ')]")
        self.assertEqual(len(articles), len(self.proc._sgi_sheet_indicators()))
        empty = _html(self.env, 'quimibond_sgi.report_indicator_sheet_process_document', self.other.ids)
        self.assertIn('Sin indicadores activos', empty)

    def test_12_boton_ficha(self):
        action = self._ind('ZR01-B').action_print_sheet()
        self.assertEqual(action['type'], 'ir.actions.report')
        self.assertEqual(action['report_name'], 'quimibond_sgi.report_indicator_sheet_document')


@tagged('post_install', '-at_install')
class TestDiagramasYMatrices(_Case):
    """C3, C5 y C7."""

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.env['sgi.process.flow'].create({
            'from_process_id': cls.proc.id, 'to_process_id': cls.other.id, 'name': 'Lote ZR'})

    def test_13_diagramas_imprimibles(self):
        Diagram = self.env['sgi.diagram']
        self.assertTrue(Diagram.data('interaction_matrix')['print_report'])
        self.assertFalse(Diagram.data('pdca', self.proc.id)['print_report'])
        self.assertTrue(Diagram.data('risk_matrix', None, {'instrument': 'iper'})['print_report'])
        self.assertFalse(Diagram.data('risk_matrix', None, {'instrument': 'patrimonial'})['print_report'],
                         "El mapa de calor no incluye el instrumento patrimonial (Q10).")
        for kind in ('process_map', 'interaction_matrix', 'roles_map', 'context_map'):
            html = _html(self.env, 'quimibond_sgi.report_sgi_diagram_document', [],
                         data={'kind': kind, 'res_id': False, 'params': {}})
            self.assertIn('class="topage"', html, kind)
        sipoc = _html(self.env, 'quimibond_sgi.report_sgi_diagram_document', [],
                      data={'kind': 'sipoc', 'res_id': self.proc.id, 'params': {}})
        self.assertIn('Lote ZR', sipoc)
        self.assertNotIn('Pase el mouse', sipoc)
        process_map = _html(self.env, 'quimibond_sgi.report_sgi_diagram_document', [],
                            data={'kind': 'process_map', 'res_id': False, 'params': {}})
        self.assertNotIn('Pase el mouse', process_map)
        action = Diagram.action_print_controlled('interaction_matrix', False, {})
        self.assertEqual(action['data']['kind'], 'interaction_matrix')
        with self.assertRaises(UserError):
            Diagram.action_print_controlled('process_flow', self.proc.id, {})
        risk = Diagram.action_print_controlled('risk_matrix', self.proc.id, {'instrument': 'iper'})
        self.assertEqual(risk['report_name'], 'quimibond_sgi.report_risk_heatmap_document')

    def test_14_mapa_de_calor_por_instrumento(self):
        Risk = self.env['sgi.risk']
        ryo = Risk.create({'name': 'ZR riesgo RyO', 'instrument': 'ryo', 'process_id': self.proc.id,
                           'eval_probability': '5', 'eval_impact': '4',
                           'residual_probability': '2', 'residual_impact': '2'})
        iper = Risk.create({'name': 'ZR peligro', 'instrument': 'iper', 'process_id': self.proc.id,
                            'eval_probability': '3', 'eval_impact': '3'})
        sheets = Risk._sgi_heatmap_sheets(ryo | iper)
        self.assertEqual([s['instrument'] for s in sheets], ['ryo', 'iper'])
        iper_grid = sheets[1]['initial']
        self.assertEqual(len(iper_grid), 3, "IPER va de 1 a 3.")
        top = iper_grid[0]['cells'][2]
        self.assertEqual((top['score'], top['color']), (9, 'rojo'))
        self.assertIn(iper.folio, top['folios'])
        html = _html(self.env, 'quimibond_sgi.report_risk_heatmap_document', (ryo | iper).ids)
        self.assertIn(ryo.folio, html)
        self.assertIn('Residual', html)
        by_data = _html(self.env, 'quimibond_sgi.report_risk_heatmap_document', [],
                        data={'instrument': 'iper', 'process_id': self.proc.id})
        self.assertIn(iper.folio, by_data)
        self.assertNotIn(ryo.folio, by_data)

    def test_15_programa_contra_realizado(self):
        program = self.env['sgi.audit.program'].create({'year': 2046})
        line = self.env['sgi.audit.program.line'].create({
            'program_id': program.id, 'process_id': self.proc.id, 'planned_month': '3',
            'audit_type': 'interna', 'lead_auditor_id': self.mast.id})
        self.env['sgi.audit.program.line'].create({
            'program_id': program.id, 'process_id': self.other.id, 'planned_month': '5',
            'audit_type': 'interna'})
        line.action_create_audit()
        audit = line.audit_id
        audit.sudo().write({'state': 'informe', 'date_start': date(2046, 4, 2),
                            'date_end': date(2046, 4, 3)})
        self.env['sgi.audit.finding'].sudo().create({
            'audit_id': audit.id, 'finding_type': 'observacion', 'description': 'ZR obs'})
        grid = program._sgi_program_grid()
        row = next(r for r in grid['rows'] if r['label'].startswith('ZR1'))
        self.assertTrue(row['months'][3]['planned'])
        self.assertEqual(row['months'][3]['status'], 'movida')
        self.assertEqual(row['months'][4]['status'], 'ejecutada')
        self.assertEqual(grid['findings'][0]['counts']['observacion'], 1)
        html = _html(self.env, 'quimibond_sgi.report_audit_program_document', program.ids)
        self.assertIn(audit.folio, html)
        self.assertIn('class="topage"', html)
        action = program.action_print_execution()
        self.assertEqual(action['report_name'], 'quimibond_sgi.report_audit_program_document')


@tagged('post_install', '-at_install')
class TestFormatoControlado(_Case):

    def test_16_reportes_nuevos_en_la_tabla(self):
        from ..models.sgi_format_map import SGI_FORMAT_REPORTS
        listed = {xmlid for xmlid, _ref in SGI_FORMAT_REPORTS}
        for xmlid in ('action_report_indicator_sheet', 'action_report_indicator_sheet_process',
                      'action_report_sgi_diagram', 'action_report_audit_program',
                      'action_report_risk_heatmap', 'action_report_8d', 'action_report_dev_request',
                      'action_report_epp_delivery', 'action_report_staff_efficiency'):
            self.assertIn('quimibond_sgi.' + xmlid, listed)
