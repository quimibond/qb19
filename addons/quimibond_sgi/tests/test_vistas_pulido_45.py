# -*- coding: utf-8 -*-
"""57.80.0+ (pulido de vistas, bloques 4 y 5 de la revisión del 2026-09-30).

Bloque 4: listas, análisis y vistas faltantes (pastillas y barras, Paretos
ordenados, programa y auditorías, chatter en mediciones y evaluaciones de
proveedor, calendario, kanban, gráfica y panel lateral, multi_edit,
búsqueda de lecciones y vistas de actividades).

Bloque 5: textos y pulido (títulos iguales al menú, sin emojis ni sufijo
«SGI», glosario, «Descartar», fichas de catálogo con título)."""
from datetime import date

from lxml import etree

from odoo.tests import TransactionCase, tagged


def _arch(env, model, view_type, view_ref=None):
    view_id = env.ref(view_ref).id if view_ref else False
    views = env[model].get_views([(view_id, view_type)])
    return etree.fromstring(views['views'][view_type]['arch'])


@tagged('post_install', '-at_install')
class TestListasYAnalisis(TransactionCase):
    """57.80.0 (V-M05, V-M13, V-M14, V-M16, V-B11, V-B12, V-B13, V-B15, V-B18)."""

    def _flush_tracking(self):
        self.env.flush_all()
        self.env.cr.precommit.run()

    def test_01_estados_como_pastilla(self):
        """V-M05: el estado de las listas del SGI es una pastilla."""
        for model, ref in (('sgi.audit', 'sgi_audit_view_list'),
                           ('sgi.incident', 'sgi_incident_view_list'),
                           ('sgi.risk', 'sgi_risk_view_list'),
                           ('sgi.fmea', 'sgi_fmea_view_list'),
                           ('sgi.ppap', 'sgi_ppap_view_list'),
                           ('sgi.control.plan', 'sgi_control_plan_view_list'),
                           ('sgi.policy', 'sgi_policy_view_list'),
                           ('sgi.emergency.drill', 'sgi_emergency_drill_view_list'),
                           ('sgi.indicator.measure', 'sgi_measure_view_list'),
                           ('sgi.document.ack', 'sgi_document_ack_view_list')):
            arch = _arch(self.env, model, 'list', 'quimibond_sgi.' + ref)
            self.assertTrue(arch.xpath("//list/field[@name='state'][@widget='badge']"), ref)
        actions = _arch(self.env, 'sgi.action.line', 'list', 'quimibond_sgi.sgi_action_line_view_list')
        self.assertTrue(actions.xpath("//field[@name='progress'][@widget='badge']"))
        evals = _arch(self.env, 'sgi.supplier.eval', 'list', 'quimibond_sgi.sgi_supplier_eval_view_list')
        self.assertTrue(evals.xpath("//field[@name='score'][@widget='progressbar']"))

    def test_02_pareto_de_mayor_a_menor(self):
        """V-M13: el Pareto de alertas sale ordenado de mayor a menor."""
        graph = _arch(self.env, 'quality.alert', 'graph', 'quimibond_sgi.sgi_quality_alert_view_graph')
        self.assertEqual(graph.get('order'), 'desc')
        pivot = _arch(self.env, 'quality.alert', 'pivot', 'quimibond_sgi.sgi_quality_alert_view_pivot')
        self.assertEqual(pivot.get('default_order'), '__count desc')

    def test_03_programa_con_avance_y_lista_de_auditorias(self):
        """V-M14: el programa dice cuántas auditorías lleva y cuántas se hicieron."""
        program = self.env['sgi.audit.program'].create({'year': 2093})
        self.assertEqual((program.line_count, program.line_done_count, program.progress_pct), (0, 0, 0.0))
        lines = self.env['sgi.audit.program.line'].create([
            {'program_id': program.id, 'planned_month': month} for month in ('1', '4', '7', '10')])
        lines[0].state = 'cerrada'
        self.assertEqual((program.line_count, program.line_done_count, program.progress_pct), (4, 1, 25.0))
        arch = _arch(self.env, 'sgi.audit.program', 'list', 'quimibond_sgi.sgi_audit_program_view_list')
        self.assertTrue(arch.xpath("//field[@name='progress_pct'][@widget='progressbar']"))
        arch = _arch(self.env, 'sgi.audit', 'list', 'quimibond_sgi.sgi_audit_view_list')
        for name in ('process_ids', 'partner_id', 'date_end'):
            self.assertTrue(arch.xpath("//field[@name='%s']" % name), name)

    def test_04_medicion_con_historial(self):
        """V-M16: quién capturó, corrigió o validó una medición queda en el chatter."""
        Measure = self.env['sgi.indicator.measure']
        self.assertIn('message_ids', Measure._fields, "La medición lleva mail.thread.")
        for name in ('value', 'state', 'note'):
            self.assertTrue(Measure._fields[name].tracking, name)
        arch = _arch(self.env, 'sgi.indicator.measure', 'form', 'quimibond_sgi.sgi_measure_view_form')
        self.assertTrue(arch.xpath('//chatter'))
        indicator = self.env['sgi.indicator'].create({
            'code': 'ZVP45', 'name': 'KPI historial', 'calc_mode': 'manual',
            'responsible_id': self.env.user.id})
        measure = Measure.create({'indicator_id': indicator.id, 'period_date': date(2032, 1, 1)})
        self._flush_tracking()
        measure.write({'value': 7.5})
        measure.action_capture()
        self._flush_tracking()
        measure.invalidate_recordset(['message_ids'])
        tracked = measure.message_ids.tracking_value_ids.field_id.mapped('name')
        self.assertIn('value', tracked)
        self.assertIn('state', tracked)

    def test_05_evaluacion_de_proveedor_con_historial(self):
        """V-M16: la evaluación de proveedores también deja rastro."""
        Eval = self.env['sgi.supplier.eval']
        self.assertIn('message_ids', Eval._fields, "La evaluación lleva mail.thread.")
        for name in ('score', 'supplier_class', 'notes'):
            self.assertTrue(Eval._fields[name].tracking, name)
        arch = _arch(self.env, 'sgi.supplier.eval', 'form', 'quimibond_sgi.sgi_supplier_eval_view_form')
        self.assertTrue(arch.xpath('//chatter'))

    def test_06_mediciones_de_12_en_12(self):
        """V-B11: la ficha del indicador muestra las mediciones de 12 en 12."""
        arch = _arch(self.env, 'sgi.indicator', 'form', 'quimibond_sgi.sgi_indicator_view_form')
        lists = arch.xpath("//field[@name='measure_ids']/list")
        self.assertEqual(lists[0].get('limit'), '12')

    def test_07_vistas_nuevas_en_su_accion(self):
        """V-B12: calendario, kanban, gráfica y panel lateral donde ayudan."""
        expected = {
            'sgi_audit_action': ('calendar', 'sgi_audit_view_calendar'),
            'sgi_emergency_drill_action': ('calendar', 'sgi_emergency_drill_view_calendar'),
            'sgi_calibration_action': ('calendar', 'sgi_calibration_view_calendar'),
            'sgi_action_line_action_all': ('kanban', 'sgi_action_line_view_kanban'),
            'sgi_incident_action': ('kanban', 'sgi_incident_view_kanban'),
        }
        for xmlid, (view_type, view_ref) in expected.items():
            action = self.env.ref('quimibond_sgi.' + xmlid)
            self.assertIn(view_type, action.view_mode.split(','), xmlid)
            _arch(self.env, action.res_model, view_type, 'quimibond_sgi.' + view_ref)
        incident = self.env.ref('quimibond_sgi.sgi_incident_action')
        self.assertIn('graph', incident.view_mode.split(','))
        graph = _arch(self.env, 'sgi.incident', 'graph', 'quimibond_sgi.sgi_incident_view_graph')
        self.assertEqual(graph.xpath("//field[@name='date']")[0].get('interval'), 'month')
        search = _arch(self.env, 'documents.document', 'search', 'quimibond_sgi.sgi_document_view_search')
        panel = {f.get('name') for f in search.xpath('//searchpanel/field')}
        self.assertEqual(panel, {'sgi_doc_type_id', 'sgi_process_id'})

    def test_08_multi_edit(self):
        """V-B13: reasignar o mover fechas de varios a la vez."""
        for model, ref in (('sgi.action.line', 'sgi_action_line_view_list'),
                           ('sgi.risk', 'sgi_risk_view_list'),
                           ('documents.document', 'sgi_document_view_list'),
                           ('sgi.legal.requirement', 'sgi_legal_requirement_view_list')):
            arch = _arch(self.env, model, 'list', 'quimibond_sgi.' + ref)
            self.assertEqual(arch.get('multi_edit'), '1', ref)

    def test_09_lecciones_con_busqueda_de_nc(self):
        """V-B15: las lecciones usan la búsqueda de NC; los hallazgos tienen la suya."""
        lessons = self.env.ref('quimibond_sgi.sgi_nc_lessons_action')
        self.assertEqual(lessons.search_view_id, self.env.ref('quimibond_sgi.sgi_nc_view_search'))
        findings = self.env.ref('quimibond_sgi.sgi_audit_finding_list_action')
        self.assertEqual(findings.search_view_id, self.env.ref('quimibond_sgi.sgi_audit_finding_view_search'))

    def test_10_vistas_de_actividades(self):
        """V-B18: los modelos con actividades las muestran en su acción."""
        for xmlid, view_ref in (
                ('sgi_calibration_action', 'sgi_calibration_view_activity'),
                ('sgi_legal_requirement_action', 'sgi_legal_requirement_view_activity'),
                ('sgi_health_record_action', 'sgi_health_record_view_activity'),
                ('sgi_csh_inspection_action', 'sgi_csh_inspection_view_activity'),
                ('sgi_objective_action', 'sgi_objective_view_activity'),
                ('sgi_machine_sheet_action', 'sgi_machine_sheet_view_activity')):
            action = self.env.ref('quimibond_sgi.' + xmlid)
            self.assertIn('activity', action.view_mode.split(','), xmlid)
            _arch(self.env, action.res_model, 'activity', 'quimibond_sgi.' + view_ref)


@tagged('post_install', '-at_install')
class TestTextosYPulido(TransactionCase):
    """57.81.0 (V-M12, V-B02, V-B03, V-B04, V-B05, V-B07, V-B08, V-B09, V-B10,
    V-B14, V-B17)."""

    # La misma acción abre «Eficiencias de mi área» (Inicio) y «Hojas
    # mensuales» (Empleados): no puede llamarse como los dos menús.
    SHARED_ACTIONS = ('menu_sgi_my_staff_efficiency',)

    def _sgi_views(self):
        return self.env['ir.ui.view'].search([
            ('id', 'in', self.env['ir.model.data'].search([
                ('module', '=', 'quimibond_sgi'), ('model', '=', 'ir.ui.view')]).mapped('res_id'))])

    def test_01_acciones_con_el_nombre_del_menu(self):
        """V-M12: la acción (migas) se llama como el menú por el que se entra."""
        root = self.env.ref('quimibond_sgi.menu_sgi_root')
        menus = self.env['ir.ui.menu'].with_context(**{'ir.ui.menu.full_list': True}).search(
            [('id', 'child_of', root.id)])
        checked = 0
        for menu in menus:
            if not menu.action or menu.action._name != 'ir.actions.act_window':
                continue
            xmlid = menu.get_external_id().get(menu.id, '')
            if xmlid.split('.')[-1] in self.SHARED_ACTIONS:
                continue
            self.assertEqual(menu.action.name, menu.name, xmlid)
            checked += 1
        self.assertGreater(checked, 30)
        self.assertEqual(self.env.ref('quimibond_sgi.menu_sgi_audit_list').name, 'Auditorías')
        for model, view_type in (('sgi.incident', 'list'), ('sgi.incident', 'search')):
            self.assertEqual(_arch(self.env, model, view_type).get('string'), 'Incidentes y accidentes')

    def test_02_sin_emojis_ni_sufijo_sgi(self):
        """V-B02 y V-B03: íconos fa en lugar de emojis; sin «(SGI)» en las etiquetas."""
        for view in self._sgi_views():
            arch = view.arch_db or ''
            for emoji in ('\U0001F4CB', '\U0001F389'):
                self.assertNotIn(emoji, arch, view.xml_id)
            tree = etree.fromstring(arch.encode())
            for node in tree.xpath('//page[@string] | //group[@string] | //button[@string]'):
                self.assertNotIn('(SGI)', node.get('string'), view.xml_id)
        partner = etree.fromstring(
            self.env.ref('quimibond_sgi.sgi_res_partner_supplier_view_form').arch_db.encode())
        self.assertTrue(partner.xpath("//page[@name='sgi_supplier'][@string='SGI']"))

    def test_03_glosario(self):
        """V-B04: no conformidad, CoA, indicador, casi accidente, Jefe MAST."""
        bad = ('No Conformidad', 'NCs', 'KPI', 'casi-accidente', 'Jefe de MAST', 'COA ')
        for view in self._sgi_views():
            tree = etree.fromstring((view.arch_db or '').encode())
            for node in tree.xpath('//*[@string] | //*[@help] | //*[@placeholder] | //*[@confirm]'):
                for attr in ('string', 'help', 'placeholder', 'confirm'):
                    for word in bad:
                        self.assertNotIn(word, node.get(attr) or '', "%s: %s" % (view.xml_id, attr))

    def test_04_estado_con_etiqueta(self):
        """V-B05: el estado de la hoja de eficiencias dice «Estado»."""
        self.assertEqual(self.env['sgi.staff.efficiency']._fields['state'].string, 'Estado')

    def test_05_ayudas_y_descartar(self):
        """V-B07 y V-B08: ayuda en «Puestos y procesos»; «Descartar» en los asistentes de NC."""
        self.assertIn('Empleados', self.env.ref('quimibond_sgi.sgi_hr_job_action_roles').help)
        for model, ref in (('sgi.nc.cancel', 'sgi_nc_cancel_view_form'),
                           ('sgi.nc.force.close', 'sgi_nc_force_close_view_form')):
            arch = _arch(self.env, model, 'form', 'quimibond_sgi.' + ref)
            labels = [b.get('string') for b in arch.xpath("//footer/button[@special='cancel']")]
            self.assertEqual(labels, ['Descartar'], ref)

    def test_06_botones_inteligentes(self):
        """V-B09: difusión solo con statinfo; en el proceso, primero las alertas."""
        doc = _arch(self.env, 'documents.document', 'form', 'quimibond_sgi.sgi_document_view_form')
        self.assertFalse(doc.xpath("//button[@name='action_open_acks']//div[contains(@class, 'o_stat_info')]"))
        process = _arch(self.env, 'sgi.process', 'form', 'quimibond_sgi.sgi_process_view_form')
        names = [b.get('name') for b in process.xpath("//div[@name='button_box']/button")]
        self.assertLess(names.index('action_open_overdue_actions'), names.index('action_view_activities'))
        self.assertLess(names.index('action_open_red_kpis'), names.index('action_open_indicators'))

    def test_07_fichas_de_catalogo_con_titulo(self):
        """V-B10: el nombre como título; chatter en la plantilla de checklist."""
        for model, ref, field in (('sgi.area', 'sgi_area_view_form', 'name'),
                                  ('sgi.job.family', 'sgi_job_family_view_form', 'name'),
                                  ('sgi.norm', 'sgi_norm_view_form', 'name'),
                                  ('sgi.norm.clause', 'sgi_norm_clause_view_form', 'name'),
                                  ('sgi.health.record', 'sgi_health_record_view_form', 'employee_id'),
                                  ('sgi.checklist.template', 'sgi_checklist_template_view_form', 'name')):
            arch = _arch(self.env, model, 'form', 'quimibond_sgi.' + ref)
            self.assertTrue(arch.xpath("//div[contains(@class, 'oe_title')]/h1/field[@name='%s']" % field), ref)
        template = _arch(self.env, 'sgi.checklist.template', 'form',
                         'quimibond_sgi.sgi_checklist_template_view_form')
        self.assertTrue(template.xpath('//chatter'))

    def test_08_mi_equipo_y_agrupaciones(self):
        """V-B14 y V-B17: el total de Mi equipo es de la página; agrupaciones en <group>."""
        team = _arch(self.env, 'hr.employee.public', 'list', 'quimibond_sgi.sgi_my_team_view_list')
        for node in team.xpath('//field[@sum]'):
            self.assertEqual(node.get('sum'), 'Total de la página')
        search = _arch(self.env, 'sgi.action.line', 'search', 'quimibond_sgi.sgi_action_line_view_search')
        grouped = {f.get('name') for f in search.xpath('//group/filter')}
        self.assertEqual(grouped, {'group_responsible', 'group_state', 'group_commit'})
        self.assertFalse(search.xpath("//filter[starts-with(@string, 'Agrupar por')]"))
