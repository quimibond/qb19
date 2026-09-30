# -*- coding: utf-8 -*-
"""57.40.0+ (pulido de vistas, bloques 1 a 3 de la revisión del 2026-09-30).

Lo que se puede probar sin pantalla: que las vistas nuevas cargan y tienen la
forma prometida, y la lógica de servidor que las acompaña (filtro «Plazo
vencido», solo lectura en cerrado, D-009, validar mediciones)."""
from datetime import datetime, timedelta

from lxml import etree

from odoo.exceptions import AccessError, UserError
from odoo.tests import TransactionCase, tagged, new_test_user

from .common_calendar import sgi_test_calendar


def _arch(env, model, view_type, view_ref=None):
    view_id = env.ref(view_ref).id if view_ref else False
    views = env[model].get_views([(view_id, view_type)])
    return etree.fromstring(views['views'][view_type]['arch'])


@tagged('post_install', '-at_install')
class TestNcFichaYLista(TransactionCase):
    """57.40.0 (V-A01, V-A02): una sola ficha de NC y su lista propia."""

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        sgi_test_calendar(cls.env)
        cls.team = cls.env.ref('quimibond_sgi.sgi_quality_team_internal')
        cls.stage_open = cls.env.ref('quimibond_sgi.sgi_nc_int_stage_open')
        cls.user = new_test_user(cls.env, login='vp_nc_user',
                                 groups='base.group_user,quimibond_sgi.group_sgi_user')

    def _nc(self, days_ago=0):
        alert = self.env['quality.alert'].create({
            'title': 'NC pulido', 'team_id': self.team.id, 'stage_id': self.stage_open.id,
            'sgi_responsible_ids': [(6, 0, self.user.ids)]})
        if days_ago:
            old = datetime.now() - timedelta(days=days_ago)
            self.env.cr.execute("UPDATE quality_alert SET create_date = %s WHERE id = %s",
                                (old, alert.id))
            alert.invalidate_recordset()
            alert._sgi_set_deadlines(force=True)
        return alert

    def test_01_un_solo_juego_de_pestanas(self):
        arch = _arch(self.env, 'quality.alert', 'form')
        self.assertEqual(len(arch.xpath('//sheet//notebook')), 1,
                         "La ficha de NC debe tener un solo notebook.")
        pages = [p.get('name') for p in arch.xpath('//sheet/notebook/page')]
        for name in ('sgi_analysis', 'sgi_actions', 'sgi_customer', 'sgi_close',
                     'description', 'sgi_supplier'):
            self.assertIn(name, pages)
        self.assertLess(pages.index('sgi_analysis'), pages.index('description'))
        self.assertNotIn('sgi_claim', pages, "Los metros reclamados van en «Cliente».")
        self.assertTrue(arch.xpath("//page[@name='sgi_customer']//field[@name='sgi_claimed_meters']"))
        # Un solo aviso con el formato controlado adentro.
        self.assertEqual(len(arch.xpath("//field[@name='sgi_format_banner']")), 1)

    def test_02_lista_y_kanban_propios(self):
        list_arch = _arch(self.env, 'quality.alert', 'list', 'quimibond_sgi.sgi_nc_view_list')
        for name in ('sgi_folio', 'sgi_classification', 'sgi_process_id',
                     'sgi_containment_state', 'sgi_root_cause_state', 'sgi_plan_state'):
            self.assertTrue(list_arch.xpath("//field[@name='%s']" % name), name)
        kanban_arch = _arch(self.env, 'quality.alert', 'kanban', 'quimibond_sgi.sgi_nc_view_kanban')
        self.assertTrue(kanban_arch.xpath("//field[@name='sgi_folio']"))
        action = self.env.ref('quimibond_sgi.sgi_nc_board_action').run()
        view_ids = dict((vt, vid) for vid, vt in action['views'])
        self.assertEqual(view_ids['list'], self.env.ref('quimibond_sgi.sgi_nc_view_list').id)
        self.assertEqual(view_ids['kanban'], self.env.ref('quimibond_sgi.sgi_nc_view_kanban').id)
        # Las vistas propias no le ganan a las de Calidad en su app.
        self.assertNotEqual(self.env['quality.alert'].get_views([(False, 'list')])['views']['list']['id'],
                            self.env.ref('quimibond_sgi.sgi_nc_view_list').id)

    def test_03_filtro_plazo_vencido(self):
        late = self._nc(days_ago=40)
        fresh = self._nc()
        self.assertTrue(late.sgi_deadline_overdue)
        self.assertFalse(fresh.sgi_deadline_overdue)
        Alert = self.env['quality.alert']
        found = Alert.search([('sgi_deadline_overdue', '=', True), ('id', 'in', (late | fresh).ids)])
        self.assertEqual(found, late)
        found = Alert.search([('sgi_deadline_overdue', '!=', True), ('id', 'in', (late | fresh).ids)])
        self.assertEqual(found, fresh)
        # Con la contención, la causa raíz y el plan cumplidos deja de estar vencida.
        for kind in ('contencion', 'correctiva'):
            self.env['sgi.action.line'].create({
                'alert_id': late.id, 'action_type': kind, 'name': 'Acción %s' % kind,
                'responsible_id': self.user.id, 'date_commit': datetime.now().date()})
        late.sgi_root_cause = 'Causa'
        self.assertFalse(late.sgi_deadline_overdue)
        self.assertFalse(Alert.search([('sgi_deadline_overdue', '=', True), ('id', '=', late.id)]))


@tagged('post_install', '-at_install')
class TestCerradoYDecisiones(TransactionCase):
    """57.41.0 (V-A03): solo lectura en cerrado; (V-A05, D-009): decisiones del
    Jefe MAST y del dueño del proceso, revisadas en el servidor."""

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        groups = 'base.group_user,quimibond_sgi.group_sgi_user'
        cls.raso = new_test_user(cls.env, login='vp_raso', groups=groups)
        cls.owner_user = new_test_user(cls.env, login='vp_owner', groups=groups)
        cls.mast = new_test_user(cls.env, login='vp_mast',
                                 groups='base.group_user,quimibond_sgi.group_sgi_manager')
        owner = cls.env['hr.employee'].create({'name': 'Dueña VP', 'user_id': cls.owner_user.id})
        cls.process = cls.env['sgi.process'].create({
            'code': 'XVP', 'name': 'Proceso pulido', 'owner_id': owner.id})

    def _risk(self):
        return self.env['sgi.risk'].create({
            'name': 'Riesgo VP', 'instrument': 'ryo', 'process_id': self.process.id,
            'eval_probability': '1', 'eval_impact': '1'})

    def test_01_cerrado_de_solo_lectura_menos_para_mast(self):
        risk = self._risk()
        self.assertFalse(risk.with_user(self.raso).sgi_is_locked)
        risk.write({'state': 'cerrado'})
        self.assertTrue(risk.with_user(self.raso).sgi_is_locked)
        self.assertTrue(risk.with_user(self.owner_user).sgi_is_locked)
        self.assertFalse(risk.with_user(self.mast).sgi_is_locked)
        # Coherente con el candado del servidor.
        with self.assertRaises(UserError):
            risk.with_user(self.owner_user).write({'name': 'Otro'})
        # Los modelos sin candado nunca se ven cerrados.
        fmea = self.env['sgi.fmea'].create({'name': 'AMEF VP', 'process_id': self.process.id})
        self.assertFalse(fmea.with_user(self.raso).sgi_is_locked)

    def test_02_incidente_del_reportante(self):
        incident = self.env['sgi.incident'].with_user(self.raso).create({'name': 'Resbalón VP'})
        self.assertFalse(incident.sgi_is_locked, "El reportante edita mientras está reportado.")
        incident.sudo().state = 'investigacion'
        self.assertTrue(incident.sgi_is_locked)
        self.assertFalse(incident.with_user(self.mast).sgi_is_locked)

    def test_03_riesgo_cerrar_y_reabrir(self):
        risk = self._risk()
        risk.write({'state': 'controlado'})
        with self.assertRaises(AccessError):
            risk.with_user(self.raso).action_set_cerrado()
        risk.with_user(self.owner_user).action_set_cerrado()
        self.assertEqual(risk.state, 'cerrado')
        with self.assertRaises(AccessError):
            risk.with_user(self.raso).action_set_identificado()
        # El dueño reabre (solo el estado), aunque el resto siga cerrado.
        risk.with_user(self.owner_user).action_set_identificado()
        self.assertEqual(risk.state, 'identificado')
        # Pasos intermedios siguen abiertos a quien edita el riesgo.
        risk.with_user(self.owner_user).action_set_en_tratamiento()
        self.assertEqual(risk.state, 'en_tratamiento')

    def test_04_ppap_aprobar_solo_mast_o_dueno(self):
        partner = self.env['res.partner'].create({'name': 'Cliente VP', 'is_company': True})
        product = self.env['product.template'].create({'name': 'Fieltro VP'})
        ppap = self.env['sgi.ppap'].create({'partner_id': partner.id, 'product_tmpl_id': product.id})
        ppap.element_ids.write({'state': 'listo'})
        ppap.action_mark_enviado()
        with self.assertRaises(AccessError):
            ppap.with_user(self.raso).action_approve()
        with self.assertRaises(AccessError):
            ppap.with_user(self.owner_user).action_reject()
        # Con un AMEF del proceso en sus elementos, el dueño sí decide.
        fmea = self.env['sgi.fmea'].create({'name': 'AMEF PPAP', 'process_id': self.process.id})
        ppap.element_ids[:1].fmea_id = fmea
        ppap.with_user(self.owner_user).action_set_interino()
        self.assertEqual(ppap.state, 'interino')
        with self.assertRaises(AccessError):
            ppap.with_user(self.raso).action_reset()
        ppap.with_user(self.mast).action_reset()
        self.assertEqual(ppap.state, 'preparacion')
        # Regresar desde «Enviado» no es decisión.
        ppap.action_mark_enviado()
        ppap.with_user(self.raso).action_reset()
        self.assertEqual(ppap.state, 'preparacion')

    def test_05_obsoletos(self):
        fmea = self.env['sgi.fmea'].create({'name': 'AMEF VP2', 'process_id': self.process.id})
        fmea.write({'state': 'vigente'})
        with self.assertRaises(AccessError):
            fmea.with_user(self.raso).action_set_obsoleto()
        fmea.with_user(self.owner_user).action_set_obsoleto()
        self.assertEqual(fmea.state, 'obsoleto')
        with self.assertRaises(AccessError):
            fmea.with_user(self.raso).action_set_borrador()
        plan = self.env['sgi.emergency.plan'].create({
            'name': 'Sismo VP', 'plan_type': 'sismo', 'responsible_id': self.raso.id})
        plan.action_set_vigente()
        # Sin proceso no hay dueño: solo el Jefe MAST.
        with self.assertRaises(AccessError):
            plan.with_user(self.owner_user).action_set_obsoleto()
        plan.with_user(self.mast).action_set_obsoleto()
        self.assertEqual(plan.state, 'obsoleto')
        cplan = self.env['sgi.control.plan'].create({'name': 'Plan VP'})
        cplan.write({'state': 'vigente'})
        with self.assertRaises(AccessError):
            cplan.with_user(self.owner_user).action_set_obsoleto()
        fmea.control_plan_id = cplan
        cplan.with_user(self.owner_user).action_set_obsoleto()
        self.assertEqual(cplan.state, 'obsoleto')


@tagged('post_install', '-at_install')
class TestFichasCoherentes(TransactionCase):
    """57.42.0 (V-M04, V-M06, V-M07, V-M15): misma lista de acciones, botones
    con el mismo nombre y el nombre como título."""

    FICHAS = ('sgi.incident', 'sgi.risk', 'sgi.fmea', 'sgi.emergency.drill', 'sgi.objective',
              'sgi.indicator.measure', 'quality.alert', 'sgi.audit', 'sgi.management.review',
              'sgi.msa.study', 'sgi.ppap', 'sgi.action.line', 'sgi.process.activity',
              'sgi.policy', 'sgi.audit.program')

    def test_01_fichas_cargan(self):
        for model in self.FICHAS:
            arch = _arch(self.env, model, 'form')
            self.assertTrue(arch.xpath('//sheet'), model)

    def test_02_botones_con_el_mismo_nombre(self):
        viejos = {'A borrador', 'Volver a borrador', 'Obsoletar', 'Marcar terminada'}
        for model in self.FICHAS:
            arch = _arch(self.env, model, 'form')
            labels = {b.get('string') for b in arch.xpath('//header/button')}
            self.assertFalse(labels & viejos, "%s: %s" % (model, labels & viejos))

    def test_03_titulo_legible(self):
        process = self.env['sgi.process'].create({'code': 'XVPT', 'name': 'Compras VP'})
        audit = self.env['sgi.audit'].create({
            'audit_type': 'interna', 'process_ids': [(6, 0, process.ids)]})
        self.assertIn('Compras VP', audit.sgi_heading)
        self.assertNotIn(audit.folio, audit.sgi_heading)
        arch = _arch(self.env, 'sgi.audit', 'form')
        self.assertTrue(arch.xpath("//div[contains(@class, 'oe_title')]/h1/field[@name='sgi_heading']"))
        for model, field in (('sgi.ppap', 'product_tmpl_id'), ('sgi.msa.study', 'equipment_id'),
                             ('sgi.emergency.drill', 'plan_id'),
                             ('sgi.management.review', 'sgi_heading')):
            arch = _arch(self.env, model, 'form')
            self.assertTrue(arch.xpath("//div[contains(@class, 'oe_title')]/h1/field[@name='%s']" % field), model)

    def test_04_actividad_sin_statusbar_de_medicion(self):
        arch = _arch(self.env, 'sgi.process.activity', 'form')
        self.assertFalse(arch.xpath("//header/field[@name='measure_state']"))
        self.assertTrue(arch.xpath("//sheet//field[@name='measure_state'][@widget='badge']"))


@tagged('post_install', '-at_install')
class TestPisoYVencimientos(TransactionCase):
    """57.43.0 (bloque 2): tableta de checklist, medición capturar → validar,
    «Mis indicadores», metrología, vencimientos y filtros."""

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        groups = 'base.group_user,quimibond_sgi.group_sgi_user'
        cls.owner = new_test_user(cls.env, login='vp_kpi_owner', groups=groups)
        cls.raso = new_test_user(cls.env, login='vp_kpi_raso', groups=groups)
        cls.indicator = cls.env['sgi.indicator'].create({
            'code': 'ZVP1', 'name': 'KPI pulido', 'calc_mode': 'manual',
            'responsible_id': cls.owner.id})

    def _measure(self, day, **vals):
        return self.env['sgi.indicator.measure'].create(dict({
            'indicator_id': self.indicator.id,
            'period_date': datetime(2031, day, 1).date()}, **vals))

    def test_01_validar_solo_responsable_o_mast(self):
        measure = self._measure(1, value=10.0, state='capturado')
        self.assertTrue(measure.with_user(self.owner).sgi_can_validate)
        self.assertFalse(measure.with_user(self.raso).sgi_can_validate)
        # Por el botón y por escritura directa (RPC).
        with self.assertRaises(AccessError):
            measure.with_user(self.raso).action_validate()
        with self.assertRaises(AccessError):
            measure.with_user(self.raso).write({'state': 'validado'})
        measure.with_user(self.owner).action_validate()
        self.assertEqual(measure.state, 'validado')
        arch = _arch(self.env, 'sgi.indicator.measure', 'form', 'quimibond_sgi.sgi_measure_view_form')
        primaries = [b.get('name') for b in arch.xpath("//header/button[contains(@class, 'btn-primary')]")]
        self.assertEqual(sorted(primaries), ['action_capture', 'action_validate'])

    def test_02_mis_indicadores_capturar(self):
        late = self._measure(2, state='pendiente')
        self._measure(3, state='pendiente')
        self.assertEqual(self.indicator.sgi_next_pending_id, late)
        action = self.indicator.action_sgi_capture()
        self.assertEqual((action['res_model'], action['res_id']), ('sgi.indicator.measure', late.id))
        late.write({'value': 5.0, 'state': 'capturado'})
        self.assertNotEqual(self.indicator.sgi_next_pending_id, late)
        mine = self.env.ref('quimibond_sgi.sgi_indicator_action_mine')
        self.assertEqual(mine.view_ids[:1].view_id, self.env.ref('quimibond_sgi.sgi_indicator_view_list_mine'))

    def test_03_vistas_del_piso(self):
        today = self.env.ref('quimibond_sgi.sgi_checklist_today_action')
        form = self.env.ref('quimibond_sgi.sgi_checklist_request_view_form')
        self.assertIn(form, today.view_ids.view_id)
        self.assertIn(form, self.env.ref('quimibond_sgi.sgi_checklist_request_action').view_ids.view_id)
        arch = _arch(self.env, 'maintenance.request', 'form', 'quimibond_sgi.sgi_checklist_request_view_form')
        self.assertTrue(arch.xpath("//header/button[@name='action_sgi_checklist_finish']"))
        self.assertTrue(arch.xpath("//field[@name='answer'][@widget='selection_badge']"))
        # La ficha estándar de Mantenimiento sigue siendo la de su app.
        default_form = self.env['maintenance.request'].get_views([(False, 'form')])['views']['form']['id']
        self.assertNotEqual(default_form, form.id)
        _arch(self.env, 'maintenance.request', 'kanban', 'quimibond_sgi.sgi_checklist_request_view_kanban')
        equipment = self.env.ref('quimibond_sgi.sgi_equipment_action_measuring')
        self.assertEqual(equipment.search_view_id, self.env.ref('quimibond_sgi.sgi_equipment_view_search_measuring'))
        _arch(self.env, 'maintenance.equipment', 'list', 'quimibond_sgi.sgi_equipment_view_list_measuring')

    def test_04_filtros_por_defecto_existen(self):
        """Cada search_default_* de las acciones tocadas existe en su búsqueda."""
        for xmlid in ('sgi_incident_action', 'sgi_audit_action', 'sgi_epp_delivery_action',
                      'sgi_emergency_drill_action', 'sgi_csh_inspection_action', 'sgi_measure_action',
                      'sgi_document_ack_action', 'sgi_legal_requirement_action',
                      'sgi_calibration_action', 'sgi_equipment_action_measuring'):
            action = self.env.ref('quimibond_sgi.%s' % xmlid)
            view = action.search_view_id
            arch = _arch(self.env, action.res_model, 'search', view.get_external_id()[view.id]) \
                if view else _arch(self.env, action.res_model, 'search')
            names = {f.get('name') for f in arch.xpath('//filter')}
            context = action.context or ''
            for key in [k for k in context.replace('"', "'").split("'") if k.startswith('search_default_')]:
                self.assertIn(key[len('search_default_'):], names, "%s: %s" % (xmlid, key))


@tagged('post_install', '-at_install')
class TestDocumentosEImpresos(TransactionCase):
    """57.44.0 (bloque 3): alta de documento, pie de formato en los reportes
    que lo perdieron, NC imprimible solo con folio y datos vacíos."""

    def _html(self, report, records):
        html, _kind = self.env['ir.actions.report']._render_qweb_html(report, records.ids)
        return html.decode() if isinstance(html, bytes) else html

    def test_01_alta_de_documento_con_titulo(self):
        arch = _arch(self.env, 'documents.document', 'form', 'quimibond_sgi.sgi_document_view_form')
        title = arch.xpath("//div[contains(@class, 'oe_title')]/h1/field")
        self.assertEqual(title[0].get('name'), 'name')
        self.assertIn('sin clave', title[0].get('placeholder'))

    def test_02_pie_por_modelo_y_por_referencia(self):
        Map = self.env['sgi.format.map']
        incident = self.env['sgi.incident'].create({'name': 'Golpe VP'})
        self.assertFalse(Map.sgi_footer_label(incident), "Sin mapeo no hay pie.")
        Map.create({'model_id': self.env['ir.model']._get('sgi.incident').id,
                    'sgi_code': 'F-P-S02-01', 'note': 'Prueba'})
        self.assertTrue(Map.sgi_footer_label(incident).startswith('F-P-S02-01'))
        self.assertIn('F-P-S02-01', self._html('quimibond_sgi.report_incident_document', incident))
        self.assertTrue(Map.sgi_footer_label(incident, 'format_ref_audit_plan').startswith('F-P-G03-03'))
        audit = self.env['sgi.audit'].create({'audit_type': 'interna'})
        self.assertIn('F-P-G03-03', self._html('quimibond_sgi.report_audit_plan_document', audit))

    def test_03_nc_solo_con_folio_y_sin_vacios(self):
        floor = self.env['quality.alert'].create({'title': 'Alerta de piso VP'})
        self.assertFalse(floor.sgi_folio)
        self.assertIn('no es una No Conformidad', self._html('quimibond_sgi.report_nc_document', floor))
        nc = self.env['quality.alert'].create({
            'title': 'NC VP', 'team_id': self.env.ref('quimibond_sgi.sgi_quality_team_internal').id,
            'stage_id': self.env.ref('quimibond_sgi.sgi_nc_int_stage_open').id})
        self.assertTrue(nc.sgi_folio)
        html = self._html('quimibond_sgi.report_nc_document', nc)
        self.assertIn('Sin análisis de 5 porqués', html)
        self.assertIn('Sin acciones registradas', html)
        self.assertNotIn('>False<', html)
