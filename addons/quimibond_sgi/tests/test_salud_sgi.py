# -*- coding: utf-8 -*-
"""57.99.0 — Salud del SGI (auditoría 2026-10: sección 8 y hallazgo D-01).

Diez indicadores de nivel Dirección del proceso E2, semanales, con su modo de
cálculo; página «Salud del SGI» en el Tablero con la tabla por dueño de
proceso; correo semanal a Dirección. Las mediciones de salud no piden
validación, ni causa y plan, ni avisan «no calculó». La base del build es
copia de producción: cada prueba mide la diferencia que causa lo que siembra,
nunca un total."""
import importlib.util
import os
import re
import time
from datetime import date, timedelta
from unittest.mock import patch

from dateutil.relativedelta import relativedelta

from odoo import fields
from odoo.exceptions import UserError
from odoo.tests import TransactionCase, new_test_user, tagged

from .common_documents import sgi_hide_real_documents
from .common_users import sgi_set_director, sgi_set_mast

_MODULE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_PRE_MIGRATE = os.path.join(_MODULE_DIR, 'migrations', '19.0.57.99.0', 'pre-migrate.py')

USER = 'base.group_user,quimibond_sgi.group_sgi_user'
MODULE = 'quimibond_sgi.'
XMLIDS = {
    'SG-01': MODULE + 'sgi_ind_salud_procesos',
    'SG-02': MODULE + 'sgi_ind_salud_personas',
    'SG-03': MODULE + 'sgi_ind_salud_planta',
    'SG-04': MODULE + 'sgi_ind_salud_acuses',
    'SG-05': MODULE + 'sgi_ind_salud_validacion',
    'SG-06': MODULE + 'sgi_ind_salud_rojos',
    'SG-07': MODULE + 'sgi_ind_salud_nc',
    'SG-08': MODULE + 'sgi_ind_salud_avisos',
    'SG-09': MODULE + 'sgi_ind_salud_auditoria',
    'SG-10': MODULE + 'sgi_ind_salud_formatos',
}
# Se arman por partes: los crea data/sgi_health_mail.xml.
CRON_XMLID = MODULE + 'sgi_cron_health_weekly'
TEMPLATE_XMLID = MODULE + 'mail_template_sgi_health_weekly'
EXCLUDED_PARAM = 'quimibond_sgi.health_excluded_user_ids'
MAIL_PARAM = 'quimibond_sgi.health_mail_user_ids'


def _load_pre_migrate():
    """Se carga dentro de la prueba: un archivo que falta no debe romper la
    carga de todo el paquete de pruebas."""
    spec = importlib.util.spec_from_file_location('sgi_mig_57_99_0', _PRE_MIGRATE)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _note_count(note, label):
    """El número que sigue a «label:» en la nota de la medición (0 si no está)."""
    match = re.search(r"%s: (\d+)" % re.escape(label), note or '')
    return int(match.group(1)) if match else 0


@tagged('post_install', '-at_install')
class TestSaludSgi(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        from ..models.sgi_calendar import sgi_today
        cls.company = cls.env['sgi.config']._sgi_company()
        cls.Indicator = cls.env['sgi.indicator']
        cls.Measure = cls.env['sgi.indicator.measure']
        cls.ind = {code: cls.env.ref(xmlid) for code, xmlid in XMLIDS.items()}
        cls.today = sgi_today(cls.env)
        # La semana pasada (lunes a domingo): el periodo que mide el cron.
        monday = cls.today - timedelta(days=cls.today.weekday())
        cls.prev_monday = monday - timedelta(days=7)
        # Para los modos que miden «hasta hoy» en las pruebas.
        cls.until_today = (cls.today - timedelta(days=6), cls.today)

    # ---- ayudas ------------------------------------------------------------
    def _detail(self, code, bounds=None):
        date_from, date_to = bounds or self.until_today
        indicator = self.ind[code]
        out = getattr(indicator, '_detail_%s' % indicator.calc_mode)(date_from, date_to)
        return (out.get('numerator') or 0, out.get('denominator') or 0, out)

    def _employee(self, name, job=True, user=None, company=None):
        company = company or self.company
        vals = {'name': name, 'company_id': company.id}
        if job:
            vals['job_id'] = self.env['hr.job'].create(
                {'name': 'Puesto salud %s' % name, 'company_id': company.id}).id
        if user:
            vals['user_id'] = user.id
        return self.env['hr.employee'].create(vals)

    def _indicator(self, code, **vals):
        base = {'code': code, 'name': 'Indicador %s' % code, 'calc_mode': 'manual',
                'uom': '%', 'target_objective': 90, 'target_acceptable': 80}
        base.update(vals)
        return self.Indicator.create(base)

    def _activity(self, record, user, deadline, key='prueba_salud'):
        return self.env['mail.activity'].create({
            'res_model_id': self.env['ir.model']._get_id(record._name),
            'res_id': record.id,
            'activity_type_id': self.env.ref('mail.mail_activity_data_todo').id,
            'summary': 'Aviso de prueba de salud',
            'user_id': user.id,
            'date_deadline': deadline,
            'sgi_cron_key': '%s:%s' % (key, record.id),
        })

    # ---- 1. datos ------------------------------------------------------------
    def test_01_diez_indicadores_de_direccion(self):
        from ..models.sgi_health_const import HEALTH_MODES
        generic = "Cálculo automático desde datos de Odoo."
        modes = set()
        for number, code in enumerate(sorted(XMLIDS), 1):
            indicator = self.ind[code]
            self.assertTrue(indicator.active, code)
            self.assertEqual(indicator.code, 'SG-%02d' % number)
            self.assertEqual(indicator.level, 'direccion', code)
            self.assertEqual(indicator.frequency, 'weekly', code)
            self.assertIn(indicator.calc_mode, HEALTH_MODES, code)
            modes.add(indicator.calc_mode)
            self.assertFalse(indicator.nc_on_red, code)
            self.assertEqual(indicator.source_type, 'auto', code)
            self.assertTrue(indicator.source_info and indicator.source_info != generic, code)
            self.assertTrue(indicator.target_objective, code)
            self.assertTrue((indicator.formula or '').strip(), code)
            self.assertTrue((indicator.source or '').strip(), code)
            self.assertEqual(indicator.snapshot, code not in ('SG-05', 'SG-07'), code)
            self.assertEqual(indicator.direction,
                             'lower_better' if code == 'SG-08' else 'higher_better', code)
            expected_uom = {'SG-02': 'personas', 'SG-08': 'avisos'}.get(code, '%')
            self.assertEqual(indicator.uom, expected_uom, code)
        self.assertEqual(len(modes), 10, "Un modo distinto por indicador.")
        # Nacen en prueba (puede que MAST ya haya pasado alguno a oficial en
        # la copia de producción: solo se revisa que el campo exista).
        self.assertIn(self.ind['SG-01'].status, ('prueba', 'oficial'))

    def test_02_liga_al_proceso_e2_idempotente(self):
        sgi_set_mast(self.env, login='zsa_mast_02')
        Process = self.env['sgi.process']
        e2 = Process.search([('code', '=', 'E2'), ('company_id', '=', self.company.id)], limit=1)
        if not e2:
            e2 = Process.create({'code': 'E2', 'name': 'Gestión del SGI',
                                 'company_id': self.company.id})
        sg01, sg02 = self.ind['SG-01'], self.ind['SG-02']
        sg01.write({'process_id': False, 'responsible_id': False})
        other = Process.create({'code': 'Z98S', 'name': 'Proceso de MAST',
                                'company_id': self.company.id})
        sg02.write({'process_id': other.id})
        changed = self.Indicator._sgi_health_link_process()
        self.assertIn(sg01.id, changed)
        self.assertEqual(sg01.process_id, e2)
        self.assertTrue(sg01.responsible_id, "Responsable: el dueño de E2 o el Jefe MAST.")
        self.assertFalse(sg01.spec_missing, "Con proceso y responsable el indicador está completo.")
        self.assertEqual(sg02.process_id, other, "Lo que MAST puso no se toca.")
        self.assertEqual(self.Indicator._sgi_health_link_process(), [], "Idempotente.")

    # ---- 2. modos -------------------------------------------------------------
    def test_03_personas_que_usan_el_sgi(self):
        con_emp = new_test_user(self.env, login='zsa_con_emp', groups=USER)
        sin_emp = new_test_user(self.env, login='zsa_sin_emp', groups=USER)
        excluido = new_test_user(self.env, login='zsa_excluido', groups=USER)
        self._employee('Salud con empleado', user=con_emp)
        self._employee('Salud excluido', user=excluido)
        self.env['ir.config_parameter'].sudo().set_param(EXCLUDED_PARAM, str(excluido.id))
        indicator = self._indicator('TST-SA03')
        measure = self.Measure.create({'indicator_id': indicator.id,
                                       'period_date': date(2020, 1, 1), 'state': 'capturado',
                                       'value': 50})
        num_before, _den, _out = self._detail('SG-02')
        for user in (con_emp, sin_emp, excluido):
            measure.message_post(body='Prueba salud', author_id=user.partner_id.id,
                                 message_type='comment')
        num_after, _den, out = self._detail('SG-02')
        self.assertEqual(num_after - num_before, 1)
        self.assertEqual(out['model'], 'res.users')
        self.assertIn(con_emp.id, out['ids'])
        self.assertNotIn(sin_emp.id, out['ids'], "Sin empleado (cuenta genérica) no cuenta.")
        self.assertNotIn(excluido.id, out['ids'], "El parámetro de excluidos manda.")
        self.assertNotIn(self.env.ref('base.user_root').id, out['ids'])

    def test_04_planta_identificable(self):
        other = self.env['res.company'].create({'name': 'Otra empresa salud SGI'})
        user = new_test_user(self.env, login='zsa_planta', groups=USER)
        user_other = new_test_user(self.env, login='zsa_planta_otra', groups=USER,
                                   company_id=other.id, company_ids=[(6, 0, other.ids)])
        num_before, den_before, _out = self._detail('SG-03')
        self._employee('Planta con usuario', user=user)
        self._employee('Planta sin usuario')
        self._employee('Planta sin puesto', job=False)
        self._employee('Planta otra empresa', user=user_other, company=other)
        num_after, den_after, out = self._detail('SG-03')
        self.assertEqual(num_after - num_before, 1)
        self.assertEqual(den_after - den_before, 2)
        self.assertEqual(out['model'], 'hr.employee.public')

    def test_05_acuses_al_dia(self):
        sgi_hide_real_documents(self.env)
        Doc = self.env['documents.document']
        doc = Doc.create({'name': 'IT-P-C11-97.pdf', 'type': 'binary', 'sgi_is_controlled': True,
                          'sgi_doc_type': 'instructivo', 'sgi_code': 'IT-P-C11-97',
                          'sgi_state': 'vigente'})
        draft = Doc.create({'name': 'IT-P-C11-98.pdf', 'type': 'binary', 'sgi_is_controlled': True,
                            'sgi_doc_type': 'instructivo', 'sgi_code': 'IT-P-C11-98',
                            'sgi_state': 'borrador'})
        employees = [self._employee('Acuse salud %d' % i) for i in range(3)]
        num_before, den_before, out = self._detail('SG-04')
        late_before = _note_count(out.get('note'), "Vencidos")
        Ack = self.env['sgi.document.ack']
        read, _recent, late = [Ack.create({'document_id': doc.id, 'employee_id': e.id})
                               for e in employees]
        read.write({'state': 'leido', 'ack_date': fields.Datetime.now()})
        Ack.create({'document_id': draft.id, 'employee_id': employees[0].id})
        self.env.flush_all()
        self.env.cr.execute("UPDATE sgi_document_ack SET create_date = %s WHERE id = %s",
                            (fields.Datetime.now() - timedelta(days=30), late.id))
        self.env.invalidate_all()
        num_after, den_after, out = self._detail('SG-04')
        self.assertEqual(num_after - num_before, 2, "Leído y pendiente en plazo.")
        self.assertEqual(den_after - den_before, 3, "El acuse del borrador no cuenta.")
        self.assertEqual(_note_count(out['note'], "Vencidos") - late_before, 1,
                         "La nota separa los vencidos.")

    def test_06_validadas_a_tiempo_y_fecha_de_validacion(self):
        mast = sgi_set_mast(self.env, login='zsa_mast_06')
        indicator = self._indicator('TST-SA06', responsible_id=mast.id)
        captured = self.today - timedelta(days=10)
        num_before, den_before, _out = self._detail('SG-05')
        late, on_time, waiting = [self.Measure.create({
            'indicator_id': indicator.id, 'period_date': date(2020, month, 1),
            'state': 'capturado', 'captured_date': captured, 'value': 95})
            for month in (1, 2, 3)]
        # Una medición de salud capturada no cuenta (no se valida).
        self.Measure.create({'indicator_id': self.ind['SG-01'].id, 'period_date': date(2020, 1, 6),
                             'state': 'capturado', 'captured_date': captured, 'value': 10})
        late.with_user(mast).action_validate()
        self.assertEqual(late.sgi_validated_date, self.today)
        self.assertGreater(self.today, late._sgi_validate_due(), "Validada después del plazo.")
        on_time.action_validate()
        on_time.sudo().write({'sgi_validated_date': on_time._sgi_validate_due()})
        self.assertEqual(waiting.state, 'capturado')
        num_after, den_after, out = self._detail('SG-05')
        self.assertEqual(num_after - num_before, 1)
        self.assertEqual(den_after - den_before, 3)
        self.assertEqual(out['model'], 'sgi.indicator.measure')
        # Re-validar una validada no mueve su fecha.
        due = on_time.sgi_validated_date
        on_time.write({'state': 'validado'})
        self.assertEqual(on_time.sgi_validated_date, due)

    def test_07_rojos_con_respuesta(self):
        owner = new_test_user(self.env, login='zsa_rojos', groups=USER)
        indicator = self._indicator('TST-SA07', direction='higher_better', responsible_id=owner.id)
        num_before, den_before, _out = self._detail('SG-06')
        first = self.today.replace(day=1)
        measures = [self.Measure.create({
            'indicator_id': indicator.id, 'period_date': first - relativedelta(months=back),
            'state': 'capturado', 'value': 10, 'sample_size': 0})
            for back in (0, 1, 2)]
        with_nc, with_plan, _bare = measures
        self.assertTrue(all(m.semaphore == 'rojo' and not m.small_sample for m in measures))
        team = self.env.ref('quimibond_sgi.sgi_quality_team_internal')
        alert = self.env['quality.alert'].create({'title': 'NC salud rojos', 'team_id': team.id,
                                                  'sgi_origin_type': 'indicador'})
        with_nc.write({'alert_id': alert.id})
        with_plan.write({'cause': 'Faltó materia prima'})
        self.env['sgi.action.line'].create({
            'measure_id': with_plan.id, 'name': 'Asegurar el abasto', 'responsible_id': owner.id,
            'date_commit': self.today + timedelta(days=10)})
        # Un rojo de salud no cuenta.
        self.Measure.create({'indicator_id': self.ind['SG-06'].id, 'period_date': date(2020, 1, 6),
                             'state': 'capturado', 'value': 0})
        num_after, den_after, _out = self._detail('SG-06')
        self.assertEqual(num_after - num_before, 2)
        self.assertEqual(den_after - den_before, 3)

    def test_08_nc_eficaces_y_abiertas_viejas(self):
        team = self.env.ref('quimibond_sgi.sgi_quality_team_internal')
        closed_stage = self.env.ref('quimibond_sgi.sgi_nc_int_stage_closed')
        self.assertTrue(closed_stage.sgi_is_closing_stage)
        num_before, den_before, out = self._detail('SG-07')
        label = "NC abiertas con más de 60 días"
        old_before = _note_count(out.get('note'), label)
        alerts = [self.env['quality.alert'].create({
            'title': 'NC salud %d' % i, 'team_id': team.id, 'company_id': self.company.id})
            for i in range(3)]
        self.assertTrue(all(a.sgi_folio for a in alerts))
        first, second, old = alerts
        # Etapa, fechas y contador por SQL: el cierre tiene candados y
        # create_date no se escribe por ORM (patrón de test_indicadores_571).
        self.env.flush_all()
        closed_on = fields.Datetime.now() - timedelta(days=10)
        self.env.cr.execute(
            "UPDATE quality_alert SET stage_id = %s, date_close = %s, sgi_effective = 'eficaz',"
            " sgi_ineffective_count = %s WHERE id = %s",
            (closed_stage.id, closed_on, 0, first.id))
        self.env.cr.execute(
            "UPDATE quality_alert SET stage_id = %s, date_close = %s, sgi_effective = 'eficaz',"
            " sgi_ineffective_count = %s WHERE id = %s",
            (closed_stage.id, closed_on, 1, second.id))
        self.env.cr.execute("UPDATE quality_alert SET create_date = %s WHERE id = %s",
                            (fields.Datetime.now() - timedelta(days=70), old.id))
        self.env.invalidate_all()
        num_after, den_after, out = self._detail('SG-07')
        self.assertEqual(num_after - num_before, 1, "Solo la eficaz a la primera.")
        self.assertEqual(den_after - den_before, 2)
        self.assertIn(label, out['note'])
        self.assertEqual(_note_count(out['note'], label) - old_before, 1)

    def test_09_avisos_vencidos_y_concentracion(self):
        user_a = new_test_user(self.env, login='zsa_avisos_a', groups=USER, name='Persona Avisos Alfa')
        user_b = new_test_user(self.env, login='zsa_avisos_b', groups=USER, name='Persona Avisos Beta')
        target = self.ind['SG-01']
        num_before, _den, _out = self._detail('SG-08')
        yesterday = self.today - timedelta(days=1)
        self._activity(target, user_a, yesterday, key='prueba_salud_1')
        self._activity(target, user_a, yesterday, key='prueba_salud_2')
        self._activity(target, user_b, yesterday, key='prueba_salud_3')
        self._activity(target, user_b, self.today + timedelta(days=1), key='prueba_salud_4')
        num_after, _den, out = self._detail('SG-08')
        self.assertEqual(num_after - num_before, 3)
        self.assertEqual(self.Indicator._sgi_health_concentration({1: 3, 2: 1}), (3, 4, 75.0))
        self.assertEqual(self.Indicator._sgi_health_concentration({}), (0, 0, 0.0))
        for name in (user_a.name, user_b.name):
            self.assertNotIn(name, out.get('note') or '', "La nota no da nombres.")

    def test_10_programa_de_auditoria_2031(self):
        program = self.env['sgi.audit.program'].create({'year': 2031, 'line_ids': [
            (0, 0, {'planned_month': month, 'audit_type': 'interna'}) for month in ('1', '2', '3')]})
        program.line_ids.filtered(lambda line: line.planned_month == '1').write({'state': 'cerrada'})
        out = self.ind['SG-09']._detail_salud_auditoria(date(2031, 2, 9), date(2031, 2, 15))
        self.assertEqual(out['numerator'], 1)
        self.assertEqual(out['denominator'], 2)
        self.assertEqual(out['value'], 50.0)
        self.assertIn("borrador", out['note'])
        none = self.ind['SG-09']._detail_salud_auditoria(date(2032, 1, 5), date(2032, 1, 11))
        self.assertIsNone(none['value'])

    def test_11_formatos_migrados_utilizables(self):
        sgi_hide_real_documents(self.env)
        Doc = self.env['documents.document'].with_context(sgi_skip_role_check=True)
        process = self.env['sgi.process'].create({'code': 'Z99F', 'name': 'Proceso formatos salud',
                                                  'company_id': self.company.id})
        product = self.env['product.product'].create({'name': 'Producto salud SGI', 'type': 'consu'})
        picking_type = self.env.ref('stock.picking_type_in')
        point_on = self.env['quality.point'].create({
            'title': 'Worksheet salud activo', 'picking_type_ids': [(4, picking_type.id)]})
        point_off = self.env['quality.point'].create({
            'title': 'Worksheet salud archivado', 'picking_type_ids': [(4, picking_type.id)]})
        point_off.active = False
        self.env['quality.check'].create({'point_id': point_on.id, 'product_id': product.id,
                                          'team_id': point_on.team_id.id})
        root = self.env['ir.ui.menu'].create({'name': 'Raíz salud SGI'})

        def _menu(name, model):
            action = self.env['ir.actions.act_window'].create({'name': name, 'res_model': model})
            return self.env['ir.ui.menu'].create({
                'name': name, 'parent_id': root.id,
                'action': 'ir.actions.act_window,%d' % action.id})
        menu_used = _menu('Destino salud con uso', 'res.partner')
        # «Sin uso»: un catálogo sin altas en la ventana de 90 días (en la
        # copia de producción, cualquiera de estos; se elige el primero vacío).
        start, _end = self.ind['SG-10']._sgi_health_bounds(self.until_today[1], 90)
        idle_model = next((name for name in ('res.country', 'res.country.state', 'res.currency',
                                             'res.lang', 'res.country.group')
                           if not self.env[name].with_context(active_test=False).search_count(
                               [('create_date', '>=', start)], limit=1)), None)
        if not idle_model:
            self.skipTest("Ningún catálogo sin altas en 90 días en esta base.")
        menu_idle = _menu('Destino salud sin uso', idle_model)
        # Un menú activo sin acción no lleva a ningún formulario: no es utilizable.
        menu_empty = self.env['ir.ui.menu'].create({'name': 'Destino salud sin acción',
                                                    'parent_id': root.id})
        self.env['res.partner'].create({'name': 'Contacto salud SGI'})

        def _doc(code, cls, **extra):
            vals = {'name': '%s Formato salud.pdf' % code, 'type': 'binary',
                    'sgi_is_controlled': True, 'sgi_doc_type': 'formato', 'sgi_code': code,
                    'sgi_state': 'vigente', 'sgi_process_id': process.id,
                    'sgi_migration_class': cls, 'sgi_migration_state': 'migrado'}
            vals.update(extra)
            return Doc.create(vals)
        num_before, den_before, out = self._detail('SG-10')
        note_before = out.get('note')
        _doc('F-P-Z99-91', 'b', sgi_migration_point_id=point_on.id)
        _doc('F-P-Z99-92', 'b', sgi_migration_point_id=point_off.id)
        _doc('F-P-Z99-93', 'a', sgi_migration_target='Ventas > Pedidos')
        _doc('F-P-Z99-94', 'a', sgi_odoo_menu_id=menu_used.id)
        _doc('F-P-Z99-95', 'a', sgi_odoo_menu_id=menu_idle.id)
        _doc('F-P-Z99-96', 'a', sgi_odoo_menu_id=menu_empty.id)
        num_after, den_after, out = self._detail('SG-10')
        self.assertEqual(num_after - num_before, 2)
        self.assertEqual(den_after - den_before, 6)
        self.assertEqual(out['model'], 'documents.document')
        for label, delta in (("Worksheet archivado", 1), ("Solo texto", 1),
                             ("Sin registros en 90 días", 2),
                             ("Pantalla sin registros que contar (cuenta como utilizable)", 0)):
            self.assertEqual(_note_count(out['note'], label) - _note_count(note_before, label),
                             delta, label)

    # ---- 3. no ensucian lo que miden -----------------------------------------
    def test_12_salud_no_pide_validacion_plan_ni_aviso(self):
        user = new_test_user(self.env, login='zsa_bandeja', groups=USER)
        sg01 = self.ind['SG-01']
        sg01.write({'responsible_id': user.id})
        health = self.Measure.create({'indicator_id': sg01.id, 'period_date': date(2020, 1, 6),
                                      'state': 'capturado', 'value': 0})
        self.assertEqual(health.semaphore, 'rojo')
        self.assertFalse(health.plan_required, "Un rojo de salud no pide causa y plan.")
        other = self._indicator('TST-SA12', responsible_id=user.id)
        normal = self.Measure.create({'indicator_id': other.id, 'period_date': date(2020, 1, 1),
                                      'state': 'capturado', 'value': 10})
        self.assertTrue(normal.plan_required, "Un rojo de otro indicador sí lo pide.")
        pending = self.env['sgi.my.pending']._sgi_pending_records(user)['validacion']
        self.assertIn(normal, pending)
        self.assertNotIn(health, pending, "Las mediciones de salud no se validan.")
        self.assertEqual(self.ind['SG-07']._sgi_calc_diagnose({'state': 'sin_dato', 'note': 'x'}),
                         ('ok', 'x'))
        automatic = self._indicator('TST-SA12B', calc_mode='otif_ventas')
        status, _reason = automatic._sgi_calc_diagnose({'state': 'sin_dato', 'note': 'x'})
        self.assertNotEqual(status, 'ok', "Un «sin dato» de otro indicador sí es un aviso.")
        # La revisión por la dirección no carga los rojos de salud.
        review = self.env['sgi.management.review'].create(
            {'period_from': date(2020, 1, 1), 'period_to': date(2020, 1, 31)})
        reds = review._sgi_load_red_measures()
        self.assertIn(normal, reds)
        self.assertNotIn(health, reds)
        # Una medición que nace validada también lleva su fecha.
        born = self.Measure.create({'indicator_id': other.id, 'period_date': date(2020, 2, 1),
                                    'state': 'validado', 'value': 95})
        self.assertEqual(born.sgi_validated_date, self.today)

    def test_13_tablero_salud_y_duenos(self):
        user = new_test_user(self.env, login='zsa_duenio', groups=USER)
        owner = self._employee('Dueño salud', user=user)
        owner_without_user = self._employee('Dueño salud sin usuario')
        Process = self.env['sgi.process']
        process = Process.create({'code': 'Z99S', 'name': 'Proceso salud', 'owner_id': owner.id,
                                  'company_id': self.company.id})
        lonely = Process.create({'code': 'Z99T', 'name': 'Proceso salud sin usuario',
                                 'owner_id': owner_without_user.id, 'company_id': self.company.id})
        self._activity(process, user, self.today - timedelta(days=2))
        started = time.time()
        board = self.env['sgi.direction.board'].create({})
        for code, indicator in self.ind.items():
            self.assertIn(indicator, board.health_indicator_ids, code)
            self.assertNotIn(indicator, board.indicator_ids, code)
        self.assertIn(process, board.health_process_ids)
        self.assertIn(lonely, board.health_process_ids)
        self.assertEqual(process.sgi_health_owner_user_id, user)
        self.assertGreaterEqual(process.sgi_health_overdue_count, 1)
        self.assertIsInstance(process.sgi_health_idle_days, int)
        self.assertGreater(process.sgi_health_idle_days, 0, "Sin movimiento propio en el SGI.")
        self.assertFalse(lonely.sgi_health_owner_user_id)
        self.assertEqual(lonely.sgi_health_overdue_count, 0)
        self.assertEqual(lonely.sgi_health_late_validation_count, 0)
        self.assertEqual(lonely.sgi_health_idle_days, 0, "Sin usuario: la columna va vacía.")
        self.assertLess(time.time() - started, 60, "El Tablero tarda demasiado en calcular.")
        # Dirección lo abre y lee las dos listas.
        director = sgi_set_director(self.env, login='zsa_direccion_13')
        mine = self.env['sgi.direction.board'].with_user(director).create({})
        self.assertTrue(mine.health_indicator_ids.mapped('code'))
        rows = mine.health_process_ids.mapped(
            lambda p: (p.code, p.owner_id.id, p.sgi_health_overdue_count, p.sgi_health_idle_days))
        self.assertTrue(rows)
        self.assertTrue(mine.health_indicator_ids.mapped('sgi_health_week_value'))

    # ---- 4. migración, correo y cron -------------------------------------------
    def test_14_pre_migrate_solo_liga(self):
        mig = _load_pre_migrate()
        sg10 = self.ind['SG-10']
        self.env.flush_all()
        self.env.cr.execute(
            "DELETE FROM ir_model_data WHERE module = 'quimibond_sgi' AND name = 'sgi_ind_salud_formatos'")
        self.env.registry.clear_cache()
        self.assertFalse(self.env.ref(XMLIDS['SG-10'], raise_if_not_found=False))
        self.assertEqual(mig._bind_existing(self.env.cr), 1)
        self.env.registry.clear_cache()
        self.assertEqual(self.env.ref(XMLIDS['SG-10']), sg10)
        self.assertEqual(self.Indicator.with_context(active_test=False).search_count(
            [('code', '=', 'SG-10')]), 1, "No crea indicadores.")
        self.assertEqual(mig._bind_existing(self.env.cr), 0, "Idempotente.")
        mig.migrate(self.env.cr, None)  # instalación nueva: no hace nada

    def test_15_correo_semanal_a_direccion(self):
        director = sgi_set_director(self.env, login='zsa_direccion_15')
        director.write({'email': 'salud.direccion@example.com'})
        extra = new_test_user(self.env, login='zsa_extra', groups=USER,
                              email='salud.extra@example.com')
        outsider = new_test_user(self.env, login='zsa_fuera', groups=USER,
                                 email='salud.fuera@example.com')
        self.env['ir.config_parameter'].sudo().set_param(MAIL_PARAM, '%d,x' % extra.id)
        owner_user = new_test_user(self.env, login='zsa_duenio_15', groups=USER)
        owner = self._employee('Dueño correo salud', user=owner_user)
        self.env['sgi.process'].create({'code': 'Z99M', 'name': 'Proceso correo salud',
                                        'owner_id': owner.id, 'company_id': self.company.id})
        self._employee('Empleado Sin Usuario Salud')
        Cron = self.env['sgi.cron']
        recipients = Cron._sgi_health_mail_users()
        self.assertIn(director, recipients)
        self.assertIn(extra, recipients)
        self.assertNotIn(outsider, recipients)
        # El envío se limita a los usuarios de la prueba: en la copia de
        # producción Dirección tiene miembros reales (por grupos implícitos)
        # que no deben recibir un correo de prueba.
        mine_only = director | extra
        with patch.object(type(Cron), '_sgi_health_mail_users', lambda self: mine_only):
            sent = Cron.cron_health_weekly_mail()
        self.assertEqual(set(sent), set(mine_only.ids))
        measures = self.Measure.search([('indicator_id', 'in', [i.id for i in self.ind.values()]),
                                        ('period_date', '=', self.prev_monday)])
        self.assertEqual(len(measures), 10, "Mide la semana pasada de los diez.")
        mails = self.env['mail.mail'].sudo().search([('subject', 'ilike', 'salud del sistema')])

        def addresses(mail):
            return ' '.join([mail.email_to or ''] + mail.recipient_ids.mapped('email'))
        mine = mails.filtered(lambda m: 'salud.direccion@' in addresses(m))
        self.assertTrue(mine)
        self.assertTrue(mails.filtered(lambda m: 'salud.extra@' in addresses(m)))
        self.assertFalse(mails.filtered(lambda m: 'salud.fuera@' in addresses(m)))
        body = ' '.join(mine.mapped('body_html'))
        for text in ('SG-01', 'SG-10', 'Proceso correo salud', 'Solo conteos'):
            self.assertIn(text, body)
        self.assertNotIn('Empleado Sin Usuario Salud', body)
        # Idempotente: una segunda corrida no mide de nuevo.
        with patch.object(type(Cron), '_sgi_health_mail_users', lambda self: mine_only):
            Cron.cron_health_weekly_mail()
        self.assertEqual(self.Measure.search_count([
            ('indicator_id', 'in', [i.id for i in self.ind.values()]),
            ('period_date', '=', self.prev_monday)]), 10)

    def test_16_datos_y_cron(self):
        cron = self.env.ref(CRON_XMLID)
        self.assertTrue(cron.active)
        self.assertEqual(cron.interval_type, 'weeks')
        self.assertIn('cron_health_weekly_mail', cron.code)
        template = self.env.ref(TEMPLATE_XMLID)
        self.assertEqual(template.model, 'res.users')
        process = self.env['sgi.process'].create({'code': 'Z99E', 'name': 'Proceso evidencia salud',
                                                  'company_id': self.company.id})
        measure = self.Measure.create({
            'indicator_id': self.ind['SG-01'].id, 'period_date': date(2020, 1, 13),
            'state': 'capturado', 'value': 0, 'detail_model': 'sgi.process',
            'detail_ids': str(process.id)})
        action = measure.action_view_evidence()
        self.assertEqual(action['res_model'], 'sgi.process')
        empty = self.Measure.create({'indicator_id': self.ind['SG-01'].id,
                                     'period_date': date(2020, 1, 20), 'state': 'sin_dato'})
        with self.assertRaises(UserError):
            empty.action_view_evidence()
