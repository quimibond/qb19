# -*- coding: utf-8 -*-
"""57.102.0 — Indicadores: sin dato y cálculos.

B1 «Sin dato» en pantalla; B2 las mediciones abren con «Con dato»; B3 el
recálculo diario re-mide sin dato y capturadas recientes (nunca validadas,
foto, salud, con NC, con plan ni corregidas a mano); B4 «medir desde» marca
las anteriores; B5 una manual en 0 sin nota no se captura ni se valida; B6
registro vacío; B7 TR-01, C5-02, C2-06 y RH-02.

La base del build es copia de producción: todo se crea aquí (claves ZS02-*,
la compañía «ZS02 Cía» como compañía de los KPI para las fórmulas). Ninguna
prueba recalcula sin acotar a sus propios indicadores."""
import importlib.util
import os
from datetime import date
from unittest.mock import patch

from dateutil.relativedelta import relativedelta

from odoo.exceptions import UserError
from odoo.tests import TransactionCase, tagged

from odoo.addons.quimibond_sgi.models import sgi_format_map

from .common_users import sgi_set_mast

_MODULE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_POST = os.path.join(_MODULE_DIR, 'migrations', '19.0.57.102.0', 'post-migrate.py')


@tagged('post_install', '-at_install')
class TestIndicadoresSinDato(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.Indicator = cls.env['sgi.indicator']
        cls.Measure = cls.env['sgi.indicator.measure']
        cls.mast = sgi_set_mast(cls.env, login='sgi_mast_s02')
        cls.company = cls.env['res.company'].create({'name': 'ZS02 Cía'})
        # El contacto de la compañía nueva no cuenta como registro de la fuente
        # (según la versión, Odoo le pone o no la compañía).
        cls.company.partner_id.company_id = False
        cls.env['ir.config_parameter'].sudo().set_param(
            'quimibond_sgi.kpi_company_id', cls.company.id)
        cls.partner_model = cls.env['ir.model']._get('res.partner')
        today = date.today()
        cls.this_month = today.replace(day=1)
        cls.last_month = cls.this_month - relativedelta(months=1)
        cls.old_month = cls.this_month - relativedelta(months=12)

    # ---- ayudantes ----------------------------------------------------------
    def _manual(self, code, **vals):
        base = {'code': code, 'name': 'Indicador %s' % code, 'calc_mode': 'manual',
                'direction': 'higher_better', 'target_objective': 90.0,
                'target_acceptable': 80.0, 'uom': '%', 'responsible_id': self.mast.id}
        base.update(vals)
        return self.Indicator.create(base)

    def _configurable(self, code, direction='lower_better', aggregation='count',
                      field_name=False, domain="[('ref', '=', 'ZS02')]"):
        lower = direction == 'lower_better'
        ind = self.Indicator.create({
            'code': code, 'name': 'Fórmula %s' % code, 'calc_mode': 'configurable',
            'direction': direction, 'target_objective': 0.0 if lower else 90.0,
            'target_acceptable': 1.0 if lower else 80.0,
            'uom': 'casos', 'responsible_id': self.mast.id})
        self.env['sgi.indicator.term'].create({
            'indicator_id': ind.id, 'role': 'numerator', 'model_id': self.partner_model.id,
            'domain': domain, 'date_field': 'create_date', 'aggregation': aggregation,
            'field_name': field_name, 'window': 'period'})
        return ind

    def _measure(self, ind, period, value=0.0, state='capturado', **vals):
        return self.Measure.create(dict(
            {'indicator_id': ind.id, 'period_date': period, 'value': value, 'state': state}, **vals))

    def _partner(self, **vals):
        return self.env['res.partner'].create(dict(
            {'name': 'Socio ZS02', 'company_id': self.company.id, 'ref': 'ZS02'}, **vals))

    def _last_month_vals(self, ind):
        return ind._sgi_measure_vals(self.last_month, self.this_month - relativedelta(days=1))

    # ---- B1 ------------------------------------------------------------------
    def test_01_etiqueta_sin_dato_y_cero_real(self):
        ind = self._manual('ZS02-01')
        self._measure(ind, date(2046, 1, 1), state='sin_dato')
        self.assertEqual(ind.last_value, 0.0)
        self.assertEqual(ind.sgi_last_value_label, "Sin dato")
        self._measure(ind, date(2046, 2, 1), value=0.0, note="0: sin caídas en el mes")
        ind.invalidate_recordset()
        self.assertEqual(ind.sgi_last_value_label, "0 %", "Un 0 con dato sí se muestra.")
        self._measure(ind, date(2046, 3, 1), value=95.25)
        ind.invalidate_recordset()
        self.assertEqual(ind.sgi_last_value_label, "95.25 %")
        self.assertEqual(ind._sgi_value_text(-0.0), "0 %", "Nunca «-0».")

    def test_02_diagrama_tablero_y_revision(self):
        empty = self._manual('ZS02-02A')
        zero = self._manual('ZS02-02B')
        self._measure(zero, date(2046, 1, 1), value=0.0, note="0 real")
        data = self.env['sgi.diagram']._data_kpi_tree()
        items = {i['res_id']: i for lane in data['lanes'] if lane['key'] == 'indicators'
                 for i in lane['items']}
        self.assertIn("Último: Sin dato", items[empty.id]['subtitle'])
        self.assertIn("Último: 0 %", items[zero.id]['subtitle'], "Un 0 real no es «—».")
        self._measure(empty, date(2046, 2, 1), state='sin_dato')
        board = self.env['sgi.indicator'].browse(empty.id)
        self.assertIn("sin dato", board.last_six)

    def test_03_vistas_usan_la_etiqueta(self):
        for xmlid in ('quimibond_sgi.sgi_indicator_view_list',
                      'quimibond_sgi.sgi_indicator_view_list_mine',
                      'quimibond_sgi.sgi_indicator_view_kanban_mine'):
            arch = self.env.ref(xmlid).arch_db
            self.assertIn('sgi_last_value_label', arch, xmlid)
        arch = self.env.ref('quimibond_sgi.sgi_measure_view_list').arch_db
        self.assertIn("state in ('pendiente', 'sin_dato')", arch)

    # ---- B2 ------------------------------------------------------------------
    def test_04_mediciones_abren_con_dato(self):
        ind = self._manual('ZS02-04')
        self.assertEqual(ind.action_sgi_measures()['context'].get('search_default_con_dato'), 1)
        ctx = self.env.ref('quimibond_sgi.sgi_measure_action').context
        self.assertIn('search_default_con_dato', ctx)
        self.assertNotIn('search_default_pending', ctx, "Pendientes y Con dato juntos dan vacío.")

    # ---- B5 ------------------------------------------------------------------
    def test_05_manual_en_cero_sin_nota_no_se_captura(self):
        ind = self._manual('ZS02-05')
        pending = self._measure(ind, date(2046, 1, 1), state='pendiente')
        with self.assertRaises(UserError):
            pending.with_user(self.mast).action_capture()
        pending.with_user(self.mast).write({'note': "0: Odoo.sh no reportó caídas"})
        pending.with_user(self.mast).action_capture()
        self.assertEqual(pending.state, 'capturado')
        other = self._measure(ind, date(2046, 2, 1), state='capturado')  # la crea el sistema
        with self.assertRaises(UserError):
            other.with_user(self.mast).action_validate()
        self.assertEqual(other.state, 'capturado')
        other.with_user(self.mast).write({'value': 99.5})
        other.with_user(self.mast).action_validate()
        self.assertEqual(other.state, 'validado')
        self.assertTrue(other.sgi_targets_frozen, "K-04 sigue guardando las metas.")

    def test_05b_automatico_en_cero_si_se_valida(self):
        ind = self._configurable('ZS02-05B', direction='higher_better')
        m = self._measure(ind, date(2046, 1, 1), value=0.0, numerator=0.0, denominator=4.0)
        m.with_user(self.mast).action_validate()
        self.assertEqual(m.state, 'validado', "B5 solo aplica a indicadores manuales.")

    # ---- B4 ------------------------------------------------------------------
    def test_06_medir_desde_marca_las_anteriores(self):
        ind = self._manual('ZS02-06')
        before = self._measure(ind, date(2045, 11, 1), value=12.0, note="nota previa")
        validated = self._measure(ind, date(2045, 12, 1), value=50.0)
        validated.with_user(self.mast).action_validate()
        after = self._measure(ind, date(2046, 1, 1), value=70.0)
        ind.write({'measure_from': date(2046, 1, 1)})
        self.assertEqual(before.state, 'sin_dato')
        self.assertIn("Antes de «Medir desde» (01/01/2046)", before.note)
        self.assertIn("Valor anterior 12.0", before.note)
        self.assertIn("nota previa", before.note, "La nota anterior no se pierde.")
        self.assertEqual(validated.state, 'validado', "La validada es evidencia: no se toca.")
        self.assertEqual(after.state, 'capturado')
        self.assertTrue(before.exists(), "Nada se borra.")
        self.assertFalse(ind._sgi_mark_before_measure_from(), "Idempotente.")

    # ---- B6 ------------------------------------------------------------------
    def test_07_registro_vacio_da_sin_dato(self):
        Partner = self.env['res.partner'].with_context(active_test=False)
        self.assertFalse(Partner.search_count([('company_id', '=', self.company.id),
                                               ('id', '!=', self.company.partner_id.id)]))
        ind = self._configurable('ZS02-07')
        vals = self._last_month_vals(ind)
        self.assertEqual(vals['state'], 'sin_dato')
        self.assertIn("Registro vacío", vals['note'])
        self._partner(ref='otro')  # el modelo ya tiene registros en la compañía
        vals = self._last_month_vals(ind)
        self.assertEqual(vals['state'], 'capturado')
        self.assertEqual(vals['value'], 0.0, "Con registros en la fuente, el 0 es real.")

    def test_07b_registro_vacio_solo_mas_bajo_es_mejor(self):
        ind = self._configurable('ZS02-07B', direction='higher_better')
        vals = self._last_month_vals(ind)
        self.assertEqual(vals['state'], 'capturado', "Un 0 rojo es honesto: no se oculta.")

    def test_07c_campo_sumado_nunca_capturado(self):
        if 'partner_latitude' not in self.env['res.partner']._fields:
            self.skipTest("res.partner sin partner_latitude")
        ind = self._configurable('ZS02-07C', aggregation='sum', field_name='partner_latitude')
        self._partner(partner_latitude=0.0)
        end = self.this_month + relativedelta(months=1, days=-1)
        vals = ind._sgi_measure_vals(self.this_month, end)
        self.assertEqual(vals['state'], 'sin_dato')
        self._partner(partner_latitude=1.5)
        vals = ind._sgi_measure_vals(self.this_month, end)
        self.assertEqual(vals['state'], 'capturado')

    # ---- B3 ------------------------------------------------------------------
    def test_08_recalculo_reciente(self):
        ind = self._configurable('ZS02-08', direction='higher_better')
        self._partner()
        stale = self._measure(ind, self.this_month, value=0.0, numerator=0.0)
        empty = self._measure(ind, self.last_month, state='sin_dato')
        old = self._measure(ind, self.old_month, value=0.0, numerator=0.0)
        Config = self.env['sgi.config'].sudo()
        result = Config.recompute_pending_measures(indicators=ind, recent=True)
        self.assertEqual(stale.value, 1.0, "La capturada reciente se re-mide.")
        self.assertEqual(old.value, 0.0, "Fuera de la ventana no se toca.")
        self.assertIn(empty.state, ('capturado', 'sin_dato'))
        self.assertGreaterEqual(result['recalculadas'], 1)
        self.assertTrue(any('Recalculada por el SGI' in (b or '')
                            for b in stale.message_ids.mapped('body')))
        result = Config.recompute_pending_measures(indicators=ind, recent=True)
        self.assertEqual(result['recalculadas'], 0, "Sin cambios no escribe.")
        Config.recompute_pending_measures(indicators=ind, recent='all')
        self.assertEqual(old.value, 0.0, "En su mes no hay socios: sigue en 0.")

    def test_09_recalculo_respeta(self):
        ind = self._configurable('ZS02-09', direction='higher_better')
        self._partner()
        by_hand = self._measure(ind, self.this_month, value=0.0)
        by_hand.with_user(self.mast).write({'value': 7.0})
        self.assertTrue(by_hand.sgi_value_by_hand)
        ind2 = self._configurable('ZS02-09B', direction='higher_better')
        validated = self._measure(ind2, self.this_month, value=0.0, numerator=0.0, denominator=1.0)
        validated.with_user(self.mast).action_validate()
        self.env['sgi.config'].sudo().recompute_pending_measures(indicators=ind | ind2, recent=True)
        self.assertEqual(by_hand.value, 7.0, "Corregida a mano: no se toca.")
        self.assertEqual(validated.value, 0.0, "Validada: no se toca.")
        by_hand.action_recompute_value()
        self.assertFalse(by_hand.sgi_value_by_hand, "«Recalcular valor» quita la marca.")
        self.assertEqual(by_hand.value, 1.0)

    def test_09b_recalculo_no_toca_salud_ni_foto(self):
        domain = self.env['sgi.config'].sudo()._sgi_recompute_domain(recent=True)
        self.assertIn(('indicator_id.snapshot', '=', False), domain)
        self.assertTrue(any(leaf[0] == 'indicator_id.calc_mode' and leaf[1] == 'not in'
                            for leaf in domain if isinstance(leaf, tuple)))
        self.assertIn(('sgi_value_by_hand', '=', False), domain)
        self.assertIn(('alert_id', '=', False), domain)

    # ---- B7 ------------------------------------------------------------------
    def _alert_term(self, ind, domain, date_field):
        return self.env['sgi.indicator.term'].create({
            'indicator_id': ind.id, 'role': 'numerator',
            'model_id': self.env['ir.model']._get('quality.alert').id,
            'domain': domain, 'date_field': date_field, 'aggregation': 'count', 'window': 'period'})

    def test_10_correccion_tr01_c502_con_candado(self):
        Indicator = self.Indicator
        tr = Indicator.create({'code': 'ZS02-TR', 'name': 'TR prueba', 'calc_mode': 'configurable'})
        old = Indicator._FIXES_57102['TR-01'][0]
        term = self._alert_term(tr, old[1], old[2])
        other = Indicator.create({'code': 'ZS02-TR2', 'name': 'TR distinto',
                                  'calc_mode': 'configurable'})
        term2 = self._alert_term(other, "[('sgi_folio', '!=', False)]", 'date_close')
        res = Indicator._sgi_formula_fixes_57102(codes={'TR-01': tr | other})
        self.assertEqual(term.date_field, 'create_date')
        self.assertIn("('date_close', '!=', False)", term.domain)
        self.assertEqual(term2.date_field, 'date_close', "Un término distinto no se toca.")
        self.assertEqual(res[tr.id], 'corregido')
        self.assertEqual(res[other.id], 'distinto')
        self.assertTrue(any('Antes: Numerador' in (b or '') for b in tr.message_ids.mapped('body')),
                        "El antes y el después quedan en el chatter.")
        res = Indicator._sgi_formula_fixes_57102(codes={'TR-01': tr})
        self.assertEqual(res[tr.id], 'sin cambio', "Idempotente.")

    def test_11_capacitacion_con_detalle_y_empresa(self):
        stype = self.env['hr.skill.type'].create({'name': 'Certificación ZS02'})
        level = self.env['hr.skill.level'].create({
            'skill_type_id': stype.id, 'name': 'Vigente', 'level_progress': 100})
        skill = self.env['hr.skill'].create({'name': 'Norma ZS02', 'skill_type_id': stype.id})
        # Sin compañía: lo ocupan empleados de las dos compañías.
        job = self.env['hr.job'].create({'name': 'Puesto ZS02', 'company_id': False})
        self.env['hr.job.skill'].create({'job_id': job.id, 'skill_id': skill.id,
                                         'skill_type_id': stype.id, 'skill_level_id': level.id})
        sgi_company = self.env['sgi.config']._sgi_company()
        self.env['hr.employee'].create({'name': 'Empleado ZS02', 'job_id': job.id,
                                        'company_id': sgi_company.id})
        self.env['hr.employee'].create({'name': 'Empleado ZS02 otra cía', 'job_id': job.id,
                                        'company_id': self.company.id})
        ind = self.Indicator.create({'code': 'ZS02-11', 'name': 'RH-02 prueba',
                                     'calc_mode': 'capacitacion'})
        detail = ind._detail_capacitacion(self.last_month, self.this_month)
        self.assertTrue(detail['denominator'])
        self.assertGreaterEqual(detail['numerator'], 0.0)
        self.assertLessEqual(detail['numerator'], detail['denominator'])
        employees = ind._sgi_capacitacion_employees()
        self.assertNotIn(self.company, employees.company_id, "Solo la empresa del SGI.")

    # ---- Revisión final ----------------------------------------------------
    def test_13_tiempo_tope_detiene_limpio(self):
        ind = self._configurable('ZS02-13', direction='higher_better')
        first = self._measure(ind, self.last_month, state='sin_dato')
        second = self._measure(ind, self.this_month, state='sin_dato')
        Param = self.env['ir.config_parameter'].sudo()
        Param.set_param(sgi_format_map.RECOMPUTE_CURSOR_PARAM, '0')
        # inicio, revisión antes de la 1.ª (a tiempo), antes de la 2.ª (se acabó)
        with patch.object(sgi_format_map, '_now', side_effect=[0, 0, 10000, 10000]):
            result = self.env['sgi.config'].sudo().recompute_pending_measures(
                indicators=ind, recent=True)
        self.assertEqual(result['sin_tiempo'], 1)
        self.assertEqual(result['errores'], 0)
        self.assertEqual(first.state, 'capturado', "La primera sí se midió.")
        self.assertEqual(second.state, 'sin_dato', "La segunda queda para mañana.")
        self.assertEqual(Param.get_param(sgi_format_map.RECOMPUTE_CURSOR_PARAM), str(first.id))
        result = self.env['sgi.config'].sudo().recompute_pending_measures(
            indicators=ind, recent=True)
        self.assertEqual(result['sin_tiempo'], 0)
        self.assertEqual(second.state, 'capturado', "La siguiente corrida sigue donde se quedó.")
        self.assertEqual(Param.get_param(sgi_format_map.RECOMPUTE_CURSOR_PARAM), '0')

    def test_14_recalculo_conserva_la_nota_de_una_persona(self):
        ind = self._configurable('ZS02-14', direction='higher_better')
        self._partner()
        human = self._measure(ind, self.this_month, value=0.0, numerator=0.0)
        human.with_user(self.mast).write({'note': "Revisado con Logística"})
        system = self._measure(ind, self.last_month, state='sin_dato',
                               note="Registro vacío: nota vieja del cálculo")
        self.env.flush_all()
        self.env.cr.precommit.run()  # el seguimiento de la nota
        self.env['sgi.config'].sudo().recompute_pending_measures(indicators=ind, recent='all')
        self.assertEqual(human.value, 1.0)
        self.assertIn("Revisado con Logística", human.note or '')
        self.assertFalse(system.note, "La nota que escribió el cálculo se reemplaza.")

    def test_15_validar_seleccionadas_salta_manuales_sin_valor(self):
        ind = self._manual('ZS02-15')
        empty = self._measure(ind, date(2046, 1, 1))
        full = self._measure(ind, date(2046, 2, 1), value=95.0)
        Pending = self.env['sgi.my.pending'].with_user(self.mast)
        rows = Pending.create([{'kind': 'validacion', 'name': 'Validar %s' % m.id,
                                'res_model': 'sgi.indicator.measure', 'res_id': m.id,
                                'user_id': self.mast.id} for m in (empty, full)])
        result = rows.action_validate_selected()
        self.assertEqual(result['tag'], 'display_notification')
        self.assertIn("no tienen valor capturado", result['params']['message'])
        self.assertEqual(full.state, 'validado')
        self.assertEqual(empty.state, 'capturado')
        self.assertTrue(rows.filtered(lambda r: r.res_id == empty.id).exists(),
                        "La que no se validó se queda en la lista.")

    # ---- Migración --------------------------------------------------------
    def test_12_post_migrate_existe(self):
        """Solo se comprueba que el archivo existe y define ``migrate``: correrlo
        aquí tocaría S6-02, TR-01, C5-02 y C2-06 reales de la copia."""
        self.assertTrue(os.path.exists(_POST), _POST)
        spec = importlib.util.spec_from_file_location('sgi_mig_57_102_0', _POST)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        self.assertTrue(callable(getattr(module, 'migrate', None)))
