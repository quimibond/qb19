# -*- coding: utf-8 -*-
"""Entrega 8a, línea «bandeja» (auditoría 2026-09, D-04): Mis pendientes con
todo adentro. Una prueba por tipo de pendiente (J-007), más G-001, G-017,
I-007, I-012 e I-021."""
import io
from datetime import date, timedelta

from dateutil.relativedelta import relativedelta

from odoo import fields
from odoo.exceptions import UserError
from odoo.tests import TransactionCase, new_test_user, tagged

from ..models.sgi_calendar import sgi_add_business_days
from .common_documents import sgi_hide_real_documents


def _blank_pdf():
    from odoo.tools.pdf import PdfFileWriter
    writer = PdfFileWriter()
    writer.add_blank_page(612, 792)
    out = io.BytesIO()
    writer.write(out)
    return out.getvalue()


@tagged('post_install', '-at_install')
class TestBandeja(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        env = cls.env
        sgi_hide_real_documents(env)
        cls.today = fields.Date.context_today(env.user)
        cls.Pending = env['sgi.my.pending']
        cls.job = env['hr.job'].create({'name': 'PUESTO BANDEJA 8A'})
        cls.boss_job = env['hr.job'].create({'name': 'DIRECCION BANDEJA 8A'})
        cls.approver_job = env['hr.job'].create({'name': 'APRUEBA BANDEJA 8A'})
        cls.process = env['sgi.process'].create({'code': 'Z8A', 'name': 'Proceso bandeja 8A'})
        # Actividad mensual que vence el día hábil 1, la aprueba otro puesto y
        # escala a Dirección al día hábil siguiente.
        cls.activity = env['sgi.process.activity'].create({
            'process_id': cls.process.id, 'name': 'Cerrar la orden 8A', 'number': '8.1',
            'measure_cadence': 'mensual', 'due_business_day': 1,
            'role_ids': [(0, 0, {'role': 'ejecuta', 'job_id': cls.job.id}),
                         (0, 0, {'role': 'aprueba', 'job_id': cls.approver_job.id}),
                         (0, 0, {'role': 'escala', 'job_id': cls.boss_job.id, 'after_days': 1})]})
        cls.user = new_test_user(env, login='zs_8a_user', email='zs.8a@example.com',
                                 groups='base.group_user,quimibond_sgi.group_sgi_user')
        cls.boss_user = new_test_user(env, login='zs_8a_boss', email='zs.8a.boss@example.com',
                                      groups='base.group_user,quimibond_sgi.group_sgi_user')
        cls.approver_user = new_test_user(env, login='zs_8a_approver', email='zs.8a.approver@example.com',
                                          groups='base.group_user,quimibond_sgi.group_sgi_user')
        cls.approver = env['hr.employee'].create({
            'name': 'ZS Aprueba 8A', 'job_id': cls.approver_job.id, 'user_id': cls.approver_user.id})
        cls.emp = env['hr.employee'].create({
            'name': 'ZS Persona 8A', 'job_id': cls.job.id, 'user_id': cls.user.id})
        cls.boss = env['hr.employee'].create({
            'name': 'ZS Dirección 8A', 'job_id': cls.boss_job.id, 'user_id': cls.boss_user.id})
        cls.emp.parent_id = cls.boss
        # I-021: gente de planta sin usuario, a cargo del mismo jefe.
        cls.floor = env['hr.employee'].create({
            'name': 'ZS Planta 8A', 'job_id': cls.job.id, 'parent_id': cls.boss.id})
        (cls.emp | cls.boss | cls.floor | cls.approver).invalidate_recordset()

    # ------------------------------------------------------------------
    def _rows(self, user=None, kind=None):
        user = user or self.user
        rows = self.Pending._sgi_pending_values(user)[user.id]
        return [r for r in rows if kind is None or r['kind'] == kind]

    def _row(self, kind, res_id, user=None):
        found = [r for r in self._rows(user, kind) if r['res_id'] == res_id]
        return found[0] if found else None

    def _late(self):
        self.activity.sudo().write({'measure_state': 'rojo'})
        self.activity.invalidate_recordset()

    def _indicator(self, code, calc_mode='manual', frequency='monthly'):
        return self.env['sgi.indicator'].create({
            'code': code, 'name': 'KPI %s' % code, 'calc_mode': calc_mode,
            'frequency': frequency, 'responsible_id': self.user.id,
            'process_id': self.process.id})

    # ---- accion -------------------------------------------------------------
    def test_01_accion(self):
        objective = self.env['sgi.objective'].create({'name': 'Objetivo 8A'})
        line = self.env['sgi.action.line'].create({
            'name': 'Acción 8A', 'responsible_id': self.user.id,
            'date_commit': self.today - timedelta(days=2), 'objective_id': objective.id})
        row = self._row('accion', line.id)
        self.assertTrue(row)
        self.assertEqual(row['state'], 'atrasada')

    # ---- nc -----------------------------------------------------------------
    def test_02_nc(self):
        Alert = self.env['quality.alert']
        if 'sgi_responsible_ids' not in Alert._fields:
            self.skipTest("Sin responsables de NC en este modelo.")
        alert = Alert.create({'name': 'ZS NC 8A', 'sgi_process_id': self.process.id,
                              'sgi_responsible_ids': [(6, 0, self.user.ids)]})
        self.assertTrue(self._row('nc', alert.id))
        self.process.active = False
        self.assertFalse(self._row('nc', alert.id), "Nada de procesos archivados.")

    # ---- medicion: «Capturar» (G-001) ---------------------------------------
    def test_03_capturar_no_nace_atrasada(self):
        indicator = self._indicator('Z8A-M')
        period = self.today.replace(day=1) - relativedelta(months=1)
        measure = self.env['sgi.indicator.measure'].create({
            'indicator_id': indicator.id, 'period_date': period})
        row = self._row('medicion', measure.id)
        self.assertTrue(row, "Un indicador manual pendiente se captura.")
        self.assertTrue(row['name'].startswith("Capturar Z8A-M"))
        self.assertEqual(row['date_due'], measure._sgi_capture_due())
        self.assertGreater(row['date_due'], measure._sgi_run_day(),
                           "Vence después del día en que se mide, no al cierre del periodo.")
        self.assertGreater(row['date_due'], period + relativedelta(day=31))
        # Un indicador automático que sí calcula no se le pide a nadie.
        auto = self._indicator('Z8A-A', calc_mode='otif_ventas')
        auto.sudo().write({'calc_status': 'ok'})
        pending = self.env['sgi.indicator.measure'].create({
            'indicator_id': auto.id, 'period_date': period})
        self.assertFalse(self._row('medicion', pending.id))
        auto.sudo().write({'calc_status': 'error'})
        self.assertTrue(self._row('medicion', pending.id), "Si el cálculo falla, se captura a mano.")

    def test_03b_capturar_cinco_habiles_y_mismo_plazo_en_el_aviso(self):
        """56.38.1 (decisión de Jose): capturar vence 5 días hábiles después del
        día en que se mide, y el aviso «Capturar indicador» del cron vence el
        mismo día que el renglón de Mis pendientes (una sola regla)."""
        self.env['ir.config_parameter'].sudo().set_param('quimibond_sgi.measure_capture_business_days', '')
        indicator = self._indicator('Z8A-C5')
        period = self.today.replace(day=1) - relativedelta(months=1)
        last = period + relativedelta(day=31)
        self.env['sgi.cron']._sgi_generate_measures(
            indicator, period, period, last, period, period.strftime('%m/%Y'))
        measure = self.env['sgi.indicator.measure'].search(
            [('indicator_id', '=', indicator.id), ('period_date', '=', period)])
        self.assertEqual(measure.state, 'pendiente')
        due = measure._sgi_capture_due()
        self.assertEqual(due, sgi_add_business_days(self.env, measure._sgi_run_day(), 5))
        self.assertEqual(self._row('medicion', measure.id)['date_due'], due)
        notice = self.env['mail.activity'].search([
            ('res_model', '=', 'sgi.indicator'), ('res_id', '=', indicator.id),
            ('summary', '=like', 'Capturar indicador Z8A-C5%')])
        self.assertEqual(len(notice), 1)
        self.assertEqual(notice.date_deadline, due, "El aviso no trae su propio plazo.")
        # Validar sigue en 3 días hábiles.
        measure.write({'value': 1.0, 'state': 'capturado'})
        self.assertEqual(measure._sgi_validate_due(), sgi_add_business_days(self.env, self.today, 3))

    # ---- validacion: «Validar» en 3 días hábiles (I-006, I-007) --------------
    def test_04_validar_dueño_tres_dias_habiles(self):
        indicator = self._indicator('Z8A-W', calc_mode='otif_ventas', frequency='weekly')
        monday = self.today - timedelta(days=self.today.weekday() + 7)
        measure = self.env['sgi.indicator.measure'].create({
            'indicator_id': indicator.id, 'period_date': monday, 'state': 'capturado', 'value': 95.0})
        self.assertEqual(measure.captured_date, self.today)
        row = self._row('validacion', measure.id)
        self.assertTrue(row)
        self.assertTrue(row['name'].startswith("Validar Z8A-W"))
        self.assertIn("semana del %s" % monday.strftime('%d/%m/%Y'), row['name'])
        self.assertEqual(row['date_due'], measure._sgi_validate_due())
        self.assertGreater(row['date_due'], self.today)
        self.assertNotEqual(row['state'], 'atrasada', "Recién calculada no nace atrasada.")
        self.assertFalse(self._row('medicion', measure.id), "Ya no dice «Medir».")
        # El dueño valida desde el renglón.
        rec = self.Pending.with_user(self.user)._sgi_build(self.emp)
        line = rec.filtered(lambda r: r.kind == 'validacion' and r.res_id == measure.id)
        line.with_user(self.user).action_validate_measure()
        self.assertEqual(measure.state, 'validado')
        other = self.Pending.create({'kind': 'accion', 'name': 'No es medición'})
        with self.assertRaises(UserError):
            other.action_validate_measure()

    # ---- legal --------------------------------------------------------------
    def test_05_legal(self):
        req = self.env['sgi.legal.requirement'].create({
            'name': 'Requisito 8A', 'system': 'ambiental', 'responsible_id': self.user.id,
            'next_eval_date': self.today + timedelta(days=3)})
        row = self._row('legal', req.id)
        self.assertTrue(row)
        self.assertEqual(row['state'], 'por_vencer')

    # ---- documento (I-007: sin la clave vieja) -------------------------------
    def test_06_documento_titulo_limpio(self):
        doc = self.env['documents.document'].create({
            'name': 'Procedimiento de bandeja', 'type': 'binary', 'sgi_is_controlled': True,
            'sgi_doc_type': 'procedimiento', 'sgi_code': 'P-A68', 'sgi_state': 'vigente',
            'sgi_owner_id': self.user.id, 'sgi_next_review_date': self.today + timedelta(days=20)})
        row = self._row('documento', doc.id)
        self.assertTrue(row)
        self.assertNotIn('P-A68', row['name'])
        self.assertIn('Procedimiento de bandeja', row['name'])

    # ---- aprobacion (Studio): test_07 se mudó a quimibond_sgi_studio
    # (tests/test_approval_studio.py, test_06) en 57.9.0 (A-010, J-018).

    # ---- solicitud (Aprobaciones) ---------------------------------------------
    def test_08_solicitud(self):
        category = self.env['approval.category'].create({
            'name': 'ZS Categoría 8A', 'approval_minimum': 1,
            'approver_ids': [(0, 0, {'user_id': self.user.id, 'required': True})]})
        request = self.env['approval.request'].create({
            'name': 'ZS solicitud 8A', 'category_id': category.id,
            'request_owner_id': self.env.user.id})
        request.action_confirm()
        self.assertTrue(self._row('solicitud', request.id))

    # ---- actividad atrasada (I-001) y semáforo de Mi procedimiento ------------
    def test_09_actividad_atrasada(self):
        self.assertFalse(self._rows(kind='actividad'), "Al día: no sale.")
        self._late()
        rows = self._rows(kind='actividad')
        self.assertEqual([r['res_id'] for r in rows], [self.activity.id])
        self.assertEqual(rows[0]['state'], 'atrasada')
        self.assertTrue(rows[0]['name'].startswith("Hacer"))
        self.assertIn('Cerrar la orden 8A', rows[0]['name'])
        role = self.activity.role_ids.filtered(lambda r: r.role == 'ejecuta')
        self.assertEqual(role.mp_status, 'atrasada', "Mismo semáforo en Mi procedimiento.")
        # La pantalla ya no dice «Estás al día».
        Wiz = self.env['sgi.my.procedure'].with_user(self.user)
        wiz = Wiz.browse(Wiz.action_open_mine()['res_id'])
        self.assertGreaterEqual(wiz.pending_late, 1)
        # Proceso archivado: fuera.
        self.process.active = False
        self.assertFalse(self._rows(kind='actividad'))

    # ---- escalamiento (I-012) --------------------------------------------------
    def test_10_escalamiento_llega_a_quien_escala(self):
        self.assertFalse(self._rows(self.boss_user, 'actividad'))
        self._late()
        rows = self._rows(self.boss_user, 'actividad')
        self.assertEqual([r['res_id'] for r in rows], [self.activity.id])
        self.assertTrue(rows[0]['name'].startswith("Escalamiento:"))
        self.assertLessEqual(rows[0]['date_due'], self.today)
        # La pestaña de escalamientos arranca en los atrasados.
        Wiz = self.env['sgi.my.procedure'].with_user(self.boss_user)
        wiz = Wiz.browse(Wiz.action_open_mine()['res_id'])
        self.assertEqual(wiz.received_late_count, 1)
        self.assertEqual(wiz.action_show_received()['name'], "Escalamientos atrasados")

    # ---- aprobador y escalador ante el atraso del ejecutor (56.38.1) ----------
    def test_10b_aprobador_no_ve_el_atraso_del_ejecutor(self):
        """Decisión de Jose: el aprobador solo ve lo que ya le toca. Una
        actividad atrasada sin hacer es del ejecutor y, pasados sus días, de
        quien escala; no del que aprueba."""
        self._late()
        self.assertTrue(self.approver.sgi_mp_role_ids.filtered(lambda r: r.role == 'aprueba'),
                        "El puesto sí aprueba la actividad.")
        # El ejecutor sí la ve.
        mine = self._rows(kind='actividad')
        self.assertEqual([r['res_id'] for r in mine], [self.activity.id])
        self.assertTrue(mine[0]['name'].startswith("Hacer"))
        # El aprobador no: nada por aprobar todavía.
        self.assertFalse(self._rows(self.approver_user, 'actividad'))
        self.assertFalse([r for r in self._rows(self.approver_user) if r['res_id'] == self.activity.id
                          and r['res_model'] == 'sgi.process.activity'])
        # Mi equipo (por persona) tampoco se lo cuenta.
        summary = self.Pending._sgi_summary_employees(self.approver)
        self.assertEqual(summary[self.approver.id][1], 0, "Sin atrasos para el aprobador.")

    def test_10c_escalador_solo_pasados_sus_dias(self):
        self._late()
        escala = self.activity.role_ids.filtered(lambda r: r.role == 'escala')
        escala.after_days = 60
        self.boss.invalidate_recordset()
        self.assertFalse(self._rows(self.boss_user, 'actividad'),
                         "Antes de sus días hábiles no le llega a quien escala.")
        self.assertTrue(self._rows(kind='actividad'), "Al ejecutor sí.")
        escala.after_days = 1
        self.boss.invalidate_recordset()
        rows = self._rows(self.boss_user, 'actividad')
        self.assertEqual([r['res_id'] for r in rows], [self.activity.id])
        self.assertTrue(rows[0]['name'].startswith("Escalamiento:"))
        self.assertFalse(self._rows(self.approver_user, 'actividad'))

    # ---- acuse de lectura (I-001) -----------------------------------------------
    def test_11_acuse(self):
        doc = self.env['documents.document'].create({
            'name': 'Instructivo de bandeja', 'type': 'binary', 'sgi_is_controlled': True,
            'sgi_doc_type': 'formato', 'sgi_code': 'F-P-A95-01', 'sgi_state': 'vigente'})
        ack = self.env['sgi.document.ack'].create({'document_id': doc.id, 'employee_id': self.emp.id})
        row = self._row('acuse', ack.id)
        self.assertTrue(row)
        self.assertTrue(row['name'].startswith("Leer y firmar"))
        self.assertGreater(row['date_due'], self.today)
        ack.with_user(self.user).action_mark_read()
        self.assertFalse(self._row('acuse', ack.id))

    # ---- firma (Firma electrónica, D-04) -----------------------------------------
    def test_12_firma(self):
        role = self.env['sign.item.role'].search([], limit=1)
        if not role:
            self.skipTest("Base sin papeles de Sign.")
        Builder = self.env['sgi.sign.builder']
        template = Builder._sgi_template('Plantilla 8A', _blank_pdf(), 1, [(0, role)])
        product = self.env['product.template'].create({'name': 'Tela 8A'})
        request = Builder._sgi_request(template, 'Firma 8A', 'Firma 8A', product,
                                       [(self.user.partner_id, role)])
        items = request.request_item_ids
        row = self._row('firma', items.id)
        self.assertTrue(row)
        self.assertEqual(row['name'], "Firmar Firma 8A")
        self.assertTrue(row['date_due'])
        # La liga con token solo es del firmante: el jefe (Mi equipo) no la recibe.
        vals = {'kind': 'firma', 'res_model': 'sign.request.item', 'res_id': items.id}
        Pending = self.env['sgi.my.pending']
        self.assertNotEqual(Pending.with_user(self.boss_user).new(vals).action_open().get('type'),
                            'ir.actions.act_url')
        if items.access_token:
            self.assertEqual(Pending.with_user(self.user).new(vals).action_open().get('type'),
                             'ir.actions.act_url')

    # ---- I-021: semáforo de Mi equipo sin usuario ----------------------------------
    def test_13_mi_equipo_sin_usuario(self):
        Public = self.env['hr.employee.public'].with_user(self.boss_user)
        self.assertFalse(Public.browse(self.floor.id).sgi_mp_pending_state)
        self._late()
        Public.invalidate_model()
        row = Public.browse(self.floor.id)
        self.assertEqual(row.sgi_mp_pending_state, 'atrasada')
        self.assertIn(self.floor.id, Public.search([('sgi_mp_pending_state', '=', 'atrasada')]).ids)
        rows = self.env['sgi.my.pending'].with_user(self.boss_user).search(
            row.action_sgi_open_pending()['domain'])
        self.assertEqual(rows.mapped('kind'), ['actividad'])

    # ---- G-017: «a tiempo» con el vencimiento periódico ----------------------------
    def test_14_semaforo_con_vencimiento(self):
        activity = self.activity
        # Evidencia con un campo Date cualquiera. Antes era res.partner.date,
        # que Odoo 19 ya no tiene (KeyError en una base nueva): el tipo de
        # cambio (res.currency.rate.name) es Date en cualquier base.
        currency = self.env['res.currency'].create({'name': 'ZSG', 'symbol': 'ZS', 'active': True})
        Rate = self.env['res.currency.rate']
        domain = [('currency_id', '=', currency.id)]
        # Mes cerrado: vence el día hábil 1 del mes. Hecha el día 20 = tarde.
        july = date(2026, 7, 20)
        due = activity._sgi_periodic_due(july)
        self.assertLess(due, july)
        self.assertEqual(activity._sgi_periodic_state(Rate, domain, 'name', july), 'rojo')
        Rate.create({'currency_id': currency.id, 'name': date(2026, 7, 20), 'rate': 1.5})
        self.assertEqual(activity._sgi_periodic_state(Rate, domain, 'name', july), 'rojo',
                         "Hecha después del vencimiento no es «al día».")
        Rate.create({'currency_id': currency.id, 'name': due, 'rate': 1.5})
        self.assertEqual(activity._sgi_periodic_state(Rate, domain, 'name', july), 'verde')
        # Sin vencimiento periódico manda la ventana de siempre.
        activity.sudo().write({'due_business_day': 0})
        self.assertIsNone(activity._sgi_periodic_state(Rate, domain, 'name', july))

    # ---- 57.92.0 (U-02): validar en lote ------------------------------------
    def test_20_validar_seleccionadas(self):
        indicator = self._indicator('Z8A-L', calc_mode='otif_ventas', frequency='weekly')
        mondays = [self.today - timedelta(days=self.today.weekday() + 7 * n) for n in (1, 2)]
        measures = self.env['sgi.indicator.measure'].create([
            {'indicator_id': indicator.id, 'period_date': d, 'state': 'capturado', 'value': 90.0}
            for d in mondays])
        objective = self.env['sgi.objective'].create({'name': 'Objetivo lote 8A'})
        line = self.env['sgi.action.line'].create({
            'name': 'Acción lote 8A', 'responsible_id': self.user.id,
            'date_commit': self.today - timedelta(days=2), 'objective_id': objective.id})
        rows = self.Pending.with_user(self.user)._sgi_build(self.emp)
        mine = rows.filtered(lambda r: r.kind == 'validacion' and r.res_id in measures.ids)
        other = rows.filtered(lambda r: r.kind == 'accion' and r.res_id == line.id)
        self.assertEqual(len(mine), 2)
        self.assertEqual(len(other), 1)
        with self.assertRaises(UserError):
            other.with_user(self.user).action_validate_selected()
        (mine | other).with_user(self.user).action_validate_selected()
        self.assertEqual(set(measures.mapped('state')), {'validado'})
        self.assertFalse(mine.exists(), "Los renglones validados desaparecen.")
        self.assertTrue(other.exists(), "Los que no son mediciones se quedan.")

    def test_22_validar_mezcla(self):
        """Quien no es dueño del indicador ni Jefe MAST no valida nada en lote:
        aviso claro y las mediciones siguen capturadas."""
        indicator = self._indicator('Z8A-M', calc_mode='otif_ventas', frequency='weekly')
        mondays = [self.today - timedelta(days=self.today.weekday() + 7 * n) for n in (1, 2)]
        measures = self.env['sgi.indicator.measure'].create([
            {'indicator_id': indicator.id, 'period_date': d, 'state': 'capturado', 'value': 90.0}
            for d in mondays])
        rows = self.Pending.with_user(self.boss_user)._sgi_build(self.emp)
        theirs = rows.filtered(lambda r: r.kind == 'validacion' and r.res_id in measures.ids)
        self.assertEqual(len(theirs), 2)
        with self.assertRaisesRegex(UserError, 'puede validar estas mediciones'):
            theirs.with_user(self.boss_user).action_validate_selected()
        self.assertEqual(set(measures.mapped('state')), {'capturado'})
        self.assertTrue(theirs.exists(), "Lo que no se validó se queda en la lista.")

    def test_23_validar_mezcla_avisa(self):
        """Selección mezclada: valida lo propio, deja lo ajeno y avisa."""
        indicator = self._indicator('Z8A-N', calc_mode='otif_ventas', frequency='weekly')
        boss_indicator = self.env['sgi.indicator'].create({
            'code': 'Z8A-J', 'name': 'KPI Z8A-J', 'calc_mode': 'otif_ventas',
            'frequency': 'weekly', 'responsible_id': self.boss_user.id,
            'process_id': self.process.id})
        monday = self.today - timedelta(days=self.today.weekday() + 7)
        Measure = self.env['sgi.indicator.measure']
        theirs = Measure.create([
            {'indicator_id': indicator.id, 'period_date': d, 'state': 'capturado', 'value': 90.0}
            for d in (monday, monday - timedelta(days=7))])
        own = Measure.create({'indicator_id': boss_indicator.id, 'period_date': monday,
                              'state': 'capturado', 'value': 90.0})
        rows = self.Pending.with_user(self.boss_user)._sgi_build(self.emp | self.boss)
        their_rows = rows.filtered(lambda r: r.kind == 'validacion' and r.res_id in theirs.ids)
        own_row = rows.filtered(lambda r: r.kind == 'validacion' and r.res_id == own.id)
        self.assertEqual(len(their_rows), 2)
        self.assertEqual(len(own_row), 1)
        result = (their_rows | own_row).with_user(self.boss_user).action_validate_selected()
        self.assertEqual(result['tag'], 'display_notification')
        self.assertEqual(result['params']['next']['tag'], 'soft_reload')
        self.assertEqual(own.state, 'validado')
        self.assertEqual(set(theirs.mapped('state')), {'capturado'})
        self.assertEqual(their_rows.exists(), their_rows, "Lo ajeno se queda en la lista.")
        self.assertFalse(own_row.exists(), "Lo validado sale de la lista.")

    def test_21_abre_desplegada_con_lo_urgente(self):
        action = self.Pending.with_user(self.user).action_open_mine()
        self.assertEqual(action['context'].get('search_default_actionable'), 1)
        arch = self.env.ref('quimibond_sgi.sgi_my_pending_view_list').arch
        self.assertIn('expand="1"', arch)
        team = self.env['hr.employee.public'].with_user(self.boss_user).browse(self.emp.id)
        action = team.action_sgi_team_pending()
        self.assertEqual(action['context'].get('search_default_group_employee'), 1)
        self.assertNotIn('search_default_actionable', action['context'],
                         "«Pendientes del equipo» muestra todo.")

    def test_24_ir_a_hacerlo_y_leer(self):
        """U-05: «Ir» lleva al menú donde se hace la actividad; «Leer» abre el
        documento del acuse y «Leído y entendido» lo firma desde el renglón."""
        # Un menú con acción (Inicio → Documentos vigentes).
        menu = self.env.ref('quimibond_sgi.menu_sgi_current_documents')
        self.activity.sudo().write({'odoo_menu_id': menu.id})
        row = self.Pending.with_user(self.user).create({
            'kind': 'actividad', 'name': 'Hacer 8.1', 'employee_id': self.emp.id,
            'res_model': 'sgi.process.activity', 'res_id': self.activity.id})
        action = row.with_user(self.user).action_open()
        self.assertNotEqual(action.get('res_model'), 'sgi.process.activity',
                            "Lleva al menú donde se hace, no a la ficha del catálogo.")
        self.assertEqual(action.get('id'), menu.action.id)
        doc = self.env['documents.document'].create({
            'name': 'Leer 8A', 'type': 'binary', 'sgi_is_controlled': True,
            'sgi_doc_type': 'instructivo', 'sgi_code': 'IT-Z8A-01', 'sgi_state': 'vigente',
            'sgi_process_id': self.process.id})
        ack = self.env['sgi.document.ack'].create({'document_id': doc.id, 'employee_id': self.emp.id})
        row = self.Pending.with_user(self.user).create({
            'kind': 'acuse', 'name': 'Leer', 'employee_id': self.emp.id,
            'res_model': 'sgi.document.ack', 'res_id': ack.id})
        action = row.with_user(self.user).action_open()
        self.assertEqual((action['res_model'], action['res_id']), ('documents.document', doc.id))
        result = row.with_user(self.user).action_sign_ack()
        self.assertEqual(result['tag'], 'soft_reload')
        self.assertEqual(ack.state, 'leido')
        self.assertFalse(row.exists(), "El renglón firmado sale de la lista.")
