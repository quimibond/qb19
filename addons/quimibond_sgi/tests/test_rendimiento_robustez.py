# -*- coding: utf-8 -*-
"""57.95.0 (auditoría 2026-10: K-08, K-05, D-06 de datos): rendimiento y
robustez.

- Acuses pendientes: un aviso por persona con usuario y uno por jefe para la
  gente sin usuario (antes, uno por acuse); el Jefe MAST solo recibe los de
  jefes sin usuario o sin departamento y los de personas sin jefe. Ningún
  aviso va sobre la ficha del empleado (``hr.employee``), que no lee quien no
  es de RH: el de la persona va sobre su acuse pendiente más viejo
  (``acuses_propios:<empleado>``) y el del jefe sobre su departamento
  (``acuses_equipo:<jefe>``); sin departamento, o sin jefe, sobre el acuse
  más viejo del grupo, al Jefe MAST.
- Clase del aviso indexada (``sgi_cron_kind``) para el barrido de episodios.
- Los filtros de Mi equipo leen un resumen guardado por empleado: no
  recalculan Mis pendientes de toda la empresa en cada búsqueda.
- Respaldo nocturno: recalcula las cuatro listas guardadas de Mi
  procedimiento y dice cuántas personas cambiaron (0 en régimen).
- K-05: las entregas de la factura siguen al estado de la entrega; lo
  ajustado a mano no se pisa; la fecha de pago depende de la fecha de factura.
- D-06: los controlados nuevos nacen con la empresa del SGI; el asistente del
  Jefe MAST cuenta y asigna la empresa a los que no tienen; regla de empresa
  en las rutinas del procedimiento anterior."""
from datetime import timedelta
from unittest.mock import patch

from odoo import fields
from odoo.exceptions import AccessError
from odoo.tests import TransactionCase, new_test_user, tagged

from .common_documents import sgi_hide_real_documents
from .common_users import sgi_set_mast

ACK_KINDS = ('acuse_pendiente', 'acuses_propios', 'acuses_equipo')


class _RendimientoCase(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        from ..models.sgi_calendar import sgi_today
        sgi_hide_real_documents(cls.env)
        # La base del build es copia de producción (147 acuses pendientes el
        # 2026-10-02): que ninguno real cruce el umbral en estas pruebas. Si
        # el aviso de un acuse real fallara, ``failures`` > 0 y el barrido no
        # correría (test_04). Se deshace al final de la prueba.
        cls.env.flush_all()
        cls.env.cr.execute("UPDATE sgi_document_ack SET create_date = now() "
                           "WHERE state = 'pendiente'")
        cls.env.invalidate_all()
        cls.today = sgi_today(cls.env)
        cls.mast = sgi_set_mast(cls.env, login='rr_mast')
        cls.company = cls.env['sgi.config']._sgi_company()
        cls.Activity = cls.env['mail.activity'].with_context(active_test=False)
        cls.Pending = cls.env['sgi.my.pending']
        cls.Cron = cls.env['sgi.cron']
        Employee = cls.env['hr.employee']
        Department = cls.env['hr.department']
        groups = 'base.group_user,quimibond_sgi.group_sgi_user'
        cls.boss_user = new_test_user(cls.env, login='rr_jefe', groups=groups)
        cls.own_user = new_test_user(cls.env, login='rr_propio', groups=groups)
        cls.boss_nodept_user = new_test_user(cls.env, login='rr_jefe_sin_depto', groups=groups)
        cls.dept = Department.create({'name': 'Departamento RR'})
        cls.dept_nouser = Department.create({'name': 'Departamento sin usuario RR'})
        cls.boss = Employee.create({'name': 'Jefe RR', 'user_id': cls.boss_user.id,
                                    'department_id': cls.dept.id})
        cls.boss_nouser = Employee.create({'name': 'Jefe sin usuario RR',
                                           'department_id': cls.dept_nouser.id})
        cls.boss_nodept = Employee.create({'name': 'Jefe sin departamento RR',
                                           'user_id': cls.boss_nodept_user.id})
        cls.op1 = Employee.create({'name': 'Operador uno RR', 'parent_id': cls.boss.id})
        cls.op2 = Employee.create({'name': 'Operador dos RR', 'parent_id': cls.boss.id})
        cls.op3 = Employee.create({'name': 'Operador tres RR', 'parent_id': cls.boss_nouser.id})
        cls.op4 = Employee.create({'name': 'Operador cuatro RR', 'parent_id': cls.boss_nodept.id})
        cls.orphan = Employee.create({'name': 'Operador sin jefe RR'})
        cls.own = Employee.create({'name': 'Con usuario RR', 'user_id': cls.own_user.id,
                                   'parent_id': cls.boss.id})
        cls.process = cls.env['sgi.process'].create({'code': 'Z95', 'name': 'Proceso 57.95'})
        Doc = cls.env['documents.document']
        cls.doc_a = Doc.create({
            'name': 'Instructivo A RR', 'type': 'binary', 'sgi_is_controlled': True,
            'sgi_doc_type': 'instructivo', 'sgi_code': 'IT-Z95-01', 'sgi_state': 'vigente',
            'sgi_process_id': cls.process.id})
        cls.doc_b = Doc.create({
            'name': 'Instructivo B RR', 'type': 'binary', 'sgi_is_controlled': True,
            'sgi_doc_type': 'instructivo', 'sgi_code': 'IT-Z95-02', 'sgi_state': 'vigente',
            'sgi_process_id': cls.process.id})

    def _ack(self, employee, doc, days_ago=30):
        """Acuse pendiente que ya pasó el umbral de días hábiles del cron
        (``days_ago=0``: recién nacido, todavía no lo pasa)."""
        ack = self.env['sgi.document.ack'].create({'document_id': doc.id, 'employee_id': employee.id})
        self.env.flush_all()
        self.env.cr.execute("UPDATE sgi_document_ack SET create_date = %s WHERE id = %s",
                            (fields.Datetime.now() - timedelta(days=days_ago), ack.id))
        ack.invalidate_recordset(['create_date'])
        return ack

    def _notice(self, kind, employee):
        """El episodio del aviso de acuses de ``employee`` (la persona con
        usuario o el jefe del equipo), con archivados: la clave lleva el
        empleado; el registro ancla no es su ficha."""
        return self.Activity.search([('sgi_cron_key', '=', '%s:%d' % (kind, employee.id))])

    def _assert_anchor(self, notice, record, msg=None):
        self.assertEqual((notice.res_model, notice.res_id), (record._name, record.id), msg)


@tagged('post_install', '-at_install')
class TestAvisosDeAcuse(_RendimientoCase):

    def test_01_sin_usuario_un_aviso_por_jefe_sobre_su_departamento(self):
        self._ack(self.op1, self.doc_a)
        self._ack(self.op1, self.doc_b)
        self._ack(self.op2, self.doc_a)
        self._ack(self.own, self.doc_a)
        self.Cron.cron_documents()
        notice = self._notice('acuses_equipo', self.boss).filtered('active')
        self.assertEqual(len(notice), 1, "Un aviso por jefe, no uno por acuse.")
        self._assert_anchor(notice, self.dept, "Va sobre el departamento del jefe, no sobre su ficha.")
        self.assertEqual(notice.user_id, self.boss_user, "Va al jefe, no al Jefe MAST.")
        self.assertEqual(notice.sgi_cron_kind, 'acuses_equipo')
        self.assertIn("3", notice.summary)
        self.assertIn("2 personas", notice.summary)
        self.assertIn("Operador uno RR", notice.note)
        self.assertIn("Operador dos RR", notice.note)
        self.assertNotIn("Con usuario RR", notice.note, "Quien tiene usuario recibe el suyo.")
        mine = self._notice('acuses_propios', self.own).filtered('active')
        self.assertEqual(len(mine), 1)
        self.assertFalse(self.Activity.search([
            ('res_model', '=', 'documents.document'), ('res_id', 'in', (self.doc_a | self.doc_b).ids),
            ('sgi_cron_kind', 'in', ACK_KINDS)]), "Ya no hay avisos de acuse sobre el documento.")
        self.assertFalse(self.Activity.search([
            ('res_model', '=', 'hr.employee'), ('sgi_cron_kind', 'in', ACK_KINDS)]),
            "Ningún aviso de acuse sobre fichas de empleado (no las lee quien no es de RH).")

    def test_02_al_jefe_mast_sin_usuario_sin_departamento_o_sin_jefe(self):
        self._ack(self.op3, self.doc_a)
        orphan_ack = self._ack(self.orphan, self.doc_b)
        oldest = self._ack(self.op4, self.doc_a, days_ago=40)
        self._ack(self.op4, self.doc_b)
        self.Cron.cron_documents()
        team = self._notice('acuses_equipo', self.boss_nouser).filtered('active')
        self.assertEqual(team.user_id, self.mast, "Jefe sin usuario: al Jefe MAST.")
        self._assert_anchor(team, self.dept_nouser, "Sobre el departamento del jefe.")
        self.assertIn("Jefe sin usuario RR", team.summary)
        self.assertIn("1 persona", team.summary, "Singular con una persona.")
        self.assertNotIn("personas", team.summary)
        nodept = self._notice('acuses_equipo', self.boss_nodept).filtered('active')
        self.assertEqual(nodept.user_id, self.mast,
                         "Jefe con usuario pero sin departamento: al Jefe MAST.")
        self._assert_anchor(nodept, oldest, "Sin departamento: sobre el acuse más viejo del grupo.")
        self.assertIn("Jefe sin departamento RR", nodept.summary)
        self.assertIn("2", nodept.summary)
        alone = self._notice('acuses_equipo', self.orphan).filtered('active')
        self.assertEqual(alone.user_id, self.mast, "Sin jefe: al Jefe MAST.")
        self._assert_anchor(alone, orphan_ack, "Sin jefe ni departamento: sobre su acuse.")
        self.assertIn("Operador sin jefe RR", alone.summary)

    def test_03_con_usuario_un_aviso_propio_que_mis_pendientes_no_repite(self):
        oldest = self._ack(self.own, self.doc_a, days_ago=40)
        self._ack(self.own, self.doc_b)
        self.Cron.cron_documents()
        notice = self._notice('acuses_propios', self.own).filtered('active')
        self.assertEqual(len(notice), 1)
        self._assert_anchor(notice, oldest, "Sobre su acuse pendiente más viejo, que sí puede leer.")
        self.assertEqual(notice.user_id, self.own_user)
        self.assertIn("2", notice.summary)

        def _rows():
            return self.Pending._sgi_pending_values(self.own_user)[self.own_user.id]

        rows = _rows()
        self.assertGreaterEqual(len([r for r in rows if r['kind'] == 'acuse'
                                     and r['res_model'] == 'sgi.document.ack']), 2)
        self.assertFalse([r for r in rows if r['kind'] == 'aviso' and r['res_id'] == notice.id],
                         "Ya tiene sus renglones «acuse»: el aviso no se repite.")
        # Firma el más viejo: el mismo aviso sigue (clave por persona, no por
        # acuse), baja a 1 y Mis pendientes tampoco lo repite aunque su ancla
        # ya no sea un acuse pendiente.
        oldest.action_mark_read()
        self.Cron.cron_documents()
        episode = self._notice('acuses_propios', self.own)
        self.assertEqual(episode.filtered('active'), notice, "Un solo aviso por persona.")
        self.assertIn("1", notice.summary)
        self.assertFalse([r for r in _rows() if r['kind'] == 'aviso' and r['res_id'] == notice.id])

    def test_04_idempotente_cierra_al_firmar_y_cierra_los_viejos(self):
        ack = self._ack(self.op1, self.doc_a)
        legacy = self.doc_b.activity_schedule(
            'mail.mail_activity_data_todo', date_deadline=self.today,
            summary='Acuse pendiente: Operador uno RR', user_id=self.mast.id)
        legacy.sudo().sgi_cron_key = 'acuse_pendiente:%d' % ack.id
        self.Cron.cron_documents()
        self.Cron.cron_documents()
        self.assertEqual(len(self._notice('acuses_equipo', self.boss)), 1, "Dos corridas, un aviso.")
        self.assertFalse(legacy.active, "El aviso de uno por acuse se cierra: lo reemplaza el agrupado.")
        self.assertTrue(legacy.sgi_episode_closed)
        ack.action_mark_read()
        self.Cron.cron_documents()
        notice = self._notice('acuses_equipo', self.boss)
        self.assertFalse(notice.active, "Sin acuses pendientes el aviso se cierra solo.")
        self.assertTrue(notice.sgi_episode_closed)

    def test_05_el_jefe_lo_ve_en_mis_pendientes_e_ir_abre_los_acuses(self):
        acks = self._ack(self.op1, self.doc_a) | self._ack(self.op2, self.doc_b)
        recent = self._ack(self.op1, self.doc_b, days_ago=0)
        self.Cron.cron_documents()
        notice = self._notice('acuses_equipo', self.boss).filtered('active')
        rows = self.Pending.with_user(self.boss_user)._sgi_build(self.boss)
        line = rows.filtered(lambda r: r.kind == 'aviso' and r.res_id == notice.id)
        self.assertEqual(len(line), 1, "El aviso sobre el departamento sale en Mis pendientes del jefe.")
        action = line.with_user(self.boss_user).action_open()
        self.assertEqual(action['res_model'], 'sgi.document.ack')
        shown = self.env['sgi.document.ack'].with_user(self.boss_user).search(action['domain'])
        self.assertEqual(shown & acks, acks)
        self.assertNotIn(recent, shown, "«Ir» lista solo los acuses que pasaron el mismo umbral.")
        self.assertNotIn("Con usuario RR", shown.employee_id.mapped('name'))

    def test_06_clase_del_aviso_indexada_y_barrido(self):
        field = self.env['mail.activity']._fields['sgi_cron_kind']
        self.assertTrue(field.store and field.index)
        make = self.doc_a.activity_schedule
        ep = make('mail.mail_activity_data_todo', summary='Ep RR', user_id=self.mast.id)
        other = make('mail.mail_activity_data_todo', summary='Otro RR', user_id=self.mast.id)
        plain = make('mail.mail_activity_data_todo', summary='Solo RR', user_id=self.mast.id)
        manual = make('mail.mail_activity_data_todo', summary='Manual RR', user_id=self.mast.id)
        ep.sudo().sgi_cron_key = 'episodio_rr:5'
        other.sudo().sgi_cron_key = 'episodio_rr_otro:5'
        plain.sudo().sgi_cron_key = 'episodio_rr'
        self.assertEqual(ep.sgi_cron_kind, 'episodio_rr')
        self.assertEqual(other.sgi_cron_kind, 'episodio_rr_otro')
        self.assertEqual(plain.sgi_cron_kind, 'episodio_rr')
        self.assertFalse(manual.sgi_cron_kind)
        self.Cron._sgi_new_run()._sgi_sweep(['episodio_rr'], "ya no aplica")
        self.assertFalse(ep.active)
        self.assertFalse(plain.active)
        self.assertTrue(other.active, "Otra clase con el mismo prefijo no se barre.")
        self.assertTrue(manual.active)


@tagged('post_install', '-at_install')
class TestMiEquipoGuardado(_RendimientoCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        objective = cls.env['sgi.objective'].create({'name': 'Objetivo 57.95'})
        cls.late = cls.env['sgi.action.line'].create({
            'name': 'Acción atrasada RR', 'responsible_id': cls.own_user.id,
            'date_commit': cls.today - timedelta(days=3), 'objective_id': objective.id})

    def test_07_el_filtro_no_recorre_la_empresa(self):
        Public = self.env['hr.employee.public'].with_user(self.boss_user)
        boom = AssertionError("El filtro recalculó Mis pendientes de la empresa.")
        with patch.object(type(self.Pending), '_sgi_summary_employees', side_effect=boom):
            before = Public.search([('sgi_mp_pending_late', '>', 0)])
        self.assertNotIn(self.own.id, before.ids, "Sin resumen guardado todavía.")
        self.env['hr.employee']._sgi_refresh_pending_summary(self.own)
        with patch.object(type(self.Pending), '_sgi_summary_employees', side_effect=boom):
            self.assertIn(self.own.id, Public.search([('sgi_mp_pending_late', '>', 0)]).ids)
            self.assertIn(self.own.id, Public.search([('sgi_mp_pending_state', '=', 'atrasada')]).ids)
            self.assertIn(self.own.id, Public.search([('sgi_mp_pending_total', '>=', 1)]).ids)
            self.assertNotIn(self.own.id, Public.search([('sgi_mp_pending_state', '=', 'al_dia')]).ids)

    def test_08_el_resumen_solo_escribe_lo_que_cambia(self):
        Employee = self.env['hr.employee']
        changed = Employee._sgi_refresh_pending_summary(self.own)
        self.assertIn(self.own, changed)
        self.assertGreaterEqual(self.own.sgi_pending_saved_late, 1)
        self.assertEqual(self.own.sgi_pending_saved_state, 'atrasada')
        self.assertFalse(Employee._sgi_refresh_pending_summary(self.own), "Sin cambios no escribe.")

    def test_09_abrir_la_lista_refresca_el_resumen(self):
        Employee = self.env['hr.employee']
        Employee._sgi_refresh_pending_summary(self.own)
        total = self.own.sgi_pending_saved_total
        self.env['sgi.action.line'].create({
            'name': 'Otra acción RR', 'responsible_id': self.own_user.id,
            'date_commit': self.today - timedelta(days=1), 'objective_id': self.late.objective_id.id})
        self.Pending.with_user(self.own_user)._sgi_build(self.own)
        self.own.invalidate_recordset(['sgi_pending_saved_total'])
        self.assertEqual(self.own.sgi_pending_saved_total, total + 1)


@tagged('post_install', '-at_install')
class TestRespaldoNocturno(_RendimientoCase):

    def test_10_listas_de_mi_procedimiento_cero_cambios_en_regimen(self):
        job = self.env['hr.job'].create({'name': 'PUESTO RESPALDO RR'})
        emp = self.env['hr.employee'].create({'name': 'Persona respaldo RR', 'job_id': job.id})
        activity = self.env['sgi.process.activity'].create({
            'process_id': self.process.id, 'name': 'Actividad respaldo RR', 'number': '9.1',
            'role_ids': [(0, 0, {'role': 'ejecuta', 'job_id': job.id})]})
        self.env.flush_all()
        self.env.invalidate_all()
        Employee = self.env['hr.employee']
        self.assertIn(activity.role_ids, emp.sgi_mp_role_ids)
        changed = Employee._sgi_mp_nightly_recompute()
        self.assertNotIn(emp, changed, "Con los disparos completos no cambia nada.")
        self.assertFalse(Employee._sgi_mp_nightly_recompute(), "Segunda corrida: 0 cambios.")
        # Un disparo que falta: la lista guardada se quedó vieja.
        self.env.cr.execute("DELETE FROM hr_employee_sgi_mp_role_rel WHERE employee_id = %s", (emp.id,))
        emp.invalidate_recordset(['sgi_mp_role_ids'])
        self.assertFalse(emp.sgi_mp_role_ids)
        with self.assertLogs('odoo.addons.quimibond_sgi.models.sgi_my_procedure_screen',
                             level='WARNING') as logs:
            changed = Employee._sgi_mp_nightly_recompute()
        self.assertIn(emp, changed)
        self.assertIn("falta un disparo", "\n".join(logs.output))
        emp.invalidate_recordset(['sgi_mp_role_ids'])
        self.assertIn(activity.role_ids, emp.sgi_mp_role_ids, "El respaldo la corrige.")

    def test_11_respaldo_nocturno_solo_el_sistema(self):
        with self.assertRaises(AccessError):
            self.Cron.with_user(self.boss_user).cron_nightly_backup()
        self.assertTrue(self.Cron.cron_nightly_backup())
        # Por su código y no por xmlid: el registro llega con la Task 5.5 y el
        # checker de referencias marcaría un env.ref a un xmlid que aún no
        # está declarado.
        cron = self.env['ir.cron'].search([('model_id.model', '=', 'sgi.cron'),
                                           ('code', 'ilike', 'cron_nightly_backup')])
        self.assertEqual(len(cron), 1, "Una acción planificada del respaldo nocturno, activa.")


@tagged('post_install', '-at_install')
class TestEntregasFacturadas(TransactionCase):
    """K-05."""

    def _sale_flow(self):
        customer = self.env['res.partner'].create({'name': 'Cliente K05'})
        product = self.env['product.product'].create({'name': 'Tela K05', 'type': 'consu'})
        order = self.env['sale.order'].create({
            'partner_id': customer.id,
            'order_line': [(0, 0, {'product_id': product.id, 'product_uom_qty': 1,
                                   'price_unit': 10.0})]})
        line = order.order_line
        wh = self.env['stock.warehouse'].search([('company_id', '=', self.env.company.id)], limit=1)
        customers = self.env.ref('stock.stock_location_customers')
        picking = self.env['stock.picking'].create({
            'picking_type_id': wh.out_type_id.id, 'partner_id': customer.id,
            'location_id': wh.lot_stock_id.id, 'location_dest_id': customers.id,
            'move_ids': [(0, 0, {
                'product_id': product.id, 'product_uom_qty': 1.0, 'product_uom': product.uom_id.id,
                'sale_line_id': line.id, 'location_id': wh.lot_stock_id.id,
                'location_dest_id': customers.id})]})
        picking.action_confirm()
        picking.move_ids.write({'quantity': 1.0, 'picked': True})
        income = self.env['account.account'].search([('account_type', '=', 'income')], limit=1)
        invoice = self.env['account.move'].create({
            'move_type': 'out_invoice', 'partner_id': customer.id,
            'invoice_line_ids': [(0, 0, {
                'product_id': product.id, 'quantity': 1, 'price_unit': 10.0,
                'account_id': income.id, 'tax_ids': [(6, 0, [])],
                'sale_line_ids': [(6, 0, line.ids)]})]})
        return picking, invoice

    def test_12_la_factura_sigue_al_estado_de_la_entrega(self):
        picking, invoice = self._sale_flow()
        self.assertFalse(invoice.sgi_picking_ids, "La entrega aún no se valida.")
        picking.button_validate()
        self.assertEqual(picking.state, 'done')
        self.assertEqual(invoice.sgi_picking_ids, picking,
                         "Validar la entrega la propone en la factura (dependencia de state).")
        self.assertFalse(invoice.sgi_picking_manual)
        # El formulario reenvía lo que el onchange recalculó: escribir lo
        # mismo que lo propuesto no es un ajuste a mano.
        invoice.write({'sgi_picking_ids': [(6, 0, picking.ids)]})
        self.assertFalse(invoice.sgi_picking_manual)
        self.assertFalse(invoice.sgi_picking_outdated)

    def test_13_lo_ajustado_a_mano_no_se_pisa_y_se_puede_regresar(self):
        picking, invoice = self._sale_flow()
        other = picking.copy()
        invoice.write({'sgi_picking_ids': [(6, 0, other.ids)]})
        self.assertTrue(invoice.sgi_picking_manual, "Escribirlas a mano marca el ajuste.")
        picking.button_validate()
        self.assertEqual(invoice.sgi_picking_ids, other, "Lo ajustado a mano no se pisa.")
        self.assertEqual(invoice.sgi_picking_proposed_ids, picking, "Lo propuesto se ve aparte.")
        invoice.action_sgi_picking_reset()
        self.assertFalse(invoice.sgi_picking_manual)
        self.assertEqual(invoice.sgi_picking_ids, picking)

    def test_14_dependencias_completas(self):
        Move = self.env['account.move']
        picking_deps = Move._fields['sgi_picking_ids'].depends
        self.assertIn('invoice_line_ids.sale_line_ids.move_ids.picking_id.state', picking_deps)
        self.assertIn('invoice_line_ids.purchase_line_id.move_ids.picking_id.state', picking_deps)
        self.assertIn('sgi_picking_manual', picking_deps)
        self.assertIn('invoice_date', Move._fields['sgi_payment_date'].depends)


@tagged('post_install', '-at_install')
class TestEmpresaDelSgi(_RendimientoCase):
    """D-06 (datos)."""

    def _loose(self, code, doc_type='instructivo', **extra):
        doc = self.env['documents.document'].create(dict({
            'name': 'Sin empresa %s' % code, 'type': 'binary', 'sgi_is_controlled': True,
            'sgi_doc_type': doc_type, 'sgi_code': code, 'sgi_state': 'vigente'}, **extra))
        self.env.flush_all()
        self.env.cr.execute("UPDATE documents_document SET company_id = NULL WHERE id = %s", (doc.id,))
        doc.invalidate_recordset(['company_id'])
        return doc

    def test_15_el_asistente_cuenta_y_luego_aplica(self):
        # La copia de producción trae 492 controlados sin empresa: aplicar el
        # asistente sobre ellos haría la prueba lenta y dependiente de datos
        # reales (un lote que choque con una regla deja ``again`` > 0). Se
        # les pone empresa por SQL dentro de la prueba (se deshace al final).
        self.env.flush_all()
        self.env.cr.execute("UPDATE documents_document SET company_id = %s "
                            "WHERE sgi_is_controlled IS TRUE AND company_id IS NULL",
                            (self.company.id,))
        self.env.invalidate_all()
        procedure = self._loose('P-Z95', doc_type='procedimiento')
        loose = procedure | self._loose('IT-Z95-03', sgi_process_id=self.process.id)
        routine = self.env['sgi.legacy.routine'].create({
            'procedure_id': procedure.id, 'n': 1, 'name': 'Rutina RR', 'state': 'eliminada',
            'reason': 'Prueba 57.95'})
        self.assertFalse(routine.company_id)
        free = self.env['documents.document'].create({'name': 'No controlado RR', 'type': 'binary'})
        self.env.cr.execute("UPDATE documents_document SET company_id = NULL WHERE id = %s", (free.id,))
        free.invalidate_recordset(['company_id'])
        with self.assertRaises(AccessError):
            self.env['sgi.company.fix'].with_user(self.boss_user).create({})
        wizard = self.env['sgi.company.fix'].with_user(self.mast).create({})
        self.assertEqual(wizard.doc_count, 2)
        self.assertEqual(wizard.routine_count, 1)
        self.assertFalse(loose.filtered('company_id'), "Contar no escribe nada.")
        wizard.action_apply()
        loose.invalidate_recordset(['company_id'])
        self.assertEqual(loose.company_id, self.company)
        self.assertTrue(any("Empresa del SGI asignada" in (m.body or '') for m in procedure.message_ids))
        routine.invalidate_recordset(['company_id'])
        self.assertEqual(routine.company_id, self.company, "La rutina la toma de su procedimiento.")
        free.invalidate_recordset(['company_id'])
        self.assertFalse(free.company_id, "Un documento no controlado no se toca.")
        self.assertEqual(wizard.done_count, 2)
        self.assertFalse(wizard.failed_count)
        again = self.env['sgi.company.fix'].with_user(self.mast).create({})
        self.assertEqual(again.doc_count, 0, "Idempotente.")

    def test_16_controlado_nuevo_nace_con_la_empresa_del_sgi(self):
        doc = self.env['documents.document'].create({
            'name': 'Nuevo RR', 'type': 'binary', 'sgi_is_controlled': True,
            'sgi_doc_type': 'instructivo', 'sgi_code': 'IT-Z95-04', 'sgi_process_id': self.process.id})
        self.assertEqual(doc.company_id, self.company)
        free = self.env['documents.document'].create({'name': 'Libre RR', 'type': 'binary'})
        self.env.flush_all()
        self.env.cr.execute("UPDATE documents_document SET company_id = NULL WHERE id = %s", (free.id,))
        free.invalidate_recordset(['company_id'])
        free.write({'sgi_is_controlled': True, 'sgi_doc_type': 'instructivo',
                    'sgi_code': 'IT-Z95-05', 'sgi_process_id': self.process.id})
        self.assertEqual(free.company_id, self.company, "Al volverse controlado toma la empresa.")
        # Una revisión nueva de una familia que vive sin empresa (D-06 aún
        # sin aplicar) se queda con su familia: la revisión se sigue
        # comparando contra las anteriores.
        old = self._loose('IT-Z95-06', sgi_process_id=self.process.id)
        rev = self.env['documents.document'].create({
            'name': 'Revisión RR', 'type': 'binary', 'sgi_is_controlled': True,
            'sgi_doc_type': 'instructivo', 'sgi_code': 'IT-Z95-06', 'sgi_revision': old.sgi_revision + 1,
            'sgi_process_id': self.process.id})
        self.assertFalse(rev.company_id)

    def test_17_regla_de_empresa_en_rutinas(self):
        rule = self.env['ir.rule'].search([('model_id.model', '=', 'sgi.legacy.routine'),
                                           ('domain_force', 'ilike', 'company_ids')])
        self.assertEqual(len(rule), 1, "Regla de empresa en las rutinas.")
        other = self.env['res.company'].create({'name': 'Otra empresa RR'})
        procedure = self.env['documents.document'].create({
            'name': 'Procedimiento otra RR', 'type': 'binary', 'sgi_is_controlled': True,
            'sgi_doc_type': 'procedimiento', 'sgi_code': 'P-Z96', 'sgi_state': 'vigente',
            'company_id': other.id})
        routine = self.env['sgi.legacy.routine'].create({
            'procedure_id': procedure.id, 'n': 1, 'name': 'Rutina otra RR', 'state': 'eliminada',
            'reason': 'Prueba 57.95'})
        self.assertEqual(routine.company_id, other)
        self.assertFalse(self.env['sgi.legacy.routine'].with_user(self.mast).search(
            [('id', '=', routine.id)]), "El Jefe MAST (solo PNTQ) no la ve.")
