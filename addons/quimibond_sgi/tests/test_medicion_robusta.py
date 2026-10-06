# -*- coding: utf-8 -*-
"""57.16.0 (entrega 8, medición robusta: G-002, G-003, H-005, H-006, G-019,
H-016, G-026; J-009). Datos propios."""
from odoo.tests import TransactionCase, tagged
from odoo.tools import mute_logger

from .common_documents import sgi_hide_real_documents
from .common_users import sgi_set_mast


@tagged('post_install', '-at_install')
class TestMedicionRobusta(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.env = cls.env(context=dict(cls.env.context, sgi_skip_role_check=True))
        cls.process = cls.env['sgi.process'].create({'code': 'ZMR', 'name': 'ZS Medición robusta'})
        cls.job = cls.env['hr.job'].create({'name': 'ZS PUESTO MEDICION'})
        cls.partner_model = cls.env['ir.model']._get('res.partner')
        cls.main = cls.env['sgi.config']._sgi_company()
        cls.other_company = cls.env['res.company'].create({'name': 'ZS Otra empresa'})

    def _act(self, name, domain):
        act = self.env['sgi.process.activity'].create({
            'process_id': self.process.id, 'name': name,
            'role_ids': [(0, 0, {'role': 'ejecuta', 'job_id': self.job.id})],
            'measure_method': 'odoo', 'measure_model_id': self.partner_model.id,
            'measure_cadence': 'evento'})
        # El dominio se escribe por SQL: así llega el dato capturado a mano
        # en producción, sin pasar por ninguna validación.
        self.env.cr.execute("UPDATE sgi_process_activity SET measure_domain = %s WHERE id = %s",
                            (domain, act.id))
        act.invalidate_recordset(['measure_domain'])
        return act

    def test_01_filtro_ilegible_no_sale_verde(self):
        """G-003: antes contaba todo el modelo y salía verde."""
        self.env['res.partner'].create({'name': 'ZS Evidencia'})
        act = self._act('Filtro roto', "[('ref', '=', ")
        act._sgi_measure_odoo()
        self.assertFalse(act.measure_state)
        self.assertFalse(act.measure_last_date)
        self.assertIn('Filtro de evidencia inválido', act.measure_warning or '')

    @mute_logger('odoo.sql_db', 'odoo.addons.quimibond_sgi.models.sgi_process_procedure')
    def test_02_error_sql_no_tumba_a_las_demas(self):
        """G-002 / H-005: un campo que no existe truena al convertirse a SQL;
        la actividad siguiente se mide igual y queda el aviso."""
        self.env['res.partner'].create({'name': 'ZS Buena', 'ref': 'ZMR-OK'})
        bad = self._act('Campo inexistente', "[('no_existe_zs', '=', 1)]")
        good = self._act('Buena', "[('ref', '=', 'ZMR-OK')]")
        (bad | good)._sgi_measure_odoo()
        self.assertFalse(bad.measure_state)
        self.assertIn('Filtro de evidencia inválido', bad.measure_warning or '')
        self.assertTrue(good.measure_last_date, "La siguiente actividad sí se midió.")
        self.assertEqual(good.measure_count_30d, 1)

    def test_03_segunda_empresa_no_cuenta(self):
        """J-009 (H-006, D-03): solo la empresa del SGI (o sin empresa)."""
        Partner = self.env['res.partner']
        Partner.create({'name': 'ZS Uno', 'ref': 'ZMR-EMP', 'company_id': self.main.id})
        Partner.create({'name': 'ZS Sin empresa', 'ref': 'ZMR-EMP', 'company_id': False})
        Partner.create({'name': 'ZS Otra', 'ref': 'ZMR-EMP', 'company_id': self.other_company.id})
        act = self._act('Por empresa', "[('ref', '=', 'ZMR-EMP')]")
        act._sgi_measure_odoo()
        self.assertEqual(act.measure_count_30d, 2, "El de la otra empresa no cuenta.")
        action = act.action_view_measure_records()
        self.assertEqual(self.env['res.partner'].search_count(action['domain']), 2,
                         "La evidencia muestra lo mismo que cuenta la medición.")

    def test_04_cron_por_pasos_con_savepoint(self):
        """G-002: el cron sigue aunque una actividad truene."""
        self._act('Campo inexistente cron', "[('no_existe_zs', '=', 1)]")
        with mute_logger('odoo.sql_db'):
            self.assertTrue(self.env['sgi.process.activity'].cron_measure_activities())

    def test_05_procedimiento_de_otro_proceso(self):
        """H-016: al entrar en vigor, avisa si otro proceso cita el
        procedimiento que se obsoleta."""
        sgi_hide_real_documents(self.env)
        mast = sgi_set_mast(self.env, login='zmr_mast')
        old = self.env['sgi.process'].create({'code': 'ZMRV', 'name': 'ZS Viejo'})
        new = self.env['sgi.process'].create({'code': 'ZMRN', 'name': 'ZS Nuevo'})
        doc = self.env['documents.document'].create({
            'name': 'P-A97', 'type': 'binary', 'sgi_is_controlled': True,
            'sgi_doc_type': 'procedimiento', 'sgi_code': 'P-A97', 'sgi_state': 'vigente',
            'sgi_process_id': old.id, 'sgi_migration_state': 'en_curso'})
        doc.sgi_replaced_by_process_id = new
        citing = self.env['sgi.process.activity'].create({
            'process_id': self.process.id, 'name': 'Revisar la flotilla',
            'role_ids': [(0, 0, {'role': 'ejecuta', 'job_id': self.job.id})],
            'related_procedure_id': doc.id})
        new.write({'state': 'vigente'})
        self.assertEqual(doc.sgi_state, 'obsoleto')
        notices = self.env['mail.activity'].search([
            ('res_model', '=', 'sgi.process'), ('res_id', '=', self.process.id),
            ('sgi_cron_key', '=', 'procedimiento_obsoleto_citado:%d' % new.id)])
        self.assertEqual(len(notices), 1)
        self.assertEqual(notices.user_id, mast, "Sin dueño del proceso, al Jefe MAST.")
        self.assertIn('P-A97', notices.note)
        self.assertEqual(citing.related_procedure_id, doc, "No cambia la fuente de verdad.")
        self.assertTrue(any('procedimiento que quedó obsoleto' in (m.body or '')
                            for m in new.message_ids))

    def test_06_reincidencia_se_recalcula(self):
        """G-026: cancelar la NC previa quita la reincidencia de la siguiente."""
        team = self.env.ref('quimibond_sgi.sgi_quality_team_internal')
        stage_open = self.env.ref('quimibond_sgi.sgi_nc_int_stage_open')
        stage_cancel = self.env.ref('quimibond_sgi.sgi_nc_int_stage_cancel')
        Alert = self.env['quality.alert']
        first = Alert.create({'title': 'ZS NC 1', 'team_id': team.id, 'stage_id': stage_open.id,
                              'sgi_process_id': self.process.id})
        second = Alert.create({'title': 'ZS NC 2', 'team_id': team.id, 'stage_id': stage_open.id,
                               'sgi_process_id': self.process.id})
        self.assertTrue(second.sgi_folio)
        self.assertEqual(second.sgi_recurrence_count, 1)
        other = self.env['sgi.process'].create({'code': 'ZMRO', 'name': 'ZS Otro proceso'})
        first.write({'sgi_process_id': other.id})
        self.env.flush_all()
        self.assertEqual(second.sgi_recurrence_count, 0, "Cambió de proceso: ya no es previa.")
        first.write({'sgi_process_id': self.process.id})
        self.env.flush_all()
        self.assertEqual(second.sgi_recurrence_count, 1, "De vuelta en el proceso, cuenta.")
        first.with_context(sgi_cancel_approved=True).write(
            {'stage_id': stage_cancel.id, 'sgi_cancel_reason': 'ZS falsa alarma'})
        self.env.flush_all()
        self.assertEqual(second.sgi_recurrence_count, 0, "La previa cancelada ya no cuenta.")
        self.assertFalse(second.sgi_is_recurrent)
