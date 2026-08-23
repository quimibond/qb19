# -*- coding: utf-8 -*-
from odoo import fields
from odoo.tests import TransactionCase, tagged
from odoo.exceptions import UserError


@tagged('post_install', '-at_install')
class TestActivityMeasurement(TransactionCase):
    """Fase 10: la actividad del procedimiento medida con acciones reales."""

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.process = cls.env['sgi.process'].create({
            'code': 'P-TST-MEAS', 'name': 'Proceso medible'})
        cls.partner_model = cls.env['ir.model']._get('res.partner')

    def _activity(self, **vals):
        base = {
            'process_id': self.process.id,
            'name': 'Actividad medible',
            'measure_model_id': self.partner_model.id,
            'measure_cadence': 'mensual',
        }
        base.update(vals)
        return self.env['sgi.process.activity'].create(base)

    def test_01_measure_green_and_counts(self):
        marker = 'Evidencia fase10 única'
        self.env['res.partner'].create({'name': marker})
        act = self._activity(
            measure_domain="[('name', '=', '%s')]" % marker)
        act._sgi_measure()
        self.assertEqual(act.measure_state, 'verde')
        self.assertEqual(act.measure_count_30d, 1)
        self.assertTrue(act.measure_last_date)

    def test_02_measure_red_without_evidence(self):
        act = self._activity(
            measure_domain="[('name', '=', 'no-existe-fase10-xyz')]")
        act._sgi_measure()
        self.assertEqual(act.measure_state, 'rojo')
        self.assertEqual(act.measure_count_30d, 0)
        self.assertFalse(act.measure_last_date)

    def test_03_event_cadence_counts_only(self):
        marker = 'Evidencia fase10 evento'
        self.env['res.partner'].create({'name': marker})
        act = self._activity(
            measure_cadence='evento',
            measure_domain="[('name', '=', '%s')]" % marker)
        act._sgi_measure()
        # Por evento nunca marca rojo: verde con evidencia, nada sin ella.
        self.assertEqual(act.measure_state, 'verde')
        bare = self._activity(
            measure_cadence='evento',
            measure_domain="[('name', '=', 'nada-fase10')]")
        bare._sgi_measure()
        self.assertFalse(bare.measure_state)

    def test_04_bad_date_field_falls_back(self):
        act = self._activity(measure_date_field='campo_inexistente')
        act._sgi_measure()
        self.assertIn(act.measure_state, ('verde', 'rojo'))

    def test_05_process_rollup(self):
        ok = self._activity(measure_domain='[]')
        bad = self._activity(
            measure_domain="[('name', '=', 'no-existe-rollup-xyz')]")
        (ok | bad)._sgi_measure()
        self.assertEqual(self.process.measurable_activity_count, 2)
        self.assertEqual(self.process.measure_red_count, 1)
        self.assertEqual(self.process.procedure_compliance, 50)

    def test_06_measure_write_does_not_flag_dirty(self):
        doc = self.env['documents.document'].create({
            'name': 'P-TST-MEAS PROCEDIMIENTO', 'type': 'binary',
            'sgi_doc_type': 'procedimiento', 'sgi_state': 'vigente',
            'sgi_process_id': self.process.id,
        })
        self.assertFalse(doc.sgi_procedure_dirty)
        act = self._activity()  # crear sí marca dirty (contenido real)
        self.assertTrue(doc.sgi_procedure_dirty)
        doc.write({'sgi_procedure_dirty': False})
        # El refresco de medición NO debe volver a marcarlo.
        act._sgi_measure()
        act.write({'measure_cadence': 'semanal'})
        self.assertFalse(doc.sgi_procedure_dirty)
        # Un cambio de contenido sí.
        act.write({'description': 'cambio de fondo'})
        self.assertTrue(doc.sgi_procedure_dirty)

    def test_08_model_name_inverse(self):
        act = self.env['sgi.process.activity'].create({
            'process_id': self.process.id, 'name': 'Por nombre técnico',
            'measure_model_name': 'sale.order'})
        self.assertEqual(act.measure_model_id.model, 'sale.order')
        act.write({'measure_model_name': 'modelo.inexistente'})
        self.assertFalse(act.measure_model_id)

    def test_07_evidence_action(self):
        act = self._activity(measure_domain='[]')
        action = act.action_view_measure_records()
        self.assertEqual(action['res_model'], 'res.partner')
        bare = self.env['sgi.process.activity'].create({
            'process_id': self.process.id, 'name': 'Sin medición'})
        with self.assertRaises(UserError):
            bare.action_view_measure_records()

    def test_10_activity_chain(self):
        """El encadenamiento liga pasos con su entregable, cruza procesos y
        el botón «Siguiente paso» navega la cadena."""
        other = self.env['sgi.process'].create({
            'code': 'P-TST-CHAIN', 'name': 'Proceso destino'})
        a = self._activity(name='Paso A')
        b = self.env['sgi.process.activity'].create({
            'process_id': other.id, 'name': 'Paso B'})
        link = self.env['sgi.activity.link'].create({
            'from_activity_id': a.id, 'to_activity_id': b.id,
            'name': 'Entregable X'})
        self.assertTrue(link.is_cross_process)
        self.assertEqual(a.next_activity_ids, b)
        self.assertEqual(b.prev_activity_ids, a)
        action = a.action_open_next()
        self.assertEqual(action['res_id'], b.id)
        # La ficha del proceso ve su cadena (entradas y salidas).
        self.assertEqual(self.process.chain_link_count, 1)
        self.assertEqual(other.chain_link_count, 1)
        chain_action = self.process.action_view_chain()
        self.assertEqual(chain_action['res_model'], 'sgi.activity.link')
        with self.assertRaises(UserError):
            b.action_open_next()
        from odoo.exceptions import ValidationError as VE
        with self.assertRaises(VE):
            self.env['sgi.activity.link'].create({
                'from_activity_id': a.id, 'to_activity_id': a.id,
                'name': 'Ciclo'})

    def test_11_inbound_references(self):
        """Si otro proceso menciona mi procedimiento o usa mi formato, yo lo
        veo en «Me referencian» sin capturar nada dos veces."""
        mine = self.env['sgi.process'].create({
            'code': 'P-TST-REF', 'name': 'Proceso citado'})
        my_proc_doc = self.env['documents.document'].create({
            'name': 'P-TST-REF PROCEDIMIENTO', 'type': 'binary',
            'sgi_doc_type': 'procedimiento', 'sgi_process_id': mine.id})
        my_format = self.env['documents.document'].create({
            'name': 'F-P-TST-REF-01', 'type': 'binary',
            'sgi_doc_type': 'formato', 'sgi_process_id': mine.id})
        citing = self.env['sgi.process.activity'].create({
            'process_id': self.process.id, 'name': 'Cita procedimiento ajeno',
            'related_procedure_id': my_proc_doc.id})
        using = self.env['sgi.process.activity'].create({
            'process_id': self.process.id, 'name': 'Usa formato ajeno',
            'format_document_ids': [(6, 0, [my_format.id])]})
        self.assertEqual(mine.inbound_reference_count, 2)
        self.assertEqual(set(mine.inbound_reference_ids.ids),
                         {citing.id, using.id})
        action = mine.action_view_inbound_references()
        self.assertEqual(action['res_model'], 'sgi.process.activity')
        # Las actividades del propio proceso no cuentan como referencia.
        self.env['sgi.process.activity'].create({
            'process_id': mine.id, 'name': 'Auto-cita',
            'related_procedure_id': my_proc_doc.id})
        self.assertEqual(mine.inbound_reference_count, 2)

    def test_09_menu_resolution_from_text(self):
        """El texto «App → Menú» se resuelve al menú real, y sin menú el
        paso abre la evidencia (nunca se queda en texto plano)."""
        action = self.env['ir.actions.act_window'].create({
            'name': 'Prueba fase10', 'res_model': 'res.partner'})
        root = self.env['ir.ui.menu'].create({'name': 'AppFase10'})
        menu = self.env['ir.ui.menu'].create({
            'name': 'PasoFase10', 'parent_id': root.id,
            'action': 'ir.actions.act_window,%d' % action.id})
        act = self._activity(odoo_ref='AppFase10 → PasoFase10')
        act._sgi_resolve_menu()
        self.assertEqual(act.odoo_menu_id, menu)
        # Fallback: sin menú pero con medición, abrir lleva a la evidencia.
        fallback = self._activity(odoo_ref='Ruta → Inexistente')
        result = fallback.action_open_odoo()
        self.assertEqual(result['res_model'], 'res.partner')
