# -*- coding: utf-8 -*-
from odoo.tests import TransactionCase, tagged


@tagged('post_install', '-at_install')
class TestFlows48(TransactionCase):
    """Mini-fase 4.8: mapa de entradas/salidas completo por proceso."""

    def _flow(self, xmlid):
        return self.env.ref('quimibond_sgi.%s' % xmlid)

    def test_01_flujos_nuevos_con_modelo(self):
        expected = {
            'flow_almacenmp_compras_req': 'approval.request',
            'flow_plan_prodent': 'mrp.production',
            'flow_prodtac_tintoreria': 'mrp.production',
            'flow_tintoreria_prodtac': 'mrp.production',
            'flow_prodent_inspeccion': 'mrp.production',
            'flow_almacenmp_lab': 'quality.check',
            'flow_lab_almacenmp': 'quality.check',
            'flow_prodtac_mto': 'maintenance.request',
            'flow_inspeccion_sgi_nc': 'quality.alert',
            'flow_ventas_cal_reclamo': 'helpdesk.ticket',
            'flow_sgi_compras_prov': 'sgi.supplier.eval',
            'flow_direccion_sgi_acuerdos': 'sgi.management.review',
            'flow_mfg_sgi_incidente': 'sgi.incident',
            'flow_diseno_mfg': 'mrp.bom',
        }
        for xmlid, model in expected.items():
            flow = self._flow(xmlid)
            self.assertEqual(flow.odoo_model_name, model,
                             "El flujo %s debe materializarse en %s" % (xmlid, model))

    def test_02_ver_registros_abre_el_modelo(self):
        action = self._flow('flow_prodtac_mto').action_view_records()
        self.assertEqual(action['res_model'], 'maintenance.request')

    def test_03_todo_proceso_operativo_conectado(self):
        """Ningún proceso hoja del mapa queda aislado (sin entradas ni salidas)."""
        Process = self.env['sgi.process']
        leaves = Process.search([('child_ids', '=', False)])
        for proc in leaves:
            self.assertTrue(
                proc.in_flow_ids or proc.out_flow_ids,
                "El proceso %s quedó aislado en el mapa (sin flujos)" % proc.name)

    def test_04_tintoreria_encadenada(self):
        """El caso que faltaba: tintorería con entrada (tejido) y salida (acabado)."""
        tint = self.env.ref('quimibond_sgi.proc_tintoreria')
        self.assertTrue(tint.in_flow_ids)
        self.assertTrue(tint.out_flow_ids)

    def test_05_ficha_de_proceso(self):
        """La ficha muestra lo ligado: documentos, indicadores, riesgos, modelos."""
        proc = self.env.ref('quimibond_sgi.proc_ventas')
        doc = self.env['documents.document'].create({
            'name': 'F-P-A28-12 COTIZACION.xlsx', 'type': 'binary',
            'sgi_is_controlled': True, 'sgi_doc_type': 'formato',
            'sgi_code': 'F-P-A28-12', 'sgi_state': 'vigente',
            'sgi_process_id': proc.id,
        })
        self.assertIn(doc, proc.linked_document_ids)
        self.assertGreaterEqual(proc.document_count, 1)
        self.assertTrue(proc.odoo_model_ids, "Ventas debe mostrar módulos conectados")
        action = proc.action_open_documents()
        self.assertEqual(action['res_model'], 'documents.document')

    def test_06_familia_documental_por_clave(self):
        """P-A28 ve a sus hijos; el formato ve a su procedimiento padre."""
        Doc = self.env['documents.document']
        proc = Doc.create({
            'name': 'P-A28 VENTAS.pdf', 'type': 'binary',
            'sgi_is_controlled': True, 'sgi_doc_type': 'procedimiento',
            'sgi_code': 'P-A28', 'sgi_state': 'vigente',
        })
        fmt = Doc.create({
            'name': 'F-P-A28-12 COTIZACION.xlsx', 'type': 'binary',
            'sgi_is_controlled': True, 'sgi_doc_type': 'formato',
            'sgi_code': 'F-P-A28-12', 'sgi_state': 'vigente',
        })
        it = Doc.create({
            'name': 'IT-P-A28-01 PEDIDOS ODOO.pdf', 'type': 'binary',
            'sgi_is_controlled': True, 'sgi_doc_type': 'instructivo',
            'sgi_code': 'IT-P-A28-01', 'sgi_state': 'vigente',
        })
        # H21: la familia es por FK; la migración por nomenclatura llena el
        # enlace (equivalente a lo que antes hacía la regex en cada lectura).
        self.env['sgi.config'].migrate_document_families()
        self.assertIn(fmt, proc.sgi_family_document_ids)
        self.assertIn(it, proc.sgi_family_document_ids)
        self.assertEqual(fmt.sgi_parent_document_id, proc)
        self.assertEqual(it.sgi_parent_document_id, proc)
        self.assertFalse(proc.sgi_parent_document_id)

    def test_07_nc_no_cierra_sin_acciones(self):
        """H1 de la auditoría ISO: cero acciones ya no pasa el candado."""
        from odoo.exceptions import UserError
        from datetime import date
        team = self.env.ref('quimibond_sgi.sgi_quality_team_internal')
        stage = self.env.ref('quimibond_sgi.sgi_nc_int_stage_closed')
        alert = self.env['quality.alert'].create({
            'title': 'NC sin acciones', 'team_id': team.id,
            'sgi_root_cause': 'Causa identificada',
            'sgi_effectiveness_note': 'Eficaz',
            'sgi_effectiveness_date': date.today(),
        })
        with self.assertRaises(UserError):
            alert.write({'stage_id': stage.id})

    def test_08_medicion_validada_inmutable(self):
        """H6: el valor de una medición validada no se edita sin privilegio."""
        from odoo.exceptions import UserError
        from datetime import date
        indicator = self.env['sgi.indicator'].create({
            'code': 'TST-LOCK', 'name': 'KPI candado', 'calc_mode': 'manual',
            'responsible_id': self.env.user.id,
        })
        measure = self.env['sgi.indicator.measure'].create({
            'indicator_id': indicator.id,
            'period_date': date.today().replace(day=1), 'value': 90.0,
        })
        measure.action_validate()
        user = self.env['res.users'].create({
            'name': 'Usuario SGI Raso', 'login': 'raso_sgi_test',
            'group_ids': [(6, 0, [self.env.ref('quimibond_sgi.group_sgi_user').id,
                                  self.env.ref('base.group_user').id])],
        })
        with self.assertRaises(UserError):
            measure.with_user(user).write({'value': 95.0})
