# -*- coding: utf-8 -*-
"""Entrega 3 (H-022/H-007): ``export_payload`` es el inverso exacto de
``load_payload``. Exportar y volver a cargar en modo de prueba reporta cero
cambios; cargar en una base «limpia» (aquí: con los registros originales
renombrados y archivados) reproduce el mismo mapa; los IDs fijos de los
filtros viajan como referencias portables."""
import copy

from odoo.tests import TransactionCase, tagged

from ..models import sgi_domain_refs


def _strip_meta(payload):
    out = copy.deepcopy(payload)
    out.pop('meta', None)
    return out


@tagged('post_install', '-at_install')
class TestExportPayload(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        Job = cls.env['hr.job']
        cls.job_a = Job.create({'name': 'PUESTO EXPORTA A'})
        cls.job_b = Job.create({'name': 'PUESTO EXPORTA B'})
        cls.env['hr.employee'].create([
            {'name': 'Emp XA', 'job_id': cls.job_a.id},
            {'name': 'Emp XB', 'job_id': cls.job_b.id}])
        cls.Process = cls.env['sgi.process']
        cls.company = cls.env.company
        cls.partner = cls.env['res.partner'].create({'name': 'XE Cliente Exporta Uno'})
        cls.out_type = cls.env.ref('stock.picking_type_out')
        cls.payload = {
            'families': [{'code': 'XE-FAM', 'name': 'Familia exporta',
                          'jobs': ['PUESTO EXPORTA B']}],
            'deliverables': [
                {'code': 'XE-PED', 'name': 'Pedido exporta', 'model': 'res.partner',
                 'domain': "[('is_company', '=', True), ('company_id', 'in', [False, %d])]"
                           % cls.company.id,
                 'date_field': 'create_date', 'user_field': 'create_uid'},
                {'code': 'XE-SAL', 'name': 'Salida exporta', 'model': 'stock.picking',
                 'domain': "[('picking_type_id', '=', %d), ('state', '=', 'done')]"
                           % cls.out_type.id,
                 'complete_domain': "[('partner_id', 'child_of', [%d])]" % cls.partner.id,
                 'date_field': 'date_done', 'boundary': 'salida_final'},
            ],
            'objectives': [{'name': 'XE objetivo', 'description': 'Prueba', 'target_year': 2030}],
            'processes': [{
                'code': 'XE1', 'name': 'Exporta uno', 'type': 'soporte',
                'purpose': 'Probar la ida y vuelta',
                'activities': [
                    {'number': 'XE1.01', 'name': 'Capturar', 'stage': 'A. Captura',
                     'sequence': 10, 'outputs': ['XE-PED'],
                     'measure': {'method': 'entregable', 'deliverable': 'XE-PED'},
                     'where': {'channel': 'odoo', 'menu': 'base.menu_administration'},
                     'roles': [{'role': 'ejecuta', 'job': 'PUESTO EXPORTA A', 'sequence': 40},
                               {'role': 'escala', 'job': 'PUESTO EXPORTA B', 'after_days': 2,
                                'sequence': 50}]},
                    {'number': 'XE1.04', 'name': 'Entregar', 'stage': 'B. Entrega',
                     'sequence': 20, 'outputs': ['XE-SAL'],
                     'inputs': [{'code': 'XE-PED', 'days': 2,
                                 'applies_domain': "[('id', 'in', [%d])]" % cls.partner.id}],
                     'due': {'weekday': 4},
                     'measure': {'method': 'consecuencia', 'proxy': 'XE1.01'},
                     'roles': [{'role': 'ejecuta', 'family': 'XE-FAM'},
                               {'role': 'aprueba', 'relative': 'dueno_proceso'}]},
                ],
            }],
            'indicators': [{
                'code': 'XE-IND', 'name': 'Indicador exporta', 'process': 'XE1',
                'objective': 'XE objetivo', 'direction': 'up', 'target': 90.0,
                'terms': [{'role': 'numerator', 'model': 'res.partner',
                           'domain': "[('company_id', '=', %d)]" % cls.company.id,
                           'date_field': 'create_date', 'aggregation': 'count',
                           'window': 'period'}],
            }],
        }
        result = cls.Process.load_payload(copy.deepcopy(cls.payload))
        assert result['ok'], result['errors']

    def _export(self):
        return self.Process.export_payload(process_codes=['XE1'])

    # ------------------------------------------------------------------
    def test_01_domains_become_portable_refs(self):
        exported = self._export()
        by_code = {d['code']: d for d in exported['deliverables']}
        ped, sal = by_code['XE-PED'], by_code['XE-SAL']
        self.assertEqual(ped['domain'], "[('is_company', '=', True), ('company_id', 'in', "
                                        "[False, %(r1)s])]")
        self.assertEqual(ped['refs']['r1'], {'model': 'res.company', 'company': True})
        self.assertIn('%(r1)s', sal['domain'])
        self.assertEqual(sal['refs']['r1'], {'model': 'stock.picking.type',
                                             'xmlid': 'stock.picking_type_out'})
        self.assertEqual(sal['refs']['r2']['key'], {'name': 'XE Cliente Exporta Uno'})
        self.assertEqual(sal['boundary'], 'salida_final')
        act = exported['processes'][0]['activities'][1]
        self.assertEqual(act['roles'][0], {'role': 'ejecuta', 'family': 'XE-FAM',
                                           'sequence': 10})
        self.assertIn('%(r1)s', act['inputs'][0]['applies_domain'])
        self.assertEqual(act['measure'], {'method': 'consecuencia', 'proxy': 'XE1.01'})
        self.assertEqual(exported['families'][0]['jobs'], ['PUESTO EXPORTA B'])
        term = exported['indicators'][0]['terms'][0]
        self.assertEqual(term['refs']['r1'], {'model': 'res.company', 'company': True})
        self.assertEqual(exported['meta']['counts']['activities'], 2)

    def test_02_export_then_dry_run_reports_zero_changes(self):
        exported = self._export()
        result = self.Process.load_payload(exported, dry_run=True)
        self.assertTrue(result['ok'], result['errors'])
        self.assertFalse(result['changes'], "Exportar y volver a cargar no cambia nada: %s"
                         % result['changes'])

    def test_03_load_in_clean_base_reproduces_the_map(self):
        exported = self._export()
        # «Base limpia»: los originales se renombran y archivan (no se borran).
        old = self.Process.search([('code', '=', 'XE1')])
        old.write({'code': 'XE1-VIEJO'})
        for deliverable in self.env['sgi.deliverable'].search([('code', 'in', ['XE-PED', 'XE-SAL'])]):
            deliverable.code = '%s-VIEJO' % deliverable.code
        self.env['sgi.job.family'].search([('code', '=', 'XE-FAM')]).write(
            {'code': 'XE-FAM-VIEJA', 'active': False})
        self.env['sgi.indicator'].search([('code', '=', 'XE-IND')]).write({'code': 'XE-IND-VIEJO'})
        self.env['sgi.objective'].search([('name', '=', 'XE objetivo')]).write({'name': 'XE viejo'})
        first = self.Process.load_payload(copy.deepcopy(exported))
        self.assertTrue(first['ok'], first['errors'])
        new = self.Process.search([('code', '=', 'XE1')])
        self.assertTrue(new and new != old)
        self.assertEqual(len(new.procedure_activity_ids), 2)
        self.assertEqual(len(new.stage_ids), 2)
        self.assertEqual(_strip_meta(self._export()), _strip_meta(exported),
                         "Lo que se carga en limpio se vuelve a exportar igual.")
        second = self.Process.load_payload(copy.deepcopy(exported))
        self.assertFalse(second['changes'], "Cargar dos veces no duplica ni cambia nada.")
        self.assertEqual(self.Process.search_count([('code', '=', 'XE1')]), 1)

    def test_04_missing_ref_is_warning_and_inert(self):
        exported = self._export()
        sal = next(d for d in exported['deliverables'] if d['code'] == 'XE-SAL')
        sal['refs']['r1'] = {'model': 'stock.picking.type', 'key': {'name': 'No existe XE'}}
        result = self.Process.load_payload(exported, dry_run=True)
        self.assertTrue(result['ok'], result['errors'])
        self.assertTrue(any('XE-SAL' == w['key'] and 'sin medir' in w['message']
                            for w in result['warnings']), result['warnings'])
        self.assertIn('measure_domain', next(
            c for c in result['changes'] if c['key'] == 'XE-SAL')['fields'])

    def test_05_templatize_keeps_the_text(self):
        text = "[('a_id', 'in', [3, 45]), ('b', '=', 'ñ'), ('c_id', 'not in', [False, 7])]"
        template, used = sgi_domain_refs.templatize(text, lambda f, v: 'r%d' % v)
        self.assertEqual(used, ['r3', 'r45', 'r7'])
        self.assertEqual(sgi_domain_refs.fill(template, {'r3': 3, 'r45': 45, 'r7': 7})[0], text)
        self.assertEqual(sgi_domain_refs.fill(template, {'r3': 3})[1], ['r45', 'r7'])
