# -*- coding: utf-8 -*-
"""El nodo nomina12:HorasExtra: el criterio de Dias medido en NOI, el reparto
del importe entre dobles y triples, los casos en que NO se emite nada, la
anotación por índice sobre percepcion_list y la vista que se configura sola.

No corre en el CI (depende de Enterprise); se corre en el shell de Odoo.sh:
``odoo-bin ... --test-tags /quimibond_nomina --stop-after-init``."""
from datetime import date

from odoo.tests.common import TransactionCase
from odoo.tools import mute_logger

from odoo.addons.quimibond_nomina.models import horas_extra as he
from odoo.addons.quimibond_nomina.models.hr_payslip import KEY_HORAS_EXTRA, TEMPLATE_XMLID, VIEW_XMLID

LOG = 'odoo.addons.quimibond_nomina.models.hr_payslip'


class TestHorasExtraCalculo(TransactionCase):
    """Cálculo puro (sin recibos)."""

    def test_dias_semanal(self):
        # 3 a 9 horas → 3 días (116 de 142 casos en NOI)
        for horas in (3, 4, 6, 9):
            self.assertEqual(he.horas_extra_dias(horas, 7), 3)
        # 1 o 2 horas sueltas → 1 día (5 casos)
        self.assertEqual(he.horas_extra_dias(1, 7), 1)
        self.assertEqual(he.horas_extra_dias(2, 7), 1)

    def test_dias_quincenal(self):
        # 18 horas en quincena → 6 días (19 casos); 15 o 16 días de periodo
        self.assertEqual(he.horas_extra_dias(18, 15), 6)
        self.assertEqual(he.horas_extra_dias(18, 16), 6)
        self.assertEqual(he.horas_extra_dias(2, 15), 1)
        # nunca más días que horas
        self.assertEqual(he.horas_extra_dias(4, 15), 4)

    def test_nodo_dobles(self):
        nodos = he.horas_extra_nodos({'01': 9}, 746.56, 7)
        self.assertEqual(nodos, [{'tipo_horas': '01', 'horas_extra': 9, 'dias': 3, 'importe_pagado': '746.56'}])

    def test_nodo_triples(self):
        nodos = he.horas_extra_nodos({'02': 3}, 500.0, 7)
        self.assertEqual(nodos, [{'tipo_horas': '02', 'horas_extra': 3, 'dias': 3, 'importe_pagado': '500.00'}])

    def test_dobles_y_triples_reparten_el_importe(self):
        nodos = he.horas_extra_nodos({'01': 6, '02': 3}, 1000.0, 7)
        self.assertEqual([n['tipo_horas'] for n in nodos], ['01', '02'])
        # 6h × 2 = 12 y 3h × 3 = 9 → 12/21 y 9/21 del total; la suma es exacta
        self.assertEqual(nodos[0]['importe_pagado'], '571.43')
        self.assertEqual(nodos[1]['importe_pagado'], '428.57')
        self.assertEqual(round(sum(float(n['importe_pagado']) for n in nodos), 2), 1000.0)
        self.assertEqual([n['dias'] for n in nodos], [3, 3])

    def test_una_y_dos_horas(self):
        self.assertEqual(he.horas_extra_nodos({'01': 1}, 80.0, 7)[0]['dias'], 1)
        self.assertEqual(he.horas_extra_nodos({'01': 2}, 160.0, 7)[0]['dias'], 1)

    def test_sin_horas(self):
        self.assertEqual(he.horas_extra_nodos({}, 100.0, 7), [])
        self.assertEqual(he.horas_extra_nodos({'01': 0, '02': 0}, 100.0, 7), [])

    def test_semanal_vs_quincenal(self):
        semanal = he.horas_extra_nodos({'01': 9}, 746.56, 7)[0]
        quincenal = he.horas_extra_nodos({'01': 18}, 1493.12, 15)[0]
        self.assertEqual(semanal['dias'], 3)
        self.assertEqual(quincenal['dias'], 6)

    def test_reconoce_percepcion_019(self):
        self.assertTrue(he.es_percepcion_019({'tipo_percepcion': '019', 'clave': 'P19'}))
        self.assertFalse(he.es_percepcion_019({'tipo_percepcion': '001', 'importe_gravado': '019'}))
        self.assertEqual(he.importe_de_percepcion({'importe_gravado': 373.28, 'importe_exento': 373.28}), 746.56)
        self.assertIsNone(he.importe_de_percepcion({'clave': 'P19'}))


class TestHorasExtraRecibo(TransactionCase):
    """Anotación sobre cfdi_values desde un recibo con entradas y líneas."""

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        env = cls.env
        stype = env['hr.payroll.structure.type'].create({'name': 'QB HE prueba'})
        cls.struct = env['hr.payroll.structure'].create({'name': 'QB HE prueba', 'code': 'QB_HE', 'type_id': stype.id})
        cat = env.ref('hr_payroll.BASIC')
        cls.rules = {}
        for code in ('HE_EXEMPT', 'HE_TAX'):
            cls.rules[code] = env['hr.salary.rule'].create({
                'name': code, 'code': code, 'struct_id': cls.struct.id, 'category_id': cat.id,
                'condition_select': 'none', 'amount_select': 'code', 'amount_python_compute': 'result = 0',
            })
        InputType = env['hr.payslip.input.type']
        cls.inputs = {}
        for code, name in (('HE_DOBLE', 'Horas extra dobles'), ('HE_TRIPLE', 'Horas extra triples')):
            itype = InputType.search([('code', '=', code)], limit=1)
            if not itype:
                itype = InputType.create({'name': name, 'code': code})
            cls.inputs[code] = itype
        cls.employee = env['hr.employee'].create({'name': 'Prueba horas extra'})

    def _recibo(self, horas=None, lineas=None, date_from=date(2026, 9, 14), date_to=date(2026, 9, 20)):
        slip = self.env['hr.payslip'].create({
            'name': 'Recibo de prueba', 'employee_id': self.employee.id,
            'struct_id': self.struct.id, 'date_from': date_from, 'date_to': date_to,
            'input_line_ids': [(0, 0, {'input_type_id': self.inputs[c].id, 'amount': h})
                               for c, h in (horas or {}).items()],
        })
        for code, amount in (lineas or {}).items():
            self.env['hr.payslip.line'].create({
                'slip_id': slip.id, 'salary_rule_id': self.rules[code].id, 'name': code,
                'amount': amount, 'quantity': 1.0, 'rate': 100.0, 'sequence': 5,
            })
        return slip

    @staticmethod
    def _cv(con_019=True, partida=False):
        """percepcion_list como la arma Odoo (llaves reales). ``partida``: las
        dos 019 separadas, una por regla, que es lo que sale hoy."""
        lista = [{'tipo_percepcion': '001', 'clave': 'P01', 'concepto': 'Sueldos',
                  'importe_gravado': 3000.0, 'importe_exento': 0.0}]
        if con_019 and partida:
            lista.append({'tipo_percepcion': '019', 'clave': 'P19_2', 'concepto': 'Horas extra exento',
                          'importe_gravado': 0, 'importe_exento': 373.28})
            lista.append({'tipo_percepcion': '019', 'clave': 'P19', 'concepto': 'Horas extra',
                          'importe_gravado': 373.28, 'importe_exento': 0})
        elif con_019:
            lista.append({'tipo_percepcion': '019', 'clave': 'P19', 'concepto': 'Horas extra',
                          'importe_gravado': 373.28, 'importe_exento': 373.28})
        return {'percepcion_list': lista}

    def test_dobles_semanal(self):
        slip = self._recibo({'HE_DOBLE': 9}, {'HE_EXEMPT': 373.28, 'HE_TAX': 373.28})
        cv = self._cv()
        slip._qb_add_horas_extra(cv)
        self.assertEqual(cv[KEY_HORAS_EXTRA], {
            1: [{'tipo_horas': '01', 'horas_extra': 9, 'dias': 3, 'importe_pagado': '746.56'}]})
        # percepcion_list no se toca: gravado y exento siguen igual
        self.assertEqual(cv['percepcion_list'][1]['importe_gravado'], 373.28)

    def test_quincenal_emite_seis_dias(self):
        slip = self._recibo({'HE_DOBLE': 18}, {'HE_EXEMPT': 746.56, 'HE_TAX': 746.56},
                            date_from=date(2026, 9, 16), date_to=date(2026, 9, 30))
        cv = self._cv()
        slip._qb_add_horas_extra(cv)
        self.assertEqual(cv[KEY_HORAS_EXTRA][1][0]['dias'], 6)

    def test_sin_horas_no_cambia_nada(self):
        slip = self._recibo()
        cv = self._cv(con_019=False)
        slip._qb_add_horas_extra(cv)
        self.assertEqual(cv[KEY_HORAS_EXTRA], {})

    @mute_logger(LOG)
    def test_percepcion_019_sin_horas_capturadas(self):
        slip = self._recibo(lineas={'HE_EXEMPT': 373.28, 'HE_TAX': 373.28})
        cv = self._cv()
        slip._qb_add_horas_extra(cv)
        self.assertEqual(cv[KEY_HORAS_EXTRA], {})

    @mute_logger(LOG)
    def test_horas_sin_percepcion_019(self):
        slip = self._recibo({'HE_DOBLE': 9}, {'HE_EXEMPT': 373.28, 'HE_TAX': 373.28})
        cv = self._cv(con_019=False)
        slip._qb_add_horas_extra(cv)
        self.assertEqual(cv[KEY_HORAS_EXTRA], {})

    @mute_logger(LOG)
    def test_horas_sin_lineas(self):
        slip = self._recibo({'HE_DOBLE': 9})
        cv = self._cv()
        slip._qb_add_horas_extra(cv)
        self.assertEqual(cv[KEY_HORAS_EXTRA], {})

    def test_dos_percepciones_019_se_fusionan_en_una(self):
        """Lo que sale hoy de Odoo: P19_2 exenta y P19 gravada por separado.
        Debe quedar UNA 019 con los dos importes y un solo nodo con el total."""
        slip = self._recibo({'HE_DOBLE': 9}, {'HE_EXEMPT': 581.77, 'HE_TAX': 581.77})
        cv = self._cv(partida=True)
        cv['percepcion_list'][1]['importe_exento'] = 581.77
        cv['percepcion_list'][2]['importe_gravado'] = 581.77
        slip._qb_add_horas_extra(cv)
        lista = cv['percepcion_list']
        p019 = [p for p in lista if p['tipo_percepcion'] == '019']
        self.assertEqual(len(p019), 1)
        self.assertEqual(p019[0]['clave'], 'P19')                 # la gravada manda
        self.assertEqual(p019[0]['concepto'], 'Horas extra')
        self.assertEqual(p019[0]['importe_gravado'], 581.77)
        self.assertEqual(p019[0]['importe_exento'], 581.77)
        self.assertEqual(lista[0]['clave'], 'P01')               # las demás no se tocan
        self.assertEqual(len(lista), 2)
        self.assertEqual(cv[KEY_HORAS_EXTRA], {1: [
            {'tipo_horas': '01', 'horas_extra': 9, 'dias': 3, 'importe_pagado': '1163.54'}]})

    def test_fusion_es_idempotente_y_solo_019(self):
        slip = self._recibo({'HE_DOBLE': 9})
        cv = self._cv(partida=True)
        cv['percepcion_list'].append({'tipo_percepcion': '029', 'clave': 'P29', 'concepto': 'Vales',
                                      'importe_gravado': 0, 'importe_exento': 100.0})
        cv['percepcion_list'].append({'tipo_percepcion': '029', 'clave': 'P29_2', 'concepto': 'Vales exento',
                                      'importe_gravado': 50.0, 'importe_exento': 0})
        self.assertEqual(slip._qb_fusionar_percepciones_019(cv), 1)
        antes = [dict(p) for p in cv['percepcion_list']]
        self.assertEqual(slip._qb_fusionar_percepciones_019(cv), 1)   # segunda vez: nada cambia
        self.assertEqual(cv['percepcion_list'], antes)
        self.assertEqual([p['clave'] for p in cv['percepcion_list']], ['P01', 'P19', 'P29', 'P29_2'])
        self.assertIsNone(slip._qb_fusionar_percepciones_019({'percepcion_list': []}))
        self.assertIsNone(slip._qb_fusionar_percepciones_019({}))

    def test_importe_pagado_cae_a_las_lineas_si_la_percepcion_no_trae_importe(self):
        slip = self._recibo({'HE_DOBLE': 9}, {'HE_EXEMPT': 373.28, 'HE_TAX': 373.28})
        cv = {'percepcion_list': [{'tipo_percepcion': '019', 'clave': 'P19', 'concepto': 'Horas extra'}]}
        slip._qb_add_horas_extra(cv)
        self.assertEqual(cv[KEY_HORAS_EXTRA][0][0]['importe_pagado'], '746.56')

    def test_dobles_y_triples_dos_nodos_en_la_misma_019(self):
        slip = self._recibo({'HE_DOBLE': 6, 'HE_TRIPLE': 3})
        cv = self._cv()
        cv['percepcion_list'][1].update(importe_gravado=500.0, importe_exento=500.0)
        slip._qb_add_horas_extra(cv)
        nodos = cv[KEY_HORAS_EXTRA][1]
        self.assertEqual([(n['tipo_horas'], n['horas_extra']) for n in nodos], [('01', 6), ('02', 3)])
        self.assertEqual(round(sum(float(n['importe_pagado']) for n in nodos), 2), 1000.0)

    def test_conceptos_en_espanol(self):
        if 'es_MX' not in [c for c, _ in self.env['res.lang'].get_installed()]:
            self.skipTest('es_MX no está instalado')
        Concept = self.env['l10n.mx.concept']
        concept = Concept.search([('payroll_code', '=', 'P19')], limit=1)
        if not concept:
            self.skipTest('no hay concepto P19')
        esperado = concept.with_context(lang='es_MX').name
        slip = self._recibo()
        cv = {'percepcion_list': [{'tipo_percepcion': '019', 'clave': 'P19', 'concepto': 'Overtime',
                                   'importe_gravado': 1.0, 'importe_exento': 0.0}],
              'deduccion_list': [{'clave': 'D_INEXISTENTE', 'concepto': 'Whatever'}]}
        slip._qb_conceptos_en_espanol(cv)
        self.assertEqual(cv['percepcion_list'][0]['concepto'], esperado)
        self.assertEqual(cv['deduccion_list'][0]['concepto'], 'Whatever')   # sin registro: se queda


class TestHorasExtraVista(TransactionCase):
    """La herencia de la plantilla se configura sola y se apaga si Odoo ya
    emite el nodo."""

    def test_estado_tras_instalar(self):
        base = self.env.ref(TEMPLATE_XMLID)
        mine = self.env.ref(VIEW_XMLID).with_context(active_test=False)
        if 'HorasExtra' in (base.arch_db or ''):
            # Odoo corrigió el defecto de origen: no debemos duplicar el nodo.
            self.assertFalse(mine.active)
            return
        self.assertTrue(mine.active, 'la herencia HorasExtra debería estar activa')
        self.assertIn('nomina12:HorasExtra', mine.arch_db)
        self.assertIn(KEY_HORAS_EXTRA, mine.arch_db)
        combinada = base.get_combined_arch()
        self.assertIn('HorasExtra', combinada)

    def test_idempotente(self):
        mine = self.env.ref(VIEW_XMLID).with_context(active_test=False)
        antes = (mine.active, mine.arch_db)
        self.env['hr.payslip'].qb_nomina_ensure_horas_extra_view()
        self.assertEqual((mine.active, mine.arch_db), antes)
