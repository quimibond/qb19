# -*- coding: utf-8 -*-
from odoo import Command
from odoo.tests import tagged
from odoo.tests.common import TransactionCase


@tagged('post_install', '-at_install', 'quimibond_rollos_bitacora')
class TestClasificacionFinal(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.kg = cls.env.ref('uom.product_uom_kgm')
        cls.metro = cls.env.ref('uom.product_uom_meter')
        cls.prod_a = cls._producto('IWJ038Q22JNT160', cls.kg)
        cls.prod_b = cls._producto('WJ038Q22JNT160', cls.metro)
        bom_a = cls.env['mrp.bom'].create({
            'product_tmpl_id': cls.prod_a.product_tmpl_id.id, 'product_qty': 1, 'product_uom_id': cls.kg.id})
        cls.orden_a = cls.env['mrp.production'].create({
            'product_id': cls.prod_a.id, 'product_qty': 100, 'bom_id': bom_a.id})
        bom_b = cls.env['mrp.bom'].create({
            'product_tmpl_id': cls.prod_b.product_tmpl_id.id, 'product_qty': 1, 'product_uom_id': cls.metro.id,
            'bom_line_ids': [Command.create({'product_id': cls.prod_a.id, 'product_qty': 0.0635})]})
        cls.orden_b = cls.env['mrp.production'].create({
            'product_id': cls.prod_b.id, 'product_qty': 100, 'bom_id': bom_b.id})
        cls.centro = cls.env['mrp.workcenter'].create({'name': 'EMPAQUE TEST', 'code': 'EMPTEST'})
        cls.workorder = cls.env['mrp.workorder'].create({
            'name': 'CLASIFICACION', 'production_id': cls.orden_b.id, 'workcenter_id': cls.centro.id})
        lote = cls.env['stock.lot'].create({
            'name': 'CLASTEST-0001', 'product_id': cls.prod_a.id, 'company_id': cls.env.company.id})
        cls.rollo = cls.env['mrp.rollo.bitacora'].create({
            'production_id': cls.orden_a.id, 'secuencia': 1, 'kg_rama': 33.0, 'lot_id': lote.id,
            'estado_inspeccion': 'inspeccionado', 'clasificacion_inicial': 'PQ', 'metros_insp': 520.0,
            'operador_insp': '99999999 - OPERADOR PRUEBA'})

    @classmethod
    def _producto(cls, codigo, uom):
        return cls.env['product.product'].create({
            'name': codigo, 'default_code': codigo, 'type': 'consu', 'is_storable': True,
            'uom_id': uom.id, 'tracking': 'lot'})

    def _final(self, clasificacion, defectos=()):
        """Rollo final (3.16.0: la etiqueta final vive aquí, no en la bitácora) y su ZPL."""
        final = self.env['mrp.rollo.final'].create({
            'name': 'CLASTEST-0001', 'secuencia': 1, 'production_id': self.orden_a.id,
            'production_m_id': self.orden_b.id, 'clasificacion': clasificacion, 'metros': 521.0,
            'kg_bruto': 34.0, 'kg_neto': 33.4, 'siglas_operador': 'RI',
            'defecto_ids': [Command.set(self.env['calidad.defecto.tela'].search([('clave', 'in', list(defectos))]).ids)],
        })
        return final._zpl_etiqueta()

    def test_01_operacion_de_clasificacion(self):
        self.assertTrue(self.workorder.es_clasificacion)
        self.assertTrue(self.orden_b.es_clasificacion_final)

    def test_02_datos_de_etiqueta(self):
        self._final('PQ')
        datos = self.env['mrp.rollo.final'].search([('name', '=', 'CLASTEST-0001')])._datos_etiqueta()
        self.assertEqual(datos['color'], 'NATURAL')
        self.assertEqual(datos['ancho'], 160)

    def test_03_etiqueta_pq(self):
        zpl = self._final('PQ')
        self.assertIn('WJ038Q22JNT160', zpl)
        self.assertNotIn('WJ038Q22JNT160..', zpl)
        # 3.17.0: diseño «Etiqueta producto terminado PQ»: peso neto y bruto, lote y rollo, un código de barras.
        for texto in ('PESO NETO', '33.40', 'PESO BRUTO', '34.00', 'LOTE: CLASTEST', 'ROLLO: 001', 'COLOR', 'ANCHO CM',
                      'METROS', 'YARDAS', '521.00', 'FECHA DE FABRICACIÓN'):
            self.assertIn(texto, zpl)
        self.assertIn('521.00-33.40-CLASTEST-001', zpl)
        self.assertNotIn('CLAVE', zpl, 'La etiqueta ya no lleva clave del producto')
        self.assertEqual(zpl.count('^BCN'), 1)

    def test_04_etiqueta_pnc_con_defectos(self):
        zpl = self._final('FE', defectos=('AD', 'AGU'))
        self.assertIn('WJ038Q22JNT160..', zpl)
        for texto in ('KILOS', '33.40', 'LIBRAS', '73.63'):
            self.assertIn(texto, zpl)
        self.assertNotIn('PESO NETO', zpl)
        self.assertIn('521.00-33.40-CLASTEST-001', zpl)
        self.assertIn('001 AD AGU', zpl, 'Segundo código de barras: rollo y siglas de los defectos')
        self.assertEqual(zpl.count('^BCN'), 2)
        self.assertIn('WJ038Q22JNT160.', self._final('SEGUNDA'))
        self.assertIn('WJ038Q22JNT160*', self._final('DESP'))

    def test_08_etiqueta_final_cabe_en_76x102(self):
        """3.3.7 (Sistemas 2026-11-05): 76 mm de ancho x 102 mm de alto = 608 x 816 puntos a 203 dpi. Todo lo
        impreso queda dentro de la etiqueta y los códigos con datos de largo real llevan módulo de 2 puntos."""
        import re
        for clasif, defectos in (('PQ', ()), ('FE', ('AD', 'AGU'))):
            zpl = self._final(clasif, defectos)
            self.assertTrue(zpl.startswith('^XA^PW608^LL816'))
            posiciones = [(int(x), int(y)) for x, y in re.findall(r'\^FO(\d+),(\d+)', zpl)]
            self.assertTrue(all(0 <= x < 608 and 0 <= y < 816 for x, y in posiciones))
            ultimo = re.findall(r'\^FO\d+,(\d+)\^BCN,(\d+)', zpl)[-1]
            self.assertLessEqual(int(ultimo[0]) + int(ultimo[1]) + 30, 816)  # barras + texto legible
        # Dato real: metros de 4 cifras, kg, orden en metros (M + 5 dígitos) y rollo de 2 cifras.
        codigo = self.rollo._zpl_codigo_barras('1234.50;49.100;M00123;12', 12, 608, 0)
        self.assertTrue(codigo.startswith('^BY2,'))
        self.assertIn('^FD>;1234>6.50;49.100;M00123;12^FS', codigo)
        # Subconjuntos: inicio y final en C con 4+ dígitos; corrida impar deja el primero en B; '>' se escapa.
        Bitacora = self.env['mrp.rollo.bitacora']
        self.assertEqual(Bitacora._code128_zpl('00004-0001'), ('>;0000>64->50001', 10 * 11 + 13))
        self.assertEqual(Bitacora._code128_zpl('A12345'), ('>:A1>52345', 7 * 11 + 13))
        self.assertEqual(Bitacora._code128_zpl('1 AD'), ('>:1 AD', 6 * 11 + 13))
        self.assertEqual(Bitacora._code128_zpl('a>b')[0], '>:a><b')

    def test_07_orden_previa_por_cadena_mto(self):
        """La MO en metros reconoce a la MO en kg que la abastece (base de la planeación encadenada)."""
        self.assertFalse(self.orden_b._ordenes_previas())
        terminado_a = self.orden_a.move_finished_ids.filtered(lambda m: m.product_id == self.prod_a)
        consumo_a = self.orden_b._clasif_move_a()
        consumo_a.move_orig_ids = terminado_a
        self.assertEqual(self.orden_b._ordenes_previas(), self.orden_a)
