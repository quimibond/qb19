# -*- coding: utf-8 -*-
"""2.9.0: ficha de materia prima con el motor de parámetros de las máquinas."""
from odoo.exceptions import ValidationError
from odoo.tests import TransactionCase, tagged


@tagged('post_install', '-at_install')
class TestMateriaPrima(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.kg = cls.env.ref('uom.product_uom_kgm')
        Categ = cls.env['product.category']
        mp = Categ.create({'name': 'Materia Prima prueba'})
        hilo = Categ.create({'name': 'Hilo', 'parent_id': mp.id})
        cls.categ_pes = Categ.create({'name': 'Poliéster', 'parent_id': hilo.id})
        cls.categ_pet = Categ.create({'name': 'PET', 'parent_id': hilo.id})
        cls.categ_mezcla = Categ.create({'name': 'Mezcla (PES/CO)', 'parent_id': hilo.id})
        cls.categ_quimico = Categ.create({'name': 'Químico', 'parent_id': mp.id})
        cls.filamento = cls.env.ref('quimibond_ficha_tecnica_tela.ft_material_tipo_hilo_filamento')
        cls.hilado = cls.env.ref('quimibond_ficha_tecnica_tela.ft_material_tipo_hilo_hilado')
        cls.quimico = cls.env.ref('quimibond_ficha_tecnica_tela.ft_material_tipo_quimico')
        Product = cls.env['product.product']
        cls.stretch = Product.create({'name': 'HILO POLIÉSTER 75/36 STRETCH TORSIÓN Z', 'default_code': 'ZZ75/36Z',
                                      'categ_id': cls.categ_pes.id, 'uom_id': cls.kg.id})
        cls.mezcla = Product.create({'name': 'HILO MEZCLA PES/CO 65/35 22/1', 'default_code': 'ZZ22/1',
                                     'categ_id': cls.categ_mezcla.id, 'uom_id': cls.kg.id})
        cls.mezcla_sin = Product.create({'name': 'HILO MEZCLA PES/CO 22/1', 'default_code': 'ZZ22/1B',
                                         'categ_id': cls.categ_mezcla.id, 'uom_id': cls.kg.id})
        cls.pet = Product.create({'name': 'HILO PET 75/72 RECICLADO', 'default_code': 'ZZPET75/72',
                                  'categ_id': cls.categ_pet.id, 'uom_id': cls.kg.id})
        cls.tela = Product.create({'name': 'TEJIDO CIRCULAR DE 75/36', 'default_code': 'ZZ044Q22HNT235',
                                   'uom_id': cls.kg.id})
        cls.Material = cls.env['ficha.tecnica.material']

    def _valores(self, material):
        return {v.code: (v.valor_texto if v.kind == 'text' else v.valor) for v in material.valor_ids
                if v.valor or v.valor_texto}

    def test_01_filamento_desde_el_nombre(self):
        action = self.stretch.action_ft_material()
        material = self.Material.browse(action['res_id'])
        self.assertEqual(material.tipo_id, self.filamento, "75/36 es filamento")
        self.assertEqual(self._valores(material), {
            'titulo_den': 75, 'filamentos': 36, 'textura': 'Texturizado stretch', 'torsion_sentido': 'Z'})
        self.assertEqual(material.composicion, '100 % PES')
        self.assertFalse(material.composicion_aviso)
        self.assertTrue(material.valor_ids.filtered(lambda v: v.code == 'tenacidad'),
                        "Las propiedades que el nombre no dice quedan vacías, no se inventan")
        self.assertEqual(self.stretch.action_ft_material()['res_id'], material.id, "No se duplica")
        self.assertEqual(self.stretch.ft_material_id, material)

    def test_02_hilado_y_mezcla(self):
        material = self.Material.browse(self.mezcla.action_ft_material()['res_id'])
        self.assertEqual(material.tipo_id, self.hilado, "22/1 es hilado (número inglés)")
        self.assertEqual(self._valores(material), {'titulo_ne': 22, 'cabos': 1})
        self.assertEqual(material.composicion, '65 % PES · 35 % CO', "65/35 es la mezcla, no el título")
        sin = self.Material.browse(self.mezcla_sin.action_ft_material()['res_id'])
        self.assertFalse(sin.composicion_ids, "Sin proporción en el nombre no se adivina la mezcla")
        pet = self.Material.browse(self.pet.action_ft_material()['res_id'])
        self.assertEqual(pet.composicion, '100 % PES reciclado')
        self.assertEqual(pet.reciclado_pct, 100)
        self.assertEqual(self._valores(pet)['filamentos'], 72)

    def test_03_opciones_y_aviso_de_composicion(self):
        material = self.Material.create({'product_id': self.stretch.id})
        self.assertEqual(material.tipo_id, self.filamento, "El tipo se propone al crear")
        torsion = material.valor_ids.filtered(lambda v: v.code == 'torsion_sentido')
        torsion.valor_texto = ' z '
        self.assertEqual(torsion.valor_texto, 'Z', "Se guarda como la opción")
        with self.assertRaises(ValidationError):
            torsion.valor_texto = 'X'
        material.composicion_ids = [(0, 0, {'fibra_id': self.env.ref('quimibond_ficha_tecnica_tela.ft_fibra_pes').id,
                                            'porcentaje': 90})]
        self.assertIn('90', material.composicion_aviso)
        material.action_ft_proponer()
        self.assertEqual(material.composicion, '90 % PES', "Proponer no toca lo que ya tiene dato")
        self.assertEqual(material._ft_value('titulo_den'), 75)

    def test_04_ambito_y_clase(self):
        with self.assertRaises(ValidationError):
            self.env['ficha.tecnica.maquina.parametro'].create({
                'tipo_id': self.quimico.id, 'code': 'ph', 'name': 'pH', 'ambito': 'maquina'})
        ph = self.env['ficha.tecnica.maquina.parametro'].create({
            'tipo_id': self.quimico.id, 'code': 'ph', 'name': 'pH', 'ambito': 'material'})
        self.quimico.categ_ids = self.categ_quimico
        acido = self.env['product.product'].create({'name': 'ÁCIDO ACÉTICO', 'categ_id': self.categ_quimico.id})
        material = self.Material.browse(acido.action_ft_material()['res_id'])
        self.assertEqual(material.tipo_id, self.quimico, "El tipo sale de la categoría del producto")
        self.assertEqual(material.valor_ids.parametro_id, ph)
        self.assertFalse(self.tela.ft_material_sugerido, "Una tela no es materia prima")
        wc = self.env['mrp.workcenter'].create({'name': 'MÁQUINA 97 materia prueba'})
        config = self.env['ficha.tecnica.maquina.config'].create({'workcenter_id': wc.id, 'tipo_id': self.quimico.id})
        self.assertFalse(config.valor_ids, "Una propiedad de materia prima no entra a una configuración de máquina")
        with self.assertRaises(ValidationError):
            self.env['ficha.tecnica.maquina.valor'].create({'config_id': config.id, 'parametro_id': ph.id})

    def test_05_donde_se_usa(self):
        wc = self.env['mrp.workcenter'].create({
            'name': 'CIRCULAR 97 materia prueba',
            'ft_tipo_id': self.env.ref('quimibond_ficha_tecnica_tela.ft_maquina_tipo_tejido_circular').id})
        ficha = self.env['ficha.tecnica.tejido'].create({
            'articulo': 'ZZ044Q22HNT235', 'maquina_tejido': wc.id, 'galga': 18,
            'hilo_line_ids': [(0, 0, {'numero': 1, 'product_id': self.stretch.id, 'porcentaje': 100, 'polea': 2})]})
        material = self.Material.browse(self.stretch.action_ft_material()['res_id'])
        self.assertEqual(material.hilo_ids, ficha.hilo_line_ids)
        self.assertEqual((material.tela_count, material.maquina_count), (1, 1))
        self.assertEqual(ficha.hilo_line_ids.workcenter_id, wc, "El renglón guarda la máquina para la cadena al revés")
        self.assertEqual(ficha.hilo_line_ids.galga, 18)
        self.assertEqual(ficha.hilo_line_ids.material_resumen, material.resumen)
        self.stretch.invalidate_recordset(['ft_hilo_tela_count'])
        self.assertEqual(self.stretch.ft_hilo_tela_count, 1)
        self.assertEqual(wc.ft_hilo_count, 1)
        self.assertEqual(self.stretch.product_tmpl_id.action_ft_telas_hilo()['domain'], [('id', 'in', ficha.ids)])
