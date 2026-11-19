from odoo.tests import HttpCase, tagged


@tagged('post_install', '-at_install')
class TestSitioB2B(HttpCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.website = cls.env['website'].search([], limit=1)
        cls.categoria = cls.env['product.public.category'].create({'name': 'No-Tejido Fusionable'})
        cls.producto = cls.env['product.template'].create({
            'name': 'ZQ9032BL152: Entretela no tejida fusionable 50  gr  PES 100% Blanco rigido',
            'default_code': 'ZQ9032BL152',
            'is_published': True,
            'sale_ok': True,
            'public_categ_ids': [(6, 0, cls.categoria.ids)],
        })

    def test_ficha_del_nombre(self):
        from odoo.addons.website_quimibond.controllers.main import ficha_producto
        ficha = ficha_producto(self.producto)
        self.assertEqual(ficha['codigo'], 'ZQ9032BL152')
        self.assertEqual(ficha['gramaje'], '50 g/m²')
        self.assertEqual(ficha['composicion'], 'PES 100%')
        self.assertEqual(ficha['color'], 'Blanco')
        self.assertEqual(ficha['tacto'], 'Rígido')
        self.assertEqual(ficha['fusion'], 'Fusionable')
        self.assertEqual(ficha['construccion'], 'notejido')

    def test_apagado_no_cambia_el_sitio(self):
        self.website.qb_diseno_b2b = False
        self.assertEqual(self.url_open('/catalogo').status_code, 404)
        self.assertEqual(self.url_open('/cotizar').status_code, 404)
        self.assertNotIn('data-qb="1"', self.url_open('/').text)

    def test_encendido_sirve_las_paginas(self):
        self.website.qb_diseno_b2b = True
        portada = self.url_open('/')
        self.assertEqual(portada.status_code, 200)
        self.assertIn('data-qb="1"', portada.text)
        self.assertIn('qb-header', portada.text)
        self.assertIn('data-qb-anim="circular"', portada.text)
        for ruta in ('/catalogo', '/planta', '/calidad', '/nosotros', '/cotizar', '/cotizar/gracias',
                     '/mercados/automotriz', '/mercados/blancos', '/mercados/confeccion'):
            self.assertEqual(self.url_open(ruta).status_code, 200, ruta)
        self.assertEqual(self.url_open('/mercados/no-existe').status_code, 404)
        self.assertIn('Punto de doble fontura', portada.text)
        planta = self.url_open('/planta').text
        for texto in ('Punto de una fontura', 'No tejido', 'Tejido tramado', 'jets de teñido', 'ramas',
                      'V10 de espolvoreo', 'id="maquinaria"'):
            self.assertIn(texto, planta, texto)
        catalogo = self.url_open('/catalogo')
        self.assertIn('ZQ9032BL152', catalogo.text)
        slug = self.env['ir.http']._slug
        ficha = self.url_open('/catalogo/producto/%s' % slug(self.producto))
        self.assertEqual(ficha.status_code, 200)
        self.assertIn('50 g/m²', ficha.text)
        self.assertEqual(self.url_open('/catalogo/%s' % slug(self.categoria)).status_code, 200)
        datos = self.url_open('/catalogo/buscar.json').json()
        self.assertTrue(any(d['codigo'] == 'ZQ9032BL152' for d in datos))

    def test_producto_sin_publicar_no_se_ve(self):
        self.website.qb_diseno_b2b = True
        self.producto.is_published = False
        slug = self.env['ir.http']._slug
        self.assertEqual(self.url_open('/catalogo/producto/%s' % slug(self.producto)).status_code, 404)
        self.assertNotIn('ZQ9032BL152', self.url_open('/catalogo').text)

    def test_formulario_crea_oportunidad(self):
        self.website.qb_diseno_b2b = True
        self.assertIn('data-model_name="crm.lead"', self.url_open('/cotizar').text)

    def test_armadoras_solo_si_hay_lista(self):
        self.website.qb_diseno_b2b = True
        self.website.qb_armadoras = False
        self.assertNotIn('qb-oem', self.url_open('/').text)
        self.website.qb_armadoras = 'Marca Uno\n\n marca uno \nMarca Dos'
        self.assertEqual(self.website._qb_lista_armadoras(), ['Marca Uno', 'Marca Dos'])
        portada = self.url_open('/').text
        self.assertIn('qb-oem', portada)
        self.assertIn('Marca Dos', portada)
        self.assertIn('Marca Uno', self.url_open('/mercados/automotriz').text)
        self.assertNotIn('qb-oem', self.url_open('/mercados/blancos').text)

    def test_maquinaria_sin_ficha_de_maquina(self):
        """Sin el módulo de fichas de máquina se sirve el texto fijo, completo."""
        from odoo.addons.website_quimibond.controllers import contenido, planta
        grupos = planta.maquinaria(self.env, self.env.company)
        self.assertEqual([g['clave'] for g in grupos], [g['clave'] for g in contenido.MAQUINARIA])
        if 'mrp.workcenter' not in self.env or 'ft_tipo_id' not in self.env['mrp.workcenter']._fields:
            self.assertEqual(grupos, contenido.MAQUINARIA)
        cifras = {c['etiqueta']: c['valor'] for c in planta.cifras(grupos)}
        self.assertEqual(cifras['Año de fundación'], 1992)
        self.assertTrue(cifras['Máquinas circulares'])
        self.assertEqual(planta._lista(['Brückner', 'Unitech', 'Icomatex']), 'Brückner, Unitech e Icomatex')
        self.assertEqual(planta._lista(['16', '18']), '16 y 18')
