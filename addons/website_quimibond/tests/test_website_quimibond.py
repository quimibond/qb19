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
        self.assertIn('href="/web/login"', portada.text)
        self.assertIn('Iniciar sesión', portada.text)
        self.assertIn('data-qb-anim="circular"', portada.text)
        # Logotipo nuevo en el encabezado y entrada animada solo en la portada.
        self.assertIn('qb-logo__marca', portada.text)
        self.assertIn('id="qb_intro"', portada.text)
        self.assertNotIn('id="qb_intro"', self.url_open('/planta').text)
        self.assertNotIn('ricot', portada.text)
        self.assertIn('Scrims', portada.text)
        self.assertNotIn('href="/catalogo"', portada.text)
        self.assertNotIn('ricot', self.url_open('/mercados/confeccion').text)
        for ruta in ('/catalogo', '/planta', '/calidad', '/nosotros', '/cotizar', '/cotizar/gracias', '/disenar',
                     '/mercados/automotriz', '/mercados/blancos', '/mercados/confeccion'):
            self.assertEqual(self.url_open(ruta).status_code, 200, ruta)
        self.assertEqual(self.url_open('/mercados/no-existe').status_code, 404)
        self.assertIn('Punto de doble fontura', portada.text)
        planta = self.url_open('/planta').text
        for texto in ('Punto de una fontura', 'qb-familia--tejido', 'qb-familia--no_tejido', 'No tejido tramado', 'Jets de teñido', 'Ramas',
                      'Entretelas fusionables, no tejido tramado', 'id="maquinaria"', 'data-qb-anim="jet"', 'data-qb-anim="rama"'):
            self.assertIn(texto, planta, texto)
        nosotros = self.url_open('/nosotros').text
        for texto in ('Misión', 'Visión 2030', 'Enfoque al cliente', 'Experiencia y especialización'):
            self.assertIn(texto, nosotros, texto)
        calidad = self.url_open('/calidad').text
        self.assertIn('ISO 45001 desde 2026', calidad)
        self.assertIn('2,081', calidad)
        self.assertIn('Planta tratadora de agua', calidad)
        for marca in ('Vanguard', 'Pai Lung', 'Wellrich', 'Brückner', 'Unitech', 'Icomatex', 'V18'):
            self.assertNotIn(marca, planta, marca)
        disenar = self.url_open('/disenar').text
        for texto in ('data-qb-disenador', 'name="qb_construccion"', 'name="qb_ancho"', 'name="qb_aplicacion"', 'data-qb-tela=', 'name="qb_mercado"', 'data-qb-mercado="Automotriz"', 'Asientos', 'Playera', 'name="qb_requisito"',
                      'name="qb_volumen"', 'data-qb-copiar', 'name="qb_hilo"', 'Poliéster reciclado', 'name="qb_fibra"', 'Polipropileno', 'data-qb-anim="tela"',
                      'name="description"'):
            self.assertIn(texto, disenar, texto)
        self.assertNotIn('Galga', disenar)
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
        automotriz = self.url_open('/mercados/automotriz').text
        self.assertIn('Marca Uno', automotriz)
        for texto in ('id="aplicaciones"', 'Asientos', 'Paneles de puerta', 'Punto de doble fontura', '/cotizar?mercado=automotriz', 'Scrim'):
            self.assertIn(texto, automotriz, texto)
        confeccion = self.url_open('/mercados/confeccion').text
        for texto in ('Playera', 'Entretela para frente de saco', 'No tejido tramado'):
            self.assertIn(texto, confeccion, texto)
        self.assertNotIn('qb-oem', self.url_open('/mercados/blancos').text)
        self.website.qb_armadoras = 'Toyota\nMarca Dos'
        self.assertEqual(self.website._qb_armadoras_con_logo(), [
            {'nombre': 'Toyota', 'logo': '/website_quimibond/static/src/img/armadoras/toyota.svg'},
            {'nombre': 'Marca Dos', 'logo': ''}])
        self.assertIn('armadoras/toyota.svg', self.url_open('/').text)

    def test_maquinaria_sin_ficha_de_maquina(self):
        """Sin el módulo de fichas de máquina se sirve el texto fijo, completo."""
        from odoo.addons.website_quimibond.controllers import contenido, planta
        grupos = planta.maquinaria(self.env, self.env.company)
        self.assertEqual([g['clave'] for g in grupos], [g['clave'] for g in contenido.MAQUINARIA])
        self.assertFalse(any(g['cantidad'] for g in grupos), 'no se publican conteos como cifra destacada')
        self.assertNotIn('Máquinas circulares', [c['etiqueta'] for c in contenido.CIFRAS])
        datos = {g['clave']: dict(g['datos']) for g in grupos}
        if 'mrp.workcenter' not in self.env or 'ft_tipo_id' not in self.env['mrp.workcenter']._fields:
            self.assertEqual(datos['tintoreria']['Máquinas'], '5 jets de teñido')
            self.assertEqual(datos['acabado']['Máquinas'], '3 ramas')
        self.assertIn('Fibras que teñimos', datos['tintoreria'])
        # La receta no se publica (Dirección, 2026-11-19).
        self.website.qb_diseno_b2b = True
        pagina = self.url_open('/planta').text
        for receta in ('Galgas', 'Diámetros', 'Salida de la tela', 'Relación de baño', 'Carga por jet'):
            self.assertNotIn(receta, pagina)
        self.assertIn('55 5570 5874', pagina)
        self.assertEqual(planta._lista(['Tubular', 'abierta', 'invertida']), 'Tubular, abierta e invertida')
        self.assertEqual(planta._lista(['16', '18']), '16 y 18')

    def test_hilos_del_disenador_segun_odoo(self):
        """Con las categorías de hilo en la base, el diseñador ofrece solo las que tienen productos."""
        from odoo.addons.website_quimibond.controllers import planta
        Categoria = self.env['product.category']
        if not Categoria.search([('complete_name', '=', planta.CATEGORIA_HILO)]):
            self.assertIn('Algodón', planta.disenador(self.env)['hilos'])
            materia = Categoria.create({'name': 'Materia Prima'})
            hilo = Categoria.create({'name': 'Hilo', 'parent_id': materia.id})
            for nombre in ('Poliéster', 'PET', 'Algodón', 'Elastano', 'Otra cosa'):
                categoria = Categoria.create({'name': nombre, 'parent_id': hilo.id})
                if nombre != 'Algodón':
                    self.env['product.template'].create({'name': 'Hilo %s' % nombre, 'categ_id': categoria.id})
        opciones = planta.disenador(self.env)
        self.assertEqual(opciones['hilos'][:2], ['Poliéster', 'Poliéster reciclado'])
        self.assertNotIn('Otra cosa', opciones['hilos'])
        self.assertNotIn('Algodón', opciones['hilos'])
        self.assertTrue(opciones['elastano'])
