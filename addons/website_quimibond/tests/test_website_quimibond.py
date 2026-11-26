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
        self.assertIn('href="/catalogo"', portada.text)
        self.assertNotIn('ricot', self.url_open('/mercados/confeccion').text)
        for ruta in ('/catalogo', '/planta', '/calidad', '/nosotros', '/cotizar', '/cotizar/gracias', '/disenar',
                     '/mercados/automotriz', '/mercados/blancos', '/mercados/confeccion'):
            self.assertEqual(self.url_open(ruta).status_code, 200, ruta)
        self.assertEqual(self.url_open('/mercados/no-existe').status_code, 404)
        self.assertIn('Punto de doble fontura', portada.text)
        planta = self.url_open('/planta').text
        for texto in ('Punto de una fontura', 'qb-familia--tejido', 'qb-familia--no_tejido', 'No tejido tramado', 'Jets de teñido', 'Ramas',
                      'Entretelas fusionables, no tejido tramado', 'id="maquinaria"', 'data-qb-anim="jet"', 'data-qb-anim="rama"', 'data-qb-fases'):
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
        catalogo = self.url_open('/catalogo').text
        for texto in ('Scrims de tejido circular', 'Scrims no tejidos', 'Entretelas no tejidas fusionables', 'Telas de punto',
                      '/catalogo/familia/scrim-tejido'):
            self.assertIn(texto, catalogo, texto)
        self.assertNotIn('ricot', catalogo)
        familia = self.url_open('/catalogo/familia/entretela-no-tejida').text
        for texto in ('ZQ9032BL152', 'De 27 a 77 g/m²', 'name="qb_origen" value="ficha"'):
            self.assertIn(texto, familia, texto)
        self.assertNotIn('ZQ9032BL152', self.url_open('/catalogo/familia/entretela-tejida').text)
        self.assertEqual(self.url_open('/catalogo/familia/no-existe').status_code, 404)
        slug = self.env['ir.http']._slug
        ficha = self.url_open('/catalogo/producto/%s' % slug(self.producto))
        self.assertEqual(ficha.status_code, 200)
        self.assertIn('50 g/m²', ficha.text)
        # La dirección vieja por categoría del sitio lleva al catálogo por familias.
        vieja = self.url_open('/catalogo/%s' % slug(self.categoria), allow_redirects=False)
        self.assertEqual(vieja.status_code, 301)
        datos = self.url_open('/catalogo/buscar.json').json()
        self.assertTrue(any(d['codigo'] == 'ZQ9032BL152' for d in datos))
        self.assertTrue(any(d['url'] == '/catalogo/familia/scrim-tejido' for d in datos))

    def test_producto_sin_publicar_no_se_ve(self):
        self.website.qb_diseno_b2b = True
        self.producto.is_published = False
        slug = self.env['ir.http']._slug
        self.assertEqual(self.url_open('/catalogo/producto/%s' % slug(self.producto)).status_code, 404)
        self.assertNotIn('ZQ9032BL152', self.url_open('/catalogo/familia/entretela-no-tejida').text)

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

    def test_familia_por_nombre(self):
        from odoo.addons.website_quimibond.controllers.familias import familia_de
        casos = {
            'ENTRETELA TEJIDA 54 g/m2 FUSIONABLE BLANCA': 'entretela-tejida',
            'ENTRETELA NO TEJIDA 50 g/m2 FUSIONABLE BLANCA': 'entretela-no-tejida',
            'ENTRETELA NO TEJIDA 56 g/m2 FUSIONABLE TRAMADA BLANCA': 'entretela-tramada',
            'A40BL152: Entretela no tejida "No Fusionable" 40  gr  PES 100% Blanco': 'no-fusionable',
        }
        for nombre, clave in casos.items():
            self.assertEqual(familia_de(nombre)['clave'], clave, nombre)
        self.assertIsNone(familia_de('Hilo poliéster 150/48'))

    def test_paginas_de_prospectos(self):
        self.website.qb_diseno_b2b = True
        compradores = self.url_open('/compradores-automotrices').text
        for texto in ('24070237A', '26220295B', '26030175C', 'Elongación a la rotura',
                      'name="qb_origen" value="automotriz"', 'Scrims no tejidos', 'SIDE Certificaciones'):
            self.assertIn(texto, compradores, texto)
        # Los tres se descargan: ya no hace falta el formulario para pedirlos.
        self.assertNotIn('name="qb_origen" value="certificados"', compradores)
        self.assertEqual(self.url_open('/automotive-buyers').status_code, 200)
        # Los certificados ISO van en el módulo y se descargan de la tarjeta.
        for pdf in ('quimibond-iso-9001-2015.pdf', 'quimibond-iso-14001-2015.pdf', 'quimibond-iso-45001-2018.pdf'):
            ruta = '/website_quimibond/static/src/doc/%s' % pdf
            self.assertIn(ruta, compradores, pdf)
            self.assertTrue(self.url_open(ruta).content.startswith(b'%PDF-'), pdf)
        agendar = self.url_open('/agendar').text
        for texto in ('name="qb_origen" value="llamada"', 'De 9:00 a 11:00'):
            self.assertIn(texto, agendar, texto)
        # Con una liga de citas en el sitio, /agendar manda a ella.
        self.website.qb_url_agendar = '/appointment/1'
        respuesta = self.url_open('/agendar', allow_redirects=False)
        self.assertIn(respuesta.status_code, (302, 303))
        self.assertTrue(respuesta.headers['Location'].endswith('/appointment/1'))
        self.website.qb_url_agendar = False
        # Un certificado cargado en el sitio se descarga; otro adjunto, no.
        cert = self.env['ir.attachment'].create({'name': 'ISO 9001.pdf', 'raw': b'%PDF-1.4 prueba', 'mimetype': 'application/pdf'})
        otro = self.env['ir.attachment'].create({'name': 'otro.pdf', 'raw': b'%PDF-1.4 otro', 'mimetype': 'application/pdf'})
        self.website.qb_certificado_ids = [(6, 0, cert.ids)]
        self.assertIn('/calidad/certificado/%s' % cert.id, self.url_open('/compradores-automotrices').text)
        self.assertEqual(self.url_open('/calidad/certificado/%s' % cert.id).content, b'%PDF-1.4 prueba')
        self.assertEqual(self.url_open('/calidad/certificado/%s' % otro.id).status_code, 404)
        self.website.qb_diseno_b2b = False
        self.assertEqual(self.url_open('/compradores-automotrices').status_code, 404)
        self.assertEqual(self.url_open('/agendar').status_code, 404)

    def test_origen_de_la_oportunidad(self):
        from odoo.addons.website_quimibond.controllers.formulario import ORIGENES, etiqueta_origen
        from odoo.http import Request
        self.assertFalse(etiqueta_origen(self.env, 'inventado'))
        etiqueta = etiqueta_origen(self.env, 'ficha')
        self.assertEqual(etiqueta.name, 'Web · Ficha técnica')
        self.assertEqual(etiqueta_origen(self.env, 'ficha'), etiqueta, 'no se duplica')
        self.website.qb_diseno_b2b = True
        self.authenticate(None, None)
        respuesta = self.url_open('/website/form/crm.lead', data={
            'csrf_token': Request.csrf_token(self),
            'name': 'Prueba de origen',
            'contact_name': 'Comprador de prueba',
            'email_from': 'comprador@example.com',
            'description': 'Scrim de 60 g/m²',
            'qb_origen': 'automotriz',
        })
        self.assertEqual(respuesta.status_code, 200)
        self.assertIn('id', respuesta.json(), respuesta.text)
        lead = self.env['crm.lead'].browse(respuesta.json()['id'])
        self.assertEqual(lead.tag_ids.mapped('name'), [ORIGENES['automotriz']])
        self.assertNotIn('qb_origen', lead.description or '')
        # Las llamadas se asignan: confección a su vendedor, lo demás al general.
        general = self.env['res.users'].create({'name': 'Ventas general', 'login': 'qb_ventas_general'})
        confeccion = self.env['res.users'].create({'name': 'Ventas confección', 'login': 'qb_ventas_confeccion'})
        self.website.write({'qb_llamadas_user_id': general.id, 'qb_llamadas_confeccion_user_id': confeccion.id})
        for mercado, vendedor in (('Confección', confeccion), ('Automotriz', general)):
            respuesta = self.url_open('/website/form/crm.lead', data={
                'csrf_token': Request.csrf_token(self), 'name': 'Llamada con Ventas', 'contact_name': 'Prueba',
                'email_from': 'llamada@example.com', 'phone': '5555555555', 'Mercado': mercado, 'qb_origen': 'llamada',
            })
            lead = self.env['crm.lead'].browse(respuesta.json()['id'])
            self.assertEqual(lead.user_id, vendedor, mercado)
            self.assertEqual(lead.tag_ids.mapped('name'), [ORIGENES['llamada']])

    def test_ingles(self):
        from odoo.addons.website_quimibond.controllers import idioma
        self.website.qb_diseno_b2b = True
        # Como en producción: el sitio nace en español y el inglés se enciende después.
        Lang = self.env['res.lang'].with_context(active_test=False)
        espanol = Lang._activate_lang('es_MX') or Lang._create_lang('es_MX')
        self.website.language_ids = [(4, espanol.id)]
        self.website.default_lang_id = espanol
        self.website.qb_ingles = True
        self.website._qb_activar_ingles()
        ingles = self.env['res.lang'].search([('code', '=', idioma.IDIOMA_INGLES)])
        self.assertEqual(ingles.url_code, 'en')
        self.assertIn(ingles, self.website.language_ids)
        self.assertEqual(idioma.traducir('Automotriz', 'es_MX'), 'Automotriz')
        self.assertEqual(idioma.traducir('Automotriz', 'en_CA'), 'Automotive')
        valor = idioma.traducir({'clave': 'automotriz', 'nombre': 'Automotriz', 'datos': [('Máquinas', '5 jets de teñido')]}, 'en_CA')
        self.assertEqual(valor, {'clave': 'automotriz', 'nombre': 'Automotive', 'datos': [('Machines', '5 jet dyeing machines')]})
        self.assertEqual(idioma.textos_js('es_MX'), '')
        self.assertIn('"Suave": "Soft"', idioma.textos_js('en_CA'))
        if self.website.default_lang_id.code == idioma.IDIOMA_INGLES:
            return
        # El cambio de idioma pasa por /website/lang para que el regreso al español no rebote a /en.
        pagina = self.url_open('/en/catalogo').text
        self.assertIn('/website/lang/%s?r=' % self.website.default_lang_id.url_code, pagina)
        regreso = self.url_open('/website/lang/%s?r=%%2Fcatalogo' % self.website.default_lang_id.url_code)
        self.assertNotIn('/en/', regreso.url.split('//', 1)[-1])
        self.assertNotIn('What we make, by family.', regreso.text)
        for ruta, texto in (('/en/catalogo', 'What we make, by family.'), ('/en/compradores-automotrices', 'In-house laboratory'),
                            ('/en/planta', 'Jet dyeing machines'), ('/en/agendar', 'Schedule a call with Sales'),
                            ('/en/disenar', 'id="qb_t"')):
            pagina = self.url_open(ruta)
            self.assertEqual(pagina.status_code, 200, ruta)
            self.assertIn(texto, pagina.text, ruta)

    def test_traducciones_completas(self):
        """Cada texto de contenido y del script tiene su inglés en el .po."""
        from odoo.addons.website_quimibond.controllers import contenido, familias, idioma, prospectos
        tabla = idioma.tabla('en_CA')
        mismos = {'Color', 'Scrim', 'Beige', 'Natural', 'Nylon', 'Stretch', 'Bamboo', 'ISO 9001', 'ISO 14001', 'ISO 45001',
                  'ISO 9001:2015', 'ISO 14001:2015', 'ISO 45001:2018', '3.7 GWh', '2,081', '152 cm'}
        faltan = []

        def revisar(valor):
            if isinstance(valor, str):
                if any(c in valor for c in 'áéíóúñ¿') and valor not in tabla and valor not in mismos:
                    faltan.append(valor)
            elif isinstance(valor, dict):
                for llave, v in valor.items():
                    if llave not in idioma.NO_TRADUCIR:
                        revisar(v)
            elif isinstance(valor, (list, tuple)):
                for v in valor:
                    revisar(v)

        for modulo in (contenido, familias, prospectos):
            for nombre in dir(modulo):
                if nombre.isupper() and nombre not in ('ARMADORAS', 'LOGOS_ARMADORAS', 'CATEGORIA_HILO'):
                    revisar(getattr(modulo, nombre))
        revisar(idioma.TEXTOS_JS)
        self.assertFalse(faltan, 'sin traducción al inglés: %s' % faltan[:10])

    def test_resumen_mensual(self):
        from datetime import date
        from odoo.addons.website_quimibond.controllers.formulario import etiqueta_origen
        self.website.qb_diseno_b2b = True
        lead = self.env['crm.lead'].create({'name': 'Del sitio', 'tag_ids': [(6, 0, etiqueta_origen(self.env, 'cotizar').ids)]})
        self.env.cr.execute("UPDATE crm_lead SET create_date = '2026-10-15 12:00:00' WHERE id = %s", [lead.id])
        asunto, cuerpo = self.website._qb_cuerpo_resumen(date(2026, 11, 1))
        self.assertEqual(asunto, 'Sitio web: resumen de octubre de 2026')
        self.assertIn('Web · Cotización', cuerpo)
        correos = self.env['mail.mail'].search_count([])
        self.website.qb_resumen_correos = False
        self.website._qb_cron_resumen_mensual()
        self.assertEqual(self.env['mail.mail'].search_count([]), correos)
        self.website.qb_resumen_correos = 'direccion@example.com'
        self.website._qb_cron_resumen_mensual()
        self.assertEqual(self.env['mail.mail'].search_count([]), correos + 1)
