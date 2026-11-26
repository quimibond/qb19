from odoo import api, fields, models


class Website(models.Model):
    _inherit = 'website'

    qb_diseno_b2b = fields.Boolean(
        string='Diseño B2B de Quimibond',
        help='Muestra en este sitio el encabezado, el pie, la portada y las '
             'páginas B2B del módulo website_quimibond (catálogo técnico, '
             'mercados, planta, calidad y solicitud de cotización). '
             'Desmárquela para regresar al diseño anterior.',
    )

    qb_armadoras = fields.Text(
        string='Armadoras (OEM) que se muestran',
        help='Una marca por línea. Se muestran como texto, sin logotipos, en la '
             'portada y en la página del mercado automotriz, bajo «Nuestros '
             'materiales viajan en vehículos de». Son las armadoras finales, no '
             'los clientes directos. Déjelo vacío para ocultar la sección.',
    )

    qb_url_agendar = fields.Char(
        string='Liga para agendar llamada',
        help='Dirección de la cita de Ventas (por ejemplo, /appointment/3). Con ella, '
             'el botón «Agendar llamada» del sitio lleva al calendario para que la '
             'persona elija día y hora. Vacía, el botón abre un formulario que crea '
             'una oportunidad en CRM con el día y el horario que la persona prefiere.',
    )
    qb_llamadas_user_id = fields.Many2one(
        'res.users', string='Atiende las solicitudes del sitio',
        domain="[('share', '=', False)]",
        help='Vendedor al que se asignan las cotizaciones, llamadas y demás solicitudes del sitio '
             '(automotriz, blancos y no tejidos). Vacío, quedan sin vendedor en el equipo de ventas.',
    )
    qb_llamadas_confeccion_user_id = fields.Many2one(
        'res.users', string='Atiende las solicitudes de confección',
        domain="[('share', '=', False)]",
        help='Vendedor al que se asignan las solicitudes del sitio del mercado de '
             'confección. Vacío, van con quien atiende las demás.',
    )
    qb_certificado_ids = fields.Many2many(
        'ir.attachment', 'website_qb_certificado_rel', 'website_id', 'attachment_id',
        string='Certificados para descargar',
        help='Los PDF de los certificados ISO vigentes. Se ofrecen para descarga en la '
             'página para compradores automotrices. Sin archivos, la página ofrece '
             'solicitarlos a Ventas.',
    )
    qb_resumen_correos = fields.Char(
        string='Correos del resumen mensual',
        help='Direcciones, separadas por coma, que reciben el día 1 de cada mes el '
             'resumen del sitio: oportunidades por origen, visitantes y páginas más '
             'vistas. Vacío, no se envía.',
    )

    qb_ingles = fields.Boolean(
        string='Versión en inglés',
        help='Enciende la versión en inglés del sitio en la dirección /en, con el '
             'cambio de idioma en el encabezado y el pie. Desmárquela para dejar '
             'el sitio solo en español.',
    )

    @api.model_create_multi
    def create(self, vals_list):
        sitios = super().create(vals_list)
        sitios.filtered('qb_ingles')._qb_activar_ingles()
        return sitios

    def write(self, vals):
        res = super().write(vals)
        if 'qb_ingles' in vals:
            if vals['qb_ingles']:
                self._qb_activar_ingles()
            else:
                self._qb_apagar_ingles()
        return res

    def _qb_telefono(self):
        """Teléfono de la oficina de ventas que publica el sitio."""
        from ..controllers import contenido
        return contenido.TELEFONO

    def _qb_lista_armadoras(self):
        self.ensure_one()
        vistas = []
        for linea in (self.qb_armadoras or '').splitlines():
            nombre = linea.strip()
            if nombre and nombre.lower() not in [v.lower() for v in vistas]:
                vistas.append(nombre)
        return vistas

    def _qb_armadoras_con_logo(self):
        """[{'nombre', 'logo'}]: el logotipo es la ruta del SVG, o '' si la marca no tiene archivo."""
        from ..controllers.contenido import logo_armadora
        return [{'nombre': nombre, 'logo': logo_armadora(nombre)} for nombre in self._qb_lista_armadoras()]

    # ------------------------------------------------------------------
    # Versión en inglés
    # ------------------------------------------------------------------

    def _qb_textos_js(self):
        """Textos del script en el idioma de la página (JSON), o '' en español."""
        from ..controllers import idioma
        return idioma.textos_js(self.env.context.get('lang') or self.env.lang)

    def _qb_activar_ingles(self):
        """Enciende la versión en inglés de estos sitios.

        El inglés del sitio es ``en_CA`` con la dirección ``/en``: ``en_US`` es
        el idioma de origen de Odoo y no admite traducción, y el módulo está
        escrito en español. Se puede llamar varias veces.
        """
        from ..controllers.idioma import IDIOMA_INGLES
        Lang = self.env['res.lang'].sudo().with_context(active_test=False)
        ingles = Lang._activate_lang(IDIOMA_INGLES) or Lang._create_lang(IDIOMA_INGLES)
        en_us = Lang.search([('code', '=', 'en_US')])
        if ingles.url_code != 'en':
            if en_us.url_code == 'en':
                en_us.url_code = 'en_US'
                en_us.flush_recordset()
            ingles.url_code = 'en'
        self.env['ir.module.module'].sudo().search([('state', '=', 'installed')])._update_translations([IDIOMA_INGLES])
        for sitio in self:
            sitio.sudo().language_ids = [(3, en_us.id), (4, ingles.id)]
        return True

    def _qb_apagar_ingles(self):
        """Quita el inglés de estos sitios; el idioma queda instalado."""
        from ..controllers.idioma import IDIOMA_INGLES
        ingles = self.env['res.lang'].sudo().with_context(active_test=False).search([('code', '=', IDIOMA_INGLES)])
        for sitio in self:
            if ingles and sitio.default_lang_id != ingles:
                sitio.sudo().language_ids = [(3, ingles.id)]
        return True
