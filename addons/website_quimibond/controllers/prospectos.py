"""Páginas para atraer prospectos: compradores automotrices, agendar llamada
y descarga de certificados."""
from werkzeug.exceptions import NotFound

from odoo import http
from odoo.http import request

from . import contenido
from .main import QuimibondSitio, _exigir_activo

# Fuente: presentación de empresa de octubre de 2026 (la que Ventas manda a
# prospectos automotrices) y lo ya publicado en /calidad. «pdf» es el certificado
# en static/src/doc; al renovarse se cambia el archivo y el número de aquí.
CERTIFICACIONES = [
    {'norma': 'ISO 9001:2015', 'sistema': 'Sistema de gestión de calidad', 'desde': 'Certificado desde 2015',
     'detalle': 'Certificado 24070237A, vigente hasta el 22 de agosto de 2027',
     'pdf': 'quimibond-iso-9001-2015.pdf'},
    {'norma': 'ISO 14001:2015', 'sistema': 'Sistema de gestión ambiental', 'desde': 'Certificado desde 2023',
     'detalle': 'Certificado 26220295B, vigente hasta el 31 de marzo de 2029',
     'pdf': 'quimibond-iso-14001-2015.pdf'},
    {'norma': 'ISO 45001:2018', 'sistema': 'Seguridad y salud en el trabajo', 'desde': 'Certificado desde 2026',
     'detalle': '', 'pdf': ''},
]

PRUEBAS_LABORATORIO = [
    'Masa por unidad de área (g/m²)', 'Espesor', 'Resistencia a la rotura', 'Elongación a la rotura',
    'Estabilidad dimensional al calor', 'Estabilidad dimensional al lavado', 'Flamabilidad',
    'Igualación de color', 'Contenido de aceite', 'Evaluación de olor',
]

DESARROLLO = [
    {'paso': '01', 'nombre': 'Análisis de su muestra',
     'texto': 'Revisamos su muestra y su especificación: construcción, fibra, peso y desempeño que debe cumplir.'},
    {'paso': '02', 'nombre': 'Diseño de la tela',
     'texto': 'Definimos la construcción y elegimos los hilos o las fibras para su aplicación.'},
    {'paso': '03', 'nombre': 'Desarrollo del proceso',
     'texto': 'Validamos el proceso de tejido, teñido y acabado antes de producir.'},
    {'paso': '04', 'nombre': 'Muestra y validación',
     'texto': 'Recibe la muestra con sus resultados de laboratorio para validarla en su planta.'},
    {'paso': '05', 'nombre': 'Producción programada',
     'texto': 'Programamos la producción para sus entregas recurrentes, con el lote trazable en cada rollo.'},
]

HORARIOS_LLAMADA = ['De 9:00 a 11:00', 'De 11:00 a 13:00', 'De 13:00 a 15:00', 'De 16:00 a 18:00']


class QuimibondProspectos(QuimibondSitio):

    @http.route(['/compradores-automotrices', '/automotive-buyers'], type='http', auth='public', website=True,
                sitemap=QuimibondSitio._sitemap_fijo(['/compradores-automotrices']))
    def compradores_automotrices(self, **kw):
        _exigir_activo()
        return request.render('website_quimibond.compradores', self._valores_comunes(
            mercado=contenido.mercado('automotriz'),
            mercado_sel='Automotriz',
            asunto='',
            qb_certificaciones=CERTIFICACIONES,
            qb_pruebas=PRUEBAS_LABORATORIO,
            qb_desarrollo=DESARROLLO,
            certificados=request.website.sudo().qb_certificado_ids,
            title='Compradores automotrices | Quimibond',
            website_meta_description='Scrims de tejido circular y no tejidos para interiores automotrices. '
                                     'Certificados ISO 9001, 14001 y 45001, laboratorio en planta y desarrollo '
                                     'contra su especificación.',
        ))

    @http.route('/agendar', type='http', auth='public', website=True,
                sitemap=QuimibondSitio._sitemap_fijo(['/agendar']))
    def agendar(self, **kw):
        _exigir_activo()
        liga = (request.website.qb_url_agendar or '').strip()
        if liga:
            # Calendario de citas de Ventas: la persona elige día y hora ahí.
            return request.redirect(liga, code=302, local=liga.startswith('/'))
        return request.render('website_quimibond.agendar', self._valores_comunes(
            qb_horarios=HORARIOS_LLAMADA,
            title='Agendar una llamada | Quimibond',
            website_meta_description='Elija el día y el horario que prefiere y un asesor de Ventas de Quimibond le llama.',
        ))

    @http.route('/calidad/certificado/<int:adjunto_id>', type='http', auth='public', website=True, sitemap=False)
    def certificado(self, adjunto_id, **kw):
        _exigir_activo()
        adjunto = request.website.sudo().qb_certificado_ids.filtered(lambda a: a.id == adjunto_id)
        if not adjunto:
            raise NotFound()
        return request.env['ir.binary']._get_stream_from(adjunto).get_response(as_attachment=True)
