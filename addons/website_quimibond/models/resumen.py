"""Resumen mensual del sitio: qué trajo y de dónde.

El día 1 de cada mes se manda por correo a las direcciones del sitio web
(«Correos del resumen mensual»): oportunidades que entraron por el sitio,
por origen, visitantes nuevos y páginas más vistas del mes anterior, contra
el mes previo.
"""
from collections import Counter
from datetime import datetime, time
from urllib.parse import urlsplit

from dateutil.relativedelta import relativedelta
from markupsafe import Markup, escape

from odoo import fields, models

MESES = ['enero', 'febrero', 'marzo', 'abril', 'mayo', 'junio', 'julio', 'agosto', 'septiembre', 'octubre',
         'noviembre', 'diciembre']
SIN_ORIGEN = 'Web · Sin origen'


class Website(models.Model):
    _inherit = 'website'

    def _qb_datos_resumen(self, inicio, fin):
        """Cifras del sitio entre dos fechas (datetime, UTC)."""
        self.ensure_one()
        from ..controllers.formulario import PREFIJO_ETIQUETA
        medio = self.env.ref('utm.utm_medium_website', raise_if_not_found=False)
        dominio = [
            ('create_date', '>=', inicio), ('create_date', '<', fin),
            ('company_id', 'in', [self.company_id.id, False]),
            '|', ('tag_ids.name', '=like', PREFIJO_ETIQUETA + '%'), ('medium_id', '=', medio.id if medio else 0),
        ]
        oportunidades = self.env['crm.lead'].sudo().with_context(active_test=False).search(dominio)
        por_origen = Counter()
        por_fuente = Counter()
        for op in oportunidades:
            etiquetas = [t.name for t in op.tag_ids if t.name.startswith(PREFIJO_ETIQUETA)]
            por_origen[etiquetas[0] if etiquetas else SIN_ORIGEN] += 1
            if op.source_id:
                por_fuente[op.source_id.name] += 1
        visitantes = self.env['website.visitor'].sudo().search_count([
            ('website_id', '=', self.id), ('create_date', '>=', inicio), ('create_date', '<', fin)])
        vistas = self.env['website.track'].sudo()._read_group(
            [('visitor_id.website_id', '=', self.id), ('visit_datetime', '>=', inicio), ('visit_datetime', '<', fin)],
            ['url'], ['__count'])
        por_pagina = Counter()
        for url, cuenta in vistas:
            por_pagina[urlsplit(url or '').path or '/'] += cuenta
        return {
            'oportunidades': len(oportunidades),
            'por_origen': por_origen.most_common(),
            'por_fuente': por_fuente.most_common(5),
            'visitantes': visitantes,
            'vistas': sum(por_pagina.values()),
            'paginas': por_pagina.most_common(8),
        }

    def _qb_cuerpo_resumen(self, hoy=None):
        """(asunto, cuerpo HTML) del resumen del mes anterior a ``hoy``."""
        self.ensure_one()
        hoy = hoy or fields.Date.context_today(self)
        fin = datetime.combine(hoy.replace(day=1), time.min)
        inicio = fin - relativedelta(months=1)
        mes = self._qb_datos_resumen(inicio, fin)
        previo = self._qb_datos_resumen(inicio - relativedelta(months=1), inicio)
        nombre_mes = '%s de %s' % (MESES[inicio.month - 1], inicio.year)

        def fila(texto, valor, antes=None):
            extra = Markup('<td style="padding:4px 12px;color:#777">antes: %s</td>') % antes if antes is not None else ''
            return Markup('<tr><td style="padding:4px 12px">%s</td><td style="padding:4px 12px"><b>%s</b></td>%s</tr>') % (
                texto, valor, extra)

        cuerpo = Markup('<p>Resumen de %s, %s.</p><table>') % (self.domain or self.name, nombre_mes)
        cuerpo += fila('Oportunidades que entraron por el sitio', mes['oportunidades'], previo['oportunidades'])
        cuerpo += fila('Visitantes nuevos', mes['visitantes'], previo['visitantes'])
        cuerpo += fila('Páginas vistas', mes['vistas'], previo['vistas'])
        cuerpo += Markup('</table>')
        if mes['por_origen']:
            cuerpo += Markup('<p><b>Oportunidades por origen</b></p><table>')
            for origen, cuenta in mes['por_origen']:
                cuerpo += fila(origen, cuenta)
            cuerpo += Markup('</table>')
        if mes['por_fuente']:
            cuerpo += Markup('<p><b>Por fuente o campaña</b></p><table>')
            for fuente, cuenta in mes['por_fuente']:
                cuerpo += fila(fuente, cuenta)
            cuerpo += Markup('</table>')
        if mes['paginas']:
            cuerpo += Markup('<p><b>Páginas más vistas</b></p><table>')
            for pagina, cuenta in mes['paginas']:
                cuerpo += fila(pagina, cuenta)
            cuerpo += Markup('</table>')
        cuerpo += Markup('<p style="color:#777">Las oportunidades están en CRM con la etiqueta de su origen (%s…).</p>') % escape(
            'Web · Cotización')
        return 'Sitio web: resumen de %s' % nombre_mes, cuerpo

    def _qb_cron_resumen_mensual(self):
        for sitio in self.search([('qb_diseno_b2b', '=', True), ('qb_resumen_correos', '!=', False)]):
            asunto, cuerpo = sitio._qb_cuerpo_resumen()
            self.env['mail.mail'].sudo().create({
                'subject': asunto,
                'body_html': cuerpo,
                'email_to': sitio.qb_resumen_correos,
                'email_from': sitio.company_id.email_formatted or self.env.user.email_formatted,
                'auto_delete': True,
            })
