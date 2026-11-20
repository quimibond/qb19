"""Origen de cada oportunidad que entra por el sitio.

Cada formulario del sitio B2B manda un campo oculto ``qb_origen``. Aquí se
convierte en una etiqueta de CRM («Web · Cotización», «Web · Diseñador»…)
para que Ventas y el resumen mensual sepan de qué página vino cada
oportunidad. La fuente y la campaña (enlaces con ``utm_source``…) las pone
Odoo solo desde la visita; el medio «Website» también.
"""
import re

from odoo.http import request

from odoo.addons.website_crm.controllers.website_form import WebsiteForm

# clave que manda el formulario → etiqueta en CRM
ORIGENES = {
    'cotizar': 'Web · Cotización',
    'disenador': 'Web · Diseñador',
    'mercado': 'Web · Mercado',
    'automotriz': 'Web · Compradores automotrices',
    'ficha': 'Web · Ficha técnica',
    'llamada': 'Web · Llamada',
    'certificados': 'Web · Certificados',
}
PREFIJO_ETIQUETA = 'Web · '

RE_RENGLON_ORIGEN = re.compile(r'^\s*qb_origen\s*:.*\n?', re.M)


def etiqueta_origen(env, origen):
    """La etiqueta de CRM del origen; se crea la primera vez."""
    nombre = ORIGENES.get(origen)
    if not nombre:
        return env['crm.tag']
    Tag = env['crm.tag'].sudo()
    return Tag.search([('name', '=', nombre)], limit=1) or Tag.create({'name': nombre})


class QuimibondFormulario(WebsiteForm):

    def insert_record(self, request, model_sudo, values, custom, meta=None):
        if model_sudo.model == 'crm.lead' and request.website.qb_diseno_b2b:
            origen = (request.params.get('qb_origen') or '').strip()
            # El campo oculto no es del modelo: Odoo lo agrega al texto de la
            # oportunidad. Se quita de ahí y queda como etiqueta.
            custom = RE_RENGLON_ORIGEN.sub('', custom or '')
            etiqueta = etiqueta_origen(request.env, origen)
            if etiqueta:
                values['tag_ids'] = [(4, etiqueta.id)]
        return super().insert_record(request, model_sudo, values, custom, meta=meta)
