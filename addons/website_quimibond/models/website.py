from odoo import fields, models


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

    def _qb_lista_armadoras(self):
        self.ensure_one()
        vistas = []
        for linea in (self.qb_armadoras or '').splitlines():
            nombre = linea.strip()
            if nombre and nombre.lower() not in [v.lower() for v in vistas]:
                vistas.append(nombre)
        return vistas
