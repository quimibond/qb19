# -*- coding: utf-8 -*-
"""2.30.0: las medidas de la ficha también se capturan como rango (de mínimo a máximo).

Jose (2026-11-18): «no me deja meter intervalos de tolerancias» en espesor, ancho y elongaciones. La ficha guarda
nominal + tolerancia arriba / abajo (2.29.0) y de ahí leen el SGI, Inspección y las importaciones; el rango es otra
forma de escribir lo mismo:

- mínimo = nominal − tolerancia (−), máximo = nominal + tolerancia (+);
- al escribir el rango, el nominal se queda si cae dentro (60 con 50 a 65 → 60 +5 / −10); si no hay nominal o cae
  fuera, el nominal pasa al centro del rango (50 a 70 → 60 ± 10).

Los campos de rango no se guardan: se calculan de nominal y tolerancias, y al guardarlos escriben esos tres.
"""
from odoo import api, fields, models
from odoo.exceptions import ValidationError

EPS = 1e-9

# (nominal, tolerancia +, tolerancia −, etiqueta, decimales)
MEDIDAS_ACABADO = (
    ('peso_acabado', 'peso_acabado_tol', 'peso_acabado_tol_minus', 'peso', None),
    ('ancho_acabado', 'ancho_acabado_tol', 'ancho_acabado_tol_minus', 'ancho', None),
    ('encogimiento_largo', 'encogimiento_largo_tol', 'encogimiento_largo_tol_minus', 'encogimiento largo', None),
    ('encogimiento_ancho', 'encogimiento_ancho_tol', 'encogimiento_ancho_tol_minus', 'encogimiento ancho', None),
    ('espesor_acabado', 'espesor_acabado_tol', 'espesor_acabado_tol_minus', 'espesor', (16, 3)),
    ('elongacion_largo_acabado', 'elongacion_largo_acabado_tol', 'elongacion_largo_acabado_tol_minus',
     'elongación largo', None),
    ('elongacion_ancho_acabado', 'elongacion_ancho_acabado_tol', 'elongacion_ancho_acabado_tol_minus',
     'elongación ancho', None),
)
MEDIDAS_TEJIDO = (
    ('peso_acondicionado', 'peso_acondicionado_tol', 'peso_acondicionado_tol_minus', 'peso', None),
    ('ancho_acondicionado', 'ancho_acondicionado_tol', 'ancho_acondicionado_tol_minus', 'ancho', None),
    ('espesor_acondicionado', 'espesor_acondicionado_tol', 'espesor_acondicionado_tol_minus', 'espesor', (16, 3)),
    ('elongacion_carga_largo', 'elongacion_carga_largo_tol', 'elongacion_carga_largo_tol_minus',
     'elongación largo', None),
    ('elongacion_carga_ancho', 'elongacion_carga_ancho_tol', 'elongacion_carga_ancho_tol_minus',
     'elongación ancho', None),
)

RANGO_HELP = ('Otra forma de capturar la tolerancia: escriba el mínimo y el máximo y la ficha calcula las '
              'tolerancias (+) y (−). El nominal se queda si cae dentro del rango; si está vacío o fuera, pasa al '
              'centro.')


def rango_a_tolerancias(nominal, minimo, maximo):
    """(nominal, tolerancia +, tolerancia −) para un rango de ``minimo`` a ``maximo``. ``None`` si el rango está
    al revés (el mínimo arriba del máximo)."""
    if minimo > maximo + EPS:
        return None
    if not (minimo - EPS <= nominal <= maximo + EPS):
        nominal = (minimo + maximo) / 2.0
    return nominal, max(maximo - nominal, 0.0), max(nominal - minimo, 0.0)


def _nombres_rango(medidas):
    return [nominal + sufijo for nominal, *_ in medidas for sufijo in ('_min', '_max')]


def _depends(medidas):
    return [name for nominal, plus, minus, *_ in medidas for name in (nominal, plus, minus)]


def compute_rangos(records, medidas):
    for rec in records:
        for nominal, plus, minus, *_ in medidas:
            rec[nominal + '_min'] = rec[nominal] - rec[minus]
            rec[nominal + '_max'] = rec[nominal] + rec[plus]


def aplicar_rangos(records, medidas, estricto):
    """Pasa el rango capturado a nominal y tolerancias. ``estricto``: al guardar, un rango al revés es error; en
    pantalla se espera a que el usuario escriba el otro extremo."""
    for rec in records:
        for nominal, plus, minus, etiqueta, _digits in medidas:
            minimo, maximo = rec[nominal + '_min'], rec[nominal + '_max']
            if (abs(minimo - (rec[nominal] - rec[minus])) < EPS
                    and abs(maximo - (rec[nominal] + rec[plus])) < EPS):
                continue  # el rango de esta medida no cambió
            valores = rango_a_tolerancias(rec[nominal], minimo, maximo)
            if valores is None:
                if estricto:
                    raise ValidationError(
                        'El rango de %s de «%s» está al revés: el mínimo (%g) es mayor que el máximo (%g).'
                        % (etiqueta, rec.display_name, minimo, maximo))
                continue
            rec[nominal], rec[plus], rec[minus] = valores


class FichaTecnicaAcabadoRango(models.Model):
    _inherit = 'ficha.tecnica.acabado'

    peso_acabado_min = fields.Float(
        string='Peso mínimo (rango)', compute='_compute_ft_rangos',
        inverse='_inverse_ft_rangos', help=RANGO_HELP)
    peso_acabado_max = fields.Float(
        string='Peso máximo (rango)', compute='_compute_ft_rangos',
        inverse='_inverse_ft_rangos', help=RANGO_HELP)
    ancho_acabado_min = fields.Float(
        string='Ancho mínimo (rango)', compute='_compute_ft_rangos',
        inverse='_inverse_ft_rangos', help=RANGO_HELP)
    ancho_acabado_max = fields.Float(
        string='Ancho máximo (rango)', compute='_compute_ft_rangos',
        inverse='_inverse_ft_rangos', help=RANGO_HELP)
    encogimiento_largo_min = fields.Float(
        string='Encogimiento largo mínimo (rango)', compute='_compute_ft_rangos',
        inverse='_inverse_ft_rangos', help=RANGO_HELP)
    encogimiento_largo_max = fields.Float(
        string='Encogimiento largo máximo (rango)', compute='_compute_ft_rangos',
        inverse='_inverse_ft_rangos', help=RANGO_HELP)
    encogimiento_ancho_min = fields.Float(
        string='Encogimiento ancho mínimo (rango)', compute='_compute_ft_rangos',
        inverse='_inverse_ft_rangos', help=RANGO_HELP)
    encogimiento_ancho_max = fields.Float(
        string='Encogimiento ancho máximo (rango)', compute='_compute_ft_rangos',
        inverse='_inverse_ft_rangos', help=RANGO_HELP)
    espesor_acabado_min = fields.Float(
        string='Espesor mínimo (rango)', digits=(16, 3), compute='_compute_ft_rangos',
        inverse='_inverse_ft_rangos', help=RANGO_HELP)
    espesor_acabado_max = fields.Float(
        string='Espesor máximo (rango)', digits=(16, 3), compute='_compute_ft_rangos',
        inverse='_inverse_ft_rangos', help=RANGO_HELP)
    elongacion_largo_acabado_min = fields.Float(
        string='Elongación largo mínimo (rango)', compute='_compute_ft_rangos',
        inverse='_inverse_ft_rangos', help=RANGO_HELP)
    elongacion_largo_acabado_max = fields.Float(
        string='Elongación largo máximo (rango)', compute='_compute_ft_rangos',
        inverse='_inverse_ft_rangos', help=RANGO_HELP)
    elongacion_ancho_acabado_min = fields.Float(
        string='Elongación ancho mínimo (rango)', compute='_compute_ft_rangos',
        inverse='_inverse_ft_rangos', help=RANGO_HELP)
    elongacion_ancho_acabado_max = fields.Float(
        string='Elongación ancho máximo (rango)', compute='_compute_ft_rangos',
        inverse='_inverse_ft_rangos', help=RANGO_HELP)

    @api.depends(*_depends(MEDIDAS_ACABADO))
    def _compute_ft_rangos(self):
        compute_rangos(self, MEDIDAS_ACABADO)

    def _inverse_ft_rangos(self):
        aplicar_rangos(self, MEDIDAS_ACABADO, estricto=True)

    @api.onchange(*_nombres_rango(MEDIDAS_ACABADO))
    def _onchange_ft_rangos(self):
        aplicar_rangos(self, MEDIDAS_ACABADO, estricto=False)


class FichaTecnicaTejidoRango(models.Model):
    _inherit = 'ficha.tecnica.tejido'

    peso_acondicionado_min = fields.Float(
        string='Peso mínimo (rango)', compute='_compute_ft_rangos',
        inverse='_inverse_ft_rangos', help=RANGO_HELP)
    peso_acondicionado_max = fields.Float(
        string='Peso máximo (rango)', compute='_compute_ft_rangos',
        inverse='_inverse_ft_rangos', help=RANGO_HELP)
    ancho_acondicionado_min = fields.Float(
        string='Ancho mínimo (rango)', compute='_compute_ft_rangos',
        inverse='_inverse_ft_rangos', help=RANGO_HELP)
    ancho_acondicionado_max = fields.Float(
        string='Ancho máximo (rango)', compute='_compute_ft_rangos',
        inverse='_inverse_ft_rangos', help=RANGO_HELP)
    espesor_acondicionado_min = fields.Float(
        string='Espesor mínimo (rango)', digits=(16, 3), compute='_compute_ft_rangos',
        inverse='_inverse_ft_rangos', help=RANGO_HELP)
    espesor_acondicionado_max = fields.Float(
        string='Espesor máximo (rango)', digits=(16, 3), compute='_compute_ft_rangos',
        inverse='_inverse_ft_rangos', help=RANGO_HELP)
    elongacion_carga_largo_min = fields.Float(
        string='Elongación largo mínimo (rango)', compute='_compute_ft_rangos',
        inverse='_inverse_ft_rangos', help=RANGO_HELP)
    elongacion_carga_largo_max = fields.Float(
        string='Elongación largo máximo (rango)', compute='_compute_ft_rangos',
        inverse='_inverse_ft_rangos', help=RANGO_HELP)
    elongacion_carga_ancho_min = fields.Float(
        string='Elongación ancho mínimo (rango)', compute='_compute_ft_rangos',
        inverse='_inverse_ft_rangos', help=RANGO_HELP)
    elongacion_carga_ancho_max = fields.Float(
        string='Elongación ancho máximo (rango)', compute='_compute_ft_rangos',
        inverse='_inverse_ft_rangos', help=RANGO_HELP)

    @api.depends(*_depends(MEDIDAS_TEJIDO))
    def _compute_ft_rangos(self):
        compute_rangos(self, MEDIDAS_TEJIDO)

    def _inverse_ft_rangos(self):
        aplicar_rangos(self, MEDIDAS_TEJIDO, estricto=True)

    @api.onchange(*_nombres_rango(MEDIDAS_TEJIDO))
    def _onchange_ft_rangos(self):
        aplicar_rangos(self, MEDIDAS_TEJIDO, estricto=False)
