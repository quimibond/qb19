# -*- coding: utf-8 -*-
"""3.3.9 (Dirección, 2026-11-05): en planta el peso se captura a mano (Tejido, Rama e Inspección). Mientras no haya
báscula IoT registrada, las ventanas de pesaje abren en captura manual con el motivo «Sin báscula IoT» ya escrito: queda
el rastro sin pedir nada en cada rollo. Con una báscula registrada vuelven a abrir en automático y capturar a mano pide
motivo, como antes."""

MOTIVO_SIN_BASCULA = 'Sin báscula IoT'


def sin_bascula(env):
    """True si no hay ninguna báscula IoT registrada (iot.device de tipo «scale»)."""
    return 'iot.device' not in env or not env['iot.device'].sudo().search_count([('type', '=', 'scale')])


def modo_por_defecto(wizard):
    return 'manual' if sin_bascula(wizard.env) else 'iot'


def motivo_por_defecto(wizard):
    return MOTIVO_SIN_BASCULA if sin_bascula(wizard.env) else False
