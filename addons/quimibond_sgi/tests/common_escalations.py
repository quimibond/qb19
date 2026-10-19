# -*- coding: utf-8 -*-


def sgi_escalations_on(env):
    """57.159.0: los escalamientos nacen apagados (``quimibond_sgi.escalations_enabled``
    vacío). Las pruebas que revisan a quién sube lo vencido los encienden; las
    de la pausa (test_escalamientos_pausa) los dejan apagados."""
    env['ir.config_parameter'].sudo().set_param('quimibond_sgi.escalations_enabled', 'True')
