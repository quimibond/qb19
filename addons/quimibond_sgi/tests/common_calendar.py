# -*- coding: utf-8 -*-

def sgi_test_calendar(env, tz='UTC'):
    """Calendario propio de lunes a viernes (UTC) para las pruebas de días
    hábiles: el de la compañía varía por base (hay uno sin viernes).

    57.66.0: ``tz`` para probar con la zona de producción
    (America/Mexico_City), donde una fecha mal tratada como medianoche UTC
    cae en el día anterior."""
    calendar = env['resource.calendar'].create({
        'name': 'SGI pruebas L-V', 'tz': tz,
        'attendance_ids': [(5, 0, 0)] + [
            (0, 0, {'name': 'Día %s' % day, 'dayofweek': day, 'hour_from': 8.0,
                    'hour_to': 17.0, 'day_period': 'morning'})
            for day in '01234'],
    })
    env['ir.config_parameter'].sudo().set_param(
        'quimibond_sgi.business_calendar_id', str(calendar.id))
    return calendar
