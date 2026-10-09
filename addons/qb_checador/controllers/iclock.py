# -*- coding: utf-8 -*-
"""Receptor del protocolo ADMS / PUSH de ZKTeco.

El reloj se configura con «Servidor en la nube» apuntando a Odoo. Desde ahí:

    GET  /iclock/cdata?SN=<serie>&options=all&pushver=2.4.1&language=83
         → opciones (qué mandar, cada cuánto, zona horaria, marca desde la que mandar)
    POST /iclock/cdata?SN=<serie>&table=ATTLOG&Stamp=<n>
         → checadas, una por línea: PIN<TAB>AAAA-MM-DD HH:MM:SS<TAB>STATUS<TAB>VERIFY<TAB>WORKCODE…
         ← "OK: <n>"
    POST /iclock/cdata?SN=<serie>&table=OPERLOG|ATTPHOTO|options|…
         ← "OK" (se acepta y se descarta; solo de «options» se guarda el firmware)
    GET  /iclock/getrequest?SN=<serie>&INFO=…
         ← "OK" (no se le mandan comandos al equipo)
    POST /iclock/devicecmd?SN=<serie>
         ← "OK"

La autorización es el número de serie: solo contestan los equipos registrados,
autorizados y activos. Un desconocido queda registrado inactivo (para que TI
vea su serie) y recibe 403.
"""
import logging
import re

from odoo import http
from odoo.http import request

_logger = logging.getLogger(__name__)

CRLF = '\r\n'


class IclockController(http.Controller):

    # ------------------------------------------------------------------
    def _texto(self, cuerpo, status=200):
        return request.make_response(cuerpo, headers=[('Content-Type', 'text/plain; charset=utf-8')], status=status)

    def _equipo(self, sn, pushver=None):
        """(equipo, respuesta_de_error). El equipo viene en sudo."""
        Equipo = request.env['qb.checador.equipo'].sudo()
        equipo = Equipo._por_serie(sn)
        if not equipo:
            Equipo._registrar_desconocido(sn, pushver)
            _logger.warning("qb_checador: equipo desconocido %r se conectó; registrado sin autorizar", sn)
            return Equipo, self._texto("Equipo no registrado", 403)
        if not equipo._acepta_checadas():
            equipo._tocar(firmware=pushver)
            return Equipo, self._texto("Equipo no autorizado", 403)
        return equipo, None

    # ------------------------------------------------------------------
    @http.route('/iclock/cdata', type='http', auth='public', methods=['GET'], csrf=False, save_session=False)
    def cdata_get(self, SN=None, options=None, pushver=None, **kw):
        equipo, error = self._equipo(SN, pushver)
        if error:
            return error
        equipo._tocar(firmware=pushver and ("PUSH %s" % pushver))
        stamp = equipo.attlog_stamp or '0'
        if (pushver or '').startswith('2') or (pushver or '').startswith('3'):
            transflag = 'TransFlag=TransData AttLog\tOpLog\tAttPhoto\tEnrollUser\tChgUser\tEnrollFP\tChgFP\tFPImag\tFACE\tUserPic'
        else:
            transflag = 'TransFlag=1111000000'
        lineas = [
            'GET OPTION FROM: %s' % equipo.serial,
            'ATTLOGStamp=%s' % stamp,
            'OPERLOGStamp=9999',
            'ATTPHOTOStamp=None',
            'ErrorDelay=30',
            'Delay=10',
            'TransTimes=00:00;14:05',
            'TransInterval=1',
            transflag,
            'TimeZone=%d' % equipo._offset_horas(),
            'Realtime=1',
            'Encrypt=None',
            'ServerVer=3.0.1',
        ]
        if pushver:
            lineas.append('PushProtVer=%s' % pushver)
        return self._texto(CRLF.join(lineas) + CRLF)

    @http.route('/iclock/cdata', type='http', auth='public', methods=['POST'], csrf=False, save_session=False)
    def cdata_post(self, SN=None, table=None, Stamp=None, **kw):
        equipo, error = self._equipo(SN)
        if error:
            return error
        cuerpo = request.httprequest.get_data(as_text=True) or ''
        tabla = (table or '').upper()
        if tabla == 'ATTLOG':
            n = request.env['qb.checada'].sudo().procesar_attlog(equipo, cuerpo)
            equipo._tocar(stamp=Stamp, checada=n > 0)
            return self._texto('OK: %d%s' % (n, CRLF))
        if tabla == 'OPTIONS':
            fw = re.search(r'FWVersion=([^,\r\n]+)', cuerpo)
            equipo._tocar(firmware=fw and fw.group(1))
        else:
            equipo._tocar()
        return self._texto('OK' + CRLF)

    @http.route('/iclock/getrequest', type='http', auth='public', methods=['GET'], csrf=False, save_session=False)
    def getrequest(self, SN=None, INFO=None, **kw):
        equipo, error = self._equipo(SN)
        if error:
            return error
        fw = None
        if INFO:
            # "Ver 6.60 Sep 19 2019,0,0,0,192.168.1.201,…": el primer campo es el firmware
            fw = INFO.split(',')[0].strip()
            partes = INFO.split(',')
            if len(partes) > 4 and re.match(r'^\d+\.\d+\.\d+\.\d+$', partes[4].strip()) and not equipo.ip:
                equipo.sudo().write({'ip': partes[4].strip()})
        equipo._tocar(firmware=fw)
        return self._texto('OK' + CRLF)

    @http.route(['/iclock/devicecmd', '/iclock/ping', '/iclock/registry', '/iclock/push'], type='http',
                auth='public', methods=['GET', 'POST'], csrf=False, save_session=False)
    def otros(self, SN=None, **kw):
        equipo, error = self._equipo(SN)
        if error:
            return error
        equipo._tocar()
        return self._texto('OK' + CRLF)
