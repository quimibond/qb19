# -*- coding: utf-8 -*-
"""Receptor del push de eventos ISAPI de Hikvision («HTTP Listening»).

Los terminales de control de acceso y asistencia de Hikvision (DS-K1T…) no
hablan el ADMS de ZKTeco. Se configura en el reloj un «servidor de escucha
HTTP» (``/ISAPI/Event/notification/httpHosts``) que apunta a::

    https://<odoo>/qb_checador/hikvision/<token>

y el reloj manda cada evento en el momento en que ocurre, como JSON, XML o
multipart (el JSON o XML del evento más la foto, si la toma). El token
identifica al reloj (va en su ficha en Odoo); un token desconocido recibe 403.

Solo se guardan como checadas los eventos de control de acceso
(``eventType = AccessControllerEvent``, ``majorEventType = 5``) que traen
número de empleado: son las verificaciones en las que el reloj reconoció a la
persona (huella, rostro, tarjeta o contraseña). Una huella no reconocida, la
puerta abierta o cerrada, el botón de salida o una alarma no traen empleado y
se contestan con OK sin guardar nada. Si algún código de evento con empleado
no debe contar (p. ej. «sin permiso a esa hora»), se excluye con el parámetro
``qb_checador.hikvision_eventos_excluidos`` (lista de ``subEventType`` separados
por coma).

La hora se toma como la marca el reloj (hora local) y se convierte a UTC con
la zona horaria del equipo en Odoo, igual que en los ZKTeco: si el reloj
trae una zona horaria equivocada (de fábrica suele traer GMT+08:00) pero la
hora en pantalla es la correcta, la checada queda bien.

Hikvision no reintenta un evento que no se pudo entregar: si Odoo no
contesta, el evento queda solo en la memoria del reloj (hasta 100,000) y se
recupera exportándolo. Por eso se contesta 200 a todo lo que llega de un reloj
autorizado, aunque no se guarde.
"""
import email.parser
import email.policy
import json
import logging
import re

from lxml import etree

from odoo import http
from odoo.http import request

_logger = logging.getLogger(__name__)

OK_JSON = json.dumps({'statusCode': 1, 'statusString': 'OK', 'subStatusCode': 'ok'})
MAJOR_CONTROL_ACCESO = 5
PARAM_EXCLUIDOS = 'qb_checador.hikvision_eventos_excluidos'
_XML_PARSER = etree.XMLParser(resolve_entities=False, no_network=True, load_dtd=False, huge_tree=False)


# ----------------------------------------------------------------------
# Lectura del cuerpo (funciones puras, se prueban sin servidor)
# ----------------------------------------------------------------------
def _local(tag):
    return tag.split('}', 1)[-1] if isinstance(tag, str) else ''


def xml_a_dict(elemento):
    """``<a><b>1</b><c><d>x</d></c></a>`` → ``{'b': '1', 'c': {'d': 'x'}}`` sin espacios de nombres.
    Si un hijo se repite, se queda el primero."""
    hijos = [h for h in elemento if isinstance(h.tag, str)]
    if not hijos:
        return (elemento.text or '').strip()
    salida = {}
    for h in hijos:
        salida.setdefault(_local(h.tag), xml_a_dict(h))
    return salida


def _evento_de_texto(texto):
    """Un JSON o XML de evento → dict, o None si no es ninguno de los dos."""
    if isinstance(texto, bytes):
        texto = texto.decode('utf-8', 'replace')
    texto = (texto or '').strip().lstrip('﻿')
    if texto.startswith('{'):
        try:
            datos = json.loads(texto)
        except ValueError:
            return None
        return datos if isinstance(datos, dict) else None
    if texto.startswith('<'):
        try:
            raiz = etree.fromstring(texto.encode('utf-8'), parser=_XML_PARSER)
        except etree.XMLSyntaxError:
            return None
        datos = xml_a_dict(raiz)
        return datos if isinstance(datos, dict) else None
    return None


def eventos_del_cuerpo(cuerpo, content_type):
    """Todos los eventos (dicts) que trae un POST del reloj: JSON, XML o multipart
    (``form-data`` o ``mixed``) con el evento en una parte y la foto en otra."""
    ctype = (content_type or '').lower()
    if 'multipart/' in ctype and cuerpo:
        mensaje = email.parser.BytesParser(policy=email.policy.default).parsebytes(
            b'Content-Type: ' + content_type.encode('latin-1') + b'\r\n\r\n' + cuerpo)
        eventos = []
        for parte in mensaje.walk():
            if parte.is_multipart() or parte.get_content_maintype() == 'image':
                continue
            evento = _evento_de_texto(parte.get_payload(decode=True) or b'')
            if evento:
                eventos.append(evento)
        return eventos
    evento = _evento_de_texto(cuerpo or b'')
    return [evento] if evento else []


def checada_del_evento(evento, excluidos=()):
    """Del dict de un evento, el registro de checada ``{pin, hora_local, status, verify, workcode, raw}``
    o None si el evento no es una verificación con empleado."""
    if 'EventNotificationAlert' in evento and isinstance(evento['EventNotificationAlert'], dict):
        evento = evento['EventNotificationAlert']
    if (evento.get('eventType') or '').lower() != 'accesscontrollerevent':
        return None
    ace = evento.get('AccessControllerEvent') or {}
    if not isinstance(ace, dict):
        return None
    try:
        mayor = int(ace.get('majorEventType'))
    except (TypeError, ValueError):
        return None
    menor = str(ace.get('subEventType') or '').strip()
    if mayor != MAJOR_CONTROL_ACCESO or menor in excluidos:
        return None
    pin = str(ace.get('employeeNoString') or ace.get('employeeNo') or '').strip()
    if not pin or pin == '0':
        return None
    fecha = str(evento.get('dateTime') or '').strip()
    # 2026-10-13T12:36:19-06:00 / 2026-10-13T12:36:19+08:00 / 2026-10-13T12:36:19Z → hora local del reloj
    m = re.match(r'^(\d{4}-\d{2}-\d{2})[T ](\d{2}:\d{2}:\d{2})', fecha)
    if not m:
        return None
    raw = {k: ace.get(k) for k in ('majorEventType', 'subEventType', 'employeeNoString', 'name', 'serialNo',
                                   'currentVerifyMode', 'attendanceStatus', 'cardReaderNo') if ace.get(k) not in (None, '')}
    raw['dateTime'] = fecha
    return {
        'pin': pin,
        'hora_local': '%s %s' % (m.group(1), m.group(2)),
        'status': str(ace.get('attendanceStatus') or '') or None,
        'verify': str(ace.get('currentVerifyMode') or '') or None,
        'workcode': menor or None,
        'raw': json.dumps(raw, ensure_ascii=False, separators=(',', ':')),
    }


def motivo_descartado(evento, excluidos=()):
    """Por qué un evento no cuenta como checada (para el diagnóstico de la ficha)."""
    if 'EventNotificationAlert' in evento and isinstance(evento['EventNotificationAlert'], dict):
        evento = evento['EventNotificationAlert']
    tipo = evento.get('eventType')
    if (tipo or '').lower() != 'accesscontrollerevent':
        return "eventType=%r (solo cuenta AccessControllerEvent)" % tipo
    ace = evento.get('AccessControllerEvent')
    if not isinstance(ace, dict):
        return "sin bloque AccessControllerEvent"
    if str(ace.get('majorEventType')) != str(MAJOR_CONTROL_ACCESO):
        return "majorEventType=%r (solo cuenta 5)" % ace.get('majorEventType')
    if str(ace.get('subEventType') or '').strip() in excluidos:
        return "subEventType=%r excluido por parámetro" % ace.get('subEventType')
    if not str(ace.get('employeeNoString') or ace.get('employeeNo') or '').strip().strip('0'):
        return "sin número de empleado (subEventType=%r)" % ace.get('subEventType')
    if not re.match(r'^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}', str(evento.get('dateTime') or '')):
        return "dateTime ilegible: %r" % evento.get('dateTime')
    return "cuenta"


def diagnostico(metodo, content_type, cuerpo, eventos, extra='', excluidos=()):
    """Texto corto de lo que llegó, para la ficha del reloj (se sobrescribe en cada mensaje)."""
    lineas = ["%s · %s · %d bytes · %d evento(s) leído(s)" % (metodo, content_type or 'sin Content-Type',
                                                              len(cuerpo or b''), len(eventos))]
    for e in eventos[:5]:
        lineas.append("- %s: %s" % (motivo_descartado(e, excluidos), json.dumps(e, ensure_ascii=False)[:1200]))
    if not eventos:
        texto = (cuerpo or b'')[:1500].decode('utf-8', 'replace')
        lineas.append("Cuerpo: %s" % (texto or '(vacío)'))
    if extra:
        lineas.append(extra)
    return '\n'.join(lineas)[:4000]


# ----------------------------------------------------------------------
class HikvisionController(http.Controller):

    def _responder(self, cuerpo, status=200, ctype='application/json'):
        return request.make_response(cuerpo, headers=[('Content-Type', ctype)], status=status)

    @http.route('/qb_checador/hikvision/<string:token>', type='http', auth='public', methods=['GET', 'POST'],
                csrf=False, save_session=False)
    def evento(self, token, **kw):
        Equipo = request.env['qb.checador.equipo'].sudo()
        equipo = Equipo._por_token(token)
        if not equipo:
            _logger.warning("qb_checador: push de Hikvision con token desconocido desde %s",
                            request.httprequest.remote_addr)
            return self._responder("Equipo no registrado", 403, 'text/plain; charset=utf-8')
        if not equipo._acepta_checadas():
            equipo._tocar()
            return self._responder("Equipo no autorizado", 403, 'text/plain; charset=utf-8')

        httpreq = request.httprequest
        cuerpo = httpreq.get_data() if httpreq.method == 'POST' else b''
        eventos = eventos_del_cuerpo(cuerpo, httpreq.headers.get('Content-Type'))
        extra = ''
        if httpreq.method == 'POST' and (httpreq.form or httpreq.files):
            extra = "Formulario: campos %s; archivos %s" % (
                list(httpreq.form.keys()),
                ["%s (%s)" % (k, f.mimetype) for k, f in httpreq.files.items()])
        if not eventos and httpreq.method == 'POST':
            # multipart/form-data: Odoo ya leyó el cuerpo como formulario; el evento viene en un campo o archivo.
            for valor in list(httpreq.form.values()):
                evento = _evento_de_texto(valor)
                if evento:
                    eventos.append(evento)
            for archivo in httpreq.files.values():
                if (archivo.mimetype or '').startswith('image/'):
                    continue
                evento = _evento_de_texto(archivo.read())
                if evento:
                    eventos.append(evento)

        ip = next((e.get('ipAddress') for e in eventos if isinstance(e.get('ipAddress'), str)), None)
        if ip and not equipo.ip:
            equipo.write({'ip': ip})
        n = request.env['qb.checada'].sudo().procesar_hikvision(equipo, eventos)
        excluidos = {v.strip() for v in (request.env['ir.config_parameter'].sudo().get_param(PARAM_EXCLUIDOS) or '')
                     .split(',') if v.strip()}
        texto = diagnostico(httpreq.method, httpreq.headers.get('Content-Type'), cuerpo, eventos, extra, excluidos)
        texto = texto.replace(' evento(s) leído(s)', ' evento(s) leído(s), %d checada(s) nueva(s)' % n, 1)
        _logger.info("qb_checador %s: mensaje Hikvision\n%s", equipo.serial, texto[:1500])
        equipo.write({'ultimo_mensaje': texto})
        equipo._tocar(checada=n > 0)
        return self._responder(OK_JSON)
