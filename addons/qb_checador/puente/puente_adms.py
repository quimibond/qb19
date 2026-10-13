#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Puente ADMS → Odoo para relojes ZKTeco que no saben HTTPS.

Los relojes de la planta (ZKTeco UA860 y MB10-VL) traen el protocolo ADMS /
PUSH, pero su pantalla «Servidor en la nube» no tiene la opción HTTPS y Odoo
solo acepta conexiones cifradas. Este programa corre en una PC de la planta
que siempre está encendida: los relojes le mandan sus checadas en HTTP plano
por la red local y el puente las reenvía, tal cual, a Odoo por HTTPS. La
respuesta de Odoo regresa al reloj sin cambios, así que el reloj se comporta
exactamente igual que si hablara directo con Odoo:

    reloj ──HTTP (LAN, puerto 8081)──► puente ──HTTPS──► quimibond-qb19.odoo.com/iclock/...

También reenvía el push de eventos de los relojes Hikvision
(``/qb_checador/hikvision/<token>``) para el que no sepa HTTPS en su «Escucha
HTTP»; para Odoo es lo mismo que si el reloj hablara directo.

- Si Odoo no contesta (sin internet, Odoo en mantenimiento), el puente
  responde 502 y el reloj guarda sus registros y reintenta solo: no se
  pierde nada.
- Odoo reconoce al reloj por su número de serie (va en la URL que manda el
  reloj); el puente no agrega ni quita nada.
- No necesita librerías: solo Python 3.8 o más nuevo.

Uso:
    python puente_adms.py            (lee puente_adms.ini junto al archivo)
    python puente_adms.py --probar   (manda una petición de prueba a Odoo y sale)

En el navegador de la PC, http://localhost:8081/ muestra el estado del puente
(última petición de cada reloj y cuántas van).
"""
import argparse
import configparser
import datetime as dt
import http.server
import logging
import os
import sys
import threading
import urllib.error
import urllib.request
from logging.handlers import RotatingFileHandler

AQUI = os.path.dirname(os.path.abspath(__file__))
INI = os.path.join(AQUI, 'puente_adms.ini')

CONFIG = {
    'odoo': 'https://quimibond-qb19.odoo.com',
    'puerto': 8081,
    'escuchar': '0.0.0.0',
    'timeout': 40,
    'log': os.path.join(AQUI, 'puente_adms.log'),
}

ESTADO = {
    'inicio': dt.datetime.now(),
    'total': 0,
    'errores': 0,
    'relojes': {},   # serie -> {'ultima': datetime, 'path': str, 'status': int, 'n': int}
}
CANDADO = threading.Lock()
log = logging.getLogger('puente')


def cargar_config():
    """Lee puente_adms.ini si existe; lo que no esté ahí usa el valor de arriba."""
    if os.path.exists(INI):
        cp = configparser.ConfigParser()
        cp.read(INI, encoding='utf-8')
        if cp.has_section('puente'):
            s = cp['puente']
            CONFIG['odoo'] = s.get('odoo', CONFIG['odoo']).strip().rstrip('/')
            CONFIG['puerto'] = s.getint('puerto', CONFIG['puerto'])
            CONFIG['escuchar'] = s.get('escuchar', CONFIG['escuchar']).strip()
            CONFIG['timeout'] = s.getint('timeout', CONFIG['timeout'])
            CONFIG['log'] = s.get('log', CONFIG['log']).strip() or CONFIG['log']
    if not CONFIG['odoo'].startswith('https://'):
        raise SystemExit("La dirección de Odoo debe empezar con https:// (está: %r)" % CONFIG['odoo'])
    return CONFIG


def preparar_log():
    log.setLevel(logging.INFO)
    fmt = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    archivo = RotatingFileHandler(CONFIG['log'], maxBytes=2_000_000, backupCount=5, encoding='utf-8')
    archivo.setFormatter(fmt)
    log.addHandler(archivo)
    consola = logging.StreamHandler(sys.stdout)
    consola.setFormatter(fmt)
    log.addHandler(consola)


RUTAS = ('/iclock/', '/qb_checador/hikvision/')


def serie_de(path):
    """El ZKTeco manda su serie como SN=... en la URL; el Hikvision, su token en la ruta
    (en el estado se muestran solo los últimos 6 caracteres)."""
    if path.startswith('/qb_checador/hikvision/'):
        token = path.split('?', 1)[0].rstrip('/').rsplit('/', 1)[-1]
        return 'hikvision …%s' % token[-6:]
    if '?' not in path:
        return ''
    for par in path.split('?', 1)[1].split('&'):
        if par.upper().startswith('SN='):
            return par[3:]
    return ''


def reenviar(metodo, path, cuerpo, content_type):
    """Manda la misma petición a Odoo. Devuelve (status, bytes, content_type)."""
    url = CONFIG['odoo'] + path
    cabeceras = {'User-Agent': 'qb-puente-adms/1.0'}
    if content_type:
        cabeceras['Content-Type'] = content_type
    req = urllib.request.Request(url, data=cuerpo if metodo == 'POST' else None,
                                 headers=cabeceras, method=metodo)
    try:
        with urllib.request.urlopen(req, timeout=CONFIG['timeout']) as resp:
            return resp.status, resp.read(), resp.headers.get('Content-Type', 'text/plain')
    except urllib.error.HTTPError as e:
        # 403 «Equipo no registrado», 404, 500...: se le pasa al reloj tal cual.
        return e.code, e.read() or str(e.reason).encode(), e.headers.get('Content-Type', 'text/plain')
    except (urllib.error.URLError, OSError) as e:
        # Sin internet u Odoo caído: 502 para que el reloj guarde y reintente.
        raise ConnectionError(str(e))


class Manejador(http.server.BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.0'   # los relojes esperan una respuesta corta y cierre
    server_version = 'qb-puente-adms/1.0'

    def log_message(self, formato, *args):   # el log propio ya registra todo
        pass

    def _responder(self, status, cuerpo, content_type='text/plain; charset=utf-8'):
        if isinstance(cuerpo, str):
            cuerpo = cuerpo.encode('utf-8')
        self.send_response(status)
        self.send_header('Content-Type', content_type)
        self.send_header('Content-Length', str(len(cuerpo)))
        self.send_header('Connection', 'close')
        self.end_headers()
        self.wfile.write(cuerpo)

    def _anotar(self, serie, status, inicio_ok=True):
        with CANDADO:
            ESTADO['total'] += 1
            if status >= 500:
                ESTADO['errores'] += 1
            r = ESTADO['relojes'].setdefault(serie or '(sin serie)', {'n': 0})
            r.update({'ultima': dt.datetime.now(), 'path': self.path.split('?')[0], 'status': status})
            r['n'] += 1

    def _pasar(self):
        if not self.path.startswith(RUTAS):
            if self.path in ('/', '/estado'):
                return self._responder(200, pagina_estado())
            return self._responder(404, 'Solo /iclock/... (ZKTeco), /qb_checador/hikvision/... (Hikvision) '
                                        'y / (estado)\n')
        serie = serie_de(self.path)
        largo = int(self.headers.get('Content-Length') or 0)
        cuerpo = self.rfile.read(largo) if largo else b''
        try:
            status, datos, ct = reenviar(self.command, self.path, cuerpo, self.headers.get('Content-Type'))
        except ConnectionError as e:
            self._anotar(serie, 502)
            log.error("%s %s %s → Odoo no contesta: %s", serie, self.command, self.path.split('?')[0], e)
            return self._responder(502, 'Odoo no disponible; reintentar\n')
        self._anotar(serie, status)
        resumen = datos[:60].decode('utf-8', 'replace').replace('\r', ' ').replace('\n', ' ')
        log.info("%s %s %s (%d bytes) → %s %s", serie, self.command, self.path.split('?')[0], largo, status, resumen)
        self._responder(status, datos, ct)

    do_GET = _pasar
    do_POST = _pasar


def pagina_estado():
    ahora = dt.datetime.now()
    lineas = [
        'Puente ADMS → Odoo',
        'Odoo: %s' % CONFIG['odoo'],
        'Corriendo desde: %s (%s)' % (ESTADO['inicio'].strftime('%Y-%m-%d %H:%M:%S'), str(ahora - ESTADO['inicio']).split('.')[0]),
        'Peticiones: %d, con Odoo sin contestar: %d' % (ESTADO['total'], ESTADO['errores']),
        '',
        'Relojes vistos:',
    ]
    if not ESTADO['relojes']:
        lineas.append('  (ninguno todavía: revisar la dirección y el puerto en el reloj)')
    for serie, r in sorted(ESTADO['relojes'].items()):
        lineas.append('  %s  última %s  %s → %s  (%d peticiones)' % (
            serie, r['ultima'].strftime('%Y-%m-%d %H:%M:%S'), r['path'], r['status'], r['n']))
    return '\n'.join(lineas) + '\n'


def probar():
    """Una petición de opciones a Odoo con una serie de prueba: confirma internet, DNS y que Odoo contesta."""
    path = '/iclock/cdata?SN=PRUEBA-PUENTE&options=all&pushver=2.4.1'
    try:
        status, datos, _ = reenviar('GET', path, b'', None)
    except ConnectionError as e:
        print("NO llega a Odoo:", e)
        print("Revisar internet, DNS y que el firewall deje salir por el puerto 443 a", CONFIG['odoo'])
        return 1
    texto = datos.decode('utf-8', 'replace').strip()
    print("Odoo contestó %s: %s" % (status, texto.splitlines()[0] if texto else ''))
    if status == 403:
        print("Correcto: la PC llega a Odoo (403 porque la serie de prueba no está registrada).")
        return 0
    if status == 200:
        print("Correcto: la PC llega a Odoo.")
        return 0
    print("Respuesta inesperada; mandar este mensaje a José.")
    return 1


def main():
    ap = argparse.ArgumentParser(description=__doc__.split('\n')[0])
    ap.add_argument('--probar', action='store_true', help='probar la conexión a Odoo y salir')
    args = ap.parse_args()
    cargar_config()
    preparar_log()
    if args.probar:
        return probar()
    servidor = http.server.ThreadingHTTPServer((CONFIG['escuchar'], CONFIG['puerto']), Manejador)
    servidor.daemon_threads = True
    log.info("Puente escuchando en %s:%d → %s", CONFIG['escuchar'], CONFIG['puerto'], CONFIG['odoo'])
    try:
        servidor.serve_forever()
    except KeyboardInterrupt:
        log.info("Puente detenido")
    return 0


if __name__ == '__main__':
    sys.exit(main())
