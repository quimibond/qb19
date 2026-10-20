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

Y para los Hikvision que no mandan sus eventos (el DS-K1T804AEF de Ciruelos
rechaza la suscripción), el puente se los **pide**: cada minuto consulta el
historial del reloj (ISAPI ``AcsEvent``, con usuario y contraseña del reloj)
y manda a Odoo, uno por uno, los eventos nuevos en el mismo formato del push.
Lo que ya mandó lo recuerda en ``puente_hikvision.json`` (número de serie del
último evento por reloj); si Odoo o el internet se caen, el reloj sigue
guardando y el puente se pone al día en la siguiente vuelta. Se configura con
una sección ``[hikvision <nombre>]`` por reloj en ``puente_adms.ini``; la
contraseña vive solo en ese archivo de la PC, nunca en Odoo ni en el repo.

- Si Odoo no contesta (sin internet, Odoo en mantenimiento), el puente
  responde 502 y el reloj guarda sus registros y reintenta solo: no se
  pierde nada.
- Odoo reconoce al reloj por su número de serie (va en la URL que manda el
  reloj); el puente no agrega ni quita nada.
- No necesita librerías: solo Python 3.8 o más nuevo.

Uso:
    python puente_adms.py            (lee puente_adms.ini junto al archivo)
    python puente_adms.py --probar   (manda una petición de prueba a Odoo y sale)
    python puente_adms.py --consultar   (una vuelta de consulta a los Hikvision y sale)

En el navegador de la PC, http://localhost:8081/ muestra el estado del puente
(última petición de cada reloj y cuántas van).
"""
import argparse
import configparser
import datetime as dt
import http.server
import json
import logging
import os
import sys
import threading
import urllib.error
import urllib.request
from logging.handlers import RotatingFileHandler

AQUI = os.path.dirname(os.path.abspath(__file__))
INI = os.path.join(AQUI, 'puente_adms.ini')
CURSORES = os.path.join(AQUI, 'puente_hikvision.json')

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
HIKVISION = []   # relojes a consultar: {'nombre', 'ip', 'usuario', 'clave', 'token', 'desde', 'cada'}


def cargar_config():
    """Lee puente_adms.ini si existe; lo que no esté ahí usa el valor de arriba."""
    if os.path.exists(INI):
        cp = configparser.ConfigParser(interpolation=None)   # una contraseña puede traer %
        cp.read(INI, encoding='utf-8')
        if cp.has_section('puente'):
            s = cp['puente']
            CONFIG['odoo'] = s.get('odoo', CONFIG['odoo']).strip().rstrip('/')
            CONFIG['puerto'] = s.getint('puerto', CONFIG['puerto'])
            CONFIG['escuchar'] = s.get('escuchar', CONFIG['escuchar']).strip()
            CONFIG['timeout'] = s.getint('timeout', CONFIG['timeout'])
            CONFIG['log'] = s.get('log', CONFIG['log']).strip() or CONFIG['log']
        for seccion in cp.sections():
            if not seccion.lower().startswith('hikvision'):
                continue
            s = cp[seccion]
            faltan = [k for k in ('ip', 'clave', 'token') if not s.get(k, '').strip()]
            if faltan:
                raise SystemExit("[%s] en puente_adms.ini: falta %s" % (seccion, ', '.join(faltan)))
            HIKVISION.append({
                'nombre': seccion.split(None, 1)[1].strip() if ' ' in seccion.strip() else seccion,
                'ip': s.get('ip').strip(),
                'usuario': s.get('usuario', 'admin').strip(),
                'clave': s.get('clave').strip(),
                'token': s.get('token').strip(),
                'desde': s.get('desde', '').strip() or dt.date.today().isoformat(),
                'cada': max(30, s.getint('cada_segundos', 60)),
            })
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
            log.warning("%s %s %s → 404 (ruta que el puente no reenvía)", self.client_address[0], self.command,
                        self.path.split('?')[0])
            return self._responder(404, 'Solo /iclock/... (ZKTeco), /qb_checador/hikvision/... (Hikvision) '
                                        'y / (estado)\n')
        serie = serie_de(self.path)
        largo = int(self.headers.get('Content-Length') or 0)
        cuerpo = self.rfile.read(largo) if largo else b''
        content_type = self.headers.get('Content-Type')
        if self.path.startswith('/iclock/') and self.command == 'POST':
            # El UA860 (PUSH 2.2) declara sus checadas como formulario web; Odoo lo lee como formulario y el
            # cuerpo le llega vacío («OK: 0» y el reloj reenvía lo mismo sin fin). Son líneas de texto.
            content_type = 'text/plain; charset=utf-8'
        try:
            status, datos, ct = reenviar(self.command, self.path, cuerpo, content_type)
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


# ----------------------------------------------------------------------
# Hikvision por consulta (ISAPI AcsEvent)
# ----------------------------------------------------------------------
MAJOR_CONTROL_ACCESO = 5
ESPERA_TRAS_401 = 1800   # el reloj se bloquea tras pocos intentos fallidos: no insistir con una clave mala


class ClaveIncorrecta(Exception):
    pass


def leer_cursores():
    try:
        with open(CURSORES, encoding='utf-8') as f:
            return json.load(f)
    except (OSError, ValueError):
        return {}


def guardar_cursores(cursores):
    tmp = CURSORES + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(cursores, f, ensure_ascii=False, indent=1)
    os.replace(tmp, CURSORES)


def _zona_local():
    """-06:00 para la hora local de esta PC (el reloj acepta la hora con su desfase)."""
    z = dt.datetime.now().astimezone().strftime('%z') or '+0000'
    return '%s:%s' % (z[:3], z[3:])


def _abridor_reloj(reloj):
    claves = urllib.request.HTTPPasswordMgrWithDefaultRealm()
    claves.add_password(None, 'http://%s/' % reloj['ip'], reloj['usuario'], reloj['clave'])
    return urllib.request.build_opener(urllib.request.HTTPDigestAuthHandler(claves))


def eventos_del_reloj(reloj, desde, abridor=None):
    """Todos los eventos de control de acceso (major 5) del reloj desde ``desde``
    (``AAAA-MM-DDTHH:MM:SS±HH:MM``) hasta mañana, en páginas de 10 (el máximo del reloj)."""
    abridor = abridor or _abridor_reloj(reloj)
    hasta = (dt.date.today() + dt.timedelta(days=1)).isoformat() + 'T23:59:59' + _zona_local()
    url = 'http://%s/ISAPI/AccessControl/AcsEvent?format=json' % reloj['ip']
    eventos, posicion = [], 0
    while True:
        cond = {'AcsEventCond': {'searchID': 'qbpuente', 'searchResultPosition': posicion, 'maxResults': 10,
                                 'major': MAJOR_CONTROL_ACCESO, 'minor': 0, 'startTime': desde, 'endTime': hasta}}
        req = urllib.request.Request(url, data=json.dumps(cond).encode('utf-8'), method='POST',
                                     headers={'Content-Type': 'application/json'})
        try:
            with abridor.open(req, timeout=CONFIG['timeout']) as resp:
                datos = json.loads(resp.read().decode('utf-8', 'replace'))
        except urllib.error.HTTPError as e:
            if e.code == 401:
                raise ClaveIncorrecta("el reloj rechazó usuario/contraseña")
            raise ConnectionError("el reloj contestó %s: %s" % (e.code, e.read()[:200]))
        except (urllib.error.URLError, OSError, ValueError) as e:
            raise ConnectionError("no se pudo leer el reloj: %s" % e)
        bloque = datos.get('AcsEvent') or {}
        pagina = bloque.get('InfoList') or []
        eventos.extend(pagina)
        posicion += len(pagina)
        if bloque.get('responseStatusStrg') != 'MORE' or not pagina:
            return eventos


def evento_push(info):
    """Un renglón de ``AcsEvent.InfoList`` → el evento como lo manda el push del reloj
    (lo que Odoo ya sabe leer en ``/qb_checador/hikvision/<token>``)."""
    ace = {'majorEventType': info.get('major'), 'subEventType': info.get('minor')}
    for k in ('employeeNoString', 'name', 'serialNo', 'currentVerifyMode', 'attendanceStatus', 'cardReaderNo',
              'cardNo', 'cardType'):
        if info.get(k) not in (None, ''):
            ace[k] = info[k]
    return {'eventType': 'AccessControllerEvent', 'eventState': 'active', 'dateTime': info.get('time'),
            'AccessControllerEvent': ace}


def nuevos(eventos, cursor):
    """Los eventos con número de serie mayor al último mandado, en orden."""
    ultimo = int(cursor.get('serial') or 0)
    salida = [e for e in eventos if int(e.get('serialNo') or 0) > ultimo]
    return sorted(salida, key=lambda e: int(e.get('serialNo') or 0))


def consultar_reloj(reloj, cursores):
    """Una vuelta: lee lo nuevo del reloj y lo manda a Odoo. Devuelve cuántos eventos mandó."""
    cursor = cursores.get(reloj['token']) or {}
    desde = cursor.get('time') or (reloj['desde'] + 'T00:00:00' + _zona_local())
    pendientes = nuevos(eventos_del_reloj(reloj, desde), cursor)
    path = '/qb_checador/hikvision/' + reloj['token']
    mandados = 0
    for info in pendientes:
        cuerpo = json.dumps(evento_push(info), ensure_ascii=False).encode('utf-8')
        status, datos, _ = reenviar('POST', path, cuerpo, 'application/json')
        if status != 200:
            raise ConnectionError("Odoo contestó %s: %s" % (status, datos[:120].decode('utf-8', 'replace')))
        cursor = {'serial': int(info.get('serialNo') or 0), 'time': info.get('time') or desde}
        cursores[reloj['token']] = cursor
        guardar_cursores(cursores)
        mandados += 1
    with CANDADO:
        r = ESTADO['relojes'].setdefault('hikvision %s (consulta)' % reloj['nombre'], {'n': 0})
        r.update({'ultima': dt.datetime.now(), 'path': 'AcsEvent → %d nuevos' % mandados, 'status': 200})
        r['n'] += 1
    return mandados


def vigilar_reloj(reloj):
    """Hilo por reloj: consulta cada ``cada`` segundos; los errores se anotan y se reintenta."""
    log.info("Hikvision %s (%s): consulta cada %d s", reloj['nombre'], reloj['ip'], reloj['cada'])
    while True:
        espera = reloj['cada']
        try:
            n = consultar_reloj(reloj, leer_cursores())
            if n:
                log.info("Hikvision %s: %d evento(s) nuevo(s) → Odoo", reloj['nombre'], n)
        except ClaveIncorrecta as e:
            log.error("Hikvision %s: %s; revisar la clave en puente_adms.ini (espero %d min)",
                      reloj['nombre'], e, ESPERA_TRAS_401 // 60)
            espera = ESPERA_TRAS_401
        except ConnectionError as e:
            log.error("Hikvision %s: %s (reintento en %d s)", reloj['nombre'], e, espera)
        except Exception:   # que un error raro no mate el hilo
            log.exception("Hikvision %s: error inesperado", reloj['nombre'])
        threading.Event().wait(espera)


def consultar_una_vez():
    if not HIKVISION:
        print("No hay secciones [hikvision ...] en puente_adms.ini")
        return 1
    codigo = 0
    for reloj in HIKVISION:
        try:
            n = consultar_reloj(reloj, leer_cursores())
            print("Hikvision %s: %d evento(s) nuevo(s) mandado(s) a Odoo" % (reloj['nombre'], n))
        except (ClaveIncorrecta, ConnectionError) as e:
            print("Hikvision %s: %s" % (reloj['nombre'], e))
            codigo = 1
    return codigo


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
    ap.add_argument('--consultar', action='store_true', help='una vuelta de consulta a los Hikvision y salir')
    args = ap.parse_args()
    cargar_config()
    preparar_log()
    if args.probar:
        return probar()
    if args.consultar:
        return consultar_una_vez()
    servidor = http.server.ThreadingHTTPServer((CONFIG['escuchar'], CONFIG['puerto']), Manejador)
    servidor.daemon_threads = True
    log.info("Puente escuchando en %s:%d → %s", CONFIG['escuchar'], CONFIG['puerto'], CONFIG['odoo'])
    for reloj in HIKVISION:
        threading.Thread(target=vigilar_reloj, args=(reloj,), daemon=True).start()
    try:
        servidor.serve_forever()
    except KeyboardInterrupt:
        log.info("Puente detenido")
    return 0


if __name__ == '__main__':
    sys.exit(main())
