# -*- coding: utf-8 -*-
"""Usuarios y huellas entre Odoo y los relojes ZKTeco (ADMS).

Del reloj a Odoo: cuando alguien se da de alta o registra una huella, el reloj lo manda en
``/iclock/cdata?table=OPERLOG`` (líneas ``USER PIN=…`` y ``FP PIN=… FID=… TMP=…``) o, en firmware
nuevo, en ``table=BIODATA`` (``BIODATA Pin=… No=… Type=1 Tmp=…``). Con la marca OPERLOG en 0 manda
todo lo que tiene (así se trae lo del UA860 viejo).

De Odoo al reloj: una cola de comandos por reloj (``qb.checador.comando``). El reloj pregunta cada
pocos segundos en ``/iclock/getrequest`` y recibe ``C:<id>:<comando>``; contesta el resultado en
``/iclock/devicecmd`` (``ID=<id>&Return=0``). Un comando sin respuesta se reenvía a los 10 minutos,
hasta 3 veces.

La huella es la plantilla que guarda el reloj (base64), no la imagen del dedo: con ella el reloj
reconoce a la persona, pero no se puede reconstruir la huella. Se guarda una por dedo y por reloj de
origen, y se manda a los relojes con «Sincronizar usuarios» con el número de checador del empleado.
"""
import re
from datetime import timedelta

from odoo import _, api, fields, models
from odoo.exceptions import UserError

from .usuario import nombre_plano

TIPOS_USUARIO = ('USER', 'USERINFO')
TIPOS_HUELLA = ('FP', 'FINGERTMP', 'BIODATA', 'TEMPLATEV10')
MINUTOS_REENVIO = 10
INTENTOS_MAX = 3
COMANDOS_POR_PETICION = 20
BYTES_POR_PETICION = 48000


def registros_de_reloj(cuerpo):
    """Las líneas de usuario y de huella que trae un POST del reloj, como ``[(tipo, {campo: valor})]``.

    Acepta los formatos de OPERLOG (``USER PIN=1\\tName=…``, ``FP PIN=1\\tFID=6\\tTMP=…``), BIODATA
    (``BIODATA Pin=1\\tNo=6\\tType=1\\tTmp=…``) y la consulta de tablas (``user pin=1\\tname=…``,
    ``templatev10 pin=1\\tfingerid=6\\ttemplate=…``). Las llaves quedan en minúsculas; las líneas
    ``OPLOG`` y lo desconocido se ignoran."""
    salida = []
    for linea in (cuerpo or '').splitlines():
        linea = linea.strip()
        if not linea:
            continue
        tipo, _sep, resto = linea.partition(' ')
        tipo = tipo.upper()
        if tipo not in TIPOS_USUARIO + TIPOS_HUELLA:
            continue
        datos = {}
        for par in resto.split('\t'):
            llave, igual, valor = par.partition('=')
            if igual:
                datos[llave.strip().lower()] = valor.strip()
        salida.append((tipo, datos))
    return salida


def usuario_de_registro(datos):
    """{pin, nombre, privilegio} de una línea de usuario, o None sin PIN."""
    pin = (datos.get('pin') or '').strip()
    if not pin:
        return None
    return {'pin': pin, 'nombre': datos.get('name') or '', 'privilegio': datos.get('pri') or datos.get('privilege') or ''}


def huella_de_registro(tipo, datos):
    """{pin, dedo, plantilla, tamano, valida, formato, version} de una línea de huella, o None si no es
    huella digital (BIODATA también trae rostro y palma: solo cuenta Type=1)."""
    pin = (datos.get('pin') or '').strip()
    plantilla = datos.get('tmp') or datos.get('template') or ''
    if tipo == 'BIODATA':
        if (datos.get('type') or '1') != '1':
            return None
        dedo = datos.get('no')
    else:
        dedo = datos.get('fid') or datos.get('fingerid')
    if not pin or not plantilla or dedo in (None, ''):
        return None
    try:
        dedo = int(dedo)
    except ValueError:
        return None
    return {
        'pin': pin, 'dedo': dedo, 'plantilla': plantilla,
        'tamano': int(datos.get('size') or 0) or len(plantilla),
        'valida': (datos.get('valid') or '1') != '0',
        'formato': 'biodata' if tipo == 'BIODATA' else 'fingertmp',
        'version': '%s.%s' % (datos.get('majorver') or '10', datos.get('minorver') or '0'),
    }


class ChecadorHuella(models.Model):
    _name = 'qb.checador.huella'
    _description = "Huella registrada en un reloj"
    _order = 'employee_id, dedo, write_date desc'

    equipo_id = fields.Many2one('qb.checador.equipo', string="Reloj de origen", required=True, ondelete='cascade',
                                index=True)
    pin = fields.Char(string="Número en ese reloj", required=True, index=True)
    employee_id = fields.Many2one('hr.employee', string="Empleado", index=True, ondelete='set null')
    dedo = fields.Integer(string="Dedo", help="0 a 9, como lo numera el reloj.")
    plantilla = fields.Text(string="Plantilla", required=True,
                            help="La plantilla de la huella en base64, tal como la manda el reloj. No es una imagen.")
    tamano = fields.Integer(string="Tamaño")
    valida = fields.Boolean(string="Válida", default=True)
    formato = fields.Selection([('fingertmp', "FINGERTMP"), ('biodata', "BIODATA")], string="Formato",
                               default='fingertmp', required=True)
    version = fields.Char(string="Versión del algoritmo", default='10.0')

    _dedo_uniq = models.Constraint('unique(equipo_id, pin, dedo)', 'Esa huella ya está registrada.')


class ChecadorComando(models.Model):
    _name = 'qb.checador.comando'
    _description = "Comando para un reloj"
    _order = 'id desc'

    equipo_id = fields.Many2one('qb.checador.equipo', string="Reloj", required=True, ondelete='cascade', index=True)
    employee_id = fields.Many2one('hr.employee', string="Empleado", ondelete='set null')
    descripcion = fields.Char(string="Qué hace")
    comando = fields.Text(string="Comando", required=True)
    state = fields.Selection([('pendiente', "Pendiente"), ('enviado', "Enviado"), ('ok', "Hecho"),
                              ('error', "Error")], string="Estado", default='pendiente', required=True, index=True)
    intentos = fields.Integer(string="Intentos", default=0)
    enviado_en = fields.Datetime(string="Enviado")
    resultado = fields.Char(string="Respuesta del reloj")

    def action_reintentar(self):
        self.write({'state': 'pendiente', 'intentos': 0, 'resultado': False})


class ChecadorEquipo(models.Model):
    _inherit = 'qb.checador.equipo'

    operlog_stamp = fields.Char(string="Marca OPERLOG", default='0', readonly=True,
                                help="Último «Stamp» de usuarios y huellas que mandó el equipo. En 0 el equipo vuelve "
                                     "a mandar todos sus usuarios y huellas.")
    sincronizar_usuarios = fields.Boolean(
        string="Sincronizar usuarios",
        help="Este reloj recibe de Odoo a los empleados (número de checador y nombre) y sus huellas, y lo que se "
             "registre aquí se manda a los demás relojes que sincronizan. Quiénes: los empleados activos cuya "
             "Referencia lleva uno de los prefijos del reloj.")
    formato_huella = fields.Selection([('fingertmp', "FINGERTMP"), ('biodata', "BIODATA")],
                                      string="Formato de huella", default='fingertmp', required=True,
                                      help="Cómo se le mandan las huellas. Se ajusta solo al formato con el que el "
                                           "reloj manda las suyas.")
    comando_ids = fields.One2many('qb.checador.comando', 'equipo_id', string="Comandos")
    comandos_pendientes = fields.Integer(string="Comandos por entregar", compute='_compute_sincronizacion')
    huella_count = fields.Integer(string="Huellas recibidas", compute='_compute_sincronizacion')

    def _compute_sincronizacion(self):
        Comando = self.env['qb.checador.comando'].sudo()
        Huella = self.env['qb.checador.huella'].sudo()
        for eq in self:
            eq.comandos_pendientes = Comando.search_count([('equipo_id', '=', eq.id),
                                                           ('state', 'in', ('pendiente', 'enviado'))])
            eq.huella_count = Huella.search_count([('equipo_id', '=', eq.id)])

    # ------------------------------------------------------------------
    # Cola de comandos
    # ------------------------------------------------------------------
    def _encolar(self, comando, descripcion=None, empleado=None):
        """Agrega un comando a la cola del reloj, salvo que el mismo ya esté por entregarse y nada lo contradiga
        después: un alta pendiente seguida de una baja del mismo número (o al revés) no cuenta como repetida,
        porque el reloj los ejecuta en orden y el último manda."""
        Comando = self.env['qb.checador.comando'].sudo()
        creados = Comando.browse()
        pin = re.search(r'\b(?:PIN|Pin)=([^\t\n]*)', comando)
        es_baja = comando.startswith('DATA DELETE')
        for eq in self:
            igual = Comando.search([('equipo_id', '=', eq.id), ('comando', '=', comando),
                                    ('state', 'in', ('pendiente', 'enviado'))], order='id desc', limit=1)
            if igual:
                if not pin:
                    continue
                numero = pin.group(1)
                if es_baja:
                    contrario = [('comando', '=like', 'DATA UPDATE %'), ('comando', 'ilike', 'PIN=%s\t' % numero)]
                else:
                    contrario = [('comando', '=', 'DATA DELETE USERINFO PIN=%s' % numero)]
                if not Comando.search_count([('equipo_id', '=', eq.id), ('state', 'in', ('pendiente', 'enviado')),
                                             ('id', '>', igual.id)] + contrario):
                    continue
            creados |= Comando.create({'equipo_id': eq.id, 'comando': comando, 'descripcion': descripcion,
                                       'employee_id': empleado.id if empleado else False})
        return creados

    def _comandos_para_entregar(self):
        """Los comandos que se le entregan al reloj en esta petición (y quedan como enviados)."""
        self.ensure_one()
        Comando = self.env['qb.checador.comando'].sudo()
        limite = fields.Datetime.now() - timedelta(minutes=MINUTOS_REENVIO)
        vencidos = Comando.search([('equipo_id', '=', self.id), ('state', '=', 'enviado'),
                                   ('enviado_en', '<', limite)])
        vencidos.filtered(lambda c: c.intentos >= INTENTOS_MAX).write(
            {'state': 'error', 'resultado': "Sin respuesta del reloj tras %d intentos" % INTENTOS_MAX})
        candidatos = Comando.search([('equipo_id', '=', self.id), '|', ('state', '=', 'pendiente'),
                                     '&', ('state', '=', 'enviado'), ('enviado_en', '<', limite)],
                                    order='id', limit=COMANDOS_POR_PETICION)
        entregar, total = Comando.browse(), 0
        for c in candidatos:
            total += len(c.comando) + 16
            if entregar and total > BYTES_POR_PETICION:
                break
            entregar |= c
        for c in entregar:
            c.write({'state': 'enviado', 'intentos': c.intentos + 1, 'enviado_en': fields.Datetime.now()})
        return entregar

    def _resultado_comando(self, comando_id, retorno, cmd=None):
        """Lo que contestó el reloj a un comando (Return=0 es éxito; negativo, error)."""
        self.ensure_one()
        try:
            comando = self.env['qb.checador.comando'].sudo().browse(int(comando_id))
        except (TypeError, ValueError):
            return
        if not comando.exists() or comando.equipo_id != self:
            return
        try:
            ok = int(retorno) >= 0
        except (TypeError, ValueError):
            ok = False
        comando.write({'state': 'ok' if ok else 'error', 'resultado': "Return=%s %s" % (retorno, cmd or '')})

    # ------------------------------------------------------------------
    # Usuarios y huellas que manda el reloj
    # ------------------------------------------------------------------
    def procesar_usuarios_y_huellas(self, cuerpo):
        """Guarda los usuarios y huellas de un POST del reloj. Devuelve (usuarios, huellas)."""
        self.ensure_one()
        Usuario = self.env['qb.checador.usuario'].sudo()
        Huella = self.env['qb.checador.huella'].sudo()
        n_usuarios = n_huellas = 0
        nuevos = Usuario.browse()
        para_propagar = Huella.browse()
        for tipo, datos in registros_de_reloj(cuerpo):
            if tipo in TIPOS_USUARIO:
                u = usuario_de_registro(datos)
                if not u:
                    continue
                fila = Usuario.search([('equipo_id', '=', self.id), ('pin', '=', u['pin'])], limit=1)
                vals = {'nombre_reloj': u['nombre'] or (fila.nombre_reloj if fila else False),
                        'privilegio': u['privilegio']}
                if fila:
                    fila.write(vals)
                else:
                    vals.update({'equipo_id': self.id, 'pin': u['pin'], 'origen': 'reloj',
                                 'employee_id': self._empleado_por_numero(u['pin']).id or False})
                    nuevos |= Usuario.create(vals)
                n_usuarios += 1
                continue
            h = huella_de_registro(tipo, datos)
            if not h:
                continue
            fila = Usuario.search([('equipo_id', '=', self.id), ('pin', '=', h['pin'])], limit=1)
            empleado = fila.employee_id if fila else self._empleado_por_numero(h['pin'])
            vals = {'plantilla': h['plantilla'], 'tamano': h['tamano'], 'valida': h['valida'],
                    'formato': h['formato'], 'version': h['version'], 'employee_id': empleado.id or False}
            huella = Huella.search([('equipo_id', '=', self.id), ('pin', '=', h['pin']), ('dedo', '=', h['dedo'])],
                                   limit=1)
            if huella:
                cambio = huella.plantilla != h['plantilla']
                huella.write(vals)
            else:
                vals.update({'equipo_id': self.id, 'pin': h['pin'], 'dedo': h['dedo']})
                huella = Huella.create(vals)
                cambio = True
            if h['formato'] == 'biodata' and self.formato_huella != 'biodata':
                self.formato_huella = 'biodata'
            if cambio and huella.employee_id:
                para_propagar |= huella
            n_huellas += 1
        nuevos._sugerir()
        if self.sincronizar_usuarios:
            for huella in para_propagar:
                otros = self._relojes_que_sincronizan(huella.employee_id) - self
                otros._enviar_empleados(huella.employee_id)
        return n_usuarios, n_huellas

    def _empleado_por_numero(self, pin):
        """Empleado por su número de checador (``qb_checador_pin``) o, en un reloj que sincroniza (numerado
        por Odoo), por el número de 4 dígitos que sale de su Referencia (S-4 → 1004). Solo si lleva a una
        sola persona. En un reloj viejo con su propia numeración no se adivina por la Referencia: se sugiere
        por nombre."""
        self.ensure_one()
        Emp = self.env['hr.employee'].sudo()
        dominio = [('company_id', '=', self.company_id.id)]
        por_pin = Emp.search(dominio + [('qb_checador_pin', '=', pin)])
        if len(por_pin) == 1:
            return por_pin
        referencia = Emp._qb_referencia_de_numero(pin) if self.sincronizar_usuarios else None
        if referencia and 'registration_number' in Emp._fields:
            por_ref = Emp.search(dominio + [('registration_number', '=', referencia)])
            if len(por_ref) == 1:
                return por_ref
        return Emp

    # ------------------------------------------------------------------
    # De Odoo a los relojes
    # ------------------------------------------------------------------
    def _prefijos_lista(self):
        self.ensure_one()
        return [p.strip().upper() for p in (self.prefijos or '').split(',') if p.strip()]

    def _empleados_a_sincronizar(self):
        """Empleados activos de la empresa del reloj cuya Referencia lleva uno de sus prefijos, salvo los que
        tienen otros relojes asignados («Relojes donde checa»; vacío = todos)."""
        self.ensure_one()
        Emp = self.env['hr.employee'].sudo()
        if 'registration_number' not in Emp._fields:
            empleados = Emp.search([('company_id', '=', self.company_id.id), ('qb_checador_pin', '!=', False)])
        else:
            prefijos = self._prefijos_lista()
            empleados = Emp.search([('company_id', '=', self.company_id.id), ('registration_number', '!=', False)])
            empleados = empleados.filtered(
                lambda e: (e.registration_number or '').upper().split('-')[0].strip() in prefijos)
        return empleados.filtered(lambda e: not e.qb_checador_equipo_ids or self in e.qb_checador_equipo_ids)

    def _relojes_que_sincronizan(self, empleado):
        """Los relojes ZKTeco activos y autorizados que sincronizan usuarios y en los que va este empleado."""
        relojes = self.sudo().search([('marca', '=', 'zkteco'), ('sincronizar_usuarios', '=', True),
                                      ('autorizado', '=', True), ('company_id', '=', empleado.company_id.id)])
        return relojes.filtered(lambda r: empleado in r._empleados_a_sincronizar())

    def _comando_usuario(self, empleado):
        nombre = nombre_plano(empleado.name)[:24]
        return ('DATA UPDATE USERINFO PIN=%s\tName=%s\tPri=0\tPasswd=\tCard=\tGrp=1\tTZ=0000000100000000\tVerify=0'
                % (empleado.qb_checador_pin, nombre))

    def _comando_huella(self, empleado, huella):
        self.ensure_one()
        if self.formato_huella == 'biodata':
            mayor, _p, menor = (huella.version or '10.0').partition('.')
            return ('DATA UPDATE BIODATA Pin=%s\tNo=%d\tIndex=0\tValid=%d\tDuress=0\tType=1\tMajorVer=%s\t'
                    'MinorVer=%s\tFormat=0\tTmp=%s' % (empleado.qb_checador_pin, huella.dedo, 1 if huella.valida else 0,
                                                      mayor or '10', menor or '0', huella.plantilla))
        return ('DATA UPDATE FINGERTMP PIN=%s\tFID=%d\tSize=%d\tValid=%d\tTMP=%s'
                % (empleado.qb_checador_pin, huella.dedo, huella.tamano or len(huella.plantilla),
                   1 if huella.valida else 0, huella.plantilla))

    def _huellas_de(self, empleado):
        """La huella más reciente de cada dedo del empleado, venga del reloj que venga."""
        huellas = self.env['qb.checador.huella'].sudo().search([('employee_id', '=', empleado.id),
                                                                ('valida', '=', True)], order='write_date desc, id desc')
        por_dedo = {}
        for h in huellas:
            por_dedo.setdefault(h.dedo, h)
        return [por_dedo[d] for d in sorted(por_dedo)]

    def _enviar_empleados(self, empleados):
        """Encola, en cada reloj, el alta de los empleados (número de checador y nombre) y sus huellas."""
        empleados = empleados.sudo()
        empleados._qb_asignar_numero_checador()
        n = 0
        for eq in self.filtered(lambda e: e.marca == 'zkteco'):
            for emp in empleados.filtered('qb_checador_pin'):
                n += len(eq._encolar(eq._comando_usuario(emp), "Alta de %s %s" % (emp.qb_checador_pin, emp.name), emp))
                for huella in eq._huellas_de(emp):
                    n += len(eq._encolar(eq._comando_huella(emp, huella),
                                         "Huella %d de %s %s" % (huella.dedo, emp.qb_checador_pin, emp.name), emp))
        return n

    def action_sincronizar_usuarios(self):
        """Manda al reloj a todos los empleados que le tocan, con su número de checador, nombre y huellas."""
        n = 0
        for eq in self:
            if eq.marca != 'zkteco':
                raise UserError(_("Solo los relojes ZKTeco reciben usuarios desde Odoo."))
            n += eq._enviar_empleados(eq._empleados_a_sincronizar())
        return {'type': 'ir.actions.client', 'tag': 'display_notification', 'params': {
            'title': _("Usuarios en camino"), 'type': 'success', 'sticky': False,
            'message': _("%d comandos en la cola. El reloj los recoge en su siguiente conexión (unos segundos si "
                         "está en línea).") % n}}

    def action_pedir_usuarios(self):
        """Pide al reloj todos sus usuarios y huellas: marca OPERLOG en 0 y orden de releer opciones."""
        for eq in self.filtered(lambda e: e.marca == 'zkteco'):
            eq.operlog_stamp = '0'
            eq._encolar('CHECK', "Releer opciones y mandar usuarios y huellas")

    def action_aceptar_sugerencias(self):
        self.ensure_one()
        self.env['qb.checador.usuario'].sudo().search([('equipo_id', '=', self.id), ('employee_id', '=', False),
                                                       ('sugerido_id', '!=', False)]).action_aceptar_sugerencia()

    def action_ver_comandos(self):
        self.ensure_one()
        return {'type': 'ir.actions.act_window', 'name': _("Comandos de %s") % self.name,
                'res_model': 'qb.checador.comando', 'view_mode': 'list,form', 'domain': [('equipo_id', '=', self.id)]}

    def action_ver_huellas(self):
        self.ensure_one()
        return {'type': 'ir.actions.act_window', 'name': _("Huellas de %s") % self.name,
                'res_model': 'qb.checador.huella', 'view_mode': 'list,form', 'domain': [('equipo_id', '=', self.id)]}

    @api.model
    def _encolar_en_relojes(self, empleado, comando, descripcion):
        """Un comando para todos los relojes que sincronizan y en los que va el empleado."""
        relojes = self.sudo().search([('marca', '=', 'zkteco'), ('sincronizar_usuarios', '=', True),
                                      ('company_id', '=', empleado.company_id.id)])
        return relojes._encolar(comando, descripcion, empleado)
