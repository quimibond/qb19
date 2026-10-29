# qb_checador — Checadores ZKTeco y Hikvision → asistencias de Odoo

Los relojes checadores de la planta de Toluca (ZKTeco **MB10-VL**, serie
UDP3255000661, y **UA860**, serie A3B7194960761) entregan sus checadas a Odoo
y un cron las convierte en asistencias (`hr.attendance`). Es la línea
«Implementar módulo de asistencia en Odoo» del plan de mejora continua de RH
(F-P-A10-02, agosto 2026). Objetivo: que las horas por día de la nómina salgan
del reloj y no de la hoja de incidencias.

## Cómo llegan las checadas

**Camino 1 — el reloj empuja (ADMS / PUSH).** En el menú del equipo,
*Comunicación → Servidor en la nube (ADMS)* se apunta a Odoo:

| Campo | Valor |
|---|---|
| Habilitar nombre de dominio | Sí |
| Dirección del servidor | `quimibond-qb19.odoo.com` (producción) o la URL del build de la rama |
| Puerto | 443 |
| HTTPS | Sí (si el firmware tiene la opción) |
| Proxy | No |

El reloj se identifica por su **número de serie**. Solo contestan los equipos
registrados en *Asistencias → Checadores → Relojes* y marcados autorizados y
activos. Odoo no puede «probar» la conexión hacia el reloj (es el reloj el
que llama); lo que sí muestra la lista y la ficha es un **semáforo** a partir
de la última vez que el reloj se presentó: *Conectado* (hace menos de
2 minutos; un reloj conectado se presenta cada pocos segundos), *Sin
conexión* (se presentó antes y lleva más de 2 minutos callado) o *Nunca
conectado* (red, firewall o servidor en la nube sin configurar), con el
«hace X min» al lado. Un equipo desconocido que se conecte queda registrado **inactivo y
sin autorizar** con su serie, para que TI lo revise; recibe 403 y sus
checadas no se guardan (parámetro `qb_checador.auto_registrar`; `0` lo apaga).

Protocolo (lo que el equipo manda, lo que Odoo contesta):

| Petición | Respuesta |
|---|---|
| `GET /iclock/cdata?SN=…&options=all&pushver=…` | Opciones: `ATTLOGStamp` (desde qué marca mandar), `Realtime=1`, `TimeZone` de la zona del equipo, `TransFlag` según versión PUSH |
| `POST /iclock/cdata?SN=…&table=ATTLOG&Stamp=…` con una checada por línea `PIN<TAB>AAAA-MM-DD HH:MM:SS<TAB>STATUS<TAB>VERIFY<TAB>WORKCODE…` | `OK: n` (n = líneas recibidas). La marca se guarda y se le devuelve en la siguiente conexión, así solo manda lo nuevo |
| `POST /iclock/cdata?SN=…&table=options` | `OK`; se guarda `FWVersion` |
| `POST …table=OPERLOG|ATTPHOTO|BIODATA|…` | `OK` (se descarta) |
| `GET /iclock/getrequest?SN=…&INFO=…` | `OK` (no se mandan comandos al equipo); de `INFO` se toma firmware e IP |
| `POST /iclock/devicecmd`, `/iclock/ping`, `/iclock/registry`, `/iclock/push` | `OK` |

**Qué reloj va por cuál camino (2026-10-12).** El **MB10-VL** trae el
interruptor HTTPS en «Cloud Server Setting» (manual de usuario ZKTeco
MB10-VL v1.2, abril 2022) y va por el camino 1, directo a Odoo. El **UA860**
no lo trae (foto de la pantalla, firmware de la generación 2015) y Odoo solo
acepta conexiones cifradas: para él, o se cambia por un equipo con HTTPS, o
va por el camino 2.

**Camino 2 — puente para el reloj sin HTTPS.** `puente/puente_adms.py` corre en una PC de la
planta (o en un servidor chico en internet): el reloj le empuja en HTTP plano
(puerto 8081) y el puente reenvía la misma petición a Odoo por HTTPS y
devuelve la respuesta de Odoo al reloj. Para Odoo es idéntico al camino 1
(mismo controlador, misma serie, mismo `OK: n`); si Odoo no contesta, el
puente responde 502 y el reloj guarda y reintenta. Instalación, configuración
del reloj y diagnóstico: `puente/README.md`. Solo Python 3, sin librerías.

**Camino 4 — Hikvision (ISAPI, «Escucha HTTP»).** Los terminales de
asistencia de Hikvision (DS-K1T…, p. ej. el **DS-K1T804AEF** de la oficina de
Ciruelos, CDMX, desde 2026-10-13) no hablan ADMS. En su ficha de Odoo la
*Marca* es «Hikvision (ISAPI)» y al guardarla se genera un **token**; la ficha
muestra el servidor, la ruta y la URL completa
(`https://<odoo>/qb_checador/hikvision/<token>`). En el reloj se da de alta un
servidor de escucha HTTP (`/ISAPI/Event/notification/httpHosts`) con esos
datos, y el reloj manda cada evento en el momento en que ocurre (JSON, XML o
multipart con foto; la foto se descarta).

- Solo cuentan los eventos de control de acceso (`AccessControllerEvent`,
  `majorEventType` 5) que traen número de empleado: verificaciones en las
  que el reloj reconoció a la persona. Huella no reconocida, puerta, botón de
  salida y alarmas se contestan con OK y no se guardan. Un código que no deba
  contar se excluye con `qb_checador.hikvision_eventos_excluidos`
  (`subEventType` separados por coma). En la checada quedan el código del
  evento (*Código de trabajo*), el modo de verificación y el estado de
  asistencia que marcó el reloj.
- La hora es la de la pantalla del reloj convertida con la zona del equipo
  en Odoo (se ignora la zona que traiga el reloj: de fábrica suele ser GMT+08:00).
- El número de empleado del reloj se liga igual que el PIN de los ZKTeco
  (usuario capturado o *Referencia* con prefijo).
- **Si el reloj numera a su gente distinto que la nómina** (Ciruelos: Irma
  es el 1 en el reloj y C-26 en nómina), la ficha del reloj trae la tabla
  **Usuarios del reloj**: número del reloj → empleado. Vale solo para ese
  reloj (el 3 de Ciruelos no es el 3 de Toluca) y gana sobre las otras
  ligas; al capturar una fila, las checadas de ese número que habían
  quedado sin empleado se ligan solas. Desde 1.5.0. En ese reloj conviene
  dejar vacío *Prefijos de referencia*, para que un número sin fila no caiga
  en la referencia de otra persona (el 12 de Ciruelos no es C-12).
- **Hikvision no reintenta**: si Odoo no contesta, el evento queda solo en
  la memoria del reloj (100,000 eventos) y se recupera exportándolo. El
  semáforo de un Hikvision tolera 24 horas sin noticias (solo habla cuando
  alguien checa o con la prueba de su escucha HTTP).
- Un token desconocido recibe 403; «Generar URL nueva» cambia el token y la
  URL anterior deja de funcionar.
- **Si el reloj no manda sus eventos** (el DS-K1T804AEF de Ciruelos acepta
  el servidor de escucha pero rechaza la suscripción con
  `MessageParametersLack` y no manda nada; 2026-10-14), el puente se los
  **pide**: una sección `[hikvision <nombre>]` en `puente_adms.ini` con IP,
  usuario, contraseña del reloj y token. Cada minuto consulta el historial
  (`POST /ISAPI/AccessControl/AcsEvent?format=json`, `major` 5, páginas de
  10) y manda a la URL de la ficha, uno por uno, los eventos con número de
  serie mayor al último mandado (`puente_hikvision.json`). Para Odoo es igual
  que el push. Si el internet o Odoo se caen, el reloj los sigue guardando y
  el puente se pone al día solo. La contraseña vive solo en el `.ini` de la
  PC; con una contraseña mala el puente espera 30 minutos antes de volver a
  intentar, para no bloquear el reloj. Prueba de una vuelta:
  `python puente_adms.py --consultar`.

Configurar la escucha HTTP en el reloj (TI, desde una PC en la misma red):

1. En el reloj: red con DHCP (o IP fija con puerta de enlace y DNS
   correctos), zona horaria GMT-06:00 y la hora en pantalla correcta.
2. En el navegador, `http://<IP del reloj>` con la cuenta de administrador
   del equipo. En *Configuración → Red → Servicio de red* (o *Avanzada*),
   **Escucha HTTP / HTTP Listening**: dirección `quimibond-qb19.odoo.com`
   (dominio, no IP), puerto `443`, protocolo **HTTPS**, URL = la *Ruta* de
   la ficha en Odoo. Guardar y usar **Prueba**: en Odoo la ficha pasa a
   «Conectado».
3. Si el reloj no tiene página web, la misma configuración por ISAPI con el
   `curl` de Windows (sustituir IP, usuario y contraseña del reloj, y el token):

   ```
   curl --digest -u USUARIO:CONTRASEÑA http://IP/ISAPI/Event/notification/httpHosts/capabilities
   curl --digest -u USUARIO:CONTRASEÑA -X PUT -H "Content-Type: application/xml" --data-binary @escucha.xml http://IP/ISAPI/Event/notification/httpHosts
   curl --digest -u USUARIO:CONTRASEÑA -X POST http://IP/ISAPI/Event/notification/httpHosts/1/test
   ```

   con `escucha.xml`:

   ```xml
   <HttpHostNotificationList version="2.0" xmlns="http://www.isapi.org/ver20/XMLSchema">
     <HttpHostNotification>
       <id>1</id>
       <url>/qb_checador/hikvision/TOKEN</url>
       <protocolType>HTTPS</protocolType>
       <parameterFormatType>JSON</parameterFormatType>
       <addressingFormatType>hostname</addressingFormatType>
       <hostName>quimibond-qb19.odoo.com</hostName>
       <portNo>443</portNo>
       <httpAuthenticationMethod>none</httpAuthenticationMethod>
     </HttpHostNotification>
   </HttpHostNotificationList>
   ```

   La primera llamada dice si el reloj acepta `HTTPS` en `protocolType` y
   `hostname` en `addressingFormatType`.
4. **Si no acepta HTTPS**: el reloj apunta en HTTP plano al puente (camino 2)
   en una PC de esa oficina: dirección = IP de la PC, puerto `8081`,
   protocolo HTTP, misma ruta. El puente reenvía `/qb_checador/hikvision/…`
   igual que `/iclock/…`.

Las credenciales del reloj no se guardan en Odoo ni en el repositorio.

**Camino 3 — API.** Para un equipo que no sepa ADMS, un programa puede mandar
las checadas por JSON-RPC con un usuario técnico (grupo *Asistencias /
Encargado: gestionar todas las asistencias*):

```python
models.execute_kw(db, uid, pwd, 'qb.checada', 'ingresar_api', ['UDP3255000661', [
    {'pin': '31', 'time': '2026-10-05 06:58:10', 'status': 0, 'verify': 1},
]])
# → {'creadas': 1, 'repetidas': 0}
```

La hora va en la zona del equipo. Las repetidas (mismo equipo, usuario, hora)
se ignoran, así que el puente puede reenviar sin miedo.

## Quién es quién

El reloj manda un número de usuario (PIN). Se liga al empleado así:

1. La tabla **Usuarios del reloj** de la ficha del reloj, si la fila tiene
   empleado (solo vale para ese reloj).
2. `hr.employee.qb_checador_pin` («Usuario en el checador», ficha del
   empleado, pestaña Ajustes, junto al PIN de asistencias) igual al PIN.
3. En un reloj que **sincroniza usuarios**, el número de checador de 4 dígitos
   aunque el empleado todavía no lo tenga capturado (`1004` → `S-4`).
4. Si no, la *Referencia de empleado* igual a `PREFIJO-PIN` para cada prefijo
   del equipo (campo *Prefijos de referencia*, p. ej. `S` para la planta:
   usuario `31` → `S-31`), o igual al PIN. Los ceros a la izquierda se
   ignoran (`0031` → `S-31`).
5. Dos candidatos = nadie: la checada queda **sin empleado**. Mejor eso que
   en la persona equivocada.

La Referencia de empleado la agrega `hr_payroll` (Enterprise); en una base sin
nómina (el CI, por ejemplo) solo aplica la liga por usuario capturado.

**Número de checador (desde 1.6.0): un dígito por nómina y el número de
nómina a tres cifras.** `S-4` → `1004`, `Q-109` → `2109`, `C-26` → `3026`
(parámetro `qb_checador.digito_por_nomina`, default `S:1,Q:2,C:3`). El número
de nómina solo no sirve: 21 números se repiten entre nóminas (S-4 y Q-4 son
dos personas; 2026-10-19). Es el mismo en todos los relojes. Odoo lo pone en
`qb_checador_pin` al sincronizar y lo vuelve a calcular si cambia la
Referencia (cambio de nómina): borra el número viejo de los relojes y da de
alta el nuevo con sus huellas, sin que la persona vuelva a registrarse.

## Usuarios y huellas entre Odoo y los relojes ZKTeco (1.6.0)

Para que cada persona registre su huella **una sola vez**, en cualquier reloj,
y quede en todos:

- **Del reloj a Odoo.** Al dar de alta a alguien o registrar una huella, el
  reloj lo manda por ADMS (`OPERLOG`: `USER PIN=…`, `FP PIN=… FID=… TMP=…`;
  firmware nuevo: `BIODATA … Type=1 Tmp=…`). Odoo guarda una fila por usuario
  en *Usuarios del reloj* (origen «Lo mandó el reloj») y la plantilla de cada
  dedo en **Checadores → Huellas** (`qb.checador.huella`). La plantilla es lo
  que guarda el reloj para reconocer el dedo, no una imagen; aun así es un
  dato biométrico: solo la ve el grupo Administrador de asistencias.
- **De Odoo al reloj.** Una cola de comandos por reloj (**Checadores →
  Comandos a relojes**): el reloj la recoge en `/iclock/getrequest` cada pocos
  segundos (`C:<id>:DATA UPDATE USERINFO PIN=…`, `DATA UPDATE FINGERTMP …` o
  `BIODATA`, `DATA DELETE USERINFO …`) y contesta en `/iclock/devicecmd`. Sin
  respuesta en 10 minutos se reenvía, hasta 3 veces; luego queda en error con
  botón «Reintentar».
- **Relojes que sincronizan** (casilla *Sincronizar usuarios* en la ficha):
  reciben a los empleados activos cuya Referencia lleva uno de sus prefijos
  (botón **Sincronizar usuarios**: número, nombre y las huellas que tenga Odoo
  de cada dedo, la más reciente), y lo que se registre en uno pasa solo a los
  demás. Al archivar a un empleado se borra de los relojes.
- **Automático desde 1.6.1:** un ingreso nuevo con Referencia (`S-…`, `Q-…`)
  recibe su número y entra solo a los relojes que sincronizan; lo mismo al
  cambiarle la Referencia o al reactivarlo. La cola respeta el orden: si a
  alguien se le da de baja y se reactiva antes de que el reloj recoja nada,
  quedan alta, baja y alta, y el último manda.
- **Desde 1.6.2 el puente manda a Odoo los POST de `/iclock/` como texto plano.**
  El UA860 (PUSH 2.2.14) los declara como formulario web
  (`application/x-www-form-urlencoded`); Odoo los lee como formulario y el
  cuerpo le llega vacío: contestaba «OK: 0» y el reloj reenviaba el mismo
  bloque cada 5 segundos sin que entrara nada (2026-10-20). Odoo deja un aviso
  en el log cuando le llega un POST vacío con bytes declarados.
- **«Checadas desde» (1.6.2).** Al conectar un reloj por primera vez, manda su
  historial completo (el UA860 mandó checadas de 2024 y 2025 el 20-oct-2026).
  Las checadas anteriores a la fecha «Checadas desde» de su ficha se guardan
  como «Ignorada» y nunca se convierten en asistencias, ni al ligar el usuario
  ni con «Reintentar». Al cambiar la fecha, Odoo ignora las anteriores y borra
  las asistencias que el checador había abierto con ellas. Un reloj que se
  conecta solo queda con la fecha de ese momento; uno dado de alta a mano, sin
  corte hasta que se capture (la migración le puso a cada reloj existente su
  fecha de alta).
- **Traer lo que ya tiene un reloj** (el UA860 viejo): botón **Pedir usuarios y
  huellas** (marca OPERLOG en 0; un reloj nuevo la tiene en 0 desde el
  principio). Sus usuarios llegan con su propia numeración: si el número no es
  el de checador de nadie, Odoo **sugiere un empleado por nombre** (parecido
  ≥ 0.66 y sin empate) y RH lo revisa en la tabla de la ficha (botón ✓ por
  fila o **Aceptar sugerencias**). Con el empleado puesto, sus huellas se
  mandan a los relojes que sincronizan con el número nuevo.
- **Formato.** Se manda en el formato en que el reloj manda las suyas
  (FINGERTMP por default; BIODATA si el reloj usa BIODATA). Huellas de un
  ZKTeco viejo en uno nuevo: **probar con una persona antes de mandar a
  todos** (mismo algoritmo ZKFinger 10 normalmente, no garantizado).
- **Opciones del reloj.** Los ZKTeco con firmware viejo reciben
  `TransFlag=1101111000` (checadas, bitácora, altas y cambios de usuario y de
  huella; sin fotos). Antes era `1111000000` y no mandaban huellas.

## Relojes donde checa cada persona (1.8.0)

Sin asignación, cada reloj que sincroniza recibe a todos los empleados de sus
prefijos y la huella registrada en uno pasa a todos. Con el campo **«Relojes
donde checa»** de la ficha del empleado (junto a «Usuario en el checador»):

- Odoo da de alta a la persona y le manda sus huellas **solo** a esos relojes;
  en los demás no existe, así que el reloj no la reconoce y no registra nada.
- Al asignarle relojes se borra de los que ya no le tocan («Borrar usuario …
  (no checa en este reloj)» en la cola) y se da de alta, con sus huellas, en
  los nuevos. Al vaciar el campo vuelve a todos.
- Una huella que registra en uno de sus relojes pasa solo a sus otros relojes
  asignados. «Sincronizar usuarios» respeta la asignación.
- Vacío = todos los relojes que sincronizan su nómina (lo de antes de 1.8.0).
  Así nadie se queda sin checar mientras se llena la lista.

La lista de RH (F «Listado de personal por checador 2026», Miguel Medina,
29-oct-2026) da un reloj por persona: ACABADO = Rama, ENTRETELAS = Entretelas,
ADMINISTRACIÓN = el MB10-VL de oficinas. El Hikvision de Ciruelos y el UA860
no sincronizan y no se ven afectados.

## Emparejar (cron cada 5 minutos)

Por persona, en orden de tiempo:

- **Una asistencia por turno** (desde 1.7.0). La primera checada abre la
  asistencia (`in_mode = checador`); las siguientes a menos de
  `qb_checador.horas_entre_turnos` horas (default 8) de la anterior son el
  mismo turno (salida a comer, regreso, salida) y mueven la salida. La comida
  queda dentro de la jornada, como en el contrato. Una checada sola acepta su
  salida hasta `horas_turno_max` horas después (el turno de 12 de noche sin
  checar a la comida). No se usa el estado entrada/salida del reloj: en la
  planta no se selecciona al checar.
- Hasta 1.6.x se alternaba entrada/salida: con una checada de más o de menos
  todo se corría un lugar (el 20-oct-2026 el reloj empezó a contar a media
  tarde y la salida de las 17:35 quedó como entrada de una «asistencia» de
  13.5 h hasta las 07:03 del día siguiente). La migración de 1.7.0 rehízo con
  la regla nueva las asistencias que había armado el checador
  (`qb.checada._reconstruir_asistencias()`; las capturadas a mano no se
  tocan). Lo mismo se puede volver a correr si hiciera falta.
- Dos checadas en menos de `qb_checador.minutos_duplicado` minutos (default 2)
  son la misma: la segunda queda **ignorada**.
- Una entrada sin salida en más de `qb_checador.horas_turno_max` horas
  (default 16, para turnos de 12) se cierra **en cero horas** (`out_mode =
  technical`) y su checada queda **huérfana**. Nunca se inventan horas: el
  supervisor corrige la asistencia a mano.
- Una checada que llega tarde y cae dentro de un turno ya armado (la de la
  comida) queda emparejada a ese turno sin cambiarlo; si cae dentro de una
  asistencia capturada a mano, queda huérfana.
- Un error de validación de Odoo al emparejar a una persona no detiene a las
  demás: sus checadas quedan en **error** con el motivo.

Lo pendiente se ve en *Asistencias → Checadores → Checadas*, filtro «Por
resolver». El botón «Volver a emparejar» vuelve a buscar el empleado y deja la
checada lista para el siguiente cron.

## Mientras no haya reloj en Odoo

La corrida piloto de nómina se sigue armando desde la hoja de incidencias de
RH. Este módulo no toca la nómina: solo asistencias. La comparación de las
horas del reloj contra la hoja de RH (semanas 42 y 43) es lo que decide cuándo
la nómina lee las asistencias.

El cron de Odoo «Asistencia: Detectar empleados ausentes» crea asistencias
técnicas de un segundo para todo el que no checó; mientras los relojes no
estén conectados conviene tenerlo apagado.

## Revisión de los equipos

La lista de lo que TI revisa en cada reloj antes de conectarlo está en
`docs/checadores/REVISION_CHECADORES_ZKTECO.md`.

## Pruebas

`tests/test_checadas.py` (ingreso, ligas, emparejamiento, API) y
`tests/test_iclock.py` (el protocolo contra el controlador) y
`tests/test_hikvision.py` (lectura de JSON/XML/multipart, qué cuenta como
checada, token y semáforo del Hikvision). Corren en el CI con
`--test-tags /qb_checador`.
