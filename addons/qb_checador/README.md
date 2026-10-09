# qb_checador — Checadores ZKTeco → asistencias de Odoo

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
activos. Un equipo desconocido que se conecte queda registrado **inactivo y
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

**Camino 2 — puente en una PC de la planta.** Si el firmware no puede
empujar por HTTPS, un programa en una PC lee las checadas nuevas de los
equipos por red (protocolo ZK, puerto 4370) y las manda a Odoo por JSON-RPC
con un usuario técnico (grupo *Asistencias / Encargado: gestionar todas las
asistencias*):

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

1. `hr.employee.qb_checador_pin` («Usuario en el checador», ficha del
   empleado, pestaña Ajustes, junto al PIN de asistencias) igual al PIN.
2. Si no, la *Referencia de empleado* igual a `PREFIJO-PIN` para cada prefijo
   del equipo (campo *Prefijos de referencia*, p. ej. `S` para la planta:
   usuario `31` → `S-31`), o igual al PIN. Los ceros a la izquierda se
   ignoran (`0031` → `S-31`).
3. Dos candidatos = nadie: la checada queda **sin empleado**. Mejor eso que
   en la persona equivocada.

La Referencia de empleado la agrega `hr_payroll` (Enterprise); en una base sin
nómina (el CI, por ejemplo) solo aplica la liga por usuario capturado.

## Emparejar (cron cada 5 minutos)

Por persona, en orden de tiempo:

- La primera checada abre una asistencia (`in_mode = checador`), la siguiente
  la cierra. No se usa el estado entrada/salida del reloj: en la planta no se
  selecciona al checar.
- Dos checadas en menos de `qb_checador.minutos_duplicado` minutos (default 2)
  son la misma: la segunda queda **ignorada**.
- Una entrada sin salida en más de `qb_checador.horas_turno_max` horas
  (default 16, para turnos de 12) se cierra **en cero horas** (`out_mode =
  technical`) y su checada queda **huérfana**. Nunca se inventan horas: el
  supervisor corrige la asistencia a mano.
- Una checada que llega tarde y cae dentro de una asistencia ya cerrada queda
  huérfana sin tocar la asistencia.
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
`tests/test_iclock.py` (el protocolo contra el controlador). Corren en el CI
con `--test-tags /qb_checador`.
