# Revisión de los checadores ZKTeco para conectarlos a Odoo

Para: Mariano Domínguez (Sistemas TI). De: José Mizrahi. Octubre 2026.

## Para qué

Las horas por día que hoy van en la hoja de incidencias de la planta tienen
que salir del reloj y llegar solas a Odoo. Odoo ya tiene el receptor
(`qb_checador`): los relojes ZKTeco le mandan cada checada por internet y Odoo
arma las asistencias. Antes de conectar nada necesito saber cómo están los
equipos y la red. Esto es lo que te pido revisar, equipo por equipo, y
mandarme con las respuestas.

## Equipos

| Equipo | Modelo | Serie | Ubicación |
|---|---|---|---|
| 1 | ZKTeco MB10-VL (rostro y huella) | UDP3255000661 | Toluca |
| 2 | ZKTeco UA860 (huella) | A3B7194960761 | Toluca |

Si hay un tercero en la oficina de México (RH revisa checadas de los
vendedores de allá), anótalo también con modelo y serie.

## Qué revisar en cada equipo

Entra al menú del reloj con la clave de administrador (si nadie la tiene, dilo:
hay que recuperarla antes que nada).

### 1. Identidad y versión (Menú → Información del sistema / Info del dispositivo)

- Número de serie (debe coincidir con la tabla de arriba).
- Versión de firmware.
- Versión de "Push" o "ADMS" si la muestra.
- Fecha y hora que marca el reloj ahora mismo, contra tu reloj. Si está
  desfasado más de un minuto, anótalo; la nómina se paga con esa hora.

### 2. Red (Menú → Comunicación → Ethernet / Wi-Fi)

- IP, máscara, puerta de enlace y DNS que tiene configurados.
- Si la IP es fija o por DHCP. Para conectarlo a Odoo necesita salir a
  internet: DNS y puerta de enlace válidos.
- Desde una PC de la misma red: `ping` a la IP del reloj y `ping
  quimibond-qb19.odoo.com`. Si el segundo no responde, la planta no sale a
  internet desde esa red o el firewall lo bloquea; con eso vemos qué abrir
  (solo salida TCP 443 hacia `*.odoo.com`).

### 3. Servidor en la nube (Menú → Comunicación → Servidor en la nube / ADMS / Configuración del servidor)

Es la pantalla clave. Anota exactamente qué campos trae:

- ¿Existe la opción? (Si no, el equipo no puede empujar y vamos por el
  camino 2, abajo.)
- Campos que muestra: "Habilitar nombre de dominio", "Dirección del servidor",
  "Puerto", "Habilitar proxy", **"HTTPS"** o "Modo seguro". Lo que más me
  importa es si tiene la opción HTTPS: Odoo solo acepta conexiones cifradas.
- Qué valor tiene hoy (si ya apunta a algún servidor, p. ej. una PC con
  BioTime, anótalo y no lo cambies todavía).

### 4. Usuarios (Menú → Gestión de usuarios)

- Cuántos usuarios tiene registrados cada equipo.
- Qué número de usuario usan: ¿es el número de empleado de NOI (el "No.
  Trab." del recibo) o es otro consecutivo del reloj?
- Exportar la lista de usuarios a USB (Menú → Gestión de datos / USB →
  Exportar usuarios) y mandármela. Es lo que liga cada checada con la ficha
  de Odoo.

### 5. Registros (Menú → Gestión de datos)

- Cuántos registros de asistencia guarda hoy y desde qué fecha.
- Exportar los registros de asistencia de octubre a USB (archivo
  `attlog`/`.dat`/`.txt`) y mandármelos. Con eso comparo el reloj contra la
  hoja de incidencias de las semanas 42 y 43 antes de conectar nada.

### 6. Software que los lee hoy

- Qué programa descarga las checadas (ZKTime, BioTime, ZKAccess u otro), en
  qué PC está instalado y quién lo usa.
- Confirmar con Miguel (RH) si la hoja de incidencias sale de ese reporte.
- Si la PC con ese software se queda encendida siempre: es la candidata para
  el puente si hace falta.

## Qué sigue según lo que encuentres

**Camino 1 (preferido): el reloj empuja solo.** Si el equipo tiene la pantalla
de servidor en la nube con opción HTTPS y la red sale a internet, lo
configuramos así:

| Campo | Valor |
|---|---|
| Habilitar nombre de dominio | Sí |
| Dirección del servidor | `quimibond-qb19.odoo.com` (te confirmo la dirección de prueba antes de tocar producción) |
| Puerto | 443 |
| HTTPS | Sí |
| Proxy | No |

Odoo reconoce al equipo por su número de serie. Ya lo tengo registrado; si se
conecta uno que no está, aparece en Odoo como "sin autorizar" y yo lo apruebo.
Primero lo probamos con la base de pruebas; a producción cuando las checadas
se vean bien.

**Camino 2: puente en una PC.** Si el equipo no sabe HTTPS, yo te paso un
programa pequeño (Python) para la PC que siempre está encendida. Cada 5
minutos lee las checadas nuevas de los dos relojes por la red de la planta
(puerto 4370 del reloj) y las sube a Odoo cifradas. Para eso necesito: la IP
de cada reloj, la **contraseña de comunicación** del reloj si tiene una
(Menú → Comunicación → Seguridad / Comm Key), y que esa PC tenga permiso de
salida a internet por el puerto 443.

## Lo que te pido de vuelta

Una respuesta por equipo con los puntos 1 a 6, más la lista de usuarios y la
exportación de registros de octubre. Si algo no sale (clave de administrador
perdida, menú distinto, sin internet), dímelo tal cual: con eso decido el
camino. No cambies la configuración del servidor en la nube hasta que te
confirme la dirección.
