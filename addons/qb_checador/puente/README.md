# Puente ADMS → Odoo (relojes sin HTTPS)

Los relojes ZKTeco de la planta (UA860 y MB10-VL) traen la pantalla
«Servidor en la nube» con modo ADMS, pero **sin opción HTTPS**. Odoo solo
acepta conexiones cifradas, así que el reloj no puede hablarle directo.

El puente es un programa de Python que corre en una PC de la planta que
siempre está encendida. Los relojes le mandan sus checadas en HTTP plano por
la red local y el puente las reenvía tal cual a Odoo por HTTPS (salvo el tipo
de contenido: los POST de `/iclock/` van siempre como texto plano, porque el
UA860 los declara como formulario y Odoo los recibiría vacíos); la respuesta
de Odoo regresa al reloj sin cambios. Para Odoo es exactamente lo mismo que si
el reloj hablara directo (`/iclock/cdata`, serie en la URL, `OK: n`).

```
reloj ──HTTP, red local, puerto 8081──► PC con el puente ──HTTPS──► quimibond-qb19.odoo.com
```

Si Odoo no contesta (sin internet, mantenimiento) el puente responde 502; el
reloj guarda sus registros y vuelve a intentar solo, así que no se pierde
nada.

## Instalación en la PC (una vez)

1. Instalar Python 3 desde <https://www.python.org/downloads/windows/>
   marcando **«Add python.exe to PATH»**. No hace falta ninguna librería.
2. Crear la carpeta `C:\qb_puente` y copiar ahí los tres archivos:
   `puente_adms.py`, `puente_adms.ini`, `instalar_tarea.bat`.
3. Ponerle **IP fija** a la PC (o reservarla en el router): los relojes la
   van a buscar por esa IP.
4. Probar que la PC llega a Odoo. En una ventana de comandos, dentro de
   `C:\qb_puente`:

   ```
   python puente_adms.py --probar
   ```

   Debe decir «Correcto: la PC llega a Odoo». Si dice «NO llega a Odoo», es
   internet, DNS o el firewall de salida por el puerto 443.
5. Clic derecho en `instalar_tarea.bat` → **Ejecutar como administrador**.
   Crea la tarea «QB Puente ADMS» que arranca con Windows (sin iniciar
   sesión), abre el puerto 8081 en el firewall de Windows y arranca el puente.
6. Abrir en el navegador de la PC <http://localhost:8081/>. Debe mostrar
   «Puente ADMS → Odoo» con cero relojes vistos.

## Configurar cada reloj

Menú → Comunicación → Servidor en la nube (Conf. Srvr. de Nube):

| Campo | Valor |
|---|---|
| Modo de servidor | ADMS |
| Habilitar nombre de dominio | OFF |
| Dirección del servidor | la IP fija de la PC del puente (p. ej. `192.168.1.50`) |
| Puerto del servidor | 8081 |
| Habilitar servidor proxy | OFF |

Guardar y, si lo pide, reiniciar el reloj. Un reloj a la vez: primero el
UA860, se confirma que entran checadas en Odoo, luego el MB10-VL.

## Comprobar

- En la PC, <http://localhost:8081/> lista cada reloj con la hora de su
  última petición y la respuesta de Odoo (`200` bien, `403` serie no
  registrada, `502` Odoo no contestó).
- La bitácora `puente_adms.log` (misma carpeta) tiene una línea por petición.
- En Odoo, *Asistencias → Checadores → Relojes*: «Última conexión» y
  «Firmware» se llenan; *Checadas* muestra lo que llega.

En la primera conexión el reloj manda todo lo que guarda (puede ser todo
octubre). Es normal.

## Si algo falla

| Síntoma | Causa | Qué hacer |
|---|---|---|
| El estado dice «ninguno todavía» | El reloj no llega a la PC | Ping del reloj a la PC; revisar IP/puerto en el reloj y que la PC tenga IP fija; firewall de Windows (el .bat abre el 8081) |
| Reloj aparece con `502` | La PC no llega a Odoo | `python puente_adms.py --probar`; internet/DNS/firewall 443 |
| Reloj aparece con `403` | La serie no está registrada o está sin autorizar en Odoo | Avisar a José con la serie que muestra el estado |
| Hay peticiones `200` pero en Odoo no entran checadas | El reloj cree que ya mandó todo | Botón «Reenviar todo» en la ficha del reloj en Odoo |
| La tarea no arranca con Windows | `pythonw.exe` no está en el PATH del sistema | Reinstalar Python marcando «Add to PATH» para todos los usuarios y volver a correr el .bat |

Para detener el puente: `schtasks /End /TN "QB Puente ADMS"`; para quitarlo,
`schtasks /Delete /TN "QB Puente ADMS" /F`.
