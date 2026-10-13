# Mis pendientes (SGI): auditoría y rediseño — 2026-10-13

Pedido de Dirección General: «le doy Hecho y no sirve, la sigo viendo ahí;
audita todo eso, dame la mejor propuesta y prográmala con las mejores
prácticas de Odoo 19». Entregado en `quimibond_sgi` 19.0.57.145.0.

## 1. Qué se auditó

- Código: `models/sgi_my_pending.py` (1,245 líneas), `views/sgi_my_pending_views.xml`,
  `models/sgi_activity_execution.py` (registro de cumplimiento y su asistente),
  `models/sgi_cron.py` (avisos con episodio), `models/sgi_archived_filters.py`,
  las 30 pruebas que tocan el modelo.
- Odoo 19 (fuente oficial, rama 19.0): `action_service.js` (cómo se ejecuta
  un botón, cómo se cierra un diálogo y qué recarga), `client_actions.js`
  (`soft_reload`, `display_notification`), `view_button_hook.js` (el
  `onClose` que recarga la lista), `mail_activity.py` (`action_feedback`).
- Producción (MCP, solo lectura): los pendientes de Jose (54, 49 atrasados),
  sus 6 registros de cumplimiento (todos «Pendiente»), sus 38 avisos «NC sin
  acción» (todos abiertos), sus 16 roles y 32 escalamientos, los 194
  registros de cumplimiento de toda la empresa y las actividades archivadas
  del último mes.

## 2. Hallazgos

| # | Hallazgo | Evidencia | Efecto para quien usa la lista |
|---|---|---|---|
| H1 | `sgi.my.pending` era `TransientModel`. El autovacuum diario de Odoo borra los renglones transitorios con más de una hora (`transient_age_limit`, 1 h). | `odoo/orm/models.py`; la lista se arma solo al abrir el menú. | Lista abierta hace más de una hora o en otra pestaña: «Hecho» da «el registro no existe» o no hace nada y el renglón sigue a la vista hasta reabrir el menú. |
| H2 | Los escalamientos y las actividades atrasadas por semáforo (`res_model = sgi.process.activity`) no tenían botón «Hecho»: solo «Ir». Compartían el tipo «Actividad atrasada». | Vista anterior: `action_exec_done` solo si `res_model == 'sgi.activity.execution'`. | De los 54 renglones de Jose, 9 escalamientos y las actividades sin registro del periodo no se podían marcar. |
| H3 | Los botones devolvían `soft_reload` (un cliente de Odoo que vuelve a montar el controlador) y el asistente de «Hecha» devolvía lo mismo desde un diálogo. Funciona en Odoo 19, pero no es el cierre estándar y no da ninguna confirmación visible. | `client_actions.js:softReload` → `restore()` → `_updateUI` → `closeAll({noReload: true})`. | Sin «Hecha: …» en pantalla; si algo falla antes (H1), el usuario no sabe qué pasó. |
| H4 | «Hecha» en una actividad de registro manual exige evidencia con un `UserError` del servidor, no con el campo obligatorio. | `_sgi_mark_done`, vista del asistente sin `required`. | Se llena el asistente, se pulsa Guardar y sale un error; parece que «no sirve». |
| H5 | En producción nadie ha marcado «Hecha» desde Mis pendientes. | 171 registros pendientes, 1 hecho a mano (Blanca Ballesteros), 22 cerrados por el sistema; ningún aviso de Jose cerrado con «Hecho desde Mis pendientes». | La función existía (57.103.0) y no se usaba: no era usable. |
| H6 | 38 de los 54 renglones de Jose son avisos «NC sin acción» de los hallazgos de la auditoría AUD-2026-19 (creados el 2026-10-08), que se resuelven en la NC, no con «Hecho». | `mail.activity` con `sgi_cron_key = nc_sin_accion`, `user_id = Jose`. | La lista no distinguía «esto lo marco yo» de «esto se resuelve en su registro». |

Lo que sí estaba bien: las fuentes (qué es pendiente y de quién, 56.36.0 a
57.107.0), los episodios de los avisos del cron (un aviso hecho **no**
reaparece mientras dure la misma causa) y los candados de identidad de
acuses, mediciones y registro de cumplimiento.

## 3. Propuesta elegida (y las descartadas)

**Elegida: la misma bandeja, con renglones guardados y un «Hecho» por tipo.**

1. `sgi.my.pending` pasa a `Model`: una fila por persona y registro
   pendiente, actualizada **en su lugar** al abrir la lista y cada noche
   (`_sgi_store`). Ids estables; lo resuelto se quita en el servidor.
2. `action_done` decide por tipo: registro de cumplimiento → asistente;
   actividad atrasada que lleva registro → **crea el registro del periodo en
   curso al vuelo** y abre el asistente; aviso → hecho en Odoo; acción de NC
   → terminada; acuse → firmado; medición → validada. Lo que se resuelve en
   su registro no tiene «Hecho»: tiene «Ir» y la ficha lo dice.
3. Tipo **Escalamiento** aparte (solo «Ir»).
4. **Ficha del pendiente** (form): qué hay que hacer, sobre qué registro,
   cuándo vence, avance y todos los botones (Hecho, En proceso, No aplica
   este periodo, Validar, Leído y entendido, Posponer una semana, Ir).
5. Recarga estándar de Odoo 19: cada botón devuelve
   `display_notification` + `next: act_window_close`; la vista recarga por
   su `onClose`. Sin `soft_reload`. «Vence» con `remaining_days`.
6. Evidencia obligatoria en el campo del asistente, además del servidor.

**Descartada: espejar todo en `mail.activity` (actividades nativas).**
`qb_obligation` ya lo intentó (3.2.0 apagó el espejo): los renglones
derivados (atraso por semáforo, medición por validar, acuse) cambian solos y
el espejo se desfasa; además las actividades nativas no saben de evidencia ni
de periodos. Los avisos del cron sí siguen siendo actividades nativas y se
cierran como tales.

**Descartada: un tablero OWL a la medida.** Más código JS propio que
mantener; la lista + kanban + form nativas de Odoo 19 cubren el celular, la
tableta y el escritorio con lo que ya se conoce.

## 4. Lo que no cambia

- Las fuentes y plazos (quién tiene qué pendiente, días hábiles).
- Los episodios de los avisos del cron.
- `Mi equipo` (semáforo por persona), el correo semanal de atrasados y el
  resumen guardado (`sgi_pending_saved_*`).
- Los menús (`tools/sgi_menu_tree.txt` sin cambios).

## 5. Cómo verificar en producción (después del `odoo-update quimibond_sgi`)

1. SGI → Inicio → Mis pendientes: abre con «Atrasadas o por vencer»
   agrupado por Estado; «Vence» dice «Hace N días».
2. Tocar un renglón abre su ficha con «Qué hay que hacer».
3. En una actividad atrasada de registro manual, **Hecho** abre el
   asistente; al guardar con una nota, Odoo muestra «Hecha: …» y el renglón
   desaparece **sin** reabrir el menú. En Registro de cumplimiento aparece
   «Hecha» con la evidencia.
4. En un aviso propio, **Hecho** lo archiva en Odoo (chatter del registro) y
   el renglón desaparece; **Posponer una semana** mueve la fecha límite.
5. Al día siguiente el aviso hecho **no** vuelve (episodio) y la lista no
   pierde renglones aunque lleve horas abierta.
6. Consulta de control: `select count(*) from sgi_my_pending` crece con el
   respaldo nocturno (una fila por persona y pendiente) y no con cada apertura.

## 6. Pendientes que quedan fuera (decisión de Dirección)

- Aprobar o rechazar una solicitud de Aprobaciones desde el renglón (hoy
  «Ir» a Aprobaciones): la API de Enterprise no se puede verificar desde el
  repo; se puede agregar cuando se confirme en producción con `get_fields`.
- Los 38 avisos «NC sin acción» de la auditoría: son NC sin plan de acción;
  el aviso se cierra solo cuando la NC tenga acción. Si Dirección no es
  quien debe recibirlos, el cron `nc_sin_accion` debe apuntar al dueño del
  proceso de cada NC.
