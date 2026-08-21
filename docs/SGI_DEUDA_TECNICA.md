# SGI — Deuda técnica y mejoras propuestas

Fuente: análisis exhaustivo del código de `quimibond_sgi` v19.0.14.0.0 y satélites
(agosto 2026, mismo barrido que generó `SGI_DIAGRAMA_FLUJOS.html`). Cada punto está
verificado contra el código; se indica archivo y, cuando aplica, el método.

Prioridad: 🔴 puede morder en operación · 🟡 inconsistencia/asimetría · 🟢 cosmético o decisión a documentar.

---

## A. Quick wins (bajo riesgo, alto retorno)

1. 🔴 **Nota de acuse con "7 días" hardcodeado** — `models/sgi_cron.py` (`cron_documents`, bloque de acuses).
   El umbral real viene de `quimibond_sgi.doc_ack_pending_days`; si MAST lo cambia, el texto de la
   actividad miente. Fix: interpolar el parámetro en la nota.

2. 🔴 **KPI de energía sin proveedor devuelve `0.0`, no `None`** — `models/sgi_indicator.py`
   (`_calc_consumo_energia`). El cron crea la medición como `capturado` con valor 0 (los demás modos
   sin fuente caen a `pendiente`). Un 0 "capturado" puede leerse como consumo cero real.
   Fix: devolver `None` sin proveedor (la nota explicativa ya existe).

3. 🟡 **RxD crea tareas para TODOS los acuerdos** — `models/sgi_management_review.py`
   (`action_mark_done`). La validación exige ≥1 acuerdo con responsable y deadline, pero luego crea
   tarea también para los acuerdos incompletos (tareas sin asignado ni fecha).
   Fix: crear tareas solo de `valid`, o exigir que todos estén completos.

4. 🟡 **`action_reset` de la medición no está en ninguna vista** — `models/sgi_indicator.py`.
   Regresar una medición validada a pendiente solo se puede por ORM. Fix: botón visible solo a
   `group_sgi_manager` (consistente con el candado de evidencia).

5. 🟡 **PPAP `action_reset` no limpia fechas** — `models/sgi_ppap.py`. Un PPAP regresado a
   Preparación conserva `date_submitted`/`date_decision` del ciclo anterior. Fix: limpiarlas al resetear.

6. 🟢 **Smart button "Puntos" del plan de control sin `invisible`** — `views/sgi_control_plan_views.xml`.
   Muestra "0 Puntos" en planes nuevos; el resto de statinfo del módulo se oculta en 0.

7. 🟢 **Botón "NC proveedor" en la OC sin `invisible`** — `views/sgi_map_hooks_views.xml`.
   Sus gemelos en picking/producción se ocultan con conteo 0.

8. 🟢 **Filtro muerto `con_nc`** — `views/sgi_process_views.xml` (dominio `[('id','!=',False)]`,
   `invisible="1"`). Eliminarlo.

9. 🟢 **`finding_count` sin `@api.depends`** — `models/sgi_audit.py`. Funciona por ser no-store,
   pero es frágil ante refactors. Añadir `@api.depends('finding_ids')`.

10. 🟢 **`action_toggle_enabled` de `sgi.alert.source` no cableado** — el toggle se hace con
    `boolean_toggle` sobre el campo. Borrar el método o cablearlo (elegir uno).

11. 🟢 **Etiqueta engañosa en el Pareto de revisado** — `quimibond_sgi_revisado`:
    el group-by "Producto" agrupa por `production_id` (orden de producción), no por producto.

12. 🟢 **README desactualizado** — documenta hasta Fase 4.6; las fases 5–6 (política integral,
    presupuesto/pronóstico P-A28, mini-fase 5.5) solo existen en código y migraciones.

## B. Lógica que puede morder

13. 🔴 **Evaluación de proveedores con métricas congeladas** — `models/sgi_supplier_eval.py`.
    `otd_pct/nc_count/score/supplier_class` son `store=True` con `@api.depends('partner_id','date_from','date_to')`:
    recepciones o NCs registradas *después* de crear la evaluación no la recalculan.
    Fix: botón "Recalcular" que invalide el compute, o quitar el store (el cron trimestral es el
    único volumen real de lectura).

14. 🔴 **El conteo de NCs del proveedor no filtra origen** — `_sgi_count_ncs` cuenta **toda**
    `quality.alert` del partner en el periodo (incluidas internas que casualmente lo referencien),
    penalizando 10 pts cada una. Fix: filtrar por origen/equipo (p. ej. solo NC con folio o de
    origen proveedor).

15. 🔴 **Riesgo controlado no se revalida** — `models/sgi_risk.py`. `_sgi_check_can_close` solo corre
    cuando `state` viene en el `write`; borrar la única acción terminada de un riesgo ya
    `controlado` no dispara nada. Fix: constraint sobre `action_line_ids` cuando el estado sea
    controlado/cerrado y la atención sea alta.

16. 🟡 **El cron de indicadores nunca recalcula mediciones existentes** — `_sgi_generate_measures`
    solo crea la faltante. Cambiar el `calc_mode` de un KPI no re-mide periodos ya generados.
    Documentarlo o añadir un botón "Recalcular valor" en mediciones no validadas.

17. 🟡 **`_check_unique_vigente` (Python) e índice de BD divergen** — `models/sgi_document.py`.
    El índice filtra `sgi_is_controlled IS TRUE`; el constraint Python no, así que un documento
    NO controlado con clave duplicada vigente es rechazado por Python aunque la BD lo permitiría.
    Alinear ambos.

18. 🟡 **`sgi.objective.health` agrega salud del PROCESO, no del indicador** — un objetivo cuyos
    indicadores no tengan `process_id` siempre sale verde. Decidir: o exigir proceso en el
    indicador, o agregar también `last_semaphore`.

19. 🟡 **`sgi.audit.action_close` escribe `program_line.state='creada'`** — no-op semántico
    (la línea no tiene estado "cerrada"). Añadir estado `cerrada` a la línea o quitar la escritura.

20. 🟡 **Revisión documental sin autoincremento** — `sgi_revision` es Char libre (default "00") y el
    bump depende de que quien captura la solicitud llene `sgi_new_revision`. Fix: default calculado
    (revisión vigente + 1) en el onchange de la solicitud.

## C. Endurecimiento de candados ISO/IATF

21. 🔴 **AMEF vigente con acciones sin terminar** (deuda ya declarada en README Fase 3) —
    `action_set_vigente` solo exige que el modo de falla con NPR alto tenga *alguna* acción
    registrada, no terminada. Endurecer: exigir `date_done`.

22. 🟡 **`npr_post` sin validación** — nada obliga a que el NPR re-evaluado baje (contraste con el
    riesgo, donde el residual debe bajar o justificarse). Añadir la misma regla: `npr_post < npr`
    o nota de justificación.

23. 🟡 **AMEF obsoleto no avisa a nadie** — asimetría con el plan de control, que agenda actividad
    al Jefe MAST para revisar puntos huérfanos. Replicar el aviso.

24. 🟡 **Nivel PPAP puramente informativo** — los niveles AIAG 1–5 definen qué se somete, pero
    `_sgi_generate_elements` siempre genera los 18 y las validaciones no lo miran. Mapear
    nivel → elementos requeridos (el resto nace en N/A).

25. 🟡 **AMEF ↔ plan de control sin relación de modelo** — IATF los trata como cadena
    PFMEA → plan de control; hoy solo se enlazan vía elementos PPAP. Añadir M2O/M2M directo
    facilitaría el read-across y los avisos de "actualizar AMEF y plan".

26. 🟢 **Sin `ir.rule` en todo el addon** — decisión consciente (dominios + candados Python), pero
    conviene documentarla y revisitarla si crece la base de usuarios: hoy cualquier Usuario SGI
    ve todos los registros quitando filtros.

## D. Funcionalidad pendiente / oportunidades

27. **Encuesta DNC huérfana** — `data/sgi_dnc_survey.xml` no la referencia ningún modelo, botón ni
    cron; se envía a mano desde Encuestas. Oportunidad: cron anual (o botón en el departamento) que
    la distribuya y una actividad a RH con los resultados, cerrando el ciclo P-A01 junto con la
    vista SQL de brechas.

28. **Acuse propio de la Política** — hoy la difusión con firma depende de ligar el MIID en
    `document_id`. Un botón "Generar acuses" en la política (delegando al documento) evitaría
    publicaciones sin difusión.

29. **Vocabulario doble de niveles de atención** — `ATTENTION_LEVELS` mezcla
    `baja/intermedia/media/inmediata` (RyO/ambiental) con `bajo/medio/alto` (IPER/patrimonial) y
    obliga a repetir pares `('inmediata','alto')` en ≥5 sitios (vistas, filtros, salud del proceso).
    Normalizar a una escala interna única con etiquetas por instrumento.

30. **Doble herencia de `base.view_partner_form`** — `sgi_integration_views.xml` y
    `sgi_res_partner_views.xml` insertan en el mismo `button_box`; funciona pero el orden de los
    smart buttons depende del orden de carga. Consolidar en un archivo.

31. **Vistas Studio inválidas de `quimibond_intelligence`** (deuda transversal del repo, ya anotada
    en `tools/no_bump.txt`): mientras no se limpien, ese módulo no puede subir versión y pinta
    builds en rojo. No es del SGI, pero condiciona los despliegues del repo.

## Sugerencia de ataque

| Lote | Puntos | Esfuerzo | Riesgo |
|---|---|---|---|
| 1. Quick wins | A.1–A.12 | Bajo (1 PR) | Nulo — textos, vistas, computes |
| 2. Proveedores + riesgo | B.13–B.15 | Medio | Bajo — lógica acotada con tests existentes |
| 3. Candados IATF | C.21–C.24 | Medio | Medio — endurece flujos en uso (avisar a MAST) |
| 4. DNC + política + niveles | D.27–D.29 | Medio/alto | Bajo |
