# quimibond_tintoreria_programacion

Programación de Tintorería (Odoo SH 19) — diseño técnico v5, §17.

## Qué hace
1. Las MOs de Producto A (kg, código con **K en la posición 9**) llegan por MPS/MTO por el total; **sin** calcular tamaño de baño.
2. **Manufactura → Programar Tintorería** (o el botón en la MO / acción de lista): Tintorería asigna el HTJ de cada orden.
3. Al confirmar:
   - capacidad por baño = `tintoreria.capacidad.rendimiento.capacidad_para_rendimiento(rendimiento de la ficha de acabado del producto B)`;
   - `n = ceil(kg / capacidad)`; reparto **simétrico** `kg / n` (el redondeo cae en el último baño), todos en el mismo HTJ;
   - si `n > 1` se usa la división nativa `mrp.production._split_productions` (conserva los enlaces MTO hacia la MO de B);
   - se asigna el HTJ a la workorder de Tintorería de cada MO → `quimibond_tintoreria_dosificacion` recalcula g/L y % con la RB del HTJ;
   - se crea un registro `tintoreria.programacion` con el detalle.
4. **Validación:** sin programar, `mrp.workorder.button_start`, `mrp.production.action_start` y `button_mark_done` bloquean la orden.

## Campos nuevos en `mrp.production`
`tintoreria_programada`, `tintoreria_programacion_id`, `requiere_programacion_tintoreria` (K en pos. 9 + operación de Tintorería), `tintoreria_pendiente`.

## Supuestos a validar en pruebas
- Producto B se obtiene por el enlace MTO (`move_dest_ids`) y, si no existe aún, derivando el código (posición 9: K → J).
- Las bandas de rendimiento son las de `capacidad_para_rendimiento`: **los rendimientos entre 6 y 7 o entre 10 y 11 no tienen banda** (p. ej. 6.5) y la programación se bloquea con un mensaje claro.

## Pendiente (siguientes fases)
- Reprogramación con merge (antes de iniciar), tablas de tiempos por centro de trabajo (`duration_expected`).

## Pruebas
```
odoo-bin -d <base_de_pruebas> -i quimibond_tintoreria_programacion --test-tags /quimibond_tintoreria_programacion --stop-after-init
```
