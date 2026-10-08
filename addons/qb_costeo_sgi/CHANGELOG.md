# Changelog — qb_costeo_sgi

## 19.0.1.0.0 — 2026-10-08

- Primera versión (spec §6.1 y §8.4, parte de cotización). `project_id` en la
  cotización (solo desarrollos de producto) con cliente, artículo, gramaje,
  ancho y galga de la tabla de características y volumen / precio objetivo de
  la solicitud; botón «Tomar datos del proyecto»; `project_revision`.
- Proyecto: botón inteligente «Cotizaciones»; al subir de revisión recalcula
  la cotización viva y, si el piso lleno cambia más que la tolerancia
  (parámetro, 0.5 %), la regresa a «Por aprobar» y detiene el proyecto
  (`qb_costo_bloqueado`: no cambia de etapa) hasta que se apruebe.
- Fichas C1-COSTO y C1-COTIZACION medidas con la cotización (aprobada /
  presentada, con fecha y persona); la actividad que solo entrega ese
  entregable pasa a medirse por él.
- Liga las cotizaciones importadas con su proyecto FT (artículo del proyecto,
  código en el nombre, y la lista a mano: 121 → proyecto 491 con galga 21).
