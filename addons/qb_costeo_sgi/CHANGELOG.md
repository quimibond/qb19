# Changelog — qb_costeo_sgi

## 19.0.1.2.0 — 2026-10-08

Jose 2026-10-08, puntos 1 y 3.

- **La liga de cotizaciones importadas no cambia valores de origen**: la
  regla a mano 121 → proyecto 491 ya no escribe la galga (1.0.0 puso «21»
  sobre el «18» de origen; se regresó a «18» por MCP el mismo día).
- **C1.06 se mide con la cotización nueva presentada** (`presented_date`,
  `presented_by_id`; estados presentada, vencida, ganada y perdida): la ficha
  que aún se medía con «Registro en Odoo» sobre `qb.cotizacion` pasa al
  entregable C1-COTIZACION. C1-COSTO y C1-COTIZACION dicen cuándo están
  completos (precio al cliente; vigencia).
- **Migración** `19.0.1.2.0`: vuelve a apuntar los entregables.

## 19.0.1.1.0 — 2026-10-08

- **El candado del puesto 183 es una aprobación de Odoo** (Jose 2026-10-08,
  punto 2): «Enviar a aprobación» crea una solicitud en Aprobaciones en la
  categoría y el asunto del rol «Aprueba» de C1.05 (hoy «Autorizaciones de
  Dirección»; el asunto «Cotización de desarrollo» se crea al instalar). La
  cotización se presenta cuando la solicitud se aprueba ahí y vuelve a
  borrador (motivo «Otro» con la nota) si se rechaza o cancela; mientras la
  solicitud está viva no se aprueba desde la cotización. Sin rol de C1.05
  como solicitud, aprueba el puesto desde la cotización como en 1.0.0.
- Con eso C1.05 se mide por esa aprobación: por el SGI (solicitudes del
  asunto) y por C1-COSTO (cotización aprobada).

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
