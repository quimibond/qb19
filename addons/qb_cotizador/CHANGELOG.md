# Changelog — qb_cotizador

Una sección por versión del manifest, la más nueva arriba. El PR que sube
`version` en `__manifest__.py` agrega aquí su entrada.

## 19.0.1.2.0 — 2026-10-08

Jose 2026-10-08, 5.6: precio en tarifa automático al ganar.

- **Agregado** `tarifa_fecha` / `tarifa_user_id`: cuándo y quién puso el
  precio en la tarifa (se ve en «Resultado»); con eso el SGI mide C1.17.
- **Agregado** gancho `_producto_para_tarifa()`: el puente con el SGI
  (`qb_costeo_sgi`) pone el artículo generado por el desarrollo cuando la
  cotización se hizo antes de que existiera; aquí sigue siendo
  `product_id`.
- **Cambiado** medio de aprobación del cliente: se agrega «Dirección
  (desarrollo interno)», el mismo catálogo que la aprobación del SGI.
- **Migración** `19.0.1.2.0`: rellena `tarifa_fecha` de las cotizaciones que
  ya tenían precio en tarifa con la fecha de alta del renglón de la tarifa.

## 19.0.1.1.1 — 2026-10-08

- **Corregido** la hoja interna de costo (PDF) tronaba con «incomplete
  format»: el cargador de datos de Odoo guarda `%%` como `%` en el arch de la
  plantilla, así que `'%.1f %%' % x` llegaba a la base como `'%.1f %'`. El
  signo de porcentaje va ahora como texto fuera de la expresión (rendimiento
  vendible y operación). Regla: en plantillas QWeb del repo no usar `%%`.

## 19.0.1.1.0 — 2026-10-08

Jose 2026-10-08, puntos 2 y 3.

- **Parámetros sin definir, vacíos**: `seguimiento_dias_habiles` y
  `borrador_archivar_dias` ya no los pone el instalador; sin valor no hay
  seguimiento automático (la cotización se presenta sin fecha de seguimiento)
  ni se archivan borradores. `validez_dias` = 15 se queda.
- **La calculadora viva guarda aquí**: `crear_desde_calculadora(d)` recibe la
  cotización con la forma del cotizador anterior y la crea como borrador con
  folio propio y fuente «cotizador anterior», para que pase por la aprobación
  del puesto. La conversión (energía, fabricación, costo por unidad vendible,
  precio objetivo MXN → moneda) es la misma de la importación
  (`_vals_desde_legado`).
- **Migración** `19.0.1.1.0`: vacía los dos parámetros solo si siguen con el
  valor que puso el instalador (5 y 30).

## 19.0.1.0.0 — 2026-10-08

- Primera versión (spec §6 y §6.1; brief C1 §6.8). `qb.cotizador.cotizacion`
  con foto del costo desde `qb.costo.unitario` del último período cerrado
  (o de un producto hermano, o capturado a mano, siempre con fuente), pisos
  con capacidad ociosa y a planta llena, márgenes y semáforo calculados del
  precio vigente, escalera de volumen por parámetro (vacío: sin escalera),
  costo de la muestra visible antes de aprobar.
- Ciclo del procedimiento C1: Borrador → Por aprobar → Presentada → Ganada /
  Perdida / Vencida (+ Reemplazada por revisión). Aprueba solo el **puesto**
  configurado (por omisión Director de Finanzas y Administración, buscado por
  nombre al instalar) o su suplente (parámetro vacío); en rojo solo el grupo
  «Autoriza precio bajo piso» (decisión 5 del CEO). Regreso a recotizar y
  pérdida con motivo de lista (`qb.cotizador.motivo`), nota opcional.
- Sin aprobación no se imprime el PDF comercial (bilingüe, nueve términos +
  hoja de seguridad, IMDS e ISO, leyenda de muestra). Hoja interna de costo.
- Seguimiento: validez por parámetro (15 días), actividad a Ventas a los N
  días hábiles (5) y al vencer; vencida obliga a decidir (renovar, ganada,
  perdida). Borradores sin movimiento 30 días se archivan (cron diario).
- Aprobación del cliente (medio, fecha, evidencia). Ganada + cliente aprobó
  ⇒ precio en la tarifa del cliente («TARIFA <cliente>», se crea si usa una
  compartida) con precio, moneda y vigencia. Ganada automática al confirmar
  un pedido del mismo cliente y producto.
- Importa las cotizaciones de `qb_capacidad_costeo` (`legacy_id`, mismo
  folio, estado, revisión, foto de costo y escalera) sin modificarlas; las
  presentadas con validez vencida entran como «Vencida».
- Ajustes → Ventas → Cotizador de costo: puestos, margen mínimo (vacío),
  validez, seguimiento, archivo, escalera, tolerancia del recálculo.
- Modelos expuestos por MCP al instalar.
