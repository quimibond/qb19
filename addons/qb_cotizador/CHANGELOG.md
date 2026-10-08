# Changelog — qb_cotizador

Una sección por versión del manifest, la más nueva arriba. El PR que sube
`version` en `__manifest__.py` agrega aquí su entrada.

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
