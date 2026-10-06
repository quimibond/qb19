# Changelog — qb_costeo

Una sección por versión del manifest, la más nueva arriba. El PR que sube
`version` en `__manifest__.py` agrega aquí su entrada.

## 19.0.1.0.0 — 2026-10-06

Esqueleto del motor (spec `docs/superpowers/specs/2026-10-06-qb-costeo-v2-diseno.md`, Fase 1):

- `qb.centro`: centros directos e indirectos con driver de horas, centros de
  trabajo, departamentos de RH, capacidad normal (calendario × eficiencia o
  capturada con motivo) y bandera de publicación de tarifa.
- `qb.cuenta.clase`: cuenta → bucket (+ centro y %). Importa la
  clasificación de `qb_capacidad_costeo` al instalar si está en la base.
  Cron diario que avisa de cuentas sin clasificar.
- `qb.parametro`: diez parámetros sembrados con ayuda.
- `qb.periodo` + `qb.tarifa`: por centro directo, pool fijo suavizado (con su
  parte de nómina e indirectos), pool variable del mes, horas normales y
  reales (órdenes de trabajo; si no hay, estándar de la receta × producido),
  tarifa fija + variable, capacidad ociosa (IAS 2), % de operación.
  Compuerta de cierre (calculado, horas en todos los centros, sin gasto sin
  clasificar, sin validaciones abiertas en los productos principales).
  Al cerrar, publica la tarifa en `mrp.workcenter.costs_hour`.
- `qb.producto.validacion`: reglas `precio_cero`, `precio_fuera_banda`,
  `receta_implausible` (colorantes y auxiliares por línea y en total); se
  cierran solas al corregir y se aceptan con motivo. Cron semanal.
- Menú Manufactura → Costos. 12 pruebas.
