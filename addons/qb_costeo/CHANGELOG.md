# Changelog — qb_costeo

Una sección por versión del manifest, la más nueva arriba. El PR que sube
`version` en `__manifest__.py` agrega aquí su entrada.

## 19.0.1.0.1 — 2026-10-06

- La importación de la clasificación de `qb_capacidad_costeo` leía la vista
  `qb_costeo_cuenta_map`, que es un `_table_query` y no existe en la base:
  en producción importó 0 cuentas. Ahora lee las tablas base
  (`qb_cuenta_class_account_rel` + `qb_costeo_cuenta_class`) con la misma
  regla de precedencia del módulo anterior. Migración `post-migrate` que la
  corre; también se puede llamar por MCP (`importar_clasificacion_legada`).
- Al instalar o actualizar, habilita sus seis modelos en el servidor MCP del
  repo si está instalado (lectura, alta, cambio, métodos).
- Validador, tras la primera corrida en producción (972 hallazgos, casi todos
  ruido): la última compra se convierte bien a moneda de la compañía
  (`currency_rate` es compañía → divisa: se divide, no se multiplica) y a la
  unidad del producto; los componentes con receta propia (teñidos, crudos,
  preparaciones) no se validan por precio, su costo sale de su receta; la
  regla de colorantes mide la preparación de color por kg de tela (30 % por
  línea, 40 % en total, parámetros) y no se aplica a las preparaciones
  mismas ni a productos de categorías excluidas (`Maquila`, parámetro
  nuevo). La migración ajusta los umbrales y vuelve a correr el validador.

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
