# Quimibond - Presupuesto y pronóstico de ventas

Presupuesto anual de ventas por mercado o cliente (F-P-A28-18) y pronóstico
semanal por cliente (F-P-A28-13) del procedimiento P-A28, en Ventas →
Presupuesto y pronóstico. Salió de `quimibond_sgi` en 57.11.0 (auditoría
A-016, A-013, E-014; decisión 5 de Jose: «lo que sale del SGI va a módulos
propios, sin borrar datos»).

## Qué trae

- Modelos `sgi.sales.budget`, `sgi.sales.budget.line`,
  `sgi.sales.budget.import` y el campo `budget.analytic.sgi_sales_budget_id`.
  **Conservan el nombre técnico `sgi.*`** a propósito: cambiarlo obligaba a
  renombrar tablas, columnas, XML IDs, reglas y referencias de 1,291 líneas.
- Vistas, matriz (web_grid), reporte, análisis (4 acciones), 8 menús, 2
  reglas por empresa, 10 accesos, la secuencia `PPV-AAAA-`, el pie de
  formato `format_map_sales_budget`, los 9 parámetros `quimibond_sgi.budget_*`,
  `price_*`, `forecast_*` y `sales_budget_alert_pct` (mismas claves) y sus
  ajustes en Ajustes → SGI → Indicadores automáticos.
- Avisos en el motor de crons del SGI (`sgi.cron`): cierre de mes (gancho
  `_sgi_monthly_close_steps`), cobertura semanal del pronóstico y revaluación
  del S2 (crons `sgi_cron_forecast_coverage` y `sgi_cron_budget_revaluation`).

## Pruebas de staging (19.0.1.3.1, 2026-10-01)

Solo pruebas; el código no cambia.

- `test_ajustes_130` (`TestBudgetMinPriceException`, casos 3 y 4): cada
  línea creaba su propio presupuesto del mismo mercado y año, y el segundo
  chocaba con la regla «un presupuesto no obsoleto por mercado y año». La
  clase usa un solo presupuesto y le agrega líneas. Error de la prueba.
- `test_sales_budget` (`TestSalesBudgetLifecycle55.test_05`): esperaba
  «omitidos 1» y el chatter decía 0. Desde 1.1.0 (N2) el presupuesto se
  omite por producto + cliente + **mes** del pronóstico, y la semana del
  pronóstico era el lunes de 2040-06-03, o sea 2040-05-28 (mayo) contra la
  línea de junio. No es regresión ni dato de producción: la prueba no se
  actualizó con N2. Ahora la semana es la del 2040-06-04.

## Ajustes del CEO (19.0.1.3.0, 2026-10-01)

- **Precio mínimo plausible propio** (producto, pestaña Ventas; categoría,
  vale para sus subcategorías): excepción al umbral general de Ajustes ($5)
  para productos que de verdad cuestan menos (tiras perforadas Perfoquim a
  $0.55–$1.28/m). Manda el del producto, luego el de la categoría más
  cercana y al final el general, que no cambia. Con 0.01 se acepta cualquier
  precio mayor que cero; el origen del precio de la línea dice «mínimo propio
  del producto / categoría '…'». Visible para el Admin de ventas y el Jefe
  MAST.
- **«Actualizar real» en «Revisado»** recalcula también el precio de lista sin
  regresar el documento a borrador: el precio sale de la lista, no de la
  captura, y el gate «sin precio» de la aprobación pide justo corregir la lista
  y refrescar. Si algún precio cambia queda constancia en el chatter (líneas,
  importe antes y después, sin precio antes y después). Los crons y «Conciliar
  facturado» siguen refrescando el precio solo en borrador; lo aprobado sigue
  congelado.

## Pantallas (19.0.1.2.0, revisión de vistas V-M10)

- La lista de líneas y la matriz de la ficha suman al pie la cantidad
  presupuestada y facturada y los importes presupuestado, facturado y pedido
  (este último en la lista, columna opcional).
- En el encabezado del presupuesto o pronóstico solo queda el flujo (enviar a
  revisión o marcar revisado, aprobar, nueva revisión, regresar a borrador y
  enviar demanda al MPS). «Análisis» es botón inteligente; «Actualizar real»,
  «Conciliar facturado» e «Importar desde Excel» están en el engrane
  (Acción) de la ficha, y el PDF en el menú Imprimir.
- Las migas dicen lo mismo que el menú: «Presupuestos», «Pronósticos» y
  «Releases de clientes» (V-M12).

## Releases de clientes (19.0.1.1.0, E1a)

Diseño: `docs/superpowers/specs/2026-09-30-releases-pronostico-presupuesto-e0.md`.

- **Partes del cliente** (`qb.customer.part`, Ventas → Presupuesto y
  pronóstico → Partes del cliente): parte del cliente (+ planta, PO y unidad
  MT/LY/M2/KG) → producto. «Sugerir producto» propone el producto con lo que
  ya está en Odoo:
  - nuestra referencia escrita en la descripción del cliente;
  - pedidos que mencionan la parte o su PO;
  - lo que se le ha vendido al cliente;
  - gramaje y ancho contra la ficha (`qb.producto.ficha`, si
    `qb_capacidad_costeo` está instalado).

  Ventas confirma. Solo una parte **confirmada** se usa al aplicar un
  release. La cantidad se convierte con el rendimiento m/kg de la ficha o con
  un factor fijo; sin ese dato la semana no se aplica.
- **Perfiles de release** (`qb.release.profile`): lector, fechas de embarque o
  de entrega (con días de tránsito), zona firme (N semanas o hasta la
  autorización Fab), semanas a pedido, acuse y CUM.
- **Releases** (`qb.release`, con partes y semanas):
  - se sube el archivo y se oprime «Leer», con los lectores de Lear (texto
    AIAG, también dentro de un .eml) y de FXI (Excel SUM);
  - Ventas lo revisa y oprime «Aplicar»: reescribe el pronóstico del cliente
    desde la primera semana del release, repartido por año (crea el
    pronóstico del año siguiente si hace falta);
  - las semanas que el release ya no trae quedan en 0;
  - el release anterior queda «reemplazado».
- **MPS**: cada celda producto × fecha lleva la **suma** de todos los
  pronósticos revisados y presupuestos aprobados de la compañía.
  - Antes el último documento enviado pisaba a los demás clientes.
  - El presupuesto solo se omite para el cliente y el mes que un pronóstico
    ya cubre.
  - En un MPS semanal, el mes del presupuesto se reparte en sus lunes.
- La lógica que no necesita Odoo vive en `release/` (`part_match.py`,
  `units.py`, `lear_aiag.py`, `fxi_sum.py`). Se prueba con pytest en el CI
  desde `tests_puros/ventas_presupuesto/`, fuera del addon para que pytest no
  lo importe.

## Qué se queda en el SGI

El KPI VE-02 (`presupuesto_ventas`) y su evidencia leen
`sgi.sales.budget.line` solo si este módulo está (`'sgi.sales.budget.line' in
env`); sin él caen al parámetro `quimibond_sgi.monthly_sales_budget`, igual
que antes sin presupuesto aprobado. El Diagnóstico cuenta los presupuestos en
borrador con la misma condición.

## Instalación y dependencias

- Depende de `quimibond_sgi` (mixins `sgi.base.mixin`/`sgi.format.mixin`,
  grupos, crons), `sale_stock`, `web_grid` y `account_budget`. El núcleo no
  puede depender de este módulo (sería circular).
- `auto_install`: una base nueva con esas dependencias lo instala junto con
  el SGI, como antes.
- En una base que viene de 57.10.0 o anterior (producción),
  `auto_install` **no basta**: Odoo solo instala solo un módulo cuando otra
  de sus dependencias se está *instalando*, no cuando se actualiza
  (`ir.module.module.button_install` / `button_upgrade`). Por eso
  `quimibond_sgi/migrations/19.0.57.11.0/pre-migrate.py` pasa los XML IDs a
  este módulo (`ir_model_data.module`, sin borrar ni recrear) y lo marca para
  instalar en el mismo update; si hay presupuestos y faltara una dependencia,
  detiene el update.

## Verificación después de desplegar

- `sgi.sales.budget` = 4 registros (ids 4, 5, 7, 137) y 1,291 líneas.
- Menús 2434 «Presupuestos» y 2435 «Pronósticos» con el mismo id, bajo 2438.
- `ir.model.data` con `module = 'quimibond_ventas_presupuesto'` para
  `menu_sale_sgi_sales_budget` (res_id 2434) y `model_sgi_sales_budget`.

## Pruebas

`tests/test_sales_budget.py` (las 117 que vivían en el SGI) y
`tests/test_sales_budget_sgi.py` (multiempresa, Dirección y el cron de
cobertura corrido dos veces, que vivían en otras pruebas del SGI).
`tests/test_ajustes_130.py` (mínimo propio y precio en revisado),
`tests/test_customer_part.py` y `tests/test_release.py` (catálogo de partes,
releases y la suma del MPS): corren en el build de Odoo.sh con
`--test-tags /quimibond_ventas_presupuesto`. Los lectores, el emparejamiento y
las unidades: `pytest tests_puros/` (sin Odoo, en el CI).
