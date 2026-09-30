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
  ajustes en Ajustes → SGI → KPIs automáticos.
- Avisos en el motor de crons del SGI (`sgi.cron`): cierre de mes (gancho
  `_sgi_monthly_close_steps`), cobertura semanal del pronóstico y revaluación
  del S2 (crons `sgi_cron_forecast_coverage` y `sgi_cron_budget_revaluation`).

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
`tests/test_customer_part.py` y `tests/test_release.py` (catálogo de partes,
releases y la suma del MPS): corren en el build de Odoo.sh con
`--test-tags /quimibond_ventas_presupuesto`. Los lectores, el emparejamiento y
las unidades: `pytest tests_puros/` (sin Odoo, en el CI).
