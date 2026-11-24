# qb19

Addons de Odoo 19 de Quimibond (productora de no tejidos y tejido circular, Toluca). El repo está ligado a Odoo.sh.

- **Reglas del repo, ramas y despliegue:** [`CLAUDE.md`](CLAUDE.md) y [`docs/RUNBOOK_DESPLIEGUE.md`](docs/RUNBOOK_DESPLIEGUE.md).
- **Go-live de Tintorería, Rama e Inspección:** [`docs/GO_LIVE_TINTORERIA.md`](docs/GO_LIVE_TINTORERIA.md).
- **Documentación del SGI:** [`docs/sgi/`](docs/sgi/). Lo que ya no describe el estado actual está en [`docs/historico/`](docs/historico/).
- `requirements.txt` lo instala Odoo.sh en cada build.

## Ramas

`quimibond` es producción, `main` es donde se integra y se prueba antes de pasar a producción, y `qbtesting`/`consolti` son de Consolti. Se mantienen como superconjuntos: `main` ⊇ `quimibond` y `qbtesting` ⊇ `quimibond` (ver `CLAUDE.md`).

## Módulos

Los de la raíz son de Consolti (José Sacramento) y de terceros (`mcp_server`); los de `addons/` son de Quimibond. La columna de producción es la foto del 2026-11-24.

| Módulo | Carpeta | Qué hace | En producción |
|---|---|---|---|
| `devolucion_hilo_produccion` | `devolucion_hilo_produccion/` | Proceso de re-empaque, pesaje y devolución de hilo a la ubicación de almacenamiento principal. | sí |
| `iot_scale_common` | `iot_scale_common/` | Widget Owl y mixin común para capturar peso desde básculas IoT en wizards | sí |
| `mantenimiento_surtido_refacciones` | `mantenimiento_surtido_refacciones/` | Alta de refacciones en solicitudes de mantenimiento con generación automática de traslado interno hacia ubicación de consumo costeada. | no (desinstalado en producción, main y pruebas) |
| `mcp_server` | `mcp_server/` | Connect AI assistants to your Odoo instance via Model Context Protocol | sí |
| `mrp_caja_surtido` | `mrp_caja_surtido/` | Surtido de hilos por Empaque Completo | sí |
| `mrp_revisado_telas` | `mrp_revisado_telas/` | Control de pesaje y revisión desde el tablero de centros de trabajo | sí |
| `pesaje_rollos_tejido` | `pesaje_rollos_tejido/` | Control de pesaje de rollos, subproductos, lotes automáticos y etiquetas ZPL | sí |
| `quimibond_ficha_tecnica_tela` | `quimibond_ficha_tecnica_tela/` | Fichas técnicas de tela en las pantallas nativas: tela, crudo, ruta, máquinas y revisión de listas | sí |
| `quimibond_hojas_calidad` | `quimibond_hojas_calidad/` | Calidad → Laboratorio tela acabada: Registro Rama y Revisión tela acabada abren el libro del artículo en Documentos | no (instalado en pruebas) |
| `quimibond_rollos_bitacora` | `quimibond_rollos_bitacora/` | Tabla de taras por producto y operación, bitácora de rollos y pesaje en Rama de la MO en kg | no (instalado en main; go-live pendiente) |
| `quimibond_tintoreria_dosificacion` | `quimibond_tintoreria_dosificacion/` | Catálogo de fórmulas de tintorería y cálculo de requerimiento de químicos (g/L) y colorantes (%) | sí |
| `quimibond_tintoreria_programacion` | `quimibond_tintoreria_programacion/` | Asignación de HTJ por orden, cálculo del tamaño de baño y división simétrica de la MO de tela acabada en kg | sí |
| `quimibond_tintoreria_rendimiento` | `quimibond_tintoreria_rendimiento/` | Capacidad por rendimiento y Relación de Baño (RB) por centro de trabajo de Tintorería | no (desinstalado en producción, main y pruebas) |
| `reporte_orden_fabricacion` | `reporte_orden_fabricacion/` | Reporte técnico PDF de trazabilidad completa para MO | sí |
| `reporte_plano_piso_tejido` | `reporte_plano_piso_tejido/` | Reporte tipo vista de plano de piso para órdenes de fabricación de la operación Tejido Circular | sí |
| `reporte_produccion_diaria` | `reporte_produccion_diaria/` | Reporte en vista (lista) de órdenes de fabricación con subproductos, centros de trabajo y costos, para la operación Tejido Circular. | sí |
| `stock_dymo_labels` | `stock_dymo_labels/` | Genera etiquetas Dymo (PDF) y Zebra (ZPL USB) desde la recepción | sí |
| `qb_build_limpio` | `addons/qb_build_limpio/` | Corrige en la base lo que pinta de naranja el build de Odoo.sh: campos y vistas de Studio con etiquetas duplicadas, dependencias no buscables, vistas inválidas, grupos inexistentes y columnas requeridas sin NOT NULL. | sí |
| `qb_checador` | `addons/qb_checador/` | Recibe las checadas de los relojes ZKTeco de la planta (protocolo ADMS/PUSH o por API) y las convierte en asistencias de Odoo. | sí |
| `qb_cierre_inventario` | `addons/qb_cierre_inventario/` | Casilla «No valuar en el cierre» en la categoría de producto: el cierre y el reporte de valuación de Odoo 19 ignoran esas categorías (refacciones, activo fijo). | sí |
| `qb_costeo` | `addons/qb_costeo/` | Tarifas por centro desde el mayor, períodos con compuerta y validación de recetas | sí |
| `qb_costeo_sgi` | `addons/qb_costeo_sgi/` | Cotizaciones ligadas al proyecto de desarrollo de producto (C1): datos del proyecto, recálculo por revisión y medición del SGI | sí |
| `qb_cotizador` | `addons/qb_cotizador/` | Cotizaciones de costo con aprobación por puesto, seguimiento, vencimiento y precio en tarifa al ganar; negociaciones de precio y acuerdos de volumen | sí |
| `qb_flujo_efectivo` | `addons/qb_flujo_efectivo/` | Proyección semanal de caja (método directo) en Contabilidad → Reportes: caja, cobranza y pagos por vencimiento, pedidos por facturar, nómina, IMSS y SAT. | no (nuevo) |
| `qb_memoria` | `addons/qb_memoria/` | Pestaña Memoria en cada contacto: hilos de correo, pendientes detectados, demanda y contactos, desde la memoria en Supabase. | sí |
| `qb_pedimento` | `addons/qb_pedimento/` | Expediente de cada pedimento de importación y exportación: documentos, contribuciones, cuenta de gastos, garantías y avisos. | sí |
| `qb_tejido_ritmo` | `addons/qb_tejido_ritmo/` | Cuánto teje cada circular, cuánto corre, cuánto para y cuánto pierde en cambios, medido rollo por rollo desde el pesaje. | sí |
| `quimibond_intelligence` | `addons/quimibond_intelligence/` | Puente mínimo Odoo ↔ Supabase: contactos, empresas y usuarios para la memoria de correo. | sí |
| `quimibond_nomina` | `addons/quimibond_nomina/` | Registro patronal por contrato, SDI y SBC del recibo y nodo HorasExtra en el CFDI de nómina, y un centinela que avisa cuando una regla salarial cambia por detrás. | sí |
| `quimibond_sat` | `addons/quimibond_sat/` | CFDI del SAT (vía Syntage) dentro de Odoo y comparación contra las facturas registradas. | sí |
| `quimibond_sat_sgi` | `addons/quimibond_sat_sgi/` | Lo que no cuadra entre el SAT y Odoo llega a Mis pendientes de quien ocupa el puesto. | sí |
| `quimibond_sgi` | `addons/quimibond_sgi/` | Sistema de Gestión Integral (ISO 9001/14001/45001) sobre apps nativas de Odoo 19 | sí |
| `quimibond_sgi_knowledge` | `addons/quimibond_sgi_knowledge/` | La documentación del SGI en Conocimiento: manuales por rol, instructivos, controles operacionales, protocolos y reglamentos, y su publicación como revisión controlada | sí |
| `quimibond_sgi_mapa` | `addons/quimibond_sgi_mapa/` | El mapa de 14 procesos de producción, para cargarlo a mano en staging o en una recuperación | no (se carga a mano, a propósito) |
| `quimibond_sgi_plm` | `addons/quimibond_sgi_plm/` | Enlaza los cambios de ingeniería (ECO) con PPAP/AMEF/planes de control del SGI | sí |
| `quimibond_sgi_revisado` | `addons/quimibond_sgi_revisado/` | Pareto de defectos del revisado de tela (etiquetas TEJIDO-*) para el SGI | sí |
| `quimibond_sgi_studio` | `addons/quimibond_sgi_studio/` | El rol «Aprueba» del SGI bloquea el botón del documento con la regla de aprobación de Studio | sí |
| `quimibond_ventas_presupuesto` | `addons/quimibond_ventas_presupuesto/` | Presupuesto de ventas anual y pronóstico vigente por cliente y semana (P-A28), en la app Ventas | sí |
| `website_quimibond` | `addons/website_quimibond/` | Sitio web B2B de Quimibond: catálogo técnico desde los productos de Odoo, cotizaciones a CRM, mercados, planta, calidad y animaciones de tejido. | sí |
| `website_quimibond_proveedores` | `addons/website_quimibond_proveedores/` | Portal de proveedores del sitio B2B: órdenes de compra, subir la factura (PDF y XML) contra la orden, estado de pago y alta de proveedor en línea. | no (nuevo) |

## Carpetas que no son módulos

| Carpeta | Qué hay |
|---|---|
| `docs/` | Runbook, go-live, SGI, diseños (`superpowers/specs`), planes, auditorías, incidentes e histórico. |
| `tools/` | Checkers del CI (`check_addons.py`, `check_odoo_views.py`, `check_odoo_fields.py`), generadores de la documentación del SGI y scripts de una vez que todavía faltan en producción (`categorias/`). |
| `tests_puros/` | Pruebas sin Odoo que corre el CI con pytest. |

## Relacionado

- [quimibond-intelligence](https://github.com/quimibond/quimibond-intelligence): Edge Functions y migraciones de Supabase (memoria de correo). El frontend de Vercel está retirado.
