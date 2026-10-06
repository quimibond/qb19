# C1 Desarrollo y alta de producto — plan de implementación

**Fecha:** 2026-10-06 · **Brief:** `docs/superpowers/specs/2026-10-06-c1-desarrollo-producto-brief.md`
(copia del documento de Drive `C1_desarrollo_producto_brief_claude_code.md`) ·
**Módulo:** `quimibond_sgi` (+ satélite para lo que toca al costeo) ·
**Rama:** `claude/compassionate-dirac-9qstog` → `main` por bloques.

## 1. Lo que cambia respecto al brief (leído el código) y lo que decidió Jose

El brief se levantó por MCP, no leyendo el módulo. Al leer el código salieron
cinco puntos; Jose los resolvió el mismo día (respuesta al PR #563).

| # | Dice el brief | Lo que hay en el código | Decisión de Jose (2026-10-06) |
|---|---|---|---|
| 1 | «Mismo catálogo para `qb.producto.ficha.spec`, para que al liberar el artículo los renglones pasen a su ficha técnica» (5.1) | `qb.producto.ficha` vive en `qb_capacidad_costeo`, no en el SGI, y la spec de costeo v2 (aprobada el mismo día, §8.6 y decisión 3) la **retira**: la ficha del producto pasa a `quimibond_ficha_tecnica_tela` (Consolti, en la raíz del repo). | **La ficha y el catálogo de características viven en `quimibond_ficha_tecnica_tela`.** Hecho en el bloque 1: catálogo `ficha.tecnica.caracteristica`, claves `ficha.tecnica.clave.codigo`, mixin de límites y renglones `ficha.tecnica.spec` en las fichas de tejido y acabado con dos juegos de límites (cliente y control interno) y la marca «va a la especificación del cliente». El SGI depende de ese módulo. La derivación de fichas desde el código queda anotada en el plan de costeo v2 (§8.6). |
| 2 | Cotización (`qb.cotizacion`) con proyecto, estado «Por aprobar», seguimiento, tarifa (6.8) | `qb.cotizacion` está en `qb_capacidad_costeo` y costeo v2 lo sustituye por `qb_cotizador` + puente `qb_costeo_sgi`. | **No tocar `qb_capacidad_costeo`.** Todo el 6.8 pasó como requisitos al plan de costeo v2 (spec §6.1), incluida la migración de las cotizaciones existentes. C1 sigue con 6.1, 6.6 y 6.3 a 6.5, que no dependen de costeo. |
| 3 | La pestaña de desarrollo se ve solo si `sgi_is_ft` (3) | `sgi_is_ft` se calcula del nombre («FT-…») con `store=True, readonly=False`; los 77 proyectos viejos no se recalcularon. | **Bandera por tipo de proyecto** que signifique «desarrollo de producto», con el **folio FT como campo aparte**. La migración marca los 77 proyectos FT-, las plantillas 480 y 481 y el proyecto 490. En el mismo bloque se cambian los dominios de medición del SGI que filtran por `name =like 'FT-%'`. |
| 4 | Tipo de desarrollo como catálogo (5.1 «por tipo de desarrollo») | `sgi_dev_type` es una `Selection` fija y `sgi.format.map` la usa para elegir el formato impreso (`format_ref_dev_<tipo>`). | Se conserva la selección (cuatro tipos estables) y los renglones por tipo son datos (`sgi.dev.characteristic.template`). Agregar un tipo sigue siendo cambio de código, igual que su formato impreso. |
| 5 | «El módulo ya tiene» campos de muestra en m y kg, volumen, precio objetivo (3) | Correcto, pero `sgi_dev_requester` y `sgi_dev_norms` son texto y `sgi_dev_spec` es un texto largo para la especificación del cliente. | `sgi_dev_spec` queda como referencia a la hoja del cliente (el valor va en la tabla); el solicitante interno pasa a `hr.employee` y las normas a lista en el bloque 2. |

Verificado contra el PDF del DAT P-D02-01 (rev. 05, mayo 2025): las tablas
del brief son correctas; solo cambian dos etiquetas de acabado (DI es
«Dryfit», RA es «Resina acrílica»). Los otros cuatro DAT (4695 a 4698) siguen
sin revisar.

## 2. Bloques y orden

Cada bloque sube versión del SGI, agrega su entrada al CHANGELOG, trae sus
pruebas y se prueba solo en el build de Odoo.sh de la rama con
`--test-tags /quimibond_sgi`.

| Bloque | Qué | Brief | Estado |
|---|---|---|---|
| 1 | Tabla numérica de características en el proyecto; catálogo, claves de codificación y límites en `quimibond_ficha_tecnica_tela` 2.1.0; renglones por tipo en el SGI | 6.2, 5.1, 5.2 (modelos) | **Hecho, 57.117.0** (PR #563) |
| 2 | Proyecto único con ciclo de vida: bandera «desarrollo de producto» por tipo de proyecto con migración (77 FT-, plantillas 480 / 481, proyecto 490) y dominios de medición corregidos; folio FT aparte por secuencia anual; etapas de avance, origen, revisión y bitácora, alias de correo, pestaña comercial, muestra física, resultado del análisis, relojes por paso | 6.1, decisión 3 | **Hecho, 57.118.0** (mismo PR #563, commit aparte). Pendiente: dominio de alias en la base; limpieza de etapas por cliente (sección 7) |
| 3 | Artículo en desarrollo y generador de código (crudo, teñido, acabado); bloqueo de 16292 / 16293 con fecha acordada con Jose | 6.6 | **Hecho, 57.119.0** (mismo PR #563). Pendiente: fecha del bloqueo del genérico (parámetro vacío) |
| 4 | Búsqueda de parecidos, solicitud de pruebas a laboratorio, checklist de factibilidad (modelo y vista, catálogo vacío) | 6.3 a 6.5 | **Hecho, 57.120.0** (mismo PR #563). Pendientes de datos: puesto autorizador (188) en el parámetro; catálogo de recursos (Yet) |
| 5 | Cotización | 6.8 | **Fuera de C1**: requisitos en el plan de costeo v2 (spec §6.1), sobre `qb_cotizador` / `qb_costeo_sgi` |
| — | Duplicidad `ficha.tecnica.tejido` (parámetros de máquina) ↔ `sgi.machine.sheet` | 6.7 | **No se tocó** en el PR #563; decisión del siguiente PR (Jose, 2026-10-06) |
| 6 | Solicitud de desarrollos (PDF con clave nueva, compuerta de Jorge, aviso a seis puestos, requisición ligada), orden de muestra, fichas de proceso de tintorería y acabado | 6.7, 6.9, 6.10 | Esperan a las sesiones con Planeación, Producción, Calidad y Compras |
| 7a | Envío de muestra y respuesta del cliente | 6.11 | **PR nuevo** (siguiente) |
| 7b | Pilotaje, habilidad, ficha interna, especificaciones al cliente, PPAP, liberación y cierre | 6.12 | Espera a las sesiones con Calidad |
| 8 | Escalamiento configurable y correcciones de medición | 6.13, 7.2 | **PR nuevo** (con 6.11) |
| 9 | Datos por MCP (fichas C1.01 a C1.19, plantillas 480 / 481, folios, partes interesadas, formatos obsoletos), primero en qbtesting | 7 | Al final |

## 3. Decisiones de diseño del bloque 1

- **Una tabla, columnas por momento.** `sgi.dev.characteristic` guarda por
  renglón: especificación del cliente (`spec_nominal`, `spec_limit` nominal ±
  / máximo / mínimo, `spec_tol_minus`, `spec_tol_plus`, `spec_tol_pct`),
  control interno (`ctrl_tol_minus`, `ctrl_tol_plus`, sobre el mismo nominal
  y nunca más abierto que el del cliente), medido en la muestra
  (`sample_value`), corrida (`run_1..3`, `run_avg`), dictamen (`verdict`, lo
  pone Diseño de Producto), aprobación del cliente y las marcas «va a la
  especificación del cliente» / «va al certificado».
- **Tres resultados** calculados contra límites: `cumple` (dentro del control
  interno), `desviacion` (fuera del interno, dentro del cliente),
  `no_conforme`. El certificado y el estudio de habilidad usan los límites del
  cliente; el PDF de la solicitud no imprime el control interno.
- **Cualitativas aparte.** `kind` del catálogo decide qué columnas aplican:
  numérica, cualitativa (`spec_text`, `sample_text`, `run_text`) o sí / no
  (`spec_bool`). Nada de valores en texto para lo que es número.
- **Rendimiento calculado** 1000 / (masa × ancho) en todas las columnas, con
  tolerancia a partir de los extremos de masa y ancho. En carda y tramado la
  masa se mide en tres puntos y el rendimiento toma el centro.
- **Posición** (izquierda / centro / derecha) además de dirección: solidez al
  frote y masa / espesor por orillas.
- **Catálogo con `code` estable** por característica
  (`ficha.tecnica.caracteristica`, en `quimibond_ficha_tecnica_tela`): es la
  llave con la que los bloques siguientes reconocen masa, ancho, galga,
  composición al generar el código del artículo y al pasar renglones a la
  ficha del producto (`_limit_vals()` del mixin).
- **Claves de codificación como datos** (`ficha.tecnica.clave.codigo`,
  `noupdate`), con `gauge_code()` / `gauge_from_code()` para la galga por
  rango.
- **Un mixin, dos documentos.** `ficha.tecnica.caracteristica.mixin` lleva
  los dos juegos de límites y las marcas de documentos; `ficha.tecnica.spec`
  (fichas de tejido y acabado) y `sgi.dev.characteristic` (proyecto) lo
  heredan. El SGI agrega solo lo del desarrollo: muestra del cliente,
  corrida, dictamen, aprobación.

## 4. Pendientes que el brief deja sin definir (no se inventan)

Ver sección 8 del brief. En el código quedan como parámetro vacío o catálogo
sin renglones: margen mínimo, suplente de aprobación, recursos del checklist
de factibilidad, lecturas por lote para Cpk, comité de pilotajes, tiempo de
conservación de muestras, destino de los 152 lotes de la ubicación 57.

## 5. Reglas de Jose del 2026-10-06 (tarde) para lo que sigue

1. **Rama de Jose Sacramento.** Tiene trabajo local sin subir sobre
   tintorería (fórmulas en g/L) y acabado (clasificación de calidad y
   defectos). Cuando exista la rama, **revisarla antes de seguir**. Lo que
   ya está subido en `consolti` (2026-10-01) para `quimibond_ficha_tecnica_tela`:
   `rendimiento_tela_tejida`, `jefe_manufactura` / `auxiliar_procesos` a
   `hr.employee`, `maquina_tejido` a `mrp.workcenter` con migraciones en
   `19.0.2.1.0` y dependencia `hr`. **Choque de versión:** `main` ya usa
   2.1.0 (PR #563, catálogo de características); al traer `consolti` su
   versión y su carpeta de migración deben pasar a 2.2.0 para que la
   migración corra en bases que ya estén en 2.1.0.
2. **No eliminar ni renombrar columnas fijas** de `ficha.tecnica.tejido` ni
   de `ficha.tecnica.acabado`: su código las lee. Los renglones de
   características conviven con ellas (el PR #563 no las tocó).
3. **Dos rendimientos, los dos como campo:** el de tejido alimenta el
   cálculo de tintorería; el de acabado valida los metros finales. README
   del módulo corregido en 2.1.1.
4. **Bloque 6.7:** la ficha de proceso de tintorería y la receta de rama
   **no capturan químicos**; apuntarán al modelo de fórmulas de Sacramento.
   No se construye hasta ver su rama.
5. `ficha.tecnica.tela` (modelo viejo) se borra del código: hecho en 2.1.1
   (la tabla queda en la base).

## 6. Incidente del despliegue a producción (2026-10-06 23:22 UTC) y reglas nuevas

- Código 57.120.1 instalado, pero la migración de datos de 57.118.0 no quedó
  en la base (bandera, folio, cliente y origen de los 77 FT-; nombres
  «Análisis» solo en en_US en 480, 481 y 490). Causa y corrección en el
  CHANGELOG 57.120.2. El cambio de cliente de 465/466/489 a SHAWMUT LLC fue
  manual (Jessica, 19:54–20:02 UTC), no de la migración.
- **Regla (Jose):** nada sube a `quimibond` sin pasar antes por `qbtesting`
  y sin su confirmación explícita. El build de `main` (staging) no corre
  pruebas; las del SGI corren en el build de desarrollo de la rama.
- Los filtros de medición de C1 excluyen plantillas (`is_template`).
- Los modelos `sgi.dev.*` y `ficha.tecnica.*` quedan expuestos al MCP (la
  corrección final de datos de la sección 7 se hace por MCP).
