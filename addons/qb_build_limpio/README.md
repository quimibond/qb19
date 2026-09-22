# qb_build_limpio — Build limpio

Corrige en la base de datos lo que pinta de **naranja** el build de Odoo.sh.

## Por qué existe

El log del build de `qbtesting` del 17-sep-2026 traía ~120 avisos. Solo uno
venía del repo (`qb.obligation`: dos campos con la etiqueta "Currency", ya
corregido). El resto vive en la **base de datos**: campos y vistas creados con
Odoo Studio a lo largo de los años, y columnas requeridas a las que Odoo no pudo
poner `NOT NULL` en alguna actualización. Como la base de cada rama es copia de
producción, el mismo aviso se repite en `quimibond`, `main` y `qbtesting` hasta
que alguien lo corrige **en la base**. Odoo.sh no da `psql` desde fuera, y
`ir.model.fields` / `ir.ui.view` no están expuestos por MCP, así que la
corrección tiene que viajar como código y correr dentro de Odoo.

## Qué corrige

| Aviso del build | Origen | Corrección |
|---|---|---|
| `Two fields (a, b) of model() have the same label: X` | dos campos del mismo modelo con la misma etiqueta (casi siempre `x_studio_*` sin renombrar: "Nuevo Campo relacionado", "Nuevo Many2One"…) | renombra el campo de Studio **más nuevo** a `X (2)`, `X (3)`… en todos los idiomas. Nunca toca campos definidos en código; si los dos son de código, se arregla en el addon. |
| `Field 'm.f' in dependency of m.x_studio_… should be searchable` | campo relacionado de Studio cuya ruta pasa por un campo calculado sin búsqueda (`product_variant_id`, `account.account.group_id`, `mail.message.channel_id`) | lo convierte en campo **calculado** equivalente (mismo valor, primer registro en cada salto, como hace `related`) con dependencias que Odoo sí puede seguir: el prefijo buscable de la ruta o, si el primer salto es el no buscable, sus propias dependencias. |
| `invalid custom view(s) for model X: … Default tree/graph/calendar view for …` | vistas "por defecto" que Studio generó en una versión anterior y que el RNG de 19 ya no acepta | si el arreglo es trivial (`quick_add` → `quick_create`) lo aplica; si no, **archiva** la vista y Odoo vuelve a generar la vista por defecto. |
| `El grupo "project.group_project_rating" que está definido en la vista no existe` | vistas personalizadas que referencian un grupo que desapareció al migrar | quita el grupo. Si era el único del nodo, quita el nodo: nadie pertenece a un grupo inexistente, así que ya no se mostraba. Vistas de un addon instalado no se tocan (se reporta). |
| `RELAXNG_ERR_INVALIDATTR: Invalid attribute modifiers for element field` / `Invalid attribute quick_add for element calendar` (y los `NOELEM` / `EXTRACONTENT` que los acompañan) | cualquier vista hecha en la base (Studio, importada) con el arch ya procesado de una versión vieja, no solo las «por defecto» | quita `modifiers` (Odoo 19 ya no lo lee, así que la vista se ve igual) y cambia `quick_add` por `quick_create`, en **todos los idiomas** del arch. Vistas de un addon no se tocan. |
| `Failed documents.document()._gc_clear_bin() … Impossible to delete folders used by other applications` (cada corrida del autovacuum) | una carpeta que una app sigue usando está en la papelera de Documentos; en producción, «Workers Payroll» (id 1708), carpeta de nómina de la empresa 1, en la papelera desde nov-2025 | **restaura** la carpeta (y las carpetas que la contienen). Busca cualquier campo que apunte a una carpeta fuera de Documentos (empresa, proyecto, empleado). |
| `Missing not-null constraint on model.field` | campo `required` cuya columna admite nulos porque al poner la restricción había filas en NULL (`website.*`, `sale.order.template.*`, `product.label.layout.*`) | rellena los nulos con el valor por defecto del campo y pone `NOT NULL`. Sin valor por defecto, lo reporta para revisarlo a mano. |

## Cuándo corre

* **Al terminar cualquier instalación o actualización de módulos**
  (`_register_hook` con `registry.updated_modules`): un build de Odoo.sh, un
  `odoo-update`. En ese momento ya está cargado todo el registro. En un
  arranque normal de los workers no hace nada.
* **A mano:** Ajustes → Técnico → *Build limpio* (solo administradores).
  Muestra qué corrigió.

Todo es idempotente y va en savepoints: un tropiezo en un punto no tumba el
build ni deja el resto sin hacer. Cada corrección queda en el log con el
prefijo `Build limpio [paso]:`.

## Qué esperar en el build

El primer build que instale el módulo **todavía sale naranja**: los avisos se
emiten al cargar el registro, antes de que corra la limpieza. El siguiente
build de esa base (y cualquier rama que se rebuildee desde producción después)
sale limpio.

## Despliegue

> **Al 22-sep-2026 el módulo NO está instalado en producción** (`uninstalled`).
> Por eso los avisos que ya sabe corregir (el `quick_add` del calendario, por
> ejemplo) siguen saliendo en cada build: la base de cada rama es copia de
> producción y ahí nunca ha corrido.

1. Merge a `main` → `quimibond`. Instalar el módulo en producción desde Apps
   (módulo nuevo: Odoo.sh no instala módulos solo) o desde el shell:
   `odoo-update -i qb_build_limpio` no existe; usar Apps.
2. Al instalar corre la limpieza. Verificar en el log:
   `grep "Build limpio" ~/logs/odoo.log`.
3. Rebuild de `qbtesting` desde producción → build sin naranja.

## Lo que NO corrige

* Etiquetas duplicadas entre dos campos **de código** (se arregla en el addon).
* Vistas de un addon instalado que referencian grupos inexistentes (se arregla
  en el addon).
* Columnas requeridas sin `NOT NULL` cuyo campo no tiene valor por defecto.
* Las etiquetas renombradas quedan como `Maquina (2)`; si el nombre correcto es
  otro, se cambia en Studio.
