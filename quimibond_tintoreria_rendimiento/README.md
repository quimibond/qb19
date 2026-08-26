# Quimibond - Tabla de Rendimientos y RB Tintorería

Módulo Odoo 19, independiente de "Quimibond - Ficha Técnica de Tela"
(aunque ambos alimentan la misma lógica de cálculo de tamaño de orden /
split en Tintorería que se construirá más adelante).

## Instalación

1. Copiar la carpeta `quimibond_tintoreria_rendimiento` a la carpeta de
   addons de tu instancia.
2. **Requisito previo**: los centros de trabajo "TINTORERIA 1" a
   "TINTORERIA 5" (códigos `HTJ1`-`HTJ5`) deben existir ya en
   Manufactura > Configuración > Centros de Trabajo — los datos
   iniciales de este módulo los buscan por código para vincularlos, no
   crean centros de trabajo nuevos.
3. Actualizar la lista de aplicaciones e instalar
   "Quimibond - Tabla de Rendimientos y RB Tintorería".
4. Requiere el módulo `mrp` instalado.

## Uso

- **Manufactura → Configuración → Tabla de Rendimientos y RB Tintorería**.
- El campo **Centro de trabajo** es obligatorio y solo permite
  seleccionar centros de trabajo cuyo nombre empiece con "Tintorería"
  (dominio `name =ilike 'Tintoreria%'`).
- **Código** ya no se captura manualmente — es un campo de solo lectura
  (`related`) que siempre refleja el código real del centro de trabajo
  seleccionado. No hay un campo "Nombre" aparte porque el nombre ya se
  ve directamente en el campo "Centro de trabajo" — mantenerlo aparte
  sería duplicar la misma información.
- Se precarga con los 5 centros de trabajo (HTJ1-HTJ5) tomados del Excel
  de origen: capacidad máxima, capacidad por banda de rendimiento
  (Grupo A 3-6, B 7-10, C 11-15 m/kg) y Relación de Baño (RB, L/kg) —
  vinculados por búsqueda de código, no por ID fijo.
- Editable en línea desde la vista de lista (excepto Código, que es de
  solo lectura).

## Actualización desde una versión anterior (19.0.1.0.0)

Si ya tenías este módulo instalado desde antes de que `codigo`/`nombre`
se volvieran campos `related`, esos 5 registros se cargaron originalmente
con `noupdate="1"` — así que una simple actualización de módulo **no**
los toca ni les asigna `workcenter_id` automáticamente. Para resolver
esto, el módulo incluye una migración (`migrations/19.0.1.1.0/`) que se
ejecuta sola al actualizar: busca, para cada registro sin
`workcenter_id`, el centro de trabajo real que coincide por código (o
por nombre) y lo vincula. Solo necesitas actualizar el módulo una vez
más con esta versión — no requiere ningún paso manual.

## Carga de datos idempotente (importante)

La carga inicial de las 5 tintorerías (HTJ1-HTJ5) se hace mediante un
método (`_load_default_data`) llamado vía `<function>`, no con
`<record>` directos. Antes de crear cada configuración, busca primero si
ya existe una para ese centro de trabajo — si ya existe, no la toca ni
la duplica. Esto evita el error `duplicate key value violates unique
constraint ...workcenter_uniq` que puede aparecer si el módulo se
(re)inicializa sobre una base de datos que ya tenía esas
configuraciones vinculadas (por ejemplo, tras una migración previa, o
en un refresco de ambiente de staging que copia datos de producción).

## Métodos disponibles para desarrollos futuros

En el modelo `tintoreria.capacidad.rendimiento`:

- `capacidad_para_rendimiento(rendimiento)` — devuelve los kg permitidos
  en ese centro según la banda de rendimiento del artículo (0.0 si no
  aplica).
- `litros_para_kg(kg)` — calcula litros de baño necesarios
  (RB × kg de la orden).

Estos métodos están pensados para ser consumidos por el desarrollo de
"split automático de orden en Tintorería" y "cálculo de químicos/agua",
descritos en el documento de Diseño Técnico general del proceso de Tela
Acabada.
