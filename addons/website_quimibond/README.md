# website_quimibond — Sitio web B2B de Quimibond

Diseño propio de www.quimibond.com para una fábrica B2B: catálogo técnico,
mercados, planta, calidad y solicitud de cotización, conectado a las apps de
Odoo. Estilo tipo shadcn (zinc, bordes finos, tipografía Geist) con
animaciones de tejido circular y de no tejido.

## Cómo se enciende (y se apaga)

Sitio web → Configuración → Sitios web → abrir el sitio → casilla
**Diseño B2B de Quimibond**.

- Marcada: ese sitio usa el encabezado, el pie, la portada y las páginas de
  este módulo.
- Desmarcada: el sitio queda exactamente como estaba; las rutas del módulo
  responden 404.

Instalar el módulo no cambia nada por sí solo. Para probar sin tocar el sitio
público, marque la casilla en otro sitio (por ejemplo «Quimibond 2») y ábralo
con el selector de sitio web del editor.

## Páginas

| Ruta | Qué muestra | De dónde salen los datos |
|---|---|---|
| `/` | Portada | Categorías y productos publicados, vacantes |
| `/catalogo` | Catálogo por familias, sin precios | `controllers/familias.py` |
| `/catalogo/familia/<clave>` | Rangos de la familia, artículos de línea y formulario de ficha técnica | `familias.py` + productos publicados (solo en las familias de entretelas) |
| `/catalogo/producto/<producto>` | Ficha del producto (solo si su nombre cae en una familia) | Nombre, código y unidad del producto |
| `/compradores-automotrices` (`/automotive-buyers`) | Lo que compras y calidad piden para evaluar: scrims, certificados, laboratorio, desarrollo | `controllers/prospectos.py` |
| `/agendar` | Solicitud de llamada con Ventas, o la liga de citas del sitio | `prospectos.py`, `website.qb_url_agendar` |
| `/mercados/automotriz`, `/blancos`, `/confeccion` | Página por mercado | `controllers/contenido.py` |
| `/planta`, `/calidad`, `/nosotros` | Empresa | `controllers/contenido.py` + datos de la compañía |
| `/cotizar` | Solicitud de cotización o muestra | Crea `crm.lead` (formulario web de Odoo) |
| `/en/…` | Las mismas páginas en inglés | `i18n/en_CA.po` |
| `/mi-cuenta`, `/mi-cuenta/pedidos`, `/mi-cuenta/pedido/<id>`, `/mi-cuenta/estado-de-cuenta` | Portal del cliente: pedidos con etapa y fecha compromiso, entregas, facturas y saldo | `sale.order`, `stock.picking`, `account.move` de la empresa del usuario |
| `/jobs`, `/my?todo=1`, `/helpdesk` | Empleos, portal nativo, reportes de calidad | Apps nativas, con el encabezado nuevo |

## Mi cuenta (portal del cliente, desde 1.12.0)

Lo que ve un cliente al iniciar sesión. `/my` lo manda a `/mi-cuenta`; el
portal nativo sigue en `/my?todo=1` (enlace «Más documentos»).

**Por invitación (desde 1.15.0).** «Mi cuenta» y Calidad en línea solo se abren
a las empresas con la casilla **Acceso a Mi cuenta** (ficha de la empresa →
Ventas y compras; la ve y la marca un gerente de Ventas). Los usuarios de portal
de una empresa sin la casilla, y quien se registra solo en el sitio, se quedan en
el portal de siempre (`/my`). Para invitar: marcar la casilla en la empresa y
dar acceso al portal al contacto (Acción → Otorgar acceso al portal).

- **Resumen**: pedidos en curso con su etapa (Confirmado · En proceso ·
  Embarcado · Facturado), próxima fecha compromiso, saldo por moneda (por
  vencer y vencido), facturas por pagar y últimas entregas.
- **Pedidos** (12 meses) y **detalle del pedido**: productos con lo pedido,
  entregado y pendiente; entregas con su remisión en PDF; facturas.
- **Estado de cuenta**: facturas por pagar y pagadas (12 meses). «PDF y XML»
  baja los documentos fiscales de la factura con la ruta nativa del portal.

Reglas (`controllers/cuenta.py`):

- **Cada cliente ve solo lo de su empresa**: todo se filtra por
  `partner_id child_of commercial_partner_id` del usuario. Un contacto ve los
  pedidos y facturas de toda su empresa (no solo los que sigue, como en el
  portal nativo). El pedido de otra empresa responde 404.
- **Nada de operación interna**: de producción solo sale la etapa «En
  producción» cuando el pedido tiene una orden de fabricación abierta. La etapa
  la calcula `etapa_pedido()`.
- **Vista previa**: un gerente de Ventas (`sales_team.group_sale_manager`) abre
  `/mi-cuenta?cliente=<id del contacto>` y ve la cuenta como ese cliente, con
  un aviso arriba. Cualquier otro usuario interno va a `/odoo`.
- Dar acceso a un cliente: en su contacto, Acción → Otorgar acceso al portal.

## Calidad en línea (dentro de Mi cuenta, desde 1.13.0)

Pestaña **Calidad** (`/mi-cuenta/calidad`, `controllers/calidad.py`):

- **Certificado por lote junto a cada entrega.** Cada entrega embarcada muestra
  sus lotes (los de la remisión) y su certificado. El certificado es el PDF que
  Laboratorio adjunta a la salida con «Adjuntar CoA» del SGI
  (`stock.picking.sgi_coa_attachment_ids`); aquí no se genera nada. Con el PDF,
  se descarga; si el cliente lo tiene acordado (`sgi_requires_coa`) y falta,
  dice «En preparación»; si no lo tiene acordado, una raya. La misma columna
  sale en las tablas de entregas del resumen y del pedido.
- **Reportes de calidad con seguimiento.** El cliente elige la entrega, marca
  los lotes, escribe qué encontró y sube fotos. Queda como ticket del equipo
  de reclamaciones de la mesa de ayuda (el del SGI,
  `quimibond_sgi.sgi_helpdesk_team_complaints`, u otro con el parámetro
  `website_quimibond.equipo_reclamaciones` = id del equipo), con pedido,
  producto y lote en los campos del SGI. El cliente ve cuatro pasos (Recibida ·
  En análisis · Respuesta · Cerrada), los mensajes públicos y contesta ahí.

Reglas:

- **Sin la mesa de ayuda o sin el SGI la página sigue abriendo** (el CI es
  Community): muestra los lotes, y el certificado y los reportes no aparecen.
  Todo se pregunta al momento (`'helpdesk.ticket' in env`, `'… ' in _fields`).
- **Cada cliente ve solo lo suyo**: un certificado se entrega solo si está
  ligado como CoA a una entrega de su empresa; un reporte, solo si es de su
  empresa y del equipo de reclamaciones. Lo ajeno responde 404.
- **Del reporte solo salen los mensajes públicos**: ni notas internas ni sus
  archivos, ni la no conformidad que Calidad abra, ni el nombre de quien
  contesta (firma «Quimibond»). La etapa interna se resume con
  `etapa_reclamo()`.
- En la vista previa de Ventas (`?cliente=`) no se levantan reportes ni se
  contestan.
- Las pruebas de reportes (`tests/test_calidad.py`) se saltan sin la mesa de
  ayuda; corren en Odoo.sh. Las de lotes y certificado corren en el CI.

Quién lo opera (Dirección, 2026-11-23): los reportes los atiende el Jefe de
Calidad con el Ingeniero de Calidad de apoyo, como miembros del equipo
«Reclamaciones de clientes»; el certificado lo adjunta en la entrega la
Coordinación de Laboratorio y Materia Prima. Los miembros se dan de alta en la
base, no en código.

## Conexión con Odoo

- **CRM**: el formulario usa `/website/form/crm.lead`. La oportunidad toma el
  equipo de ventas configurado en el sitio web (`crm_default_team_id`).
  Mercado, tipo de solicitud y volumen llegan en la descripción.
- **Productos**: las familias de entretelas listan los productos con
  «Publicado». Gramaje, composición, color y tacto se leen del
  nombre del producto (`ficha_producto` en `controllers/main.py`); lo que no
  se encuentra no se muestra. La foto es la del producto; si no tiene, se
  dibuja una muestra de tela.
- **Buscador ⌘K**: `/catalogo/buscar.json` (artículos, familias, mercados y páginas).
- **Reclutamiento, portal y Helpdesk**: se enlazan tal cual.

## Catálogo por familias

Los productos publicados son solo entretelas y lo automotriz (la mayor parte
de la venta) son telas hechas para un cliente, que no se publican una por
una. Por eso el catálogo va por **familias** escritas en
`controllers/familias.py`: scrims de tejido circular, scrims no tejidos,
entretelas tejidas, no tejidas y tramadas, no tejidos no fusionables y telas
de punto. Cada una trae rangos de peso, ancho y color (de lo vendido en los
últimos 12 meses; sin precios ni parámetros de proceso) y un formulario para
pedir la ficha técnica.

Las familias de entretelas listan además los artículos publicados en Odoo,
clasificados **por su nombre** (`familia_de`), no por la categoría del sitio.
Un producto publicado cuyo nombre no cae en ninguna familia no se muestra.
`/catalogo/<categoría>` (dirección vieja) redirige a `/catalogo`.

## Origen de las oportunidades y medición

- Cada formulario manda un campo oculto `qb_origen`; la oportunidad queda con
  la etiqueta de CRM de su origen (`controllers/formulario.py`): «Web ·
  Cotización», «Web · Diseñador», «Web · Mercado», «Web · Compradores
  automotrices», «Web · Ficha técnica», «Web · Llamada», «Web · Certificados».
  Fuente y campaña (`utm_source`…) las pone Odoo desde la visita.
- Las páginas del módulo llevan `track="1"`: quedan en el historial del
  visitante (Sitio web → Reportes → Visitantes).
- Si el sitio tiene Google Analytics, el script manda `generate_lead` al
  llegar a la página de gracias (con el origen) y los clics en teléfono,
  correo y «Agendar llamada».
- **Resumen mensual** (`models/resumen.py`, cron el día 1): oportunidades por
  origen, visitantes nuevos y páginas más vistas del mes anterior contra el
  previo, por correo a «Correos del resumen mensual» del sitio. Sin correos,
  no se manda.
- Search Console: se pega el código de verificación en Sitio web →
  Configuración (campo nativo de Odoo); el módulo no hace nada ahí.

## Certificados y citas

Desde 1.15.1 los certificados ISO 9001 y 14001, y desde 1.15.2 el de ISO
45001, van en el módulo (`static/src/doc`) y cada tarjeta de
`/compradores-automotrices` trae su botón de descarga (`pdf` en
`CERTIFICACIONES` de `controllers/prospectos.py`). Al renovarse un certificado
se cambia el archivo y su número y vigencia ahí. El formulario para pedirlos
aparece solo si a alguno le falta el PDF.

En la ficha del sitio web: **Certificados (PDF)** (los adjuntos se descargan
desde `/compradores-automotrices` por `/calidad/certificado/<id>`; sin
adjuntos, la página ofrece pedirlos por formulario) y **Liga para agendar**
(por ejemplo la de la app Citas; vacía, `/agendar` muestra su propio
formulario, que crea una oportunidad «Web · Llamada»). Las llamadas se asignan
al vendedor de **Atiende las llamadas**; las del mercado de confección, al de
**Atiende las llamadas de confección**.

## Dibujos de tejido (desde 1.16.0)

`/planta#dibujos` anima cómo se forma cada dibujo de punto, con los nombres
del catálogo de la planta: once de una fontura (jersey, listado, vanizado,
piqué y crepé de cargas y de anulado, french terry, flece, desagujado y
jacquard) y cuatro de doble fontura (rib, interlock, punto de Roma y
cardigan). Nombres y textos en `DIBUJOS` de `controllers/contenido.py`; el
ligamento de cada uno (malla `K`, carga `T`, anulado `M`, aguja retirada `X`
por aguja y pasada) en `DIBUJOS` de `static/src/js/quimibond.js`, con la misma
clave. Un dibujo nuevo va en los dos lados y con su inglés en el `.po`.

## Acabados (desde 1.17.0)

`/planta#acabados` anima el efecto de cada acabado sobre la tela (no la
máquina), con los nombres del catálogo de acabados de la planta: nueve que se
dan en rama (termofijado, ignífugo, repelente, antiestático, antibacterial,
secado rápido, tacto suave, tacto rígido y resina acrílica) y tres de
superficie (afelpado, esmerilado y biopulido). «Secado rápido» es el «Dryfit»
del catálogo: no se publica con ese nombre porque es marca de un tercero.
Nombres y textos en `ACABADOS` de `controllers/contenido.py`; el dibujo de
cada uno en `ACABADOS` de `static/src/js/quimibond.js`, con la misma clave.
Sin químicos, temperaturas ni concentraciones: eso es receta.

## Versión en inglés

El módulo está escrito en español. `en_US` es el idioma de origen de Odoo y
no admite traducción, así que el inglés del sitio es **`en_CA` con la
dirección `/en`**; los textos van en `i18n/en_CA.po`.

Se enciende por sitio con la casilla **Versión en inglés** de la ficha del
sitio web (`website.qb_ingles`, desde 1.14.2), que llama a
`_qb_activar_ingles()` (activa `en_CA`, le da la
dirección `/en`, carga las traducciones y lo pone en el sitio en lugar de
`en_US`). El selector ES / EN aparece solo cuando el sitio tiene más de un
idioma y pasa por `/website/lang/<código>` para que el regreso al español no
rebote a `/en`.

- Plantillas: Odoo las traduce solo.
- Textos de `contenido.py`, `familias.py`, `prospectos.py` y de los
  controladores: pasan por `idioma.traducir` (`controllers/idioma.py`).
- Textos del script: `TEXTOS_JS` en `idioma.py`; el script los busca con
  `T("…")`.
- Un texto nuevo va en español en el código y con su inglés en el `.po`
  (los de Python, con el comentario `#. odoo-python`). La prueba
  `test_traducciones_completas` avisa si falta alguno. Lo que no tiene
  traducción sale en español; nunca falla la página.
- Los nombres de los productos se traducen en Odoo, en el producto.

## Armadoras (OEM)

Sitio web → Configuración → Sitios web → **Armadoras (OEM) que se muestran**,
una marca por línea. Salen con su logotipo en la portada y en
`/mercados/automotriz` (SVG en `static/src/img/armadoras/`, de Simple Icons,
CC0; el renglón de cada marca está en `LOGOS_ARMADORAS` de
`controllers/contenido.py`). Una marca sin archivo sale con su nombre: hoy
Stellantis y Mercedes-Benz, que no están en ese juego de iconos; se agrega su
SVG oficial a la carpeta y su renglón. Son las armadoras finales, nunca los clientes directos.
Vacío = la sección no aparece.

Al instalar se carga la lista que confirmó Dirección el 2026-11-19 (12 marcas)
en los sitios que no tengan una.

## Mercados

De lo general a lo particular (Dirección, 2026-11-19): la portada presenta
primero los mercados, cada uno con sus aplicaciones; `/mercados/<clave>` va del
mercado a las aplicaciones y de cada aplicación a la tela propuesta, con enlace
al diseñador (`/disenar?ap=<aplicación>`). Las aplicaciones y su tela de partida
salen de `DISENADOR['aplicaciones']` (`contenido.aplicaciones`).

## Maquinaria y tipos de tela

`/planta` (y la portada) muestran los tipos de tela en dos familias, tejido
(una y doble fontura) y no tejido (sencillo y tramado: el tramado es un no
tejido, Dirección 2026-11-19), y la maquinaria por área. Los textos están
en `controllers/contenido.py` (`TEJIDOS`, `PRESENTACIONES`, `MAQUINARIA`) y
salieron de los centros de trabajo de Odoo el 2026-11-19.

Con `quimibond_ficha_tecnica_tela` instalado, `controllers/planta.py` vuelve a
leer de Odoo, cada vez que se sirve la página, galgas, diámetros y salida de la
tela de las circulares (ficha vigente). Sin ese módulo, o si la lectura falla,
se sirve el texto fijo. Desde 1.7.0 no se publica cuántas máquinas hay, ni en
las cifras ni en las tarjetas de maquinaria (Dirección, 2026-11-19); el
encabezado y el pie traen «Iniciar sesión» (nombre o «Mi cuenta» con sesión), y
`/nosotros` muestra misión, visión, valores y razones tomados de la propuesta de
Calidad («PROPUESTA PAGINA WEB», 2026-11-19). La ISO 45001 se anunció como «en proceso» hasta 1.9.0, cuando Dirección confirmó
que ya está certificada.

Va en términos generales (Dirección, 2026-11-19): sin marcas de máquinas ni
nombres internos de línea, y las páginas van de lo general a lo particular.

No se publica lo que Odoo no tiene: cuántas circulares son de una fontura y
cuántas de doble. El catálogo
(`/catalogo`) sigue leyendo los productos publicados, pero ya no se anuncia en
la portada: los productos publicados se corrigen en un segundo paso.

Desde 1.8.0 `/planta` trae dos esquemas animados más, en canvas y sin
librerías: el jet de teñido (`jet`) y la rama (`rama`), junto a la máquina
circular. Y los textos generales (portada, pie, títulos para buscadores,
mercado automotriz, diseñador) nombran los **scrims**, el producto automotriz:
Dirección pidió no dejar de lado lo automotriz (2026-11-19).

Desde 1.8.0 los textos de proceso y maquinaria cuentan lo que recibe el cliente
(termofijado según especificación, acabados especiales, inspección al 100 % con
laboratorio) y no cómo se opera por dentro: sin galgas, diámetros, cargas,
relación de baño ni menciones a Odoo (Dirección, 2026-11-19).

Desde 1.8.0 ningún menú, botón ni pie enlaza a `/catalogo`: solo mostraba telas
de confección porque los productos publicados están mal (Dirección,
2026-11-19). La ruta sigue viva y se vuelve a enlazar cuando se corrijan.

## Diseñador de tela

`/disenar`: la persona elige mercado, aplicación, construcción, fibra, peso, ancho, color, tacto,
acabado y presentación, y ve la tela en un lienzo que cambia al momento (la
malla se hace más fina con la galga, que desde 1.7.1 no se pregunta: la pone la aplicación, el hilo engorda con el peso, el punto de
adhesivo aparece con «fusionable» y la tela ondea más o menos según el tacto).
El diseño se escribe solo en la «Especificación» del formulario de la misma
página, que crea la oportunidad en CRM como en `/cotizar`. Los hilos salen de
las categorías de «Materia Prima / Hilo» de Odoo que tienen productos
(poliéster, poliéster reciclado, mezcla poliéster y algodón, algodón, nylon,
viscosa; el elastano va como acompañante), con el filamento de poliéster
texturizado, stretch o microfibra; el no tejido ofrece las fibras de
«Materia Prima / Fibra». Nunca se muestran títulos, proveedores ni códigos. Las opciones están
en `DISENADOR` de `controllers/contenido.py`; son lo que el cliente pide, no
una promesa de fabricación: Ventas confirma la factibilidad.

Desde 1.4.0 el diseñador empieza por la aplicación (asiento, toldo, colchón,
cuello de camisa…): cada una propone una tela de partida (`aplicaciones` en
`DISENADOR`) que la persona ajusta, y fija el mercado del formulario. Además
pide requisitos (solidez, pilling, flamabilidad…), norma, volumen y fecha;
guarda el diseño en la dirección de la página (parámetros cortos, sin datos
personales) para copiar el enlace y compartirlo; e imprime la ficha o la
guarda en PDF (hoja de estilos de impresión). El formulario acepta una foto o
ficha técnica de referencia. El formulario de Odoo se reinicia al cargar, por
eso el diseño se escribe también como valor por defecto de sus campos.

## Textos

Los mercados, los pasos del proceso, la historia y las cifras están en
`controllers/contenido.py`. Los títulos y párrafos de cada página están en
`views/pages.xml` y también se pueden editar desde el editor del sitio web.

## Animaciones

`static/src/js/quimibond.js`, sin librerías:

- Máquina de tejido circular (canvas): bobinas, alimentadores, agujas que
  suben con la leva y tela que baja en espiral. Acelera con el cursor.
- Fibras de no tejido con puntos de adhesivo (canvas).
- Aparición al hacer scroll, contadores, hilo de avance de lectura, hilo del
  proceso, foco que sigue al cursor en las tarjetas.

Todo se detiene con «reducir movimiento» del sistema y los lienzos solo
corren cuando están a la vista.

## Pruebas

`tests/test_website_quimibond.py` (corre en CI): con la casilla apagada el
sitio no cambia; encendida, todas las páginas responden, el catálogo muestra
solo lo publicado y el formulario apunta a `crm.lead`.

La tipografía Geist (SIL Open Font License) va en `static/fonts/`.

Desde 1.9.0 (Dirección, 2026-11-19: «esto es nuestra receta») `/planta` no
publica galgas, diámetros, salida de la tela, cargas ni pasos de proceso. Cada
tarjeta dice qué se puede hacer (tejidos, fibras que se tiñen y se acaban,
productos de no tejido) y, en tintorería y ramas, cuántas máquinas hay: el
número se lee de los centros de trabajo de Odoo y, si no se puede, vale el de
`contenido.MAQUINARIA` (5 jets y 3 ramas). Las fibras salen de la presentación
de empresa de octubre de 2026. El teléfono publicado es `contenido.TELEFONO`
(el de las firmas de Ventas), no el de la ficha de la compañía, que trae otro
número. En el mercado automotriz se explica para qué sirve el scrim (soporte
que se lamina con espuma y tela, vinil o piel de las vestiduras). ISO 45001:
certificada (lo confirmó Dirección el 2026-11-19), se muestra igual que las otras dos. El
diseñador pregunta «Composición» y ya no el tipo de filamento.

Desde 1.10.0 `/calidad` muestra los paneles solares y la planta tratadora de
agua (Dirección, 2026-11-19), con las cifras de la presentación de empresa de
octubre de 2026: 2,081 paneles, 3.7 GWh desde 2021 y tratadora desde 2025.
También entran a la historia de `/nosotros`, junto con la ISO 45001.

Desde 1.11.0 el encabezado y el pie llevan el logotipo nuevo (monograma QB,
vector trazado sobre el original de mayo de 2026, en `views/components.xml`;
ya no se usa el logotipo cargado en el sitio web) y la portada abre con una
entrada de dos segundos en la que el logotipo se dibuja con un solo trazo.
Es solo CSS, no bloquea clics ni desplazamiento, sale una vez por visita
(`sessionStorage`) y no aparece con «reducir movimiento».
