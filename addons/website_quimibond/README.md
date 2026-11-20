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
| `/catalogo`, `/catalogo/<categoría>` | Catálogo técnico sin precios | `product.template` publicado + `product.public.category` |
| `/catalogo/producto/<producto>` | Ficha del producto | Nombre, código, unidad y categoría del producto |
| `/mercados/automotriz`, `/blancos`, `/confeccion` | Página por mercado | `controllers/contenido.py` |
| `/planta`, `/calidad`, `/nosotros` | Empresa | `controllers/contenido.py` + datos de la compañía |
| `/cotizar` | Solicitud de cotización o muestra | Crea `crm.lead` (formulario web de Odoo) |
| `/jobs`, `/my`, `/helpdesk` | Empleos, portal, reportes de calidad | Apps nativas, con el encabezado nuevo |

## Conexión con Odoo

- **CRM**: el formulario usa `/website/form/crm.lead`. La oportunidad toma el
  equipo de ventas configurado en el sitio web (`crm_default_team_id`).
  Mercado, tipo de solicitud y volumen llegan en la descripción.
- **Productos**: el catálogo muestra los productos con «Publicado» y con
  categoría del sitio web. Gramaje, composición, color y tacto se leen del
  nombre del producto (`ficha_producto` en `controllers/main.py`); lo que no
  se encuentra no se muestra. La foto es la del producto; si no tiene, se
  dibuja una muestra de tela.
- **Buscador ⌘K**: `/catalogo/buscar.json` (productos, mercados y páginas).
- **Reclutamiento, portal y Helpdesk**: se enlazan tal cual.

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
