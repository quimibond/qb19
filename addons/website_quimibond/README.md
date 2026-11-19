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
una marca por línea. Salen como texto, sin logotipos, en la portada y en
`/mercados/automotriz`. Son las armadoras finales, nunca los clientes directos.
Vacío = la sección no aparece.

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
