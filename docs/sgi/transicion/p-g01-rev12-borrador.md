<!-- P-G01 Rev. 12 (borrador del 13-oct-2026). Reescritura del procedimiento
«Elaboración y control de información documentada» (Rev. 11, jul-2026, 13
páginas en el Dropbox) para el SGI que opera en Odoo. Mismo orden de secciones
que la Rev. 11. Lo que ya hace Odoo se describe como lo hace Odoo; lo que
sigue siendo una regla para las personas se conserva. Pendientes de decisión
de Dirección: sección 10. Se carga como revisión nueva del documento 3565 con
una solicitud de cambio documental (sección 4.4). -->

# P-G01 — Elaboración y control de información documentada (Rev. 12, borrador)

| Dato | Valor |
| --- | --- |
| Organización | Productora de No Tejidos Quimibond S.A. de C.V. (PNTQ) |
| Clave | P-G01 (clave anterior; el procedimiento vive en el proceso **E2 Gestión del SGI**) |
| Fecha de emisión | Noviembre de 2022 |
| Área | SGI |
| Revisión | 12, en borrador. Sustituye a la revisión 11 de julio de 2026 |
| Responsable del documento | Jefe de MAST y SGI (dueño del proceso E2) |
| Aprueba | Director de Operaciones |
| Marco normativo | ISO 9001:2015, ISO 14001:2015, ISO 45001:2018 (cláusula 7.5) |

**Qué cambia frente a la revisión 11.** La revisión 11 describía cómo se
elaboraban archivos en Word y Excel, cómo se codificaban, cómo se pedía un
cambio con un formato firmado, el calendario mensual de cambios y las listas
de control y distribución. En la revisión 12 la información documentada del
SGI **se crea, aprueba, publica, difunde y controla en Odoo**: los
procedimientos de proceso se generan desde el sistema, los instructivos se
redactan en Conocimiento, los formatos son pantallas de Odoo que imprimen su
clave y revisión, los cambios se aprueban firmando en Firma electrónica, la
difusión se demuestra con acuses de lectura y la lista maestra sale sola. El
Dropbox queda como archivo histórico de solo lectura. La correspondencia
cláusula por cláusula está en la sección 9.

## 1. Propósito

Establecer cómo se identifica, elabora, codifica, aprueba, publica, difunde,
modifica, conserva y controla la información documentada del Sistema de
Gestión Integral (SGI) de PNTQ en Odoo, para que el personal trabaje siempre
con la versión vigente y la evidencia de cada registro se conserve y pueda
auditarse.

## 2. Alcance

Aplica a toda la información documentada del SGI generada o recibida en
PNTQ, en sus tres formas:

| Forma | Qué es | Dónde vive |
| --- | --- | --- |
| **Documentos controlados** | Manual (MIID), procedimientos de proceso, instructivos, controles operacionales, protocolos, DAT, métodos y anexos, descripciones de puesto, «Mi procedimiento» de cada puesto, reglamentos, anexos y documentos externos | **Documentos → carpeta SGI** (ficha con clave, revisión, estado, proceso y responsable) |
| **Registros** | La evidencia de que las actividades se hicieron: órdenes, recepciones, inspecciones, NC, checklists, mediciones, actas, permisos, responsivas… | Los registros de Odoo; cada uno imprime la clave y la revisión del formato que sustituye |
| **Registros en papel** | Lo que todavía se llena a mano (bitácoras de piso, formatos no migrados) | Bajo control del jefe del área, en el lugar que define su proceso |

Las áreas involucradas son todas las del organigrama de PNTQ en Odoo
(**Empleados → Organigrama**), que sustituye al Anexo 15.

## 3. Responsabilidades de las áreas

| Quién | Responsabilidad |
| --- | --- |
| **Jefe de MAST y SGI** (dueño del proceso E2) | Mantener este procedimiento y el MIID. Aprobar cada solicitud de cambio documental firmando como «Aprobó» y responderla en 5 días hábiles (E2.01). Publicar la revisión vigente, dejar obsoleta la anterior y avisar a las áreas en 2 días hábiles (E2.02). Revisar cada mes la vigencia de los documentos (E2.03). Registrar los documentos externos (E2.36). Mantener los tipos de documento y sus patrones de clave. Revisar y difundir el boletín NEWS mensual. Publicar «Mi procedimiento» de cada puesto. |
| **Director de Operaciones** | Aprobar este procedimiento, el MIID (firma «Aprobó» del MIID) y los procedimientos de proceso que Dirección decida aprobar. Autorizar la entrega de documentos a personas o entidades externas (sección 4.8.5). |
| **Dueño de proceso** | Responder por los documentos de su proceso (es su «Responsable SGI»). Firmar como «Revisó» cada cambio documental de su proceso. Redactar o delegar la redacción de instructivos, controles operacionales y protocolos en Conocimiento y pedir su publicación. Implantar los documentos externos de su proceso en 10 días hábiles. Mantener al día el cuerpo de su procedimiento (objetivo, alcance, responsabilidades, actividades). |
| **Responsable de un documento** (quien lo elabora) | Solicitar el alta, la modificación o la baja con la solicitud de cambio documental y el archivo propuesto, firmando como «Elaboró». Redactarlo de forma que refleje fielmente las actividades. Implantar el cambio aprobado en 15 días hábiles. Revisar los documentos relacionados con el cambio. |
| **Jefe de área y supervisor** | Asegurar que su gente trabaja con la versión vigente de Odoo, que firma sus acuses de lectura y que los registros en papel se conservan legibles y ordenados. Verificar en piso la implantación de los cambios de su área. |
| **Todo el personal** | Consultar los documentos en Odoo (**SGI → Inicio → Documentos vigentes**), no en copias impresas ni en el Dropbox. Firmar «Leído y entendido» de cada documento que le aplica dentro de 7 días hábiles. Registrar su trabajo en la pantalla de Odoo que corresponde, nunca en formatos que ya se migraron. |
| **Administrador SGI** | Dar de alta o modificar los tipos de documento y sus patrones de clave (**SGI → Administración → Configuración → Tipos de documento**). No crea ni aprueba documentos. |

Las firmas del documento se registran en Odoo (Firma electrónica) con fecha
y hora; ya no se firma en el documento original en papel.

## 4. Desarrollo

### 4.1 Estructura de la información documentada

**Procedimiento de proceso (PR-{proceso}).** No se redacta en Word: Odoo lo
genera desde los datos del proceso (**SGI → Sistema → Mapa de procesos →
proceso → Imprimir procedimiento**) con esta estructura:

1. Objetivo
2. Alcance
3. Responsabilidades de las áreas
4. Desarrollo: una tabla por actividad con **qué** se hace, **quién** (puesto
   que ejecuta, aprueba o escala), **dónde** (pantalla de Odoo o lugar),
   **cómo** (pasos e instructivo), **cuándo** (cadencia y vencimiento),
   **criterio de terminado** y **qué hacer si no se puede cumplir**
5. Descripción de aspectos ambientales
6. Identificación de peligros y evaluación de riesgos y oportunidades (los
   riesgos e IPER ligados al proceso)
7. Marco normativo (normas y cláusulas ligadas)
8. Información documentada (la lista maestra del proceso: formatos,
   instructivos, controles y anexos vigentes)

**Instructivo de trabajo (IT), control operacional (CO), protocolo (PROT) y
reglamento (R).** Se redactan en **Conocimiento → SGI → proceso** y se
publican desde ahí (sección 4.3.2). La estructura recomendada del
instructivo se conserva: actividades preparatorias, iniciales, principales y
finales, más aspectos ambientales, peligros y riesgos, marco normativo e
información documentada. Un control operacional lleva alcance, controles,
responsables y qué hacer en desviación.

**Formato (F).** Es la pantalla de Odoo donde se registra la actividad
(«Formulario de Odoo»); su clave y revisión las imprime el PDF del registro
en el pie de cada hoja. Los formatos que siguen como archivo (Excel o Word
no migrados) conservan la estructura libre de la revisión 11, con el
encabezado de la sección 4.3.4.

**Manual (MIID).** Se genera desde Odoo (**SGI → Sistema → Manual del SGI
(MIID)**): secciones de texto fijo que edita el Jefe de MAST y SGI más los
bloques vivos que salen del sistema (procesos, tipos de documento, controles,
anexos, correspondencia con los procedimientos anteriores).

### 4.2 Codificación de documentos

La clave la asigna Odoo según el **tipo de documento** y el proceso. Los
tipos y sus patrones viven como datos en **Tipos de documento**; agregar o
cambiar un tipo no requiere programar.

| Tipo | Patrón de clave | Ejemplo |
| --- | --- | --- |
| Manual | MIID | MIID |
| Procedimiento de proceso | PR-{proceso} | PR-E2 |
| Instructivo de trabajo | IT-{proceso}-{nn} | IT-C4-03 |
| Formato (y formato de instructivo) | F-{proceso}-{nn} | F-E2-16 |
| Control operacional | CO-{proceso}-{nn} | CO-E2-04 |
| Método o anexo de proceso | MA-{proceso}-{nn} | MA-C5-01 |
| DAT (dato integral) | DA-{proceso}-{nn} | DA-C1-02 |
| Protocolo | PROT-{proceso}-{nn} | PROT-E2-01 |
| Descripción de puesto | DP-{nn} | DP-14 |
| Mi procedimiento (por puesto) | MP-{nnn} | MP-031 |
| Diagrama de flujo, reglamento, anexo | Conservan su clave | DF-P10-01, R-01, ANEXO 08 |
| Documento externo | Sin clave propia; se identifica por emisor y revisión del emisor | NOM-017-STPS-2008 |

Los procesos son los 14 del mapa: **E1** Dirección y planeación, **E2**
Gestión del SGI, **C1** Desarrollo y alta de producto, **C2** Pedido a
entrega, **C3** Planeación y programación, **C4** Producción, **C5** Calidad
de producto, **C6** Almacén e inventarios, **S1** Compra a pago, **S2**
Facturación, crédito y cobranza, **S3** Contabilidad, costos y cierre, **S4**
Recursos Humanos y nómina, **S5** Mantenimiento e instalaciones y **S6**
Tecnología. El consecutivo {nn} lo da Odoo por proceso y tipo.

Reglas:

- Odoo valida la clave contra el patrón de su tipo; un documento controlado
  no se guarda sin clave válida.
- **La clave anterior** del Dropbox (P-A14, F-IT-P-P01-08-01, DAT P-C06-01…)
  se conserva en el documento como «Clave anterior» y se busca en **SGI →
  Sistema → Del Dropbox a Odoo**; no aparece en las pantallas del día a día.
- Los formatos que nacen de una Norma Oficial Mexicana llevan la clave normal
  de formato y citan la NOM en el título (por ejemplo «F-E2-21 Registro de
  capacitación NOM-030-STPS»); la clave ya no incorpora «NOM-{número}».
- La clave y la revisión vigentes se imprimen en cada hoja del PDF que
  genera Odoo (procedimiento, registro, lista maestra, MIID).

### 4.3 Elaboración de información documentada

#### 4.3.1 Quién elabora qué y dónde

| Documento | Quién lo elabora | Dónde | Cómo queda controlado |
| --- | --- | --- | --- |
| Procedimiento de proceso | Dueño del proceso, con las propuestas de cambio de su gente | Datos del proceso en Odoo (objetivo, alcance, responsabilidades, actividades) | Odoo genera el PDF; el Jefe de MAST y SGI publica la revisión (sección 4.4.3) |
| Mi procedimiento (por puesto) | Odoo, a partir de los roles del puesto | **SGI → Administración → Publicar Mi procedimiento** | Cada publicación es una revisión con acuse de cada persona |
| Instructivo, control operacional, protocolo, reglamento | Dueño del proceso o quien designe | Artículo en **Conocimiento → SGI → proceso** | «Pedir publicación» → el Jefe de MAST y SGI publica: el artículo se congela como revisión nueva del documento |
| Formato | El dueño del proceso define qué se registra; Sistemas lo construye como pantalla de Odoo | Odoo | El formato de Odoo lleva la clave que sustituye; su PDF la imprime |
| DAT, método o anexo, anexo, descripción de puesto, archivo no migrado | Responsable del documento | Archivo (PDF) adjunto a la solicitud de cambio documental | Revisión publicada en Documentos al aprobarse |
| Manual (MIID) | Jefe de MAST y SGI | **SGI → Sistema → Manual del SGI (MIID)** | «Solicitar cambio del MIID» → firma de Dirección → revisión nueva |
| Documento externo | Lo emite el cliente, la autoridad, la norma o el proveedor | **SGI → Sistema → Documentos → Documentos externos** | Registro con emisor, revisión del emisor, fecha de recepción y proceso que lo aplica |

#### 4.3.2 Redacción en Conocimiento

1. El dueño del proceso abre el artículo del documento bajo su proceso en
   Conocimiento (los documentos del Dropbox ya importados están ahí como
   borrador con su PDF adjunto).
2. Redacta o corrige el texto. Mientras es borrador solo lo ven él y el Jefe
   de MAST y SGI.
3. Pulsa **Pedir publicación**. Al Jefe de MAST y SGI le llega la actividad.
4. El Jefe de MAST y SGI revisa y pulsa **Publicar**: Odoo congela el artículo
   como revisión nueva del documento (PDF con la misma clave y título), deja
   obsoleta la anterior, pide los acuses a los puestos que lo usan y
   re-apunta las actividades que lo citan. El artículo queda bloqueado y lo
   lee todo el personal.
5. Si alguien desbloquea y cambia un artículo publicado, Odoo lo marca
   «Cambió desde la publicación» y avisa cada día hasta que se publique otra
   vez.

#### 4.3.3 Plazos del personal de nuevo ingreso o de puesto nuevo

- Al entrar a un puesto, la persona recibe el acuse de **Mi procedimiento**
  del puesto y de los documentos que le aplican; lo firma en 7 días hábiles.
- Dentro de los **30 días naturales** siguientes revisa su procedimiento y
  propone lo que falte o sobre con **Proponer actividad** (desde Mi
  procedimiento) o con una solicitud de cambio documental. El jefe directo
  verifica que lo hizo.
- Las matrices que la revisión 11 pedía elaborar en 2 o 3 meses (aspectos
  ambientales, comunicación, partes interesadas, IPER, FODA y riesgos) ya no
  se elaboran por titular: son registros vivos de Odoo que se revisan con
  cada dueño en sus fechas (E2.10 en enero y julio, E2.23 cada año, E2.38
  cada año).

#### 4.3.4 Formato de los documentos que siguen como archivo

Para lo que Odoo genera (procedimientos, Mi procedimiento, registros, lista
maestra, MIID) el encabezado y el pie los pone el sistema con el layout del
SGI: logo y razón social, clave, revisión, fecha de emisión y «Página x de
y» en cada hoja. No hay nada que ajustar a mano.

Para los documentos que siguen como archivo (DAT, anexos, descripciones de
puesto, formatos no migrados) se conservan las reglas de la revisión 11:

- Encabezado: tabla de una fila y tres columnas con el logo vigente a la
  izquierda; en el centro la razón social y el nombre del documento en rojo
  (#C00000); a la derecha «Clave:», «Fecha de emisión:», «Área:» y
  «Rev. nn en Mmm-AAAA», con el dato en rojo. Relleno del encabezado:
  procedimientos #DAEEF3, instructivos #C2D69B, protocolos #C9C9C9.
- Títulos en Arial 12 negrita, subtítulos en Arial 10 negrita, contenido en
  Arial 10 con interlineado 1.15 y numeración «1., 1.1., 1.1.1.».
- Pie: en la primera página el bloque de tres columnas «Responsable del
  documento», «Aprueba» y «Vo.Bo. de Medio Ambiente, Seguridad en el Trabajo
  y SGI» con el puesto de cada uno; debajo, la clave del formato de plantilla
  con su revisión y el texto «ISO 9001:2015 / 14001:2015 / 45001:2018». Los
  espacios de firma ya no se firman a mano: la aprobación queda en Firma
  electrónica (sección 4.4).
- Nombre del archivo: la clave y el título completo, sin comas ni acentos
  («P-G01 ELABORACION Y CONTROL DE INFORMACION DOCUMENTADA»).
- El archivo se entrega en PDF. Quien lo exporta verifica que conserve
  formato, funciones y datos del original.

#### 4.3.5 Reglas generales

- Queda prohibido trabajar con información que no esté publicada como
  vigente en Odoo. Un formato con estado «Migrado a Odoo» ya no se llena en
  Excel ni en papel.
- Los registros que genera un procedimiento o instructivo se citan en su
  sección «Información documentada» (Odoo la arma desde la lista maestra del
  proceso).
- La familia P-I01 (contiene credenciales de sistemas) no se importa a
  Conocimiento ni se lista en «Del Dropbox a Odoo»; la controla Sistemas.

### 4.4 Aprobación de la información documentada

#### 4.4.1 Quién aprueba

| Documento | Elaboró | Revisó | Aprobó |
| --- | --- | --- | --- |
| MIID | Jefe de MAST y SGI | — | Director de Operaciones (firma en Firma electrónica) |
| Este procedimiento | Jefe de MAST y SGI | — | Director de Operaciones |
| Procedimiento de proceso | Dueño del proceso («Responsable del documento») | — | Quien el proceso declare en «Aprueba» (por omisión el dueño del proceso); Vo.Bo. del Jefe de MAST y SGI |
| Instructivo, control operacional, protocolo, reglamento, DAT, formato, anexo | Quien solicita el cambio | Dueño del proceso | Jefe de MAST y SGI |
| Cambios a actividades y roles de un proceso | Quien propone | Jefe directo de quien propone | Dueño del proceso (propuesta de cambio en Aprobaciones) |

La aprobación del Director Estratégico mediante el Manifiesto de compromiso
(Anexo 08) sigue vigente como documento firmado.

#### 4.4.2 Solicitud de cambio documental (sustituye al F-P-G01-06)

1. Quien necesita un alta, modificación o baja crea la solicitud en
   **SGI → Sistema → Documentos → Solicitudes de cambio** (categoría
   «Modificación de documento SGI»): documento afectado, tipo de cambio,
   qué se modifica (formato o contenido), nueva revisión (Odoo la sugiere),
   motivo, descripción de cambios, procesos afectados y, si aplica, prueba
   piloto con sus fechas. Adjunta el archivo propuesto cuando el documento
   es un archivo.
2. Al enviarla, Odoo arma el PDF de la solicitud (los datos del cambio, el
   archivo propuesto y una hoja de firmas) y crea la solicitud de firma en
   orden: **Elaboró** (quien pide) → **Revisó** (dueño del proceso) →
   **Aprobó** (Jefe de MAST y SGI). Nadie aprueba con el botón de
   Aprobaciones: la aprobación es la firma. Rechazar sí se hace desde
   Aprobaciones, con motivo.
3. El Jefe de MAST y SGI responde cada solicitud (aprobada o rechazada con
   motivo) a más tardar **5 días hábiles** después de recibida (E2.01), y en
   la revisión verifica que el documento no contradiga a otro y que los
   documentos relacionados se actualicen en la misma solicitud o en otra
   ligada.
4. Con la última firma, Odoo aplica el cambio solo: publica la revisión
   nueva (documento nuevo si venía archivo; en el mismo registro si no), le
   pone fecha de emisión y próxima revisión a 2 años, deja **obsoleta** la
   anterior con fecha y motivo, avisa a los puestos del proceso y deja
   pendiente el acuse de quienes deben leerlo. El PDF firmado queda en la
   solicitud y en el documento como evidencia.
5. **Alta:** al aprobarse, al Jefe de MAST y SGI le llega la actividad de
   crear el documento con su clave nueva. **Baja:** el documento queda
   obsoleto con el motivo de la solicitud.
6. **Prueba piloto:** si la solicitud la pide, el documento queda en estado
   «Prueba piloto» hasta la fecha de fin (60 días por omisión); Odoo avisa
   7 días antes de que venza para confirmarlo vigente o corregirlo.

#### 4.4.3 Publicación del procedimiento de proceso

El procedimiento de proceso se publica cuando el proceso entra en vigor y
cada vez que su cuerpo cambia de forma aprobada. Odoo compara los datos
vivos del proceso con el PDF de la revisión vigente: si alguien edita el
objetivo, el alcance, las responsabilidades o las actividades fuera de una
solicitud aprobada, el documento queda marcado **«Procedimiento vivo
cambió: revisar»** y el Jefe de MAST y SGI recibe el aviso para publicar la
revisión nueva o revertir el cambio. Un procedimiento impreso es copia no
controlada; el vigente es el de Odoo.

### 4.5 Modificación de la información documentada

#### 4.5.1 Cuándo se modifica

La información documentada puede modificarse en cualquier momento por:

1. cambios en ISO 9001, ISO 14001 o ISO 45001;
2. decisión de la Dirección;
3. solicitud de un dueño de proceso o responsable de área, cuando su proceso
   cambió o mejoró, o cuando se detectaron anomalías;
4. cambio en un requisito legal o de cliente que afecte el proceso;
5. una observación o hallazgo de auditoría de primera, segunda o tercera
   parte, o una no conformidad;
6. haber pasado **2 años** desde la última revisión: Odoo avisa al
   responsable 60 y 30 días antes de la fecha de próxima revisión (E2.03
   revisa cada mes la lista de los que vencen);
7. un documento externo nuevo o con revisión nueva (sección 4.8.1);
8. el aviso diario de que **el MIID vigente ya no coincide con el sistema**
   o de que un **procedimiento vivo cambió**.

No es motivo de revisión el cambio de titular de un área: el procedimiento
es del proceso, no de la persona.

#### 4.5.2 Cómo se modifica

Toda modificación pasa por la solicitud de cambio documental de la sección
4.4.2 o, para los artículos de Conocimiento, por «Pedir publicación»
(sección 4.3.2); para los cambios de actividades y roles, por la propuesta
de cambio de Mi procedimiento. Odoo sube el número de revisión y la fecha de
emisión al publicar; no se capturan a mano.

El calendario mensual de la revisión 11 (recepción hasta el día 15, carga en
5 días hábiles, validación en 2 y revalidación en 2) y la suspensión por
días de auditoría dejan de aplicar: las solicitudes se atienden conforme
llegan, con los plazos de E2.01 (5 días hábiles para responder) y E2.02
(2 días hábiles para publicar). La validación del solicitante es la propia
revisión nueva publicada, que él mismo firmó.

Al modificar un documento se revisan los que se relacionan con el motivo del
cambio (los que lo citan, los formatos que genera, el procedimiento del
proceso) y se actualizan en la misma solicitud o en otra ligada; E2.01 lo
verifica antes de aprobar.

### 4.6 Implantación de los cambios

1. El responsable del documento implanta el cambio aprobado en **15 días
   hábiles**.
2. El Jefe de MAST y SGI publica la revisión en Odoo y avisa a las áreas
   que usan el documento a más tardar **2 días hábiles** después de aprobado
   (E2.02); Odoo genera los acuses de lectura para los puestos a los que
   aplica el documento.
3. Cada persona firma **«Leído y entendido»** en Odoo (Mis pendientes,
   Documentos vigentes o la tableta de su área con su PIN) en **7 días
   hábiles**; después, Odoo avisa a la persona y a su jefe.
4. La verificación de implantación con jefes y supervisores (antes
   F-P-G01-08) se registra en la revisión de E2.02: a los 15 días hábiles el
   Jefe de MAST y SGI confirma con el jefe del área que el cambio se aplica en
   piso y deja la nota en el historial del documento; lo que no se aplica se
   levanta como no conformidad.
5. La evidencia de difusión es el porcentaje de acuses del documento y, para
   el personal administrativo, el boletín NEWS mensual (sección 4.8.4).

### 4.7 Almacenamiento y resguardo de la información documentada

- **Electrónico.** Toda la información documentada del SGI se controla y
  almacena en Odoo (Documentos → carpeta SGI y los registros de cada app).
  Odoo.sh conserva respaldos diarios de la base y los archivos; Sistemas
  responde por ellos.
- **Nada se borra.** Un documento controlado que deja de usarse queda
  **obsoleto** con fecha y motivo, no se elimina; el obsoleto conserva su
  archivo y su historial. Un registro de Odoo se archiva o se cancela con
  motivo; lo cerrado solo lo modifica el Jefe de MAST y SGI.
- **Retención y disposición final.** Cada documento o familia declara en su
  ficha los años de retención después de quedar obsoleto o cerrado y su
  disposición final (archivo muerto, destrucción controlada o devolución al
  cliente). Los clientes automotrices pueden exigir «vida del programa más
  n años»: se captura por documento.
- **Registros físicos.** Los que todavía se llenan en papel se conservan
  bajo el control de jefes, coordinadores o supervisores, en lugar
  accesible, protegidos de daño y deterioro y en orden cronológico. Al
  terminar su plazo de conservación pasan al archivo muerto que resguarda
  MAST y SGI; E2.03 incluye ese paso cada año. Los que una ley exija por más
  tiempo se conservan ese tiempo.
- **Historia.** Los documentos y registros anteriores a Odoo quedaron en el
  Dropbox, de solo lectura. El PDF de cada procedimiento anterior se abre
  desde **Del Dropbox a Odoo → Procedimientos anteriores**.
- Al publicar una revisión nueva, Odoo retira la anterior (obsoleta) y las
  copias impresas se retiran y se destruyen; no se archivan copias.

### 4.8 Control y distribución de la información documentada

#### 4.8.1 Información documentada externa (sustituye al F-P-G01-09)

Normas, NOM, especificaciones de cliente, manuales de proveedor y
disposiciones oficiales se registran en **SGI → Sistema → Documentos →
Documentos externos** con emisor, revisión del emisor, fecha de recepción y
proceso que lo aplica (E2.36). Desde la recepción hay **10 días hábiles**
para implantarlo; Odoo avisa al dueño del proceso 2 días hábiles antes y
cuando venció. Actualizar la versión del documento es responsabilidad del
emisor; registrar que llegó y aplicarlo, del proceso.

#### 4.8.2 Seguridad de la información

- La información que se recaba y consolida en registros (nombre, ocupación,
  actividades, domicilio, información económica, RFC, CURP, identificación,
  teléfonos, correo) se trata con absoluta confidencialidad, se almacena en
  Odoo con acceso restringido y se elimina una vez concluida la relación
  jurídica o comercial y transcurridos los plazos legales de resguardo.
- PNTQ protege los datos recibidos mediante los actos jurídicos necesarios
  y tecnologías que controlan el acceso, uso o divulgación: el acceso a cada
  registro de Odoo depende del grupo de cada usuario; los datos de salud y
  de salarios tienen acceso restringido; los servicios en la nube son los
  contratados por PNTQ (Odoo.sh).
- La información que genera PNTQ es propiedad de PNTQ. La que contiene
  cantidades monetarias (cotizaciones, facturas, nómina, remisiones) y la
  comercial sensible (actas constitutivas, licencias, permisos) se mantiene
  confidencial.

#### 4.8.3 Lista maestra (sustituye al F-P-G01-03)

La lista de control de la información documentada sale sola de Odoo:
**SGI → Sistema → Documentos → Lista maestra** (clave, título, tipo,
revisión, proceso, responsable, fecha de emisión, próxima revisión y
estado) y, por proceso, el PDF **Lista maestra de documentos** desde el
proceso. No se captura a mano.

#### 4.8.4 Difusión de cambios

- **Acuse de lectura.** Al publicar, Odoo crea el acuse de cada persona de
  los puestos a los que aplica el documento. Es la evidencia individual de
  difusión (sustituye a la lista de asistencia para cambios documentales).
- **NEWS Quimibond (F-P-G01-16).** El primer día de cada mes, si en el mes
  anterior hubo cambios documentales aplicados, Odoo genera el boletín NEWS
  con esos cambios y agenda al Jefe de MAST y SGI su revisión y envío por
  correo al personal administrativo, dentro de los 7 días siguientes.
- **Reunión presencial.** Cuando el cambio lo amerite (control operacional,
  instructivo de piso), el dueño del proceso lo explica en reunión y la
  registra con minuta y lista de asistencia (F-P-A10-01 y F-P-A10-09), y
  cada asistente firma su acuse en la tableta o en Odoo.

#### 4.8.5 Distribución

- El personal consulta los documentos en **SGI → Inicio → Documentos
  vigentes** (título, revisión y su acuse en el mismo renglón) o en
  **Conocimiento → SGI**. No se entregan copias controladas; las listas de
  distribución (F-P-G01-05) y el sello «Documento controlado No.» quedan sin
  efecto.
- Un documento impreso desde Odoo es **copia no controlada**: el PDF del
  procedimiento lo dice en su pie y no lleva imagen de firma. Sirve para
  consulta en el momento; no demuestra cumplimiento.
- **Entrega a externos.** Un documento del SGI se entrega a personas o
  entidades externas (clientes, auditores, proveedores) solo con
  autorización del Director de Operaciones, marcado «COPIA NO CONTROLADA»,
  vigente únicamente al momento de su emisión y sin registro en la lista de
  distribución. La autorización y la entrega se anotan en el historial del
  documento en Odoo (quién lo pidió, quién autorizó, a quién se entregó y
  cuándo). Ver la decisión pendiente 10.2.
- **El control existe aunque no se registre cada entrega:** el documento
  tiene identificación y versión vigente, tiene responsable de su
  actualización y hay un mecanismo para autorizar cambios.

## 5. Información ambiental

No aplica.

## 6. Identificación de peligros y evaluación de riesgos y oportunidades

No aplica.

## 7. Seguridad patrimonial

No aplica, salvo lo dicho en 4.8.2 sobre confidencialidad y acceso.

## 8. Marco normativo

- ISO 9001:2015, cláusula 7.5 Información documentada.
- ISO 14001:2015, cláusula 7.5.
- ISO 45001:2018, cláusula 7.5.

## 9. Información documentada y correspondencia con la revisión 11

### 9.1 Formatos y anexos de la revisión 11 y su sustituto

| Clave (Rev. 11) | Documento | En la revisión 12 |
| --- | --- | --- |
| ANEXO 15 | Estructura organizacional de PNTQ | Organigrama de Empleados en Odoo |
| ANEXO 08 | Manifiesto de compromiso | Se conserva como documento firmado |
| F-P-A04-01 | Matriz de comunicación | Registro en Odoo (Partes interesadas y comunicación); revisión anual E2.38 |
| F-P-A04-02 | Matriz de partes interesadas | SGI → Planeación → Partes interesadas |
| F-P-A10-01 | Minuta | Se conserva para difusión presencial |
| F-P-A10-09 | Lista de asistencia | Se conserva para difusión presencial; el acuse de lectura es la evidencia individual |
| F-P-C09-01 | Matriz FODA | SGI → Planeación → Riesgos y oportunidades (contexto) |
| F-P-C09-02 | Matriz de riesgos y oportunidades | SGI → Planeación → Riesgos y oportunidades |
| F-P-E01-01 | Matriz de aspectos ambientales | SGI → Planeación → Aspectos ambientales |
| F-P-G01-02 (F-E2-12) | Hoja para procedimientos e instructivos | Layout del SGI que imprime Odoo; plantilla solo para archivos (4.3.4) |
| F-P-G01-03 (F-E2-13) | Lista para el control de información documentada | Lista maestra de Odoo (4.8.3) |
| F-P-G01-03 | Diagrama de flujo | Diagramas del proceso en Odoo (flujo, interacción, tortuga) |
| F-P-G01-05 (F-E2-14) | Lista de distribución de documentos | Baja: no hay copias controladas (4.8.5) |
| F-P-G01-06 | Solicitud de creación y modificación de documentación | Solicitud de cambio documental en Aprobaciones, firmada en Firma electrónica (4.4.2) |
| F-P-G01-08 (F-E2-15) | Verificación de implantación | Nota de verificación de E2.02 en el historial del documento y acuses (4.6) |
| F-P-G01-09 (F-E2-16) | Inventario de documentos externos | Documentos externos en Odoo (4.8.1) |
| F-P-G01-16 | NEWS Quimibond | Boletín NEWS generado por Odoo cada mes (4.8.4) |
| F-P-S01-01 | Matriz IPER | SGI → Planeación → Riesgos y oportunidades, instrumento IPER |
| MID | Manual de información documentada | MIID generado desde Odoo |

### 9.2 Correspondencia cláusula por cláusula

| Rev. 11 | Tema | Rev. 12 | Qué lo cubre en Odoo |
| --- | --- | --- | --- |
| 3.1–3.7 | Responsabilidades | 3 | Roles de E2 (E2.01, E2.02, E2.03, E2.36), dueño de proceso como Responsable SGI, firmas Elaboró / Revisó / Aprobó |
| 4.1 | Estructura de procedimientos, instructivos y protocolos | 4.1 | Procedimiento generado desde el proceso; instructivos en Conocimiento |
| 4.2 | Codificación | 4.2 | Tipos de documento con patrón de clave; clave anterior en «Del Dropbox a Odoo» |
| 4.2.4 | Formatos de NOM | 4.2 | La NOM va en el título, no en la clave |
| 4.3.1 | Nombre de archivo | 4.3.4 | Solo para archivos; lo generado lo nombra Odoo |
| 4.3.2 | Plazos del nuevo titular (30 días, 2 y 3 meses) | 4.3.3 | Acuse de Mi procedimiento en 7 días hábiles; 30 días para proponer; matrices vivas en Odoo |
| 4.3.3–4.3.5 | Plantilla F-P-G01-02, ligas a formatos, prohibición | 4.1, 4.3.5 | Lista maestra del proceso en la sección 8 del procedimiento |
| 4.3.6–4.3.7 | Tipografía, encabezado, pie, colores | 4.3.4 | Layout del SGI; reglas solo para archivos |
| 4.4 | Aprobación | 4.4 | Firma en Firma electrónica; E2.01 en 5 días hábiles |
| 4.5.1–4.5.2 | Redistribución, solicitud F-P-G01-06, envío con 3 firmas y archivos | 4.4.2 | La solicitud lleva el archivo y las tres firmas; Odoo aplica el cambio |
| 4.5.3 | Calendario de 4 periodos y días de auditoría | 4.5.2 | Retirado: solicitudes continuas con plazos de E2.01 y E2.02 |
| 4.5.4–4.5.7 | Cambio de titular, motivos, documentos relacionados, número de revisión | 4.5.1, 4.5.2 | Avisos de Odoo (2 años, procedimiento vivo, MIID, externos); revisión automática |
| 4.6 | Implantación en 15 días hábiles y F-P-G01-08 | 4.6 | E2.02 verifica a los 15 días hábiles; acuses de lectura |
| 4.7 | Almacenamiento, archivo muerto, copias físicas | 4.7 | Obsoleto con motivo, retención y disposición, respaldos, Dropbox histórico |
| 4.8.1 | Documentos externos F-P-G01-09 | 4.8.1 | Documentos externos, 10 días hábiles, E2.36 |
| 4.8.2 | Seguridad de la información | 4.8.2 | Se conserva; acceso por grupos de Odoo |
| 4.8.3 | Registro de cambios F-P-G01-03 | 4.8.3 | Lista maestra |
| 4.8.4 | Difusión (reunión o NEWS) | 4.8.4 | Acuses, NEWS mensual automático, reunión cuando amerite |
| 4.8.5 | Copias controladas, no controladas, nota de control | 4.8.5 | Sin copias controladas; impresos = copia no controlada; entrega a externos autorizada |
| 5–8 | N/A y marco normativo | 5–8 | Sin cambio |
| 9 | Información documentada | 9.1 | Tabla de sustitutos |

### 9.3 Rutinas de la revisión 11 en «Rutina por rutina»

Las 16 rutinas de P-G01 en **Del Dropbox a Odoo → Rutina por rutina** quedan:
11 cubiertas por E2.01, E2.02, E2.03 y E2.36; 3 las hace Odoo (calendario
mensual, conversión a PDF, lista de control); 2 pendientes de decisión
(sección 10).

## 10. Pendiente antes de aprobar

<!-- NO CARGAR: lista de trabajo del borrador, no es parte del procedimiento. -->

- [ ] **10.1 Rutina 1 (nuevo titular, 30 días).** Confirmar la regla de 4.3.3
  (acuse en 7 días hábiles y 30 días naturales para proponer cambios, con
  verificación del jefe directo) y decidir si se registra como actividad de
  E2 o de S4 (ingreso de personal) para que Mis pendientes la cobre.
- [ ] **10.2 Rutina 16 (entrega a externos).** Confirmar la regla de 4.8.5
  (autoriza el Director de Operaciones; registro en el historial del
  documento) y decidir si se registra como actividad «por evento» de E2 con
  rol Aprueba = Director de Operaciones.
- [ ] **10.3 Claves con NOM.** Confirmar que la NOM va en el título y no en la
  clave (4.2); hoy ningún tipo de documento tiene patrón con NOM.
- [ ] **10.4 Días de auditoría.** Confirmar que se retira la suspensión del
  flujo de cambios durante auditorías (4.5.2).
- [ ] **10.5 Formatos F-E2-14 y F-E2-15.** Darlos de baja (lista de
  distribución y verificación de implantación) o conservarlos como archivo;
  hoy están «Migrado a Odoo» con destino «Plantilla de elaboración de
  documentos», que no corresponde.
- [ ] **10.6 Aprobación de este procedimiento.** Decidir si la revisión 12 se
  publica como revisión nueva del documento P-G01 (3565, hoy «En curso») con
  Dirección como «Aprobó», o si P-G01 queda obsoleto al publicar el proceso
  E2 y este texto pasa a la sección 7.5 del MIID y al artículo de E2 en
  Conocimiento. Recomendación: publicarlo como P-G01 Rev. 12 ahora (es lo que
  el auditor pedirá en 7.5) y darlo de baja cuando E2 esté en vigor y el MIID
  Rev. 03 aprobado.
- [ ] Revisión del Jefe de MAST y SGI y aprobación del Director de
  Operaciones; carga en Odoo con una solicitud de cambio documental y
  generación de acuses.
