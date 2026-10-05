# quimibond_sgi — Sistema de Gestión Integral de PNTQ

Addon de Odoo 19 Enterprise con el SGI de Productora de No Tejidos Quimibond
(empresa 1 de la instancia, decisión D-03). Convierte los procedimientos del
Dropbox en **procesos y actividades** con responsable por puesto, vencimiento,
entregable medible y escalamiento, y extiende las apps nativas (Documentos,
Calidad, Aprobaciones, Helpdesk, Proyecto, Mantenimiento, Encuestas, Firmas)
sin duplicarlas. Versión y cambios: `CHANGELOG.md` (una entrada por versión).

**Normas** (D-28; organismo SIDE Certificaciones, acreditado ante la ema):

| Norma | Estado |
|---|---|
| ISO 9001:2015 | Certificada desde el 24-ago-2021; certificado 24070237A vigente al 22-ago-2027 |
| ISO 14001:2015 | Certificada desde el 3-jul-2023; el certificado vigente lo confirma MAST |
| ISO 45001:2018 | En certificación (auditoría del 22 al 24-sep-2026, resultado pendiente) |
| IATF 16949 | No certificada. Lo que piden los clientes automotrices va en «Requisitos específicos de clientes» |

## Qué trae y qué no

- **Se instala vacío** (decisión 6): trae estructura (modelos, vistas, menús,
  grupos, secuencias, tipos de documento, áreas, normas ISO con sus cláusulas,
  catálogo de indicadores automáticos, crons y parámetros), **no** el mapa de
  procesos. El mapa de producción está en `quimibond_sgi_mapa` y solo se
  instala en staging o en una recuperación; su carga es manual, con modo de
  prueba primero.
- **Estructura contra datos** (decisión 4): procesos, actividades, roles y
  entregables son **dato** que se captura en pantalla o por carga
  (`sgi.process.load_payload`), nunca XML del núcleo. La norma «Requisitos
  específicos de clientes» y sus cláusulas CLI-01…CLI-10 existen solo en
  producción, sin XML ID; no se agregan por XML sin una migración que cree
  antes los XML IDs de los registros existentes (A-029). Las cláusulas de
  tercer nivel de ISO 9001, 14001 y 45001 (57.97.0) sí traen XML ID
  (`data/sgi_norms_tercer_nivel.xml`); las CLI siguen sin él.
- **Claves:** en pantalla solo la clave nueva (`PR-{proceso}`, `F-{proceso}-{nn}`,
  `IT-…`, `DA-…`; D-02). La clave del Dropbox solo aparece en «Del Dropbox a
  Odoo» (decisión 1).

## Glosario de pantallas

Así se escriben los términos del SGI en etiquetas, ayudas y avisos
(revisión de vistas V-B04, 57.81.0). Todo el texto va en «usted».

| Se escribe | No | Nota |
|---|---|---|
| no conformidad (NC); en plural, «no conformidades» o «NC» | No Conformidad, NCs | Mayúscula solo al inicio de un título o una oración |
| certificado de calidad (CoA) | COA, certificado de análisis | |
| indicador | KPI | También «indicadores en rojo», «indicadores automáticos» |
| casi accidente | casi-accidente | |
| Jefe MAST | Jefe de MAST | «MAST» solo, para el área |
| Revisión por la dirección | Revisión por la Dirección | «Dirección» con mayúscula es el grupo que aprueba |
| Sin sufijo «SGI» dentro de la app SGI | «Áreas SGI», «(SGI)» | En fichas de otras apps (contacto, empleado, equipo…), la pestaña se llama «SGI» |
| Descartar | Volver, Cancelar | Botón para salir de un asistente sin hacer nada |

## Colores

Un estado se pinta igual en todas las listas y pastillas del SGI
(auditoría I-06, 57.98.0). `tests/test_interfaz.py` lo revisa en las vistas
del módulo (las expresiones simples sobre campos de estado).

| Color | `decoration-` | Estados |
|---|---|---|
| Gris | `muted` | borrador, cancelado, obsoleto |
| Azul | `info` | abierto, en curso |
| Amarillo | `warning` | pendiente, por vencer |
| Rojo | `danger` | vencido, rechazado |
| Verde | `success` | cerrado, vigente, validado |

Excepciones (con su razón, en la prueba): la hoja de eficiencias «Cerrada,
por recibir» es azul (espera a RH), la actividad sin método de medición es
gris y, en «Rutina por rutina», el renglón pendiente sin decisión y su fecha
límite siguen en rojo. Los estados intermedios propios de cada ficha
(solicitado, capturado, adjunto…) van en azul.

## Impresos

Desde 57.98.0 los reportes propios del SGI llevan el pie del formato
controlado en **cada hoja** (`quimibond_sgi.sgi_report_layout`: clave,
revisión y emisión del documento vigente, y «Página x de y»); 57.101.0 lo
agrega al 8D, la solicitud de desarrollo, la responsiva de EPP y las
eficiencias, y a los PDF nuevos de esta tabla. Si el formato
aún no tiene clave del SGI, el pie lleva solo la página y el Diagnóstico lo
lista aparte, «sin clave del SGI» (la clave se da de alta en el código,
`format_ref_*` en `data/sgi_format_map_data.xml`).

| PDF | Dónde se imprime | Pie |
|---|---|---|
| Ficha del indicador (cómo se mide, metas, gráfica de los últimos 12 periodos con medición, causa y acciones de los rojos) | Indicador: «Ficha en PDF» o Imprimir; todas las del proceso: Imprimir → «Fichas de indicadores del proceso» | Página (sin clave) |
| Diagrama en formato controlado: mapa de procesos, interacción (4.4), tortuga, roles (5.3), contexto (4.1/4.2); las flechas van como tabla «Conexiones» | Barra del diagrama, botón PDF (junto a la impresora). Carriles y PDCA siguen con la impresión de pantalla | Página (sin clave) |
| Programa de auditorías: programado contra realizado | Programa: «Programado contra realizado» o Imprimir | Página (sin clave) |
| Mapa de calor de riesgos (R&O, IPER, ambiental; cuadrícula inicial y residual) | Lista de riesgos: Imprimir; diagrama de riesgos: botón PDF | IPER: F-P-S01-01; R&O y ambiental: página |
| Reporte 8D | NC: Imprimir | Página (no comparte la clave del reporte de NC) |
| Solicitud de desarrollo, responsiva de EPP, eficiencias del personal | Su ficha: Imprimir | La clave de su tipo |
| Acta de revisión por la dirección | Revisión: Imprimir | Por el modelo |

- **Nombre del archivo:** NC, 8D, plan e informe de auditoría, acta,
  investigación de incidente y AMEF salen con su folio («Reporte de NC -
  NCI-2026-001»).
- **Copia guardada del acta:** al imprimir un acta **cerrada**, Odoo guarda
  el PDF como adjunto de la revisión y lo vuelve a entregar igual. Reabrirla
  (solo el Jefe MAST) renombra esa copia («… (reabierta el dd-mm-aaaa).pdf»),
  no la borra; al cerrarla otra vez se guarda la nueva. El informe de
  auditoría no lleva copia por este camino: se archiva en Documentos al
  cerrar la auditoría (AU-3). El acta cerrada imprime la copia guardada;
  para corregirla, reábrala (la copia anterior se renombra).
- **Tendencia del indicador:** la gráfica **promedia** las mediciones del
  periodo (antes las sumaba: las semanales daban 218 % de OTIF en un mes),
  los indicadores semanales abren por semana y la tendencia abre con el
  filtro «Con dato» (un «sin dato» o un pendiente valen 0). El pivote de
  riesgos «Mapa de calor» separa los instrumentos (cada uno con su escala).

## Indicadores: sin dato y cálculos (57.102.0)

- **«Sin dato» no es 0.** Un indicador sin ninguna medición con dato dice
  «Sin dato» en la lista de indicadores, el Tablero, la pestaña Indicadores
  del proceso, Mis indicadores (lista y celular), el texto de la revisión
  por la dirección, los «Últimos 6 periodos» y el diagrama de indicadores
  (campo `sgi_last_value_label`); un 0 real se muestra como 0. El valor de
  una medición sin dato se sigue **guardando** en 0 (un `Float` no guarda
  vacío): por eso las listas no lo muestran y «Mediciones» y «Tendencia»
  abren con el filtro «Con dato».
- **Capturar un 0 (manuales).** Una medición de indicador manual en 0, sin
  numerador, sin denominador y **sin nota** no se marca capturada ni se
  valida: si el valor de verdad es 0, el responsable lo dice en la nota
  («0: sin caídas en el mes»). Solo aplica a personas (el sistema no se
  revisa); «Validar mediciones» de la revisión (P-40) las salta y las lista.
- **Recálculo diario.** El cron de indicadores re-mide las pendientes y,
  desde 57.102.0, las «sin dato» y las capturadas no validadas de los
  últimos `quimibond_sgi.indicator_recompute_months` meses (2 por omisión,
  desde el día 1 del mes de hace 2 meses). Nunca toca validadas, indicadores
  de foto ni de salud, mediciones con NC, con causa o acciones, ni las
  **corregidas a mano** (quien cambia el valor de una medición automática la
  marca «Valor corregido a mano»; «Recalcular valor» o «Recalcular ahora»
  quitan la marca). Solo escribe si algo cambió y deja el antes y el después
  en el chatter de la medición. El botón **Recalcular mediciones** de la
  lista (Administrador SGI) con indicadores seleccionados re-mide todo lo no
  validado de ellos, sin ventana de meses. La corrida del cron va después de
  la medición mensual y tiene tiempo tope (240 s): lo que no alcanza sigue al
  día siguiente. Una nota que escribió una persona no se borra al recalcular.
- **«Medir desde».** Al cambiarla, las mediciones no validadas cuyo periodo
  termina antes pasan a «Sin dato» con el valor anterior en la nota; las
  validadas no se tocan y nada se borra.
- **Registro vacío.** En una fórmula configurable «más bajo es mejor», un
  numerador en 0 con algún término cuya fuente nunca ha tenido registros
  (contar) o cuyo campo sumado nunca se ha capturado (sumar) da «Sin dato»
  con la nota «Registro vacío…», no un verde falso (SST-01, C5-01). En «más
  alto es mejor» el 0 rojo se deja.
- **Fórmulas corregidas:** TR-01 = NC levantadas en el periodo (sin
  canceladas) que ya están cerradas ÷ NC levantadas en el periodo; C5-02 =
  reclamaciones cerradas en 30 días naturales ÷ reclamaciones del periodo;
  C2-06 = salidas con sello de embarque ÷ salidas validadas de la semana (el
  campo de Studio «Tipo de transporte» nunca se capturó); RH-02 = solo
  empleados de la empresa del SGI, con numerador y denominador.

## Salud del SGI

Desde 57.99.0 (auditoría 2026-10, sección 8 y hallazgo D-01) Dirección ve
cada semana si el SGI se está usando o solo lo operan el CEO, el Jefe MAST y
los crons. Diez indicadores de nivel Dirección del proceso E2, semanales, en
prueba y sin NC automática (`data/sgi_health_indicators.xml`, `noupdate`):

| Clave | Qué mide |
|---|---|
| SG-01 | Procesos en «Vigente» ÷ procesos activos |
| SG-02 | Personas (con usuario y empleado) que crearon, modificaron o comentaron algo del SGI en 30 días |
| SG-03 | Empleados con puesto que tienen usuario |
| SG-04 | Acuses leídos o pendientes dentro del plazo del aviso |
| SG-05 | Mediciones validadas a tiempo (3 días hábiles desde la captura) |
| SG-06 | Rojos de los últimos 3 meses con NC o con causa y acción |
| SG-07 | NC cerradas en 90 días sin ninguna verificación «No eficaz»; la nota da las abiertas con más de 60 días |
| SG-08 | Avisos del SGI vencidos; la nota da qué parte los tiene una sola persona |
| SG-09 | Auditorías internas del programa del año hechas hasta el mes en curso |
| SG-10 | Formatos «Migrado a Odoo» con destino activo y uso en 90 días |

- **Dónde se ven:** SGI → Dirección → Tablero → página «Salud del SGI»: los
  diez con la medición de la semana pasada y, por el hallazgo D-01, una
  tabla por dueño de proceso (avisos vencidos, validaciones atrasadas y días
  sin movimiento en el SGI). No ocupan los 12 lugares de «Indicadores de
  dirección».
- **Correo de los lunes** (acción planificada «SGI: Salud del SGI (correo
  semanal a Dirección)», 08:00 de México): lo mismo, a los miembros de
  Dirección de Operaciones (SGI) y a los usuarios de
  `quimibond_sgi.health_mail_user_ids` (ids separados por coma). Solo
  conteos; sin datos de salud ni de nómina.
- **Quién no cuenta en SG-02:** OdooBot, el administrador técnico, las
  cuentas sin empleado de la empresa del SGI y los usuarios de
  `quimibond_sgi.health_excluded_user_ids` (ids separados por coma).
- **Sus mediciones no se validan** (no salen en «Validar medición»), no
  piden causa y plan, no escalan y un «sin dato» no agenda «Indicador no
  calculó»: su respuesta es la revisión semanal. Tampoco cuentan en SG-05 ni
  en SG-06.

## Metas congeladas (K-04)

Desde 57.100.0, una medición validada guarda las metas con las que se juzgó:
sentido, objetivo y aceptable del periodo (el escalón de la trayectoria si lo
hay) y, en «dentro de un rango», mínimo, máximo y tolerancia
(`models/sgi_indicator_integrity.py`). Su semáforo, el objetivo y el
aceptable que muestra y el semáforo de su desglose salen de esas metas aunque
después cambie la meta del indicador o se corrija un escalón. Al cambiar las
metas de un indicador con validadas, su chatter dice cuántas las conservan.

- **Reabrir:** solo el Jefe MAST («Regresar a pendiente»); las metas se
  sueltan (nota en el chatter) y la medición se juzga con las actuales.
  Validarla otra vez guarda las de ese momento.
- **Corregir el valor sin reabrir** (Jefe MAST): el color se recalcula contra
  las metas guardadas.
- Nadie escribe las metas guardadas por RPC. El despliegue de 57.100.0
  guardó las metas de las validadas que ya había (el color no cambió).

## Competencias por examen o curso y eficacia (N-13)

- **Ligar:** el Jefe MAST liga un examen de certificación (Empleados →
  Competencias SGI → Exámenes y competencias (Encuestas)) o un curso de
  eLearning (… → Cursos y competencias (eLearning)) con una competencia y un
  nivel. Del curso se captura «Vigencia (meses)» (0 = no vence); la del examen
  es la validez de la certificación (app Encuestas).
- **Otorgar:** quien aprueba el examen o termina el curso recibe la
  competencia (`hr.employee.skill`) con su vigencia en cuanto lo nativo
  escribe la línea de currículum; el cron diario de cursos es el respaldo.
  Sube de nivel (el renglón viejo se cierra el día anterior) o renueva; nunca
  baja ni acorta. Solo empleados de la empresa del SGI. Quien aprueba sin
  usuario se encuentra por su contacto de trabajo.
- **Vencimientos:** el aviso «por vencer / vencida» (cron de competencias)
  cubre toda competencia con vigencia, no solo las certificaciones.
- **Eficacia (ISO 9001 7.2 c):** cada competencia nueva o subida de nivel abre
  una evaluación (`sgi.training.effectiveness`) con aviso al jefe inmediato
  (si no tiene usuario: responsable del departamento, RH, Jefe MAST) a los
  `quimibond_sgi.training_effectiveness_days` días (90). «Eficaz» o «No
  eficaz» (pide comentario y avisa a RH «Reprogramar capacitación»; la
  competencia no se quita). Ya evaluada, solo el Jefe MAST cambia el
  resultado; no se borra. La ven quien evalúa (las suyas), RH y el Jefe MAST;
  el Auditor y Dirección no (datos de RH). Encuesta opcional al jefe:
  `quimibond_sgi.training_effectiveness_survey_id` (0 = sin encuesta).
- Colores de la eficacia: pendiente amarillo, eficaz verde, no eficaz rojo.

## Cliente automotriz (N-14)

- En el contacto del cliente, grupo «Cliente automotriz» (por compañía, solo
  SGI o Calidad lo cambian): «Exige PPAP ante cambios» y «Exige plan de
  contingencia». Salen vacías; las marca Calidad (pista de datos de Q12).
- Con `quimibond_sgi_plm`, el ECO marca solo «Requiere PPAP» cuando el
  producto se vendió en los últimos `quimibond_sgi.ppap_sales_window_months`
  meses (12) o ya tiene PPAP con un cliente que lo exige, y al aplicarlo
  genera un PPAP por cliente. Nunca desmarca.
- **Salida sin CoA:** validar una salida a un cliente con «Requiere CoA en
  cada embarque» sin el CoA adjunto deja nota en la salida y un aviso «Salida
  sin CoA» al Jefe de Calidad (puesto `quimibond_sgi.coa_exception_job_id`) o,
  si no hay, al Jefe MAST, que se cierra solo al adjuntar el CoA. Llega a Mis
  pendientes y al correo semanal. El bloqueo (`coa_block_validation`) sigue
  apagado.
- El plan de contingencia es solo la casilla: qué documento se exige se
  decide con Q12.

## Sugerencia de IA en la NC

Desde 57.100.0 (sección 7 del reporte de auditoría, puerta Q16;
`models/sgi_ai.py`). **Sale apagada.**

- **Encender** (solo con la autorización escrita de Jose, anotada en
  `docs/audit/decisiones.md`): un administrador captura la llave de Anthropic
  en `quimibond_sgi.ai_api_key` y pone `quimibond_sgi.ai_enabled = True`.
  Otros parámetros: `ai_model` (`claude-opus-5-5`; `claude-sonnet-5-5` cuesta
  la mitad), `ai_timeout` (segundos, 60), `ai_include_history` (manda la
  desviación y la causa raíz de hasta 3 NC cerradas del mismo proceso),
  `ai_backend` (solo `anthropic`).
- **Qué hace:** en la NC con folio abierta, un Usuario SGI pulsa «Pedir
  sugerencia a la IA» (pestaña «Desviación y análisis»): cláusula sugerida
  (de las cargadas), clasificación, el porqué, borrador de 5 porqués e
  Ishikawa 6M, en campos de sugerencia. «Usar cláusula y clasificación
  sugeridas» y «Copiar el borrador de porqués» (solo los vacíos) los copian
  con su nombre en el seguimiento. **Nunca escribe la causa raíz, nunca cambia
  la etapa ni cierra.**
- **Qué se manda:** título, desviación, descripción, origen, proceso,
  producto, lote y la lista de cláusulas, con correos, teléfonos y RFC
  tachados. Nunca cliente o proveedor, N° NCR del cliente, usuarios,
  responsables ni adjuntos. Un nombre escrito a mano dentro de la desviación
  no se puede tachar con certeza, y el nombre del producto puede llevar el de
  un cliente.
- **Fallas:** si la IA no responde, rechaza o contesta algo que no sirve,
  aparece un aviso y nada cambia; en el log queda un `warning` sin el texto de
  la NC.

## Menú

Siete entradas bajo **SGI** (57.98.0): Inicio (Mis pendientes, Mi
procedimiento, Documentos vigentes, Mis indicadores, Mi equipo, Checklists de
hoy), Reportar (no conformidad, casi accidente o incidente, queja o
sugerencia: cada una abre la ficha nueva), Procesos (mapa y actividades para
todos; entregables, flujos, matriz de responsabilidades, puestos y procesos y
fichas por máquina solo para dueño de proceso, Jefe MAST, Dirección y Auditor;
«Del Dropbox a Odoo»), Mejora (NC, reclamaciones, acciones, mejora continua,
auditorías), Seguridad y ambiente, Dirección y Administración SGI. Al tocar la
app, Dirección abre en el Tablero y los demás en Mis pendientes (acción del
menú raíz, `sgi_home_action`). El árbol completo con grupos está en
`tools/sgi_menu_tree.txt`, y `tests/test_menu_tree.py` lo compara con la base.

## Grupos

| Grupo | Para quién |
|---|---|
| Usuario SGI | Todo el personal con usuario: sus pendientes, su procedimiento, levantar NC |
| Dueño de proceso (SGI) | Jefes que son dueños de un proceso (implica Usuario) |
| Captura de eficiencias | Jefe o supervisor de área |
| Jefe MAST y SGI | Quien administra el SGI |
| Administrador SGI | Carga del catálogo y cambios de estructura (implica Jefe MAST) |
| Dirección de Operaciones (SGI) | Consulta y aprueba; **no** implica Jefe MAST |
| Auditor SGI | Lectura para auditar (D-05); no ve salud ni salarios |
| Salud ocupacional (SGI) | Solo el Coordinador de RH: exámenes y estudios de higiene |
| Salarios de eficiencias (SGI) | Importes de eficiencias (RH y Nóminas) |
| Comisión de Seguridad e Higiene (SGI) | Recorridos y hallazgos de la CSH |
| Tableta de planta (SGI) | Cuenta compartida de una tableta: solo la app SGI en planta; firma a nombre de quien teclea su PIN |

**Capturista de planta** = persona sin usuario que firma en la tableta (app
SGI en planta) con su PIN de empleado; las cuentas de tableta se dan de alta
en Configuración → Tabletas de planta. El PIN no tiene límite de intentos; se
acepta en planta (F-018). **Apagado por ahora** (decisión de Dirección,
2026-10-02): no hay tabletas dadas de alta; encenderlo pide antes contestar el
límite de intentos (Q12).

## Satélites

| Módulo | Qué agrega | Se instala |
|---|---|---|
| `quimibond_sgi_mapa` | El mapa de procesos de producción (JSON) y el asistente de carga | A mano, solo staging o recuperación |
| `quimibond_sgi_pesaje` | Rollo fuera de tolerancia → alerta de calidad; parámetro `pesaje_tolerance_kg` | `auto_install` con `pesaje_rollos_tejido` |
| `quimibond_sgi_plm` | «Requiere PPAP» por los clientes del producto → un PPAP por cliente y aviso a ventas (pruebas propias desde 3.2.0) | `auto_install` con `mrp_plm` |
| `quimibond_sgi_revisado` | Pareto de defectos del revisado e indicador MA-03 «Calidad PQ» | `auto_install` con `mrp_revisado_telas` |
| `quimibond_sgi_knowledge` | Instructivo escrito en Conocimiento y publicado como IT | `auto_install` con `knowledge` |
| `quimibond_sgi_studio` | Regla de aprobación de Studio para el rol «Aprueba» | `auto_install` con `web_studio` |

**Lo que el núcleo garantiza a los satélites** (A-021; no cambiar sin revisar
los satélites): `quality.alert.sgi_auto_create(source_code, vals)`,
`sgi.cron._sgi_schedule(...)`, `sgi.cron._sgi_manager_user_id()`, los XML IDs
de los equipos de calidad (`sgi_quality_team_internal`) y del indicador
`sgi_ind_calidad_pq`. Fuera de los satélites, `quimibond_intelligence`
(señales) lee modelos `sgi.*`; al renombrar o sacar un modelo, revisar
`quimibond_intelligence/models/senales/` (K-020).

## Instalar, actualizar y probar

```bash
# Odoo.sh, shell de la rama. Varios módulos se separan con COMAS, sin espacios.
odoo-update quimibond_sgi,quimibond_sgi_pesaje && odoosh-restart http && odoosh-restart cron
```

- Verificaciones después del despliegue: `docs/RUNBOOK_DESPLIEGUE.md`, sección SGI.
- **Pruebas:** el CI de GitHub no instala el SGI (depende de Enterprise). Se
  corren en el build de desarrollo de Odoo.sh de la rama, **solo con
  `--test-tags`**, porque el suite completo se detiene en las pruebas de
  nómina:
  `odoo-bin -u quimibond_sgi --test-tags /quimibond_sgi --stop-after-init --no-http`
  (agregar `,/quimibond_sgi_pesaje`… para los satélites). Desde 57.100.0 el
  build de la rama corre `--test-tags /quimibond_sgi,/quimibond_sgi_plm`
  (`quimibond_sgi_plm` estrena pruebas; P19).
- `post_init_hook` (`__init__.py`): al instalar, liga los puntos de control de
  los equipos «CALIDAD Materia Prima» y «Revisado de Tela» a los planes de
  control del módulo; busca por nombre y nunca detiene la instalación (A-030).

## Reglas para programar aquí

- **Versión y CHANGELOG:** todo cambio en `addons/quimibond_sgi/` sube la
  versión y agrega su `## <versión>` en `CHANGELOG.md` en el mismo commit
  (`tools/check_addons.py` lo exige).
- **Sin herencias propias:** el SGI no hereda sus propias vistas (0 desde
  57.30.0). La ficha propia va completa en un solo `<record>`; la herencia es
  para vistas de otros módulos. Ver `CLAUDE.md` de la raíz.
- Menús: todos en `views/sgi_menus.xml`, al final del manifest; si cambian,
  cambia `tools/sgi_menu_tree.txt` en el mismo commit.
- Crons en `noupdate`: cambiar uno en la base requiere migración.
- **Nombres** (A-024, C-020; no renombrar lo existente): modelos y campos en
  inglés, etiquetas en español; en lo nuevo, NC ligada = `alert_id`, área =
  `area_id` (M2O a `sgi.area`), responsable = `responsible_id`, revisión
  `Integer`, sin prefijo `sgi_` en campos de modelos propios.
- Numerales de actividad (`number`): no se renumeran; son la llave natural de
  la carga y la exportación. Los huecos son esperados (H-017).
- Campos calculados sin `store`: ninguno se usa donde Odoo exige almacenarlo
  (C-023). Si Mis pendientes se vuelve lento, el candidato es `store=True` en
  `quality.alert.sgi_stage_is_closing/is_cancel`.
- Adjuntos de salud (`sgi.health.record`): pendiente de verificar al capturar
  el primero que el PDF quede ligado al registro (F-019).
- `help` de campos en «usted» (D-29), para la persona que llena el campo.

## Documentación

| Qué | Dónde |
|---|---|
| Manuales por rol, administración y transición | `docs/sgi/` |
| Técnica generada del código (modelos, campos, vistas, menús, crons) | `docs/sgi/tecnica/` (`python3 tools/sgi_docs.py`) |
| Decisiones de Jose | `docs/audit/decisiones.md` |
| Historia (README anterior, manuales viejos) | `docs/historico/sgi/` |

Licencia: OPL-1 (D-31). Autor: Quimibond.
