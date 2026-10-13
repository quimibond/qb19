# P-G01 «Elaboración y control de información documentada» en el nuevo SGI

Dirección General, 2026-10-13: el P-G01 Rev. 11 (jul-2026, 13 páginas en el
Dropbox) no se reescribe como otro procedimiento en Word (borrador Rev. 12
del 13-oct, retirado el mismo día: «¿por qué lo haces en Word?»). **Sus
reglas viven en el capítulo 6 del Manual del SGI generado desde Odoo
(`quimibond_sgi` 57.146.0) y sus rutinas son actividades del proceso E2
Gestión del SGI.** El documento 3565 (P-G01) queda como procedimiento
anterior en «Del Dropbox a Odoo», con sus 16 rutinas resueltas.

Este archivo dice **dónde quedó cada cosa** para el Jefe de MAST y SGI y
para el auditor que pida el 7.5.

## 1. Secciones de la Rev. 11 → dónde viven hoy

| Rev. 11 | Tema | Hoy | Cómo se ve |
| --- | --- | --- | --- |
| 1, 2 | Propósito y alcance | MIID 6 (encabezado) y 1.2 | El capítulo 6 dice qué sustituye y a qué aplica (documentos, registros, registros en papel) |
| 3.1–3.7 | Responsabilidades | MIID 4.4 (bloque `roles`) y 6.2–6.3 | Dueño, puestos que ejecutan, aprueban y escalan por proceso, sacados de las actividades; firmas Elaboró / Revisó / Aprobó por tipo de documento |
| 4.1 | Estructura de procedimientos, instructivos y protocolos | MIID 6.2 | Procedimiento generado desde el proceso (`PR-{proceso}`), instructivos y controles en Conocimiento, formato = pantalla de Odoo |
| 4.2 | Codificación | MIID 6.1 (bloque `tipos_documento`) | Tipos de documento con su patrón de clave; la NOM va en el título |
| 4.3 | Elaboración (nuevo titular, plantilla, tipografía, archivo) | MIID 6.2 y actividad **E2.42** | Plantilla solo para archivos no migrados; el nuevo titular firma su Mi procedimiento y lo revisa en 30 días |
| 4.4 | Aprobación | MIID 6.3 | Solicitud de cambio documental + firmas en Firma electrónica; E2.01 responde en 5 días hábiles |
| 4.5 | Modificación (solicitud, calendario mensual, documentos relacionados, revisión) | MIID 6.3 | Solicitudes continuas; sin calendario ni suspensión por auditoría; revisión y fecha las pone Odoo; relacionados en la misma solicitud |
| 4.6 | Implantación en 15 días hábiles (F-P-G01-08) | MIID 6.4 y actividad E2.02 | Verificación en piso a los 15 días hábiles, acuses de lectura |
| 4.7 | Almacenamiento, archivo muerto, copias físicas | MIID 6.6 y actividad E2.03 | Obsoleto con motivo, retención y disposición final, respaldos de Odoo.sh, paso anual al archivo muerto |
| 4.8.1 | Documentos externos (F-P-G01-09) | MIID 6.4 y actividad E2.36 | Documentos externos en Odoo; implantar en 10 días hábiles |
| 4.8.2 | Seguridad de la información | MIID 6.6 | Datos personales, acceso por grupo, propiedad de la información |
| 4.8.3 | Registro de cambios (F-P-G01-03) | MIID 6.3 | Lista maestra (SGI → Sistema → Documentos → Lista maestra) |
| 4.8.4 | Difusión (reunión o NEWS) | MIID 6.5 | Acuses de lectura, NEWS mensual generado por Odoo, reunión con minuta cuando amerite |
| 4.8.5 | Copias controladas y entrega a externos | MIID 6.5 y actividad **E2.43** | Sin copias controladas; impreso = copia no controlada; entrega a externos autorizada por el Director de Operaciones y anotada en el historial |
| 5–8 | Información ambiental, riesgos, seguridad patrimonial, marco normativo | MIID 2 (bloque `normas`) y 5.1 | Lo que aplica al proceso E2 está en su procedimiento generado |
| 9 | Información documentada del P-G01 | «Del Dropbox a Odoo» | Formatos y anexos de la Rev. 11 con su sustituto (tabla 3) |

## 2. Las 16 rutinas de la Rev. 11 («Rutina por rutina», procedimiento 3565)

| N.º | Rutina | Estado | Quién la cubre |
| --- | --- | --- | --- |
| 1 | Elaborar o modificar documentos en 30 días tras un ingreso | Cubierta (2026-10-13) | **E2.42** Verificar que cada nuevo titular firme su Mi procedimiento y lo revise en 30 días |
| 2 | Matrices de aspectos, comunicación, partes interesadas, IPER, FODA y riesgos | La hace Odoo | Revisiones periódicas E2.10, E2.23 y E2.38 |
| 3 | Solicitar alta, baja o modificación con solicitud firmada | Cubierta | E2.01 |
| 4 | Aprobar por el jefe directo con visto bueno de MAST | Cubierta | E2.01 |
| 5 | Revisar los documentos relacionados con el cambio | Cubierta | E2.01 |
| 6 | Calendario mensual de cambios | La hace Odoo | Plazos por solicitud de E2.01 y E2.02 |
| 7 | Convertir a PDF y verificar el formato | La hace Odoo | E2.02 publica la versión vigente |
| 8 | Revisar los documentos que cumplen 2 años | Cubierta | E2.03 |
| 9 | Implantar en 15 días hábiles y verificar (F-P-G01-08) | Cubierta | E2.02 |
| 10 | Archivo muerto de registros físicos | Cubierta | E2.03 |
| 11 | Copias controladas selladas (F-P-G01-05) | La hace Odoo | Se consulta la versión vigente; E2.02 retira impresos |
| 12 | Retirar el documento obsoleto | Cubierta | E2.02 |
| 13 | Documentos externos (F-P-G01-09) | Cubierta | E2.36 |
| 14 | Lista de control de documentos | La hace Odoo | Lista maestra |
| 15 | Difundir cada cambio (reunión o NEWS) en 15 días | Cubierta | E2.02 |
| 16 | Autorizar la entrega a externos («COPIA NO CONTROLADA») | Cubierta (2026-10-13) | **E2.43** Autorizar con el Director de Operaciones cada entrega a externos |

**E2.42** y **E2.43** se crearon en producción el 2026-10-13 (ids 791 y
792): etapa «A. Información documentada», por evento, registro manual con
su justificación (la verificación a los 30 días y la entrega a externos no
dejan un registro propio con fecha; se anotan en el registro de
cumplimiento), cláusula 7.5 de las tres normas, ligadas al P-G01. E2.42:
ejecuta el Jefe MAST, participa el Asistente MAST, escala al Director de
Operaciones a los 5 días hábiles; recibe «Acuse de Mi procedimiento de un
ingreso o cambio de puesto» (22 días hábiles ≈ 30 naturales) y entrega
«Revisión de Mi procedimiento del nuevo titular verificada». E2.43: ejecuta
el Jefe MAST, aprueba el Director de Operaciones (escala a los 2 días
hábiles); recibe «Solicitud de un documento del SGI desde fuera de PNTQ» (2
días hábiles) y entrega «Documento del SGI entregado a un externo como copia
no controlada». Las dos con la especificación completa.

## 3. Formatos y anexos de la Rev. 11 y su sustituto

| Clave (Rev. 11) | Documento | Hoy |
| --- | --- | --- |
| ANEXO 15 | Estructura organizacional de PNTQ | Organigrama de Empleados en Odoo |
| ANEXO 08 | Manifiesto de compromiso | Se conserva como documento firmado |
| F-P-A04-01 | Matriz de comunicación | Partes interesadas y comunicación en Odoo; revisión anual E2.38 |
| F-P-A04-02 | Matriz de partes interesadas | SGI → Planeación → Partes interesadas |
| F-P-A10-01 | Minuta | Se conserva para difusión presencial |
| F-P-A10-09 | Lista de asistencia | Se conserva para difusión presencial; el acuse de lectura es la evidencia individual |
| F-P-C09-01 | Matriz FODA | SGI → Planeación → Riesgos y oportunidades (contexto) |
| F-P-C09-02 | Matriz de riesgos y oportunidades | SGI → Planeación → Riesgos y oportunidades |
| F-P-E01-01 | Matriz de aspectos ambientales | SGI → Planeación → Aspectos ambientales |
| F-P-G01-02 (F-E2-12) | Hoja para procedimientos e instructivos | Layout del SGI que imprime Odoo; plantilla solo para archivos no migrados (MIID 6.2) |
| F-P-G01-03 (F-E2-13) | Lista para el control de información documentada | Lista maestra de Odoo |
| F-P-G01-03 | Diagrama de flujo | Diagramas del proceso en Odoo (flujo, interacción, tortuga) |
| F-P-G01-05 (F-E2-14) | Lista de distribución de documentos | Baja: no hay copias controladas (MIID 6.5) |
| F-P-G01-06 | Solicitud de creación y modificación de documentación | Solicitud de cambio documental en Aprobaciones, firmada en Firma electrónica (MIID 6.3) |
| F-P-G01-08 (F-E2-15) | Verificación de implantación | Nota de verificación de E2.02 en el historial del documento y acuses |
| F-P-G01-09 (F-E2-16) | Inventario de documentos externos | Documentos externos en Odoo (E2.36) |
| F-P-G01-16 | NEWS Quimibond | Boletín NEWS generado por Odoo cada mes (MIID 6.5) |
| F-P-S01-01 | Matriz IPER | SGI → Planeación → Riesgos y oportunidades, instrumento IPER |
| MID | Manual de información documentada | MIID generado desde Odoo |

## 4. Lo que falta decidir (Dirección o Jefe de MAST y SGI)

- [ ] **Aprobación de E2.43 en Odoo.** La actividad avisa «La aprobación de
  DIRECTOR DE OPERACIONES no está activa en Odoo»: decidir si basta la
  nota del Director en el historial del documento (como dice el MIID 6.5)
  o si se configura una aprobación nativa en SGI → Administración →
  Aprobaciones del SGI.
- [ ] **Claves con NOM.** El MIID 6.1 dice que la NOM va en el título y no
  en la clave; hoy ningún tipo de documento tiene patrón con NOM.
- [ ] **Formatos F-E2-14 y F-E2-15.** Darlos de baja o conservarlos como
  archivo; hoy están «Migrado a Odoo» con destino «Plantilla de elaboración
  de documentos», que no corresponde.
- [ ] **Documento 3565 (P-G01).** Hoy «En curso» en Del Dropbox a Odoo. Al
  aprobarse el MIID generado (Rev. 03) y publicarse E2, pasa a «Migrado a
  Odoo» con el capítulo 6 del MIID como sustituto; mientras, sigue vigente
  la Rev. 11 como referencia histórica.
