<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# Modelo de datos del SGI

Modelos que definen el núcleo y sus satélites (106) y modelos de otras apps que extienden (48). El detalle de cada uno está en `diccionario/`.

## Modelos propios

| Modelo | Descripción | Qué es (docstring) | Tipo | Campos | Archivo |
|---|---|---|---|---:|---|
| [`sgi.action.line`](diccionario/sgi.action.line.md) | Acción / corrección de No Conformidad | — | Model | 17 | `addons/quimibond_sgi/models/sgi_nonconformity.py` |
| [`sgi.activity.change`](diccionario/sgi.activity.change.md) | Propuesta de cambio a una actividad (Mi procedimiento) | Propuesta de cambio a una actividad: los mismos campos de la actividad con los valores propuestos, más quién la hace. Nace con los valores de hoy; lo que la persona cambie es lo que se aprueba y se a… | Model | 32 | `addons/quimibond_sgi/models/sgi_mp_change.py` |
| [`sgi.activity.change.role`](diccionario/sgi.activity.change.role.md) | Quién hace la actividad (propuesta) | — | Model | 9 | `addons/quimibond_sgi/models/sgi_mp_change.py` |
| [`sgi.activity.exec.stat`](diccionario/sgi.activity.exec.stat.md) | Ejecuciones de una actividad SGI por semana y usuario | — | Model | 13 | `addons/quimibond_sgi/models/sgi_exec_stat.py` |
| [`sgi.activity.input`](diccionario/sgi.activity.input.md) | Entregable que recibe una actividad SGI | Un «recibe» de la actividad: qué entregable y en cuántos días hábiles debe llegar a ella. El plazo es de quien recibe (la misma salida puede urgirle a uno y no a otro) y de él sale el eslabón atorado. | Model | 11 | `addons/quimibond_sgi/models/sgi_deliverable.py` |
| [`sgi.activity.link`](diccionario/sgi.activity.link.md) | Encadenamiento entre actividades | Liga entre dos actividades de procedimiento: qué ENTREGABLE pasa de un paso al siguiente. Puede cruzar procesos (el pedido de Ventas alimenta el programa de Planeación): es el hilo conductor de la op… | Model | 13 | `addons/quimibond_sgi/models/sgi_process_procedure.py` |
| [`sgi.activity.role`](diccionario/sgi.activity.role.md) | Rol de un puesto en una actividad SGI | — | Model | 47 | `addons/quimibond_sgi/models/sgi_catalog.py` |
| [`sgi.activity.spec.gap`](diccionario/sgi.activity.spec.gap.md) | Faltante de especificación de una actividad SGI | — | Model | 6 | `addons/quimibond_sgi/models/sgi_activity_spec.py` |
| [`sgi.activity.week.stat`](diccionario/sgi.activity.week.stat.md) | Cumplimiento semanal de una actividad SGI | Aplicables, hechas, completas, a tiempo y vencidas abiertas, por actividad y semana. Va aparte de ``sgi.activity.exec.stat`` (que tiene un renglón por usuario): repetir estos totales en cada renglón … | Model | 13 | `addons/quimibond_sgi/models/sgi_activity_spec.py` |
| [`sgi.acuse.attach.wizard`](diccionario/sgi.acuse.attach.wizard.md) | Adjuntar acuse firmado a la entrega | — | TransientModel | 4 | `addons/quimibond_sgi/models/sgi_links.py` |
| [`sgi.alert.source`](diccionario/sgi.alert.source.md) | Fuente de NC automática | — | Model | 10 | `addons/quimibond_sgi/models/sgi_alert_source.py` |
| [`sgi.area`](diccionario/sgi.area.md) | Área documental SGI | — | Model | 4 | `addons/quimibond_sgi/models/sgi_area.py` |
| [`sgi.audit`](diccionario/sgi.audit.md) | Auditoría interna (P-G03) | — | Model | 24 | `addons/quimibond_sgi/models/sgi_audit.py` |
| [`sgi.audit.checklist.line`](diccionario/sgi.audit.checklist.line.md) | Pregunta del checklist de auditoría (por actividad) | AU-1 (50.0.0): el checklist de la auditoría sale del proceso, una pregunta por actividad («¿se cumple C2.17 … en plazo y con evidencia?»), con acceso a los registros reales del entregable. Cada respu… | Model | 12 | `addons/quimibond_sgi/models/sgi_audit.py` |
| [`sgi.audit.finding`](diccionario/sgi.audit.finding.md) | Hallazgo de auditoría | — | Model | 10 | `addons/quimibond_sgi/models/sgi_audit.py` |
| [`sgi.audit.program`](diccionario/sgi.audit.program.md) | Programa anual de auditorías (P-G03) | — | Model | 4 | `addons/quimibond_sgi/models/sgi_audit.py` |
| [`sgi.audit.program.line`](diccionario/sgi.audit.program.line.md) | Línea del programa de auditorías | — | Model | 9 | `addons/quimibond_sgi/models/sgi_audit.py` |
| [`sgi.base.mixin`](diccionario/sgi.base.mixin.md) | Cimiento de registros del SGI | — | AbstractModel | 1 | `addons/quimibond_sgi/models/sgi_base.py` |
| [`sgi.calculated.connection.mixin`](diccionario/sgi.calculated.connection.mixin.md) | Conexión calculada de un entregable | Lo común a ligas y flujos calculados: no se editan a mano; si uno no aplica se desactiva con su motivo. Los capturados a mano (sin entregable) son de la versión anterior: se archivan cuando un entreg… | AbstractModel | 4 | `addons/quimibond_sgi/models/sgi_deliverable.py` |
| [`sgi.calibration`](diccionario/sgi.calibration.md) | Calibración de equipo de medición (P-C03) | — | Model | 11 | `addons/quimibond_sgi/models/sgi_calibration.py` |
| [`sgi.catalog.load.wizard`](diccionario/sgi.catalog.load.wizard.md) | Cargar catálogo SGI | — | TransientModel | 9 | `addons/quimibond_sgi/models/sgi_load_wizard.py` |
| [`sgi.catalog.load.wizard.line`](diccionario/sgi.catalog.load.wizard.line.md) | Resultado de la carga del catálogo SGI | — | TransientModel | 5 | `addons/quimibond_sgi/models/sgi_load_wizard.py` |
| [`sgi.checklist.finish`](diccionario/sgi.checklist.finish.md) | Terminar checklist: quién lo llenó | — | TransientModel | 4 | `addons/quimibond_sgi/models/sgi_checklist.py` |
| [`sgi.checklist.line`](diccionario/sgi.checklist.line.md) | Punto revisado en una hoja de mantenimiento | — | Model | 7 | `addons/quimibond_sgi/models/sgi_checklist.py` |
| [`sgi.checklist.template`](diccionario/sgi.checklist.template.md) | Plantilla de checklist de mantenimiento (planta o unidades) | — | Model | 11 | `addons/quimibond_sgi/models/sgi_checklist.py` |
| [`sgi.checklist.template.item`](diccionario/sgi.checklist.template.item.md) | Punto a revisar de una plantilla de checklist | — | Model | 4 | `addons/quimibond_sgi/models/sgi_checklist.py` |
| [`sgi.coa.attach.wizard`](diccionario/sgi.coa.attach.wizard.md) | Adjuntar COA a la entrega | — | TransientModel | 4 | `addons/quimibond_sgi/models/sgi_coa.py` |
| [`sgi.coa.exception.wizard`](diccionario/sgi.coa.exception.wizard.md) | Validar salida sin COA (Jefe de Calidad) | — | TransientModel | 2 | `addons/quimibond_sgi/models/sgi_coa.py` |
| [`sgi.coa.inbox`](diccionario/sgi.coa.inbox.md) | COA recibido por correo | Correos al buzón «COA»: cada PDF se liga a su salida por el nombre del archivo. Lo que no se liga queda aquí para que Calidad lo asigne. | Model | 7 | `addons/quimibond_sgi/models/sgi_coa.py` |
| [`sgi.competence.gap`](diccionario/sgi.competence.gap.md) | Brecha de competencia (DNC) | Vista SQL: brechas entre las competencias esperadas del puesto (hr.job.skill) y las que tiene el empleado (hr.employee.skill). | Model | 10 | `addons/quimibond_sgi/models/sgi_competence.py` |
| [`sgi.config`](diccionario/sgi.config.md) | Configuración SGI | Utilidades de configuración del SGI (siembra idempotente). | AbstractModel | 0 | `addons/quimibond_sgi/models/sgi_format_map.py` |
| [`sgi.control.plan`](diccionario/sgi.control.plan.md) | Plan de control (P-C11) | — | Model | 12 | `addons/quimibond_sgi/models/sgi_control_plan.py` |
| [`sgi.cron`](diccionario/sgi.cron.md) | Tareas programadas SGI | — | AbstractModel | 0 | `addons/quimibond_sgi/models/sgi_cron.py` |
| [`sgi.csh.finding`](diccionario/sgi.csh.finding.md) | Hallazgo del recorrido de la Comisión de Seguridad e Higiene | — | Model | 9 | `addons/quimibond_sgi/models/sgi_hse_records.py` |
| [`sgi.csh.inspection`](diccionario/sgi.csh.inspection.md) | Recorrido de la Comisión de Seguridad e Higiene | — | Model | 12 | `addons/quimibond_sgi/models/sgi_hse_records.py` |
| [`sgi.deliverable`](diccionario/sgi.deliverable.md) | Entregable SGI (lo que pasa de una actividad a otra) | — | Model | 23 | `addons/quimibond_sgi/models/sgi_deliverable.py` |
| [`sgi.dev.characteristic`](diccionario/sgi.dev.characteristic.md) | Característica pedida en la solicitud de desarrollo | — | Model | 9 | `addons/quimibond_sgi/models/sgi_dev_request.py` |
| [`sgi.diagnostic`](diccionario/sgi.diagnostic.md) | Diagnóstico de configuración y adopción del SGI | — | TransientModel | 6 | `addons/quimibond_sgi/models/sgi_diagnostic.py` |
| [`sgi.diagnostic.line`](diccionario/sgi.diagnostic.line.md) | Hallazgo del diagnóstico del SGI | — | TransientModel | 6 | `addons/quimibond_sgi/models/sgi_diagnostic.py` |
| [`sgi.diagram`](diccionario/sgi.diagram.md) | Diagramas del SGI (datos para el componente sgi_diagram) | — | AbstractModel | 0 | `addons/quimibond_sgi/models/sgi_diagram.py` |
| [`sgi.direction.board`](diccionario/sgi.direction.board.md) | Tablero de dirección (I-9) | — | TransientModel | 9 | `addons/quimibond_sgi/models/sgi_direction_board.py` |
| [`sgi.document.ack`](diccionario/sgi.document.ack.md) | Acuse de lectura de documento SGI | — | Model | 8 | `addons/quimibond_sgi/models/sgi_document.py` |
| [`sgi.document.type`](diccionario/sgi.document.type.md) | Tipo de documento SGI | Tipo de documento controlado. Los prefijos de clave viven aquí como datos: agregar un tipo o cambiar su nomenclatura no requiere programar. | Model | 9 | `addons/quimibond_sgi/models/sgi_catalog.py` |
| [`sgi.dropbox.key`](diccionario/sgi.dropbox.key.md) | Clave anterior del Dropbox | Buscador por clave anterior: cada renglón es una clave vieja y dice qué es hoy y dónde vive. | Model | 14 | `addons/quimibond_sgi/models/sgi_dropbox_views.py` |
| [`sgi.dropbox.progress`](diccionario/sgi.dropbox.progress.md) | Avance de la transición del Dropbox | Avance de la transición: un renglón por proceso activo. | Model | 20 | `addons/quimibond_sgi/models/sgi_dropbox_views.py` |
| [`sgi.dyd.task.mixin`](diccionario/sgi.dyd.task.mixin.md) | Liga a la tarea del desarrollo | Liga a la tarea del proyecto de desarrollo (Diseño y Desarrollo). | AbstractModel | 1 | `addons/quimibond_sgi/models/sgi_links.py` |
| [`sgi.emergency.drill`](diccionario/sgi.emergency.drill.md) | Simulacro de emergencia | — | Model | 10 | `addons/quimibond_sgi/models/sgi_emergency.py` |
| [`sgi.emergency.plan`](diccionario/sgi.emergency.plan.md) | Plan de emergencia (ISO 14001/45001 8.2) | — | Model | 12 | `addons/quimibond_sgi/models/sgi_emergency.py` |
| [`sgi.epp.delivery`](diccionario/sgi.epp.delivery.md) | Responsiva de entrega de EPP (S03-02) | — | Model | 14 | `addons/quimibond_sgi/models/sgi_epp.py` |
| [`sgi.epp.delivery.line`](diccionario/sgi.epp.delivery.line.md) | Renglón de la responsiva de EPP | — | Model | 7 | `addons/quimibond_sgi/models/sgi_epp_sign.py` |
| [`sgi.fmea`](diccionario/sgi.fmea.md) | AMEF - Análisis de Modo y Efecto de Falla (P-C10) | — | Model | 13 | `addons/quimibond_sgi/models/sgi_fmea.py` |
| [`sgi.fmea.line`](diccionario/sgi.fmea.line.md) | Línea de AMEF | — | Model | 18 | `addons/quimibond_sgi/models/sgi_fmea.py` |
| [`sgi.format.map`](diccionario/sgi.format.map.md) | Formato SGI en documentos de Odoo | Mapeo formato SGI ↔ documento de Odoo que lo sustituye. | Model | 10 | `addons/quimibond_sgi/models/sgi_format_map.py` |
| [`sgi.format.mixin`](diccionario/sgi.format.mixin.md) | Mixin: clave de formato SGI | Agrega al modelo la clave del formato SGI que sustituye (pantalla y PDF). | AbstractModel | 1 | `addons/quimibond_sgi/models/sgi_format_map.py` |
| [`sgi.health.record`](diccionario/sgi.health.record.md) | Estudio de higiene o examen médico por trabajador | — | Model | 14 | `addons/quimibond_sgi/models/sgi_hse_records.py` |
| [`sgi.incident`](diccionario/sgi.incident.md) | Incidente / Accidente SST (P-S02, SCAT) | — | Model | 18 | `addons/quimibond_sgi/models/sgi_incident.py` |
| [`sgi.indicator`](diccionario/sgi.indicator.md) | Indicador SGI (F-P-A10-03) | — | Model | 51 | `addons/quimibond_sgi/models/sgi_indicator.py` |
| [`sgi.indicator.measure`](diccionario/sgi.indicator.measure.md) | Medición de indicador SGI | — | Model | 36 | `addons/quimibond_sgi/models/sgi_indicator.py` |
| [`sgi.indicator.measure.split`](diccionario/sgi.indicator.measure.split.md) | Desglose de medición de indicador (equipo o mercado) | Un renglón del desglose (equipo de ventas o mercado) dentro de la medición del periodo. La oficial (NC, semáforo del proceso, tablero, RxD) sigue siendo el total. | Model | 15 | `addons/quimibond_sgi/models/sgi_business_line.py` |
| [`sgi.indicator.step`](diccionario/sgi.indicator.step.md) | Escalón trimestral de la meta de un indicador SGI | — | Model | 7 | `addons/quimibond_sgi/models/sgi_indicator_trajectory.py` |
| [`sgi.indicator.term`](diccionario/sgi.indicator.term.md) | Término de la fórmula de un indicador SGI | — | Model | 14 | `addons/quimibond_sgi/models/sgi_indicator_formula.py` |
| [`sgi.instruction.publish`](diccionario/sgi.instruction.publish.md) | Publicar artículo de Knowledge como instructivo (IT) | — | TransientModel | 4 | `addons/quimibond_sgi_knowledge/models/sgi_instruction_knowledge.py` |
| [`sgi.interested.party`](diccionario/sgi.interested.party.md) | Parte interesada (ISO 4.2) | — | Model | 13 | `addons/quimibond_sgi/models/sgi_context.py` |
| [`sgi.inventory.value`](diccionario/sgi.inventory.value.md) | Valor del inventario al cierre de mes (cálculo de AL-01) | — | Model | 6 | `addons/quimibond_sgi/models/sgi_kpi_account.py` |
| [`sgi.job.family`](diccionario/sgi.job.family.md) | Familia de puestos SGI | Familia de puestos: el mismo rol repartido en puestos que solo cambian por nivel o letra (Operador de tejido circular A…J). El nivel se queda en hr.job; el SGI asigna actividades a la familia. | Model | 6 | `addons/quimibond_sgi/models/sgi_catalog.py` |
| [`sgi.legacy.routine`](diccionario/sgi.legacy.routine.md) | Rutina del procedimiento anterior | — | Model | 22 | `addons/quimibond_sgi/models/sgi_legacy_routine.py` |
| [`sgi.legacy.routine.import`](diccionario/sgi.legacy.routine.import.md) | Importar rutinas del Dropbox | — | TransientModel | 11 | `addons/quimibond_sgi/models/sgi_legacy_routine_import.py` |
| [`sgi.legacy.routine.import.line`](diccionario/sgi.legacy.routine.import.line.md) | Resultado de la importación de rutinas | — | TransientModel | 6 | `addons/quimibond_sgi/models/sgi_legacy_routine_import.py` |
| [`sgi.legal.evaluate`](diccionario/sgi.legal.evaluate.md) | Registrar evaluación de cumplimiento legal | — | TransientModel | 4 | `addons/quimibond_sgi/models/sgi_legal.py` |
| [`sgi.legal.evaluation`](diccionario/sgi.legal.evaluation.md) | Evaluación del cumplimiento de un requisito legal | DIR-1 (51.0.0): cada evaluación del cumplimiento es un registro con resultado, evidencia y fecha de la siguiente (9.1.2: conservar evidencia). | Model | 7 | `addons/quimibond_sgi/models/sgi_legal.py` |
| [`sgi.legal.requirement`](diccionario/sgi.legal.requirement.md) | Requisito legal / otro requisito (14001·45001 6.1.3) | — | Model | 21 | `addons/quimibond_sgi/models/sgi_legal.py` |
| [`sgi.lock.date.log`](diccionario/sgi.lock.date.log.md) | Bitácora de fechas de bloqueo contable | — | Model | 7 | `addons/quimibond_sgi/models/sgi_kpi_account.py` |
| [`sgi.machine.sheet`](diccionario/sgi.machine.sheet.md) | Ficha técnica de proceso por máquina (F-IT-P-P01-08-05) | — | Model | 22 | `addons/quimibond_sgi/models/sgi_machine_sheet.py` |
| [`sgi.machine.sheet.param`](diccionario/sgi.machine.sheet.param.md) | Parámetro de la ficha de proceso por máquina | — | Model | 7 | `addons/quimibond_sgi/models/sgi_machine_sheet.py` |
| [`sgi.machine.sheet.yarn`](diccionario/sgi.machine.sheet.yarn.md) | Hilo de la ficha de proceso por máquina | — | Model | 9 | `addons/quimibond_sgi/models/sgi_machine_sheet.py` |
| [`sgi.management.review`](diccionario/sgi.management.review.md) | Revisión por la Dirección (IT-P-A10-01) | — | Model | 22 | `addons/quimibond_sgi/models/sgi_management_review.py` |
| [`sgi.management.review.agreement`](diccionario/sgi.management.review.agreement.md) | Acuerdo de Revisión por la Dirección | — | Model | 10 | `addons/quimibond_sgi/models/sgi_management_review.py` |
| [`sgi.mapa.load.wizard`](diccionario/sgi.mapa.load.wizard.md) | Cargar mapa de procesos SGI | — | TransientModel | 13 | `addons/quimibond_sgi_mapa/models/sgi_mapa_wizard.py` |
| [`sgi.mapa.load.wizard.line`](diccionario/sgi.mapa.load.wizard.line.md) | Resultado de la carga del mapa SGI | — | TransientModel | 5 | `addons/quimibond_sgi_mapa/models/sgi_mapa_wizard.py` |
| [`sgi.msa.study`](diccionario/sgi.msa.study.md) | Estudio MSA (IATF 7.1.5.1.1) | — | Model | 9 | `addons/quimibond_sgi/models/sgi_msa.py` |
| [`sgi.my.pending`](diccionario/sgi.my.pending.md) | Mis pendientes (SGI) | — | TransientModel | 10 | `addons/quimibond_sgi/models/sgi_my_pending.py` |
| [`sgi.my.procedure`](diccionario/sgi.my.procedure.md) | Mi procedimiento (pantalla) | — | TransientModel | 43 | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py` |
| [`sgi.my.procedure.check`](diccionario/sgi.my.procedure.check.md) | Mi procedimiento: revisión previa a publicar | Revisión previa a publicar, con listas nativas (antes HTML). | TransientModel | 5 | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py` |
| [`sgi.my.procedure.mixin`](diccionario/sgi.my.procedure.mixin.md) | Mi procedimiento en la ficha | «Mi procedimiento» dentro de la ficha (empleado, empleado público y puesto): las mismas listas que la pantalla de Inicio, como campos calculados, para que el procedimiento se vea donde vive la person… | AbstractModel | 9 | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py` |
| [`sgi.nc.cancel`](diccionario/sgi.nc.cancel.md) | Cancelación de No Conformidad | NC-4: cancelar solo con motivo y aprobación del Jefe MAST. Cualquier usuario del SGI pide la cancelación con su motivo (queda en el chatter y agenda la aprobación a MAST); el Jefe MAST la aprueba con… | TransientModel | 3 | `addons/quimibond_sgi/models/sgi_nonconformity.py` |
| [`sgi.nc.force.close`](diccionario/sgi.nc.force.close.md) | Cierre forzado de No Conformidad | — | TransientModel | 2 | `addons/quimibond_sgi/models/sgi_nonconformity.py` |
| [`sgi.norm`](diccionario/sgi.norm.md) | Norma ISO | — | Model | 4 | `addons/quimibond_sgi/models/sgi_norm.py` |
| [`sgi.norm.clause`](diccionario/sgi.norm.clause.md) | Cláusula de norma ISO | — | Model | 9 | `addons/quimibond_sgi/models/sgi_norm.py` |
| [`sgi.objective`](diccionario/sgi.objective.md) | Objetivo Integral SGI | — | Model | 9 | `addons/quimibond_sgi/models/sgi_objective.py` |
| [`sgi.policy`](diccionario/sgi.policy.md) | Política integral del SGI | — | Model | 8 | `addons/quimibond_sgi/models/sgi_policy.py` |
| [`sgi.ppap`](diccionario/sgi.ppap.md) | PPAP - Proceso de Aprobación de Partes de Producción (P-C15) | — | Model | 9 | `addons/quimibond_sgi/models/sgi_ppap.py` |
| [`sgi.ppap.element`](diccionario/sgi.ppap.element.md) | Elemento de un PPAP | — | Model | 10 | `addons/quimibond_sgi/models/sgi_ppap.py` |
| [`sgi.ppap.element.template`](diccionario/sgi.ppap.element.template.md) | Elemento PPAP (catálogo AIAG) | — | Model | 4 | `addons/quimibond_sgi/models/sgi_ppap.py` |
| [`sgi.process`](diccionario/sgi.process.md) | Proceso SGI | — | Model | 73 | `addons/quimibond_sgi/models/sgi_process.py` |
| [`sgi.process.activity`](diccionario/sgi.process.activity.md) | Actividad del procedimiento | Actividad (numeral) del Desarrollo del procedimiento (sección 4). | Model | 87 | `addons/quimibond_sgi/models/sgi_process_procedure.py` |
| [`sgi.process.flow`](diccionario/sgi.process.flow.md) | Flujo entre procesos SGI | — | Model | 8 | `addons/quimibond_sgi/models/sgi_process.py` |
| [`sgi.process.responsibility`](diccionario/sgi.process.responsibility.md) | Responsabilidad de área en el procedimiento | Responsabilidad de un rol/puesto dentro del procedimiento (sección 3). | Model | 6 | `addons/quimibond_sgi/models/sgi_process_procedure.py` |
| [`sgi.process.stage`](diccionario/sgi.process.stage.md) | Etapa de un proceso SGI | — | Model | 7 | `addons/quimibond_sgi/models/sgi_deliverable.py` |
| [`sgi.risk`](diccionario/sgi.risk.md) | Riesgo / Oportunidad SGI | — | Model | 33 | `addons/quimibond_sgi/models/sgi_risk.py` |
| [`sgi.risk.category`](diccionario/sgi.risk.category.md) | Categoría de riesgo/oportunidad | — | Model | 2 | `addons/quimibond_sgi/models/sgi_risk.py` |
| [`sgi.sign.builder`](diccionario/sgi.sign.builder.md) | Plantillas de Sign armadas por el SGI | — | AbstractModel | 0 | `addons/quimibond_sgi/models/sgi_sign_builder.py` |
| [`sgi.sign.record.mixin`](diccionario/sgi.sign.record.mixin.md) | Firmas de Sign ligadas al registro | — | AbstractModel | 4 | `addons/quimibond_sgi/models/sgi_sign_record.py` |
| [`sgi.sign.request.wizard`](diccionario/sgi.sign.request.wizard.md) | Crear solicitud de firma ligada a un registro | — | TransientModel | 5 | `addons/quimibond_sgi/models/sgi_sign_record.py` |
| [`sgi.staff.efficiency`](diccionario/sgi.staff.efficiency.md) | Eficiencias de personal (F-P-A01-32/34) | — | Model | 13 | `addons/quimibond_sgi/models/sgi_staff_efficiency.py` |
| [`sgi.staff.efficiency.line`](diccionario/sgi.staff.efficiency.line.md) | Calificación mensual de un empleado | — | Model | 17 | `addons/quimibond_sgi/models/sgi_staff_efficiency.py` |
| [`sgi.supplier.eval`](diccionario/sgi.supplier.eval.md) | Evaluación de proveedor SGI (8.4) | — | Model | 9 | `addons/quimibond_sgi/models/sgi_supplier_eval.py` |

## Modelos de otras apps que el SGI extiende

| Modelo | Campos que agrega | Archivos |
|---|---:|---|
| [`account.move`](diccionario/account.move.md) | 2 | `addons/quimibond_sgi/models/sgi_kpi_account.py`, `addons/quimibond_sgi/models/sgi_links.py` |
| [`account.move.line`](diccionario/account.move.line.md) | 1 | `addons/quimibond_sgi/models/sgi_kpi_account.py` |
| [`approval.category`](diccionario/approval.category.md) | 5 | `addons/quimibond_sgi/models/sgi_approval_native.py`, `addons/quimibond_sgi/models/sgi_doc_change.py`, `addons/quimibond_sgi/models/sgi_doc_change_sign.py`, `addons/quimibond_sgi/models/sgi_mp_change.py` |
| [`approval.request`](diccionario/approval.request.md) | 28 | `addons/quimibond_sgi/models/sgi_doc_change.py`, `addons/quimibond_sgi/models/sgi_doc_change_sign.py`, `addons/quimibond_sgi/models/sgi_links.py`, `addons/quimibond_sgi/models/sgi_mp_change.py` |
| [`crm.team`](diccionario/crm.team.md) | 1 | `addons/quimibond_sgi/models/sgi_business_line.py`, `addons/quimibond_sgi/models/sgi_customer_reply.py` |
| [`crm.team.member`](diccionario/crm.team.member.md) | 0 | `addons/quimibond_sgi/models/sgi_business_line.py` |
| [`documents.document`](diccionario/documents.document.md) | 61 | `addons/quimibond_sgi/models/sgi_current_documents.py`, `addons/quimibond_sgi/models/sgi_document.py`, `addons/quimibond_sgi/models/sgi_document_owner.py`, `addons/quimibond_sgi/models/sgi_external_doc.py`, `addons/quimibond_sgi/models/sgi_legacy_routine.py`, `addons/quimibond_sgi/models/sgi_my_procedure.py`, `addons/quimibond_sgi/models/sgi_my_procedure_sign.py`, `addons/quimibond_sgi/models/sgi_sign_elearning.py`, `addons/quimibond_sgi_knowledge/models/sgi_instruction_knowledge.py` |
| [`helpdesk.team`](diccionario/helpdesk.team.md) | 1 | `addons/quimibond_sgi/models/sgi_complaint.py` |
| [`helpdesk.ticket`](diccionario/helpdesk.ticket.md) | 7 | `addons/quimibond_sgi/models/sgi_complaint.py` |
| [`hr.department`](diccionario/hr.department.md) | 0 | `addons/quimibond_sgi/models/sgi_competence.py` |
| [`hr.employee`](diccionario/hr.employee.md) | 17 | `addons/quimibond_sgi/models/sgi_business_line.py`, `addons/quimibond_sgi/models/sgi_competence.py`, `addons/quimibond_sgi/models/sgi_epp.py`, `addons/quimibond_sgi/models/sgi_hse_records.py`, `addons/quimibond_sgi/models/sgi_integration.py`, `addons/quimibond_sgi/models/sgi_kpi_hr.py`, `addons/quimibond_sgi/models/sgi_my_procedure.py`, `addons/quimibond_sgi/models/sgi_my_procedure_screen.py` |
| [`hr.employee.public`](diccionario/hr.employee.public.md) | 9 | `addons/quimibond_sgi/models/sgi_my_pending.py`, `addons/quimibond_sgi/models/sgi_my_procedure_screen.py` |
| [`hr.job`](diccionario/hr.job.md) | 22 | `addons/quimibond_sgi/models/sgi_archived_filters.py`, `addons/quimibond_sgi/models/sgi_business_line.py`, `addons/quimibond_sgi/models/sgi_catalog.py`, `addons/quimibond_sgi/models/sgi_indicator_ind2.py`, `addons/quimibond_sgi/models/sgi_integration.py`, `addons/quimibond_sgi/models/sgi_my_procedure.py`, `addons/quimibond_sgi/models/sgi_my_procedure_screen.py` |
| [`hr.version`](diccionario/hr.version.md) | 2 | `addons/quimibond_sgi/models/sgi_kpi_hr.py` |
| [`ir.actions.act_window.view`](diccionario/ir.actions.act_window.view.md) | 1 | `addons/quimibond_sgi/models/sgi_diagram_view.py` |
| [`ir.attachment`](diccionario/ir.attachment.md) | 1 | `addons/quimibond_sgi/models/sgi_links.py` |
| [`ir.config_parameter`](diccionario/ir.config_parameter.md) | 0 | `addons/quimibond_sgi/models/sgi_activity_spec.py` |
| [`ir.ui.menu`](diccionario/ir.ui.menu.md) | 0 | `addons/quimibond_sgi/models/sgi_cleanup.py` |
| [`ir.ui.view`](diccionario/ir.ui.view.md) | 1 | `addons/quimibond_sgi/models/sgi_diagram_view.py` |
| [`mail.activity`](diccionario/mail.activity.md) | 3 | `addons/quimibond_sgi/models/sgi_cron.py`, `addons/quimibond_sgi/models/sgi_nonconformity.py` |
| [`maintenance.equipment`](diccionario/maintenance.equipment.md) | 14 | `addons/quimibond_sgi/models/sgi_calibration.py`, `addons/quimibond_sgi/models/sgi_msa.py` |
| [`maintenance.request`](diccionario/maintenance.request.md) | 7 | `addons/quimibond_sgi/models/sgi_checklist.py`, `addons/quimibond_sgi/models/sgi_format_map.py`, `addons/quimibond_sgi/models/sgi_integration.py` |
| [`mrp.bom`](diccionario/mrp.bom.md) | 0 | `addons/quimibond_sgi/models/sgi_links.py` |
| [`mrp.eco`](diccionario/mrp.eco.md) | 6 | `addons/quimibond_sgi_plm/models/mrp_eco.py` |
| [`mrp.production`](diccionario/mrp.production.md) | 2 | `addons/quimibond_sgi/models/sgi_format_map.py`, `addons/quimibond_sgi/models/sgi_integration.py`, `addons/quimibond_sgi/models/sgi_links.py` |
| [`mrp.weigh.roll.wizard`](diccionario/mrp.weigh.roll.wizard.md) | 0 | `addons/quimibond_sgi_pesaje/models/mrp_weigh_wizard.py` |
| [`product.product`](diccionario/product.product.md) | 3 | `addons/quimibond_sgi/models/sgi_integration.py`, `addons/quimibond_sgi/models/sgi_ppap.py`, `addons/quimibond_sgi/models/sgi_sign_record.py` |
| [`product.template`](diccionario/product.template.md) | 5 | `addons/quimibond_sgi/models/sgi_integration.py`, `addons/quimibond_sgi/models/sgi_kpi_sales.py`, `addons/quimibond_sgi/models/sgi_links.py`, `addons/quimibond_sgi/models/sgi_ppap.py`, `addons/quimibond_sgi/models/sgi_sign_record.py` |
| [`project.project`](diccionario/project.project.md) | 21 | `addons/quimibond_sgi/models/sgi_dev_request.py`, `addons/quimibond_sgi/models/sgi_improvement.py` |
| [`project.task`](diccionario/project.task.md) | 10 | `addons/quimibond_sgi/models/sgi_improvement.py`, `addons/quimibond_sgi/models/sgi_links.py` |
| [`project.task.type`](diccionario/project.task.type.md) | 1 | `addons/quimibond_sgi/models/sgi_improvement.py` |
| [`purchase.order`](diccionario/purchase.order.md) | 2 | `addons/quimibond_sgi/models/sgi_format_map.py`, `addons/quimibond_sgi/models/sgi_integration.py`, `addons/quimibond_sgi/models/sgi_links.py`, `addons/quimibond_sgi/models/sgi_sign_record.py` |
| [`quality.alert`](diccionario/quality.alert.md) | 74 | `addons/quimibond_sgi/models/sgi_customer_reply.py`, `addons/quimibond_sgi/models/sgi_format_map.py`, `addons/quimibond_sgi/models/sgi_incident.py`, `addons/quimibond_sgi/models/sgi_indicator.py`, `addons/quimibond_sgi/models/sgi_kpi_quality.py`, `addons/quimibond_sgi/models/sgi_links.py`, `addons/quimibond_sgi/models/sgi_nonconformity.py`, `addons/quimibond_sgi/models/sgi_supplier_nc.py` |
| [`quality.alert.stage`](diccionario/quality.alert.stage.md) | 2 | `addons/quimibond_sgi/models/sgi_nonconformity.py` |
| [`quality.alert.team`](diccionario/quality.alert.team.md) | 1 | `addons/quimibond_sgi/models/sgi_nonconformity.py` |
| [`quality.check`](diccionario/quality.check.md) | 1 | `addons/quimibond_sgi/models/sgi_calibration.py` |
| [`quality.point`](diccionario/quality.point.md) | 9 | `addons/quimibond_sgi/models/sgi_control_plan.py` |
| [`res.company`](diccionario/res.company.md) | 0 | `addons/quimibond_sgi/models/sgi_kpi_account.py` |
| [`res.config.settings`](diccionario/res.config.settings.md) | 36 | `addons/quimibond_sgi/models/sgi_epp_sign.py`, `addons/quimibond_sgi/models/sgi_my_procedure_sign.py`, `addons/quimibond_sgi/models/sgi_settings.py`, `addons/quimibond_sgi_pesaje/models/res_config_settings.py` |
| [`res.partner`](diccionario/res.partner.md) | 14 | `addons/quimibond_sgi/models/sgi_coa.py`, `addons/quimibond_sgi/models/sgi_integration.py`, `addons/quimibond_sgi/models/sgi_ppap.py`, `addons/quimibond_sgi/models/sgi_supplier_eval.py` |
| [`res.users`](diccionario/res.users.md) | 2 | `addons/quimibond_sgi/models/sgi_kpi_hr.py`, `addons/quimibond_sgi/models/sgi_weekly_overdue.py` |
| [`sale.order`](diccionario/sale.order.md) | 5 | `addons/quimibond_sgi/models/sgi_coa.py`, `addons/quimibond_sgi/models/sgi_format_map.py`, `addons/quimibond_sgi/models/sgi_kpi_sales.py`, `addons/quimibond_sgi/models/sgi_sale_commitment.py` |
| [`sign.request`](diccionario/sign.request.md) | 0 | `addons/quimibond_sgi/models/sgi_doc_change_sign.py`, `addons/quimibond_sgi/models/sgi_my_procedure_sign.py` |
| [`sign.request.item`](diccionario/sign.request.item.md) | 0 | `addons/quimibond_sgi/models/sgi_doc_change_sign.py` |
| [`slide.channel`](diccionario/slide.channel.md) | 3 | `addons/quimibond_sgi/models/sgi_sign_elearning.py` |
| [`stock.lot`](diccionario/stock.lot.md) | 0 | `addons/quimibond_sgi/models/sgi_control_plan.py`, `addons/quimibond_sgi/models/sgi_format_map.py`, `addons/quimibond_sgi/models/sgi_sign_record.py` |
| [`stock.picking`](diccionario/stock.picking.md) | 17 | `addons/quimibond_sgi/models/sgi_coa.py`, `addons/quimibond_sgi/models/sgi_format_map.py`, `addons/quimibond_sgi/models/sgi_integration.py`, `addons/quimibond_sgi/models/sgi_kpi_sales.py`, `addons/quimibond_sgi/models/sgi_links.py`, `addons/quimibond_sgi/models/sgi_release.py`, `addons/quimibond_sgi/models/sgi_sign_record.py` |
| [`studio.approval.rule`](diccionario/studio.approval.rule.md) | 1 | `addons/quimibond_sgi_studio/models/sgi_approval_studio.py`, `addons/quimibond_sgi_studio/models/studio_approval_rule_archive.py` |

Modelos propios sin docstring de clase: 83 de 106.
