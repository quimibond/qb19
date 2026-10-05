<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# Seguridad del SGI

## Grupos (11)

«+» = implica ese grupo; «−» = quita una implicación que ya estaba en la base.

| Grupo | Nombre | Implica | Archivo |
|---|---|---|---|
| `quimibond_sgi.group_sgi_user` | Usuario SGI | `+base.group_user`, `+quality.group_quality_user`, `+documents.group_documents_user`, `+helpdesk.group_helpdesk_user`, `+project.group_project_user`, `−approvals.group_approval_user` | `addons/quimibond_sgi/security/sgi_security.xml` |
| `quimibond_sgi.group_sgi_auditor` | Auditor SGI | `−quimibond_sgi.group_sgi_user`, `+base.group_user` | `addons/quimibond_sgi/security/sgi_security.xml` |
| `quimibond_sgi.group_sgi_manager` | Jefe MAST y SGI | `+quimibond_sgi.group_sgi_user`, `+quimibond_sgi.group_sgi_auditor`, `+quality.group_quality_manager`, `+documents.group_documents_manager`, `+helpdesk.group_helpdesk_manager`, `+project.group_project_manager`, `+approvals.group_approval_manager` | `addons/quimibond_sgi/security/sgi_security.xml` |
| `quimibond_sgi.group_sgi_admin` | Administrador SGI | `+quimibond_sgi.group_sgi_manager` | `addons/quimibond_sgi/security/sgi_security.xml` |
| `quimibond_sgi.group_sgi_director` | Dirección de Operaciones (SGI) | `−quimibond_sgi.group_sgi_manager`, `+quimibond_sgi.group_sgi_user`, `+quimibond_sgi.group_sgi_auditor` | `addons/quimibond_sgi/security/sgi_security.xml` |
| `quimibond_sgi.group_sgi_process_owner` | Dueño de proceso (SGI) | `+quimibond_sgi.group_sgi_user` | `addons/quimibond_sgi/security/sgi_security.xml` |
| `quimibond_sgi.group_sgi_efficiency_capture` | Captura de eficiencias (jefe o supervisor de área) | `+quimibond_sgi.group_sgi_user` | `addons/quimibond_sgi/security/sgi_security.xml` |
| `quimibond_sgi.group_sgi_health` | Salud ocupacional (SGI) | `+quimibond_sgi.group_sgi_user` | `addons/quimibond_sgi/security/sgi_security.xml` |
| `quimibond_sgi.group_sgi_salary` | Salarios de eficiencias (SGI) | `+base.group_user` | `addons/quimibond_sgi/security/sgi_security.xml` |
| `quimibond_sgi.group_sgi_csh` | Comisión de Seguridad e Higiene (SGI) | `+quimibond_sgi.group_sgi_user` | `addons/quimibond_sgi/security/sgi_security.xml` |
| `quimibond_sgi.group_sgi_floor_tablet` | Tableta de planta (SGI) | `+base.group_user` | `addons/quimibond_sgi/security/sgi_security.xml` |

## Permisos por modelo (321 renglones del CSV)

l = leer, e = escribir, c = crear, b = borrar. Es el CSV tal cual; el permiso efectivo suma lo que implican los grupos y lo que quitan las reglas.

| Modelo | Grupo | Permisos | Módulo |
|---|---|---|---|
| `approval.request` | `group_sgi_auditor` | l | quimibond_sgi |
| `documents.document` | `group_sgi_auditor` | l | quimibond_sgi |
| `helpdesk.team` | `group_sgi_auditor` | l | quimibond_sgi |
| `helpdesk.ticket` | `group_sgi_auditor` | l | quimibond_sgi |
| `maintenance.equipment` | `group_sgi_auditor` | l | quimibond_sgi |
| `maintenance.request` | `group_sgi_auditor` | l | quimibond_sgi |
| `mrp.production` | `group_sgi_auditor` | l | quimibond_sgi |
| `mrp.workorder` | `group_sgi_auditor` | l | quimibond_sgi |
| `project.project` | `group_sgi_auditor` | l | quimibond_sgi |
| `project.task` | `group_sgi_auditor` | l | quimibond_sgi |
| `purchase.order` | `group_sgi_auditor` | l | quimibond_sgi |
| `purchase.order.line` | `group_sgi_auditor` | l | quimibond_sgi |
| `quality.alert` | `group_sgi_auditor` | l | quimibond_sgi |
| `quality.alert.stage` | `group_sgi_auditor` | l | quimibond_sgi |
| `quality.alert.team` | `group_sgi_auditor` | l | quimibond_sgi |
| `quality.check` | `group_sgi_auditor` | l | quimibond_sgi |
| `quality.point` | `group_sgi_auditor` | l | quimibond_sgi |
| `sale.order` | `group_sgi_auditor` | l | quimibond_sgi |
| `sale.order.line` | `group_sgi_auditor` | l | quimibond_sgi |
| `sgi.action.line` | `group_sgi_auditor` | l | quimibond_sgi |
| `sgi.action.line` | `group_sgi_manager` | lecb | quimibond_sgi |
| `sgi.action.line` | `group_sgi_user` | lec | quimibond_sgi |
| `sgi.activity.change` | `base.group_user` | lec | quimibond_sgi |
| `sgi.activity.change` | `quimibond_sgi.group_sgi_manager` | lecb | quimibond_sgi |
| `sgi.activity.change.role` | `base.group_user` | lecb | quimibond_sgi |
| `sgi.activity.exec.stat` | `group_sgi_auditor` | l | quimibond_sgi |
| `sgi.activity.exec.stat` | `group_sgi_user` | l | quimibond_sgi |
| `sgi.activity.input` | `group_sgi_auditor` | l | quimibond_sgi |
| `sgi.activity.input` | `group_sgi_manager` | lecb | quimibond_sgi |
| `sgi.activity.input` | `group_sgi_user` | l | quimibond_sgi |
| `sgi.activity.link` | `group_sgi_auditor` | l | quimibond_sgi |
| `sgi.activity.link` | `group_sgi_manager` | lecb | quimibond_sgi |
| `sgi.activity.link` | `group_sgi_user` | l | quimibond_sgi |
| `sgi.activity.role` | `group_sgi_auditor` | l | quimibond_sgi |
| `sgi.activity.role` | `group_sgi_manager` | lecb | quimibond_sgi |
| `sgi.activity.role` | `group_sgi_user` | l | quimibond_sgi |
| `sgi.activity.spec.gap` | `group_sgi_auditor` | l | quimibond_sgi |
| `sgi.activity.spec.gap` | `group_sgi_manager` | lecb | quimibond_sgi |
| `sgi.activity.spec.gap` | `group_sgi_user` | l | quimibond_sgi |
| `sgi.activity.week.stat` | `group_sgi_auditor` | l | quimibond_sgi |
| `sgi.activity.week.stat` | `group_sgi_manager` | lecb | quimibond_sgi |
| `sgi.activity.week.stat` | `group_sgi_user` | l | quimibond_sgi |
| `sgi.acuse.attach.wizard` | `base.group_user` | lec | quimibond_sgi |
| `sgi.alert.source` | `group_sgi_auditor` | l | quimibond_sgi |
| `sgi.alert.source` | `group_sgi_manager` | le | quimibond_sgi |
| `sgi.alert.source` | `group_sgi_user` | l | quimibond_sgi |
| `sgi.area` | `group_sgi_auditor` | l | quimibond_sgi |
| `sgi.area` | `group_sgi_manager` | lecb | quimibond_sgi |
| `sgi.area` | `group_sgi_user` | l | quimibond_sgi |
| `sgi.audit` | `group_sgi_auditor` | lec | quimibond_sgi |
| `sgi.audit` | `group_sgi_manager` | lecb | quimibond_sgi |
| `sgi.audit` | `group_sgi_user` | l | quimibond_sgi |
| `sgi.audit.checklist.line` | `group_sgi_auditor` | lecb | quimibond_sgi |
| `sgi.audit.checklist.line` | `group_sgi_manager` | lecb | quimibond_sgi |
| `sgi.audit.checklist.line` | `group_sgi_user` | l | quimibond_sgi |
| `sgi.audit.finding` | `group_sgi_auditor` | lecb | quimibond_sgi |
| `sgi.audit.finding` | `group_sgi_manager` | lecb | quimibond_sgi |
| `sgi.audit.finding` | `group_sgi_user` | l | quimibond_sgi |
| `sgi.audit.program` | `group_sgi_auditor` | lec | quimibond_sgi |
| `sgi.audit.program` | `group_sgi_manager` | lecb | quimibond_sgi |
| `sgi.audit.program` | `group_sgi_user` | l | quimibond_sgi |
| `sgi.audit.program.line` | `group_sgi_auditor` | lec | quimibond_sgi |
| `sgi.audit.program.line` | `group_sgi_manager` | lecb | quimibond_sgi |
| `sgi.audit.program.line` | `group_sgi_user` | l | quimibond_sgi |
| `sgi.calibration` | `group_sgi_auditor` | l | quimibond_sgi |
| `sgi.calibration` | `group_sgi_manager` | lecb | quimibond_sgi |
| `sgi.calibration` | `group_sgi_user` | lec | quimibond_sgi |
| `sgi.calibration` | `quality.group_quality_user` | lec | quimibond_sgi |
| `sgi.catalog.load.wizard` | `group_sgi_admin` | lecb | quimibond_sgi |
| `sgi.catalog.load.wizard.line` | `group_sgi_admin` | lecb | quimibond_sgi |
| `sgi.checklist.finish` | `base.group_user` | lec | quimibond_sgi |
| `sgi.checklist.line` | `base.group_user` | lec | quimibond_sgi |
| `sgi.checklist.line` | `group_sgi_manager` | lecb | quimibond_sgi |
| `sgi.checklist.template` | `base.group_user` | l | quimibond_sgi |
| `sgi.checklist.template` | `group_sgi_manager` | lecb | quimibond_sgi |
| `sgi.checklist.template` | `maintenance.group_equipment_manager` | lec | quimibond_sgi |
| `sgi.checklist.template.item` | `base.group_user` | l | quimibond_sgi |
| `sgi.checklist.template.item` | `group_sgi_manager` | lecb | quimibond_sgi |
| `sgi.checklist.template.item` | `maintenance.group_equipment_manager` | lecb | quimibond_sgi |
| `sgi.coa.attach.wizard` | `group_sgi_user` | lecb | quimibond_sgi |
| `sgi.coa.attach.wizard` | `quality.group_quality_user` | lecb | quimibond_sgi |
| `sgi.coa.exception.wizard` | `stock.group_stock_user` | lecb | quimibond_sgi |
| `sgi.coa.inbox` | `group_sgi_auditor` | l | quimibond_sgi |
| `sgi.coa.inbox` | `group_sgi_manager` | lecb | quimibond_sgi |
| `sgi.coa.inbox` | `quality.group_quality_user` | lec | quimibond_sgi |
| `sgi.company.fix` | `group_sgi_manager` | lec | quimibond_sgi |
| `sgi.competence.gap` | `group_sgi_auditor` | l | quimibond_sgi |
| `sgi.competence.gap` | `group_sgi_manager` | l | quimibond_sgi |
| `sgi.competence.gap` | `group_sgi_user` | l | quimibond_sgi |
| `sgi.competence.gap` | `hr.group_hr_user` | l | quimibond_sgi |
| `sgi.control.plan` | `group_sgi_auditor` | l | quimibond_sgi |
| `sgi.control.plan` | `group_sgi_manager` | lecb | quimibond_sgi |
| `sgi.control.plan` | `group_sgi_user` | lec | quimibond_sgi |
| `sgi.control.plan` | `quality.group_quality_user` | lec | quimibond_sgi |
| `sgi.csh.finding` | `group_sgi_auditor` | l | quimibond_sgi |
| `sgi.csh.finding` | `group_sgi_csh` | lecb | quimibond_sgi |
| `sgi.csh.finding` | `group_sgi_health` | lecb | quimibond_sgi |
| `sgi.csh.finding` | `group_sgi_manager` | lecb | quimibond_sgi |
| `sgi.csh.inspection` | `group_sgi_auditor` | l | quimibond_sgi |
| `sgi.csh.inspection` | `group_sgi_manager` | lecb | quimibond_sgi |
| `sgi.csh.inspection` | `group_sgi_user` | lec | quimibond_sgi |
| `sgi.deliverable` | `group_sgi_auditor` | l | quimibond_sgi |
| `sgi.deliverable` | `group_sgi_manager` | lecb | quimibond_sgi |
| `sgi.deliverable` | `group_sgi_user` | l | quimibond_sgi |
| `sgi.dev.characteristic` | `base.group_user` | lec | quimibond_sgi |
| `sgi.diagnostic` | `group_sgi_auditor` | lec | quimibond_sgi |
| `sgi.diagnostic` | `group_sgi_manager` | lecb | quimibond_sgi |
| `sgi.diagnostic.line` | `group_sgi_auditor` | lec | quimibond_sgi |
| `sgi.diagnostic.line` | `group_sgi_manager` | lecb | quimibond_sgi |
| `sgi.direction.board` | `group_sgi_auditor` | lecb | quimibond_sgi |
| `sgi.direction.board` | `group_sgi_director` | lecb | quimibond_sgi |
| `sgi.direction.board` | `group_sgi_manager` | lecb | quimibond_sgi |
| `sgi.document.ack` | `group_sgi_auditor` | l | quimibond_sgi |
| `sgi.document.ack` | `group_sgi_manager` | lecb | quimibond_sgi |
| `sgi.document.ack` | `group_sgi_user` | lec | quimibond_sgi |
| `sgi.document.type` | `base.group_user` | l | quimibond_sgi |
| `sgi.document.type` | `group_sgi_admin` | lecb | quimibond_sgi |
| `sgi.document.type` | `group_sgi_auditor` | l | quimibond_sgi |
| `sgi.document.type` | `group_sgi_user` | l | quimibond_sgi |
| `sgi.dropbox.key` | `group_sgi_auditor` | l | quimibond_sgi |
| `sgi.dropbox.key` | `group_sgi_user` | l | quimibond_sgi |
| `sgi.dropbox.progress` | `group_sgi_auditor` | l | quimibond_sgi |
| `sgi.dropbox.progress` | `group_sgi_director` | l | quimibond_sgi |
| `sgi.dropbox.progress` | `group_sgi_manager` | l | quimibond_sgi |
| `sgi.dropbox.progress` | `group_sgi_process_owner` | l | quimibond_sgi |
| `sgi.emergency.drill` | `group_sgi_auditor` | l | quimibond_sgi |
| `sgi.emergency.drill` | `group_sgi_manager` | lecb | quimibond_sgi |
| `sgi.emergency.drill` | `group_sgi_user` | lec | quimibond_sgi |
| `sgi.emergency.plan` | `group_sgi_auditor` | l | quimibond_sgi |
| `sgi.emergency.plan` | `group_sgi_manager` | lecb | quimibond_sgi |
| `sgi.emergency.plan` | `group_sgi_user` | lec | quimibond_sgi |
| `sgi.env.aspect` | `group_sgi_auditor` | l | quimibond_sgi |
| `sgi.env.aspect` | `group_sgi_manager` | lecb | quimibond_sgi |
| `sgi.env.aspect` | `group_sgi_user` | lec | quimibond_sgi |
| `sgi.env.aspect.transfer` | `group_sgi_manager` | lecb | quimibond_sgi |
| `sgi.env.aspect.transfer.line` | `group_sgi_manager` | lecb | quimibond_sgi |
| `sgi.epp.delivery` | `group_sgi_auditor` | l | quimibond_sgi |
| `sgi.epp.delivery` | `group_sgi_manager` | lecb | quimibond_sgi |
| `sgi.epp.delivery` | `group_sgi_user` | lec | quimibond_sgi |
| `sgi.epp.delivery` | `hr.group_hr_user` | lec | quimibond_sgi |
| `sgi.epp.delivery.line` | `base.group_user` | l | quimibond_sgi |
| `sgi.epp.delivery.line` | `hr.group_hr_user` | lecb | quimibond_sgi |
| `sgi.epp.delivery.line` | `quimibond_sgi.group_sgi_manager` | lecb | quimibond_sgi |
| `sgi.floor.tablet` | `group_sgi_auditor` | l | quimibond_sgi |
| `sgi.floor.tablet` | `group_sgi_floor_tablet` | l | quimibond_sgi |
| `sgi.floor.tablet` | `group_sgi_manager` | lecb | quimibond_sgi |
| `sgi.fmea` | `group_sgi_auditor` | l | quimibond_sgi |
| `sgi.fmea` | `group_sgi_manager` | lecb | quimibond_sgi |
| `sgi.fmea` | `group_sgi_user` | lec | quimibond_sgi |
| `sgi.fmea` | `quality.group_quality_user` | lec | quimibond_sgi |
| `sgi.fmea.line` | `group_sgi_auditor` | l | quimibond_sgi |
| `sgi.fmea.line` | `group_sgi_manager` | lecb | quimibond_sgi |
| `sgi.fmea.line` | `group_sgi_user` | lecb | quimibond_sgi |
| `sgi.fmea.line` | `quality.group_quality_user` | lecb | quimibond_sgi |
| `sgi.format.map` | `group_sgi_auditor` | l | quimibond_sgi |
| `sgi.format.map` | `group_sgi_manager` | lecb | quimibond_sgi |
| `sgi.format.map` | `group_sgi_user` | l | quimibond_sgi |
| `sgi.health.record` | `group_sgi_health` | lec | quimibond_sgi |
| `sgi.health.record` | `group_sgi_manager` | lecb | quimibond_sgi |
| `sgi.incident` | `group_sgi_auditor` | l | quimibond_sgi |
| `sgi.incident` | `group_sgi_manager` | lecb | quimibond_sgi |
| `sgi.incident` | `group_sgi_user` | lec | quimibond_sgi |
| `sgi.indicator` | `group_sgi_auditor` | l | quimibond_sgi |
| `sgi.indicator` | `group_sgi_manager` | lecb | quimibond_sgi |
| `sgi.indicator` | `group_sgi_user` | l | quimibond_sgi |
| `sgi.indicator.measure` | `group_sgi_auditor` | l | quimibond_sgi |
| `sgi.indicator.measure` | `group_sgi_manager` | lecb | quimibond_sgi |
| `sgi.indicator.measure` | `group_sgi_user` | lec | quimibond_sgi |
| `sgi.indicator.measure.split` | `group_sgi_auditor` | l | quimibond_sgi |
| `sgi.indicator.measure.split` | `group_sgi_manager` | lecb | quimibond_sgi |
| `sgi.indicator.measure.split` | `group_sgi_user` | lecb | quimibond_sgi |
| `sgi.indicator.step` | `group_sgi_auditor` | l | quimibond_sgi |
| `sgi.indicator.step` | `group_sgi_manager` | lecb | quimibond_sgi |
| `sgi.indicator.step` | `group_sgi_user` | l | quimibond_sgi |
| `sgi.indicator.term` | `group_sgi_admin` | lecb | quimibond_sgi |
| `sgi.indicator.term` | `group_sgi_auditor` | l | quimibond_sgi |
| `sgi.indicator.term` | `group_sgi_user` | l | quimibond_sgi |
| `sgi.instruction.publish` | `quimibond_sgi.group_sgi_manager` | lecb | quimibond_sgi_knowledge |
| `sgi.interested.party` | `group_sgi_auditor` | l | quimibond_sgi |
| `sgi.interested.party` | `group_sgi_manager` | lecb | quimibond_sgi |
| `sgi.interested.party` | `group_sgi_user` | l | quimibond_sgi |
| `sgi.inventory.value` | `group_sgi_auditor` | l | quimibond_sgi |
| `sgi.inventory.value` | `group_sgi_manager` | lecb | quimibond_sgi |
| `sgi.inventory.value` | `group_sgi_user` | l | quimibond_sgi |
| `sgi.job.family` | `base.group_user` | l | quimibond_sgi |
| `sgi.job.family` | `group_sgi_auditor` | l | quimibond_sgi |
| `sgi.job.family` | `group_sgi_manager` | lecb | quimibond_sgi |
| `sgi.legacy.routine` | `group_sgi_auditor` | l | quimibond_sgi |
| `sgi.legacy.routine` | `group_sgi_director` | l | quimibond_sgi |
| `sgi.legacy.routine` | `group_sgi_manager` | lec | quimibond_sgi |
| `sgi.legacy.routine` | `group_sgi_process_owner` | l | quimibond_sgi |
| `sgi.legacy.routine.import` | `group_sgi_manager` | lecb | quimibond_sgi |
| `sgi.legacy.routine.import.line` | `group_sgi_manager` | lecb | quimibond_sgi |
| `sgi.legal.evaluate` | `group_sgi_user` | lecb | quimibond_sgi |
| `sgi.legal.evaluation` | `group_sgi_auditor` | l | quimibond_sgi |
| `sgi.legal.evaluation` | `group_sgi_manager` | lecb | quimibond_sgi |
| `sgi.legal.evaluation` | `group_sgi_user` | lec | quimibond_sgi |
| `sgi.legal.requirement` | `group_sgi_auditor` | l | quimibond_sgi |
| `sgi.legal.requirement` | `group_sgi_manager` | lecb | quimibond_sgi |
| `sgi.legal.requirement` | `group_sgi_user` | le | quimibond_sgi |
| `sgi.lock.date.log` | `group_sgi_auditor` | l | quimibond_sgi |
| `sgi.lock.date.log` | `group_sgi_manager` | l | quimibond_sgi |
| `sgi.lock.date.log` | `group_sgi_user` | l | quimibond_sgi |
| `sgi.loto` | `group_sgi_auditor` | l | quimibond_sgi |
| `sgi.loto` | `group_sgi_manager` | lecb | quimibond_sgi |
| `sgi.loto` | `group_sgi_user` | lec | quimibond_sgi |
| `sgi.loto.energy` | `group_sgi_auditor` | l | quimibond_sgi |
| `sgi.loto.energy` | `group_sgi_manager` | lecb | quimibond_sgi |
| `sgi.loto.energy` | `group_sgi_user` | lecb | quimibond_sgi |
| `sgi.loto.lock` | `group_sgi_auditor` | l | quimibond_sgi |
| `sgi.loto.lock` | `group_sgi_manager` | lecb | quimibond_sgi |
| `sgi.loto.lock` | `group_sgi_user` | lecb | quimibond_sgi |
| `sgi.machine.sheet` | `base.group_user` | l | quimibond_sgi |
| `sgi.machine.sheet` | `mrp.group_mrp_user` | lec | quimibond_sgi |
| `sgi.machine.sheet` | `quimibond_sgi.group_sgi_manager` | lecb | quimibond_sgi |
| `sgi.machine.sheet.param` | `base.group_user` | l | quimibond_sgi |
| `sgi.machine.sheet.param` | `mrp.group_mrp_user` | lecb | quimibond_sgi |
| `sgi.machine.sheet.param` | `quimibond_sgi.group_sgi_manager` | lecb | quimibond_sgi |
| `sgi.machine.sheet.yarn` | `base.group_user` | l | quimibond_sgi |
| `sgi.machine.sheet.yarn` | `mrp.group_mrp_user` | lecb | quimibond_sgi |
| `sgi.machine.sheet.yarn` | `quimibond_sgi.group_sgi_manager` | lecb | quimibond_sgi |
| `sgi.management.review` | `group_sgi_auditor` | l | quimibond_sgi |
| `sgi.management.review` | `group_sgi_director` | lecb | quimibond_sgi |
| `sgi.management.review` | `group_sgi_manager` | lecb | quimibond_sgi |
| `sgi.management.review` | `group_sgi_user` | l | quimibond_sgi |
| `sgi.management.review.agreement` | `group_sgi_auditor` | l | quimibond_sgi |
| `sgi.management.review.agreement` | `group_sgi_director` | lec | quimibond_sgi |
| `sgi.management.review.agreement` | `group_sgi_manager` | lecb | quimibond_sgi |
| `sgi.management.review.agreement` | `group_sgi_user` | l | quimibond_sgi |
| `sgi.mapa.load.wizard` | `quimibond_sgi.group_sgi_admin` | lecb | quimibond_sgi_mapa |
| `sgi.mapa.load.wizard.line` | `quimibond_sgi.group_sgi_admin` | lecb | quimibond_sgi_mapa |
| `sgi.msa.study` | `group_sgi_auditor` | l | quimibond_sgi |
| `sgi.msa.study` | `group_sgi_manager` | lecb | quimibond_sgi |
| `sgi.msa.study` | `group_sgi_user` | lec | quimibond_sgi |
| `sgi.msa.study` | `quality.group_quality_user` | lec | quimibond_sgi |
| `sgi.my.pending` | `base.group_user` | lecb | quimibond_sgi |
| `sgi.my.procedure` | `group_sgi_auditor` | lec | quimibond_sgi |
| `sgi.my.procedure` | `group_sgi_user` | lecb | quimibond_sgi |
| `sgi.my.procedure.check` | `group_sgi_manager` | lecb | quimibond_sgi |
| `sgi.nc.cancel` | `group_sgi_user` | lecb | quimibond_sgi |
| `sgi.nc.force.close` | `group_sgi_manager` | lecb | quimibond_sgi |
| `sgi.nc.force.close` | `group_sgi_user` | l | quimibond_sgi |
| `sgi.norm` | `group_sgi_auditor` | l | quimibond_sgi |
| `sgi.norm` | `group_sgi_manager` | lecb | quimibond_sgi |
| `sgi.norm` | `group_sgi_user` | l | quimibond_sgi |
| `sgi.norm.clause` | `group_sgi_auditor` | l | quimibond_sgi |
| `sgi.norm.clause` | `group_sgi_manager` | lecb | quimibond_sgi |
| `sgi.norm.clause` | `group_sgi_user` | l | quimibond_sgi |
| `sgi.objective` | `group_sgi_auditor` | l | quimibond_sgi |
| `sgi.objective` | `group_sgi_manager` | lecb | quimibond_sgi |
| `sgi.objective` | `group_sgi_user` | l | quimibond_sgi |
| `sgi.policy` | `group_sgi_auditor` | l | quimibond_sgi |
| `sgi.policy` | `group_sgi_manager` | lecb | quimibond_sgi |
| `sgi.policy` | `group_sgi_user` | l | quimibond_sgi |
| `sgi.ppap` | `group_sgi_auditor` | l | quimibond_sgi |
| `sgi.ppap` | `group_sgi_manager` | lecb | quimibond_sgi |
| `sgi.ppap` | `group_sgi_user` | lec | quimibond_sgi |
| `sgi.ppap` | `quality.group_quality_user` | lec | quimibond_sgi |
| `sgi.ppap.element` | `group_sgi_auditor` | l | quimibond_sgi |
| `sgi.ppap.element` | `group_sgi_manager` | lecb | quimibond_sgi |
| `sgi.ppap.element` | `group_sgi_user` | lecb | quimibond_sgi |
| `sgi.ppap.element` | `quality.group_quality_user` | lecb | quimibond_sgi |
| `sgi.ppap.element.template` | `group_sgi_auditor` | l | quimibond_sgi |
| `sgi.ppap.element.template` | `group_sgi_manager` | lecb | quimibond_sgi |
| `sgi.ppap.element.template` | `group_sgi_user` | l | quimibond_sgi |
| `sgi.ppap.element.template` | `quality.group_quality_user` | l | quimibond_sgi |
| `sgi.process` | `group_sgi_auditor` | l | quimibond_sgi |
| `sgi.process` | `group_sgi_manager` | lecb | quimibond_sgi |
| `sgi.process` | `group_sgi_user` | l | quimibond_sgi |
| `sgi.process.activity` | `group_sgi_auditor` | l | quimibond_sgi |
| `sgi.process.activity` | `group_sgi_manager` | lecb | quimibond_sgi |
| `sgi.process.activity` | `group_sgi_user` | l | quimibond_sgi |
| `sgi.process.flow` | `group_sgi_auditor` | l | quimibond_sgi |
| `sgi.process.flow` | `group_sgi_manager` | lecb | quimibond_sgi |
| `sgi.process.flow` | `group_sgi_user` | l | quimibond_sgi |
| `sgi.process.responsibility` | `group_sgi_auditor` | l | quimibond_sgi |
| `sgi.process.responsibility` | `group_sgi_manager` | lecb | quimibond_sgi |
| `sgi.process.responsibility` | `group_sgi_user` | l | quimibond_sgi |
| `sgi.process.stage` | `group_sgi_auditor` | l | quimibond_sgi |
| `sgi.process.stage` | `group_sgi_manager` | lecb | quimibond_sgi |
| `sgi.process.stage` | `group_sgi_user` | l | quimibond_sgi |
| `sgi.risk` | `group_sgi_auditor` | l | quimibond_sgi |
| `sgi.risk` | `group_sgi_manager` | lecb | quimibond_sgi |
| `sgi.risk` | `group_sgi_user` | lec | quimibond_sgi |
| `sgi.risk.category` | `group_sgi_auditor` | l | quimibond_sgi |
| `sgi.risk.category` | `group_sgi_manager` | lecb | quimibond_sgi |
| `sgi.risk.category` | `group_sgi_user` | l | quimibond_sgi |
| `sgi.sign.request.wizard` | `group_sgi_user` | lecb | quimibond_sgi |
| `sgi.staff.efficiency` | `group_sgi_salary` | lecb | quimibond_sgi |
| `sgi.staff.efficiency` | `hr.group_hr_user` | lecb | quimibond_sgi |
| `sgi.staff.efficiency` | `quimibond_sgi.group_sgi_efficiency_capture` | lecb | quimibond_sgi |
| `sgi.staff.efficiency` | `quimibond_sgi.group_sgi_manager` | l | quimibond_sgi |
| `sgi.staff.efficiency.line` | `group_sgi_salary` | lecb | quimibond_sgi |
| `sgi.staff.efficiency.line` | `hr.group_hr_user` | lecb | quimibond_sgi |
| `sgi.staff.efficiency.line` | `quimibond_sgi.group_sgi_efficiency_capture` | lecb | quimibond_sgi |
| `sgi.staff.efficiency.line` | `quimibond_sgi.group_sgi_manager` | l | quimibond_sgi |
| `sgi.supplier.eval` | `group_sgi_auditor` | l | quimibond_sgi |
| `sgi.supplier.eval` | `group_sgi_manager` | lecb | quimibond_sgi |
| `sgi.supplier.eval` | `group_sgi_user` | lec | quimibond_sgi |
| `sgi.supplier.eval` | `purchase.group_purchase_user` | lec | quimibond_sgi |
| `sgi.training.effectiveness` | `base.group_user` | le | quimibond_sgi |
| `sgi.training.effectiveness` | `group_sgi_manager` | le | quimibond_sgi |
| `sgi.training.effectiveness` | `hr.group_hr_user` | le | quimibond_sgi |
| `sgi.work.permit` | `group_sgi_auditor` | l | quimibond_sgi |
| `sgi.work.permit` | `group_sgi_manager` | lecb | quimibond_sgi |
| `sgi.work.permit` | `group_sgi_user` | lec | quimibond_sgi |
| `sgi.work.permit.check` | `group_sgi_auditor` | l | quimibond_sgi |
| `sgi.work.permit.check` | `group_sgi_manager` | lecb | quimibond_sgi |
| `sgi.work.permit.check` | `group_sgi_user` | lecb | quimibond_sgi |
| `sgi.work.permit.skill` | `group_sgi_auditor` | l | quimibond_sgi |
| `sgi.work.permit.skill` | `group_sgi_manager` | lecb | quimibond_sgi |
| `sgi.work.permit.skill` | `group_sgi_user` | l | quimibond_sgi |
| `sign.request` | `group_sgi_auditor` | l | quimibond_sgi |
| `sign.request` | `group_sgi_manager` | l | quimibond_sgi |
| `sign.template` | `group_sgi_manager` | l | quimibond_sgi |
| `slide.channel` | `group_sgi_manager` | le | quimibond_sgi |
| `stock.lot` | `group_sgi_auditor` | l | quimibond_sgi |
| `stock.move` | `group_sgi_auditor` | l | quimibond_sgi |
| `stock.picking` | `group_sgi_auditor` | l | quimibond_sgi |
| `survey.survey` | `group_sgi_manager` | l | quimibond_sgi |
| `survey.user_input` | `group_sgi_auditor` | l | quimibond_sgi |

## Reglas de registro (67)

| Regla | Nombre | Modelo | Dominio | Grupos | Archivo |
|---|---|---|---|---|---|
| `quimibond_sgi.rule_sgi_action_line_manager_all` | SGI: MAST edita cualquier acción | `sgi.action.line` | `[(1, '=', 1)]` | [(4, ref('group_sgi_manager'))] | `addons/quimibond_sgi/security/sgi_security.xml` |
| `quimibond_sgi.rule_sgi_action_line_user_own` | SGI: el usuario edita solo sus acciones | `sgi.action.line` | `['\|', ('responsible_id', '=', user.id), ('create_uid', '=', user.id)]` | [(4, ref('group_sgi_user'))] | `addons/quimibond_sgi/security/sgi_security.xml` |
| `quimibond_sgi.rule_sgi_activity_change_auditor_no_write` | SGI: el Auditor no escribe cambios a actividades | `sgi.activity.change` | `[(0, '=', 1)]` | [(4, ref('group_sgi_auditor'))] | `addons/quimibond_sgi/security/sgi_security.xml` |
| `quimibond_sgi.rule_sgi_activity_change_role_auditor_no_write` | SGI: el Auditor no escribe roles de cambios a actividades | `sgi.activity.change.role` | `[(0, '=', 1)]` | [(4, ref('group_sgi_auditor'))] | `addons/quimibond_sgi/security/sgi_security.xml` |
| `quimibond_sgi.rule_sgi_activity_change_role_user_write` | SGI: el Usuario SGI escribe roles de cambios a actividades (sin cambio) | `sgi.activity.change.role` | `[(1, '=', 1)]` | [(4, ref('group_sgi_user'))] | `addons/quimibond_sgi/security/sgi_security.xml` |
| `quimibond_sgi.rule_sgi_activity_change_user_write` | SGI: el Usuario SGI escribe cambios a actividades (sin cambio) | `sgi.activity.change` | `[(1, '=', 1)]` | [(4, ref('group_sgi_user'))] | `addons/quimibond_sgi/security/sgi_security.xml` |
| `quimibond_sgi.rule_sgi_activity_exec_stat_company` | SGI: Ejecuciones por empresa | `sgi.activity.exec.stat` | `['\|', ('company_id', '=', False), ('company_id', 'in', company_ids)]` |  | `addons/quimibond_sgi/security/sgi_security.xml` |
| `quimibond_sgi.rule_sgi_activity_input_company` | SGI: Lo que recibe cada actividad, por empresa | `sgi.activity.input` | `['\|', ('company_id', '=', False), ('company_id', 'in', company_ids)]` |  | `addons/quimibond_sgi/security/sgi_security.xml` |
| `quimibond_sgi.rule_sgi_activity_link_company` | SGI: Ligas entre actividades por empresa | `sgi.activity.link` | `['\|', ('company_id', '=', False), ('company_id', 'in', company_ids)]` |  | `addons/quimibond_sgi/security/sgi_security.xml` |
| `quimibond_sgi.rule_sgi_activity_role_company` | SGI: Roles de actividad por empresa | `sgi.activity.role` | `['\|', ('company_id', '=', False), ('company_id', 'in', company_ids)]` |  | `addons/quimibond_sgi/security/sgi_security.xml` |
| `quimibond_sgi.rule_sgi_activity_spec_gap_company` | SGI: Faltantes de especificación por empresa | `sgi.activity.spec.gap` | `['\|', ('company_id', '=', False), ('company_id', 'in', company_ids)]` |  | `addons/quimibond_sgi/security/sgi_security.xml` |
| `quimibond_sgi.rule_sgi_activity_week_stat_company` | SGI: Cumplimiento semanal por empresa | `sgi.activity.week.stat` | `['\|', ('company_id', '=', False), ('company_id', 'in', company_ids)]` |  | `addons/quimibond_sgi/security/sgi_security.xml` |
| `quimibond_sgi.rule_sgi_checklist_line_auditor_no_write` | SGI: el Auditor no escribe renglones de checklist | `sgi.checklist.line` | `[(0, '=', 1)]` | [(4, ref('group_sgi_auditor'))] | `addons/quimibond_sgi/security/sgi_security.xml` |
| `quimibond_sgi.rule_sgi_checklist_line_user_write` | SGI: el Usuario SGI escribe renglones de checklist (sin cambio) | `sgi.checklist.line` | `[(1, '=', 1)]` | [(4, ref('group_sgi_user'))] | `addons/quimibond_sgi/security/sgi_security.xml` |
| `quimibond_sgi.rule_sgi_checklist_template_company` | SGI: Plantillas de checklist por empresa | `sgi.checklist.template` | `['\|', ('company_id', '=', False), ('company_id', 'in', company_ids)]` |  | `addons/quimibond_sgi/security/sgi_security.xml` |
| `quimibond_sgi.rule_sgi_coa_inbox_company` | SGI: Bandeja de certificados de análisis por empresa | `sgi.coa.inbox` | `['\|', ('company_id', '=', False), ('company_id', 'in', company_ids)]` |  | `addons/quimibond_sgi/security/sgi_security.xml` |
| `quimibond_sgi.rule_sgi_competence_gap_all` | SGI: RH, MAST y Auditor ven todas las brechas de competencia | `sgi.competence.gap` | `[(1, '=', 1)]` | [(4, ref('hr.group_hr_user')), (4, ref('group_sgi_manager')), (4, ref('group_sgi_auditor'))] | `addons/quimibond_sgi/security/sgi_security.xml` |
| `quimibond_sgi.rule_sgi_competence_gap_user_own` | SGI: el usuario ve sus brechas de competencia y las de su equipo | `sgi.competence.gap` | `['\|', '\|', ('employee_id.user_id', '=', user.id), ('employee_id.parent_id.user_id', '=', user.id), ('department_id.manager_id.user_id', '=', user.id)]` | [(4, ref('group_sgi_user'))] | `addons/quimibond_sgi/security/sgi_security.xml` |
| `quimibond_sgi.rule_sgi_csh_inspection_company` | SGI: Inspecciones de la comisión de seguridad e higiene por empresa | `sgi.csh.inspection` | `['\|', ('company_id', '=', False), ('company_id', 'in', company_ids)]` |  | `addons/quimibond_sgi/security/sgi_security.xml` |
| `quimibond_sgi.rule_sgi_deliverable_company` | SGI: Entregables por empresa | `sgi.deliverable` | `['\|', ('company_id', '=', False), ('company_id', 'in', company_ids)]` |  | `addons/quimibond_sgi/security/sgi_security.xml` |
| `quimibond_sgi.rule_sgi_dev_characteristic_auditor_no_write` | SGI: el Auditor no escribe características de desarrollo | `sgi.dev.characteristic` | `[(0, '=', 1)]` | [(4, ref('group_sgi_auditor'))] | `addons/quimibond_sgi/security/sgi_security.xml` |
| `quimibond_sgi.rule_sgi_dev_characteristic_user_write` | SGI: el Usuario SGI escribe características de desarrollo (sin cambio) | `sgi.dev.characteristic` | `[(1, '=', 1)]` | [(4, ref('group_sgi_user'))] | `addons/quimibond_sgi/security/sgi_security.xml` |
| `quimibond_sgi.rule_sgi_document_type_company` | SGI: Tipos de documento por empresa | `sgi.document.type` | `['\|', ('company_id', '=', False), ('company_id', 'in', company_ids)]` |  | `addons/quimibond_sgi/security/sgi_security.xml` |
| `quimibond_sgi.rule_sgi_dropbox_key_readable_document` | SGI: clave anterior solo de documentos que el usuario puede leer | `sgi.dropbox.key` | `['\|', ('document_id', '=', False), ('document_id', 'any', [])]` |  | `addons/quimibond_sgi/security/sgi_security.xml` |
| `quimibond_sgi.rule_sgi_dropbox_progress_company` | SGI: avance de la transición por empresa | `sgi.dropbox.progress` | `['\|', ('company_id', '=', False), ('company_id', 'in', company_ids)]` |  | `addons/quimibond_sgi/security/sgi_security.xml` |
| `quimibond_sgi.rule_sgi_env_aspect_company` | SGI: Aspectos ambientales por empresa | `sgi.env.aspect` | `['\|', ('company_id', '=', False), ('company_id', 'in', company_ids)]` |  | `addons/quimibond_sgi/security/sgi_security.xml` |
| `quimibond_sgi.rule_sgi_floor_tablet_manager` | SGI: MAST y Auditor ven todas las tabletas | `sgi.floor.tablet` | `[(1, '=', 1)]` | [(4, ref('group_sgi_manager')), (4, ref('group_sgi_auditor'))] | `addons/quimibond_sgi/security/sgi_security.xml` |
| `quimibond_sgi.rule_sgi_floor_tablet_own` | SGI: la tableta lee solo su registro | `sgi.floor.tablet` | `[('user_id', '=', user.id)]` | [(4, ref('group_sgi_floor_tablet'))] | `addons/quimibond_sgi/security/sgi_security.xml` |
| `quimibond_sgi.rule_sgi_health_record_company` | SGI: Expedientes de salud por empresa | `sgi.health.record` | `['\|', ('company_id', '=', False), ('company_id', 'in', company_ids)]` |  | `addons/quimibond_sgi/security/sgi_security.xml` |
| `quimibond_sgi.rule_sgi_incident_auditor_read` | SGI: Auditor y Dirección leen todos los incidentes | `sgi.incident` | `[(1, '=', 1)]` | [(4, ref('group_sgi_auditor'))] | `addons/quimibond_sgi/security/sgi_security.xml` |
| `quimibond_sgi.rule_sgi_incident_sst_all` | SGI: MAST y Salud ocupacional investigan todos los incidentes | `sgi.incident` | `[(1, '=', 1)]` | [(4, ref('group_sgi_manager')), (4, ref('group_sgi_health'))] | `addons/quimibond_sgi/security/sgi_security.xml` |
| `quimibond_sgi.rule_sgi_incident_user_edit_reported` | SGI: el reportante edita su incidente mientras está reportado | `sgi.incident` | `[('state', '=', 'reportado'), '\|', ('reporter_id', '=', user.id), '&', ('create_uid', '=', user.id), ('sgi_pin_tablet_id', '=', False)]` | [(4, ref('group_sgi_user'))] | `addons/quimibond_sgi/security/sgi_security.xml` |
| `quimibond_sgi.rule_sgi_incident_user_read_own` | SGI: el usuario consulta los incidentes que reportó o donde tiene una acción | `sgi.incident` | `['\|', '\|', ('reporter_id', '=', user.id), '&', ('create_uid', '=', user.id), ('sgi_pin_tablet_id', '=', False), ('action_line_ids.responsible_id', '=', user.id…` | [(4, ref('group_sgi_user'))] | `addons/quimibond_sgi/security/sgi_security.xml` |
| `quimibond_sgi.rule_sgi_inventory_value_company` | SGI: Valor del inventario por empresa | `sgi.inventory.value` | `['\|', ('company_id', '=', False), ('company_id', 'in', company_ids)]` |  | `addons/quimibond_sgi/security/sgi_security.xml` |
| `quimibond_sgi.rule_sgi_job_family_company` | SGI: Familias de puestos por empresa | `sgi.job.family` | `['\|', ('company_id', '=', False), ('company_id', 'in', company_ids)]` |  | `addons/quimibond_sgi/security/sgi_security.xml` |
| `quimibond_sgi.rule_sgi_legacy_routine_company` | SGI: Rutinas del procedimiento anterior por empresa | `sgi.legacy.routine` | `['\|', ('company_id', '=', False), ('company_id', 'in', company_ids)]` |  | `addons/quimibond_sgi/security/sgi_security.xml` |
| `quimibond_sgi.rule_sgi_legal_eval_manager_all` | SGI: MAST registra cualquier evaluación legal | `sgi.legal.evaluation` | `[(1, '=', 1)]` | [(4, ref('group_sgi_manager'))] | `addons/quimibond_sgi/security/sgi_security.xml` |
| `quimibond_sgi.rule_sgi_legal_eval_user_own` | SGI: el usuario registra evaluaciones de sus requisitos | `sgi.legal.evaluation` | `[('requirement_id.responsible_id', '=', user.id)]` | [(4, ref('group_sgi_user'))] | `addons/quimibond_sgi/security/sgi_security.xml` |
| `quimibond_sgi.rule_sgi_legal_manager_all` | SGI: MAST evalúa cualquier requisito legal | `sgi.legal.requirement` | `[(1, '=', 1)]` | [(4, ref('group_sgi_manager'))] | `addons/quimibond_sgi/security/sgi_security.xml` |
| `quimibond_sgi.rule_sgi_legal_user_own` | SGI: el usuario evalúa solo sus requisitos legales | `sgi.legal.requirement` | `[('responsible_id', '=', user.id)]` | [(4, ref('group_sgi_user'))] | `addons/quimibond_sgi/security/sgi_security.xml` |
| `quimibond_sgi.rule_sgi_lock_date_log_company` | SGI: Bitácora de fechas de bloqueo por empresa | `sgi.lock.date.log` | `['\|', ('company_id', '=', False), ('company_id', 'in', company_ids)]` |  | `addons/quimibond_sgi/security/sgi_security.xml` |
| `quimibond_sgi.rule_sgi_loto_company` | SGI: Bloqueo y etiquetado por empresa | `sgi.loto` | `['\|', ('company_id', '=', False), ('company_id', 'in', company_ids)]` |  | `addons/quimibond_sgi/security/sgi_security.xml` |
| `quimibond_sgi.rule_sgi_machine_sheet_company` | SGI: Hojas de máquina por empresa | `sgi.machine.sheet` | `['\|', ('company_id', '=', False), ('company_id', 'in', company_ids)]` |  | `addons/quimibond_sgi/security/sgi_security.xml` |
| `quimibond_sgi.rule_sgi_measure_manager_all` | SGI: MAST captura cualquier indicador | `sgi.indicator.measure` | `[(1, '=', 1)]` | [(4, ref('group_sgi_manager'))] | `addons/quimibond_sgi/security/sgi_security.xml` |
| `quimibond_sgi.rule_sgi_measure_split_manager_all` | SGI: MAST recalcula el desglose de cualquier indicador | `sgi.indicator.measure.split` | `[(1, '=', 1)]` | [(4, ref('group_sgi_manager'))] | `addons/quimibond_sgi/security/sgi_security.xml` |
| `quimibond_sgi.rule_sgi_measure_split_user_own` | SGI: el usuario recalcula el desglose de solo sus indicadores | `sgi.indicator.measure.split` | `['\|', ('measure_id.indicator_id.responsible_id', '=', user.id), ('measure_id.indicator_id.process_id.owner_id.user_id', '=', user.id)]` | [(4, ref('group_sgi_user'))] | `addons/quimibond_sgi/security/sgi_security.xml` |
| `quimibond_sgi.rule_sgi_measure_user_own` | SGI: el usuario captura solo sus indicadores | `sgi.indicator.measure` | `['\|', ('indicator_id.responsible_id', '=', user.id), ('indicator_id.process_id.owner_id.user_id', '=', user.id)]` | [(4, ref('group_sgi_user'))] | `addons/quimibond_sgi/security/sgi_security.xml` |
| `quimibond_sgi.rule_sgi_process_activity_company` | SGI: Actividades por empresa | `sgi.process.activity` | `['\|', ('company_id', '=', False), ('company_id', 'in', company_ids)]` |  | `addons/quimibond_sgi/security/sgi_security.xml` |
| `quimibond_sgi.rule_sgi_process_company` | SGI: Procesos por empresa | `sgi.process` | `['\|', ('company_id', '=', False), ('company_id', 'in', company_ids)]` |  | `addons/quimibond_sgi/security/sgi_security.xml` |
| `quimibond_sgi.rule_sgi_process_flow_company` | SGI: Flujos entre procesos por empresa | `sgi.process.flow` | `['\|', ('company_id', '=', False), ('company_id', 'in', company_ids)]` |  | `addons/quimibond_sgi/security/sgi_security.xml` |
| `quimibond_sgi.rule_sgi_process_responsibility_company` | SGI: Responsabilidades por empresa | `sgi.process.responsibility` | `['\|', ('company_id', '=', False), ('company_id', 'in', company_ids)]` |  | `addons/quimibond_sgi/security/sgi_security.xml` |
| `quimibond_sgi.rule_sgi_process_stage_company` | SGI: Etapas por empresa | `sgi.process.stage` | `['\|', ('company_id', '=', False), ('company_id', 'in', company_ids)]` |  | `addons/quimibond_sgi/security/sgi_security.xml` |
| `quimibond_sgi.rule_sgi_risk_manager_all` | SGI: MAST edita cualquier riesgo | `sgi.risk` | `[(1, '=', 1)]` | [(4, ref('group_sgi_manager'))] | `addons/quimibond_sgi/security/sgi_security.xml` |
| `quimibond_sgi.rule_sgi_risk_user_own` | SGI: el usuario edita los riesgos de sus procesos | `sgi.risk` | `['\|', ('process_id.owner_id.user_id', '=', user.id), ('create_uid', '=', user.id)]` | [(4, ref('group_sgi_user'))] | `addons/quimibond_sgi/security/sgi_security.xml` |
| `quimibond_sgi.rule_sgi_staff_eff_capture_edit` | SGI: el jefe de área captura la hoja de su departamento en borrador | `sgi.staff.efficiency` | `[('state', '=', 'borrador'), '\|', ('department_id', 'child_of', user.employee_ids.department_id.ids), ('department_id.manager_id.user_id', '=', user.id)]` | [(4, ref('group_sgi_efficiency_capture'))] | `addons/quimibond_sgi/security/sgi_security.xml` |
| `quimibond_sgi.rule_sgi_staff_eff_capture_read` | SGI: el jefe de área ve las eficiencias de su departamento | `sgi.staff.efficiency` | `['\|', ('department_id', 'child_of', user.employee_ids.department_id.ids), ('department_id.manager_id.user_id', '=', user.id)]` | [(4, ref('group_sgi_efficiency_capture'))] | `addons/quimibond_sgi/security/sgi_security.xml` |
| `quimibond_sgi.rule_sgi_staff_eff_hr_all` | SGI: RH ve y recibe todas las eficiencias | `sgi.staff.efficiency` | `[(1, '=', 1)]` | [(4, ref('hr.group_hr_user')), (4, ref('group_sgi_manager'))] | `addons/quimibond_sgi/security/sgi_security.xml` |
| `quimibond_sgi.rule_sgi_staff_eff_line_capture_edit` | SGI: el jefe de área captura renglones de su hoja en borrador | `sgi.staff.efficiency.line` | `[('sheet_id.state', '=', 'borrador'), '\|', ('sheet_id.department_id', 'child_of', user.employee_ids.department_id.ids), ('sheet_id.department_id.manager_id.use…` | [(4, ref('group_sgi_efficiency_capture'))] | `addons/quimibond_sgi/security/sgi_security.xml` |
| `quimibond_sgi.rule_sgi_staff_eff_line_capture_read` | SGI: el jefe de área ve los renglones de su departamento | `sgi.staff.efficiency.line` | `['\|', ('sheet_id.department_id', 'child_of', user.employee_ids.department_id.ids), ('sheet_id.department_id.manager_id.user_id', '=', user.id)]` | [(4, ref('group_sgi_efficiency_capture'))] | `addons/quimibond_sgi/security/sgi_security.xml` |
| `quimibond_sgi.rule_sgi_staff_eff_line_hr_all` | SGI: RH ve todos los renglones de eficiencias | `sgi.staff.efficiency.line` | `[(1, '=', 1)]` | [(4, ref('hr.group_hr_user')), (4, ref('group_sgi_manager'))] | `addons/quimibond_sgi/security/sgi_security.xml` |
| `quimibond_sgi.rule_sgi_staff_eff_line_payroll_all` | SGI: Salarios de eficiencias ve todos los renglones | `sgi.staff.efficiency.line` | `[(1, '=', 1)]` | [(4, ref('group_sgi_salary'))] | `addons/quimibond_sgi/security/sgi_security.xml` |
| `quimibond_sgi.rule_sgi_staff_eff_payroll_all` | SGI: Salarios de eficiencias ve todas las hojas | `sgi.staff.efficiency` | `[(1, '=', 1)]` | [(4, ref('group_sgi_salary'))] | `addons/quimibond_sgi/security/sgi_security.xml` |
| `quimibond_sgi.rule_sgi_staff_efficiency_company` | SGI: Eficiencia de personal por empresa | `sgi.staff.efficiency` | `['\|', ('company_id', '=', False), ('company_id', 'in', company_ids)]` |  | `addons/quimibond_sgi/security/sgi_security.xml` |
| `quimibond_sgi.rule_sgi_training_effectiveness_all` | SGI: eficacia de la capacitación, RH y Jefe MAST ven todas | `sgi.training.effectiveness` | `[(1, '=', 1)]` | [(4, ref('hr.group_hr_user')), (4, ref('group_sgi_manager'))] | `addons/quimibond_sgi/security/sgi_security.xml` |
| `quimibond_sgi.rule_sgi_training_effectiveness_company` | SGI: eficacia de la capacitación por compañía | `sgi.training.effectiveness` | `[('company_id', 'in', company_ids)]` |  | `addons/quimibond_sgi/security/sgi_security.xml` |
| `quimibond_sgi.rule_sgi_training_effectiveness_own` | SGI: eficacia de la capacitación, solo las que evalúo | `sgi.training.effectiveness` | `[('evaluator_id', '=', user.id)]` | [(4, ref('base.group_user'))] | `addons/quimibond_sgi/security/sgi_security.xml` |
| `quimibond_sgi.rule_sgi_work_permit_company` | SGI: Permisos de trabajo de alto riesgo por empresa | `sgi.work.permit` | `['\|', ('company_id', '=', False), ('company_id', 'in', company_ids)]` |  | `addons/quimibond_sgi/security/sgi_security.xml` |
