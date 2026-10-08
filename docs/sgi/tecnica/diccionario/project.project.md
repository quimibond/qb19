<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `project.project`

Modelo de otra app que el SGI extiende.

Archivos: `addons/quimibond_sgi/models/sgi_dev_analysis.py`, `addons/quimibond_sgi/models/sgi_dev_board.py`, `addons/quimibond_sgi/models/sgi_dev_characteristic.py`, `addons/quimibond_sgi/models/sgi_dev_measure.py`, `addons/quimibond_sgi/models/sgi_dev_process.py`, `addons/quimibond_sgi/models/sgi_dev_product.py`, `addons/quimibond_sgi/models/sgi_dev_project.py`, `addons/quimibond_sgi/models/sgi_dev_request.py`, `addons/quimibond_sgi/models/sgi_dev_start.py`, `addons/quimibond_sgi/models/sgi_improvement.py`.

## Campos (97)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `sgi_dev_analysis_by_id` | Many2one | Análisis capturado por | Quién capturó el resultado del análisis (se llena solo). |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_dev_measure.py:128` |
| `sgi_dev_analysis_date` | Datetime | Análisis capturado el | Cuándo se capturó el resultado del análisis (se llena solo). |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_measure.py:126` |
| `sgi_dev_analysis_result` | Selection | Resultado del análisis | Producto de línea: un artículo existente cumple todo, se cotiza ese y el proyecto cierra sin FT. Producto nuevo: sigue el desarrollo. No factible: cierra con motivo. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_project.py:296` |
| `sgi_dev_application_id` | Many2one | Aplicación | Para qué se va a usar el producto (lista). |  | `sgi.dev.option` |  |  | `addons/quimibond_sgi/models/sgi_dev_project.py:257` |
| `sgi_dev_approved_by_id` | Many2one | Aprobó (Dirección de Operaciones) | Persona de Dirección de Operaciones que aprueba la solicitud de desarrollo. |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_dev_request.py:78` |
| `sgi_dev_approved_date` | Datetime | Aprobada el | Cuándo se firmó «Aprobó» en la solicitud de desarrollo (se llena solo). |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_measure.py:130` |
| `sgi_dev_base_product_id` | Many2one | Artículo de línea o base | Artículo existente que cumple la solicitud (producto de línea) o que sirve de base al desarrollo nuevo. |  | `product.product` |  |  | `addons/quimibond_sgi/models/sgi_dev_project.py:301` |
| `sgi_dev_code_acabado_code` | Char | Código acabado | Código propuesto del acabado (J). |  |  | compute `_compute_sgi_dev_codes`, sin guardar |  | `addons/quimibond_sgi/models/sgi_dev_product.py:165` |
| `sgi_dev_code_acabado_id` | Many2one | Acabado (15-16) | Posiciones 15 y 16, opcionales: el acabado especial. |  | `ficha.tecnica.clave.codigo` |  |  | `addons/quimibond_sgi/models/sgi_dev_product.py:154` |
| `sgi_dev_code_ancho` | Integer | Ancho acabado (12-14, cm) | Posiciones 12 a 14 del acabado: ancho de tela abierta en cm. Se toma de la tabla. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_product.py:148` |
| `sgi_dev_code_ancho_crudo` | Integer | Ancho crudo (cm) | Ancho de tela cruda en cm: posiciones 12 a 14 del crudo y del teñido. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_product.py:151` |
| `sgi_dev_code_color_id` | Many2one | Color (10-11) | Posiciones 10 y 11: color del producto terminado. |  | `ficha.tecnica.clave.codigo` |  |  | `addons/quimibond_sgi/models/sgi_dev_product.py:145` |
| `sgi_dev_code_composicion_id` | Many2one | Composición (1) | Posición 1 del código: la composición de la tela. |  | `ficha.tecnica.clave.codigo` |  |  | `addons/quimibond_sgi/models/sgi_dev_product.py:131` |
| `sgi_dev_code_crudo` | Char | Código crudo | Código propuesto del crudo (H). |  |  | compute `_compute_sgi_dev_codes`, sin guardar |  | `addons/quimibond_sgi/models/sgi_dev_product.py:161` |
| `sgi_dev_code_dibujo_id` | Many2one | Dibujo (2) | Posición 2 del código: el dibujo o ligamento. |  | `ficha.tecnica.clave.codigo` |  |  | `addons/quimibond_sgi/models/sgi_dev_product.py:134` |
| `sgi_dev_code_galga` | Integer | Galga (7-8) | Galga de la máquina (14, 16, 18…); el código lleva su rango. Se toma de la tabla. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_product.py:142` |
| `sgi_dev_code_hilo_id` | Many2one | Tipo de hilo (6) | Posición 6: hilo natural, preteñido o reciclado. |  | `ficha.tecnica.clave.codigo` |  |  | `addons/quimibond_sgi/models/sgi_dev_product.py:139` |
| `sgi_dev_code_peso` | Integer | Peso (3-5, g/m²) | Posiciones 3 a 5: masa por unidad de área en g/m². Se toma de la tabla. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_product.py:137` |
| `sgi_dev_code_tenido` | Boolean | Lleva teñido | Si la ruta tiene teñido se crea también el artículo I. Se propone cuando el color no es natural. |  |  | compute `_compute_sgi_dev_code_tenido`, guardado |  | `addons/quimibond_sgi/models/sgi_dev_product.py:157` |
| `sgi_dev_code_tenido_code` | Char | Código teñido | Código propuesto del teñido (I). |  |  | compute `_compute_sgi_dev_codes`, sin guardar |  | `addons/quimibond_sgi/models/sgi_dev_product.py:163` |
| `sgi_dev_consumption_annual` | Float | Consumo anual | Consumo anual estimado, en la unidad elegida. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_project.py:262` |
| `sgi_dev_consumption_annual_m` | Float | Consumo anual (m) | Consumo anual en metros (las yardas se convierten; en kg queda vacío). |  |  | compute `_compute_sgi_dev_consumption`, guardado |  | `addons/quimibond_sgi/models/sgi_dev_project.py:269` |
| `sgi_dev_consumption_annual_yd` | Float | Consumo anual (yd) | Consumo anual en yardas (los metros se convierten; en kg queda vacío). |  |  | compute `_compute_sgi_dev_consumption`, guardado |  | `addons/quimibond_sgi/models/sgi_dev_project.py:272` |
| `sgi_dev_consumption_monthly` | Float | Consumo mensual | Consumo anual entre doce, en la misma unidad (calculado). |  |  | compute `_compute_sgi_dev_consumption`, guardado |  | `addons/quimibond_sgi/models/sgi_dev_project.py:266` |
| `sgi_dev_consumption_uom` | Selection | Unidad del consumo | Unidad del consumo anual: metros, yardas o kilogramos. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_project.py:264` |
| `sgi_dev_contact_id` | Many2one | Contacto del cliente | Persona del cliente que hizo la solicitud. |  | `res.partner` |  |  | `addons/quimibond_sgi/models/sgi_dev_project.py:243` |
| `sgi_dev_currency_id` | Many2one | Moneda del precio | Moneda del precio objetivo del desarrollo. |  | `res.currency` |  |  | `addons/quimibond_sgi/models/sgi_dev_request.py:51` |
| `sgi_dev_customer_property` | Selection | Propiedad del cliente recibida | Qué entregó el cliente para el desarrollo (muestra, especificación, ambas o nada). Es propiedad del cliente y se resguarda. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_request.py:56` |
| `sgi_dev_customer_status` | Selection | Cliente actual o prospecto | Actual si el cliente tiene pedidos de venta confirmados; prospecto si no. |  |  | compute `_compute_sgi_dev_customer_status`, sin guardar |  | `addons/quimibond_sgi/models/sgi_dev_project.py:252` |
| `sgi_dev_date` | Date | Fecha de solicitud | Fecha en que se recibió la solicitud de desarrollo. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_request.py:36` |
| `sgi_dev_deviation_count` | Integer | Fuera del control interno | Renglones cuya corrida cumple al cliente pero sale del margen interno: se embarca con aviso a Calidad y a Diseño de Procesos. |  |  | compute `_compute_sgi_dev_counts`, guardado |  | `addons/quimibond_sgi/models/sgi_dev_request.py:69` |
| `sgi_dev_feasibility_ids` | One2many | Checklist de factibilidad |  |  | `sgi.dev.feasibility` |  |  | `addons/quimibond_sgi/models/sgi_dev_analysis.py:303` |
| `sgi_dev_feasibility_no` | Integer | Factibilidad en «No» | Renglones del checklist contestados con «No». |  |  | compute `_compute_sgi_dev_feasibility`, guardado |  | `addons/quimibond_sgi/models/sgi_dev_analysis.py:306` |
| `sgi_dev_feasibility_pending` | Integer | Factibilidad pendiente | Renglones del checklist sin contestar. |  |  | compute `_compute_sgi_dev_feasibility`, guardado |  | `addons/quimibond_sgi/models/sgi_dev_analysis.py:304` |
| `sgi_dev_format_code` | Char | Formato |  |  |  | compute `_compute_sgi_dev_format_code`, sin guardar |  | `addons/quimibond_sgi/models/sgi_dev_request.py:35` |
| `sgi_dev_hours_dev` | Float | Horas de desarrollo | Horas calendario desde la solicitud sin contar las esperas de materia prima. |  |  | compute `_compute_sgi_dev_clocks`, sin guardar |  | `addons/quimibond_sgi/models/sgi_dev_project.py:312` |
| `sgi_dev_hours_mp` | Float | Horas de materia prima | Horas calendario acumuladas esperando materia prima. |  |  | compute `_compute_sgi_dev_clocks`, sin guardar |  | `addons/quimibond_sgi/models/sgi_dev_project.py:314` |
| `sgi_dev_hours_stage` | Float | Horas en la etapa actual | Horas de desarrollo en la etapa en la que está el proyecto. |  |  | compute `_compute_sgi_dev_clocks`, sin guardar |  | `addons/quimibond_sgi/models/sgi_dev_project.py:316` |
| `sgi_dev_lab_open_count` | Integer | Laboratorio en curso | Solicitudes solicitadas o autorizadas sin medir. |  |  | compute `_compute_sgi_dev_lab`, sin guardar |  | `addons/quimibond_sgi/models/sgi_dev_analysis.py:312` |
| `sgi_dev_lab_pending_count` | Integer | Pendientes de laboratorio | Renglones marcados para medir en la muestra del cliente que aún no tienen resultado. |  |  | compute `_compute_sgi_dev_counts`, guardado |  | `addons/quimibond_sgi/models/sgi_dev_request.py:73` |
| `sgi_dev_lab_request_count` | Integer | Número de solicitudes de laboratorio | Cuántas solicitudes de pruebas tiene el proyecto. |  |  | compute `_compute_sgi_dev_lab`, sin guardar |  | `addons/quimibond_sgi/models/sgi_dev_analysis.py:309` |
| `sgi_dev_lab_request_ids` | One2many | Solicitudes de laboratorio |  |  | `sgi.dev.lab.request` |  |  | `addons/quimibond_sgi/models/sgi_dev_analysis.py:308` |
| `sgi_dev_lamination_id` | Many2one | Tipo de laminado | Tipo de laminado que lleva el producto, si aplica (lista). |  | `sgi.dev.option` |  |  | `addons/quimibond_sgi/models/sgi_dev_project.py:275` |
| `sgi_dev_legal_ids` | Many2many | Requisitos legales y reglamentarios | Requisitos legales y reglamentarios que aplican (lista, no texto). |  | `sgi.dev.option` |  |  | `addons/quimibond_sgi/models/sgi_dev_project.py:278` |
| `sgi_dev_line_count` | Integer | Características | Renglones de la tabla de características del proyecto. |  |  | compute `_compute_sgi_dev_counts`, guardado |  | `addons/quimibond_sgi/models/sgi_dev_request.py:64` |
| `sgi_dev_line_ids` | One2many | Características del producto |  |  | `sgi.dev.characteristic` |  |  | `addons/quimibond_sgi/models/sgi_dev_request.py:63` |
| `sgi_dev_line_key` | Selection | Línea | Línea de producción del desarrollo; define el checklist de factibilidad. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_analysis.py:301` |
| `sgi_dev_market_id` | Many2one | Mercado | Mercado al que va el producto (lista). |  | `sgi.dev.option` |  |  | `addons/quimibond_sgi/models/sgi_dev_project.py:282` |
| `sgi_dev_missing_partner` | Boolean | Falta el cliente | Desarrollo de origen cliente sin cliente capturado. |  |  | compute `_compute_sgi_dev_missing_partner`, sin guardar |  | `addons/quimibond_sgi/models/sgi_dev_board.py:23` |
| `sgi_dev_mp_line_ids` | One2many | Materia prima de la muestra |  |  | `sgi.dev.mp.line` |  |  | `addons/quimibond_sgi/models/sgi_dev_start.py:81` |
| `sgi_dev_mp_missing_count` | Integer | Materias primas que faltan |  |  |  | compute `_compute_sgi_dev_mp_missing_count`, sin guardar |  | `addons/quimibond_sgi/models/sgi_dev_start.py:82` |
| `sgi_dev_mp_pending` | Boolean | Materia prima pendiente | Hay una espera de materia prima abierta: el reloj de desarrollo está detenido. |  |  | compute `_compute_sgi_dev_clocks`, sin guardar |  | `addons/quimibond_sgi/models/sgi_dev_project.py:310` |
| `sgi_dev_mp_wait_ids` | One2many | Esperas de materia prima |  |  | `sgi.dev.mp.wait` |  |  | `addons/quimibond_sgi/models/sgi_dev_project.py:309` |
| `sgi_dev_norms` | Char | Norma(s) a cumplir |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_request.py:54` |
| `sgi_dev_not_feasible_reason_id` | Many2one | Motivo de no factibilidad | Por qué no es factible (lista). |  | `sgi.dev.option` |  |  | `addons/quimibond_sgi/models/sgi_dev_project.py:304` |
| `sgi_dev_origin` | Selection | Origen | Quién pide el desarrollo: un cliente o alguien de Dirección. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_project.py:240` |
| `sgi_dev_other` | Text | Otras características |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_request.py:62` |
| `sgi_dev_out_of_spec_count` | Integer | No conformes en corrida | Renglones cuya corrida quedó fuera de lo que pide el cliente. |  |  | compute `_compute_sgi_dev_counts`, guardado |  | `addons/quimibond_sgi/models/sgi_dev_request.py:66` |
| `sgi_dev_packaging` | Text | Datos en la etiqueta y empaque |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_request.py:55` |
| `sgi_dev_prepared_by_id` | Many2one | Elaboró (Diseño y Desarrollo) | Persona de Diseño y Desarrollo que elaboró la solicitud. |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_dev_request.py:76` |
| `sgi_dev_product_crudo_id` | Many2one | Artículo crudo | Artículo crudo (H) del desarrollo. |  | `product.product` |  |  | `addons/quimibond_sgi/models/sgi_dev_product.py:167` |
| `sgi_dev_product_id` | Many2one | Artículo en desarrollo | Artículo generado para el desarrollo (lo crea el generador de código). Su referencia interna forma parte del nombre del proyecto. |  | `product.product` |  |  | `addons/quimibond_sgi/models/sgi_dev_project.py:232` |
| `sgi_dev_product_name` | Char | Producto pedido | Cómo llama el cliente al producto mientras no hay código de artículo. Forma parte del nombre del proyecto. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_project.py:229` |
| `sgi_dev_product_tenido_id` | Many2one | Artículo teñido | Artículo teñido (I) del desarrollo. |  | `product.product` |  |  | `addons/quimibond_sgi/models/sgi_dev_product.py:169` |
| `sgi_dev_program` | Char | Programa | Programa o plataforma del cliente para el que es el producto (nombre). |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_project.py:255` |
| `sgi_dev_program_years` | Float | Tiempo de programa (años) | Cuántos años durará el programa del cliente. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_project.py:260` |
| `sgi_dev_requester` | Char | Nombre del solicitante |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_request.py:38` |
| `sgi_dev_requester_employee_id` | Many2one | Solicitante interno | Quien pide el desarrollo cuando es interno. |  | `hr.employee` |  |  | `addons/quimibond_sgi/models/sgi_dev_project.py:245` |
| `sgi_dev_required_date` | Date | Fecha requerida | Fecha en que el cliente necesita el producto o la muestra. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_project.py:284` |
| `sgi_dev_requisition_count` | Integer |  |  |  |  | compute `_compute_sgi_dev_requisition_count`, sin guardar |  | `addons/quimibond_sgi/models/sgi_dev_start.py:86` |
| `sgi_dev_requisition_ids` | One2many | Requisiciones a Compras |  |  | `approval.request` |  |  | `addons/quimibond_sgi/models/sgi_dev_start.py:84` |
| `sgi_dev_review_date` | Datetime | Revisado el |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_analysis.py:319` |
| `sgi_dev_review_note` | Char | Motivo del regreso | Qué falta cuando Ventas regresa el análisis. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_analysis.py:320` |
| `sgi_dev_review_state` | Selection | Revisión de Ventas | Ventas aprueba juntos el análisis de Diseño de Producto y la factibilidad de Diseño de Procesos. Sin aprobación no se cotiza. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_analysis.py:314` |
| `sgi_dev_reviewed_by_id` | Many2one | Revisó |  |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_dev_analysis.py:318` |
| `sgi_dev_revision` | Integer | Revisión | Revisión del desarrollo. Sube con «Subir revisión» cuando el cliente ajusta lo que pidió; cada cambio queda en la bitácora. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_project.py:235` |
| `sgi_dev_revision_ids` | One2many | Bitácora de revisiones |  |  | `sgi.dev.revision` |  |  | `addons/quimibond_sgi/models/sgi_dev_project.py:238` |
| `sgi_dev_salesperson_id` | Many2one | Vendedor | Vendedor que atiende al cliente en este desarrollo. |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_dev_project.py:250` |
| `sgi_dev_sample_folder` | Char | Ubicación en carpeta | Carpeta y posición donde se guarda el recorte tamaño carta de la muestra. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_project.py:293` |
| `sgi_dev_sample_handed` | Date | Muestra entregada a Diseño | Fecha en que Ventas entregó la muestra a Diseño de Producto. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_project.py:291` |
| `sgi_dev_sample_kg` | Float | Cantidad de la muestra (kg) | Cantidad de muestra pedida, en kilogramos. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_request.py:43` |
| `sgi_dev_sample_m` | Float | Cantidad de la muestra (m) | Cantidad de muestra pedida, en metros. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_request.py:41` |
| `sgi_dev_sample_received` | Date | Muestra recibida | Fecha en que llegó la muestra física del cliente (a nombre de Ventas). |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_project.py:289` |
| `sgi_dev_spec` | Text | Especificación del cliente |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_request.py:40` |
| `sgi_dev_spec_number` | Char | Número de especificación | Número o clave de la especificación del cliente. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_project.py:286` |
| `sgi_dev_stage_key` | Char | Clave de la etapa | Clave interna de la etapa de avance del desarrollo (solicitud, analisis…). |  |  | compute `_compute_sgi_dev_stage_key`, sin guardar |  | `addons/quimibond_sgi/models/sgi_dev_project.py:225` |
| `sgi_dev_stage_log_ids` | One2many | Reloj por etapa |  |  | `sgi.dev.stage.log` |  |  | `addons/quimibond_sgi/models/sgi_dev_project.py:308` |
| `sgi_dev_stage_seq` | Integer | Orden de la etapa | Posición de la etapa de avance; 0 si la etapa no es de desarrollo. |  |  | compute `_compute_sgi_dev_stage_key`, sin guardar |  | `addons/quimibond_sgi/models/sgi_dev_project.py:227` |
| `sgi_dev_target_price` | Monetary | Precio objetivo |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_request.py:50` |
| `sgi_dev_team_id` | Many2one | Equipo de ventas | Equipo de ventas que atiende la solicitud (Industrial, Confección…). |  | `crm.team` |  |  | `addons/quimibond_sgi/models/sgi_dev_project.py:248` |
| `sgi_dev_type` | Selection | Tipo de desarrollo | Tipo de desarrollo que se solicita; define qué datos pide la solicitud. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_request.py:32` |
| `sgi_dev_use` | Text | Descripción y uso del producto |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_request.py:39` |
| `sgi_dev_volume` | Float | Volumen estimado | Volumen mensual que el cliente estima comprar si el desarrollo se aprueba. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_request.py:45` |
| `sgi_dev_volume_uom` | Selection | Unidad del volumen | Unidad del volumen estimado: metros o kilogramos por mes. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_request.py:48` |
| `sgi_ft_folio` | Char | Folio FT | Folio del desarrollo (FT-039-2026). Lo asigna la secuencia al pasar a «Muestra»; un folio histórico se puede capturar a mano. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_project.py:222` |
| `sgi_is_ft` | Boolean | Desarrollo de producto | El proyecto es un desarrollo de producto (procedimiento C1): habilita las pestañas de desarrollo, el folio FT, las etapas de avance y las mediciones del SGI. Las plantillas de Diseño y Desarrollo ya lo traen marcado. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_request.py:27` |
| `sgi_is_improvement` | Boolean | Proyecto de mejora SGI |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_improvement.py:9` |

## Métodos públicos (27)

| Método | Qué hace (docstring) |
|---|---|
| `action_sgi_dev_approve_request` | Firma «Aprobó» de la solicitud de desarrollo (Dirección de Operaciones). Es el botón que aprueba C1.07; la fecha se sella sola (``sgi_dev_approved_date``). |
| `action_sgi_dev_assign_folio` | Asigna el folio FT de la secuencia anual (FT-001-2027) si no lo tiene. |
| `action_sgi_dev_close_as_line_product` | Un artículo existente cumple todo: se cotiza ese y el proyecto cierra sin FT. |
| `action_sgi_dev_close_not_feasible` | — |
| `action_sgi_dev_code_from_table` | Toma peso, galga y ancho de la tabla de características (nominal del cliente). |
| `action_sgi_dev_find_similar` | — |
| `action_sgi_dev_generate_products` | Crea (o liga, si el código ya existe) los artículos de la ruta: crudo, teñido y acabado. |
| `action_sgi_dev_load_feasibility` | — |
| `action_sgi_dev_load_lines` | Propone las características del tipo desde el catálogo (``sgi.dev.characteristic.template``); solo agrega las que faltan. |
| `action_sgi_dev_mp_check` | — |
| `action_sgi_dev_mp_requisition` | Requisición a Compras con la materia prima que falta (una por clic). |
| `action_sgi_dev_mp_wait_start` | — |
| `action_sgi_dev_mp_wait_stop` | — |
| `action_sgi_dev_new_revision` | Sube la revisión del desarrollo (el cliente ajustó lo que pidió) y lo anota en la bitácora. |
| `action_sgi_dev_open_lab_requests` | — |
| `action_sgi_dev_print` | — |
| `action_sgi_dev_print_sample_label` | — |
| `action_sgi_dev_request_lab_tests` | — |
| `action_sgi_dev_review_approve` | — |
| `action_sgi_dev_review_return` | — |
| `action_sgi_dev_view_requisitions` | — |
| `action_sgi_dev_view_tasks` | — |
| `action_view_tasks` | La tarjeta de un desarrollo abre su ficha. Con ``sgi_dev_force_tasks`` (botón de la ficha, enlaces «Tareas») se abren las tareas como siempre. |
| `create` | — |
| `cron_sgi_dev_mp_wait` | — |
| `sgi_dev_format_info` | 'F-P-D01-18 · Rev. 02' para el pie del PDF (clave y revisión vivas del documento ligado al mapeo). |
| `write` | — |
