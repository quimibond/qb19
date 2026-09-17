# qb_memoria — Memoria del contacto en Odoo

La memoria de Quimibond (correo de 52 buzones con adjuntos, pendientes y
señales de demanda extraídas) vive en Supabase. Este módulo la muestra dentro
de Odoo sin copiar correos: en cada contacto, la pestaña **Memoria**.

| Sección | Qué muestra | De dónde |
|---|---|---|
| Resumen | Hilos en 90 días, cuántos esperan respuesta nuestra, último correo, contactos conocidos | `threads`, `contacts` |
| Relación | Resumen de la relación, notas estratégicas, riesgos y oportunidades | `companies` |
| Pendientes detectados en correo | Compromisos de entrega, promesas de pago, solicitudes de documento… con vencimiento y estado (abiertos resaltados) | `email_pending_actions` |
| Hilos recientes | Últimos 15 hilos: asunto (enlace a Gmail del buzón), buzón, mensajes, quién escribió al último, estado | `threads` |
| Demanda mencionada | Producto, cantidad y periodo que el cliente mencionó | `customer_demand_signals` |
| Contactos y ritmo de respuesta | Personas de la empresa, último correo, interacciones, horas promedio de respuesta | `contacts` |

Resolución: por el partner comercial (`companies.odoo_partner_id`) y, si no,
por RFC. Un contacto persona muestra la memoria de su empresa. Caché por
contacto (`memoria_cache`, 6 h) y botón **Actualizar**. Si Supabase no responde,
la pestaña dice qué pasó y la ficha sigue abriendo.

Configuración: usa `quimibond_intelligence.supabase_url` y
`quimibond_intelligence.supabase_service_key` (ya existen en producción).
