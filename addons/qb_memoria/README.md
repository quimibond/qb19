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

### Ficha consolidada (v1.2, RPC `memoria_brief`)

Encima de las tablas crudas, la pestaña muestra lo que la memoria **entiende**
(Fase 3 del diseño de memoria, `docs/memoria-quimibond-diseno.md` en
`quimibond-intelligence`):

| Sección | Qué muestra | De dónde |
|---|---|---|
| Quién la atiende | Buzón interno que más le escribe y el que atiende cada tipo de pendiente, con volumen y % | grafo `kg_edges` (`atiende`, `atiende:<area>`) |
| Lo que sabemos | Hechos con vigencia por categoría (condiciones de pago, precios, producto, logística, calidad, contactos clave, proceso, preferencias, riesgos), con fecha desde la que aplican y sobre quién | `memoria_facts` |
| Conversaciones (resumen de la memoria) | Por conversación: tema, estado (abierto / cerrado / informativo), quién debe responder, resumen de 3-6 frases, pendientes con vencimiento y enlace a Gmail | `memoria_thread_summaries` |

Los resúmenes y hechos los escribe Claude cada 5 minutos sobre las
conversaciones con correo nuevo (Edge Function `memory-consolidate`); una
conversación se resume una sola vez aunque Gmail la tenga en varios buzones
(`threads.conv_key`). Si la ficha no responde, el resto de la pestaña sigue.

Resolución: por el partner comercial (`companies.odoo_partner_id`) y, si no,
por RFC. Un contacto persona muestra la memoria de su empresa. Caché por
contacto (`memoria_cache`, 6 h) y botón **Actualizar**. Si Supabase no responde,
la pestaña dice qué pasó y la ficha sigue abriendo.

Configuración: usa `quimibond_intelligence.supabase_url` y
`quimibond_intelligence.supabase_service_key` (ya existen en producción).

## Dueños aprendidos (quién atiende a quién)

En Quimibond el correo sale de buzones funcionales (logistica@, cxcobrar@,
innovacion@, comprasplanta@…), así que "quién es el encargado" se aprende en
dos pasos:

1. **La memoria cuenta.** La vista `memoria_encargados` en Supabase da, por
   empresa, el buzón interno que más le escribe (180 días) y, por empresa y
   área, el buzón que atendió sus pendientes (365 días; promesa de pago →
   finanzas, entrega/cotización/documento → comercial, RFQ → compras).
   Se cree una señal con ≥ 3 correos y ≥ 40 % del total.
2. **Odoo pone la persona.** Contactos → Configuración → *Buzones (memoria)*
   dice quién está detrás de cada buzón compartido; si el buzón es el login de
   un usuario se resuelve solo. El cron nocturno escribe en el contacto
   comercial *Encargado (memoria)* con la evidencia y los encargados por área.
   *Fijado a mano* protege una asignación manual.

API: `partner.memoria_owner_for(area)` → `res.users` (área con señal, si no
el general, si no vacío). Las obligaciones (`qb_obligation`) lo usan como
primer criterio para el dueño.
