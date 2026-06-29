/** @odoo-module */
import { patch } from "@web/core/utils/patch";
import { FormController } from "@web/views/form/form_controller";
import { onMounted, onWillDestroy } from "@odoo/owl";

patch(FormController.prototype, {
    setup() {
        super.setup(...arguments);
        this.tejidoAbortController = null;

        onMounted(() => {
            const root = this.model && this.model.root;
            if (!root) return;
            
            if (root.resModel === 'mrp.weigh.roll.wizard' || root.resModel === 'mrp.subproduct.wizard') {
                // Capturamos el clic en la fase inicial del documento para ganarle al backend de Odoo
                this._onXmlScaleButtonClickBound = this._onXmlScaleButtonClick.bind(this);
                document.addEventListener('click', this._onXmlScaleButtonClickBound, true);
            }
        });

        onWillDestroy(() => {
            if (this._onXmlScaleButtonClickBound) {
                document.removeEventListener('click', this._onXmlScaleButtonClickBound, true);
            }
            if (this.tejidoAbortController) {
                this.tejidoAbortController.abort();
            }
        });
    },

    _onXmlScaleButtonClick(ev) {
        const btn = ev.target.closest('.btn_trigger_rhino');
        if (!btn) return;

        // CRUCIAL: Detenemos el evento DE INMEDIATO para que Odoo no viaje a Python ni cierre la ventana
        ev.preventDefault();
        ev.stopPropagation();

        console.log("--> [TEJIDO] ¡Clic interceptado con éxito! Bloqueando viaje a Python para evitar cierre.");

        const root = this.model && this.model.root;
        if (!root || root.data.weighing_mode !== 'iot') {
            console.log("--> Operación cancelada: El modo no es IoT.");
            return;
        }

        btn.disabled = true;
        btn.innerHTML = '⏳ Leyendo báscula...';

        const iotUrl = "https://192-168-100-30.3991e8c5.odoo-iot.com/hw_proxy/scale_read";
        this.tejidoAbortController = new AbortController();

        console.log("--> Conectando con la báscula en planta en: " + iotUrl);

        fetch(iotUrl, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            signal: this.tejidoAbortController.signal,
            body: JSON.stringify({ 
                jsonrpc: "2.0", 
                method: "call", 
                params: {}, 
                id: Math.floor(Math.random() * 1000)
            })
        })
        .then(response => response.json())
        .then(payload => {
            console.log("--> Respuesta cruda de la báscula:", payload);
            const data = payload.result || payload;
            if (data) {
                let weightValue = data.weight !== undefined ? data.weight : data.value;
                weightValue = parseFloat(weightValue);

                if (!isNaN(weightValue) && weightValue >= 0) {
                    root.update({ weight: weightValue });
                    console.log("--> Peso insertado en Odoo:", weightValue);
                }
            }
        })
        .catch(err => {
            console.log("--> Petición finalizada (Esperado en tu entorno local sin hardware):", err.name);
        })
        .finally(() => {
            if (btn) {
                btn.disabled = false;
                btn.innerHTML = '⚖️ Capturar';
            }
        });
    }
});