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

        // Bloqueamos el viaje a Python para evitar que Odoo cierre la ventana modal
        ev.preventDefault();
        ev.stopPropagation();

        console.log("--> [TEJIDO] ¡Clic interceptado con éxito! Bloqueando viaje a Python para evitar cierre.");

        const root = this.model && this.model.root;
        btn.disabled = true;
        btn.innerHTML = '⏳ Leyendo báscula...';

        const iotUrl = "https://192-168-100-30.3991e8c5.odoo-iot.com/hw_proxy/scale_read";
        this.tejidoAbortController = new AbortController();

        fetch(iotUrl, {
            method: "POST",
            // LAS CABECERAS QUE MATAN LA CACHÉ DE CHROME DE RAÍZ:
            headers: { 
                "Content-Type": "application/json",
                "Cache-Control": "no-cache, no-store, must-revalidate",
                "Pragma": "no-cache",
                "Expires": "0"
            },
            cache: "no-store", // Forzar al navegador a no almacenar el resultado
            signal: this.tejidoAbortController.signal,
            body: JSON.stringify({ 
                jsonrpc: "2.0", 
                method: "call", 
                params: {}, 
                id: Math.floor(Math.random() * 1000) // ID dinámico para romper cachés del servidor
            })
        })
        .then(response => response.json())
        .then(payload => {
            console.log("--> [TEJIDO] Respuesta cruda completa de la báscula:", payload);
            
            let weightValue = undefined;

            if (payload && payload.result !== undefined) {
                // Si 'result' es directamente un número (formato plano)
                if (typeof payload.result === 'number' || (!isNaN(parseFloat(payload.result)) && typeof payload.result !== 'object')) {
                    weightValue = parseFloat(payload.result);
                } 
                // Si 'result' es un diccionario/objeto con claves internas
                else if (typeof payload.result === 'object' && payload.result !== null) {
                    const resData = payload.result;
                    weightValue = resData.weight !== undefined ? resData.weight : (resData.value !== undefined ? resData.value : resData.net);
                }
            }

            weightValue = parseFloat(weightValue);

            // Inyectamos el número final extraído en el recuadro gigante de Odoo
            if (!isNaN(weightValue) && weightValue >= 0) {
                root.update({ weight: weightValue });
                console.log("--> [TEJIDO] ¡Peso insertado con éxito en Odoo! Valor:", weightValue);
            } else {
                console.log("--> [TEJIDO] No se pudo extraer un peso numérico válido.");
            }
        })
        .catch(err => {
            console.log("--> Petición finalizada o error de red:", err.name);
        })
        .finally(() => {
            if (btn) {
                btn.disabled = false;
                btn.innerHTML = '⚖️ Capturar';
            }
        });
    }
});