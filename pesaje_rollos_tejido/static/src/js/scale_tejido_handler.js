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

        ev.preventDefault();
        ev.stopPropagation();

        console.log("--> [TEJIDO] ¡Clic interceptado con éxito! Bloqueando viaje a Python para evitar cierre.");

        const root = this.model && this.model.root;
        if (!root) return;

        btn.disabled = true;
        btn.innerHTML = '⏳ Leyendo...';

        // BLINDAJE DE URL: Rompe el caché de Chrome sin alterar headers ni romper el CORS
        const iotUrl = "https://192-168-100-30.3991e8c5.odoo-iot.com/hw_proxy/scale_read?_=" + new Date().getTime();
        this.tejidoAbortController = new AbortController();

        console.log("--> Conectando en vivo con la báscula: " + iotUrl);

        fetch(iotUrl, {
            method: "POST",
            headers: { 
                "Content-Type": "application/json"
            },
            signal: this.tejidoAbortController.signal,
            body: JSON.stringify({ 
                jsonrpc: "2.0", 
                method: "call", 
                params: {}, 
                id: Math.floor(Math.random() * 100000)
            })
        })
        .then(response => response.json())
        .then(payload => {
            console.log("--> [TEJIDO] Respuesta cruda completa de la báscula:", payload);
            
            let weightValue = undefined;

            if (payload && payload.result !== undefined) {
                const resData = payload.result;
                
                if (typeof resData === 'object' && resData !== null) {
                    if (resData.value !== undefined && resData.value !== null && resData.value !== 0) {
                        weightValue = resData.value;
                    } else if (resData.weight !== undefined && resData.weight !== null) {
                        weightValue = resData.weight;
                    } else if (resData.net !== undefined && resData.net !== null) {
                        weightValue = resData.net;
                    } else {
                        weightValue = resData.value || resData.weight || 0;
                    }
                } else if (typeof resData === 'number' || typeof resData === 'string') {
                    weightValue = resData;
                }
            }

            const parsedWeight = parseFloat(weightValue);

            if (!isNaN(parsedWeight)) {
                root.update({ weight: parsedWeight });
                console.log("--> [TEJIDO] ¡Peso insertado con éxito en Odoo! Valor real:", parsedWeight);
            } else {
                console.log("--> [TEJIDO] No se pudo parsear el valor extraído:", weightValue);
            }
        })
        .catch(err => {
            console.log("--> Error de comunicación:", err.name);
        })
        .finally(() => {
            if (btn) {
                btn.disabled = false;
                btn.innerHTML = '⚖️ Capturar';
            }
        });
    }
});