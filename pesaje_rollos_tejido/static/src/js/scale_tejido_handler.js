/** @odoo-module */
import { patch } from "@web/core/utils/patch";
import { FormController } from "@web/views/form/form_controller";
import { onMounted, onWillDestroy } from "@odoo/owl";

patch(FormController.prototype, {
    setup() {
        super.setup(...arguments);
        this.tejidoScaleInterval = null;
        this.tejidoAbortController = null; // Controlador para abortar peticiones fetch en vuelo
        this.isFirstRead = true;

        onMounted(() => {
            const root = this.model && this.model.root;
            if (!root) return;
            
            const modelName = root.resModel;
            if (modelName === 'mrp.weigh.roll.wizard' || modelName === 'mrp.subproduct.wizard') {
                this.isFirstRead = true;
                this._connectToTejidoScale();
            }
        });

        onWillDestroy(() => {
            // 1. Limpiamos el intervalo inmediatamente
            if (this.tejidoScaleInterval) {
                clearInterval(this.tejidoScaleInterval);
                this.tejidoScaleInterval = null;
            }
            // 2. Abortamos cualquier petición de red que esté a mitad de camino
            if (this.tejidoAbortController) {
                this.tejidoAbortController.abort();
                this.tejidoAbortController = null;
            }
        });
    },

    _connectToTejidoScale() {
        const root = this.model && this.model.root;
        if (!root || root.data.weighing_mode !== 'iot' || !root.data.iot_device_id) return;
            
        const iotUrl = "https://192-168-100-30.3991e8c5.odoo-iot.com/hw_proxy/scale_read";

        this.tejidoScaleInterval = setInterval(() => {
            // Si ya se inició el proceso de destrucción, abortamos antes de enviar
            if (!this.tejidoScaleInterval) return;

            // Instanciamos un nuevo manejador de aborto para esta petición
            this.tejidoAbortController = new AbortController();

            fetch(iotUrl, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                signal: this.tejidoAbortController.signal, // Vinculamos el escudo de aborto
                body: JSON.stringify({ 
                    jsonrpc: "2.0", 
                    method: "call", 
                    params: {}, 
                    id: Math.floor(Math.random() * 1000)
                })
            })
            .then(response => response.json())
            .then(payload => {
                // Doble escudo: si el componente se destruyó mientras respondía la red, salir
                if (!this.tejidoScaleInterval || !root) return;

                const data = payload.result || payload;
                if (data) {
                    let weightValue = data.weight !== undefined ? data.weight : data.value;
                    weightValue = parseFloat(weightValue);

                    if (!isNaN(weightValue) && weightValue >= 0) {
                        if (this.isFirstRead) {
                            this.isFirstRead = false;
                            return; 
                        }

                        const targetField = 'weight';
                        // Validamos que el modelo siga vivo y el campo exista antes de actualizar
                        if (root.data && root.data[targetField] !== weightValue) {
                            root.update({ [targetField]: weightValue });
                        }
                    }
                }
            })
            .catch(err => {
                // Captura silenciosa si la petición fue abortada por el usuario al dar cancelar
                if (err.name !== 'AbortError') {
                    console.log("Leyendo peso dinámico (Tejido)...");
                }
            });
        }, 1000);
    }
});