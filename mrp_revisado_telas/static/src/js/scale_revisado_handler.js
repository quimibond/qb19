/** @odoo-module */
import { patch } from "@web/core/utils/patch";
import { FormController } from "@web/views/form/form_controller";
import { onMounted, onWillDestroy } from "@odoo/owl";

patch(FormController.prototype, {
    setup() {
        super.setup(...arguments);
        this.revisadoScaleInterval = null;

        onMounted(() => {
            if (this.model.root.resModel === 'mrp.revisado.wizard') {
                this._connectToRevisadoScale();
            }
        });

        onWillDestroy(() => {
            if (this.revisadoScaleInterval) {
                clearInterval(this.revisadoScaleInterval);
                this.revisadoScaleInterval = null;
            }
            // Forzamos limpieza en el modelo de datos al cerrar
            if (this.model && this.model.root) {
                if (this.model.root.data.peso_actual !== undefined) {
                    this.model.root.update({ peso_actual: 0.0 });
                }
            }
        });
    },

    _connectToRevisadoScale() {
        const root = this.model.root;
        if (root.data.weighing_mode === 'iot' && root.data.iot_device_id) {
            
            const iotUrl = "https://192-168-100-30.3991e8c5.odoo-iot.com/hw_proxy/scale_read";

            this.revisadoScaleInterval = setInterval(() => {
                if (!this.revisadoScaleInterval) return;

                fetch(iotUrl, {
                    method: "POST",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify({ 
                        jsonrpc: "2.0", 
                        method: "call", 
                        params: {}, 
                        id: Math.floor(Math.random() * 1000)
                    })
                })
                .then(response => response.json())
                .then(payload => {
                    const data = payload.result || payload;
                    if (data) {
                        let weightValue = data.weight !== undefined ? data.weight : data.value;
                        weightValue = parseFloat(weightValue);

                        // Aceptamos explícitamente el 0 para limpiar la pantalla al bajar el rollo
                        if (!isNaN(weightValue) && weightValue >= 0) {
                            if (root.data.peso_actual !== weightValue) {
                                root.update({ peso_actual: weightValue });
                            }
                        }
                    }
                })
                .catch(err => console.log("Leyendo peso dinámico (Revisado)..."));
            }, 1000);
        }
    }
});