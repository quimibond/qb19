/** @odoo-module */
import { patch } from "@web/core/utils/patch";
import { FormController } from "@web/views/form/form_controller";
import { onMounted, onWillDestroy } from "@odoo/owl";

patch(FormController.prototype, {
    setup() {
        super.setup(...arguments);
        this.tejidoScaleInterval = null;
        this.isFirstRead = true; // Bandera para ignorar el búfer viejo del IoT Box

        onMounted(() => {
            const modelName = this.model.root.resModel;
            if (modelName === 'mrp.weigh.roll.wizard' || modelName === 'mrp.subproduct.wizard') {
                // 1. Forzamos de inmediato que la interfaz inicie limpia en 0.0
                const targetField = 'weight';
                if (this.model.root.data[targetField] !== 0.0) {
                    this.model.root.update({ [targetField]: 0.0 });
                }
                
                this.isFirstRead = true; // Reiniciamos bandera de control
                this._connectToTejidoScale();
            }
        });

        onWillDestroy(() => {
            if (this.tejidoScaleInterval) {
                clearInterval(this.tejidoScaleInterval);
                this.tejidoScaleInterval = null;
            }
        });
    },

    _connectToTejidoScale() {
        const root = this.model.root;
        if (root.data.weighing_mode === 'iot' && root.data.iot_device_id) {
            
            const iotUrl = "https://192-168-100-30.3991e8c5.odoo-iot.com/hw_proxy/scale_read";

            this.tejidoScaleInterval = setInterval(() => {
                if (!this.tejidoScaleInterval) return;

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
                    if (!this.tejidoScaleInterval) return;

                    const data = payload.result || payload;
                    if (data) {
                        let weightValue = data.weight !== undefined ? data.weight : data.value;
                        weightValue = parseFloat(weightValue);

                        if (!isNaN(weightValue) && weightValue >= 0) {
                            // 2. Si es la primera lectura tras abrir el asistente, la ignoramos 
                            // para dar tiempo a que el IoT Box actualice su búfer real
                            if (this.isFirstRead) {
                                this.isFirstRead = false;
                                return; 
                            }

                            const targetField = 'weight';
                            if (root.data[targetField] !== weightValue) {
                                root.update({ [targetField]: weightValue });
                            }
                        }
                    }
                })
                .catch(err => console.log("Leyendo peso dinámico (Tejido)..."));
            }, 1000);
        }
    }
});