/** @odoo-module */
import { patch } from "@web/core/utils/patch";
import { FormController } from "@web/views/form/form_controller";
import { onMounted, onWillDestroy } from "@odoo/owl";

patch(FormController.prototype, {
    setup() {
        super.setup(...arguments);
        this.tejidoScaleInterval = null;

        onMounted(() => {
            const modelName = this.model.root.resModel;
            if (modelName === 'mrp.weigh.roll.wizard' || modelName === 'mrp.subproduct.wizard') {
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
            
            // 🔄 DIRECTO A LOCALHOST (Usa la extensión instalada en Chrome para saltar el CORS)
            this.tejidoScaleInterval = setInterval(() => {
                fetch("http://127.0.0.1:8069/hw_proxy/perform_action", {
                    method: "POST",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify({ 
                        jsonrpc: "2.0", 
                        params: { "action": "read_scale" } 
                    })
                })
                .then(response => response.json())
                .then(payload => {
                    const data = payload.result || payload;
                    // Validamos la estructura nativa del Virtual IoT Box
                    if (data) {
                        let weightValue = data.weight !== undefined ? data.weight : data.value;
                        weightValue = parseFloat(weightValue);

                        if (!isNaN(weightValue) && weightValue >= 0) {
                            const targetField = 'weight';
                            if (root.data[targetField] !== weightValue) {
                                root.update({ [targetField]: weightValue });
                            }
                        }
                    }
                })
                .catch(err => console.log("Buscando báscula local Rhino..."));
            }, 1000);
        }
    }
});