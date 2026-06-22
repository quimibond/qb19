/** @odoo-module */
import { patch } from "@web/core/utils/patch";
import { FormController } from "@web/views/form/form_controller";
import { onMounted, onWillDestroy } from "@odoo/owl";

patch(FormController.prototype, {
    setup() {
        super.setup(...arguments);
        this.tejidoScaleInterval = null;
        this.tejidoAbortController = null;
        // Variables de control para el filtro de estabilidad
        this.lastRawWeight = null; 
        this.readCounter = 0;

        onMounted(() => {
            const root = this.model && this.model.root;
            if (!root) return;
            
            const modelName = root.resModel;
            if (modelName === 'mrp.weigh.roll.wizard' || modelName === 'mrp.subproduct.wizard') {
                this.lastRawWeight = null;
                this.readCounter = 0;

                // Forzamos la limpieza visual inmediata del input para romper la memoria caché del navegador
                setTimeout(() => {
                    const inputWeight = document.querySelector("input[name='weight']");
                    if (inputWeight) inputWeight.value = "0.00";
                }, 200);

                this._connectToTejidoScale();
            }
        });

        onWillDestroy(() => {
            if (this.tejidoScaleInterval) {
                clearInterval(this.tejidoScaleInterval);
                this.tejidoScaleInterval = null;
            }
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
            if (!this.tejidoScaleInterval) return;

            this.tejidoAbortController = new AbortController();

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
                if (!this.tejidoScaleInterval || !root) return;

                const data = payload.result || payload;
                if (data) {
                    let weightValue = data.weight !== undefined ? data.weight : data.value;
                    weightValue = parseFloat(weightValue);

                    if (!isNaN(weightValue) && weightValue >= 0) {
                        
                        // 🔒 FILTRO DE ESTABILIDAD:
                        // Comparamos la lectura actual con la anterior enviada por el IoT Box
                        if (weightValue === this.lastRawWeight) {
                            this.readCounter++;
                        } else {
                            this.lastRawWeight = weightValue;
                            this.readCounter = 1; // Si cambió el peso, reiniciamos conteo
                        }

                        // Solo actualizamos Odoo si el peso se mantiene idéntico por al menos 2 lecturas consecutivas
                        // Esto descarta de inmediato los brincos y el arrastre del búfer viejo
                        if (this.readCounter >= 2) {
                            const targetField = 'weight';
                            if (root.data && root.data[targetField] !== weightValue) {
                                root.update({ [targetField]: weightValue });
                            }
                        }
                    }
                }
            })
            .catch(err => {
                if (err.name !== 'AbortError') {
                    console.log("Leyendo peso dinámico (Tejido)...");
                }
            });
        }, 800); // Bajamos ligeramente a 800ms para que el filtro de dos lecturas responda rápido
    }
});