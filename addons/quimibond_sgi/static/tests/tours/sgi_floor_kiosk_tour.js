/** @odoo-module **/
// 57.94.0 (U-01): la operadora elige su foto, teclea su PIN y firma su acuse
// en «SGI en planta». Lo llama tests/test_sgi_en_planta_tour.py.
import { registry } from "@web/core/registry";

registry.category("web_tour.tours").add("sgi_floor_kiosk_tour", {
    steps: () => [
        { trigger: ".o_sgi_kiosk_tile:contains('ZK Operadora del tour')", run: "click" },
        { trigger: ".o_sgi_kiosk_key[data-key='2']", run: "click" },
        { trigger: ".o_sgi_kiosk_key[data-key='4']", run: "click" },
        { trigger: ".o_sgi_kiosk_key[data-key='6']", run: "click" },
        { trigger: ".o_sgi_kiosk_key[data-key='8']", run: "click" },
        { trigger: ".o_sgi_kiosk_key[data-key='ok']", run: "click" },
        { trigger: ".o_sgi_kiosk_menu_docs", run: "click" },
        { trigger: ".o_sgi_kiosk_doc_row:contains('ZK Instructivo del tour') .o_sgi_kiosk_sign", run: "click" },
        { trigger: ".o_sgi_kiosk_message:contains('quedó firmado')" },
        { trigger: ".o_sgi_kiosk_exit", run: "click" },
        { trigger: ".o_sgi_kiosk_tile:contains('ZK Operadora del tour')" },
    ],
});
