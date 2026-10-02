/** @odoo-module **/
// 57.94.0 «SGI en planta» (U-01). Pantalla de la tableta compartida: mosaico
// de personas del área, teclado numérico para el PIN y el menú de la persona
// (documentos por leer, casi accidente, EPP y checklist). Toda la lógica y
// todas las validaciones están en el servidor (sgi.floor.kiosk): el PIN se
// manda en CADA llamada y el servidor lo vuelve a validar. Aquí solo vive en
// memoria mientras la persona está dentro; «Salir» o 90 s sin tocar la
// pantalla lo borran.
import { Component, onWillStart, onWillUnmount, useState } from "@odoo/owl";
import { registry } from "@web/core/registry";
import { useService } from "@web/core/utils/hooks";

const KEYS = ["1", "2", "3", "4", "5", "6", "7", "8", "9", "borrar", "0", "ok"];
const PIN_MAX = 12;
const MODEL = "sgi.floor.kiosk";
// Leyendo un documento la persona puede pasar minutos sin tocar la pantalla
// de la tableta: ahí la salida sola espera más.
const DOC_IDLE_MS = 10 * 60 * 1000;

export class SgiFloorKiosk extends Component {
    static template = "quimibond_sgi.FloorKiosk";
    static props = ["*"];

    setup() {
        this.orm = useService("orm");
        this.keys = KEYS;
        this.answers = [["ok", "Bien", "btn-success"], ["falla", "Falla", "btn-danger"], ["na", "No aplica", "btn-secondary"]];
        this.state = useState({
            screen: "loading", busy: false, error: "", message: "",
            tablet: null, departments: [], department: false, employees: [],
            person: null, pin: "", counts: {},
            docs: [], doc: null, docUrl: null,
            nearMiss: { description: "", location: "" },
            epp: [], checklists: [], sheet: null,
        });
        this.idleMs = 90000;
        this.timer = null;
        // Una llamada a la vez: dos toques rápidos no mandan dos firmas
        // encimadas (ver call()).
        this.pending = Promise.resolve();
        this.onActivity = () => this.resetIdle();
        // Teclear (descripción del casi accidente, observaciones) también es
        // actividad: con solo pointerdown la pantalla salía a los 90 s a media
        // frase y se perdía el texto.
        this.activityEvents = ["pointerdown", "keydown", "input"];
        for (const name of this.activityEvents) {
            window.addEventListener(name, this.onActivity, true);
        }
        onWillStart(() => this.loadEmployees());
        onWillUnmount(() => {
            for (const name of this.activityEvents) {
                window.removeEventListener(name, this.onActivity, true);
            }
            clearTimeout(this.timer);
            this.revokeDoc();
        });
    }

    // El mensaje del servidor (UserError / AccessError) se muestra en la
    // pantalla; null le dice a quien llama que no siga. Las llamadas van en
    // fila, y la respuesta de una persona que ya salió (Salir o tiempo) se
    // descarta: la pantalla ya es de otra.
    call(method, args) {
        const person = this.state.person;
        const run = async () => {
            if (this.state.person !== person) {
                return null;
            }
            this.state.error = "";
            this.state.busy = true;
            try {
                const result = await this.orm.call(MODEL, method, args);
                return this.state.person === person ? result : null;
            } catch (error) {
                if (this.state.person === person) {
                    this.state.error = (error && error.data && error.data.message)
                        || (error && error.message) || "No se pudo completar. Intente de nuevo.";
                }
                return null;
            } finally {
                this.state.busy = false;
            }
        };
        const result = this.pending.then(run, run);
        this.pending = result.catch(() => null);
        return result;
    }

    get personArgs() {
        return [this.state.person.id, this.state.pin];
    }

    // ---- mosaico ----------------------------------------------------------
    async loadEmployees() {
        const data = await this.call("kiosk_employees", [this.state.department || false]);
        this.state.screen = "mosaic";
        if (!data) {
            return;
        }
        this.state.tablet = data.tablet;
        this.state.departments = data.departments;
        this.state.employees = data.employees;
        this.idleMs = (data.idle_seconds || 90) * 1000;
    }

    async filterDepartment(departmentId) {
        this.state.department = departmentId;
        await this.loadEmployees();
    }

    selectEmployee(employee) {
        this.state.message = "";
        if (!employee.has_pin) {
            this.state.error = `${employee.name} no tiene PIN registrado. Pida a RH que lo capture en su ficha (el mismo del quiosco de asistencia).`;
            return;
        }
        this.state.error = "";
        this.state.person = employee;
        this.state.pin = "";
        this.state.screen = "pin";
        this.resetIdle();
    }

    // ---- PIN --------------------------------------------------------------
    keyLabel(key) {
        return key === "borrar" ? "Borrar" : key === "ok" ? "Entrar" : key;
    }

    async pressKey(key) {
        if (key === "borrar") {
            this.state.pin = this.state.pin.slice(0, -1);
        } else if (key === "ok") {
            await this.submitPin();
        } else if (this.state.pin.length < PIN_MAX) {
            this.state.pin += key;
        }
    }

    async submitPin() {
        const res = await this.call("kiosk_check_pin", this.personArgs);
        if (!res) {
            this.state.pin = "";
            return;
        }
        this.state.counts = res.counts;
        this.state.screen = "menu";
    }

    async toMenu() {
        this.revokeDoc();
        const person = this.state.person;
        const res = await this.call("kiosk_check_pin", this.personArgs);
        if (res) {
            this.state.counts = res.counts;
            this.state.screen = "menu";
        } else if (this.state.person === person) {
            // Solo si sigue la misma persona: una respuesta tardía no saca a la siguiente.
            this.exit();
        }
    }

    // ---- documentos por leer ---------------------------------------------
    async openDocs() {
        this.revokeDoc();
        const docs = await this.call("kiosk_pending_docs", this.personArgs);
        if (docs) {
            this.state.docs = docs;
            this.state.screen = "docs";
        }
        this.resetIdle();
    }

    async readDoc(doc) {
        const file = await this.call("kiosk_document_file", [...this.personArgs, doc.ack_id]);
        if (!file) {
            return;
        }
        this.revokeDoc();
        this.state.doc = doc;
        if (file.url) {
            window.open(file.url, "_blank", "noopener");
        } else {
            const bytes = Uint8Array.from(atob(file.data), (c) => c.charCodeAt(0));
            this.blobUrl = URL.createObjectURL(new Blob([bytes], { type: file.mimetype }));
            // Chrome de Android no pinta PDF dentro de un <iframe>: el PDF va
            // por el visor pdf.js que trae Odoo (mismo origen, lee el blob:).
            // Ruta del visor en Odoo 19: web/static/lib/pdfjs (por confirmar en
            // la tableta real).
            this.state.docUrl = file.mimetype === "application/pdf"
                ? `/web/static/lib/pdfjs/web/viewer.html?file=${encodeURIComponent(this.blobUrl)}`
                : this.blobUrl;
        }
        this.state.screen = "doc";
        this.resetIdle();
    }

    // Tocar o desplazarse dentro del visor (otro documento, mismo origen) no
    // llega a la ventana de la pantalla: se escucha también ahí.
    onViewerLoad(ev) {
        try {
            const win = ev.target.contentWindow;
            for (const name of [...this.activityEvents, "scroll", "wheel"]) {
                win.addEventListener(name, this.onActivity, true);
            }
        } catch {
            // Otro origen (no debería): queda el tiempo largo de lectura.
        }
    }

    revokeDoc() {
        if (this.blobUrl) {
            URL.revokeObjectURL(this.blobUrl);
            this.blobUrl = null;
        }
        this.state.docUrl = null;
    }

    async signDoc(doc) {
        const res = await this.call("kiosk_ack_document", [...this.personArgs, doc.ack_id]);
        if (res) {
            this.state.message = res.message;
            this.revokeDoc();
            await this.openDocs();
        }
    }

    // ---- casi accidente ---------------------------------------------------
    openNearMiss() {
        this.state.nearMiss = { description: "", location: "" };
        this.state.screen = "nearmiss";
    }

    async sendNearMiss() {
        const res = await this.call("kiosk_report_near_miss", [...this.personArgs, { ...this.state.nearMiss }]);
        if (res) {
            this.state.message = res.message;
            await this.toMenu();
        }
    }

    // ---- EPP --------------------------------------------------------------
    async openEpp() {
        const rows = await this.call("kiosk_epp", this.personArgs);
        if (rows) {
            this.state.epp = rows;
            this.state.screen = "epp";
        }
    }

    async signEpp(row) {
        const res = await this.call("kiosk_sign_epp", [...this.personArgs, row.id]);
        if (res) {
            this.state.message = res.message;
            await this.openEpp();
        }
    }

    // ---- checklist --------------------------------------------------------
    async openChecklists() {
        const rows = await this.call("kiosk_checklists", this.personArgs);
        if (rows) {
            this.state.checklists = rows;
            this.state.sheet = null;
            this.state.screen = "checklists";
        }
    }

    openSheet(sheet) {
        this.state.sheet = sheet;
        this.state.screen = "sheet";
    }

    async saveSheet(answers, restOk = false, finish = false) {
        const row = await this.call("kiosk_checklist_save",
            [...this.personArgs, this.state.sheet.id, answers, restOk, finish]);
        if (!row) {
            return;
        }
        if (finish) {
            this.state.message = "Listo: el checklist quedó firmado a su nombre.";
            await this.openChecklists();
        } else {
            this.state.sheet = row;
        }
    }

    answer(line, value) {
        return this.saveSheet({ [line.id]: { answer: value } });
    }

    saveNote(line, ev) {
        return this.saveSheet({ [line.id]: { note: ev.target.value } });
    }

    // ---- salir ------------------------------------------------------------
    exit() {
        this.revokeDoc();
        clearTimeout(this.timer);
        Object.assign(this.state, {
            person: null, pin: "", counts: {}, docs: [], doc: null,
            epp: [], checklists: [], sheet: null, screen: "mosaic", error: "", message: "",
        });
    }

    resetIdle() {
        clearTimeout(this.timer);
        if (this.state.person) {
            const limit = this.state.screen === "doc" ? DOC_IDLE_MS : this.idleMs;
            this.timer = setTimeout(() => this.exit(), limit);
        }
    }
}

registry.category("actions").add("sgi_floor_kiosk", SgiFloorKiosk);
