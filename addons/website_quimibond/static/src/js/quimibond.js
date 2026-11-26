/** @odoo-module ignore */
/* Sitio B2B de Quimibond: animaciones e interacción.
   JS plano, sin dependencias: solo actúa si la página trae html[data-qb]. */
(function () {
    "use strict";

    var raiz = document.documentElement;
    var sinMovimiento = window.matchMedia && window.matchMedia("(prefers-reduced-motion: reduce)").matches;
    var TAU = Math.PI * 2;
    var HILOS = ["#2f54c8", "#e8590c", "#0f9d7a"];

    function listo(fn) {
        if (document.readyState === "loading") {
            document.addEventListener("DOMContentLoaded", fn);
        } else {
            fn();
        }
    }
    function todos(sel, ctx) {
        return Array.prototype.slice.call((ctx || document).querySelectorAll(sel));
    }
    /* Textos en el idioma de la página: el servidor los deja en #qb_t (vacío en
       español). T("texto en español") devuelve la traducción o el mismo texto. */
    var QB_T = null;
    function T(s) {
        if (QB_T === null) {
            QB_T = {};
            try {
                var caja = document.getElementById("qb_t");
                if (caja) { QB_T = JSON.parse(caja.getAttribute("data-t")) || {}; }
            } catch (e) { QB_T = {}; }
        }
        return QB_T[s] || s;
    }
    function normal(s) {
        return (s || "").toLowerCase().normalize("NFD").replace(/[̀-ͯ]/g, "");
    }

    /* Lienzo que se ajusta al tamaño y solo corre cuando está a la vista */
    function lienzo(canvas, dibujar) {
        var ctx = canvas.getContext("2d");
        var estado = { w: 0, h: 0, visible: false, t0: 0, raf: 0 };
        function medir() {
            var r = canvas.getBoundingClientRect();
            var dpr = Math.min(window.devicePixelRatio || 1, 2);
            estado.w = r.width;
            estado.h = r.height;
            canvas.width = Math.max(1, Math.round(r.width * dpr));
            canvas.height = Math.max(1, Math.round(r.height * dpr));
            ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
            if (sinMovimiento || !estado.visible) {
                dibujar(ctx, estado.w, estado.h, 0, 0);
            }
        }
        function cuadro(t) {
            if (!estado.visible) {
                estado.raf = 0;
                return;
            }
            var dt = Math.min(0.05, (t - (estado.t0 || t)) / 1000);
            estado.t0 = t;
            dibujar(ctx, estado.w, estado.h, t / 1000, dt);
            estado.raf = requestAnimationFrame(cuadro);
        }
        if ("ResizeObserver" in window) {
            new ResizeObserver(medir).observe(canvas);
        } else {
            window.addEventListener("resize", medir);
        }
        medir();
        if (sinMovimiento || !("IntersectionObserver" in window)) {
            estado.visible = !sinMovimiento;
            if (!sinMovimiento) {
                estado.raf = requestAnimationFrame(cuadro);
            }
            return;
        }
        new IntersectionObserver(function (entradas) {
            estado.visible = entradas[0].isIntersecting;
            if (estado.visible && !estado.raf) {
                estado.t0 = 0;
                estado.raf = requestAnimationFrame(cuadro);
            }
        }).observe(canvas);
    }

    /* ---------------------------------------------------------------------
       Máquina de tejido circular, vista desde arriba.
       Las bobinas alimentan el hilo; el cilindro gira; las agujas suben al
       pasar por cada alimentador (leva fija) y la tela baja en espiral por
       el centro, con una pasada por alimentador.
       ------------------------------------------------------------------ */
    function maquinaCircular(canvas) {
        var N = 96;            // agujas
        var F = 9;             // alimentadores
        var PASADAS = 15;      // anillos de tela visibles
        var colores = [];
        for (var f = 0; f < F; f++) {
            colores.push(f % 3 === 0 ? HILOS[(f / 3) % 3] : "#a1a1aa");
        }
        var rot = 0;
        var vel = 0.32;
        var objetivo = 0.32;
        var mallas = 0;
        var cursor = null;
        var elRpm = document.querySelector("[data-qb-rpm]");
        var elMallas = document.querySelector("[data-qb-mallas]");
        var formato = new Intl.NumberFormat("es-MX");

        canvas.parentElement.addEventListener("pointerenter", function () { objetivo = 0.85; });
        canvas.parentElement.addEventListener("pointerleave", function () { objetivo = 0.32; cursor = null; });
        canvas.parentElement.addEventListener("pointermove", function (e) {
            var r = canvas.getBoundingClientRect();
            cursor = { x: e.clientX - r.left, y: e.clientY - r.top };
        });

        function leva(a) {
            // Altura de la aguja según su posición: sube al acercarse a cada alimentador
            var paso = TAU / F;
            var d = ((a % paso) + paso) % paso / paso;      // 0..1 entre alimentadores
            var x = Math.abs(d - 0.5) * 2;                  // 1 en el alimentador
            return Math.pow(x, 3);
        }

        lienzo(canvas, function (ctx, w, h, t, dt) {
            var c = Math.min(w, h);
            var cx = w / 2;
            var cy = h / 2;
            var R = c * 0.47;
            var Rc = R * 0.60;          // cilindro de agujas
            var Rb = R * 0.90;          // bobinas
            vel += (objetivo - vel) * Math.min(1, dt * 2.5);
            rot += vel * dt;
            mallas += (vel * dt / TAU) * N * F;
            ctx.clearRect(0, 0, w, h);

            // Base y aros guía
            var g = ctx.createRadialGradient(cx, cy, Rc * 0.2, cx, cy, R * 1.05);
            g.addColorStop(0, "rgba(244,244,245,0)");
            g.addColorStop(0.62, "rgba(244,244,245,.9)");
            g.addColorStop(1, "rgba(244,244,245,0)");
            ctx.fillStyle = g;
            ctx.beginPath();
            ctx.arc(cx, cy, R * 1.05, 0, TAU);
            ctx.fill();
            ctx.lineWidth = 1;
            ctx.strokeStyle = "#e4e4e7";
            ctx.setLineDash([3, 5]);
            ctx.beginPath();
            ctx.arc(cx, cy, Rb, 0, TAU);
            ctx.stroke();
            ctx.setLineDash([]);

            // Tela: anillos de mallas que bajan hacia el centro
            var avance = (rot * F / TAU) % 1;               // fracción de pasada
            var base = Math.floor(rot * F / TAU);
            var porColor = {};
            for (var j = 0; j < PASADAS; j++) {
                var k = (j + avance) / PASADAS;             // 0 borde → 1 centro
                var r1 = Rc * (1 - k * 0.86);
                var r2 = Rc * (1 - (j + avance + 0.78) / PASADAS * 0.86);
                var color = colores[(((base - j) % F) + F) % F];
                var alfa = Math.max(0, 1 - k * 1.05);
                var clave = color + "|" + alfa.toFixed(2);
                var lista = porColor[clave] || (porColor[clave] = []);
                lista.push(r1, r2);
            }
            ctx.lineCap = "round";
            Object.keys(porColor).forEach(function (clave) {
                var partes = clave.split("|");
                var radios = porColor[clave];
                ctx.strokeStyle = partes[0];
                ctx.globalAlpha = parseFloat(partes[1]) * (partes[0] === "#a1a1aa" ? 0.75 : 1);
                ctx.beginPath();
                for (var q = 0; q < radios.length; q += 2) {
                    var ra = radios[q];
                    var rb = radios[q + 1];
                    ctx.lineWidth = Math.max(0.6, ra / Rc * 1.7);
                    for (var i = 0; i < N; i++) {
                        var a0 = rot + i / N * TAU;
                        var am = a0 + 0.5 / N * TAU;
                        var a1 = a0 + 1 / N * TAU;
                        // Malla en V: dos patas que se juntan hacia el centro
                        ctx.moveTo(cx + Math.cos(a0 + 0.1 / N * TAU) * ra, cy + Math.sin(a0 + 0.1 / N * TAU) * ra);
                        ctx.lineTo(cx + Math.cos(am) * rb, cy + Math.sin(am) * rb);
                        ctx.lineTo(cx + Math.cos(a1 - 0.1 / N * TAU) * ra, cy + Math.sin(a1 - 0.1 / N * TAU) * ra);
                    }
                }
                ctx.stroke();
            });
            ctx.globalAlpha = 1;

            // Centro: el tubo de tela que baja
            var gc = ctx.createRadialGradient(cx, cy, 0, cx, cy, Rc * 0.3);
            gc.addColorStop(0, "#18181b");
            gc.addColorStop(0.55, "rgba(24,24,27,.55)");
            gc.addColorStop(1, "rgba(24,24,27,0)");
            ctx.fillStyle = gc;
            ctx.beginPath();
            ctx.arc(cx, cy, Rc * 0.3, 0, TAU);
            ctx.fill();

            // Hilos: de cada bobina a su alimentador
            for (var f = 0; f < F; f++) {
                var af = f / F * TAU - Math.PI / 2;
                var ab = af - 0.34;
                var bx = cx + Math.cos(ab) * Rb;
                var by = cy + Math.sin(ab) * Rb;
                var fx = cx + Math.cos(af) * (Rc + c * 0.045);
                var fy = cy + Math.sin(af) * (Rc + c * 0.045);
                var tiembla = Math.sin(t * 9 + f * 1.7) * c * 0.004 * (vel / 0.32);
                var mx = cx + Math.cos((af + ab) / 2) * (R * 0.80) + tiembla;
                var my = cy + Math.sin((af + ab) / 2) * (R * 0.80) + tiembla;
                ctx.strokeStyle = colores[f];
                ctx.globalAlpha = colores[f] === "#a1a1aa" ? 0.7 : 0.95;
                ctx.lineWidth = 1.3;
                ctx.setLineDash([5, 3]);
                ctx.lineDashOffset = -t * 40 * (vel / 0.32);
                ctx.beginPath();
                ctx.moveTo(bx, by);
                ctx.quadraticCurveTo(mx, my, fx, fy);
                ctx.stroke();
                ctx.setLineDash([]);
                ctx.globalAlpha = 1;

                // Alimentador
                ctx.fillStyle = "#fff";
                ctx.strokeStyle = "#18181b";
                ctx.lineWidth = 1.2;
                ctx.beginPath();
                ctx.arc(fx, fy, c * 0.011, 0, TAU);
                ctx.fill();
                ctx.stroke();

                // Bobina: cono de hilo que gira al desenrollar
                var rbob = c * 0.043;
                var cerca = cursor ? Math.max(0, 1 - Math.hypot(cursor.x - bx, cursor.y - by) / (c * 0.18)) : 0;
                rbob *= 1 + cerca * 0.18;
                ctx.fillStyle = "#fff";
                ctx.strokeStyle = "#d4d4d8";
                ctx.lineWidth = 1;
                ctx.shadowColor = "rgba(9,9,11,.12)";
                ctx.shadowBlur = 10;
                ctx.shadowOffsetY = 3;
                ctx.beginPath();
                ctx.arc(bx, by, rbob, 0, TAU);
                ctx.fill();
                ctx.shadowColor = "transparent";
                ctx.shadowBlur = 0;
                ctx.shadowOffsetY = 0;
                ctx.stroke();
                ctx.strokeStyle = colores[f];
                ctx.lineWidth = 1.4;
                var giro = -t * 2.2 * (vel / 0.32) + f;
                for (var v = 0; v < 3; v++) {
                    ctx.globalAlpha = 0.9 - v * 0.22;
                    ctx.beginPath();
                    ctx.arc(bx, by, rbob * (0.78 - v * 0.2), giro + v * 1.3, giro + v * 1.3 + 4.4);
                    ctx.stroke();
                }
                ctx.globalAlpha = 1;
                ctx.fillStyle = "#18181b";
                ctx.beginPath();
                ctx.arc(bx, by, rbob * 0.16, 0, TAU);
                ctx.fill();
            }

            // Cilindro y agujas
            ctx.strokeStyle = "#18181b";
            ctx.lineWidth = 1.6;
            ctx.beginPath();
            ctx.arc(cx, cy, Rc, 0, TAU);
            ctx.stroke();
            ctx.strokeStyle = "#d4d4d8";
            ctx.lineWidth = 1;
            ctx.beginPath();
            ctx.arc(cx, cy, Rc + c * 0.062, 0, TAU);
            ctx.stroke();
            for (var n = 0; n < N; n++) {
                var an = rot + n / N * TAU;
                var sube = leva(an + Math.PI / 2);
                var largo = c * (0.012 + sube * 0.04);
                var x0 = cx + Math.cos(an) * Rc;
                var y0 = cy + Math.sin(an) * Rc;
                var x1 = cx + Math.cos(an) * (Rc + largo);
                var y1 = cy + Math.sin(an) * (Rc + largo);
                ctx.strokeStyle = sube > 0.6 ? "#18181b" : "#71717a";
                ctx.lineWidth = sube > 0.6 ? 1.5 : 1;
                ctx.beginPath();
                ctx.moveTo(x0, y0);
                ctx.lineTo(x1, y1);
                ctx.stroke();
                if (sube > 0.75) {
                    ctx.fillStyle = "#18181b";
                    ctx.beginPath();
                    ctx.arc(x1, y1, 1.5, 0, TAU);
                    ctx.fill();
                }
            }

            if (elRpm) {
                elRpm.textContent = Math.round(vel / 0.32 * 26);
            }
            if (elMallas) {
                elMallas.textContent = formato.format(Math.floor(mallas));
            }
        });
    }

    /* ---------------------------------------------------------------------
       No tejido: fibras al azar y puntos de adhesivo que se encienden en ola.
       ------------------------------------------------------------------ */
    function fibras(canvas) {
        var semilla = 7;
        function azar() {
            semilla = (semilla * 16807) % 2147483647;
            return (semilla - 1) / 2147483646;
        }
        var lista = [];
        for (var i = 0; i < 110; i++) {
            lista.push({ x: azar(), y: azar(), a: azar() * Math.PI, l: 0.12 + azar() * 0.3, c: (azar() - 0.5) * 0.5, f: azar() * TAU, o: 0.05 + azar() * 0.13 });
        }
        var cursor = null;
        var seccion = canvas.parentElement;
        seccion.addEventListener("pointermove", function (e) {
            var r = canvas.getBoundingClientRect();
            cursor = { x: e.clientX - r.left, y: e.clientY - r.top };
        });
        seccion.addEventListener("pointerleave", function () { cursor = null; });

        lienzo(canvas, function (ctx, w, h, t) {
            ctx.clearRect(0, 0, w, h);
            var m = Math.max(w, h);
            ctx.lineCap = "round";
            lista.forEach(function (fb) {
                var x = fb.x * w + Math.sin(t * 0.12 + fb.f) * 14;
                var y = fb.y * h + Math.cos(t * 0.1 + fb.f) * 10;
                var a = fb.a + Math.sin(t * 0.07 + fb.f) * 0.08;
                var dx = Math.cos(a) * fb.l * m / 2;
                var dy = Math.sin(a) * fb.l * m / 2;
                ctx.strokeStyle = "rgba(255,255,255," + fb.o + ")";
                ctx.lineWidth = 1;
                ctx.beginPath();
                ctx.moveTo(x - dx, y - dy);
                ctx.quadraticCurveTo(x - dy * fb.c, y + dx * fb.c, x + dx, y + dy);
                ctx.stroke();
            });
            // Puntos de adhesivo
            var paso = 26;
            for (var py = paso / 2, fila = 0; py < h; py += paso * 0.866, fila++) {
                for (var px = (fila % 2 ? paso : paso / 2); px < w; px += paso) {
                    var ola = Math.sin(px * 0.012 + py * 0.006 - t * 0.9) * 0.5 + 0.5;
                    var luz = Math.pow(ola, 4);
                    if (cursor) {
                        luz = Math.max(luz, Math.max(0, 1 - Math.hypot(cursor.x - px, cursor.y - py) / 150));
                    }
                    if (luz < 0.03) {
                        continue;
                    }
                    ctx.fillStyle = "rgba(125,150,255," + (luz * 0.75).toFixed(3) + ")";
                    ctx.beginPath();
                    ctx.arc(px, py, 1 + luz * 2.2, 0, TAU);
                    ctx.fill();
                }
            }
        });
    }

    /* ---------------------------------------------------------------------
       Tela diseñada: dibuja la construcción elegida (malla de una o doble
       fontura, no tejido sencillo o tramado) con su color, peso y punto de adhesivo, y
       la mueve como un lienzo según el tacto.
       --------------------------------------------------------------------- */
    var TELA_DEMO = [
        { construccion: "una_fontura", galga: 24, peso: 150, color: "#f5f5f2", tacto: "Suave", fusionable: false },
        { construccion: "doble_fontura", galga: 18, peso: 260, color: "#1f2a4a", tacto: "Semirrígido", fusionable: false },
        { construccion: "no_tejido", galga: 24, peso: 60, color: "#e9e2d0", tacto: "Rígido", fusionable: true },
        { construccion: "tramado", galga: 24, peso: 90, color: "#9a9aa2", tacto: "Semirrígido", fusionable: true }
    ];
    var TELA_ONDA = { "Suave": 9, "Semirrígido": 4.5, "Rígido": 1.5 };
    function ondaDeTacto(tacto) {
        var k;
        if (TELA_ONDA[tacto] !== undefined) { return TELA_ONDA[tacto]; }
        for (k in TELA_ONDA) { if (T(k) === tacto) { return TELA_ONDA[k]; } }
        return undefined;
    }

    function rgb(hex) {
        var m = /^#?([0-9a-f]{6})$/i.exec(hex || "");
        var n = parseInt(m ? m[1] : "f5f5f2", 16);
        return [(n >> 16) & 255, (n >> 8) & 255, n & 255];
    }
    function mezcla(a, b, p) {
        return "rgb(" + [0, 1, 2].map(function (i) {
            return Math.round(a[i] + (b[i] - a[i]) * p);
        }).join(",") + ")";
    }
    function esClaro(c) {
        return (c[0] * 299 + c[1] * 587 + c[2] * 114) / 1000 > 140;
    }
    function azar(semilla) {
        var s = semilla || 1;
        return function () {
            s = (s * 16807) % 2147483647;
            return (s - 1) / 2147483646;
        };
    }

    function pintarTela(ctx, w, h, d) {
        var base = rgb(d.color);
        var claro = esClaro(base);
        var hilo = mezcla(base, claro ? [20, 20, 28] : [255, 255, 255], claro ? 0.42 : 0.38);
        var hilo2 = mezcla(base, claro ? [47, 84, 200] : [150, 175, 255], 0.6);
        var peso = Math.max(20, Math.min(400, d.peso || 150));
        var carga = (peso - 20) / 380;
        ctx.fillStyle = mezcla(base, [255, 255, 255], 0);
        ctx.fillRect(0, 0, w, h);
        ctx.lineCap = "round";
        ctx.lineJoin = "round";

        function mallas(paso, dx, dy, color, alfa, grosor) {
            var k = paso / 20;
            var fila = 11 * k;
            ctx.strokeStyle = color;
            ctx.globalAlpha = alfa;
            ctx.lineWidth = grosor;
            ctx.beginPath();
            for (var y = dy - fila; y < h + fila; y += fila) {
                for (var x = dx - paso; x < w + paso; x += paso) {
                    ctx.moveTo(x, y + 10 * k);
                    ctx.bezierCurveTo(x + 5 * k, y + 10 * k, x + 7 * k, y + 7 * k, x + 6 * k, y + 4 * k);
                    ctx.bezierCurveTo(x + 5 * k, y - k, x + 15 * k, y - k, x + 14 * k, y + 4 * k);
                    ctx.bezierCurveTo(x + 13 * k, y + 7 * k, x + 15 * k, y + 10 * k, x + 20 * k, y + 10 * k);
                }
            }
            ctx.stroke();
            ctx.globalAlpha = 1;
            return fila;
        }

        var paso = Math.max(9, Math.min(26, 416 / (d.galga || 24)));
        var grosor = (1 + carga * 2.4) * (paso / 17);
        if (d.construccion === "no_tejido" || d.construccion === "tramado") {
            var r = azar(7 + Math.round(peso));
            var n = Math.round(90 + carga * 520);
            ctx.strokeStyle = hilo;
            ctx.lineWidth = 0.9;
            ctx.globalAlpha = 0.34 + carga * 0.2;
            ctx.beginPath();
            for (var i = 0; i < n; i++) {
                var x0 = r() * w, y0 = r() * h, ang = r() * TAU, largo = 50 + r() * 150;
                var x1 = x0 + Math.cos(ang) * largo, y1 = y0 + Math.sin(ang) * largo;
                ctx.moveTo(x0, y0);
                ctx.quadraticCurveTo((x0 + x1) / 2 + (r() - 0.5) * 70, (y0 + y1) / 2 + (r() - 0.5) * 70, x1, y1);
            }
            ctx.stroke();
            if (d.construccion === "tramado") {
                // No tejido tramado: el velo de fibra con hilos de trama encima.
                ctx.strokeStyle = hilo2;
                ctx.lineWidth = 1.6 + carga * 1.6;
                ctx.globalAlpha = 0.85;
                ctx.beginPath();
                for (var yt = 11; yt < h + 22; yt += 22) {
                    ctx.moveTo(-4, yt);
                    ctx.lineTo(w + 4, yt);
                }
                ctx.stroke();
            }
            ctx.globalAlpha = 1;
        } else if (d.construccion === "doble_fontura") {
            mallas(paso, paso / 2, 11 * paso / 40, hilo2, 0.5, grosor);
            mallas(paso, 0, 0, hilo, 0.95, grosor);
        } else {
            mallas(paso, 0, 0, hilo, 0.95, grosor);
        }
        if (d.fusionable) {
            ctx.fillStyle = claro ? "rgba(47, 84, 200, .55)" : "rgba(255, 255, 255, .6)";
            for (var py = 9; py < h; py += 18) {
                for (var px = ((py / 18) % 2 ? 18 : 9); px < w; px += 18) {
                    ctx.beginPath();
                    ctx.arc(px, py, 2.1, 0, TAU);
                    ctx.fill();
                }
            }
        }
    }

    function tela(canvas) {
        var demo = canvas.hasAttribute("data-qb-demo");
        var diseno = Object.assign({}, TELA_DEMO[0]);
        var fuera = document.createElement("canvas");
        var margen = 14;
        var sucio = true;
        var ultimo = null;
        var turno = 0;

        function preparar(w, h) {
            var dpr = Math.min(window.devicePixelRatio || 1, 2);
            fuera.width = Math.max(1, Math.round(w * dpr));
            fuera.height = Math.max(1, Math.round((h + margen * 2) * dpr));
            var c = fuera.getContext("2d");
            c.setTransform(dpr, 0, 0, dpr, 0, 0);
            pintarTela(c, w, h + margen * 2, diseno);
            sucio = false;
        }

        function dibujar(ctx, w, h, t) {
            if (!w || !h) {
                return;
            }
            if (demo && !sinMovimiento) {
                var ahora = Math.floor(t / 3.4) % TELA_DEMO.length;
                if (ahora !== turno) {
                    turno = ahora;
                    diseno = Object.assign({}, TELA_DEMO[turno]);
                    sucio = true;
                }
            }
            if (sucio || !ultimo || ultimo.w !== w || ultimo.h !== h) {
                preparar(w, h);
            }
            ultimo = { ctx: ctx, w: w, h: h };
            var amp = Math.min(margen - 2, ondaDeTacto(diseno.tacto) === undefined ? 6 : ondaDeTacto(diseno.tacto));
            var tira = 3;
            var esc = fuera.width / w;
            ctx.clearRect(0, 0, w, h);
            for (var x = 0; x < w; x += tira) {
                var f = x * 0.012 + t * 0.9;
                var g = x * 0.027 - t * 0.6;
                var dy = amp * (Math.sin(f) * 0.7 + Math.sin(g) * 0.3);
                var pend = Math.cos(f) * 0.7 + Math.cos(g) * 0.3;
                ctx.drawImage(fuera, x * esc, 0, tira * esc, fuera.height, x, dy - margen, tira, h + margen * 2);
                var sombra = pend * amp * 0.011;
                if (sombra > 0.004) {
                    ctx.fillStyle = "rgba(9, 9, 11, " + Math.min(0.2, sombra).toFixed(3) + ")";
                    ctx.fillRect(x, 0, tira, h);
                } else if (sombra < -0.004) {
                    ctx.fillStyle = "rgba(255, 255, 255, " + Math.min(0.2, -sombra).toFixed(3) + ")";
                    ctx.fillRect(x, 0, tira, h);
                }
            }
        }

        canvas.qbTela = function (nuevo) {
            diseno = Object.assign({}, diseno, nuevo);
            sucio = true;
            if (sinMovimiento && ultimo) {
                dibujar(ultimo.ctx, ultimo.w, ultimo.h, 0);
            }
        };
        lienzo(canvas, dibujar);
    }

    /* --------------------------------------------------------------------- */
    function disenador() {
        var raizDis = document.querySelector("[data-qb-disenador]");
        if (!raizDis) {
            return;
        }
        var canvas = raizDis.querySelector('canvas[data-qb-anim="tela"]');
        var ficha = raizDis.querySelector("[data-qb-ficha]");
        var detalle = document.getElementById("qb_f_detalle");
        var asunto = document.getElementById("qb_f_asunto");
        var solicitud = document.getElementById("qb_f_solicitud");
        var bloqueAnterior = "";
        var asuntoAnterior = "";
        var colorLibre = false;

        function elegido(nombre) {
            return raizDis.querySelector('input[name="' + nombre + '"]:checked');
        }
        function campo(nombre) {
            return raizDis.querySelector('input[name="' + nombre + '"]');
        }

        function leer() {
            var cons = elegido("qb_construccion");
            var col = elegido("qb_color");
            var libre = campo("qb_color_libre");
            var ancho = parseInt(campo("qb_ancho").value, 10);
            var acabados = [];
            if (campo("qb_fusionable").checked) {
                acabados.push(T("Fusionable"));
            }
            if (campo("qb_termofijado").checked) {
                acabados.push(T("Termofijada"));
            }
            return {
                construccion: cons ? cons.value : "una_fontura",
                construccionNombre: cons ? cons.getAttribute("data-nombre") : "",
                galga: parseInt((elegido("qb_galga") || {}).value, 10) || 24,
                fibra: (elegido("qb_fibra") || {}).value || "",
                hilo: (elegido("qb_hilo") || {}).value || "",
                hiloTipo: (elegido("qb_hilo_tipo") || {}).value || "",
                elastano: !!(campo("qb_elastano") && campo("qb_elastano").checked),
                mercado: (elegido("qb_mercado") || {}).value || "",
                aplicacion: (elegido("qb_aplicacion") || {}).value || "",
                requisitos: todos('input[name="qb_requisito"]:checked', raizDis).map(function (r) {
                    return r.value;
                }),
                norma: (campo("qb_norma").value || "").trim(),
                volumen: parseInt(campo("qb_volumen").value, 10) || 0,
                unidad: raizDis.querySelector('select[name="qb_unidad"]').value,
                fecha: campo("qb_fecha").value || "",
                peso: parseInt(campo("qb_peso").value, 10) || 150,
                color: colorLibre || !col ? libre.value : col.value,
                colorNombre: colorLibre || !col ? T("Personalizado") + " " + libre.value.toUpperCase() : col.getAttribute("data-nombre"),
                tacto: (elegido("qb_tacto") || {}).value || T("Suave"),
                fusionable: campo("qb_fusionable").checked,
                acabados: acabados,
                presentacion: (elegido("qb_presentacion") || {}).value || "",
                ancho: ancho > 0 ? ancho : 0
            };
        }

        function filas(d) {
            var punto = d.construccion === "una_fontura" || d.construccion === "doble_fontura";
            var f = [];
            if (d.mercado) {
                f.push([T("Mercado"), d.mercado]);
            }
            if (d.aplicacion) {
                f.push([T("Aplicación"), d.aplicacion]);
            }
            f.push([T("Construcción"), d.construccionNombre]);
            if (!punto) {
                f.push([T("Fibra"), d.fibra]);
            } else {
                var hilo = d.hilo;
                if (hilo.indexOf(T("Poliéster")) === 0 && d.hiloTipo) {
                    hilo += " " + d.hiloTipo.toLowerCase();
                }
                f.push([T("Hilo"), hilo + (d.elastano ? " " + T("con elastano") : "")]);
            }
            f.push([T("Peso"), d.peso + " g/m²"]);
            if (d.ancho) {
                f.push([T("Ancho"), d.ancho + " cm"]);
            }
            f.push([T("Color"), d.colorNombre], [T("Tacto"), d.tacto]);
            if (d.acabados.length) {
                f.push([T("Acabado"), d.acabados.join(", ")]);
            }
            f.push([T("Presentación"), d.presentacion]);
            if (d.requisitos.length) {
                f.push([T("Requisitos"), d.requisitos.join(", ")]);
            }
            if (d.norma) {
                f.push([T("Norma"), d.norma]);
            }
            if (d.volumen) {
                f.push([T("Volumen"), d.volumen.toLocaleString("es-MX") + " " + d.unidad]);
            }
            if (d.fecha) {
                f.push([T("Lo necesita para"), d.fecha.split("-").reverse().join("/")]);
            }
            return f;
        }

        function aplicar() {
            var d = leer();
            // De lo general a lo particular: las aplicaciones salen al elegir el mercado.
            var grupoAp = raizDis.querySelector("[data-qb-aplicaciones]");
            if (grupoAp) {
                var hay = false;
                todos("[data-qb-mercado]", grupoAp).forEach(function (g) {
                    g.hidden = g.getAttribute("data-qb-mercado") !== d.mercado;
                    hay = hay || !g.hidden;
                });
                grupoAp.hidden = !hay;
            }
            todos("[data-qb-solo]", raizDis).forEach(function (g) {
                g.hidden = g.getAttribute("data-qb-solo").split(" ").indexOf(d.construccion) < 0;
            });
            todos("[data-qb-solo-hilo]", raizDis).forEach(function (g) {
                g.hidden = d.hilo.indexOf(g.getAttribute("data-qb-solo-hilo")) !== 0;
            });
            var salidaPeso = raizDis.querySelector('[data-qb-salida="peso"]');
            var salidaColor = raizDis.querySelector('[data-qb-salida="color"]');
            salidaPeso.textContent = d.peso + " g/m²";
            salidaColor.textContent = d.colorNombre;
            raizDis.querySelector(".qb-dis__color--libre").classList.toggle("activo", colorLibre);
            var lista = filas(d);
            ficha.textContent = "";
            lista.forEach(function (par) {
                var div = document.createElement("div");
                var dt = document.createElement("dt");
                var dd = document.createElement("dd");
                dt.textContent = par[0];
                dd.textContent = par[1];
                div.appendChild(dt);
                div.appendChild(dd);
                ficha.appendChild(div);
            });
            if (canvas && canvas.qbTela) {
                canvas.qbTela(d);
            }
            // El diseño viaja a Ventas dentro de la especificación del formulario.
            var bloque = T("Diseño de tela (diseñador en línea)") + "\n" + lista.map(function (par) {
                return "- " + par[0] + ": " + par[1];
            }).join("\n");
            if (detalle) {
                var resto = detalle.value;
                if (bloqueAnterior && resto.indexOf(bloqueAnterior) === 0) {
                    resto = resto.slice(bloqueAnterior.length).replace(/^\n+/, "");
                }
                detalle.value = bloque + "\n\n" + resto;
                detalle.defaultValue = detalle.value;
                bloqueAnterior = bloque;
            }
            if (asunto) {
                var nuevoAsunto = T("Diseño de tela") + ": " + d.construccionNombre + ", " + d.peso + " g/m²";
                if (!asunto.value || asunto.value === asuntoAnterior) {
                    asunto.value = nuevoAsunto;
                    asunto.defaultValue = nuevoAsunto;
                }
                asuntoAnterior = nuevoAsunto;
            }
            var volForm = document.getElementById("qb_f_volumen");
            if (volForm && d.volumen) {
                volForm.value = d.volumen.toLocaleString("es-MX") + " " + d.unidad;
            }
            guardarEnlace();
        }

        // Nombres de los controles en el enlace del diseño (cortos, sin datos personales).
        var CLAVES = {
            m: "qb_mercado", ap: "qb_aplicacion", c: "qb_construccion", g: "qb_galga", h: "qb_hilo", ht: "qb_hilo_tipo",
            f: "qb_fibra", t: "qb_tacto", pr: "qb_presentacion"
        };
        var CASILLAS = { e: "qb_elastano", fu: "qb_fusionable", te: "qb_termofijado" };
        var TEXTOS = { p: "qb_peso", a: "qb_ancho", n: "qb_norma", v: "qb_volumen", fe: "qb_fecha" };

        function marcar(nombre, valor) {
            var hecho = false;
            todos('input[name="' + nombre + '"]', raizDis).forEach(function (o) {
                if (o.value === String(valor)) {
                    o.checked = true;
                    hecho = true;
                }
            });
            return hecho;
        }
        function ponerColor(hex) {
            if (!/^#[0-9a-f]{6}$/i.test(hex || "")) {
                return;
            }
            colorLibre = !marcar("qb_color", hex.toLowerCase());
            if (colorLibre) {
                todos('input[name="qb_color"]', raizDis).forEach(function (c) {
                    c.checked = false;
                });
                campo("qb_color_libre").value = hex.toLowerCase();
            }
        }
        // Tela de partida de una aplicación: lo que no trae vuelve a su valor neutro.
        function ponerTela(tela) {
            marcar("qb_construccion", tela.construccion);
            marcar("qb_galga", tela.galga || 24);
            if (tela.hilo) {
                marcar("qb_hilo", tela.hilo);
            }
            if (tela.hiloTipo) {
                marcar("qb_hilo_tipo", tela.hiloTipo);
            }
            if (tela.fibra) {
                marcar("qb_fibra", tela.fibra);
            }
            marcar("qb_tacto", tela.tacto || T("Suave"));
            campo("qb_peso").value = tela.peso || 150;
            campo("qb_fusionable").checked = !!tela.fusionable;
            campo("qb_termofijado").checked = !!tela.termofijado;
            if (campo("qb_elastano")) {
                campo("qb_elastano").checked = !!tela.elastano;
            }
            ponerColor(tela.color);
        }
        function guardarEnlace() {
            if (!window.URLSearchParams || !window.history || !history.replaceState || location.protocol === "file:") {
                return;
            }
            var q = new URLSearchParams();
            Object.keys(CLAVES).forEach(function (k) {
                var o = elegido(CLAVES[k]);
                if (o) {
                    q.set(k, o.value);
                }
            });
            Object.keys(CASILLAS).forEach(function (k) {
                if (campo(CASILLAS[k]) && campo(CASILLAS[k]).checked) {
                    q.set(k, "1");
                }
            });
            Object.keys(TEXTOS).forEach(function (k) {
                if (campo(TEXTOS[k]).value) {
                    q.set(k, campo(TEXTOS[k]).value);
                }
            });
            var d = leer();
            q.set("col", d.color);
            if (d.requisitos.length) {
                q.set("r", d.requisitos.join("|"));
            }
            if (d.volumen) {
                q.set("u", d.unidad);
            }
            try {
                history.replaceState(null, "", location.pathname + "?" + q.toString() + location.hash);
            } catch (err) {
                // Sin historial (vista previa incrustada): el diseño sigue funcionando.
            }
        }
        function ponerMercadoForm() {
            var mercadoForm = document.getElementById("qb_f_mercado");
            var m = elegido("qb_mercado");
            if (mercadoForm && m) {
                mercadoForm.value = m.value;
            }
        }
        function leerEnlace() {
            if (!window.URLSearchParams || !location.search) {
                return;
            }
            var q = new URLSearchParams(location.search);
            Object.keys(CLAVES).forEach(function (k) {
                if (q.has(k)) {
                    marcar(CLAVES[k], q.get(k));
                }
            });
            // Enlace desde un mercado: solo trae la aplicación y arranca con su tela propuesta.
            var apli = elegido("qb_aplicacion");
            if (apli && apli.getAttribute("data-mercado")) {
                marcar("qb_mercado", apli.getAttribute("data-mercado"));
            }
            ponerMercadoForm();
            if (q.has("ap") && !q.has("c") && apli && apli.getAttribute("data-qb-tela")) {
                ponerTela(JSON.parse(apli.getAttribute("data-qb-tela")));
                return;
            }
            Object.keys(CASILLAS).forEach(function (k) {
                if (campo(CASILLAS[k])) {
                    campo(CASILLAS[k]).checked = q.get(k) === "1";
                }
            });
            Object.keys(TEXTOS).forEach(function (k) {
                if (q.has(k)) {
                    campo(TEXTOS[k]).value = q.get(k).slice(0, 80);
                }
            });
            if (q.has("col")) {
                ponerColor(q.get("col"));
            }
            if (q.has("r")) {
                var pedidos = q.get("r").split("|");
                todos('input[name="qb_requisito"]', raizDis).forEach(function (r) {
                    r.checked = pedidos.indexOf(r.value) >= 0;
                });
            }
            if (q.has("u")) {
                raizDis.querySelector('select[name="qb_unidad"]').value = q.get("u");
            }
        }

        raizDis.addEventListener("input", function (e) {
            if (e.target.name === "qb_mercado") {
                // Cambió el mercado: la aplicación de otro mercado ya no aplica.
                var previa = elegido("qb_aplicacion");
                if (previa && previa.getAttribute("data-mercado") !== e.target.value) {
                    previa.checked = false;
                }
                ponerMercadoForm();
            }
            if (e.target.name === "qb_aplicacion") {
                var tela = e.target.getAttribute("data-qb-tela");
                if (tela) {
                    ponerTela(JSON.parse(tela));
                }
            }
            if (e.target.name === "qb_color_libre") {
                colorLibre = true;
                todos('input[name="qb_color"]', raizDis).forEach(function (c) {
                    c.checked = false;
                });
            } else if (e.target.name === "qb_color") {
                colorLibre = false;
            }
            aplicar();
        });
        var botonAzar = raizDis.querySelector("[data-qb-azar]");
        if (botonAzar) {
            botonAzar.addEventListener("click", function () {
                ["qb_construccion", "qb_galga", "qb_fibra", "qb_hilo", "qb_hilo_tipo", "qb_color", "qb_tacto"].forEach(function (nombre) {
                    var ops = todos('input[name="' + nombre + '"]', raizDis);
                    ops[Math.floor(Math.random() * ops.length)].checked = true;
                });
                colorLibre = false;
                campo("qb_peso").value = 20 + Math.round(Math.random() * 76) * 5;
                campo("qb_fusionable").checked = Math.random() < 0.4;
                aplicar();
            });
        }
        // El formulario de Odoo rellena sus campos al cargar: la solicitud se fija después,
        // y solo mientras la persona no la cambie.
        var solicitudTocada = false;
        if (solicitud) {
            solicitud.addEventListener("change", function () {
                solicitudTocada = true;
            });
            var fijar = function () {
                if (!solicitudTocada) {
                    solicitud.value = "Desarrollo de producto";
                    todos("option", solicitud).forEach(function (o) {
                        o.defaultSelected = o.value === "Desarrollo de producto";
                    });
                }
            };
            fijar();
            raizDis.addEventListener("input", fijar);
        }
        var botonCopiar = raizDis.querySelector("[data-qb-copiar]");
        if (botonCopiar) {
            botonCopiar.addEventListener("click", function () {
                var texto = botonCopiar.querySelector("span");
                var original = texto.textContent;
                var listoCopia = function () {
                    texto.textContent = T("Enlace copiado");
                    setTimeout(function () {
                        texto.textContent = original;
                    }, 2200);
                };
                if (navigator.clipboard && navigator.clipboard.writeText) {
                    navigator.clipboard.writeText(location.href).then(listoCopia, function () {
                        window.prompt(T("Copie el enlace de su diseño:"), location.href);
                    });
                } else {
                    window.prompt(T("Copie el enlace de su diseño:"), location.href);
                }
            });
        }
        var botonImprimir = raizDis.querySelector("[data-qb-imprimir]");
        if (botonImprimir) {
            botonImprimir.addEventListener("click", function () {
                window.print();
            });
        }
        raizDis.addEventListener("change", function (e) {
            if (e.target.name === "qb_unidad") {
                aplicar();
            }
        });
        leerEnlace();
        aplicar();
        // El formulario de Odoo se inicia después y vacía sus campos: el diseño se vuelve a
        // escribir al terminar la carga y al ir a enviarlo.
        setTimeout(aplicar, 600);
        setTimeout(aplicar, 2000);
        var botonEnviar = raizDis.querySelector("[data-qb-enviar]");
        if (botonEnviar) {
            botonEnviar.addEventListener("click", aplicar);
        }
        if (detalle) {
            detalle.addEventListener("focus", function () {
                if (detalle.value.indexOf(bloqueAnterior) !== 0) {
                    aplicar();
                }
            });
        }
    }

    /* --------------------------------------------------------------------- */
    function aparicion() {
        var els = todos(".qb-reveal");
        if (!els.length) {
            return;
        }
        if (sinMovimiento || !("IntersectionObserver" in window)) {
            els.forEach(function (el) { el.classList.add("qb-in"); });
            return;
        }
        raiz.classList.add("qb-js");
        var io = new IntersectionObserver(function (entradas) {
            entradas.forEach(function (en) {
                if (!en.isIntersecting) {
                    return;
                }
                var el = en.target;
                var hermanos = todos(":scope > .qb-reveal", el.parentElement);
                var i = Math.max(0, hermanos.indexOf(el));
                el.style.setProperty("--qb-d", Math.min(i, 8) * 70 + "ms");
                el.classList.add("qb-in");
                io.unobserve(el);
            });
        }, { rootMargin: "0px 0px -8% 0px", threshold: 0.05 });
        els.forEach(function (el) { io.observe(el); });
    }

    function cuentas() {
        var els = todos("[data-qb-cuenta]");
        if (!els.length || sinMovimiento || !("IntersectionObserver" in window)) {
            return;
        }
        var formato = new Intl.NumberFormat("es-MX");
        var io = new IntersectionObserver(function (entradas) {
            entradas.forEach(function (en) {
                if (!en.isIntersecting) {
                    return;
                }
                var el = en.target;
                io.unobserve(el);
                var fin = parseFloat(el.dataset.valor) || 0;
                var sinMiles = !!el.dataset.sinMiles;
                var desde = sinMiles ? fin - 60 : 0;
                var inicio = performance.now();
                (function paso(ahora) {
                    var p = Math.min(1, (ahora - inicio) / 1400);
                    var v = Math.round(desde + (fin - desde) * (1 - Math.pow(1 - p, 4)));
                    el.textContent = sinMiles ? String(v) : formato.format(v);
                    if (p < 1) {
                        requestAnimationFrame(paso);
                    }
                })(inicio);
            });
        }, { threshold: 0.6 });
        els.forEach(function (el) { io.observe(el); });
    }

    function desplazamiento() {
        var header = document.querySelector(".qb-header");
        var procesos = todos("[data-qb-proceso]");
        var contenedor = document.getElementById("wrapwrap");
        var pendiente = false;
        function leer() {
            pendiente = false;
            var y = window.scrollY || (contenedor && contenedor.scrollTop) || 0;
            var alto = Math.max(document.documentElement.scrollHeight, contenedor ? contenedor.scrollHeight : 0) - window.innerHeight;
            raiz.style.setProperty("--qb-avance", alto > 0 ? Math.min(1, y / alto).toFixed(4) : "0");
            if (header) {
                header.classList.toggle("qb-header--scroll", y > 8);
            }
            procesos.forEach(function (ol) {
                var r = ol.getBoundingClientRect();
                var p = Math.max(0, Math.min(1, (window.innerHeight * 0.72 - r.top) / Math.max(1, r.height + window.innerHeight * 0.15)));
                if (window.innerWidth > 960) {
                    p = Math.max(0, Math.min(1, (window.innerHeight * 0.85 - r.top) / (window.innerHeight * 0.55)));
                }
                ol.style.setProperty("--qb-proceso", p.toFixed(3));
                var pasos = todos(".qb-proceso__paso", ol);
                pasos.forEach(function (li, i) {
                    li.classList.toggle("qb-hecho", p >= (i + 0.15) / pasos.length);
                });
            });
        }
        function alMover() {
            if (!pendiente) {
                pendiente = true;
                requestAnimationFrame(leer);
            }
        }
        window.addEventListener("scroll", alMover, { passive: true });
        if (contenedor) {
            contenedor.addEventListener("scroll", alMover, { passive: true });
        }
        window.addEventListener("resize", alMover);
        leer();
    }

    function foco() {
        document.addEventListener("pointermove", function (e) {
            var tarjeta = e.target.closest && e.target.closest(".qb-spot");
            if (!tarjeta) {
                return;
            }
            var r = tarjeta.getBoundingClientRect();
            tarjeta.style.setProperty("--mx", (e.clientX - r.left) + "px");
            tarjeta.style.setProperty("--my", (e.clientY - r.top) + "px");
        }, { passive: true });
    }

    function menuMovil() {
        var boton = document.querySelector("[data-qb-menu]");
        var panel = document.querySelector("[data-qb-movil]");
        if (!boton || !panel) {
            return;
        }
        boton.addEventListener("click", function () {
            var abierto = panel.classList.toggle("abierto");
            boton.setAttribute("aria-expanded", abierto ? "true" : "false");
        });
    }

    /* Buscador ⌘K: productos del catálogo, mercados y páginas */
    function buscador() {
        var caja = document.querySelector("[data-qb-cmd]");
        if (!caja) {
            return;
        }
        var entrada = caja.querySelector("input");
        var lista = caja.querySelector(".qb-cmd__lista");
        var datos = null;
        var visibles = [];
        var activo = 0;

        function pintar() {
            var q = normal(entrada.value).trim();
            var terminos = q.split(/\s+/).filter(Boolean);
            visibles = (datos || []).filter(function (d) {
                if (!terminos.length) {
                    return !d.codigo;
                }
                return terminos.every(function (tm) { return d._txt.indexOf(tm) !== -1; });
            }).slice(0, 30);
            activo = 0;
            lista.textContent = "";
            if (!datos) {
                lista.innerHTML = '<div class="qb-cmd__nada">' + T("Cargando catálogo…") + '</div>';
                return;
            }
            if (!visibles.length) {
                var nada = document.createElement("div");
                nada.className = "qb-cmd__nada";
                nada.textContent = T("Sin resultados. Fabricamos sobre especificación: pida una cotización.");
                lista.appendChild(nada);
                return;
            }
            var grupo = null;
            visibles.forEach(function (d, i) {
                var nombre = d.codigo ? T("Productos") : d.categoria;
                if (nombre !== grupo) {
                    grupo = nombre;
                    var g = document.createElement("div");
                    g.className = "qb-cmd__grupo";
                    g.textContent = nombre;
                    lista.appendChild(g);
                }
                var a = document.createElement("a");
                a.className = "qb-cmd__op" + (i === 0 ? " activo" : "");
                a.href = d.url;
                a.setAttribute("role", "option");
                if (d.codigo) {
                    var c = document.createElement("code");
                    c.textContent = d.codigo;
                    a.appendChild(c);
                }
                var s = document.createElement("span");
                s.textContent = d.titulo;
                a.appendChild(s);
                var sm = document.createElement("small");
                sm.textContent = d.detalle || "";
                a.appendChild(sm);
                a.addEventListener("mousemove", function () { marcar(i); });
                lista.appendChild(a);
            });
        }
        function marcar(i) {
            var ops = todos(".qb-cmd__op", lista);
            if (!ops.length) {
                return;
            }
            activo = (i + ops.length) % ops.length;
            ops.forEach(function (o, k) { o.classList.toggle("activo", k === activo); });
            ops[activo].scrollIntoView({ block: "nearest" });
        }
        function abrir() {
            caja.hidden = false;
            entrada.value = "";
            pintar();
            entrada.focus();
            if (!datos) {
                fetch(((raiz.lang || "es").toLowerCase().indexOf("es") === 0 ? "" : "/en") + "/catalogo/buscar.json", { credentials: "same-origin" })
                    .then(function (r) { return r.ok ? r.json() : []; })
                    .catch(function () { return []; })
                    .then(function (json) {
                        datos = json.map(function (d) {
                            d._txt = normal([d.codigo, d.titulo, d.detalle, d.categoria].join(" "));
                            return d;
                        });
                        pintar();
                    });
            }
        }
        function cerrar() {
            caja.hidden = true;
        }
        todos("[data-qb-buscar]").forEach(function (b) { b.addEventListener("click", abrir); });
        caja.addEventListener("mousedown", function (e) {
            if (e.target === caja) {
                cerrar();
            }
        });
        entrada.addEventListener("input", pintar);
        document.addEventListener("keydown", function (e) {
            if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === "k") {
                e.preventDefault();
                if (caja.hidden) {
                    abrir();
                } else {
                    cerrar();
                }
                return;
            }
            if (caja.hidden) {
                return;
            }
            if (e.key === "Escape") {
                cerrar();
            } else if (e.key === "ArrowDown") {
                e.preventDefault();
                marcar(activo + 1);
            } else if (e.key === "ArrowUp") {
                e.preventDefault();
                marcar(activo - 1);
            } else if (e.key === "Enter") {
                var op = todos(".qb-cmd__op", lista)[activo];
                if (op) {
                    window.location.href = op.href;
                }
            }
        });
        if (!/Mac|iPhone|iPad/.test(navigator.platform || "")) {
            todos(".qb-kbd-btn kbd").forEach(function (k) { k.textContent = "Ctrl K"; });
        }
    }

    /* Filtro del catálogo: texto y fichas, sin recargar */
    function filtroCatalogo() {
        var rejilla = document.querySelector("[data-qb-rejilla]");
        if (!rejilla) {
            return;
        }
        var entrada = document.querySelector("[data-qb-filtro]");
        var vacio = document.querySelector("[data-qb-vacio]");
        var cuenta = document.querySelector("[data-qb-cuenta-filtro]");
        var tarjetas = todos(".qb-prod", rejilla);
        var fichas = todos("[data-qb-chips] .qb-chip");
        function aplicar() {
            var terminos = normal(entrada ? entrada.value : "").split(/\s+/).filter(Boolean);
            var activas = {};
            fichas.forEach(function (f) {
                if (f.classList.contains("activo")) {
                    (activas[f.dataset.campo] = activas[f.dataset.campo] || []).push(f.dataset.valor);
                }
            });
            var n = 0;
            tarjetas.forEach(function (tj) {
                var texto = normal(tj.dataset.qbTexto);
                var ok = terminos.every(function (tm) { return texto.indexOf(tm) !== -1; });
                Object.keys(activas).forEach(function (campo) {
                    var valor = tj.dataset["qb" + campo.charAt(0).toUpperCase() + campo.slice(1)];
                    if (activas[campo].indexOf(valor) === -1) {
                        ok = false;
                    }
                });
                tj.hidden = !ok;
                if (ok) {
                    n++;
                    tj.classList.add("qb-in");
                }
            });
            if (cuenta) {
                cuenta.textContent = n;
            }
            if (vacio) {
                vacio.hidden = n > 0;
            }
        }
        if (entrada) {
            entrada.addEventListener("input", aplicar);
            if (entrada.value) {
                aplicar();
            }
        }
        fichas.forEach(function (f) {
            f.addEventListener("click", function () {
                f.classList.toggle("activo");
                aplicar();
            });
        });
    }

    /* ---------------------------------------------------------------------
       Jet de teñido y rama, como planos técnicos animados.
       Mismo lenguaje que la circular de la portada: trazo fino, tinta y un
       acento, cotas y rótulos en monoespaciada. Se dibujan en un plano de
       640 x 360 que se escala al lienzo. No llevan temperaturas, tiempos ni
       velocidades: son esquemas, no la receta.
       ------------------------------------------------------------------ */
    var TINTA = "#18181b";
    var ACENTO = "#2f54c8";
    var CALOR = "#e8590c";

    function mezclaHex(a, b, k) {
        function canal(hex, i) { return parseInt(hex.substr(1 + i * 2, 2), 16); }
        var r = [0, 1, 2].map(function (i) { return Math.round(canal(a, i) + (canal(b, i) - canal(a, i)) * k); });
        return "rgb(" + r.join(",") + ")";
    }
    function suave(k) {
        k = Math.max(0, Math.min(1, k));
        return k * k * (3 - 2 * k);
    }
    function acelera(canvas) {
        var v = { vel: 1, objetivo: 1 };
        canvas.parentElement.addEventListener("pointerenter", function () { v.objetivo = 2.2; });
        canvas.parentElement.addEventListener("pointerleave", function () { v.objetivo = 1; });
        return v;
    }
    function rectRedondo(ctx, x, y, w, h, r) {
        ctx.beginPath();
        ctx.moveTo(x + r, y);
        ctx.arcTo(x + w, y, x + w, y + h, r);
        ctx.arcTo(x + w, y + h, x, y + h, r);
        ctx.arcTo(x, y + h, x, y, r);
        ctx.arcTo(x, y, x + w, y, r);
        ctx.closePath();
    }
    /* Plano de 640 x 360 centrado en el lienzo, con retícula de puntos. Devuelve la escala. */
    function plano(ctx, w, h) {
        var s = Math.min(w / 640, h / 360);
        ctx.clearRect(0, 0, w, h);
        ctx.fillStyle = "rgba(9, 9, 11, .07)";
        for (var gx = (w % 20) / 2; gx < w; gx += 20) {
            for (var gy = (h % 20) / 2; gy < h; gy += 20) {
                ctx.fillRect(gx, gy, 1, 1);
            }
        }
        ctx.save();
        ctx.translate((w - 640 * s) / 2, (h - 360 * s) / 2);
        ctx.scale(s, s);
        ctx.lineCap = "round";
        ctx.lineJoin = "round";
        return s;
    }
    function rueda(ctx, x, y, r, giro, aspas) {
        ctx.beginPath();
        for (var i = 0; i < aspas; i++) {
            var a = giro + i * TAU / aspas;
            ctx.moveTo(x, y);
            ctx.lineTo(x + Math.cos(a) * r, y + Math.sin(a) * r);
        }
        ctx.stroke();
    }

    /* Polilínea plana [x0, y0, x1, y1…] con su largo acumulado, para trazar solo un tramo. */
    function medirLinea(pts) {
        var ac = [0];
        for (var i = 2; i < pts.length; i += 2) {
            ac.push(ac[ac.length - 1] + Math.hypot(pts[i] - pts[i - 2], pts[i + 1] - pts[i - 1]));
        }
        return ac;
    }
    function tramo(ctx, pts, ac, s0, s1) {
        var abierto = false;
        ctx.beginPath();
        for (var i = 1; i < ac.length; i++) {
            if (ac[i] < s0) { continue; }
            if (ac[i - 1] > s1) { break; }
            var x0 = pts[2 * i - 2];
            var y0 = pts[2 * i - 1];
            var x1 = pts[2 * i];
            var y1 = pts[2 * i + 1];
            var d = (ac[i] - ac[i - 1]) || 1;
            if (!abierto) {
                var k0 = Math.max(0, (s0 - ac[i - 1]) / d);
                ctx.moveTo(x0 + (x1 - x0) * k0, y0 + (y1 - y0) * k0);
                abierto = true;
            }
            var k1 = Math.min(1, (s1 - ac[i - 1]) / d);
            ctx.lineTo(x0 + (x1 - x0) * k1, y0 + (y1 - y0) * k1);
        }
    }
    function ruido(n) {
        var v = Math.sin(n * 127.1 + 311.7) * 43758.5453;
        return v - Math.floor(v);
    }
    function conAlfa(rgb, a) {
        return rgb.replace("rgb(", "rgba(").replace(")", ", " + a.toFixed(3) + ")");
    }
    /* Etapas del proceso (lista junto al lienzo): marca la que va corriendo. */
    function etapasDe(canvas) {
        var fig = canvas.closest ? canvas.closest("figure") : null;
        var lista = fig ? fig.querySelector("[data-qb-fases]") : null;
        var items = lista ? todos("li", lista) : [];
        var actual = -1;
        var tono = "";
        return function (i, color) {
            if (!lista) { return; }
            if (i !== actual) {
                actual = i;
                items.forEach(function (li, k) { li.classList.toggle("es-actual", k === i); });
            }
            if (color !== tono) {
                tono = color;
                lista.style.setProperty("--qb-lote", color);
            }
        };
    }
    var LOTES = ["#2f54c8", "#0f766e", "#9f1239", "#27272a"];

    /* ---------------------------------------------------------------------
       Jet de teñido, en corte. La tela va cosida en cuerda sin fin: el
       torniquete la sube, la tobera la impulsa con el baño por el tubo y cae
       plegada al fondo, donde avanza despacio hasta volver a subir. El baño
       entra claro, toma el color y lo va cediendo a la tela.
       ------------------------------------------------------------------ */
    function jet(canvas) {
        var CICLO = 17;
        var CRUDO = "#e2dccd";
        var ritmo = acelera(canvas);
        var marcar = etapasDe(canvas);
        var reloj = CICLO * 0.36;
        var recorrido = 0;
        var VEL = 64;                   // avance de la cuerda
        var VX0 = 96, VX1 = 544, VY0 = 150, VY1 = 296, VR = 73, VCY = 223;
        var XR = 496, XF = 166;         // extremos de la pila de pliegues
        var PASO = 20, AMP = 26, C = PASO / TAU, M = 0.46, DQ = TAU / 20;
        var NIVEL = 238;

        function piso(x) {
            var dx = 0;
            if (x < VX0 + VR) { dx = VX0 + VR - x; } else if (x > VX1 - VR) { dx = x - (VX1 - VR); }
            var r = VR - 9;
            return VCY + Math.sqrt(Math.max(0, r * r - dx * dx));
        }
        // largo de cuerda que guarda cada pliegue: con él la pila avanza al ritmo justo
        var LARGO = (function () {
            var l = 0, xa = 0, ya = 0;
            for (var q = DQ; q <= TAU + 0.001; q += DQ) {
                var x = -C * q + C * M * Math.sin(2 * q);
                var y = -AMP * 0.97 * (1 + Math.sin(q));
                l += Math.hypot(x - xa, y - ya);
                xa = x; ya = y;
            }
            return l;
        })();
        function cuerda() {
            var p = [];
            var i, k, ang;
            var qs = -(recorrido / LARGO) * TAU;
            p.push(250, 104, 478, 104);
            for (i = 1; i <= 8; i++) {
                ang = -Math.PI / 2 + i / 8 * Math.PI / 2;
                p.push(478 + 34 * Math.cos(ang), 138 + 34 * Math.sin(ang));
            }
            var yr = piso(XR);
            p.push(512, 236);
            for (i = 1; i <= 7; i++) {
                k = i / 7;
                p.push(512 + (XR - 512) * k * k, (1 - k) * (1 - k) * 236 + (1 - (1 - k) * (1 - k)) * yr);
            }
            var qe = qs + (XR - XF) / C;
            for (var q = qs + DQ; q < qe; q += DQ) {
                var base = XR - C * (q - qs);
                var env = suave((XR - base) / 36) * suave((base - XF) / 40);
                var n = Math.floor((q - 1.5 * Math.PI) / TAU);
                var amp = AMP * env * (0.74 + 0.4 * ruido(n));
                var sube = 1 + Math.sin(q);
                p.push(base + C * M * Math.sin(2 * q) * env + 0.16 * amp * sube * (ruido(n + 40) - 0.2),
                    piso(base) - amp * sube);
            }
            var yf = piso(XF);
            for (i = 0; i <= 7; i++) {
                k = i / 7;
                p.push(XF + (150 - XF) * (1 - (1 - k) * (1 - k)), yf + (244 - yf) * k * k);
            }
            p.push(150, 92);
            for (i = 1; i <= 10; i++) {
                ang = Math.PI + i / 10 * 0.78 * Math.PI;
                p.push(176 + 26 * Math.cos(ang), 92 + 26 * Math.sin(ang));
            }
            for (i = 1; i <= 8; i++) {
                k = i / 8;
                var u = 1 - k;
                p.push(u * u * u * 196 + 3 * u * u * k * 204.9 + 3 * u * k * k * 218 + k * k * k * 232,
                    u * u * u * 75.4 + 3 * u * u * k * 86.2 + 3 * u * k * k * 104 + k * k * k * 104);
            }
            p.push(250, 104);
            return p;
        }
        function vasija(ctx) {
            rectRedondo(ctx, VX0, VY0, VX1 - VX0, VY1 - VY0, VR);
        }
        function tubo(ctx) {
            ctx.beginPath();
            ctx.moveTo(204, 104);
            ctx.lineTo(478, 104);
            ctx.arc(478, 138, 34, -Math.PI / 2, 0);
            ctx.lineTo(512, 176);
        }
        function recirculacion(ctx) {
            ctx.beginPath();
            ctx.moveTo(330, 294);
            ctx.lineTo(330, 314);
            ctx.arcTo(330, 324, 320, 324, 10);
            ctx.lineTo(68, 324);
            ctx.arcTo(58, 324, 58, 314, 10);
            ctx.lineTo(58, 50);
            ctx.arcTo(58, 40, 68, 40, 10);
            ctx.lineTo(220, 40);
            ctx.arcTo(230, 40, 230, 50, 10);
            ctx.lineTo(230, 90);
        }

        lienzo(canvas, function (ctx, w, h, t, dt) {
            ritmo.vel += (ritmo.objetivo - ritmo.vel) * Math.min(1, dt * 2.5);
            reloj += dt * ritmo.vel;
            recorrido += dt * ritmo.vel * VEL;
            var lote = Math.floor(reloj / CICLO);
            var p = (reloj % CICLO) / CICLO;
            var color = LOTES[lote % LOTES.length];

            // Guion del lote
            var entra = suave(p / 0.07);
            var sale = suave((p - 0.9) / 0.075);
            var nivel = p < 0.6 ? suave(p / 0.075)
                : p < 0.66 ? 1 - suave((p - 0.6) / 0.06)
                : p < 0.84 ? suave((p - 0.66) / 0.06)
                : 1 - suave((p - 0.84) / 0.05);
            var carga = p < 0.66 ? suave((p - 0.085) / 0.06) * (1 - 0.8 * suave((p - 0.17) / 0.4)) : 0.06;
            var tenido = suave((p - 0.11) / 0.45);
            var calor = suave((p - 0.08) / 0.1) * (1 - suave((p - 0.5) / 0.1));
            var tono = mezclaHex(CRUDO, color, tenido);
            var bano = mezclaHex("#dbe5fb", color, 0.1 + 0.62 * carga);
            var banoFuerte = mezclaHex("#9db4e8", color, 0.25 + 0.7 * carga);
            var yBano = VY1 - (VY1 - NIVEL) * nivel;
            var bombea = suave((nivel - 0.35) / 0.3);
            marcar(p < 0.085 ? 0 : p < 0.6 ? 1 : p < 0.86 ? 2 : 3, tenido > 0.02 ? tono : "");

            var s = plano(ctx, w, h);
            var i;

            // Piso y sombra
            var sombra = ctx.createRadialGradient(320, 344, 10, 320, 344, 250);
            sombra.addColorStop(0, "rgba(9, 9, 11, .10)");
            sombra.addColorStop(1, "rgba(9, 9, 11, 0)");
            ctx.save();
            ctx.translate(0, 344);
            ctx.scale(1, 0.05);
            ctx.translate(0, -344);
            ctx.fillStyle = sombra;
            ctx.fillRect(40, 94, 560, 500);
            ctx.restore();
            ctx.strokeStyle = "#e4e4e7";
            ctx.lineWidth = 1;
            ctx.beginPath();
            ctx.moveTo(36, 344);
            ctx.lineTo(604, 344);
            ctx.stroke();

            // Apoyos
            ctx.strokeStyle = "#a1a1aa";
            ctx.lineWidth = 1.2;
            [196, 444].forEach(function (ax) {
                ctx.beginPath();
                ctx.moveTo(ax - 12, 290);
                ctx.lineTo(ax - 18, 344);
                ctx.moveTo(ax + 12, 290);
                ctx.lineTo(ax + 18, 344);
                ctx.moveTo(ax - 26, 344);
                ctx.lineTo(ax + 26, 344);
                ctx.moveTo(ax - 15, 318);
                ctx.lineTo(ax + 15, 318);
                ctx.stroke();
            });

            // Recirculación del baño: bomba, intercambiador y regreso a la tobera
            recirculacion(ctx);
            ctx.strokeStyle = "#a1a1aa";
            ctx.lineWidth = 6.4;
            ctx.stroke();
            ctx.strokeStyle = "#fff";
            ctx.lineWidth = 4.2;
            ctx.stroke();
            if (bombea > 0.01) {
                ctx.save();
                ctx.globalAlpha = bombea;
                ctx.strokeStyle = banoFuerte;
                ctx.lineWidth = 2.2;
                ctx.setLineDash([7, 9]);
                ctx.lineDashOffset = -reloj * 70;
                ctx.stroke();
                ctx.restore();
            }
            // intercambiador de calor
            rectRedondo(ctx, 46, 132, 24, 96, 12);
            ctx.fillStyle = "#fff";
            ctx.fill();
            ctx.strokeStyle = TINTA;
            ctx.lineWidth = 1.3;
            ctx.stroke();
            ctx.save();
            rectRedondo(ctx, 46, 132, 24, 96, 12);
            ctx.clip();
            ctx.fillStyle = "rgba(232, 89, 12, " + (0.05 + 0.2 * calor).toFixed(3) + ")";
            ctx.fillRect(46, 132, 24, 96);
            ctx.strokeStyle = "rgba(232, 89, 12, " + (0.3 + 0.7 * calor).toFixed(3) + ")";
            ctx.lineWidth = 1.3;
            ctx.beginPath();
            for (i = 0; i < 9; i++) {
                ctx.moveTo(51, 142 + i * 9.6);
                ctx.lineTo(65, 142 + i * 9.6);
            }
            ctx.stroke();
            ctx.restore();
            // bomba
            ctx.fillStyle = "#fff";
            ctx.strokeStyle = TINTA;
            ctx.lineWidth = 1.3;
            ctx.beginPath();
            ctx.arc(236, 324, 13, 0, TAU);
            ctx.fill();
            ctx.stroke();
            ctx.strokeStyle = "#71717a";
            ctx.lineWidth = 1.2;
            rueda(ctx, 236, 324, 8.5, reloj * 6 * (0.15 + bombea), 5);
            ctx.fillStyle = TINTA;
            ctx.beginPath();
            ctx.arc(236, 324, 2, 0, TAU);
            ctx.fill();

            // Tubo de transporte y cuello de subida
            ctx.lineCap = "butt";
            tubo(ctx);
            ctx.strokeStyle = TINTA;
            ctx.lineWidth = 31;
            ctx.stroke();
            ctx.strokeStyle = "#fff";
            ctx.lineWidth = 28;
            ctx.stroke();
            ctx.beginPath();
            ctx.moveTo(150, 176);
            ctx.lineTo(150, 100);
            ctx.strokeStyle = TINTA;
            ctx.lineWidth = 25;
            ctx.stroke();
            if (bombea > 0.01) {
                ctx.beginPath();
                ctx.moveTo(250, 104);
                ctx.lineTo(478, 104);
                ctx.arc(478, 138, 34, -Math.PI / 2, 0);
                ctx.lineTo(512, 176);
                ctx.strokeStyle = conAlfa(bano, 0.75 * bombea);
                ctx.lineWidth = 28;
                ctx.stroke();
            }
            ctx.lineCap = "round";

            // Autoclave
            vasija(ctx);
            var cuerpo = ctx.createLinearGradient(0, VY0, 0, VY1);
            cuerpo.addColorStop(0, "#ffffff");
            cuerpo.addColorStop(1, "#f6f6f7");
            ctx.fillStyle = cuerpo;
            ctx.fill();
            // carcasa del torniquete
            ctx.beginPath();
            ctx.arc(176, 92, 37.5, 0, TAU);
            ctx.fillStyle = "#fff";
            ctx.fill();
            ctx.strokeStyle = TINTA;
            ctx.lineWidth = 1.5;
            ctx.stroke();
            // Pared del autoclave; el cuello y el tubo se abren a él
            vasija(ctx);
            ctx.strokeStyle = TINTA;
            ctx.lineWidth = 1.6;
            ctx.stroke();
            ctx.save();
            vasija(ctx);
            ctx.clip();
            ctx.strokeStyle = "#ececee";
            ctx.lineWidth = 9;
            vasija(ctx);
            ctx.stroke();
            ctx.restore();
            ctx.lineCap = "butt";
            ctx.strokeStyle = "#fff";
            ctx.beginPath();
            ctx.moveTo(150, 98);
            ctx.lineTo(150, 186);
            ctx.lineWidth = 22;
            ctx.stroke();
            ctx.beginPath();
            ctx.moveTo(512, 140);
            ctx.lineTo(512, 178);
            ctx.lineWidth = 28;
            ctx.stroke();
            ctx.lineCap = "round";
            if (bombea > 0.01) {
                // el baño cae del tubo con la tela
                ctx.fillStyle = conAlfa(bano, 0.75 * bombea);
                ctx.beginPath();
                ctx.moveTo(498, 138);
                ctx.lineTo(526, 138);
                ctx.lineTo(526, 176);
                ctx.quadraticCurveTo(524, 210, 519, yBano + 2);
                ctx.lineTo(505, yBano + 2);
                ctx.quadraticCurveTo(500, 210, 498, 176);
                ctx.closePath();
                ctx.fill();
            }

            // Baño (fondo)
            function superficie() {
                ctx.beginPath();
                ctx.moveTo(VX0 - 4, VY1 + 4);
                for (var bx = VX0 - 4; bx <= VX1 + 4; bx += 6) {
                    ctx.lineTo(bx, yBano + Math.sin(bx * 0.04 + reloj * 1.5) * 1.5 * nivel + Math.sin(bx * 0.11 - reloj * 2.3) * 0.8 * nivel);
                }
                ctx.lineTo(VX1 + 4, VY1 + 4);
                ctx.closePath();
            }
            ctx.save();
            vasija(ctx);
            ctx.clip();
            if (nivel > 0.01) {
                superficie();
                var fondo = ctx.createLinearGradient(0, yBano, 0, VY1);
                fondo.addColorStop(0, conAlfa(bano, 0.7));
                fondo.addColorStop(1, conAlfa(banoFuerte, 0.55));
                ctx.fillStyle = fondo;
                ctx.fill();
            }
            ctx.restore();

            // Tela en cuerda
            var pts = cuerda();
            var ac = medirLinea(pts);
            var total = ac[ac.length - 1];
            var s0 = sale * total;
            var s1 = entra * total;
            if (s1 - s0 > 1) {
                tramo(ctx, pts, ac, s0, s1);
                ctx.strokeStyle = mezclaHex("#8b8778", color, tenido * 0.85);
                ctx.globalAlpha = 0.9;
                ctx.lineWidth = 8.2;
                ctx.stroke();
                ctx.globalAlpha = 1;
                ctx.strokeStyle = tono;
                ctx.lineWidth = 6.4;
                ctx.stroke();
                ctx.save();
                ctx.lineCap = "butt";
                ctx.setLineDash([1.3, 5.2]);
                ctx.lineDashOffset = s0 - recorrido;
                ctx.strokeStyle = "rgba(255, 255, 255, .34)";
                ctx.lineWidth = 6.4;
                ctx.stroke();
                ctx.restore();
            }

            // Baño (frente): la tela sumergida se ve a través
            ctx.save();
            vasija(ctx);
            ctx.clip();
            if (nivel > 0.01) {
                superficie();
                ctx.fillStyle = conAlfa(bano, 0.42);
                ctx.fill();
                // burbujas: el baño hierve cuando sube la temperatura
                ctx.strokeStyle = "rgba(255, 255, 255, " + (0.25 + 0.6 * calor).toFixed(3) + ")";
                ctx.lineWidth = 1;
                for (i = 0; i < 16; i++) {
                    var kb = (reloj * (0.28 + 0.2 * ruido(i)) + ruido(i + 9)) % 1;
                    var bx0 = VX0 + 40 + ruido(i + 3) * (VX1 - VX0 - 80);
                    var by0 = piso(bx0) - kb * (piso(bx0) - yBano);
                    if (by0 > yBano + 3) {
                        ctx.beginPath();
                        ctx.arc(bx0 + Math.sin(kb * 9 + i) * 2, by0, 1 + 1.6 * ruido(i + 5), 0, TAU);
                        ctx.stroke();
                    }
                }
                ctx.strokeStyle = conAlfa(banoFuerte, 0.9);
                ctx.lineWidth = 1.1;
                ctx.beginPath();
                for (var sx = VX0; sx <= VX1; sx += 6) {
                    ctx.lineTo(sx, yBano + Math.sin(sx * 0.04 + reloj * 1.5) * 1.5 * nivel + Math.sin(sx * 0.11 - reloj * 2.3) * 0.8 * nivel);
                }
                ctx.stroke();
            }
            ctx.restore();

            // Costuras del autoclave
            ctx.strokeStyle = "rgba(9, 9, 11, .10)";
            ctx.lineWidth = 1;
            ctx.beginPath();
            ctx.moveTo(VX0 + VR, VY0 + 1);
            ctx.lineTo(VX0 + VR, VY1 - 1);
            ctx.moveTo(VX1 - VR, VY0 + 1);
            ctx.lineTo(VX1 - VR, VY1 - 1);
            ctx.stroke();

            // Chorro del baño: empuja la cuerda por el tubo y cae con ella
            if (bombea > 0.01) {
                ctx.save();
                ctx.globalAlpha = bombea;
                ctx.lineCap = "round";
                ctx.strokeStyle = "rgba(255, 255, 255, .9)";
                ctx.lineWidth = 1.3;
                [-8.5, 8.5].forEach(function (dy, j) {
                    ctx.beginPath();
                    ctx.moveTo(254, 104 + dy);
                    ctx.lineTo(478, 104 + dy);
                    ctx.arc(478, 138, 34 - dy, -Math.PI / 2, 0);
                    ctx.lineTo(512 - dy, yBano - 3);
                    ctx.setLineDash([9, 16]);
                    ctx.lineDashOffset = -reloj * 190 - j * 11;
                    ctx.stroke();
                });
                ctx.restore();
            }

            // Bridas del tubo
            ctx.strokeStyle = TINTA;
            ctx.lineWidth = 1.2;
            ctx.beginPath();
            [330, 420].forEach(function (bx) {
                ctx.moveTo(bx, 86);
                ctx.lineTo(bx, 122);
                ctx.moveTo(bx + 4, 86);
                ctx.lineTo(bx + 4, 122);
            });
            ctx.stroke();

            // Torniquete
            var giro = recorrido / 26;
            ctx.strokeStyle = "#71717a";
            ctx.lineWidth = 1.2;
            ctx.beginPath();
            ctx.arc(176, 92, 21.5, 0, TAU);
            ctx.stroke();
            rueda(ctx, 176, 92, 21.5, giro, 8);
            ctx.fillStyle = "#fff";
            ctx.strokeStyle = TINTA;
            ctx.lineWidth = 1.3;
            ctx.beginPath();
            ctx.arc(176, 92, 5, 0, TAU);
            ctx.fill();
            ctx.stroke();

            // Tobera
            ctx.fillStyle = TINTA;
            ctx.beginPath();
            ctx.moveTo(210, 86);
            ctx.lineTo(240, 86);
            ctx.lineTo(256, 92);
            ctx.lineTo(256, 116);
            ctx.lineTo(240, 122);
            ctx.lineTo(210, 122);
            ctx.closePath();
            ctx.fill();
            ctx.lineJoin = "round";
            ctx.strokeStyle = TINTA;
            ctx.lineWidth = 3;
            ctx.stroke();
            ctx.strokeStyle = "rgba(255, 255, 255, .35)";
            ctx.lineWidth = 1;
            ctx.beginPath();
            ctx.moveTo(217, 92);
            ctx.lineTo(217, 116);
            ctx.moveTo(223, 92);
            ctx.lineTo(223, 116);
            ctx.stroke();
            ctx.restore();
        });
    }

    /* ---------------------------------------------------------------------
       Rama, vista en perspectiva. La tela entra húmeda y angosta, las cadenas
       la toman por las orillas y la abren al ancho; cruza las cámaras de aire
       caliente, donde se seca y se termofija, y sale derecha al rollo.
       ------------------------------------------------------------------ */
    function rama(canvas) {
        var ritmo = acelera(canvas);
        var marcar = etapasDe(canvas);
        var LOTE = 780;
        var avance = 300;
        var reloj = 0;
        var AXX = 1, AXY = 0.17, AYX = -0.5, AYY = 0.42;
        var OX = 84, OY = 150;
        var M0 = 38, M1 = 78;
        var X_IN = 8, X_ROLLO = 505, R_ROLLO = 21;
        var H0 = 150, MOD = 68, NMOD = 4, H1 = H0 + MOD * NMOD;
        var YB = 96, ZB = -40, ZT = 44, ZPISO = -40;

        function P(x, y, z) {
            return [OX + x * AXX + y * AYX, OY + x * AXY + y * AYY - (z || 0)];
        }
        function medio(x) {
            return M0 + (M1 - M0) * suave((x - 40) / 110);
        }
        // dibuja en el plano horizontal de altura z (x a lo largo, y a lo ancho)
        function horizontal(ctx, z, fn) {
            ctx.save();
            ctx.transform(AXX, AXY, AYX, AYY, OX, OY - z);
            fn();
            ctx.restore();
        }
        // dibuja en el plano vertical a lo largo de la máquina (x a lo largo, z hacia arriba)
        function lateral(ctx, y, fn) {
            var o = P(0, y, 0);
            ctx.save();
            ctx.transform(AXX, AXY, 0, -1, o[0], o[1]);
            fn();
            ctx.restore();
        }
        function cara(ctx, pts) {
            ctx.beginPath();
            pts.forEach(function (q, i) {
                var c = P(q[0], q[1], q[2]);
                if (i) { ctx.lineTo(c[0], c[1]); } else { ctx.moveTo(c[0], c[1]); }
            });
            ctx.closePath();
        }
        function linea(ctx, a, b) {
            var c0 = P(a[0], a[1], a[2]);
            var c1 = P(b[0], b[1], b[2]);
            ctx.moveTo(c0[0], c0[1]);
            ctx.lineTo(c1[0], c1[1]);
        }
        function colorEn(x) {
            var n = Math.floor((x - avance) / LOTE);
            return LOTES[((-n % LOTES.length) + LOTES.length) % LOTES.length];
        }
        function orillas(ctx, x0, x1) {
            var x;
            ctx.beginPath();
            ctx.moveTo(x0, -medio(x0));
            for (x = x0; x < x1; x += 5) { ctx.lineTo(x, -medio(x)); }
            ctx.lineTo(x1, -medio(x1));
            ctx.lineTo(x1, medio(x1));
            for (x = x1; x > x0; x -= 5) { ctx.lineTo(x, medio(x)); }
            ctx.lineTo(x0, medio(x0));
            ctx.closePath();
        }
        // cilindro acostado a lo ancho de la máquina
        function cilindro(ctx, x, z, r, y0, y1, relleno, borde) {
            var dx = -AYX, dy = AYY;      // perpendicular al eje, en pantalla
            var amax = 0, amin = 0, vmax = -1e9, vmin = 1e9;
            for (var a = 0; a < TAU; a += 0.05) {
                var px = Math.sin(a) * r * AXX;
                var py = Math.sin(a) * r * AXY - Math.cos(a) * r;
                var v = px * dy + py * dx;
                if (v > vmax) { vmax = v; amax = a; }
                if (v < vmin) { vmin = v; amin = a; }
            }
            function borda(y, a) { return P(x + Math.sin(a) * r, y, z + Math.cos(a) * r); }
            function tapa(y) {
                ctx.beginPath();
                for (var a2 = 0; a2 <= TAU + 0.01; a2 += 0.12) {
                    var c = borda(y, a2);
                    if (a2) { ctx.lineTo(c[0], c[1]); } else { ctx.moveTo(c[0], c[1]); }
                }
                ctx.closePath();
            }
            var a0 = borda(y0, amax), a1 = borda(y1, amax), b0 = borda(y0, amin), b1 = borda(y1, amin);
            ctx.fillStyle = relleno;
            ctx.strokeStyle = borde;
            tapa(y0);
            ctx.fill();
            ctx.beginPath();
            ctx.moveTo(a0[0], a0[1]);
            ctx.lineTo(a1[0], a1[1]);
            ctx.lineTo(b1[0], b1[1]);
            ctx.lineTo(b0[0], b0[1]);
            ctx.closePath();
            ctx.fill();
            ctx.beginPath();
            ctx.moveTo(a0[0], a0[1]);
            ctx.lineTo(a1[0], a1[1]);
            ctx.moveTo(b0[0], b0[1]);
            ctx.lineTo(b1[0], b1[1]);
            ctx.stroke();
            tapa(y1);
            ctx.fill();
            ctx.stroke();
            return { amin: amin, amax: amax, borda: borda };
        }

        lienzo(canvas, function (ctx, w, h, t, dt) {
            ritmo.vel += (ritmo.objetivo - ritmo.vel) * Math.min(1, dt * 2.5);
            reloj += dt * ritmo.vel;
            avance += dt * ritmo.vel * 40;
            var costura = ((avance % LOTE) + LOTE) % LOTE - 70;      // x de la unión entre lotes
            var cabeza = colorEn(costura - 1);
            marcar(costura < 40 ? 0 : costura < H0 ? 1 : costura < H1 ? 2 : 3, cabeza);
            var s = plano(ctx, w, h);
            var i, x, c;
            ctx.translate(320, 180);
            ctx.scale(0.93, 0.93);
            ctx.translate(-320, -180);

            // Sombra en el piso
            ctx.save();
            ctx.shadowColor = "rgba(9, 9, 11, .10)";
            ctx.shadowBlur = 30 * s;
            ctx.shadowOffsetY = 6 * s;
            ctx.fillStyle = "rgba(9, 9, 11, .025)";
            cara(ctx, [[-40, -60, ZPISO], [X_ROLLO + 24, -92, ZPISO], [X_ROLLO + 24, 92, ZPISO], [-6, 60, ZPISO]]);
            ctx.fill();
            ctx.restore();

            // Bastidor: patas y largueros
            ctx.strokeStyle = "#c4c4cc";
            ctx.lineWidth = 1.1;
            ctx.beginPath();
            [-1, 1].forEach(function (lado) {
                [44, 100, 440, 476].forEach(function (px) {
                    var m = medio(px) + 4;
                    linea(ctx, [px, lado * m, 0], [px, lado * m, ZPISO]);
                });
                linea(ctx, [44, lado * (medio(44) + 4), -22], [100, lado * (medio(100) + 4), -22]);
                linea(ctx, [440, lado * (M1 + 4), -22], [476, lado * (M1 + 4), -22]);
            });
            ctx.stroke();

            // Carro de tela húmeda y subida a la entrada
            var XC = -34, ZC = -20;
            ctx.fillStyle = "#fff";
            ctx.strokeStyle = "#a1a1aa";
            ctx.lineWidth = 1.1;
            cara(ctx, [[XC - 16, -M0 - 8, ZC], [XC + 12, -M0 - 8, ZC], [XC + 12, M0 + 8, ZC], [XC - 16, M0 + 8, ZC]]);
            ctx.fill();
            ctx.stroke();
            cara(ctx, [[XC - 16, M0 + 8, ZC], [XC + 12, M0 + 8, ZC], [XC + 12, M0 + 8, ZPISO], [XC - 16, M0 + 8, ZPISO]]);
            ctx.fill();
            ctx.stroke();
            cara(ctx, [[XC + 12, -M0 - 8, ZC], [XC + 12, M0 + 8, ZC], [XC + 12, M0 + 8, ZPISO], [XC + 12, -M0 - 8, ZPISO]]);
            ctx.fill();
            ctx.stroke();
            var tIn = colorEn(X_IN);
            cara(ctx, [[XC, -M0, ZC], [XC, M0, ZC], [X_IN, M0, 0], [X_IN, -M0, 0]]);
            ctx.fillStyle = mezclaHex(tIn, "#ffffff", 0.52);
            ctx.fill();
            ctx.strokeStyle = conAlfa(mezclaHex(tIn, "#ffffff", 0), 0.5);
            ctx.lineWidth = 0.75;
            ctx.beginPath();
            for (var ks = ((avance % 9) / 9) / 5; ks < 1; ks += 0.2) {
                linea(ctx, [XC + (X_IN - XC) * ks, -M0, ZC * (1 - ks)], [XC + (X_IN - XC) * ks, M0, ZC * (1 - ks)]);
            }
            for (c = -8; c <= 8; c++) {
                linea(ctx, [XC, c / 9 * M0, ZC], [X_IN, c / 9 * M0, 0]);
            }
            ctx.stroke();

            // Fondo y pared de atrás de las cámaras
            ctx.fillStyle = "rgba(244, 244, 245, .9)";
            ctx.strokeStyle = "rgba(9, 9, 11, .22)";
            ctx.lineWidth = 1;
            cara(ctx, [[H0, -YB, ZB], [H1, -YB, ZB], [H1, YB, ZB], [H0, YB, ZB]]);
            ctx.fill();
            cara(ctx, [[H0, -YB, ZB], [H1, -YB, ZB], [H1, -YB, ZT], [H0, -YB, ZT]]);
            ctx.fillStyle = "rgba(250, 250, 250, .9)";
            ctx.fill();
            ctx.stroke();
            cara(ctx, [[H0, -YB, ZB], [H0, YB, ZB], [H0, YB, ZT], [H0, -YB, ZT]]);
            ctx.fill();
            ctx.stroke();

            // Tela
            horizontal(ctx, 0, function () {
                // color por franjas: húmeda al entrar, más clara conforme seca
                for (x = X_IN; x < X_ROLLO; x += 5) {
                    var seca = suave((x - H0 + 20) / 230);
                    var x2 = Math.min(X_ROLLO, x + 5.6);
                    ctx.fillStyle = mezclaHex(colorEn(x + 2.5), "#ffffff", 0.52 + 0.3 * seca);
                    ctx.beginPath();
                    ctx.moveTo(x, -medio(x));
                    ctx.lineTo(x2, -medio(x2));
                    ctx.lineTo(x2, medio(x2));
                    ctx.lineTo(x, medio(x));
                    ctx.closePath();
                    ctx.fill();
                }
                // malla: columnas que se abren con el ancho y pasadas que entran
                // arqueadas y salen derechas
                var cortes = [X_IN, Math.max(X_IN, Math.min(X_ROLLO, costura)), X_ROLLO];
                for (var k = 0; k < 2; k++) {
                    var xa = cortes[k], xb = cortes[k + 1];
                    if (xb - xa < 1) { continue; }
                    var tinta = colorEn((xa + xb) / 2);
                    ctx.save();
                    orillas(ctx, xa, xb);
                    ctx.clip();
                    ctx.strokeStyle = conAlfa(mezclaHex(tinta, "#ffffff", 0), 0.42);
                    ctx.lineWidth = 0.75;
                    ctx.beginPath();
                    for (c = -8; c <= 8; c++) {
                        for (x = xa; x <= xb + 6; x += 6) {
                            var yy = (c / 9) * medio(x);
                            if (x === xa) { ctx.moveTo(x, yy); } else { ctx.lineTo(x, yy); }
                        }
                    }
                    ctx.stroke();
                    ctx.strokeStyle = conAlfa(mezclaHex(tinta, "#ffffff", 0), 0.5);
                    ctx.beginPath();
                    for (var xp = X_IN - 30 + (avance % 9); xp <= X_ROLLO + 4; xp += 9) {
                        var chueca = 1 - suave((xp - 30) / 135);
                        var m = medio(xp);
                        for (var f = -1; f <= 1.001; f += 0.125) {
                            var xo = xp - chueca * (15 * (1 - f * f) + 7 * f + 2.5 * Math.sin(f * 5 + xp * 0.3));
                            if (f === -1) { ctx.moveTo(xo, f * m); } else { ctx.lineTo(xo, f * m); }
                        }
                    }
                    ctx.stroke();
                    ctx.restore();
                }
                // orillas y unión entre lotes
                ctx.lineWidth = 1;
                [-1, 1].forEach(function (lado) {
                    ctx.strokeStyle = "rgba(9, 9, 11, .28)";
                    ctx.beginPath();
                    for (x = X_IN; x <= X_ROLLO; x += 5) {
                        ctx.lineTo(x, lado * medio(x));
                    }
                    ctx.stroke();
                });
                if (costura > X_IN && costura < X_ROLLO) {
                    ctx.strokeStyle = "rgba(9, 9, 11, .55)";
                    ctx.lineWidth = 1.2;
                    ctx.setLineDash([3, 2.5]);
                    ctx.beginPath();
                    ctx.moveTo(costura, -medio(costura));
                    ctx.lineTo(costura, medio(costura));
                    ctx.stroke();
                    ctx.setLineDash([]);
                }
                // calor bajo las cámaras
                ctx.fillStyle = "rgba(232, 89, 12, .07)";
                ctx.fillRect(H0, -M1, H1 - H0, M1 * 2);
            });

            // Cadenas: riel, agujas que avanzan con la tela y piñones
            horizontal(ctx, 0.5, function () {
                [-1, 1].forEach(function (lado) {
                    ctx.beginPath();
                    for (x = 34; x <= 474; x += 4) {
                        ctx.lineTo(x, lado * (medio(x) + 3));
                    }
                    ctx.strokeStyle = TINTA;
                    ctx.lineWidth = 1.5;
                    ctx.stroke();
                    ctx.save();
                    ctx.lineCap = "butt";
                    ctx.setLineDash([1.3, 5.7]);
                    ctx.lineDashOffset = -avance;
                    ctx.lineWidth = 6.5;
                    ctx.strokeStyle = "rgba(9, 9, 11, .62)";
                    ctx.stroke();
                    ctx.restore();
                    [34, 474].forEach(function (xs) {
                        var ys = lado * (medio(xs) + 3);
                        ctx.fillStyle = "#fff";
                        ctx.strokeStyle = TINTA;
                        ctx.lineWidth = 1.3;
                        ctx.beginPath();
                        ctx.arc(xs, ys, 8, 0, TAU);
                        ctx.fill();
                        ctx.stroke();
                        ctx.strokeStyle = "#71717a";
                        ctx.lineWidth = 1;
                        rueda(ctx, xs, ys, 5.5, avance * 0.12 * lado, 6);
                    });
                });
            });

            // Toberas de aire caliente sobre la tela
            horizontal(ctx, 15, function () {
                ctx.lineWidth = 2.2;
                for (i = 0; i < NMOD * 5; i++) {
                    var xt = H0 + 9 + i * (MOD / 5);
                    var pulso = 0.5 + 0.5 * Math.sin(reloj * 2.6 - i * 0.55);
                    ctx.strokeStyle = "rgba(232, 89, 12, " + (0.2 + 0.5 * pulso).toFixed(3) + ")";
                    ctx.beginPath();
                    ctx.moveTo(xt, -M1 + 4);
                    ctx.lineTo(xt, M1 - 4);
                    ctx.stroke();
                }
            });

            // Cámaras: cristal al frente, a la salida y arriba
            ctx.strokeStyle = "rgba(9, 9, 11, .6)";
            ctx.lineWidth = 1.2;
            cara(ctx, [[H0, YB, ZB], [H1, YB, ZB], [H1, YB, ZT], [H0, YB, ZT]]);
            ctx.fillStyle = "rgba(255, 255, 255, .66)";
            ctx.fill();
            ctx.stroke();
            cara(ctx, [[H1, -YB, ZB], [H1, YB, ZB], [H1, YB, ZT], [H1, -YB, ZT]]);
            ctx.fillStyle = "rgba(255, 255, 255, .5)";
            ctx.fill();
            ctx.stroke();
            // boca de salida
            ctx.strokeStyle = "rgba(9, 9, 11, .5)";
            ctx.lineWidth = 1;
            cara(ctx, [[H1, -M1 - 8, -4], [H1, M1 + 8, -4], [H1, M1 + 8, 6], [H1, -M1 - 8, 6]]);
            ctx.stroke();
            lateral(ctx, YB, function () {
                for (i = 0; i < NMOD; i++) {
                    var xm = H0 + i * MOD;
                    if (i) {
                        ctx.strokeStyle = "rgba(9, 9, 11, .35)";
                        ctx.lineWidth = 1;
                        ctx.beginPath();
                        ctx.moveTo(xm, ZT);
                        ctx.lineTo(xm, ZB);
                        ctx.stroke();
                    }
                    // ventana: se ve el calor adentro
                    var brillo = 0.5 + 0.5 * Math.sin(reloj * 1.7 - i * 1.1);
                    rectRedondo(ctx, xm + 11, ZT - 34, MOD - 22, 22, 4);
                    ctx.fillStyle = "rgba(232, 89, 12, " + (0.1 + 0.14 * brillo).toFixed(3) + ")";
                    ctx.fill();
                    ctx.strokeStyle = "rgba(9, 9, 11, .5)";
                    ctx.lineWidth = 1;
                    ctx.stroke();
                    ctx.strokeStyle = "rgba(9, 9, 11, .4)";
                    ctx.beginPath();
                    ctx.moveTo(xm + MOD / 2 - 7, ZT - 44);
                    ctx.lineTo(xm + MOD / 2 + 7, ZT - 44);
                    ctx.moveTo(xm + (i ? 0 : 0), -22);
                    ctx.lineTo(xm + MOD, -22);
                    ctx.stroke();
                }
            });
            cara(ctx, [[H0, -YB, ZT], [H1, -YB, ZT], [H1, YB, ZT], [H0, YB, ZT]]);
            ctx.fillStyle = "rgba(255, 255, 255, .5)";
            ctx.fill();
            ctx.strokeStyle = "rgba(9, 9, 11, .6)";
            ctx.lineWidth = 1.2;
            ctx.stroke();
            horizontal(ctx, ZT, function () {
                for (i = 0; i < NMOD; i++) {
                    var xc = H0 + (i + 0.5) * MOD;
                    if (i) {
                        ctx.strokeStyle = "rgba(9, 9, 11, .3)";
                        ctx.lineWidth = 1;
                        ctx.beginPath();
                        ctx.moveTo(H0 + i * MOD, -YB);
                        ctx.lineTo(H0 + i * MOD, YB);
                        ctx.stroke();
                    }
                    // ventilador de circulación
                    ctx.fillStyle = "rgba(255, 255, 255, .85)";
                    ctx.strokeStyle = TINTA;
                    ctx.lineWidth = 1.3;
                    ctx.beginPath();
                    ctx.arc(xc, 26, 21, 0, TAU);
                    ctx.fill();
                    ctx.stroke();
                    ctx.strokeStyle = "rgba(9, 9, 11, .22)";
                    ctx.lineWidth = 1;
                    ctx.beginPath();
                    ctx.arc(xc, 26, 15, 0, TAU);
                    ctx.stroke();
                    ctx.strokeStyle = "#52525b";
                    ctx.lineWidth = 1.5;
                    rueda(ctx, xc, 26, 15, reloj * 5 * (i % 2 ? -1 : 1) + i, 5);
                    ctx.fillStyle = TINTA;
                    ctx.beginPath();
                    ctx.arc(xc, 26, 3, 0, TAU);
                    ctx.fill();
                    // base del escape
                    ctx.fillStyle = "#fff";
                    ctx.strokeStyle = TINTA;
                    ctx.lineWidth = 1.2;
                    ctx.beginPath();
                    ctx.arc(xc, -56, 10, 0, TAU);
                    ctx.fill();
                    ctx.stroke();
                }
            });
            // Escapes: chimenea corta y vapor de la tela que seca
            for (i = 0; i < NMOD; i++) {
                var e0 = P(H0 + (i + 0.5) * MOD, -56, ZT);
                var ancho = 10 * 1.0;
                ctx.fillStyle = "#fff";
                ctx.strokeStyle = TINTA;
                ctx.lineWidth = 1.2;
                ctx.beginPath();
                ctx.moveTo(e0[0] - ancho, e0[1]);
                ctx.lineTo(e0[0] - ancho, e0[1] - 16);
                ctx.lineTo(e0[0] + ancho, e0[1] - 16);
                ctx.lineTo(e0[0] + ancho, e0[1]);
                ctx.fill();
                ctx.stroke();
                ctx.beginPath();
                ctx.ellipse(e0[0], e0[1] - 16, ancho, 4.4, 0.12, 0, TAU);
                ctx.fill();
                ctx.stroke();
                var fuerza = 1 - i / NMOD * 0.7;      // seca más al principio
                ctx.lineWidth = 1.2;
                for (var v = -1; v <= 1; v++) {
                    for (var tz = 0; tz < 10; tz++) {
                        var k0 = tz / 10, k1 = (tz + 1) / 10;
                        var fase = reloj * 3.2 + v * 1.9 + i * 0.8;
                        ctx.strokeStyle = "rgba(113, 113, 122, " + (0.75 * fuerza * (1 - k0) * (0.35 + 0.65 * k0 * 3 > 1 ? 1 : 0.35 + 0.65 * k0 * 3)).toFixed(3) + ")";
                        ctx.beginPath();
                        ctx.moveTo(e0[0] + v * 5 + Math.sin(k0 * 7 - fase) * 2.6 * k0 * 2, e0[1] - 21 - k0 * 34);
                        ctx.lineTo(e0[0] + v * 5 + Math.sin(k1 * 7 - fase) * 2.6 * k1 * 2, e0[1] - 21 - k1 * 34);
                        ctx.stroke();
                    }
                }
            }

            // Rollo terminado
            // al llegar la unión entre lotes se cambia de rollo
            var tRollo = costura > X_ROLLO
                ? mezclaHex(colorEn(costura + 1), colorEn(X_ROLLO), suave((costura - X_ROLLO) / 60))
                : mezclaHex(colorEn(X_ROLLO), colorEn(X_ROLLO), 0);
            var seco = "rgb(" + tRollo.slice(4, -1).split(",").map(function (v) { return Math.round(+v + (255 - +v) * 0.82); }).join(",") + ")";
            ctx.strokeStyle = "#a1a1aa";
            ctx.lineWidth = 1.2;
            ctx.beginPath();
            [-1, 1].forEach(function (lado) {
                var yy2 = lado * (M1 + 7);
                linea(ctx, [X_ROLLO, yy2, -R_ROLLO], [X_ROLLO - 13, yy2, ZPISO]);
                linea(ctx, [X_ROLLO, yy2, -R_ROLLO], [X_ROLLO + 13, yy2, ZPISO]);
            });
            ctx.stroke();
            ctx.lineWidth = 1.2;
            var rollo = cilindro(ctx, X_ROLLO, -R_ROLLO, R_ROLLO, -M1, M1, seco, "rgba(9, 9, 11, .7)");
            // pasadas sobre el rollo: giran con él
            ctx.strokeStyle = conAlfa(tRollo, 0.42);
            ctx.lineWidth = 0.8;
            ctx.beginPath();
            var desde = rollo.amin, hasta = rollo.amax;
            if (hasta < desde) { hasta += TAU; }
            if (!(desde <= TAU && TAU <= hasta) && !(desde <= 0 && 0 <= hasta)) { var tmp = desde; desde = hasta; hasta = tmp + TAU; }
            var pasoA = 9 / R_ROLLO;
            for (var a = desde + ((avance / R_ROLLO - desde) % pasoA + pasoA) % pasoA; a < hasta; a += pasoA) {
                var r0 = rollo.borda(-M1, a), r1 = rollo.borda(M1, a);
                ctx.moveTo(r0[0], r0[1]);
                ctx.lineTo(r1[0], r1[1]);
            }
            ctx.stroke();
            // cara del rollo: capas y mandril
            var eje = P(X_ROLLO, M1, -R_ROLLO);
            ctx.save();
            ctx.transform(AXX, AXY, 0, -1, eje[0], eje[1]);
            ctx.strokeStyle = conAlfa(tRollo, 0.45);
            ctx.lineWidth = 0.8;
            [15.5, 10.5].forEach(function (rr) {
                ctx.beginPath();
                ctx.arc(0, 0, rr, 0, TAU);
                ctx.stroke();
            });
            ctx.fillStyle = "#fff";
            ctx.strokeStyle = TINTA;
            ctx.lineWidth = 1.2;
            ctx.beginPath();
            ctx.arc(0, 0, 5.5, 0, TAU);
            ctx.fill();
            ctx.stroke();
            ctx.strokeStyle = "#71717a";
            ctx.lineWidth = 1;
            rueda(ctx, 0, 0, 5.5, -avance / R_ROLLO, 3);
            ctx.restore();

            // Cotas de ancho: angosta al entrar, al ancho pedido al salir
            horizontal(ctx, 1, function () {
                [[H1 + 40, M1, ACENTO]].forEach(function (ct) {
                    var cx = ct[0], m = ct[1] - 4;
                    ctx.strokeStyle = ct[2];
                    ctx.fillStyle = ct[2];
                    ctx.lineWidth = 1.1;
                    ctx.beginPath();
                    ctx.moveTo(cx, -m + 4);
                    ctx.lineTo(cx, m - 4);
                    ctx.stroke();
                    [-1, 1].forEach(function (sg) {
                        ctx.beginPath();
                        ctx.moveTo(cx, sg * m);
                        ctx.lineTo(cx - 3.4, sg * (m - 8));
                        ctx.lineTo(cx + 3.4, sg * (m - 8));
                        ctx.closePath();
                        ctx.fill();
                    });
                });
            });
            ctx.restore();
        });
    }

    /* Medición: avisa a Google Analytics, si el sitio lo tiene cargado, de las
       acciones que importan (oportunidad enviada, llamada, descarga). Sin
       Analytics no hace nada. */
    function evento(nombre, datos) {
        if (typeof window.gtag === "function") {
            try { window.gtag("event", nombre, datos || {}); } catch (e) { /* sin medición */ }
        }
    }
    function medicion() {
        var conversion = document.querySelector("[data-qb-conversion]");
        if (conversion) {
            evento("generate_lead", { origen: conversion.getAttribute("data-qb-conversion"), idioma: raiz.lang || "" });
        }
        document.addEventListener("click", function (e) {
            var el = e.target && e.target.closest ? e.target.closest("a, button") : null;
            if (!el) {
                return;
            }
            var href = el.getAttribute("href") || "";
            if (el.hasAttribute("data-qb-evento")) {
                evento(el.getAttribute("data-qb-evento"), { detalle: el.getAttribute("data-qb-detalle") || "" });
            } else if (href.indexOf("tel:") === 0) {
                evento("clic_telefono", {});
            } else if (href.indexOf("mailto:") === 0) {
                evento("clic_correo", {});
            } else if (/\/agendar$/.test(href)) {
                evento("clic_agendar", {});
            }
        });
    }

    listo(function () {
        if (!raiz.hasAttribute("data-qb")) {
            return;
        }
        todos('canvas[data-qb-anim="circular"]').forEach(maquinaCircular);
        todos('canvas[data-qb-anim="fibras"]').forEach(fibras);
        todos('canvas[data-qb-anim="jet"]').forEach(jet);
        todos('canvas[data-qb-anim="rama"]').forEach(rama);
        todos('canvas[data-qb-anim="tela"]').forEach(tela);
        disenador();
        aparicion();
        cuentas();
        desplazamiento();
        foco();
        menuMovil();
        buscador();
        filtroCatalogo();
        medicion();
    });
})();
