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
            var amp = Math.min(margen - 2, TELA_ONDA[diseno.tacto] === undefined ? 6 : TELA_ONDA[diseno.tacto]);
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
                acabados.push("Fusionable");
            }
            if (campo("qb_termofijado").checked) {
                acabados.push("Termofijada");
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
                colorNombre: colorLibre || !col ? "Personalizado " + libre.value.toUpperCase() : col.getAttribute("data-nombre"),
                tacto: (elegido("qb_tacto") || {}).value || "Suave",
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
                f.push(["Mercado", d.mercado]);
            }
            if (d.aplicacion) {
                f.push(["Aplicación", d.aplicacion]);
            }
            f.push(["Construcción", d.construccionNombre]);
            if (!punto) {
                f.push(["Fibra", d.fibra]);
            } else {
                var hilo = d.hilo;
                if (hilo.indexOf("Poliéster") === 0 && d.hiloTipo) {
                    hilo += " " + d.hiloTipo.toLowerCase();
                }
                f.push(["Hilo", hilo + (d.elastano ? " con elastano" : "")]);
            }
            f.push(["Peso", d.peso + " g/m²"]);
            if (d.ancho) {
                f.push(["Ancho", d.ancho + " cm"]);
            }
            f.push(["Color", d.colorNombre], ["Tacto", d.tacto]);
            if (d.acabados.length) {
                f.push(["Acabado", d.acabados.join(", ")]);
            }
            f.push(["Presentación", d.presentacion]);
            if (d.requisitos.length) {
                f.push(["Requisitos", d.requisitos.join(", ")]);
            }
            if (d.norma) {
                f.push(["Norma", d.norma]);
            }
            if (d.volumen) {
                f.push(["Volumen", d.volumen.toLocaleString("es-MX") + " " + d.unidad]);
            }
            if (d.fecha) {
                f.push(["Lo necesita para", d.fecha.split("-").reverse().join("/")]);
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
            var bloque = "Diseño de tela (diseñador en línea)\n" + lista.map(function (par) {
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
                var nuevoAsunto = "Diseño de tela: " + d.construccionNombre + ", " + d.peso + " g/m²";
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
            marcar("qb_tacto", tela.tacto || "Suave");
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
                    texto.textContent = "Enlace copiado";
                    setTimeout(function () {
                        texto.textContent = original;
                    }, 2200);
                };
                if (navigator.clipboard && navigator.clipboard.writeText) {
                    navigator.clipboard.writeText(location.href).then(listoCopia, function () {
                        window.prompt("Copie el enlace de su diseño:", location.href);
                    });
                } else {
                    window.prompt("Copie el enlace de su diseño:", location.href);
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
                lista.innerHTML = '<div class="qb-cmd__nada">Cargando catálogo…</div>';
                return;
            }
            if (!visibles.length) {
                var nada = document.createElement("div");
                nada.className = "qb-cmd__nada";
                nada.textContent = "Sin resultados. Fabricamos sobre especificación: pida una cotización.";
                lista.appendChild(nada);
                return;
            }
            var grupo = null;
            visibles.forEach(function (d, i) {
                var nombre = d.codigo ? "Productos" : d.categoria;
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
                fetch("/catalogo/buscar.json", { credentials: "same-origin" })
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
       Jet de teñido, visto de lado y en esquema.
       La tela va en cuerda: la tobera la sube con el propio baño, recorre el
       tubo de arriba y cae de nuevo al tanque, vuelta tras vuelta, hasta que
       el color queda parejo. El baño toma color poco a poco y cambia de
       tono en cada ciclo.
       ------------------------------------------------------------------ */
    function mezclaHex(a, b, k) {
        function canal(hex, i) { return parseInt(hex.substr(1 + i * 2, 2), 16); }
        var r = [0, 1, 2].map(function (i) { return Math.round(canal(a, i) + (canal(b, i) - canal(a, i)) * k); });
        return "rgb(" + r.join(",") + ")";
    }
    function acelera(canvas) {
        var v = { vel: 1, objetivo: 1 };
        canvas.parentElement.addEventListener("pointerenter", function () { v.objetivo = 2.4; });
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

    function jet(canvas) {
        var CICLO = 9;            // segundos por baño
        var ritmo = acelera(canvas);
        var reloj = 0;
        var avance = 0;
        var burbujas = [];
        for (var i = 0; i < 16; i++) {
            burbujas.push({ x: Math.random(), y: Math.random(), r: 0.5 + Math.random(), v: 0.4 + Math.random() * 0.6 });
        }
        lienzo(canvas, function (ctx, w, h, t, dt) {
            ritmo.vel += (ritmo.objetivo - ritmo.vel) * Math.min(1, dt * 2.5);
            reloj += dt * ritmo.vel;
            avance += dt * ritmo.vel * 70;
            var fase = (reloj % CICLO) / CICLO;
            var tinte = HILOS[Math.floor(reloj / CICLO) % HILOS.length];
            var k = Math.min(1, fase * 1.35);                 // qué tanto color tomó
            var cuerda = mezclaHex("#e4e4e7", tinte, k);
            ctx.clearRect(0, 0, w, h);

            var x0 = w * 0.10, x1 = w * 0.90;
            var yt = h * 0.46, yb = h * 0.86;                 // tanque
            var rt = (yb - yt) / 2;
            var ys = h * 0.17;                                // tubo de arriba
            var xs = x0 + rt * 0.9, xb = x1 - rt * 0.9;       // subida y bajada
            var nivel = yt + (yb - yt) * 0.42;

            // Baño dentro del tanque
            ctx.save();
            rectRedondo(ctx, x0, yt, x1 - x0, yb - yt, rt);
            ctx.clip();
            ctx.fillStyle = "#fafafa";
            ctx.fillRect(x0, yt, x1 - x0, yb - yt);
            ctx.fillStyle = tinte;
            ctx.globalAlpha = 0.10 + k * 0.20;
            ctx.beginPath();
            ctx.moveTo(x0, yb);
            ctx.lineTo(x0, nivel);
            for (var x = x0; x <= x1; x += 6) {
                ctx.lineTo(x, nivel + Math.sin(x * 0.05 + reloj * 2.2) * 2.2 + Math.sin(x * 0.021 - reloj * 1.3) * 1.6);
            }
            ctx.lineTo(x1, yb);
            ctx.closePath();
            ctx.fill();
            ctx.globalAlpha = 0.35 + k * 0.3;
            burbujas.forEach(function (b) {
                b.y -= dt * ritmo.vel * b.v * 0.22;
                if (b.y < 0) { b.y = 1; b.x = Math.random(); }
                var by = nivel + (yb - nivel) * b.y;
                ctx.beginPath();
                ctx.arc(x0 + (x1 - x0) * b.x, by, b.r * 1.6, 0, TAU);
                ctx.fill();
            });
            ctx.restore();
            ctx.globalAlpha = 1;

            // Circuito del baño: del fondo del tanque a la bomba y a la tobera
            var bx = w * 0.07, bym = h * 0.60;
            ctx.strokeStyle = tinte;
            ctx.globalAlpha = 0.55;
            ctx.lineWidth = 1.4;
            ctx.setLineDash([4, 5]);
            ctx.lineDashOffset = -avance * 0.6;
            ctx.beginPath();
            ctx.moveTo(x0 + rt, yb);
            ctx.lineTo(x0 + rt, h * 0.94);
            ctx.lineTo(bx, h * 0.94);
            ctx.lineTo(bx, ys);
            ctx.lineTo(xs - w * 0.02, ys);
            ctx.stroke();
            ctx.setLineDash([]);
            ctx.globalAlpha = 1;
            ctx.fillStyle = "#fff";
            ctx.strokeStyle = "#18181b";
            ctx.lineWidth = 1.3;
            ctx.beginPath();
            ctx.arc(bx, bym, h * 0.045, 0, TAU);
            ctx.fill();
            ctx.stroke();
            ctx.beginPath();
            for (var a = 0; a < 3; a++) {
                var an = reloj * 5 + a * TAU / 3;
                ctx.moveTo(bx, bym);
                ctx.lineTo(bx + Math.cos(an) * h * 0.034, bym + Math.sin(an) * h * 0.034);
            }
            ctx.stroke();

            // Tubo de arriba y tanque, en contorno
            ctx.strokeStyle = "#d4d4d8";
            ctx.lineWidth = h * 0.085;
            ctx.lineCap = "round";
            ctx.lineJoin = "round";
            ctx.beginPath();
            ctx.moveTo(xs, yt + rt * 0.5);
            ctx.lineTo(xs, ys + h * 0.07);
            ctx.arcTo(xs, ys, xs + h * 0.07, ys, h * 0.07);
            ctx.lineTo(xb - h * 0.07, ys);
            ctx.arcTo(xb, ys, xb, ys + h * 0.07, h * 0.07);
            ctx.lineTo(xb, yt + rt * 0.5);
            ctx.stroke();
            ctx.strokeStyle = "#fff";
            ctx.lineWidth = h * 0.085 - 2.4;
            ctx.stroke();
            ctx.strokeStyle = "#18181b";
            ctx.lineWidth = 1.6;
            rectRedondo(ctx, x0, yt, x1 - x0, yb - yt, rt);
            ctx.stroke();

            // La tela en cuerda: sube, cruza y cae plegada al baño
            function ruta() {
                ctx.beginPath();
                ctx.moveTo(xs, nivel + rt * 0.45);
                ctx.lineTo(xs, ys + h * 0.07);
                ctx.arcTo(xs, ys, xs + h * 0.07, ys, h * 0.07);
                ctx.lineTo(xb - h * 0.07, ys);
                ctx.arcTo(xb, ys, xb, ys + h * 0.07, h * 0.07);
                ctx.lineTo(xb, nivel + rt * 0.2);
                var largo = xb - xs;
                var ondas = 9;
                for (var q = 1; q <= 60; q++) {
                    var u = q / 60;
                    ctx.lineTo(xb - largo * u,
                        nivel + rt * 0.45 + Math.sin(u * ondas * TAU + reloj * 1.5) * rt * 0.26 * Math.sin(u * Math.PI));
                }
            }
            ctx.strokeStyle = cuerda;
            ctx.lineWidth = Math.max(4, h * 0.035);
            ruta();
            ctx.stroke();
            ctx.strokeStyle = "rgba(24,24,27,.55)";
            ctx.lineWidth = 1.1;
            ctx.setLineDash([2, 7]);
            ctx.lineDashOffset = -avance;
            ruta();
            ctx.stroke();
            ctx.setLineDash([]);

            // Tobera: el chorro que empuja la tela
            var tx = xs + h * 0.07 + w * 0.05;
            ctx.fillStyle = "#18181b";
            ctx.beginPath();
            ctx.moveTo(tx - w * 0.03, ys - h * 0.07);
            ctx.lineTo(tx + w * 0.03, ys - h * 0.05);
            ctx.lineTo(tx + w * 0.03, ys + h * 0.05);
            ctx.lineTo(tx - w * 0.03, ys + h * 0.07);
            ctx.closePath();
            ctx.fill();
            ctx.strokeStyle = tinte;
            ctx.lineWidth = 1.2;
            for (var c = 0; c < 4; c++) {
                var d = ((reloj * 1.8 + c / 4) % 1);
                ctx.globalAlpha = (1 - d) * 0.8;
                ctx.beginPath();
                ctx.moveTo(tx + w * 0.035 + d * w * 0.07, ys - h * 0.022 + (c % 2) * h * 0.044);
                ctx.lineTo(tx + w * 0.05 + d * w * 0.07, ys - h * 0.022 + (c % 2) * h * 0.044);
                ctx.stroke();
            }
            ctx.globalAlpha = 1;

            // Avance del teñido
            var px = x0 + rt, pw = (x1 - x0) - rt * 2;
            ctx.fillStyle = "#e4e4e7";
            rectRedondo(ctx, px, h * 0.925, pw, 3, 1.5);
            ctx.fill();
            ctx.fillStyle = tinte;
            rectRedondo(ctx, px, h * 0.925, Math.max(3, pw * fase), 3, 1.5);
            ctx.fill();
        });
    }

    /* ---------------------------------------------------------------------
       Rama, vista desde arriba.
       La tela entra húmeda y angosta, las cadenas la toman de las orillas y
       la abren al ancho; cruza las cámaras de aire caliente, donde se seca y
       se termofija, y sale enrollada.
       ------------------------------------------------------------------ */
    function rama(canvas) {
        var ritmo = acelera(canvas);
        var avance = 0;
        var reloj = 0;
        lienzo(canvas, function (ctx, w, h, t, dt) {
            ritmo.vel += (ritmo.objetivo - ritmo.vel) * Math.min(1, dt * 2.5);
            reloj += dt * ritmo.vel;
            avance += dt * ritmo.vel * 46;
            ctx.clearRect(0, 0, w, h);
            var cy = h * 0.5;
            var xe = w * 0.03, xa = w * 0.24, xf = w * 0.86;   // entrada, ya abierta, salida
            var m0 = h * 0.15, m1 = h * 0.30;                   // medio ancho al entrar y abierta
            function medio(x) {
                if (x >= xa) { return m1; }
                var u = Math.max(0, (x - xe) / (xa - xe));
                u = u * u * (3 - 2 * u);
                return m0 + (m1 - m0) * u;
            }

            // Cámaras de aire caliente
            var CAM = 3;
            var c0 = w * 0.29, c1 = w * 0.82, hueco = w * 0.012;
            var anchoCam = (c1 - c0 - hueco * (CAM - 1)) / CAM;
            for (var c = 0; c < CAM; c++) {
                var x = c0 + c * (anchoCam + hueco);
                ctx.fillStyle = "rgba(232,89,12,.06)";
                ctx.strokeStyle = "#d4d4d8";
                ctx.lineWidth = 1;
                rectRedondo(ctx, x, cy - h * 0.40, anchoCam, h * 0.80, 10);
                ctx.fill();
                ctx.setLineDash([4, 4]);
                ctx.stroke();
                ctx.setLineDash([]);
            }

            // Tela: húmeda a la entrada, seca a la salida
            var g = ctx.createLinearGradient(xe, 0, xf, 0);
            g.addColorStop(0, "rgba(47,84,200,.42)");
            g.addColorStop(0.3, "rgba(47,84,200,.30)");
            g.addColorStop(1, "rgba(47,84,200,.10)");
            ctx.fillStyle = g;
            ctx.beginPath();
            ctx.moveTo(xe, cy - medio(xe));
            for (var x1 = xe; x1 <= xf; x1 += 4) { ctx.lineTo(x1, cy - medio(x1)); }
            ctx.lineTo(xf, cy - m1);
            ctx.lineTo(xf, cy + m1);
            for (var x2 = xf; x2 >= xe; x2 -= 4) { ctx.lineTo(x2, cy + medio(x2)); }
            ctx.closePath();
            ctx.fill();
            ctx.save();
            ctx.clip();
            // Columnas de malla (a lo largo) y pasadas (a lo ancho) que avanzan
            ctx.strokeStyle = "rgba(47,84,200,.28)";
            ctx.lineWidth = 1;
            ctx.beginPath();
            for (var col = -6; col <= 6; col++) {
                ctx.moveTo(xe, cy + col / 7 * m0);
                for (var x3 = xe; x3 <= xf; x3 += 6) { ctx.lineTo(x3, cy + col / 7 * medio(x3)); }
            }
            ctx.stroke();
            ctx.strokeStyle = "rgba(24,24,27,.16)";
            ctx.beginPath();
            var paso = 14;
            for (var x4 = xe - paso + (avance % paso); x4 <= xf; x4 += paso) {
                ctx.moveTo(x4, cy - m1);
                ctx.lineTo(x4, cy + m1);
            }
            ctx.stroke();
            ctx.restore();

            // Aire caliente dentro de cada cámara
            ctx.strokeStyle = "#e8590c";
            ctx.lineWidth = 1.3;
            ctx.lineCap = "round";
            for (var c2 = 0; c2 < CAM; c2++) {
                var xc = c0 + c2 * (anchoCam + hueco);
                for (var o = 0; o < 4; o++) {
                    var u2 = ((reloj * 0.5 + o / 4 + c2 * 0.13) % 1);
                    var ox = xc + anchoCam * (0.14 + o * 0.24);
                    for (var lado = -1; lado <= 1; lado += 2) {
                        var oy = cy + lado * (h * 0.37 - u2 * h * 0.12);
                        ctx.globalAlpha = Math.sin(u2 * Math.PI) * 0.75;
                        ctx.beginPath();
                        ctx.moveTo(ox - 5, oy);
                        ctx.quadraticCurveTo(ox, oy - lado * 5, ox + 5, oy);
                        ctx.stroke();
                    }
                }
            }
            ctx.globalAlpha = 1;

            // Cadenas: los pernos que llevan la tela por las orillas
            ctx.strokeStyle = "#18181b";
            ctx.lineWidth = 1.5;
            for (var s = -1; s <= 1; s += 2) {
                ctx.beginPath();
                ctx.moveTo(xe, cy + s * medio(xe));
                for (var x5 = xe; x5 <= xf; x5 += 4) { ctx.lineTo(x5, cy + s * medio(x5)); }
                ctx.stroke();
            }
            ctx.fillStyle = "#18181b";
            var sep = 16;
            for (var x6 = xe + (avance % sep); x6 <= xf; x6 += sep) {
                for (var s2 = -1; s2 <= 1; s2 += 2) {
                    ctx.beginPath();
                    ctx.arc(x6, cy + s2 * medio(x6), 2.1, 0, TAU);
                    ctx.fill();
                }
            }

            // Flechas del ensanche, a la entrada
            ctx.strokeStyle = "#0f9d7a";
            ctx.lineWidth = 1.4;
            var xm = (xe + xa) / 2, late = Math.sin(reloj * 2.4) * 3;
            for (var s3 = -1; s3 <= 1; s3 += 2) {
                var y0 = cy + s3 * (medio(xm) + 10 + late);
                var y1 = y0 + s3 * 14;
                ctx.beginPath();
                ctx.moveTo(xm, y0);
                ctx.lineTo(xm, y1);
                ctx.moveTo(xm - 4, y1 - s3 * 5);
                ctx.lineTo(xm, y1);
                ctx.lineTo(xm + 4, y1 - s3 * 5);
                ctx.stroke();
            }

            // Rollo a la salida
            var rw = w * 0.085;
            ctx.fillStyle = "#fff";
            ctx.strokeStyle = "#18181b";
            ctx.lineWidth = 1.6;
            rectRedondo(ctx, xf, cy - m1 - 4, rw, m1 * 2 + 8, rw * 0.45);
            ctx.fill();
            ctx.stroke();
            ctx.save();
            ctx.clip();
            ctx.strokeStyle = "rgba(47,84,200,.35)";
            ctx.lineWidth = 1;
            ctx.beginPath();
            var pr = 7;
            for (var x7 = xf - pr + (avance * 0.5 % pr); x7 <= xf + rw; x7 += pr) {
                ctx.moveTo(x7, cy - m1 - 4);
                ctx.lineTo(x7, cy + m1 + 4);
            }
            ctx.stroke();
            ctx.restore();
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
    });
})();
