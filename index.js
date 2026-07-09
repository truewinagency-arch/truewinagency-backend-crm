const express = require('express');
const cors = require('cors');
const pino = require('pino');
const { default: makeWASocket, DisconnectReason, initAuthCreds, BufferJSON } = require('@whiskeysockets/baileys');
const { createServer } = require('http');
const { Server } = require('socket.io');

const { initializeApp, cert } = require('firebase-admin/app');
const { getFirestore } = require('firebase-admin/firestore'); 
const serviceAccount = require('./firebase-credentials.json');

// =========================================================================
// 0. DETECTORES DE ERRORES CRÍTICOS (Anti-colapsos silenciosos)
// =========================================================================
process.on('uncaughtException', (err) => console.error('\n[NODE FATAL] Excepción no capturada:', err));
process.on('unhandledRejection', (reason, promise) => console.error('\n[NODE FATAL] Promesa rechazada no manejada:', reason));

// =========================================================================
// 1. INICIALIZACIÓN DE FIREBASE CON CONFIGURACIÓN DE TOLERANCIA
// =========================================================================
initializeApp({
    credential: cert(serviceAccount)
});

const db = getFirestore();
db.settings({ ignoreUndefinedProperties: true });
const coleccionSesion = db.collection('whatsapp_session');

// 🚀 NUEVA COLECCIÓN: Cerebro de mensajes
const coleccionMensajes = db.collection('mensajes_crm');

// 🚀 NUEVO: Detector del número conectado actualmente al servidor
function getHostNumber() {
    if (whatsappSock && whatsappSock.user && whatsappSock.user.id) {
        // Baileys guarda el número así: "584121234567:1@s.whatsapp.net". Esto lo limpia.
        return whatsappSock.user.id.split(':')[0].split('@')[0]; 
    }
    return 'desconectado';
}

// 🚀 FUNCIÓN MAESTRA: Guarda cada disparo en la base de datos
async function guardarMensajeBD(numero, nombre, texto, tipo) {
    try {
        const hostActual = getHostNumber(); // Sabemos de quién es este mensaje
        
        // Si no hay host, no guardamos para evitar datos corruptos
        if (hostActual === 'desconectado') return; 

        await coleccionMensajes.add({
            host: hostActual, // <-- EL CANDADO DE AISLAMIENTO
            numero: numero,
            nombre: nombre || "Desconocido",
            texto: texto,
            tipo: tipo,
            hora: new Date().toISOString(),
            timestamp: Date.now() 
        });
    } catch (error) {
        console.error("Error guardando en historial:", error);
    }
}
// =========================================================================
// 2. CONFIGURACIÓN DEL SERVIDOR HTTP Y WEBSOCKETS
// =========================================================================
const app = express();
const PORT = process.env.PORT || 3000; 

const httpServer = createServer(app);
const io = new Server(httpServer, {
    cors: {
        origin: "*",
        methods: ["GET", "POST"]
    }
});

app.use(cors({
    origin: '*',
    methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
    allowedHeaders: ['Content-Type', 'Authorization', 'Bypass-Tunnel-Reminder', 'bypass-tunnel-reminder']
}));

app.use((req, res, next) => {
    res.header('Access-Control-Allow-Origin', '*');
    res.header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS, PUT, DELETE');
    res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept, Authorization, Bypass-Tunnel-Reminder, bypass-tunnel-reminder');
    if (req.method === 'OPTIONS') return res.sendStatus(200);
    next();
});

app.use(express.json());

let whatsappSock = null;
let ultimoQR = null;

// =========================================================================
// 3. CONEXIÓN A WHATSAPP CON CACHÉ EN RAM + LOTES EN FIRESTORE
// =========================================================================
async function connectToWhatsApp() {
    console.log("[TrueWin-Backend] Sincronizando e inicializando sesión remota...");

    let cacheCreds = {};
    let cacheKeys = {};
    let cacheCargada = false;

    const readState = async () => {
        if (cacheCargada) {
            return { creds: cacheCreds, keys: cacheKeys, tieneDatos: true };
        }
        try {
            console.log("[TrueWin-Optimizado] Descargando llaves desde Firestore por primera vez...");
            const snapshot = await coleccionSesion.get();
            let tieneDatos = false; 
            
            snapshot.forEach(doc => {
                tieneDatos = true;
                const parsedData = JSON.parse(doc.data().payload, BufferJSON.reviver);
                if (doc.id === 'creds') cacheCreds = parsedData;
                else cacheKeys[doc.id] = parsedData;
            });

            if (tieneDatos) cacheCargada = true;
            return { creds: cacheCreds, keys: cacheKeys, tieneDatos };
        } catch (e) { 
            return { creds: {}, keys: {}, tieneDatos: false }; 
        }
    };

    const writeState = async (data, id) => {
        try {
            if (id === 'creds') cacheCreds = data;
            else cacheKeys[id] = data;

            const stringifiedData = JSON.stringify(data, BufferJSON.replacer);
            await coleccionSesion.doc(id).set({ payload: stringifiedData });
        } catch (e) { 
            console.error("Error al escribir datos de sesión en la nube:", e); 
        }
    };

    const sesionFirebase = await readState();

    let credencialesActivas = sesionFirebase.creds;
    if (!sesionFirebase.tieneDatos || Object.keys(credencialesActivas).length === 0) {
        console.log('[TrueWin] Base de datos limpia. Generando credenciales oficiales para pedir QR...');
        credencialesActivas = initAuthCreds();
        cacheCreds = credencialesActivas;
    }

    const { state, saveCreds } = {
        state: {
            creds: cacheCreds,
            keys: {
                get: async (type, ids) => {
                    const data = {};
                    for (const id of ids) {
                        const docId = `${type}-${id}`;
                        data[id] = cacheKeys[docId];
                    }
                    return data;
                },
                set: async (data) => {
                    const batch = db.batch(); 
                    let contador = 0;

                    for (const type in data) {
                        for (const id in data[type]) {
                            const value = data[type][id];
                            const docId = `${type}-${id}`;
                            const docRef = coleccionSesion.doc(docId);
                            
                            if (value) {
                                cacheKeys[docId] = value;
                                const stringifiedData = JSON.stringify(value, BufferJSON.replacer);
                                batch.set(docRef, { payload: stringifiedData });
                            } else {
                                delete cacheKeys[docId];
                                batch.delete(docRef);
                            }
                            contador++;
                        }
                    }
                    
                    if (contador > 0) {
                        await batch.commit().catch(e => console.error("Error en Lote de Firebase:", e));
                    }
                }
            }
        },
        saveCreds: async () => {
            await writeState(state.creds, 'creds');
        }
    };

    whatsappSock = makeWASocket({
        auth: state,
        printQRInTerminal: false,
        // 🚀 CAMUFLAJE 1: Nos hacemos pasar por una PC normal usando Chrome o Edge
        browser: ['Windows', 'Chrome', '111.0.0.0'], 
        logger: pino({ level: 'error' })
    });

    whatsappSock.ev.on('messages.upsert', async (m) => {
        const msg = m.messages[0];
        if (!msg.message || msg.key.fromMe || msg.key.remoteJid === 'status@broadcast') return;
        
        const remoteJid = msg.key.remoteJid;
        const esGrupo = remoteJid.endsWith('@g.us');
        
        // 🚀 MAGIA PARA GRUPOS: Obtenemos el título del grupo
        let nombrePerfil = msg.pushName || "Usuario"; 
        if (esGrupo) {
            try {
                const metadata = await whatsappSock.groupMetadata(remoteJid);
                nombrePerfil = metadata.subject || "Grupo de WhatsApp";
            } catch (error) {
                // Falla silenciosa si no logramos descargar el nombre a tiempo
            }
        }
        
        // Mantenemos las etiquetas @lid y @g.us para que el sistema las reconozca luego
        const identificador = remoteJid.replace('@s.whatsapp.net', ''); 

        const texto = msg.message.conversation || msg.message.extendedTextMessage?.text || "[Multimedia/Sticker recibido]";
        
        await guardarMensajeBD(identificador, nombrePerfil, texto, 'in');

        io.emit('nuevo-mensaje', { 
            numero: identificador, 
            nombre: nombrePerfil, 
            texto: texto, 
            hora: new Date().toISOString() 
        });
    });

    whatsappSock.ev.on('connection.update', async (update) => {
        const { connection, lastDisconnect, qr } = update;

        if (qr) {
            ultimoQR = qr;
            io.emit('qr-update', qr);
            console.log('[TrueWin] 📲 Código QR generado con éxito. Listo para escanear en el CRM.');
        }

        if (connection === 'close') {
            const statusCode = lastDisconnect?.error?.output?.statusCode;
            const shouldReconnect = statusCode !== DisconnectReason.loggedOut;
            console.log(`[TrueWin] Conexión cerrada (Código: ${statusCode}). ¿Reconectando?: ${shouldReconnect}`);
            io.emit('estado-conexion', 'desconectado');

            if (whatsappSock) {
                try { whatsappSock.ev.removeAllListeners(); } catch (e) {}
                whatsappSock = null;
            }

            if (shouldReconnect) {
                console.log('[TrueWin] Reiniciando flujo de socket de forma limpia en 3 segundos...');
                setTimeout(() => { connectToWhatsApp(); }, 3000);
            } else {
                console.log('[TrueWin] Sesión desvinculada voluntariamente.');
                ultimoQR = null;
            }
        } else if (connection === 'open') {
            console.log('[TrueWin] ¡CONEXIÓN GLOBAL CONFIGURADA EN LA NUBE CON FIRESTORE!');
            ultimoQR = null;
            io.emit('estado-conexion', 'conectado');
        }
    });

    whatsappSock.ev.on('creds.update', saveCreds);
}

io.on('connection', (socket) => {
    console.log('[Socket.IO] ¡Nueva pestaña del CRM sincronizada al túnel en tiempo real!');
    
    // 1. Mantenemos el reporte de estado automático al conectar la pestaña
    if (whatsappSock && whatsappSock.user) {
        socket.emit('estado-conexion', 'conectado');
    } else if (ultimoQR) {
        socket.emit('estado-conexion', 'desconectado');
        socket.emit('qr-update', ultimoQR);
    } else {
        socket.emit('estado-conexion', 'desconectado');
    }

    // 🚀 NUEVO: Escucha el tecleo manual en tiempo real desde el CRM y lo inyecta a WhatsApp
    socket.on('crm-presencia', async ({ numero, estado }) => {
        if (!whatsappSock) return;
        try {
            const jid = formatearJid(numero); // 🚀 Usamos el enrutador
            await whatsappSock.sendPresenceUpdate(estado, jid);
        } catch (e) {}
    });

// En tu Endpoint de Texto:
app.post('/send-text', async (req, res) => {
    const { numero, mensaje } = req.body;
    if (!whatsappSock) return res.status(500).json({ error: "WhatsApp no inicializado." });
    try {
        const jid = formatearJid(numero); // 🚀 Usamos el enrutador
        
        await whatsappSock.sendPresenceUpdate('composing', jid);
        await delay(Math.floor(Math.random() * 2000) + 2500); 
        
        await whatsappSock.sendMessage(jid, { text: mensaje });
        await whatsappSock.sendPresenceUpdate('paused', jid);

        await guardarMensajeBD(numero, "TrueWin", mensaje, 'out'); 
        res.json({ success: true });
    } catch (error) {
        console.error(`[Error] Fallo enviando texto a ${numero}:`, error);
        res.status(500).json({ error: error.message });
    }
});

// 🚀 NUEVO ENRUTADOR INTELIGENTE: Detecta Grupos, LIDs y Personas
function formatearJid(numero) {
    const numStr = numero.toString();
    if (numStr.includes('@g.us')) return numStr; // Es grupo (conserva sus guiones)
    if (numStr.includes('@lid')) return numStr;  // Es identidad oculta
    return `${numStr.replace(/[^0-9]/g, '')}@s.whatsapp.net`; // Es persona normal
}

// =========================================================================
// 4. ENDPOINTS DE CONTROL (ACTUALIZADOS PARA SOPORTAR @lid Y @s.whatsapp.net)
// =========================================================================
app.get('/status', (req, res) => {
    if (whatsappSock && whatsappSock.user) {
        return res.json({ status: "connected", user: whatsappSock.user });
    }
    res.json({ status: "disconnected", qr: ultimoQR });
});




const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms));

app.post('/send-text', async (req, res) => {
    const { numero, mensaje } = req.body;
    if (!whatsappSock) return res.status(500).json({ error: "WhatsApp no inicializado." });
    try {
        const esLid = numero.toString().includes('lid');
        const numeroLimpio = numero.toString().replace(/[^0-9]/g, '');
        const jid = esLid ? `${numeroLimpio}@lid` : `${numeroLimpio}@s.whatsapp.net`;
        
        // 🚀 CAMUFLAJE 1: Mostramos "Escribiendo..." en el teléfono del cliente
        await whatsappSock.sendPresenceUpdate('composing', jid);
        
        // Generamos un tiempo de espera aleatorio entre 2.5 y 4.5 segundos simulando el tipeo humano
        await delay(Math.floor(Math.random() * 2000) + 2500); 
        
        // Despachamos el mensaje y pausamos el estado de presencia
        await whatsappSock.sendMessage(jid, { text: mensaje });
        await whatsappSock.sendPresenceUpdate('paused', jid);

        // Guardamos de forma segura en tu base de datos mensajes_crm
        await guardarMensajeBD(numero, "TrueWin", mensaje, 'out'); 
        res.json({ success: true });
    } catch (error) {
        console.error(`[Error] Fallo enviando texto a ${numero}:`, error);
        res.status(500).json({ error: error.message });
    }
});

app.post('/send-image', async (req, res) => {
    const { numero, urlImagen, caption } = req.body;
    if (!whatsappSock) return res.status(500).json({ error: "WhatsApp no inicializado." });
    try {
        const esLid = numero.toString().includes('lid');
        const numeroLimpio = numero.toString().replace(/[^0-9]/g, '');
        const jid = esLid ? `${numeroLimpio}@lid` : `${numeroLimpio}@s.whatsapp.net`;
        
        // 🚀 CAMUFLAJE 2: Simulamos que estamos adjuntando un archivo imitando el retraso de subida
        await whatsappSock.sendPresenceUpdate('composing', jid);
        await delay(Math.floor(Math.random() * 1500) + 2000); 
        
        await whatsappSock.sendMessage(jid, { image: { url: urlImagen }, caption: caption });
        await whatsappSock.sendPresenceUpdate('paused', jid);

        // Corregido: Guardamos la etiqueta del archivo multimedia enviado
        await guardarMensajeBD(numero, "TrueWin", caption || "[Imagen enviada]", 'out'); 

        res.json({ success: true });
    } catch (error) {
        console.error(`[Error] Fallo enviando imagen a ${numero}:`, error);
        res.status(500).json({ error: error.message });
    }
});

app.post('/send-audio', async (req, res) => {
    const { numero, urlAudio } = req.body;
    if (!whatsappSock) return res.status(500).json({ error: "WhatsApp no inicializado." });
    try {
        const esLid = numero.toString().includes('lid');
        const numeroLimpio = numero.toString().replace(/[^0-9]/g, '');
        const jid = esLid ? `${numeroLimpio}@lid` : `${numeroLimpio}@s.whatsapp.net`;
        
        // 🚀 CAMUFLAJE 3: Mostramos de forma legítima "Grabando audio..." antes de soltar el PTT
        await whatsappSock.sendPresenceUpdate('recording', jid);
        
        // Hacemos que simule estar grabando durante 4 segundos enteros
        await delay(4000); 
        
        await whatsappSock.sendMessage(jid, { audio: { url: urlAudio }, mimetype: 'audio/mp4', ptt: true });
        await whatsappSock.sendPresenceUpdate('paused', jid);

        // Corregido: Guardamos el registro en Firebase
        await guardarMensajeBD(numero, "TrueWin", "[Nota de voz enviada]", 'out'); 
        res.json({ success: true });
    } catch (error) {
        console.error(`[Error] Fallo enviando audio a ${numero}:`, error);
        res.status(500).json({ error: error.message });
    }
});
app.get('/api/historial', async (req, res) => {
    try {
        const hostActivo = getHostNumber();
        
        // Si no has escaneado el QR, devolvemos un historial vacío
        if (hostActivo === 'desconectado') {
            return res.json({ historial: {}, nombres: {} }); 
        }

        // 1. Buscamos SOLO los mensajes que le pertenecen a TU número conectado
        const snapshot = await coleccionMensajes.where('host', '==', hostActivo).get();
        
        let todosLosMensajes = [];
        snapshot.forEach(doc => todosLosMensajes.push(doc.data()));
        
        // 2. Ordenamos por fecha en la memoria del servidor
        todosLosMensajes.sort((a, b) => a.timestamp - b.timestamp);

        const historial = {};
        const nombres = {};
        
        // 3. Empaquetamos la respuesta
        todosLosMensajes.forEach(data => {
            if (!historial[data.numero]) historial[data.numero] = [];
            historial[data.numero].push({ tipo: data.tipo, texto: data.texto, hora: data.hora });

            if (data.tipo === 'in' && data.nombre) nombres[data.numero] = data.nombre;
        });
        
        res.json({ historial, nombres });
    } catch (error) {
        console.error("Error obteniendo historial:", error);
        res.status(500).json({ error: "Fallo al obtener historial" });
    }
});

// =========================================================================
// 5. ARRANQUE SEGURO EN ORDEN
// =========================================================================
async function iniciarEcosistema() {
    try {
        await connectToWhatsApp();
        httpServer.listen(PORT, () => {
            console.log(`[TrueWin-Web] 🚀 API y WebSockets listos y escuchando en el puerto ${PORT}`);
        });
    } catch (error) {
        console.error("[Crítico] Fallo al iniciar el ecosistema de TrueWin:", error);
    }
}

iniciarEcosistema();