const express = require('express');
const cors = require('cors');
const pino = require('pino');
const { default: makeWASocket, useMultiFileAuthState, DisconnectReason } = require('@whiskeysockets/baileys');
const { createServer } = require('http');
const { Server } = require('socket.io');

// =========================================================================
// 1. IMPORTACIÓN MODULAR COMPLETA (CORREGIDA Y SEGURA)
// =========================================================================
const { initializeApp, cert } = require('firebase-admin/app');
const { getFirestore } = require('firebase-admin/firestore'); // Importamos Firestore por separado
const serviceAccount = require('./firebase-credentials.json');

// Inicializar la App de Firebase de forma moderna
initializeApp({
    credential: cert(serviceAccount)
});

// Inicializar la base de datos usando getFirestore()
const db = getFirestore();
const coleccionSesion = db.collection('whatsapp_session');

// =========================================================================
// 2. CONFIGURACIÓN DEL SERVIDOR Y WEBSOCKETS
// =========================================================================
const app = express();
const PORT = process.env.PORT || 3000; // Ajustado a 3001 para evitar bloqueos locales

const httpServer = createServer(app);
const io = new Server(httpServer, {
    cors: {
        origin: "*",
        methods: ["GET", "POST"]
    }
});

// Middleware de CORS manual anti-bloqueos
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
// 3. CONEXIÓN A WHATSAPP CON PERSISTENCIA EN FIRESTORE (GRATUITO)
// =========================================================================
async function connectToWhatsApp() {
    console.log("[TrueWin-Backend] Conectando con Firebase Firestore para verificar sesión remota...");

    const readState = async () => {
        try {
            const snapshot = await coleccionSesion.get();
            let creds = {};
            let keys = {};
            let tieneDatos = false; // Bandera para saber si Firestore tiene información
            
            snapshot.forEach(doc => {
                tieneDatos = true;
                if (doc.id === 'creds') creds = doc.data();
                else keys[doc.id] = doc.data();
            });
            return { creds, keys, tieneDatos };
        } catch (e) { 
            return { creds: {}, keys: {}, tieneDatos: false }; 
        }
    };

    const writeState = async (data, id) => {
        try {
            await coleccionSesion.doc(id).set(data);
        } catch (e) { 
            console.error("Error al escribir datos de sesión en la nube:", e); 
        }
    };

    // Leemos el estado inicial de Firebase
    const sesionFirebase = await readState();

    const { state, saveCreds } = {
        state: {
            creds: sesionFirebase.creds || {},
            keys: {
                get: async (type, ids) => {
                    const data = {};
                    const dbState = await readState();
                    for (const id of ids) {
                        data[id] = dbState.keys[`${type}-${id}`];
                    }
                    return data;
                },
                set: async (data) => {
                    for (const type in data) {
                        for (const id in data[type]) {
                            const value = data[type][id];
                            const docId = `${type}-${id}`;
                            if (value) await writeState(value, docId);
                        }
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
        logger: pino({ level: 'silent' })
    });

    // Escucha en tiempo real de mensajes entrantes
    whatsappSock.ev.on('messages.upsert', async (m) => {
        const msg = m.messages[0];
        if (!msg.message || msg.key.fromMe || msg.key.remoteJid === 'status@broadcast') return;

        const numero = msg.key.remoteJid.replace('@s.whatsapp.net', '');
        const texto = msg.message.conversation || msg.message.extendedTextMessage?.text || "[Multimedia/Sticker recibido]";
        
        const mensajeEntrante = { numero, texto, hora: new Date().toISOString() };
        console.log(`[TrueWin-Chat] Mensaje de +${numero}: ${texto}`);
        io.emit('nuevo-mensaje', mensajeEntrante);
    });

    // CONTROL DE CONEXIÓN CORREGIDO ANTI-BUCLE (Arranque en frío)
    whatsappSock.ev.on('connection.update', async (update) => {
        const { connection, lastDisconnect, qr } = update;

        if (qr) {
            ultimoQR = qr;
            io.emit('qr-update', qr);
            console.log('[TrueWin] 📲 Código QR generado con éxito. Listo para escanear en el CRM.');
        }

        if (connection === 'close') {
            const statusCode = lastDisconnect?.error?.output?.statusCode;
            
            // SI FIRESTORE NO TIENE DATOS o el código es deslogueado voluntario, FRENAMOS LA RECONEXIÓN
            const esArranqueEnFrio = !sesionFirebase.tieneDatos;
            const esSesionInvalida = statusCode === 401 || statusCode === DisconnectReason.loggedOut;
            
            const shouldReconnect = !esArranqueEnFrio && !esSesionInvalida;

            console.log(`[TrueWin] Conexión cerrada (Código: ${statusCode}). ¿Reconectando?: ${shouldReconnect}`);
            io.emit('estado-conexion', 'desconectado');

            if (shouldReconnect) {
                console.log('[TrueWin] Esperando 5 segundos antes de reintentar...');
                setTimeout(() => { connectToWhatsApp(); }, 5000);
            } else {
                console.log('[TrueWin] Base de datos vacía o desvinculada. Esperando escaneo de código QR original...');
            }
        } else if (connection === 'open') {
            console.log('[TrueWin] ¡CONEXIÓN GLOBAL CONFIGURADA EN LA NUBE CON FIRESTORE!');
            ultimoQR = null;
            io.emit('estado-conexion', 'conectado');
        }
    });

    whatsappSock.ev.on('creds.update', saveCreds);
}

// Control del túnel de Sockets entrantes
io.on('connection', (socket) => {
    console.log('[Socket.IO] ¡Nueva pestaña del CRM sincronizada al túnel en tiempo real!');
    
    // 1. Si WhatsApp ya está abierto y conectado de antes
    if (whatsappSock && whatsappSock.user) {
        socket.emit('estado-conexion', 'conectado');
    } 
    // 2. Si hay un código QR listo y esperando a ser escaneado
    else if (ultimoQR) {
        socket.emit('estado-conexion', 'desconectado');
        socket.emit('qr-update', ultimoQR);
    } 
    // 3. Si está en el arranque en frío (base de datos vacía pero inicializando)
    else {
        socket.emit('estado-conexion', 'desconectado');
    }
});

// =========================================================================
// 4. ENDPOINTS DE CONTROL (MÓDULOS DE DISPARO MULTIMEDIA)
// =========================================================================

app.get('/status', (req, res) => {
    if (whatsappSock && whatsappSock.user) {
        return res.json({ status: "connected", user: whatsappSock.user });
    }
    res.json({ status: "disconnected", qr: ultimoQR });
});

app.post('/send-text', async (req, res) => {
    const { numero, mensaje } = req.body;
    if (!whatsappSock) return res.status(500).json({ error: "WhatsApp no inicializado." });
    try {
        const jid = `${numero}@s.whatsapp.net`;
        await whatsappSock.sendMessage(jid, { text: mensaje });
        res.json({ success: true });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

app.post('/send-image', async (req, res) => {
    const { numero, urlImagen, caption } = req.body;
    if (!whatsappSock) return res.status(500).json({ error: "WhatsApp no inicializado." });
    try {
        const jid = `${numero}@s.whatsapp.net`;
        await whatsappSock.sendMessage(jid, { image: { url: urlImagen }, caption: caption });
        res.json({ success: true });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

app.post('/send-audio', async (req, res) => {
    const { numero, urlAudio } = req.body;
    if (!whatsappSock) return res.status(500).json({ error: "WhatsApp no inicializado." });
    try {
        const jid = `${numero}@s.whatsapp.net`;
        await whatsappSock.sendMessage(jid, { audio: { url: urlAudio }, mimetype: 'audio/mp4', ptt: true });
        res.json({ success: true });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// Arrancar servidor unificado
httpServer.listen(PORT, () => {
    console.log(`[TrueWin-Web] Servidor de la Agencia corriendo en el puerto ${PORT}`);
});

connectToWhatsApp();