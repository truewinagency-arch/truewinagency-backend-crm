const express = require('express');
const cors = require('cors');
const pino = require('pino');
// 1. IMPORTAMOS initAuthCreds PARA QUE SEPA QUÉ HACER CUANDO LA BASE ESTÉ VACÍA
const { default: makeWASocket, DisconnectReason, initAuthCreds } = require('@whiskeysockets/baileys');
const { createServer } = require('http');
const { Server } = require('socket.io');

const { initializeApp, cert } = require('firebase-admin/app');
const { getFirestore } = require('firebase-admin/firestore'); 
const serviceAccount = require('./firebase-credentials.json');

initializeApp({
    credential: cert(serviceAccount)
});

const db = getFirestore();
const coleccionSesion = db.collection('whatsapp_session');

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

async function connectToWhatsApp() {
    console.log("[TrueWin-Backend] Conectando con Firebase Firestore para verificar sesión remota...");

    const readState = async () => {
        try {
            const snapshot = await coleccionSesion.get();
            let creds = {};
            let keys = {};
            let tieneDatos = false; 
            
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

    const sesionFirebase = await readState();

    // 2. LA MAGIA: Si no hay datos, creamos credenciales oficiales en blanco para forzar el QR
    let credencialesActivas = sesionFirebase.creds;
    if (!sesionFirebase.tieneDatos || Object.keys(credencialesActivas).length === 0) {
        console.log('[TrueWin] Base de datos limpia. Generando credenciales oficiales para pedir QR...');
        credencialesActivas = initAuthCreds();
    }

    const { state, saveCreds } = {
        state: {
            creds: credencialesActivas,
            keys: {
                get: async (type, ids) => {
                    const data = {};
                    const dbState = await readState(); // Lee todo el bloque de una vez
                    for (const id of ids) {
                        data[id] = dbState.keys[`${type}-${id}`];
                    }
                    return data;
                },
                set: async (data) => {
                    // CUELLO DE BOTELLA RESUELTO: Agrupamos las tareas
                    const promesasDeGuardado = []; 
                    
                    for (const type in data) {
                        for (const id in data[type]) {
                            const value = data[type][id];
                            const docId = `${type}-${id}`;
                            if (value) {
                                // Quitamos el 'await' individual. Preparamos el misil, no lo disparamos aún.
                                promesasDeGuardado.push(writeState(value, docId));
                            }
                        }
                    }
                    // Disparamos las 500 peticiones a Firestore al mismo tiempo sin bloquear el servidor
                    Promise.all(promesasDeGuardado).catch(err => console.error("Fallo guardando llaves en lote:", err));
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

    whatsappSock.ev.on('messages.upsert', async (m) => {
        const msg = m.messages[0];
        if (!msg.message || msg.key.fromMe || msg.key.remoteJid === 'status@broadcast') return;
        const numero = msg.key.remoteJid.replace('@s.whatsapp.net', '');
        const texto = msg.message.conversation || msg.message.extendedTextMessage?.text || "[Multimedia/Sticker recibido]";
        io.emit('nuevo-mensaje', { numero, texto, hora: new Date().toISOString() });
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
            
            // Permitimos reconectar siempre, a menos que el usuario haya cerrado sesión a propósito
            const shouldReconnect = statusCode !== DisconnectReason.loggedOut;
            console.log(`[TrueWin] Conexión cerrada (Código: ${statusCode}). ¿Reconectando?: ${shouldReconnect}`);
            io.emit('estado-conexion', 'desconectado');

            if (shouldReconnect) {
                setTimeout(() => { connectToWhatsApp(); }, 5000);
            } else {
                console.log('[TrueWin] Sesión desvinculada desde el teléfono.');
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
    if (whatsappSock && whatsappSock.user) {
        socket.emit('estado-conexion', 'conectado');
    } else if (ultimoQR) {
        socket.emit('estado-conexion', 'desconectado');
        socket.emit('qr-update', ultimoQR);
    } else {
        socket.emit('estado-conexion', 'desconectado');
    }
});

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

httpServer.listen(PORT, () => {
    console.log(`[TrueWin-Web] Servidor de la Agencia corriendo en el puerto ${PORT}`);
});

connectToWhatsApp();