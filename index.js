const { default: makeWASocket, useMultiFileAuthState, DisconnectReason } = require('@whiskeysockets/baileys');
const qrcode = require('qrcode-terminal');
const pino = require('pino');
const express = require('express');
const cors = require('cors');
const app = express();

const PORT = process.env.PORT || 3000;

// 1. CONFIGURACIÓN COMPLETA Y FORZADA DE CORS
app.use(cors({
    origin: '*',
    methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
    allowedHeaders: ['Content-Type', 'Authorization', 'Bypass-Tunnel-Reminder', 'bypass-tunnel-reminder']
}));

// 2. MIDDLEWARE MANUAL DE CONTROL DE PREFLIGHT (EL REMEDIO DEFINITIVO)
app.use((req, res, next) => {
    res.header('Access-Control-Allow-Origin', '*');
    res.header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS, PUT, DELETE');
    res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept, Authorization, Bypass-Tunnel-Reminder, bypass-tunnel-reminder');
    
    // Si la petición es de tipo OPTIONS (Preflight), respondemos con éxito inmediato status 200
    if (req.method === 'OPTIONS') {
        return res.sendStatus(200);
    }
    next();
});

// 3. MIDDLEWARE PARA ENTIENDER JSON (Debe ir abajo de los controles de arriba)
app.use(express.json());


// Variable global para almacenar la instancia del socket
let whatsappSock = null;

async function connectToWhatsApp() {
    console.log("[TrueWin-Backend] Inicializando módulo de autenticación...");
    const { state, saveCreds } = await useMultiFileAuthState('auth_session');

    whatsappSock = makeWASocket({
        auth: state,
        printQRInTerminal: false,
        logger: pino({ level: 'silent' })
    });

    whatsappSock.ev.on('connection.update', (update) => {
        const { connection, lastDisconnect, qr } = update;

        if (qr) {
            console.log("\n==================================================");
            console.log("[TrueWin] ESCANEA EL SIGUIENTE CÓDIGO QR CON TU TELÉFONO:");
            console.log("==================================================\n");
            qrcode.generate(qr, { small: true });
        }

        if (connection === 'close') {
            const shouldReconnect = (lastDisconnect?.error?.output?.statusCode !== DisconnectReason.loggedOut);
            console.log('[TrueWin] Conexión redirigida o caída. ¿Reconectando?:', shouldReconnect);
            if (shouldReconnect) {
                connectToWhatsApp();
            }
        } else if (connection === 'open') {
            console.log('\n==================================================');
            console.log('[TrueWin] ¡CONEXIÓN EXITOSA EN SEGUNDO PLANO!');
            console.log('El backend está vinculado y listo para recibir órdenes.');
            console.log('==================================================\n');
        }
    });

    whatsappSock.ev.on('creds.update', saveCreds);
}

// =========================================================================
// ENDPOINTS DE LA API (Rutas de control para la agencia)
// =========================================================================

// Ruta de prueba para verificar el estado desde el navegador
app.get('/status', (req, res) => {
    if (whatsappSock && whatsappSock.user) {
        return res.json({ status: "connected", user: whatsappSock.user });
    }
    res.json({ status: "disconnected" });
});

// =========================================================================
// ENDPOINT: ENVIAR IMAGEN CON PIE DE FOTO
// =========================================================================
app.post('/send-image', async (req, res) => {
    const { numero, urlImagen, caption } = req.body;

    if (!whatsappSock) {
        return res.status(500).json({ error: "El cliente de WhatsApp no está inicializado." });
    }

    try {
        const jid = `${numero}@s.whatsapp.net`;
        
        // Despachamos el paquete indicando que el objeto contiene una imagen por URL
        await whatsappSock.sendMessage(jid, { 
            image: { url: urlImagen }, 
            caption: caption 
        });
        
        console.log(`[TrueWin-API] Imagen enviada con éxito a ${numero}`);
        res.json({ success: true, message: "Imagen despachada." });
    } catch (error) {
        console.error("[TrueWin-API] Error enviando imagen:", error);
        res.status(500).json({ error: "Fallo al enviar el paquete de imagen." });
    }
});

// =========================================================================
// ENDPOINT: ENVIAR AUDIO COMO NOTA DE VOZ REAL (PTT)
// =========================================================================
app.post('/send-audio', async (req, res) => {
    const { numero, urlAudio } = req.body;

    if (!whatsappSock) {
        return res.status(500).json({ error: "El cliente de WhatsApp no está inicializado." });
    }

    try {
        const jid = `${numero}@s.whatsapp.net`;
        
        // La clave aquí es 'ptt: true'. Esto le dice a Meta que simule que el 
        // audio fue grabado directamente con el micrófono del teléfono.
        await whatsappSock.sendMessage(jid, { 
            audio: { url: urlAudio }, 
            mimetype: 'audio/mp4', 
            ptt: true 
        });
        
        console.log(`[TrueWin-API] Nota de voz (PTT) enviada a ${numero}`);
        res.json({ success: true, message: "Nota de voz enviada." });
    } catch (error) {
        console.error("[TrueWin-API] Error enviando audio:", error);
        res.status(500).json({ error: "Fallo al despachar el paquete de audio." });
    }
});

// Levantar el servidor HTTP Express
app.listen(PORT, () => {
    console.log(`[TrueWin-Web] Servidor API corriendo en el puerto ${PORT}`);
});

// Iniciar la conexión de WhatsApp
connectToWhatsApp();