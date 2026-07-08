const express = require('express');
const cors = require('cors');
const pino = require('pino');
const { default: makeWASocket, useMultiFileAuthState, DisconnectReason } = require('@whiskeysockets/baileys');

// 1. NUEVAS IMPORTACIONES PARA WEBSOCKETS
const { createServer } = require('http');
const { Server } = require('socket.io');

const app = express();
const PORT = process.env.PORT || 3000;

// 2. CONFIGURAMOS EL SERVIDOR HTTP PARA QUE SOPORTE EXPRESS Y SOCKETS A LA VEZ
const httpServer = createServer(app);
const io = new Server(httpServer, {
    cors: {
        origin: "*", // Permite a tu web en Firebase conectarse en tiempo real
        methods: ["GET", "POST"]
    }
});

// Middleware de CORS manual (tu remedio anti-bloqueos)
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
    console.log("[TrueWin-Backend] Inicializando módulo de autenticación...");
    const { state, saveCreds } = await useMultiFileAuthState('auth_session');

    whatsappSock = makeWASocket({
        auth: state,
        printQRInTerminal: false,
        logger: pino({ level: 'silent' })
    });

    // =========================================================================
    // 3. LA MAGIA EN TIEMPO REAL: ESCUCHA DE MENSAJES ENTRANTES
    // =========================================================================
    whatsappSock.ev.on('messages.upsert', async (m) => {
        const msg = m.messages[0];
        
        // Ignoramos los mensajes que enviamos nosotros mismos o estados/historias
        if (!msg.message || msg.key.fromMe || msg.key.remoteJid === 'status@broadcast') return;

        const numero = msg.key.remoteJid.replace('@s.whatsapp.net', '');
        
        // Extraemos el texto del mensaje (soporta texto plano o respuestas a otros mensajes)
        const texto = msg.message.conversation || msg.message.extendedTextMessage?.text || "[Multimedia/Sticker recibido]";
        
        const mensajeEntrante = {
            numero: numero,
            texto: texto,
            hora: new Date().toISOString()
        };

        console.log(`[TrueWin-Chat] Nuevo mensaje de ${numero}: ${texto}`);

        // Emitimos el mensaje por el túnel a la interfaz web (Firebase)
        io.emit('nuevo-mensaje', mensajeEntrante);
    });

    whatsappSock.ev.on('connection.update', (update) => {
        const { connection, lastDisconnect, qr } = update;

        if (qr) {
            ultimoQR = qr;
            io.emit('qr-update', qr); // Transmitimos el QR en vivo por si la web está abierta
        }

        if (connection === 'close') {
            const shouldReconnect = (lastDisconnect?.error?.output?.statusCode !== DisconnectReason.loggedOut);
            console.log('[TrueWin] Conexión caída. ¿Reconectando?:', shouldReconnect);
            io.emit('estado-conexion', 'desconectado');
            if (shouldReconnect) connectToWhatsApp();
        } else if (connection === 'open') {
            console.log('[TrueWin] ¡CONEXIÓN EXITOSA EN SEGUNDO PLANO!');
            ultimoQR = null;
            io.emit('estado-conexion', 'conectado');
        }
    });

    whatsappSock.ev.on('creds.update', saveCreds);
}

io.on('connection', (socket) => {
    console.log('[Socket.IO] Panel de TrueWin Agency conectado al túnel en vivo.');
    
    // Si hay un estado pendiente, se lo enviamos a la pestaña que se acaba de abrir
    if (ultimoQR) {
        socket.emit('qr-update', ultimoQR);
    } else if (whatsappSock && whatsappSock.user) {
        socket.emit('estado-conexion', 'conectado');
    }
});

// =========================================================================
// ENDPOINTS DE LA API (Rutas de control para la agencia)
// =========================================================================

// Ruta de prueba para verificar el estado desde el navegador
app.get('/status', (req, res) => {
    if (whatsappSock && whatsappSock.user) {
        return res.json({ status: "connected", user: whatsappSock.user });
    }
    // Si no está conectado, le escupe el string del QR a la web
    res.json({ status: "disconnected", qr: ultimoQR });
});

// =========================================================================
// ENDPOINT: ENVIAR TEXTO PLANO
// =========================================================================
app.post('/send-text', async (req, res) => {
    const { numero, mensaje } = req.body;

    if (!whatsappSock) {
        return res.status(500).json({ error: "El cliente de WhatsApp no está inicializado." });
    }

    try {
        const jid = `${numero}@s.whatsapp.net`;
        await whatsappSock.sendMessage(jid, { text: mensaje });
        console.log(`[TrueWin-API] Mensaje enviado con éxito a ${numero}`);
        res.json({ success: true, message: "Mensaje despachado." });
    } catch (error) {
        console.error("[TrueWin-API] Error enviando mensaje:", error);
        res.status(500).json({ error: "Fallo al enviar el paquete de texto." });
    }
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
httpServer.listen(PORT, () => {
    console.log(`[TrueWin-Web] Servidor API + WebSockets corriendo en el puerto ${PORT}`);
});

// Iniciar la conexión de WhatsApp
connectToWhatsApp();