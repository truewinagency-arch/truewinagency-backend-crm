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
const coleccionSesion = db.collection('crm_whatsapp_session');
const coleccionMensajes = db.collection('crm_mensajes');
const coleccionPlantillas = db.collection('crm_plantillas'); // 🚀 NUEVA BASE PARA PLANTILL

// 🚀 DETECTOR DEL NÚMERO CONECTADO ACTUALMENTE AL SERVIDOR
function getHostNumber() {
    if (whatsappSock && whatsappSock.user && whatsappSock.user.id) {
        // Baileys guarda el número así: "584121234567:1@s.whatsapp.net". Esto lo limpia.
        return whatsappSock.user.id.split(':')[0].split('@')[0]; 
    }
    return 'desconectado';
}

// 🚀 FUNCIÓN MAESTRA: Guarda cada disparo con soporte multimedia integral
async function guardarMensajeBD(numero, nombre, texto, tipo, remitente = null, mediaUrl = null, mediaType = null) {
    try {
        const hostActual = getHostNumber(); 
        if (hostActual === 'desconectado') return; 

        await db.collection('crm_mensajes').add({
            host: hostActual, 
            numero: numero,
            nombre: nombre || "Desconocido",
            texto: texto,
            tipo: tipo,
            remitente: remitente,
            mediaUrl: mediaUrl,   // 🚀 NUEVO
            mediaType: mediaType, // 🚀 NUEVO
            hora: new Date().toISOString(),
            timestamp: Date.now() 
        });
    } catch (error) {
        console.error("Error guardando en historial:", error);
    }
}

// 🚀 ENRUTADOR INTELIGENTE: Detecta si es un Grupo, un @lid o Persona normal sin romper formatos
function formatearJid(numero) {
    const numStr = numero.toString();
    if (numStr.includes('@g.us')) return numStr; 
    if (numStr.includes('@lid')) return numStr;  
    return `${numStr.replace(/[^0-9]/g, '')}@s.whatsapp.net`; 
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
let ultimosMensajesKey = {};

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
        browser: ['Windows', 'Chrome', '111.0.0.0'], 
        logger: pino({ level: 'error' })
    });

    whatsappSock.ev.on('messages.upsert', async (m) => {
        const msg = m.messages[0];
        if (!msg.message || msg.key.fromMe || msg.key.remoteJid === 'status@broadcast' || msg.key.remoteJid.includes('@newsletter')) {
            return;
        }

        // 🚀 CANDADO ANTI-BLOQUEO: Filtra y destruye la sincronización histórica masiva
        const tiempoActualUnix = Math.floor(Date.now() / 1000);
        if (msg.messageTimestamp && (tiempoActualUnix - msg.messageTimestamp) > 60) {
            // Si el mensaje tiene más de 60 segundos de haber sido enviado en el pasado, 
            // significa que es parte del historial viejo de WhatsApp y lo ignoramos.
            console.log(`[Sincronización] Ignorando mensaje antiguo del JID: ${msg.key.remoteJid}`);
            return;
        }

        const remoteJid = msg.key.remoteJid;
        const esGrupo = remoteJid.endsWith('@g.us');
        let nombrePerfil = msg.pushName || "Usuario"; 
        let remitenteEspecifico = null; 

        if (esGrupo) {
            remitenteEspecifico = msg.pushName || msg.key.participant?.split('@')[0] || "Miembro";
            try {
                const metadata = await whatsappSock.groupMetadata(remoteJid);
                nombrePerfil = metadata.subject || "Grupo de WhatsApp";
            } catch (error) {}
        }
        
        const identificador = remoteJid; 
        ultimosMensajesKey[identificador] = msg.key;
        
        // 🚀 MOTOR DE TRADUCCIÓN MULTIMEDIA ENTRANTE
        const messageType = Object.keys(msg.message || {})[0];
        let texto = msg.message?.conversation || msg.message?.extendedTextMessage?.text || "";
        let mediaUrl = null;
        let mediaType = null;

       if (messageType === 'imageMessage') {
            mediaType = 'image';
            // 🚀 CORREGIDO: Si no hay caption, se guarda un texto vacío "" en lugar de "[Imagen recibida]"
            texto = msg.message.imageMessage.caption || ""; 
        } else if (messageType === 'videoMessage') {
            mediaType = 'video';
            // 🚀 CORREGIDO: Si no hay caption, se guarda un texto vacío "" en lugar de "[Video recibido]"
            texto = msg.message.videoMessage.caption || ""; 
        } else if (messageType === 'audioMessage') {
            mediaType = 'audio';
            // 🚀 CORREGIDO: Las notas de voz no llevan texto complementario, lo dejamos vacío
            texto = ""; 
        } else if (!texto) {
            texto = "[Archivo o mensaje interactivo recibido]";
        }

        // Si se detectó multimedia, lo descargamos del servidor de Meta y lo subimos a tu Storage
        if (mediaType) {
            try {
                const { downloadMediaMessage } = require('@whiskeysockets/baileys');
                const buffer = await downloadMediaMessage(msg, 'buffer', {}, { 
                    logger: pino({ level: 'error' }) 
                });
                
                if (buffer) {
                    const crypto = require('crypto');
                    const token = crypto.randomUUID(); // Token criptográfico nativo de Firebase
                    let extension = 'bin';
                    let contentType = 'application/octet-stream';
                    
                    if (mediaType === 'image') { extension = 'png'; contentType = 'image/png'; }
                    else if (mediaType === 'video') { extension = 'mp4'; contentType = 'video/mp4'; }
                    else if (mediaType === 'audio') { extension = 'ogg'; contentType = 'audio/ogg; codecs=opus'; }

                    const nombreArchivo = `crm_incoming/${identificador.replace(/[^a-zA-Z0-9]/g, '')}_${Date.now()}.${extension}`;
                    const { getStorage } = require('firebase-admin/storage');
                    const bucket = getStorage().bucket('truezone-agency.firebasestorage.app');
                    const archivoBlob = bucket.file(nombreArchivo);
                    
                    await archivoBlob.save(buffer, {
                        metadata: {
                            metadata: { firebaseStorageDownloadTokens: token }
                        },
                        contentType: contentType
                    });
                    
                    // Armamos la URL pública con el formato nativo compatible con el visualizador del frontend
                    mediaUrl = `https://firebasestorage.googleapis.com/v0/b/${bucket.name}/o/${encodeURIComponent(nombreArchivo)}?alt=media&token=${token}`;
                }
            } catch (err) {
                console.error("Error procesando o subiendo multimedia entrante de WhatsApp:", err);
            }
        }
        
        await guardarMensajeBD(identificador, nombrePerfil, texto, 'in', remitenteEspecifico, mediaUrl, mediaType);

        // 🚀 CONEXIÓN DEL BOT: Activamos el evaluador en la nube sobre la marcha
        procesarBotEnNube(identificador, texto);

        io.emit('nuevo-mensaje', { 
            numero: identificador, 
            nombre: nombrePerfil, 
            texto: texto, 
            hora: new Date().toISOString(),
            remitente: remitenteEspecifico,
            mediaUrl: mediaUrl, // 🚀 ENVIADO EN VIVO A LA WEB
            mediaType: mediaType
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
    
    if (whatsappSock && whatsappSock.user) {
        socket.emit('estado-conexion', 'conectado');
    } else if (ultimoQR) {
        socket.emit('estado-conexion', 'desconectado');
        socket.emit('qr-update', ultimoQR);
    } else {
        socket.emit('estado-conexion', 'desconectado');
    }

    socket.on('crm-presencia', async ({ numero, estado }) => {
        if (!whatsappSock) return;
        try {
            const jid = formatearJid(numero);
            await whatsappSock.sendPresenceUpdate(estado, jid);
        } catch (e) {}
    });
});

// =========================================================================
// 4. ENDPOINTS DE CONTROL (BLINDADOS ANTI-BANEO Y SOPORTE DE GRUPOS / LIDS)
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
    if (!whatsappSock) return res.status(500).json({ error: "No conectado" });
    
    try {
        const urls = mensaje.match(/(https?:\/\/[^\s]+)/g);
        
        if (urls && urls.length > 0) {
            const urlDetectada = urls[0];
            const esGrupo = urlDetectada.includes('chat.whatsapp.com');
            
            await whatsappSock.sendMessage(numero, { 
                text: mensaje,
                contextInfo: {
                    externalAdReply: {
                        title: esGrupo ? "Únete a nuestro Grupo de WhatsApp" : "🌐 Toca aquí para abrir el enlace",
                        body: "Truezone Agency",
                        sourceUrl: urlDetectada,
                        mediaType: 1,
                        showAdAttribution: true
                    }
                }
            });
        } else {
            await whatsappSock.sendMessage(numero, { text: mensaje });
        }

        await guardarMensajeBD(numero, "TrueWin", mensaje, 'out');
        res.json({ success: true });
    } catch (error) {
        console.error("Error enviando texto:", error);
        res.status(500).json({ error: "Fallo al enviar texto" });
    }
});

app.post('/send-image', async (req, res) => {
    const { numero, urlImagen, caption } = req.body;
    if (!whatsappSock) return res.status(500).json({ error: "WhatsApp no inicializado." });
    try {
        const jid = formatearJid(numero);
        
        await whatsappSock.sendPresenceUpdate('composing', jid);
        await delay(Math.floor(Math.random() * 1500) + 2000); 
        
        await whatsappSock.sendMessage(jid, { image: { url: urlImagen }, caption: caption });
        await whatsappSock.sendPresenceUpdate('paused', jid);

        await guardarMensajeBD(numero, "TrueWin", caption || "[Imagen enviada]", 'out', null, urlImagen, 'image');
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
        const jid = formatearJid(numero);
        
        // 🚀 CAMUFLAJE: Mostramos que estamos grabando
        await whatsappSock.sendPresenceUpdate('recording', jid);
        await delay(4000); 
        
        // 🚀 DETECCIÓN INTELIGENTE DE FORMATO
        // Revisamos si el enlace de Firebase contiene ".mp3"
        const esMP3 = urlAudio.toLowerCase().includes('.mp3');
        
        await whatsappSock.sendMessage(jid, { 
            audio: { url: urlAudio }, 
            // Si es MP3 usamos el formato oficial de música, si no, el de nota de voz
            mimetype: esMP3 ? 'audio/mpeg' : 'audio/ogg; codecs=opus', 
            // Apagamos el micrófono verde (PTT) si es un MP3 para evitar que el teléfono receptor colapse
            ptt: !esMP3 
        });

        await whatsappSock.sendPresenceUpdate('paused', jid);

        await guardarMensajeBD(numero, "TrueWin", "[Nota de voz enviada]", 'out', null, urlAudio, 'audio');
        res.json({ success: true });
    } catch (error) {
        console.error(`[Error] Fallo enviando audio a ${numero}:`, error);
        res.status(500).json({ error: error.message });
    }
});
// 🚀 ENDPOINT CORREGIDO Y BLINDADO: Carga el historial en milisegundos sin congelarse
app.get('/api/historial', async (req, res) => {
    try {
        const hostActivo = getHostNumber();
        if (hostActivo === 'desconectado') {
            return res.json({ historial: {}, nombres: {} }); 
        }

        // Buscamos tus mensajes en la base de datos crm_mensajes
        const snapshot = await db.collection('crm_mensajes').where('host', '==', hostActivo).get();
        let todosLosMensajes = [];
        snapshot.forEach(doc => todosLosMensajes.push(doc.data()));
        
        todosLosMensajes.sort((a, b) => a.timestamp - b.timestamp);

        const historial = {};
        const nombres = {};
        
        todosLosMensajes.forEach(data => {
            if (!historial[data.numero]) historial[data.numero] = [];
            historial[data.numero].push({ 
                tipo: data.tipo, 
                texto: data.texto, 
                hora: data.hora,
                remitente: data.remitente || null,
                mediaUrl: data.mediaUrl || null,
                mediaType: data.mediaType || null
            });

            if (data.tipo === 'in' && data.nombre) nombres[data.numero] = data.nombre;
        });
        
        // 🚀 RESPUESTA INMEDIATA: Entregamos el cerebro visual sin retrasos de red
        res.json({ historial, nombres });
    } catch (error) {
        console.error("Error obteniendo historial:", error);
        res.status(500).json({ error: "Fallo al obtener historial" });
    }
});

// 🚀 ENDPOINT: Activa el doble check azul en el teléfono del cliente
app.post('/api/marcar-visto', async (req, res) => {
    const { numero } = req.body;
    if (!whatsappSock || !numero) return res.json({ success: false });
    
    try {
        if (ultimosMensajesKey[numero]) {
            // Enviamos el recibo de lectura oficial a los servidores de Meta
            await whatsappSock.readMessages([ultimosMensajesKey[numero]]);
            res.json({ success: true });
        } else {
            res.json({ success: true, message: "Sin mensajes pendientes en caché" });
        }
    } catch (e) {
        console.error("Error al marcar visto:", e);
        res.status(500).json({ error: e.message });
    }
});

// 🚀 NUEVO ENDPOINT INDEPENDIENTE: Resuelve fotos bajo demanda en segundo plano
app.get('/api/foto-perfil', async (req, res) => {
    const { jid } = req.query;
    if (!whatsappSock || !jid) return res.json({ url: null });
    try {
        const urlFoto = await whatsappSock.profilePictureUrl(jid, 'image');
        res.json({ url: urlFoto });
    } catch (e) {
        res.json({ url: null }); // Si no tiene foto pública, responde null limpiamente sin tumbar el backend
    }
});

// =====================================================================
// 🤖 ENDPOINTS PARA EL MOTOR DE AUTOMATIZACIONES
// =====================================================================

app.get('/api/automatizaciones', async (req, res) => {
    try {
        const snapshot = await db.collection('crm_automatizaciones').get();
        let autos = [];
        snapshot.forEach(doc => autos.push(doc.data()));
        res.json(autos);
    } catch (error) {
        res.status(500).json({ error: "Fallo al obtener automatizaciones" });
    }
});

app.post('/api/automatizaciones', async (req, res) => {
    try {
        const data = req.body;
        await db.collection('crm_automatizaciones').doc(data.id).set(data);
        res.json({ success: true });
    } catch (error) {
        res.status(500).json({ error: "Fallo al guardar automatización" });
    }
});

app.delete('/api/automatizaciones/:id', async (req, res) => {
    try {
        await db.collection('crm_automatizaciones').doc(req.params.id).delete();
        res.json({ success: true });
    } catch (error) {
        res.status(500).json({ error: "Fallo al eliminar automatización" });
    }
});

// 🚀 ENDPOINTS DE CONFIGURACIÓN GLOBAL DEL BOT EN LA NUBE
app.get('/api/config/automatizaciones', async (req, res) => {
    try {
        const doc = await db.collection('crm_config').doc('automatizaciones').get();
        if (!doc.exists) {
            return res.json({ activo: false }); // Estado inicial por defecto
        }
        res.json(doc.data());
    } catch (e) { 
        res.status(500).json({ error: e.message }); 
    }
});

app.post('/api/config/automatizaciones', async (req, res) => {
    try {
        const { activo } = req.body;
        await db.collection('crm_config').doc('automatizaciones').set({ activo });
        res.json({ success: true });
    } catch (e) { 
        res.status(500).json({ error: e.message }); 
    }
});


// =========================================================================
// 5. GESTOR DE PLANTILLAS DINÁMICAS (SECUENCIAS) Y MULTIMEDIA
// =========================================================================
app.get('/api/plantillas', async (req, res) => {
    try {
        const snapshot = await coleccionPlantillas.get();
        const plantillas = [];
        snapshot.forEach(doc => plantillas.push({ id: doc.id, ...doc.data() }));
        res.json(plantillas);
    } catch (error) {
        console.error("Error obteniendo plantillas:", error);
        res.status(500).json({ error: "Fallo al obtener plantillas" });
    }
});

app.post('/api/plantillas', async (req, res) => {
    try {
        const { id, nombre, secuencia } = req.body;
        
        // 🚀 MAGIA: Usamos el ID generado (ej: info_de_cursos) como candado único del documento
        await coleccionPlantillas.doc(id).set({
            nombre: nombre,
            secuencia: secuencia,
            timestamp: Date.now()
        });
        
        res.json({ success: true, id: id });
    } catch (error) {
        console.error("Error guardando plantilla:", error);
        res.status(500).json({ error: "Fallo al guardar plantilla" });
    }
});

app.delete('/api/plantillas/:id', async (req, res) => {
    try {
        const { id } = req.params;
        await coleccionPlantillas.doc(id).delete();
        res.json({ success: true });
    } catch (error) {
        res.status(500).json({ error: "Fallo al eliminar plantilla" });
    }
});

// 🚀 NUEVO ENDPOINT: Soporte nativo para videos
app.post('/send-video', async (req, res) => {
    const { numero, urlVideo, caption } = req.body;
    if (!whatsappSock) return res.status(500).json({ error: "WhatsApp no inicializado." });
    try {
        const jid = formatearJid(numero);
        
        await whatsappSock.sendPresenceUpdate('composing', jid);
        await delay(Math.floor(Math.random() * 2000) + 3000); // Simula más peso de carga
        
        await whatsappSock.sendMessage(jid, { video: { url: urlVideo }, caption: caption });
        await whatsappSock.sendPresenceUpdate('paused', jid);

        await guardarMensajeBD(numero, "TrueWin", caption || "[Video enviado]", 'out', null, urlVideo, 'video');
        res.json({ success: true });
    } catch (error) {
        console.error(`[Error] Fallo enviando video a ${numero}:`, error);
        res.status(500).json({ error: error.message });
    }
});

// 🚀 CEREBRO DEL CHATBOT EN LA NUBE: Evalúa palabras clave 24/7 de forma autónoma
// 🚀 AJUSTE EN EN backend (index.js): Fuerza el visto automático en la nube antes de disparar
async function procesarBotEnNube(numeroCliente, textoMensaje) {
    if (!textoMensaje || !whatsappSock) return;
    const textoLimpio = textoMensaje.toLowerCase().trim();

    try {
        const configDoc = await db.collection('crm_config').doc('automatizaciones').get();
        if (!configDoc.exists || !configDoc.data().activo) return;

        const autosSnapshot = await db.collection('crm_automatizaciones').get();
        let automatizaciones = [];
        autosSnapshot.forEach(doc => automatizaciones.push(doc.data()));

        for (const auto of automatizaciones) {
            const keyword = auto.palabraClave.toLowerCase().trim();
            let haceMatch = false;

            if (auto.condicion === 'exacta' && textoLimpio === keyword) haceMatch = true;
            if (auto.condicion === 'contiene' && textoLimpio.includes(keyword)) haceMatch = true;

            if (haceMatch) {
                
                // 1. 🚀 EL CANDADO DE FRECUENCIA INTELIGENTE
                if (auto.frecuencia === 'unica') {
                    // Creamos una firma limpia eliminando caracteres técnicos del JID (ej: auto_12345_584123456789)
                    const idLogUnico = `${auto.id}_${numeroCliente.replace(/[^a-zA-Z0-9]/g, '')}`;
                    
                    // Verificamos si este cliente específico ya quemó esta regla en el pasado
                    const registroDoc = await db.collection('crm_registro_bot').doc(idLogUnico).get();
                    
                    if (registroDoc.exists) {
                        console.log(`[🤖 Bot Protegido] El cliente ${numeroCliente} ya recibió la regla ÚNICA "${keyword}" anteriormente. Omitiendo despacho.`);
                        break; // Rompemos el ciclo de forma segura sin enviar nada
                    }
                    
                    // Si no existe el registro, lo creamos de inmediato en la base de datos para congelar futuros intentos
                    await db.collection('crm_registro_bot').doc(idLogUnico).set({
                        idAutomatizacion: auto.id,
                        palabraClave: auto.palabraClave,
                        numeroCliente: numeroCliente,
                        ejecutadoEl: new Date().toISOString()
                    });
                }

                console.log(`[🤖 Bot en Nube] Ejecución autorizada para "${keyword}". Despachando secuencia...`);
                
                // 2. Ejecutar visto automático anti-ban
                if (ultimosMensajesKey[numeroCliente]) {
                    try {
                        await whatsappSock.readMessages([ultimosMensajesKey[numeroCliente]]);
                    } catch (e) { console.warn("Fallo al marcar visto autonomo:", e.message); }
                }

                // 3. Cargar secuencia y disparar ráfaga asíncrona
                const tplDoc = await db.collection('crm_plantillas').doc(auto.idPlantilla).get();
                if (!tplDoc.exists) {
                    console.error(`La plantilla ${auto.idPlantilla} no existe en Firestore.`);
                    break;
                }
                
                despacharFlujoDesdeNube(numeroCliente, tplDoc.data());
                break; 
            }
        }
    } catch (err) {
        console.error("Error en el validador de frecuencia en la nube:", err);
    }
}

// 🚀 DESPACHADOR ASÍNCRONO EN NUBE: Ejecuta secuencias con pausas humanas anti-ban
async function despacharFlujoDesdeNube(numeroDestino, tpl) {
    const pause = (ms) => new Promise(res => setTimeout(res, ms));
    
    for (const msj of tpl.secuencia) {
        try {
            let textoBurbuja = msj.texto || "";
            let mUrl = msj.url || null;
            let mType = null;

            if (msj.tipo === 'media' && msj.url) {
                mType = msj.url.includes('.mp4') || msj.url.includes('.mov') ? 'video' : 'image';
                if (!textoBurbuja) textoBurbuja = mType === 'video' ? "[Video enviado]" : "[Imagen enviada]";
            } else if (msj.tipo === 'audio') {
                mType = 'audio';
                textoBurbuja = "[Nota de voz enviada]";
            }

            // 🚀 TELEMETRÍA HUMANA AUTOMÁTICA: Activamos "Escribiendo..." o "Grabando audio..." antes del disparo
            try {
                if (msj.tipo === 'audio') {
                    // Le avisa a WhatsApp que el robot está grabando una nota de voz
                    await whatsappSock.sendPresenceUpdate('recording', numeroDestino);
                    await pause(3500); // Sostiene el "Grabando audio..." por 3.5 segundos para simular realismo
                } else {
                    // Le avisa a WhatsApp que el robot está escribiendo texto
                    await whatsappSock.sendPresenceUpdate('composing', numeroDestino);
                    await pause(2000); // Sostiene el "Escribiendo..." por 2 segundos simulación de tipeo
                }
            } catch (e) { 
                console.warn("No se pudo actualizar la telemetría de presencia en la nube:", e.message); 
            }

            // Disparo nativo vía Baileys según la morfología de la secuencia
           if (msj.tipo === 'texto') {
                const urls = msj.texto.match(/(https?:\/\/[^\s]+)/g);
                
                if (urls && urls.length > 0) {
                    const urlDetectada = urls[0];
                    const esGrupo = urlDetectada.includes('chat.whatsapp.com');

                    // 🚀 CANDADO DE SEGURIDAD: Inyectamos thumbnailUrl para evitar el cuelgue
                    await whatsappSock.sendMessage(numeroDestino, { 
                        text: msj.texto,
                        contextInfo: {
                            externalAdReply: {
                                title: esGrupo ? "Únete a nuestro Grupo de WhatsApp" : "🌐 Toca aquí para abrir el enlace",
                                body: "Truezone Agency",
                                sourceUrl: urlDetectada,
                                thumbnailUrl: "https://i.imgur.com/jM8A80e.jpg", // 🚀 Imagen por defecto (Negro/Dorado) que destraba la Promesa
                                mediaType: 1,
                                showAdAttribution: true
                            }
                        }
                    });
                } else {
                    await whatsappSock.sendMessage(numeroDestino, { text: msj.texto });
                }
            } else if (msj.tipo === 'media' && msj.url) {
                if (mType === 'video') {
                    await whatsappSock.sendMessage(numeroDestino, { video: { url: msj.url }, caption: msj.texto });
                } else {
                    await whatsappSock.sendMessage(numeroDestino, { image: { url: msj.url }, caption: msj.texto });
                }
            } else if (msj.tipo === 'audio' && msj.url) {
                await whatsappSock.sendMessage(numeroDestino, { audio: { url: msj.url }, mimetype: 'audio/ogg; codecs=opus', ptt: true });
            }

            // 🚀 APAGAR ESTADO: Le avisamos a Meta que el operador dejó de interactuar en este bloque
            try {
                await whatsappSock.sendPresenceUpdate('paused', numeroDestino);
            } catch (e) {}

            // Guardamos en el historial global de Firestore
            await guardarMensajeBD(numeroDestino, "TrueWin", textoBurbuja, 'out', null, mUrl, mType);

            // Emitimos por WebSockets para pintar los cambios en vivo en la web si está abierta
            io.emit('nuevo-mensaje', {
                numero: numeroDestino,
                nombre: "TrueWin",
                texto: textoBurbuja,
                hora: new Date().toISOString(),
                remitente: null,
                mediaUrl: mUrl,
                mediaType: mType,
                tipo: 'out'
            });

            // Delay inteligente calculado en caliente en la nube entre mensajes (2.5s a 5.5s)
            const delayHumano = Math.floor(Math.random() * (5500 - 2500 + 1)) + 2500;
            await pause(delayHumano);
            
        } catch (e) {
            console.error("Error disparando pieza del bot en la nube:", e);
        }
    }
}

// =========================================================================
// 6. ARRANQUE SEGURO EN ORDEN
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