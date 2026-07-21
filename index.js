const express = require('express');
const cors = require('cors');

const app = express(); // 🚀 1. Primero creamos 'app'
app.use(cors({ origin: true, credentials: true }));

// 1. Requerir multer y el módulo 'fs'
const multer = require('multer');
const fs = require('fs');

if (!fs.existsSync('uploads')) {
    fs.mkdirSync('uploads');
}
const upload = multer({ dest: 'uploads/' });

const pino = require('pino');
const { default: makeWASocket, DisconnectReason, initAuthCreds, BufferJSON, Browsers } = require('@whiskeysockets/baileys');
const { createServer } = require('http');
const { Server } = require('socket.io');

const { initializeApp, cert } = require('firebase-admin/app');
const { getFirestore } = require('firebase-admin/firestore'); 
const serviceAccount = require('./firebase-credentials.json');

// =========================================================================
// 0. DETECTORES DE ERRORES CRÍTICOS
// =========================================================================
process.on('uncaughtException', (err) => console.error('\n[NODE FATAL] Excepción no capturada:', err));
process.on('unhandledRejection', (reason, promise) => console.error('\n[NODE FATAL] Promesa rechazada no manejada:', reason));

// =========================================================================
// 1. INICIALIZACIÓN DE FIREBASE Y FUNCIONES HELPER (user_profiles + email)
// =========================================================================
initializeApp({
    credential: cert(serviceAccount)
});

const db = getFirestore();
db.settings({ ignoreUndefinedProperties: true });

// 🚀 HELPER FUNCTIONS APUNTANDO A 'user_profiles' Y AL EMAIL
const getColeccionSesion = (email) => db.collection('user_profiles').doc(email).collection('crm_whatsapp_session');
const getColeccionMensajes = (email) => db.collection('user_profiles').doc(email).collection('crm_mensajes');
const getColeccionPlantillas = (email) => db.collection('user_profiles').doc(email).collection('crm_plantillas');
const getColeccionContactos = (email) => db.collection('user_profiles').doc(email).collection('crm_contactos');

const Jimp = require('jimp');
const sharp = require('sharp');

// 🚀 GUARDAR MENSAJE EN BASE DE DATOS
async function guardarMensajeBD(email, numero, nombre, texto, tipo, remitente = null, mediaUrl = null, mediaType = null) {
    try {
        if (!email) {
            console.error("Error: Se intentó guardar un mensaje sin proporcionar el email del usuario.");
            return; 
        }

        await getColeccionMensajes(email).add({
            numero: numero,
            nombre: nombre || "Desconocido",
            texto: texto,
            tipo: tipo,
            remitente: remitente,
            mediaUrl: mediaUrl,
            mediaType: mediaType,
            hora: new Date().toISOString(),
            timestamp: Date.now() 
        });
    } catch (error) {
        console.error(`Error guardando mensaje en historial para el usuario ${email}:`, error);
    }
}

// 🚀 REGISTRAR CONTACTO INTELIGENTE
async function registrarContactoInteligente(email, jid, pushName, esGrupo, whatsappSockLocal) {
    if (!whatsappSockLocal || jid.includes('status@broadcast')) return;

    try {
        const docRef = getColeccionContactos(email).doc(jid);
        const doc = await docRef.get();

        if (doc.exists) {
            await docRef.update({ ultimaActividad: Date.now() });
            return;
        }

        let nombreOficial = pushName || "Usuario Desconocido";
        let fotoUrl = null;
        let tipoEntidad = esGrupo ? 'grupo' : 'persona';

        if (esGrupo) {
            try {
                const metadata = await whatsappSockLocal.groupMetadata(jid);
                nombreOficial = metadata.subject || nombreOficial;
                if (metadata.announce) tipoEntidad = 'comunidad_avisos';
            } catch (e) { }
        }

        try { fotoUrl = await whatsappSockLocal.profilePictureUrl(jid, 'image'); } catch (e) { }

        await docRef.set({
            jid: jid,
            nombreOriginal: nombreOficial,
            nombrePersonalizado: "",
            tipo: tipoEntidad,
            fotoPerfil: fotoUrl,
            creadoEl: new Date().toISOString(),
            ultimaActividad: Date.now()
        });
    } catch (error) {}
}

function formatearJid(numero) {
    const numStr = numero.toString();
    if (numStr.includes('@g.us')) return numStr; 
    if (numStr.includes('@lid')) return numStr;  
    return `${numStr.replace(/[^0-9]/g, '')}@s.whatsapp.net`; 
}

// =========================================================================
// 2. CONFIGURACIÓN DEL SERVIDOR HTTP Y WEBSOCKETS
// =========================================================================
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

const sesionesActivas = new Map(); 
const qrActivos = new Map();
const cacheCriptografica = new Map();
const inicializandoSesiones = new Set();
let ultimosMensajesKey = {};
const idsEnviadosPorBot = new Set();

// =========================================================================
// 3. CONEXIÓN A WHATSAPP CON CACHÉ EN RAM + LOTES EN FIRESTORE
// =========================================================================
async function connectToWhatsApp(email) {
    // 🛡️ DOBLE VALIDACIÓN: Si llega vacío hasta aquí, abortamos antes de tocar Firebase
    if (!email || typeof email !== 'string' || email.trim() === '') {
        console.error(`[TrueWin-Backend] 🛑 ERROR FATAL EVITADO: connectToWhatsApp recibió un email vacío.`);
        return;
    }
    
    email = email.trim();
    console.log(`[TrueWin-Backend] Sincronizando sesión para: ${email}...`);

    // Si pasamos la barrera de arriba, esta línea ya NUNCA dará error
    const coleccionSesionUsuario = getColeccionSesion(email);

    if (!cacheCriptografica.has(email)) {
        cacheCriptografica.set(email, { creds: {}, keys: {}, cargada: false });
    }
    let cacheLocal = cacheCriptografica.get(email);

    const readState = async () => {
        if (cacheLocal.cargada) {
            return { creds: cacheLocal.creds, keys: cacheLocal.keys, tieneDatos: true };
        }
        try {
            const snapshot = await coleccionSesionUsuario.get();
            let tieneDatos = false; 
            
            snapshot.forEach(doc => {
                tieneDatos = true;
                const parsedData = JSON.parse(doc.data().payload, BufferJSON.reviver);
                if (doc.id === 'creds') cacheLocal.creds = parsedData;
                else cacheLocal.keys[doc.id] = parsedData;
            });

            if (tieneDatos) cacheLocal.cargada = true;
            return { creds: cacheLocal.creds, keys: cacheLocal.keys, tieneDatos };
        } catch (e) { 
            return { creds: {}, keys: {}, tieneDatos: false }; 
        }
    };

    const writeState = async (data, id) => {
        try {
            if (id === 'creds') cacheLocal.creds = data;
            else cacheLocal.keys[id] = data;

            const stringifiedData = JSON.stringify(data, BufferJSON.replacer);
            await coleccionSesionUsuario.doc(id).set({ payload: stringifiedData });
        } catch (e) { }
    };

    const sesionFirebase = await readState();
    let credencialesActivas = cacheLocal.creds; 
    
    if (Object.keys(credencialesActivas).length === 0) {
        credencialesActivas = initAuthCreds();
        cacheLocal.creds = credencialesActivas;
        cacheLocal.cargada = true; 
    }

    const { state, saveCreds } = {
        state: {
            creds: cacheLocal.creds,
            keys: {
                get: async (type, ids) => {
                    const data = {};
                    for (const id of ids) {
                        const docId = `${type}-${id}`;
                        data[id] = cacheLocal.keys[docId];
                    }
                    return data;
                },
                set: async (data) => {
                    let batch = db.batch(); 
                    let contador = 0;

                    for (const type in data) {
                        for (const id in data[type]) {
                            const value = data[type][id];
                            const docId = `${type}-${id}`;
                            
                            if (docId.includes('lid-mapping')) continue; 

                            const docRef = coleccionSesionUsuario.doc(docId); 
                            
                            if (value) {
                                cacheLocal.keys[docId] = value; 
                                const stringifiedData = JSON.stringify(value, BufferJSON.replacer);
                                batch.set(docRef, { payload: stringifiedData });
                            } else {
                                delete cacheLocal.keys[docId];
                                batch.delete(docRef);
                            }
                            
                            contador++;
                            if (contador >= 490) {
                                await batch.commit().catch(e => console.error("Error en lote parcial:", e));
                                batch = db.batch(); 
                                contador = 0; 
                            }
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

    const { fetchLatestWaWebVersion } = require('@whiskeysockets/baileys');
    let versionWaWeb = [2, 3000, 1015901307];
    try {
        const { version } = await fetchLatestWaWebVersion();
        versionWaWeb = version;
    } catch (e) { }

    const whatsappSock = makeWASocket({
        auth: state,
        printQRInTerminal: false,
        version: versionWaWeb, 
        browser: Browsers.ubuntu('Chrome'), 
        getMessage: async (key) => undefined,
        logger: pino({ level: 'silent' }) 
    });

    sesionesActivas.set(email, whatsappSock);

    const sendMessageOriginal = whatsappSock.sendMessage.bind(whatsappSock);
    whatsappSock.sendMessage = async (jid, content, options) => {
        const msgEnviado = await sendMessageOriginal(jid, content, options);
        if (msgEnviado && msgEnviado.key && msgEnviado.key.id) {
            idsEnviadosPorBot.add(msgEnviado.key.id);
            if (idsEnviadosPorBot.size > 500) {
                idsEnviadosPorBot.delete(idsEnviadosPorBot.values().next().value);
            }
        }
        return msgEnviado;
    };

    const { Boom } = require('@hapi/boom'); 

    whatsappSock.ev.on('connection.update', async (update) => {
        const { connection, lastDisconnect, qr } = update; 
        
        if (qr) {
            console.log(`[TrueWin] Nuevo QR para el usuario: ${email}`);
            qrActivos.set(email, qr);
            io.to(email).emit('qr-update', qr); 
        }

        if (connection === 'close') {
            const errorBoom = lastDisconnect?.error instanceof Boom ? lastDisconnect.error : null;
            const codigoError = errorBoom ? errorBoom.output?.statusCode : (lastDisconnect?.error?.output?.statusCode || 500);
            
            if (codigoError === 405 || codigoError === 401) {
                io.to(email).emit('estado-conexion', 'desconectado'); 
                sesionesActivas.delete(email);
                cacheCriptografica.set(email, { creds: {}, keys: {}, cargada: false });

                try {
                    const snapshot = await coleccionSesionUsuario.get();
                    if (!snapshot.empty) {
                        const batch = db.batch();
                        snapshot.docs.forEach((doc) => batch.delete(doc.ref));
                        await batch.commit();
                    }
                } catch (fsError) {}

                setTimeout(() => connectToWhatsApp(email), 4000);
                return;
            }

            if (codigoError === 403 || codigoError === DisconnectReason.forbidden || codigoError === DisconnectReason.loggedOut) {
                io.to(email).emit('estado-conexion', 'desconectado'); 
                sesionesActivas.delete(email);
                cacheCriptografica.set(email, { creds: {}, keys: {}, cargada: false });
                return; 
            }

            if (whatsappSock) whatsappSock.ev.removeAllListeners();
            setTimeout(() => connectToWhatsApp(email), 3000); 
        }
        
        if (connection === 'open') {
            console.log(`[TrueWin] ¡CONEXIÓN ESTABLECIDA PARA: ${email}!`);
            qrActivos.delete(email); 
            io.to(email).emit('estado-conexion', 'conectado'); 
        }
    });

    whatsappSock.ev.on('messages.upsert', async (m) => {
        const msg = m.messages[0];

        if (msg.key.fromMe && msg.key.id && idsEnviadosPorBot.has(msg.key.id)) return;
        if (!msg.message || msg.key.remoteJid === 'status@broadcast' || msg.key.remoteJid.includes('@newsletter')) return;

        const tipoMensaje = msg.key.fromMe ? 'out' : 'in';
        const messageType = Object.keys(msg.message || {})[0];

        if (['protocolMessage', 'pollUpdateMessage', 'pollCreationMessage', 'reactionMessage', 'senderKeyDistributionMessage'].includes(messageType)) return;

        const tiempoActualUnix = Math.floor(Date.now() / 1000);
        if (msg.messageTimestamp && (tiempoActualUnix - msg.messageTimestamp) > 60) return;

        const remoteJid = msg.key.remoteJid;
        const esGrupo = remoteJid.endsWith('@g.us');
        let nombrePerfil = msg.pushName || (esGrupo ? "Grupo de WhatsApp" : "Usuario"); 
        let remitenteEspecifico = esGrupo ? (msg.pushName || msg.key.participant?.split('@')[0] || "Miembro") : null; 

        await registrarContactoInteligente(email, remoteJid, msg.pushName, esGrupo, whatsappSock);
        
        try {
            const contactoDoc = await getColeccionContactos(email).doc(remoteJid).get();
            if (contactoDoc.exists) {
                const cData = contactoDoc.data();
                nombrePerfil = cData.nombrePersonalizado || cData.nombreOriginal || nombrePerfil;
            }
        } catch (e) {}
        
        const identificador = remoteJid;
        
        if (tipoMensaje === 'in') {
            if (!ultimosMensajesKey[email]) ultimosMensajesKey[email] = {};
            ultimosMensajesKey[email][identificador] = msg.key;
        }
        
        let texto = msg.message?.conversation || msg.message?.extendedTextMessage?.text || "";
        let mediaUrl = null;
        let mediaType = null;

        if (messageType === 'imageMessage') {
            mediaType = 'image';
            texto = msg.message.imageMessage.caption || ""; 
        } else if (messageType === 'videoMessage') {
            mediaType = 'video';
            texto = msg.message.videoMessage.caption || ""; 
        } else if (messageType === 'audioMessage') {
            mediaType = 'audio';
            texto = ""; 
        } else if (!texto) {
            texto = "[Archivo o mensaje interactivo]";
        }

        if (mediaType) {
            try {
                const { downloadMediaMessage } = require('@whiskeysockets/baileys');
                const buffer = await downloadMediaMessage(msg, 'buffer', {}, { logger: pino({ level: 'silent' }) });
                
                if (buffer) {
                    const crypto = require('crypto');
                    const token = crypto.randomUUID(); 
                    let extension = mediaType === 'image' ? 'png' : (mediaType === 'video' ? 'mp4' : 'ogg');
                    let contentType = mediaType === 'image' ? 'image/png' : (mediaType === 'video' ? 'video/mp4' : 'audio/ogg; codecs=opus');

                    const nombreArchivo = `crm_incoming/${email}/${identificador.replace(/[^a-zA-Z0-9]/g, '')}_${Date.now()}.${extension}`;
                    const { getStorage } = require('firebase-admin/storage');
                    const bucket = getStorage().bucket('truezone-agency.firebasestorage.app');
                    const archivoBlob = bucket.file(nombreArchivo);
                    
                    await archivoBlob.save(buffer, {
                        metadata: { metadata: { firebaseStorageDownloadTokens: token } },
                        contentType: contentType
                    });
                    
                    mediaUrl = `https://firebasestorage.googleapis.com/v0/b/${bucket.name}/o/${encodeURIComponent(nombreArchivo)}?alt=media&token=${token}`;
                }
            } catch (err) {
                console.error(`[Email: ${email}] Error procesando multimedia:`, err);
            }
        }
        
        await guardarMensajeBD(email, identificador, nombrePerfil, texto, tipoMensaje, remitenteEspecifico, mediaUrl, mediaType);

        io.to(email).emit('nuevo-mensaje', { 
            numero: identificador, 
            nombre: nombrePerfil, 
            texto: texto, 
            hora: new Date().toISOString(),
            remitente: remitenteEspecifico,
            mediaUrl: mediaUrl,
            mediaType: mediaType,
            tipo: tipoMensaje
        });
    });

    whatsappSock.ev.on('creds.update', saveCreds);
}

// =========================================================================
// 🔌 SOCKET.IO - EVENTOS DE CONEXIÓN Y PRESENCIA
// =========================================================================

io.on('connection', (socket) => {
    socket.on('autenticar', async (data) => {
    // Extrae email ya sea que venga como string directo o como objeto { email }
    let email = typeof data === 'string' ? data : data?.email || data?.uid;

    if (!email || typeof email !== 'string' || email.trim() === '') {
        console.error(`[Socket.IO] 🛑 Intento de autenticación rechazado: Email inválido.`);
        return;
    }

    email = email.trim();
    console.log(`[Socket.IO] Autenticando sala privada para el email: ${email}`);
    socket.join(email); 

    if (sesionesActivas.has(email) || inicializandoSesiones.has(email)) {
        const whatsappSockLocal = sesionesActivas.get(email);
        if (whatsappSockLocal && whatsappSockLocal.user) {
            socket.emit('estado-conexion', 'conectado');
        } else if (qrActivos.has(email)) {
            socket.emit('estado-conexion', 'desconectado');
            socket.emit('qr-update', qrActivos.get(email));
        }
        return;
    }

    inicializandoSesiones.add(email);
    
    try {
        await connectToWhatsApp(email);
    } finally {
        inicializandoSesiones.delete(email);
    }
});

    socket.on('crm-presencia', async (data) => {
        // También protegemos este evento por si acaso
        let email = typeof data.email === 'string' ? data.email.trim() : null;
        if (!email) return;

        const whatsappSockLocal = sesionesActivas.get(email);
        if (!whatsappSockLocal) return;
        try {
            const jid = formatearJid(data.numero);
            await whatsappSockLocal.sendPresenceUpdate(data.estado, jid);
        } catch (e) {}
    });
});
// =========================================================================
// 🚀 ENDPOINTS BASE & CRM CONTACTOS
// =========================================================================

app.get('/status', (req, res) => {
    const email = req.query.email || req.query.uid;
    if (!email) return res.status(401).json({ status: "disconnected" });
    
    const sock = sesionesActivas.get(email);
    if (sock && sock.user) {
        return res.json({ status: "connected", user: sock.user });
    }
    res.json({ status: "disconnected", qr: qrActivos.get(email) });
});

app.get('/api/contactos', async (req, res) => {
    try {
        const email = req.query.email || req.query.uid;
        if (!email) return res.status(401).json({ error: "Falta email" });

        const snapshot = await getColeccionContactos(email).orderBy('ultimaActividad', 'desc').get();
        let contactos = [];
        snapshot.forEach(doc => contactos.push(doc.data()));
        res.json(contactos);
    } catch (error) {
        res.status(500).json({ error: "Fallo al obtener contactos" });
    }
});

app.put('/api/contactos/:jid', async (req, res) => {
    try {
        const { jid } = req.params;
        const email = req.body.email || req.body.uid;
        const { nombrePersonalizado } = req.body;
        if (!email) return res.status(401).json({ error: "Falta email" });
        
        await getColeccionContactos(email).doc(jid).update({
            nombrePersonalizado: nombrePersonalizado
        });
        
        res.json({ success: true, message: "Nombre actualizado con éxito" });
    } catch (error) {
        res.status(500).json({ error: "Fallo al actualizar el nombre" });
    }
});

// =========================================================================
// 💬 ENDPOINTS DE ENVÍO MANUALE DE MENSAJES
// =========================================================================

// --- ENVIAR TEXTO ---
app.post('/send-text', async (req, res) => {
    const email = req.body.email || req.body.uid;
    const { numero, mensaje, linkData } = req.body; 
    
    if (!email) {
        return res.status(400).json({ error: "Falta el parámetro email." });
    }

    const whatsappSockLocal = sesionesActivas.get(email);
    if (!whatsappSockLocal) {
        return res.status(401).json({ error: "Tu sesión de WhatsApp no está activa." });
    }

    try {
        const mensajeFinal = typeof procesarSpintax === 'function' ? procesarSpintax(mensaje) : mensaje;
        const jidReal = formatearJid(numero);

        if (linkData && linkData.url) {
            await enviarTarjetaEnlace(jidReal, mensajeFinal, linkData, whatsappSockLocal);
        } else {
            const urlRegex = /(https?:\/\/[^\s]+|www\.[^\s]+|[a-zA-Z0-9-]+\.[a-zA-Z]{2,}(?:\/[^\s]*)?)/g;
            const urls = mensajeFinal.match(urlRegex);

            if (urls && urls.length > 0) {
                const linkDetectado = urls[0];
                const linkDataInfo = await extraerMetadatos(linkDetectado);
                await enviarTarjetaEnlace(jidReal, mensajeFinal, linkDataInfo, whatsappSockLocal);
            } else {
                await whatsappSockLocal.sendMessage(jidReal, { text: mensajeFinal });
            }
        }

        await guardarMensajeBD(email, numero, "TrueWin", mensajeFinal, 'out');
        
        io.to(email).emit('nuevo-mensaje', { 
            numero: numero, 
            nombre: "TrueWin", 
            texto: mensajeFinal, 
            hora: new Date().toISOString(),
            timestamp: Date.now(),
            remitente: null,
            mediaUrl: null,
            mediaType: null,
            tipo: 'out' 
        });

        res.json({ success: true });
    } catch (error) {
        console.error("Fallo al enviar texto manual:", error);
        res.status(500).json({ error: "Fallo al enviar texto: " + error.message });
    }
});

// --- ENVIAR IMAGEN ---
app.post('/send-image', async (req, res) => {
    try {
        const email = req.body.email || req.body.uid;
        const { numero, urlImagen, caption } = req.body;

        if (!email) return res.status(400).json({ success: false, message: 'Falta email.' });

        const whatsappSockLocal = sesionesActivas.get(email);
        if (!whatsappSockLocal) return res.status(401).json({ success: false, message: `Instancia no conectada para ${email}` });

        if (!numero || !urlImagen) return res.status(400).json({ success: false, message: 'Número y urlImagen requeridos.' });

        const formattedNumber = formatearJid(numero);
        const captionFinal = typeof procesarSpintax === 'function' ? procesarSpintax(caption) : caption;

        await whatsappSockLocal.sendPresenceUpdate('composing', formattedNumber);
        
        // 🚀 DESCARGA EN RAM: Garantiza que la imagen fluya desde Firebase Storage a Meta
        const resMedia = await fetch(urlImagen);
        const bufferMedia = Buffer.from(await resMedia.arrayBuffer());

        await whatsappSockLocal.sendMessage(formattedNumber, {
            image: bufferMedia,
            caption: captionFinal || ''
        });

        await whatsappSockLocal.sendPresenceUpdate('paused', formattedNumber);

        const textoMensaje = captionFinal || '[Imagen]';
        await guardarMensajeBD(email, formattedNumber, "TrueWin", textoMensaje, 'out', null, urlImagen, 'image');

        io.to(email).emit('nuevo-mensaje', {
            numero: formattedNumber,
            nombre: "TrueWin",
            texto: textoMensaje,
            hora: new Date().toISOString(),
            timestamp: Date.now(),
            remitente: null,
            mediaUrl: urlImagen, 
            mediaType: 'image',
            tipo: 'out'
        });
        res.json({ success: true, message: 'Imagen enviada con éxito.' });
    } catch (error) {
        console.error('Error al enviar la imagen:', error);
        res.status(500).json({ success: false, message: 'Error al enviar imagen: ' + error.message });
    }
});

// --- ENVIAR VIDEO ---
app.post('/send-video', async (req, res) => {
    try {
        const email = req.body.email || req.body.uid;
        const { numero, urlVideo, caption } = req.body;

        if (!email) return res.status(400).json({ success: false, message: 'Falta email.' });

        const whatsappSockLocal = sesionesActivas.get(email);
        if (!whatsappSockLocal) return res.status(401).json({ success: false, message: `Instancia no conectada para ${email}` });

        if (!numero || !urlVideo) return res.status(400).json({ success: false, message: 'Número y urlVideo requeridos.' });

        const formattedNumber = formatearJid(numero);
        const captionFinal = typeof procesarSpintax === 'function' ? procesarSpintax(caption) : caption;

        await whatsappSockLocal.sendPresenceUpdate('composing', formattedNumber);

        // 🚀 EL TRUCO QUE SALVÓ LAS AUTOMATIZACIONES: Descargar y forzar el buffer
        const resMedia = await fetch(urlVideo);
        const bufferMedia = Buffer.from(await resMedia.arrayBuffer());

        await whatsappSockLocal.sendMessage(formattedNumber, {
            video: bufferMedia,
            caption: captionFinal || '',
            mimetype: 'video/mp4' // 🚀 Sello obligatorio anti-imagen rota
        });

        await whatsappSockLocal.sendPresenceUpdate('paused', formattedNumber);

        const textoMensaje = captionFinal || '[Video]';
        await guardarMensajeBD(email, formattedNumber, "TrueWin", textoMensaje, 'out', null, urlVideo, 'video');

        io.to(email).emit('nuevo-mensaje', {
            numero: formattedNumber,
            nombre: "TrueWin",
            texto: textoMensaje,
            hora: new Date().toISOString(),
            timestamp: Date.now(),
            remitente: null,
            mediaUrl: urlVideo,
            mediaType: 'video',
            tipo: 'out'
        });
        res.json({ success: true, message: 'Video enviado con éxito.' });
    } catch (error) {
        res.status(500).json({ success: false, message: 'Error al enviar video: ' + error.message });
    }
});

// --- ENVIAR AUDIO ---
app.post('/send-audio', async (req, res) => {
    try {
        const email = req.body.email || req.body.uid;
        const { numero, urlAudio } = req.body;

        if (!email) return res.status(400).json({ success: false, message: 'Falta email.' });

        const whatsappSockLocal = sesionesActivas.get(email);
        if (!whatsappSockLocal) return res.status(401).json({ success: false, message: `Instancia no conectada para ${email}` });

        if (!numero || !urlAudio) return res.status(400).json({ success: false, message: 'Número y urlAudio requeridos.' });

        const formattedNumber = formatearJid(numero);

        await whatsappSockLocal.sendPresenceUpdate('recording', formattedNumber);
        
        await whatsappSockLocal.sendMessage(formattedNumber, {
            audio: { url: urlAudio },
            mimetype: 'audio/ogg; codecs=opus',
            ptt: true
        });

        await whatsappSockLocal.sendPresenceUpdate('paused', formattedNumber);

        const textoMensaje = '[Nota de voz]';
        await guardarMensajeBD(email, formattedNumber, "TrueWin", textoMensaje, 'out', null, urlAudio, 'audio');

        io.to(email).emit('nuevo-mensaje', {
            numero: formattedNumber,
            nombre: "TrueWin",
            texto: textoMensaje,
            hora: new Date().toISOString(),
            timestamp: Date.now(),
            remitente: null,
            mediaUrl: urlAudio,
            mediaType: 'audio',
            tipo: 'out'
        });
        res.json({ success: true, message: 'Audio enviado con éxito.' });
    } catch (error) {
        res.status(500).json({ success: false, message: 'Error al enviar audio: ' + error.message });
    }
});

// =========================================================================
// 📜 HISTORIAL DE CONVERSACIONES
// =========================================================================

app.get('/api/historial', async (req, res) => {
    try {
        const email = req.query.email || req.query.uid; // Corregida lectura de email
        if (!email) return res.status(401).json({ error: "Falta email" });

        const snapshot = await db.collection('user_profiles').doc(email).collection('crm_mensajes').orderBy('timestamp', 'asc').get();
        
        let todosLosMensajes = [];
        snapshot.forEach(doc => todosLosMensajes.push(doc.data()));
        
        const historial = {};
        const nombres = {};
        
        todosLosMensajes.forEach(data => {
            if (!historial[data.numero]) historial[data.numero] = [];
            
            let chatArray = historial[data.numero];
            let ultimoMensaje = chatArray.length > 0 ? chatArray[chatArray.length - 1] : null;

            // Lógica de Collage para imágenes consecutivas
            if (
                ultimoMensaje && 
                ultimoMensaje.tipo === data.tipo &&
                ultimoMensaje.mediaType === 'image' && 
                data.mediaType === 'image' &&
                (data.timestamp - ultimoMensaje.timestamp) < 60000 
            ) {
                if (!ultimoMensaje.esCollage) {
                    ultimoMensaje.esCollage = true;
                    ultimoMensaje.mediaUrls = [ultimoMensaje.mediaUrl];
                }
                ultimoMensaje.mediaUrls.push(data.mediaUrl);
                
                if (data.texto && data.texto !== "[Archivo o mensaje interactivo]") {
                    ultimoMensaje.texto = ultimoMensaje.texto + "\n" + data.texto;
                }
            } else {
                chatArray.push({ 
                    tipo: data.tipo, 
                    texto: data.texto, 
                    hora: data.hora,
                    timestamp: data.timestamp,
                    remitente: data.remitente || null,
                    mediaUrl: data.mediaUrl || null,
                    mediaType: data.mediaType || null,
                    esCollage: false
                });
            }

            if (data.tipo === 'in' && data.nombre) nombres[data.numero] = data.nombre;
        });
        
        res.json({ historial, nombres });
    } catch (error) {
        console.error("Error obteniendo historial:", error);
        res.status(500).json({ error: "Fallo al obtener historial" });
    }
});

// =====================================================================
// 🚀 ENDPOINTS DE UTILIDAD Y SESIÓN
// =====================================================================

// Activa el doble check azul en el teléfono del cliente
app.post('/api/marcar-visto', async (req, res) => {
    const { numero, email } = req.body;
    if (!email || !numero) return res.status(400).json({ success: false, error: "Faltan parámetros" });

    const whatsappSockLocal = sesionesActivas.get(email);
    if (!whatsappSockLocal) return res.json({ success: false, message: "Sesión no encontrada" });
    
    try {
        if (ultimosMensajesKey[email] && ultimosMensajesKey[email][numero]) {
            await whatsappSockLocal.readMessages([ultimosMensajesKey[email][numero]]);
            res.json({ success: true });
        } else {
            res.json({ success: true, message: "Sin mensajes pendientes en caché" });
        }
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

// Obtiene la foto de perfil del contacto en WhatsApp
app.get('/api/foto-perfil', async (req, res) => {
    const { jid, email } = req.query;
    if (!email || !jid) return res.json({ url: null });

    const whatsappSockLocal = sesionesActivas.get(email);
    if (!whatsappSockLocal) return res.json({ url: null });
    
    try {
        await new Promise(resolve => setTimeout(resolve, Math.floor(Math.random() * 200) + 200));
        const urlFoto = await whatsappSockLocal.profilePictureUrl(jid, 'image');
        return res.json({ url: urlFoto });
    } catch (e) {
        return res.json({ url: null }); 
    }
});

// =====================================================================
// 🤖 ENDPOINTS PARA EL MOTOR DE AUTOMATIZACIONES
// =====================================================================

app.get('/api/automatizaciones', async (req, res) => {
    try {
        const { email } = req.query;
        if (!email) return res.status(401).json({ error: "Falta email" });

        const snapshot = await db.collection('user_profiles').doc(email).collection('crm_automatizaciones').get();
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
        const { email } = data;
        if (!email) return res.status(401).json({ error: "Falta email" });

        await db.collection('user_profiles').doc(email).collection('crm_automatizaciones').doc(data.id).set(data);
        res.json({ success: true });
    } catch (error) {
        res.status(500).json({ error: "Fallo al guardar automatización" });
    }
});

app.delete('/api/automatizaciones/:id', async (req, res) => {
    try {
        const { id } = req.params;
        const { email } = req.query;
        if (!email) return res.status(401).json({ error: "Falta email" });

        await db.collection('user_profiles').doc(email).collection('crm_automatizaciones').doc(id).delete();
        res.json({ success: true });
    } catch (error) {
        res.status(500).json({ error: "Fallo al eliminar automatización" });
    }
});

app.get('/api/config/automatizaciones', async (req, res) => {
    try {
        const { email } = req.query;
        if (!email) return res.status(401).json({ error: "Falta email" });

        const doc = await db.collection('user_profiles').doc(email).collection('crm_config').doc('automatizaciones').get();
        if (!doc.exists) {
            return res.json({ activo: false }); 
        }
        res.json(doc.data());
    } catch (e) { 
        res.status(500).json({ error: e.message }); 
    }
});

app.post('/api/config/automatizaciones', async (req, res) => {
    try {
        const { activo, email } = req.body;
        if (!email) return res.status(401).json({ error: "Falta email" });

        await db.collection('user_profiles').doc(email).collection('crm_config').doc('automatizaciones').set({ activo });
        res.json({ success: true });
    } catch (e) { 
        res.status(500).json({ error: e.message }); 
    }
});

// =========================================================================
// 📄 GESTOR DE PLANTILLAS DINÁMICAS (SECUENCIAS)
// =========================================================================

app.get('/api/plantillas', async (req, res) => {
    try {
        const { email } = req.query;
        if (!email) return res.status(401).json({ error: "Falta email" });

        const snapshot = await db.collection('user_profiles').doc(email).collection('crm_plantillas').get();
        const plantillas = [];
        snapshot.forEach(doc => plantillas.push({ id: doc.id, ...doc.data() }));
        res.json(plantillas);
    } catch (error) { 
        res.status(500).json({ error: "Fallo al obtener plantillas" }); 
    }
});

app.post('/api/plantillas', async (req, res) => {
    try {
        const { email, id, nombre, secuencia } = req.body;
        if (!email) return res.status(401).json({ error: "Falta email" });

        await db.collection('user_profiles').doc(email).collection('crm_plantillas').doc(id).set({
            nombre: nombre, 
            secuencia: secuencia, 
            timestamp: Date.now()
        });
        res.json({ success: true, id: id });
    } catch (error) { 
        res.status(500).json({ error: "Fallo al guardar plantilla" }); 
    }
});

app.delete('/api/plantillas/:id', async (req, res) => {
    try {
        const { id } = req.params;
        const { email } = req.query;
        if (!email) return res.status(401).json({ error: "Falta email" });

        await db.collection('user_profiles').doc(email).collection('crm_plantillas').doc(id).delete();
        res.json({ success: true });
    } catch (error) {
        res.status(500).json({ error: "Fallo al eliminar plantilla" });
    }
});

// =========================================================================
// 🛠️ FUNCIONES AUXILIARES DE PROCESAMIENTO Y RASTREO
// =========================================================================

async function extraerMetadatos(urlStr) {
    try {
        let urlFinal = urlStr.startsWith('http') ? urlStr : 'https://' + urlStr;
        const res = await fetch(urlFinal, {
            headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)' },
        });
        const html = await res.text();

        const titleMatch = html.match(/<meta[^>]*property="og:title"[^>]*content="([^"]+)"/i) || html.match(/<title>([^<]+)<\/title>/i);
        const descMatch = html.match(/<meta[^>]*property="og:description"[^>]*content="([^"]+)"/i) || html.match(/<meta[^>]*name="description"[^>]*content="([^"]+)"/i);
        const imgMatch = html.match(/<meta[^>]*property="og:image"[^>]*content="([^"]+)"/i);

        return {
            url: urlFinal,
            title: titleMatch ? titleMatch[1] : "Enlace Oficial",
            description: descMatch ? descMatch[1] : "",
            imageUrl: imgMatch ? imgMatch[1] : null
        };
    } catch (e) {
        console.warn("[Scraper] Fallo al leer la web:", e.message);
        return { url: urlStr, title: "Visitar Enlace", description: "", imageUrl: null };
    }
}

async function enviarTarjetaEnlace(jidReal, mensajeFinal, linkData, whatsappSockLocal) {
    let thumbnailBuffer = null;
    let finalWidth = 0;
    let finalHeight = 0;

    let textoVisible = mensajeFinal || "";
    if (linkData && linkData.url && !textoVisible.includes(linkData.url)) {
        textoVisible = textoVisible ? `${textoVisible}\n\n🌐 ${linkData.url}` : linkData.url;
    }

    if (linkData && linkData.imageUrl) {
        try {
            const resImagen = await fetch(linkData.imageUrl, { headers: { 'User-Agent': 'Mozilla/5.0' } });
            if (resImagen.ok) {
                const originalBuffer = Buffer.from(await resImagen.arrayBuffer());
                const sharp = require('sharp');
                
                const metadata = await sharp(originalBuffer).metadata();
                let originalWidth = metadata.width || 800;
                let originalHeight = metadata.height || 418;
                
                if (originalWidth > 800) {
                    originalHeight = Math.round((800 / originalWidth) * originalHeight);
                    originalWidth = 800;
                }
                
                finalWidth = originalWidth;
                finalHeight = originalHeight;
                let calidad = 80;

                thumbnailBuffer = await sharp(originalBuffer)
                    .resize({ width: finalWidth, height: finalHeight, fit: 'inside' })
                    .jpeg({ quality: calidad })
                    .toBuffer();

                while (thumbnailBuffer.length > 40000 && calidad > 10) {
                    calidad -= 5;
                    thumbnailBuffer = await sharp(originalBuffer)
                        .resize({ width: finalWidth, height: finalHeight, fit: 'inside' })
                        .jpeg({ quality: calidad })
                        .toBuffer();
                }
            }
        } catch (e) {
            console.warn("[Tarjeta Orgánica] Fallo al procesar proporciones:", e.message);
        }
    }

    const { generateWAMessageFromContent } = require('@whiskeysockets/baileys');

    const payloadExtended = {
        text: textoVisible, 
        matchedText: linkData.url,
        canonicalUrl: linkData.url,
        title: linkData.title || "Enlace",
        description: linkData.description || ""
    };

    if (thumbnailBuffer) {
        payloadExtended.jpegThumbnail = thumbnailBuffer;
        payloadExtended.thumbnailWidth = finalWidth;
        payloadExtended.thumbnailHeight = finalHeight;
    }

    const mensajeProtobuf = generateWAMessageFromContent(jidReal, {
        extendedTextMessage: payloadExtended
    }, { userJid: whatsappSockLocal.user.id });

    await whatsappSockLocal.relayMessage(jidReal, mensajeProtobuf.message, { messageId: mensajeProtobuf.key.id });
}

// =========================================================================
// 🤖 MOTOR DE EVALUACIÓN Y DESPACHO EN NUBE
// =========================================================================

async function procesarBotEnNube(email, numeroCliente, textoMensaje, whatsappSockLocal) {
    if (!textoMensaje || !whatsappSockLocal || !email) return;
    const textoLimpio = textoMensaje.toLowerCase().trim();

    try {
        const configDoc = await db.collection('user_profiles').doc(email).collection('crm_config').doc('automatizaciones').get();
        if (!configDoc.exists || !configDoc.data().activo) return;

        const autosSnapshot = await db.collection('user_profiles').doc(email).collection('crm_automatizaciones').get();
        let automatizaciones = [];
        autosSnapshot.forEach(doc => automatizaciones.push(doc.data()));

        for (const auto of automatizaciones) {
            const arrayKeywords = auto.palabraClave.split(',').map(k => k.toLowerCase().trim()).filter(k => k);
            let haceMatch = false;

            for (const kw of arrayKeywords) {
                if (auto.condicion === 'exacta' && textoLimpio === kw) { haceMatch = true; break; }
                if (auto.condicion === 'contiene' && textoLimpio.includes(kw)) { haceMatch = true; break; }
            }

            if (haceMatch) {
                if (auto.frecuencia === 'unica') {
                    const idLogUnico = `${auto.id}_${numeroCliente.replace(/[^a-zA-Z0-9]/g, '')}`;
                    const registroDoc = await db.collection('user_profiles').doc(email).collection('crm_registro_bot').doc(idLogUnico).get();
                    
                    if (registroDoc.exists) break; 
                    
                    await db.collection('user_profiles').doc(email).collection('crm_registro_bot').doc(idLogUnico).set({
                        idAutomatizacion: auto.id, 
                        palabraClave: auto.palabraClave, 
                        numeroCliente: numeroCliente, 
                        ejecutadoEl: new Date().toISOString()
                    });
                }

                const tiempoLecturaHumana = Math.floor(Math.random() * (3500 - 1500 + 1)) + 1500;
                setTimeout(async () => {
                    if (ultimosMensajesKey[email] && ultimosMensajesKey[email][numeroCliente]) {
                        try { 
                            await whatsappSockLocal.readMessages([ultimosMensajesKey[email][numeroCliente]]); 
                        } catch (e) { }
                    }
                }, tiempoLecturaHumana);

                const tplDoc = await db.collection('user_profiles').doc(email).collection('crm_plantillas').doc(auto.idPlantilla).get();
                if (!tplDoc.exists) break;
                
                despacharFlujoDesdeNube(email, numeroCliente, tplDoc.data(), whatsappSockLocal);
                break; 
            }
        }
    } catch (err) {
        console.error("Error procesando bot en nube:", err);
    }
}

async function despacharFlujoDesdeNube(email, numeroDestino, tpl, whatsappSockLocal) {
    const pause = (ms) => new Promise(res => setTimeout(res, ms));
    try { await pause(Math.floor(Math.random() * (2200 - 1200 + 1)) + 1200); } catch (e) {}

    for (const msj of tpl.secuencia) {
        try {
            let textoOriginal = msj.texto || ""; 
            let mUrl = msj.url || null; 
            let mType = null;
            const jidReal = formatearJid(numeroDestino);
            let textoBurbuja = (msj.tipo === 'texto' || msj.tipo === 'media' || msj.tipo === 'enlace') ? procesarSpintax(textoOriginal) : textoOriginal;

            try {
                if (msj.tipo === 'audio') {
                    await whatsappSockLocal.sendPresenceUpdate('recording', numeroDestino);
                    await pause(4000); 
                } else {
                    await whatsappSockLocal.sendPresenceUpdate('composing', numeroDestino);
                    const caracteres = textoBurbuja ? textoBurbuja.length : 20;
                    let tiempoTipeo = Math.max(1200, Math.min((caracteres * Math.floor(Math.random() * (55 - 25 + 1)) + 25) + Math.floor(Math.random() * (800 - 300 + 1)) + 300, 6500));
                    await pause(tiempoTipeo);
                }
            } catch (e) { }

            if (msj.tipo === 'texto') {
                const urlRegex = /(https?:\/\/[^\s]+|www\.[^\s]+|[a-zA-Z0-9-]+\.[a-zA-Z]{2,}(?:\/[^\s]*)?)/g;
                const urls = textoBurbuja.match(urlRegex);
                if (urls && urls.length > 0) {
                    const linkDataInfo = await extraerMetadatos(urls[0]);
                    await enviarTarjetaEnlace(jidReal, textoBurbuja, linkDataInfo, whatsappSockLocal);
                } else {
                    await whatsappSockLocal.sendMessage(jidReal, { text: textoBurbuja });
                }
            } else if (msj.tipo === 'media' && msj.url) {
                try {
                    const resMedia = await fetch(msj.url);
                    const bufferMedia = Buffer.from(await resMedia.arrayBuffer());
                    const contentType = resMedia.headers.get('content-type') || '';

                    if (contentType.includes('video') || msj.url.toLowerCase().includes('.mp4') || msj.url.toLowerCase().includes('.mov')) {
                        mType = 'video';
                        await whatsappSockLocal.sendMessage(jidReal, { video: bufferMedia, caption: textoBurbuja || "[Video enviado]", mimetype: 'video/mp4' });
                    } else {
                        mType = 'image';
                        await whatsappSockLocal.sendMessage(jidReal, { image: bufferMedia, caption: textoBurbuja || "[Imagen enviada]" });
                    }
                } catch (error) { 
                    console.error("[Bot Nube] Error enviando archivo multimedia:", error);
                }
            } else if (msj.tipo === 'audio' && msj.url) {
                mType = 'audio';
                await whatsappSockLocal.sendMessage(jidReal, { audio: { url: msj.url }, mimetype: 'audio/ogg; codecs=opus', ptt: true });
            }

            try { await whatsappSockLocal.sendPresenceUpdate('paused', numeroDestino); } catch (e) {}

            await guardarMensajeBD(email, numeroDestino, "TrueWin", textoBurbuja, 'out', null, mUrl, mType);

            // 🚀 EMISIÓN POR SALA PRIVADA IDENTIFICADA POR EMAIL
            io.to(email).emit('nuevo-mensaje', { 
                numero: numeroDestino, 
                nombre: "TrueWin", 
                texto: textoBurbuja, 
                hora: new Date().toISOString(), 
                timestamp: Date.now(),
                remitente: null, 
                mediaUrl: mUrl, 
                mediaType: mType, 
                tipo: 'out' 
            });

            await pause(Math.floor(Math.random() * (5500 - 2500 + 1)) + 2500);
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
        // 🚀 ELIMINADO: await connectToWhatsApp(); 
        // (Ya no iniciamos a ciegas, esperamos a que el usuario se conecte y nos dé su UID)
        
        httpServer.listen(PORT, () => {
            console.log(`[TrueWin-Web] 🚀 API y WebSockets listos y escuchando en el puerto ${PORT}`);
        });
    } catch (error) {
        console.error("[Crítico] Fallo al iniciar el ecosistema de TrueWin:", error);
    }
}

// 🚀 PROCESADOR DE SPINTAX DIRECTO EN CAPA DE RED
function procesarSpintax(texto) {
    if (!texto) return "";
    // Escanea bloques estilo {opcion1|opcion2|opcion3} de forma recursiva
    return texto.replace(/\{([^{}]+)\}/g, (match, opciones) => {
        const arrayOpciones = opciones.split('|');
        // Elige un índice aleatorio en caliente
        return arrayOpciones[Math.floor(Math.random() * arrayOpciones.length)].trim();
    });
}



iniciarEcosistema();