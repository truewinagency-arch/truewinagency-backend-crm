const express = require('express');
const cors = require('cors');
const pino = require('pino');
const { default: makeWASocket, DisconnectReason, initAuthCreds, BufferJSON, Browsers } = require('@whiskeysockets/baileys');
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

const Jimp = require('jimp');
const sharp = require('sharp');
// 🚀 DETECTOR DEL NÚMERO CONECTADO ACTUALMENTE AL SERVIDOR
function getHostNumber() {
    if (whatsappSock && whatsappSock.user && whatsappSock.user.id) {
        // Baileys guarda el número así: "584121234567:1@s.whatsapp.net". Esto lo limpia.
        return whatsappSock.user.id.split(':')[0].split('@')[0]; 
    }
    return 'desconectado';
}

// 🚀 FUNCIÓN MAESTRA: Guarda cada disparo con soporte multimedia integral
async function guardarMensajeBD(uid, numero, nombre, texto, tipo, remitente = null, mediaUrl = null, mediaType = null) {
    try {
        if (!uid) return; 

        // 🌟 MAGIA: Guardamos en la subcolección privada del UID
        await db.collection('usuarios').doc(uid).collection('crm_mensajes').add({
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
        console.error("Error guardando en historial aislado:", error);
    }
}

// 🚀 GESTOR DE CONTACTOS: Guarda, nombra y extrae metadatos de Grupos/Comunidades
// 🚀 GESTOR DE CONTACTOS AISLADO
async function registrarContactoInteligente(uid, jid, pushName, esGrupo, whatsappSockLocal) {
    if (!whatsappSockLocal || jid.includes('status@broadcast')) return;

    try {
        const docRef = db.collection('usuarios').doc(uid).collection('crm_contactos').doc(jid);
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

const sesionesActivas = new Map(); 
const qrActivos = new Map();
const cacheCriptografica = new Map();
const inicializandoSesiones = new Set(); // 🚀 NUEVO ESCUDO ANTI-BUCLES
let ultimosMensajesKey = {};

// 🚀 VARIABLES GLOBALES DE CACHÉ CRIPTOGRÁFICO (El parche del mensaje fantasma)
// Al estar aquí afuera, sobreviven a los reinicios del socket (Código 515)
let cacheCreds = {};
let cacheKeys = {};
let cacheCargada = false;;

const idsEnviadosPorBot = new Set();

// =========================================================================
// 3. CONEXIÓN A WHATSAPP CON CACHÉ EN RAM + LOTES EN FIRESTORE
// =========================================================================
async function connectToWhatsApp(uid) {
    console.log(`[TrueWin-Backend] Sincronizando e inicializando sesión para el UID: ${uid}...`);

    // 1. Aislamiento de Base de Datos: Cada usuario tiene su propia bóveda de llaves
    const coleccionSesionUsuario = db.collection('usuarios').doc(uid).collection('whatsapp_session');

    // 2. Aislamiento de RAM: Inicializamos el espacio de este usuario en el diccionario de caché
    if (!cacheCriptografica.has(uid)) {
        cacheCriptografica.set(uid, { creds: {}, keys: {}, cargada: false });
    }
    let cacheLocal = cacheCriptografica.get(uid);

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
                    // 🚀 CAMBIO 1: Usamos 'let' para poder reasignar el lote más adelante
                    let batch = db.batch(); 
                    let contador = 0;

                    for (const type in data) {
                        for (const id in data[type]) {
                            const value = data[type][id];
                            const docId = `${type}-${id}`;
                            
                            // Filtro Anti-Basura (Solo bloqueamos los grupos)
                            if (docId.includes('lid-mapping')) {
                                continue; 
                            }

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
                            
                            // 🚀 CAMBIO 2: Cuando llegamos a 490, subimos a la nube Y CREAMOS UN LOTE NUEVO FRESCO
                            if (contador >= 490) {
                                await batch.commit().catch(e => console.error("Error en lote parcial:", e));
                                batch = db.batch(); // <--- LA PIEZA FALTANTE: Inicializar lote nuevo
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

    // =========================================================================
    // 🚀 EXTRACCIÓN DE VERSIÓN OFICIAL Y CONFIGURACIÓN DEL SOCKET
    // =========================================================================
    const { fetchLatestWaWebVersion, Browsers } = require('@whiskeysockets/baileys');
    let versionWaWeb = [2, 3000, 1015901307];
    try {
        const { version } = await fetchLatestWaWebVersion();
        versionWaWeb = version;
    } catch (e) { }

    // 🚀 INICIALIZACIÓN DEL SOCKET AISLADO
    const whatsappSock = makeWASocket({
        auth: state,
        printQRInTerminal: false,
        version: versionWaWeb, 
        browser: Browsers.ubuntu('Chrome'), 
        getMessage: async (key) => undefined,
        logger: pino({ level: 'silent' }) 
    });

    // 🌟 EL PASO MAESTRO: Guardamos el teléfono de este usuario en nuestro diccionario global
    sesionesActivas.set(uid, whatsappSock);
    // =========================================================================
    // 🚀 INTERCEPTOR MAESTRO DE ENVÍOS (El aniquilador de duplicados)
    // Atrapa cualquier cosa que el bot envíe y guarda su ID en la memoria temporal.
    // =========================================================================
    const sendMessageOriginal = whatsappSock.sendMessage.bind(whatsappSock);
    whatsappSock.sendMessage = async (jid, content, options) => {
        const msgEnviado = await sendMessageOriginal(jid, content, options);
        if (msgEnviado && msgEnviado.key && msgEnviado.key.id) {
            idsEnviadosPorBot.add(msgEnviado.key.id);
            // Limpieza inteligente para no saturar la RAM de tu servidor
            if (idsEnviadosPorBot.size > 500) {
                idsEnviadosPorBot.delete(idsEnviadosPorBot.values().next().value);
            }
        }
        return msgEnviado;
    };

    const { DisconnectReason } = require('@whiskeysockets/baileys');
    const { Boom } = require('@hapi/boom'); 

 whatsappSock.ev.on('connection.update', async (update) => {
        const { connection, lastDisconnect, qr } = update; 
        
        if (qr) {
            console.log(`[TrueWin] Nuevo código QR para el UID: ${uid}`);
            qrActivos.set(uid, qr); // Guardamos su QR en el diccionario global
            io.to(uid).emit('qr-update', qr); // Emitimos SOLO a su sala privada
        }

        if (connection === 'close') {
            const errorBoom = lastDisconnect?.error instanceof Boom ? lastDisconnect.error : null;
            const codigoError = errorBoom ? errorBoom.output?.statusCode : (lastDisconnect?.error?.output?.statusCode || 500);
            
            if (codigoError === 405 || codigoError === 401) {
                io.to(uid).emit('estado-conexion', 'desconectado'); 
                sesionesActivas.delete(uid); // Sacamos del mapa
                cacheCriptografica.set(uid, { creds: {}, keys: {}, cargada: false });

                try {
                    const snapshot = await db.collection('usuarios').doc(uid).collection('whatsapp_session').get();
                    if (!snapshot.empty) {
                        const batch = db.batch();
                        snapshot.docs.forEach((doc) => batch.delete(doc.ref));
                        await batch.commit();
                    }
                } catch (fsError) {}

                setTimeout(() => connectToWhatsApp(uid), 4000);
                return;
            }

            if (codigoError === 403 || codigoError === DisconnectReason.forbidden || codigoError === DisconnectReason.loggedOut) {
                io.to(uid).emit('estado-conexion', 'desconectado'); 
                sesionesActivas.delete(uid);
                cacheCriptografica.set(uid, { creds: {}, keys: {}, cargada: false });
                return; 
            }

            if (whatsappSock) whatsappSock.ev.removeAllListeners();
            setTimeout(() => connectToWhatsApp(uid), 3000); 
        }
        
        if (connection === 'open') {
            console.log(`[TrueWin] ¡CONEXIÓN ESTABLECIDA PARA UID: ${uid}!`);
            qrActivos.delete(uid); 
            io.to(uid).emit('estado-conexion', 'conectado'); 
        }
    });

    whatsappSock.ev.on('messages.upsert', async (m) => {
        const msg = m.messages[0];

        
        
        // 🚀 ESCUDO ANTI-ECO: Si el mensaje lo envió este mismo servidor, lo ignoramos.
        // Ya fue guardado y dibujado por la función que lo disparó originalmente.
        if (msg.key.fromMe && msg.key.id && idsEnviadosPorBot.has(msg.key.id)) {
            return;
        }

        // Dejamos pasar todo lo demás (Mensajes del cliente y Mensajes enviados desde tu celular físico)
        if (!msg.message || msg.key.remoteJid === 'status@broadcast' || msg.key.remoteJid.includes('@newsletter')) {
            return;
        }

        const tipoMensaje = msg.key.fromMe ? 'out' : 'in';

        // EXTRAEMOS EL TIPO DE MENSAJE AL INICIO PARA LOS FILTROS
        const messageType = Object.keys(msg.message || {})[0];

        // Muro de contención para eventos de sistema.
        if (
            messageType === 'protocolMessage' || 
            messageType === 'pollUpdateMessage' || 
            messageType === 'pollCreationMessage' ||
            messageType === 'reactionMessage' ||
            messageType === 'senderKeyDistributionMessage'
        ) {
            return; 
        }

        // Filtra y destruye la sincronización histórica masiva
        const tiempoActualUnix = Math.floor(Date.now() / 1000);
        if (msg.messageTimestamp && (tiempoActualUnix - msg.messageTimestamp) > 60) {
            console.log(`[Sincronización] Ignorando mensaje antiguo del JID: ${msg.key.remoteJid}`);
            return;
        }

        const remoteJid = msg.key.remoteJid;
        const esGrupo = remoteJid.endsWith('@g.us');
        let nombrePerfil = msg.pushName || (esGrupo ? "Grupo de WhatsApp" : "Usuario"); 
        let remitenteEspecifico = null; 

        if (esGrupo) {
            remitenteEspecifico = msg.pushName || msg.key.participant?.split('@')[0] || "Miembro";
        }

        // 🚀 CLAVE: Forzamos al servidor a esperar que el contacto/grupo se registre y asiente su nombre real
        await registrarContactoInteligente(remoteJid, msg.pushName, esGrupo);
        
        // Buscamos si ya guardamos un nombre personalizado o real para este JID en Firestore
        try {
            const contactoDoc = await db.collection('crm_contactos').doc(remoteJid).get();
            if (contactoDoc.exists) {
                const cData = contactoDoc.data();
                nombrePerfil = cData.nombrePersonalizado || cData.nombreOriginal || nombrePerfil;
            }
        } catch (e) {
            console.warn("[Backend] No se pudo cruzar el nombre en caliente:", e.message);
        }
        
        const identificador = remoteJid;
        
        // 🚀 Solo guardamos la llave para el "visto azul automático" si el mensaje es del cliente
        if (tipoMensaje === 'in') {
            ultimosMensajesKey[identificador] = msg.key;
        }
        
        // MOTOR DE TRADUCCIÓN MULTIMEDIA ENTRANTE
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
                const buffer = await downloadMediaMessage(msg, 'buffer', {}, { 
                    logger: pino({ level: 'error' }) 
                });
                
                if (buffer) {
                    const crypto = require('crypto');
                    const token = crypto.randomUUID(); 
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
                    
                    mediaUrl = `https://firebasestorage.googleapis.com/v0/b/${bucket.name}/o/${encodeURIComponent(nombreArchivo)}?alt=media&token=${token}`;
                }
            } catch (err) {
                console.error("Error procesando multimedia de WhatsApp:", err);
            }
        }
        
        // 🚀 GUARDADO DINÁMICO: Pasamos la variable 'tipoMensaje' ('in' u 'out') a la base de datos
        await guardarMensajeBD(identificador, nombrePerfil, texto, tipoMensaje, remitenteEspecifico, mediaUrl, mediaType);

        // 🚀 PROTECCIÓN DEL BOT: Solo activamos la automatización si el mensaje es ENTRANTE
        if (tipoMensaje === 'in') {
            procesarBotEnNube(identificador, texto);
        }

        io.emit('nuevo-mensaje', { 
            numero: identificador, 
            nombre: nombrePerfil, 
            texto: texto, 
            hora: new Date().toISOString(),
            remitente: remitenteEspecifico,
            mediaUrl: mediaUrl,
            mediaType: mediaType,
            tipo: tipoMensaje // 🚀 MANDAMOS EL TIPO AL FRONTEND PARA QUE LO DIBUJE A LA DERECHA
        });
    });

 

whatsappSock.ev.on('creds.update', saveCreds);
}

io.on('connection', (socket) => {
    console.log('[Socket.IO] Un cliente se ha conectado al túnel en tiempo real.');

    // 🚀 EL DISPARADOR MAESTRO BLINDADO
    socket.on('autenticar', async (uid) => {
        if (!uid) return;
        
        console.log(`[Socket.IO] Autenticando sala privada para el UID: ${uid}`);
        socket.join(uid); 

        // 🚀 ESCUDO: Si ya está encendido O está en proceso de encenderse, lo frenamos
        if (sesionesActivas.has(uid) || inicializandoSesiones.has(uid)) {
            const whatsappSockLocal = sesionesActivas.get(uid);
            if (whatsappSockLocal && whatsappSockLocal.user) {
                socket.emit('estado-conexion', 'conectado');
            } else if (qrActivos.has(uid)) {
                socket.emit('estado-conexion', 'desconectado');
                socket.emit('qr-update', qrActivos.get(uid));
            }
            return; // Evitamos que se creen múltiples instancias
        }

        // Bloqueamos la puerta para que nadie más inicie sesión al mismo tiempo
        inicializandoSesiones.add(uid);
        
        try {
            await connectToWhatsApp(uid);
        } finally {
            // Liberamos la puerta una vez termine el proceso
            inicializandoSesiones.delete(uid);
        }
    });

    socket.on('crm-presencia', async ({ numero, estado, uid }) => {
        if (!uid) return;
        const whatsappSockLocal = sesionesActivas.get(uid);
        if (!whatsappSockLocal) return;
        try {
            const jid = formatearJid(numero);
            await whatsappSockLocal.sendPresenceUpdate(estado, jid);
        } catch (e) {}
    });
});
// =========================================================================
// 4. ENDPOINTS DE CONTROL (BLINDADOS ANTI-BANEO Y SOPORTE DE GRUPOS / LIDS)
// =========================================================================
app.get('/status', (req, res) => {
    const { uid } = req.query;
    if (!uid) return res.status(401).json({ status: "disconnected" });
    
    const sock = sesionesActivas.get(uid);
    if (sock && sock.user) {
        return res.json({ status: "connected", user: sock.user });
    }
    res.json({ status: "disconnected", qr: qrActivos.get(uid) });
});



const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms));


// =========================================================================
// 🚀 ENDPOINTS DE GESTIÓN DE CONTACTOS Y COMUNIDADES
// =========================================================================

app.get('/api/contactos', async (req, res) => {
    try {
        const { uid } = req.query;
        if (!uid) return res.status(401).json({ error: "Falta UID" });

        const snapshot = await db.collection('usuarios').doc(uid).collection('crm_contactos').orderBy('ultimaActividad', 'desc').get();
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
        const { nombrePersonalizado, uid } = req.body;
        if (!uid) return res.status(401).json({ error: "Falta UID" });
        
        await db.collection('usuarios').doc(uid).collection('crm_contactos').doc(jid).update({
            nombrePersonalizado: nombrePersonalizado
        });
        
        res.json({ success: true, message: "Nombre actualizado con éxito" });
    } catch (error) {
        res.status(500).json({ error: "Fallo al actualizar el nombre" });
    }
});

// =====================================================================
// 🌐 ENDPOINT MANUAL: BANNER HD CRISTALINO (SIN CDN - RECTÁNGULO PURO)
// =====================================================================
app.post('/send-text', async (req, res) => {
    // 🚀 1. Recibimos el 'uid' del frontend
    const { uid, numero, mensaje, linkData } = req.body; 
    
    // 🚀 2. Extraemos el "teléfono" específico de este usuario
    const whatsappSockLocal = sesionesActivas.get(uid);

    if (!whatsappSockLocal) {
        return res.status(401).json({ error: "Tu sesión de WhatsApp no está activa o el QR no ha sido escaneado." });
    }

    try {
        const mensajeFinal = procesarSpintax(mensaje);
        const jidReal = formatearJid(numero);

        if (linkData && linkData.url) {
            // Nota: Luego adaptaremos enviarTarjetaEnlace para que reciba 'whatsappSockLocal'
            await enviarTarjetaEnlace(jidReal, mensajeFinal, linkData, whatsappSockLocal);
        } else {
            const urlRegex = /(https?:\/\/[^\s]+|www\.[^\s]+|[a-zA-Z0-9-]+\.[a-zA-Z]{2,}(?:\/[^\s]*)?)/g;
            const urls = mensajeFinal.match(urlRegex);

            if (urls && urls.length > 0) {
                const linkDetectado = urls[0];
                const linkDataInfo = await extraerMetadatos(linkDetectado);
                await enviarTarjetaEnlace(jidReal, mensajeFinal, linkDataInfo, whatsappSockLocal);
            } else {
                // 🚀 3. Usamos la instancia aislada del usuario
                await whatsappSockLocal.sendMessage(jidReal, { text: mensajeFinal });
            }
        }

        // Nota: Luego adaptaremos guardarMensajeBD para que reciba el 'uid'
        await guardarMensajeBD(uid, numero, "TrueWin", mensajeFinal, 'out');
        
        // 🚀 4. EMISIÓN PRIVADA CON EL PAYLOAD ORIGINAL INTACTO
        io.to(uid).emit('nuevo-mensaje', { 
            numero: identificador, 
            nombre: nombrePerfil, 
            texto: texto, 
            hora: new Date().toISOString(),
            remitente: remitenteEspecifico,
            mediaUrl: mediaUrl,
            mediaType: mediaType,
            tipo: tipoMensaje 
        });

        res.json({ success: true });
    } catch (error) {
        console.error("Fallo al enviar texto manual:", error);
        res.status(500).json({ error: "Fallo al enviar texto" });
    }
});

app.post('/send-image', async (req, res) => {
    const { numero, urlImagen, caption } = req.body;
    if (!whatsappSock) return res.status(500).json({ error: "WhatsApp no inicializado." });
    try {
        const jid = formatearJid(numero);
        const captionFinal = procesarSpintax(caption);

        await whatsappSock.sendPresenceUpdate('composing', jid);
        await delay(Math.floor(Math.random() * 1500) + 2000); 

        // 🚀 DESCARGA EN RAM: Garantiza que la imagen no llegue rota
        const resMedia = await fetch(urlImagen);
        const bufferMedia = Buffer.from(await resMedia.arrayBuffer());

        await whatsappSock.sendMessage(jid, { image: bufferMedia, caption: captionFinal });
        await whatsappSock.sendPresenceUpdate('paused', jid);

        await guardarMensajeBD(numero, "TrueWin", captionFinal || "", 'out', null, urlImagen, 'image');
        
        io.emit('nuevo-mensaje', { 
            numero: jid, nombre: "TrueWin", texto: captionFinal || "", 
            hora: new Date().toISOString(), timestamp: Date.now(),
            remitente: null, mediaUrl: urlImagen, mediaType: 'image', tipo: 'out'
        });

        res.json({ success: true });
    } catch (error) { res.status(500).json({ error: error.message }); }
});

// =====================================================================
// 🚀 ENDPOINT DE VIDEO PARCHEADO (Descarga en RAM + Forzado de MP4)
// =====================================================================
app.post('/send-video', async (req, res) => {
    const { numero, urlVideo, caption } = req.body;
    if (!whatsappSock) return res.status(500).json({ error: "WhatsApp no inicializado." });
    try {
        const jid = formatearJid(numero);
        const captionFinal = procesarSpintax(caption);

        await whatsappSock.sendPresenceUpdate('composing', jid);
        await delay(Math.floor(Math.random() * 2000) + 3000); 

        console.log(`[API Video] Descargando video a RAM para envío seguro: ${urlVideo}`);
        
        // 🚀 EL TRUCO QUE SALVÓ LAS AUTOMATIZACIONES:
        // Descargamos el video y lo forzamos a empacarse como MP4
        const resMedia = await fetch(urlVideo);
        const bufferMedia = Buffer.from(await resMedia.arrayBuffer());

        await whatsappSock.sendMessage(jid, { 
            video: bufferMedia, 
            caption: captionFinal,
            mimetype: 'video/mp4' // Sello obligatorio para evitar la imagen rota
        });
        
        await whatsappSock.sendPresenceUpdate('paused', jid);

        await guardarMensajeBD(numero, "TrueWin", captionFinal || "[Video enviado]", 'out', null, urlVideo, 'video');
        
        io.emit('nuevo-mensaje', { 
            numero: jid, nombre: "TrueWin", texto: captionFinal || "[Video enviado]", 
            hora: new Date().toISOString(), timestamp: Date.now(),
            remitente: null, mediaUrl: urlVideo, mediaType: 'video', tipo: 'out'
        });

        res.json({ success: true });
    } catch (error) { 
        console.error("[API Video] Fallo al procesar archivo:", error);
        res.status(500).json({ error: error.message }); 
    }
});

app.post('/send-audio', async (req, res) => {
    const { numero, urlAudio } = req.body;
    if (!whatsappSock) return res.status(500).json({ error: "WhatsApp no inicializado." });
    try {
        const jid = formatearJid(numero);
        await whatsappSock.sendPresenceUpdate('recording', jid);
        await delay(4000); 
        
        const esMP3 = urlAudio.toLowerCase().includes('.mp3');
        await whatsappSock.sendMessage(jid, { 
            audio: { url: urlAudio }, mimetype: esMP3 ? 'audio/mpeg' : 'audio/ogg; codecs=opus', ptt: !esMP3 
        });
        await whatsappSock.sendPresenceUpdate('paused', jid);

        await guardarMensajeBD(numero, "TrueWin", "[Nota de voz enviada]", 'out', null, urlAudio, 'audio');
        
        // 🚀 EMISIÓN FALTANTE PARA DIBUJAR LA BURBUJA EN VIVO
        io.emit('nuevo-mensaje', { 
            numero: jid, nombre: "TrueWin", texto: "[Nota de voz enviada]", 
            hora: new Date().toISOString(), timestamp: Date.now(),
            remitente: null, mediaUrl: urlAudio, mediaType: 'audio', tipo: 'out'
        });

        res.json({ success: true });
    } catch (error) { res.status(500).json({ error: error.message }); }
});


app.get('/api/historial', async (req, res) => {
    try {
        const { uid } = req.query; // 🚀 Recibimos quién pregunta
        if (!uid) return res.status(401).json({ error: "Falta UID" });

        // Buscamos solo en la bóveda de este UID
        const snapshot = await db.collection('usuarios').doc(uid).collection('crm_mensajes').orderBy('timestamp', 'asc').get();
        
        let todosLosMensajes = [];
        snapshot.forEach(doc => todosLosMensajes.push(doc.data()));
        
        const historial = {};
        const nombres = {};
        
        todosLosMensajes.forEach(data => {
            if (!historial[data.numero]) historial[data.numero] = [];
            
            let chatArray = historial[data.numero];
            let ultimoMensaje = chatArray.length > 0 ? chatArray[chatArray.length - 1] : null;

            // 🌟 LÓGICA DE COLLAGE
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
        // 🌟 PAUSA DE CAMUFLAJE: Esperamos 350ms aleatorios para que Meta 
        // no detecte que las peticiones se hacen en ráfaga automática desde el CRM
        await new Promise(resolve => setTimeout(resolve, Math.floor(Math.random() * 200) + 200));

        const urlFoto = await whatsappSock.profilePictureUrl(jid, 'image');
        
        console.log(`[Foto Perfil 🟢] Éxito al resolver JID: ${jid}`);
        return res.json({ url: urlFoto });

    } catch (e) {
        // 🌟 AQUÍ VERÁS EL DIAGNÓSTICO EXACTO EN LA CONSOLA DE RENDER
        console.error(`[Foto Perfil 🚨] Error exacto para JID ${jid}:`, {
            mensaje: e.message,
            codigo: e.statusCode || e.output?.statusCode || 'Sin código',
            stack: e.stack ? e.stack.split('\n')[1].trim() : ''
        });

        // Devolvemos null limpiamente al frontend para que no colapse la interfaz
        return res.json({ url: null }); 
    }
});

// =====================================================================
// 🤖 ENDPOINTS PARA EL MOTOR DE AUTOMATIZACIONES
// =====================================================================

app.get('/api/automatizaciones', async (req, res) => {
    try {
        const { uid } = req.query;
        if (!uid) return res.status(401).json({ error: "Falta UID" });

        const snapshot = await db.collection('usuarios').doc(uid).collection('crm_automatizaciones').get();
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
        const { uid } = data; // 🚀 El frontend debe inyectarlo en el JSON
        if (!uid) return res.status(401).json({ error: "Falta UID" });

        await db.collection('usuarios').doc(uid).collection('crm_automatizaciones').doc(data.id).set(data);
        res.json({ success: true });
    } catch (error) {
        res.status(500).json({ error: "Fallo al guardar automatización" });
    }
});

app.delete('/api/automatizaciones/:id', async (req, res) => {
    try {
        const { id } = req.params;
        const { uid } = req.query;
        if (!uid) return res.status(401).json({ error: "Falta UID" });

        await db.collection('usuarios').doc(uid).collection('crm_automatizaciones').doc(id).delete();
        res.json({ success: true });
    } catch (error) {
        res.status(500).json({ error: "Fallo al eliminar automatización" });
    }
});


app.get('/api/config/automatizaciones', async (req, res) => {
    try {
        const { uid } = req.query;
        if (!uid) return res.status(401).json({ error: "Falta UID" });

        const doc = await db.collection('usuarios').doc(uid).collection('crm_config').doc('automatizaciones').get();
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
        const { activo, uid } = req.body;
        if (!uid) return res.status(401).json({ error: "Falta UID" });

        await db.collection('usuarios').doc(uid).collection('crm_config').doc('automatizaciones').set({ activo });
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
        const { uid } = req.query;
        if (!uid) return res.status(401).json({ error: "Falta UID" });

        const snapshot = await db.collection('usuarios').doc(uid).collection('crm_plantillas').get();
        const plantillas = [];
        snapshot.forEach(doc => plantillas.push({ id: doc.id, ...doc.data() }));
        res.json(plantillas);
    } catch (error) { res.status(500).json({ error: "Fallo al obtener plantillas" }); }
});

app.post('/api/plantillas', async (req, res) => {
    try {
        const { uid, id, nombre, secuencia } = req.body;
        if (!uid) return res.status(401).json({ error: "Falta UID" });

        await db.collection('usuarios').doc(uid).collection('crm_plantillas').doc(id).set({
            nombre: nombre, secuencia: secuencia, timestamp: Date.now()
        });
        res.json({ success: true, id: id });
    } catch (error) { res.status(500).json({ error: "Fallo al guardar plantilla" }); }
});

app.delete('/api/plantillas/:id', async (req, res) => {
    try {
        const { id } = req.params;
        const { uid } = req.query; // 🚀 Atrapamos el UID que viene en la URL
        if (!uid) return res.status(401).json({ error: "Falta UID" });

        // 🚀 Eliminamos el documento directamente de la bóveda del usuario
        await db.collection('usuarios').doc(uid).collection('crm_plantillas').doc(id).delete();
        res.json({ success: true });
    } catch (error) {
        res.status(500).json({ error: "Fallo al eliminar plantilla" });
    }
});

// =========================================================================
// 🚀 FÁBRICA DE TARJETAS ORGÁNICAS (Resolución Nativa + Entrega Blindada)
// =========================================================================
async function enviarTarjetaEnlace(jidReal, mensajeFinal, linkData) {
    let thumbnailBuffer = null;
    let finalWidth = 0;
    let finalHeight = 0;

    // Estructuración del cuerpo del texto
    let textoVisible = mensajeFinal || "";
    if (linkData && linkData.url && !textoVisible.includes(linkData.url)) {
        textoVisible = textoVisible ? `${textoVisible}\n\n🌐 ${linkData.url}` : linkData.url;
    }

    if (linkData && linkData.imageUrl) {
        try {
            console.log(`[Tarjeta Orgánica] Analizando imagen por defecto: ${linkData.imageUrl}`);
            const resImagen = await fetch(linkData.imageUrl, { headers: { 'User-Agent': 'Mozilla/5.0' } });
            
            if (resImagen.ok) {
                const originalBuffer = Buffer.from(await resImagen.arrayBuffer());
                const sharp = require('sharp');
                
                // 1. 🌟 LECTURA DE METADATOS: Extraemos la resolución real por defecto de la imagen
                const metadata = await sharp(originalBuffer).metadata();
                let originalWidth = metadata.width || 800;
                let originalHeight = metadata.height || 418;
                
                // 2. ESCALADO PROPORCIONAL INTELIGENTE (No deforma, no estira forzadamente)
                // Si la imagen es gigante, la reducimos manteniendo su aspecto original exacto
                if (originalWidth > 800) {
                    originalHeight = Math.round((800 / originalWidth) * originalHeight);
                    originalWidth = 800;
                }
                
                finalWidth = originalWidth;
                finalHeight = originalHeight;
                let calidad = 80;

                // Renderizamos respetando el tamaño y proporciones nativas de la web
                thumbnailBuffer = await sharp(originalBuffer)
                    .resize({ width: finalWidth, height: finalHeight, fit: 'inside' })
                    .jpeg({ quality: calidad })
                    .toBuffer();

                // 3. 🛡️ FILTRO DE PESO STRICTO ANTI-BLOQUEO
                // Mantener el búfer debajo de 40KB es lo que asegura que el servidor de Meta 
                // no clasifique el paquete como corrupto y se lo entregue al receptor de inmediato.
                while (thumbnailBuffer.length > 40000 && calidad > 10) {
                    calidad -= 5;
                    thumbnailBuffer = await sharp(originalBuffer)
                        .resize({ width: finalWidth, height: finalHeight, fit: 'inside' })
                        .jpeg({ quality: calidad })
                        .toBuffer();
                }
                console.log(`[Tarjeta Orgánica] Procesada con éxito a ${finalWidth}x${finalHeight}. Peso seguro: ${(thumbnailBuffer.length / 1024).toFixed(2)} KB.`);
            }
        } catch (e) {
            console.warn("[Tarjeta Orgánica] Fallo al procesar proporciones nativas:", e.message);
        }
    }

    const { generateWAMessageFromContent } = require('@whiskeysockets/baileys');

    // 4. ENSAMBLAJE PROTOBUF PURO (100% idéntico al comportamiento humano)
    const payloadExtended = {
        text: textoVisible, 
        matchedText: linkData.url,
        canonicalUrl: linkData.url,
        title: linkData.title || "Enlace",
        description: linkData.description || ""
    };

    if (thumbnailBuffer) {
        // Inyectamos el Base64 limpio sin CDN intermediarios
        payloadExtended.jpegThumbnail = thumbnailBuffer;
        
        // Informamos a la aplicación receptora las dimensiones reales de tu imagen
        payloadExtended.thumbnailWidth = finalWidth;
        payloadExtended.thumbnailHeight = finalHeight;
    }

    // Acoplamos el contenido usando el validador estándar de Baileys
    const mensajeProtobuf = generateWAMessageFromContent(jidReal, {
        extendedTextMessage: payloadExtended
    }, { userJid: whatsappSock.user.id });

    // Despachamos el paquete directamente al túnel de mensajes
    await whatsappSock.relayMessage(jidReal, mensajeProtobuf.message, { messageId: mensajeProtobuf.key.id });
    console.log(`[Tarjeta Orgánica] Mensaje transmitido de forma segura al JID: ${jidReal}`);
}


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
            // 🚀 SEPARADOR DE CHIPS EN LA NUBE
            const arrayKeywords = auto.palabraClave.split(',').map(k => k.toLowerCase().trim()).filter(k => k);
            let haceMatch = false;
            let keywordUsada = "";

            // Evaluamos cada chip de forma individual
            for (const kw of arrayKeywords) {
                if (auto.condicion === 'exacta' && textoLimpio === kw) { haceMatch = true; keywordUsada = kw; break; }
                if (auto.condicion === 'contiene' && textoLimpio.includes(kw)) { haceMatch = true; keywordUsada = kw; break; }
            }

            if (haceMatch) {
                
                if (auto.frecuencia === 'unica') {
                    // (Aquí mantienes tu código exacto del idLogUnico, registroDoc, etc...)
                    const idLogUnico = `${auto.id}_${numeroCliente.replace(/[^a-zA-Z0-9]/g, '')}`;
                    const registroDoc = await db.collection('crm_registro_bot').doc(idLogUnico).get();
                    
                    if (registroDoc.exists) {
                        console.log(`[🤖 Bot Protegido] El cliente ya recibió la regla. Omitiendo.`);
                        break; 
                    }
                    
                    await db.collection('crm_registro_bot').doc(idLogUnico).set({
                        idAutomatizacion: auto.id,
                        palabraClave: auto.palabraClave,
                        numeroCliente: numeroCliente,
                        ejecutadoEl: new Date().toISOString()
                    });
                }

                console.log(`[🤖 Bot en Nube] Ejecución autorizada para la variante "${keywordUsada}". Despachando secuencia...`);
                
                // 🚀 CANDADO ANTI-BAN 2: Pausa biológica antes de clavar el visto
                const tiempoLecturaHumana = Math.floor(Math.random() * (3500 - 1500 + 1)) + 1500;
                
                setTimeout(async () => {
                    if (ultimosMensajesKey[numeroCliente]) {
                        try {
                            await whatsappSock.readMessages([ultimosMensajesKey[numeroCliente]]);
                        } catch (e) { }
                    }
                }, tiempoLecturaHumana);

                // 3. Cargar secuencia y disparar ráfaga asíncrona (esto se mantiene igual)
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

// =========================================================================
// 🚀 RASTREADOR WEB (AUTO-METADATOS PARA EL CRM)
// =========================================================================
async function extraerMetadatos(urlStr) {
    try {
        let urlFinal = urlStr.startsWith('http') ? urlStr : 'https://' + urlStr;
        
        // Entramos a la web simulando ser un navegador Chrome de PC para que no nos bloqueen
        const res = await fetch(urlFinal, {
            headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)' },
        });
        const html = await res.text();

        // Extraemos las etiquetas SEO oficiales
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

// 🚀 DESPACHADOR ASÍNCRONO EN NUBE: Ejecuta secuencias con pausas humanas anti-ban
async function despacharFlujoDesdeNube(numeroDestino, tpl) {
    const pause = (ms) => new Promise(res => setTimeout(res, ms));
    
    try {
        const tiempoLectura = Math.floor(Math.random() * (2200 - 1200 + 1)) + 1200; 
        await pause(tiempoLectura);
    } catch (e) {}

    for (const msj of tpl.secuencia) {
        try {
            let textoOriginal = msj.texto || "";
            let mUrl = msj.url || null;
            let mType = null;
            const jidReal = formatearJid(numeroDestino);

            // Procesamiento Spintax
            let textoBurbuja = msj.tipo === 'texto' || msj.tipo === 'media' || msj.tipo === 'enlace' ? procesarSpintax(textoOriginal) : textoOriginal;

            // Telemetría humana...
            try {
                if (msj.tipo === 'audio') {
                    await whatsappSock.sendPresenceUpdate('recording', numeroDestino);
                    await pause(4000); 
                } else {
                    await whatsappSock.sendPresenceUpdate('composing', numeroDestino);
                    const caracteres = textoBurbuja ? textoBurbuja.length : 20;
                    let tiempoTipeo = (caracteres * Math.floor(Math.random() * (55 - 25 + 1)) + 25) + Math.floor(Math.random() * (800 - 300 + 1)) + 300;
                    
                    const limiteMinimo = Math.floor(Math.random() * (1900 - 1200 + 1)) + 1200; 
                    const limiteMaximo = Math.floor(Math.random() * (6500 - 4800 + 1)) + 4800; 
                    tiempoTipeo = Math.max(limiteMinimo, Math.min(tiempoTipeo, limiteMaximo));
                    
                    console.log(`[Anti-Ban] Simulando tipeo por ${(tiempoTipeo / 1000).toFixed(2)}s`);
                    await pause(tiempoTipeo);
                }
            } catch (e) { }

            // 🚀 DISPARO CORREGIDO: LECTURA EN RAM Y FORZADO DE FORMATO
            if (msj.tipo === 'texto') {
                const urlRegex = /(https?:\/\/[^\s]+|www\.[^\s]+|[a-zA-Z0-9-]+\.[a-zA-Z]{2,}(?:\/[^\s]*)?)/g;
                const urls = textoBurbuja.match(urlRegex);

                if (urls && urls.length > 0) {
                    const linkDataInfo = await extraerMetadatos(urls[0]);
                    await enviarTarjetaEnlace(jidReal, textoBurbuja, linkDataInfo);
                } else {
                    await whatsappSock.sendMessage(jidReal, { text: textoBurbuja });
                }
            } else if (msj.tipo === 'media' && msj.url) {
                try {
                    console.log(`[Bot Nube] Descargando y analizando archivo: ${msj.url}`);
                    
                    // 1. Descargamos el archivo a la RAM del servidor (Súper rápido y no falla el stream)
                    const resMedia = await fetch(msj.url);
                    const bufferMedia = Buffer.from(await resMedia.arrayBuffer());
                    
                    // 2. Leemos la etiqueta interna (ADN) directa desde Firebase Storage
                    const contentType = resMedia.headers.get('content-type') || '';

                    // 3. Ya no adivinamos. Si es video, forzamos el empaquetado de video.
                    if (contentType.includes('video') || msj.url.toLowerCase().includes('.mp4') || msj.url.toLowerCase().includes('.mov')) {
                        mType = 'video';
                        if (!textoBurbuja) textoBurbuja = "[Video enviado]";
                        
                        // 🚀 OBLIGAMOS a Baileys y a Meta a procesarlo como Video MP4
                        await whatsappSock.sendMessage(jidReal, { 
                            video: bufferMedia, 
                            caption: textoBurbuja, 
                            mimetype: 'video/mp4' 
                        });
                    } else {
                        mType = 'image';
                        if (!textoBurbuja) textoBurbuja = "[Imagen enviada]";
                        
                        await whatsappSock.sendMessage(jidReal, { 
                            image: bufferMedia, 
                            caption: textoBurbuja 
                        });
                    }
                } catch (error) {
                    console.error("[Bot Nube] Error crítico procesando media:", error);
                }
            } else if (msj.tipo === 'audio' && msj.url) {
                mType = 'audio';
                if (!textoBurbuja) textoBurbuja = "[Nota de voz enviada]";
                await whatsappSock.sendMessage(jidReal, { audio: { url: msj.url }, mimetype: 'audio/ogg; codecs=opus', ptt: true });
            }

            try { await whatsappSock.sendPresenceUpdate('paused', numeroDestino); } catch (e) {}

            await guardarMensajeBD(numeroDestino, "TrueWin", textoBurbuja, 'out', null, mUrl, mType);

            io.emit('nuevo-mensaje', { 
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