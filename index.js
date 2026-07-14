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

// 🚀 VARIABLES GLOBALES DE CACHÉ CRIPTOGRÁFICO (El parche del mensaje fantasma)
// Al estar aquí afuera, sobreviven a los reinicios del socket (Código 515)
let cacheCreds = {};
let cacheKeys = {};
let cacheCargada = false;;

const idsEnviadosPorBot = new Set();

// =========================================================================
// 3. CONEXIÓN A WHATSAPP CON CACHÉ EN RAM + LOTES EN FIRESTORE
// =========================================================================
async function connectToWhatsApp() {
    console.log("[TrueWin-Backend] Sincronizando e inicializando sesión remota...");

    // 🚨 (Asegúrate de que aquí adentro YA NO ESTÉN let cacheCreds, let cacheKeys ni let cacheCargada)

    const readState = async () => {
        if (cacheCargada) {
            console.log("[TrueWin] Usando memoria caché rápida (Ignorando base de datos)...");
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

   // =========================================================================
    // 🚀 BLOQUE DE CREDENCIALES CORREGIDO (UNIFICADO Y ANTI-BUCLE 428)
    // =========================================================================
    const sesionFirebase = await readState();

    // Usamos directamente cacheCreds en lugar de la respuesta de Firebase para proteger la RAM
    let credencialesActivas = cacheCreds; 
    
    if (Object.keys(credencialesActivas).length === 0) {
        console.log('[TrueWin] Memoria y DB limpias. Generando credenciales oficiales para pedir QR...');
        credencialesActivas = initAuthCreds();
        cacheCreds = credencialesActivas;
        
        // 🌟 PIEZA MÁGICA: Evita que si hay un micro-corte 428, se borre la RAM y genere llaves infinitas
        cacheCargada = true; 
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

    whatsappSock = makeWASocket({
        auth: state,
        printQRInTerminal: false,
        getMessage: async (key) => {
            return undefined;
        },
        logger: pino({ level: 'silent' }) 
    });

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
            console.log("[TrueWin] Nuevo código QR generado. Enviando a la web...");
            ultimoQR = qr;
            io.emit('qr-update', qr);
        }

        if (connection === 'close') {
            const errorBoom = lastDisconnect?.error instanceof Boom ? lastDisconnect.error : null;
            const codigoError = errorBoom ? errorBoom.output?.statusCode : (lastDisconnect?.error?.output?.statusCode || 500);
            const razonError = errorBoom ? errorBoom.message : (lastDisconnect?.error?.message || 'Error desconocido');
            
            console.log(`[TrueWin] Conexión cerrada. Código extraído: ${codigoError}. Razón: ${razonError}`);

            if (codigoError === 440 || (razonError && razonError.includes('conflict'))) {
                console.error("[🚨 Choque de Instancias] Se detectó otra sesión. Deteniendo reconexión.");
                return; 
            }

            // 🚀 CORRECCIÓN DEFINITIVA: Destrucción por Falla de Conexión / Sesión Inválida (405 y 401)
            if (codigoError === 405 || codigoError === 401) {
            console.error(`[🚨 ALERTA ${codigoError}] Credenciales corruptas. Purgando RAM y Firebase.`);
            io.emit('estado-conexion', 'desconectado'); 
            whatsappSock = null; 
            
            // 1. Limpiamos la RAM
            cacheCreds = {}; 
            cacheKeys = {}; 
            cacheCargada = false;

            // 2. 🌟 EL PARARRAYOS: Borramos el registro corrupto de Firestore
            // Reemplaza esto con tu lógica exacta de guardado (ej. borrar documento o vaciarlo)
            try {
                const db = admin.firestore();
                // Asumiendo una estructura estándar, ajusta tu colección/documento:
                await db.collection('tu_coleccion_sesiones').doc('tu_documento_bot').delete();
                console.log("[Firestore] Registro de sesión corrupto eliminado de la base de datos con éxito.");
            } catch (fsError) {
                console.error("[Firestore] Error al intentar borrar la sesión corrupta:", fsError.message);
            }

            console.log("[TrueWin] Inicializando flujo limpio desde cero en 4 segundos...");
            setTimeout(() => connectToWhatsApp(), 4000);
            return;
        }

            // 🚀 CORRECCIÓN: Destrucción Total ante Baneo (403)
            if (codigoError === 403 || codigoError === DisconnectReason.forbidden) {
                console.error("[🚨 ALERTA 403] Servidores de Meta rechazaron autenticación (Posible Baneo). DETENIENDO.");
                io.emit('estado-conexion', 'desconectado'); 
                whatsappSock = null; 
                
                cacheCreds = {}; 
                cacheKeys = {}; 
                cacheCargada = false;
                return; 
            }

            // 🚀 CORRECCIÓN: Destrucción Total ante Desvinculación de Dispositivo
            if (codigoError === DisconnectReason.loggedOut) {
                console.error("[🚨 Sesión Cerrada] Usuario desvinculó el bot desde el celular. Deteniendo reconexión.");
                io.emit('estado-conexion', 'desconectado'); 
                whatsappSock = null; 
                
                cacheCreds = {}; 
                cacheKeys = {}; 
                cacheCargada = false;
                return; 
            }

            console.log(`[TrueWin] Reiniciando flujo de forma limpia en 3 segundos (Código: ${codigoError})...`);
            
            if (whatsappSock) {
                whatsappSock.ev.removeAllListeners();
            }

            setTimeout(() => connectToWhatsApp(), 3000); 
        }
        
        if (connection === 'open') {
            console.log('[TrueWin] ¡CONEXIÓN GLOBAL ESTABLECIDA CON ÉXITO EN WHATSAPP!');
            ultimoQR = null; 
            io.emit('estado-conexion', 'conectado'); 
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
        let nombrePerfil = msg.pushName || "Usuario"; 
        let remitenteEspecifico = null; 

        if (esGrupo) {
            remitenteEspecifico = msg.pushName || msg.key.participant?.split('@')[0] || "Miembro";
            nombrePerfil = "Grupo de WhatsApp";
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

// =====================================================================
// 🌐 ENDPOINT MANUAL: EL MOTOR HÍBRIDO (SHARP + JIMP) 100% ESTABLE
// =====================================================================
app.post('/send-text', async (req, res) => {
    const { numero, mensaje, linkData } = req.body; 
    if (!whatsappSock) return res.status(500).json({ error: "No conectado" });

    try {
        const mensajeFinal = procesarSpintax(mensaje);
        const jidReal = formatearJid(numero);

        if (linkData) {
            let thumbnailBuffer = null;
            let hqImageMsg = null;

            if (linkData.imageUrl) {
                try {
                    console.log(`[Backend] Motor Híbrido procesando: ${linkData.imageUrl}`);
                    const resImagen = await fetch(linkData.imageUrl, {
                        headers: { 'User-Agent': 'Mozilla/5.0' }
                    });
                    
                    if (resImagen.ok) {
                        const originalBuffer = Buffer.from(await resImagen.arrayBuffer());
                        
                        // 🌟 1. EL MÚSCULO (SHARP): Evita el Error 408 y el colapso del CPU.
                        // Reducimos la foto de Firebase a 800px en milisegundos.
                        const bufferPre = await sharp(originalBuffer)
                            .resize({ width: 800, height: 800, fit: 'inside' })
                            .png() // Lo pasamos como PNG a Jimp para no perder calidad
                            .toBuffer();

                        // 🌟 2. EL TRADUCTOR (JIMP): Garantiza compatibilidad JFIF (Cero Cajas Vacías).
                        // Como la imagen ya es de 800px, Jimp la procesará al instante sin colapsar.
                        const image = await Jimp.read(bufferPre);
                        image.background(0xFFFFFFFF); 
                        
                        // 🌟 3. SUBIDA AL CDN DE META (Forzamos el diseño de Banner Grande)
                        const bufferHQ = await image.getBufferAsync(Jimp.MIME_JPEG);
                        const { prepareWAMessageMedia } = require('@whiskeysockets/baileys');
                        const mediaUpload = await prepareWAMessageMedia(
                            { image: bufferHQ },
                            { upload: whatsappSock.waUploadToServer }
                        );
                        hqImageMsg = mediaUpload.imageMessage;
                        console.log("[Backend] Llaves CDN generadas.");

                        // 🌟 4. LA MINIATURA HD (La cura al pixelado)
                        // Mantenemos la forma geométrica original (sin recortar a cuadrado) 
                        // para que al expandirse no se vea pixelada.
                        image.scaleToFit(500, 500); 
                        let calidad = 65;
                        thumbnailBuffer = await image.getBufferAsync(Jimp.MIME_JPEG);
                        
                        // Bucle estricto < 45KB para evitar el rechazo de red
                        while (thumbnailBuffer.length > 45000 && calidad > 20) {
                            calidad -= 10;
                            image.quality(calidad);
                            thumbnailBuffer = await image.getBufferAsync(Jimp.MIME_JPEG);
                        }
                        console.log(`[Backend] Miniatura Híbrida lista. Peso: ${(thumbnailBuffer.length / 1024).toFixed(2)} KB.`);
                    }
                } catch (e) {
                    console.warn("[Backend] Fallo en motor híbrido. Usando respaldo.", e.message);
                }
            }

            if (!thumbnailBuffer) {
                thumbnailBuffer = Buffer.from("/9j/4AAQSkZJRgABAQEASABIAAD/2wBDAP//////////////////////////////////////////////////////////////////////////////////////wgALCAABAAEBAREA/8QAFBABAAAAAAAAAAAAAAAAAAAAAP/aAAgBAQABPxA=", "base64");
            }

            // =================================================================
            // 🚀 5. ENSAMBLAJE PROTOBUF NATIVO
            // =================================================================
            const { generateWAMessageFromContent } = require('@whiskeysockets/baileys');

            const payloadExtended = {
                text: mensajeFinal,
                matchedText: linkData.url,
                canonicalUrl: linkData.url,
                title: linkData.title,
                description: linkData.description,
                jpegThumbnail: thumbnailBuffer // Formato antiguo JFIF que WhatsApp lee perfecto
            };

            // Inyectamos dimensiones reales para que dibuje la tarjeta panorámica
            if (hqImageMsg) {
                payloadExtended.thumbnailDirectPath = hqImageMsg.directPath;
                payloadExtended.thumbnailSha256 = hqImageMsg.fileSha256;
                payloadExtended.thumbnailEncSha256 = hqImageMsg.fileEncSha256;
                payloadExtended.mediaKey = hqImageMsg.mediaKey;
                payloadExtended.mediaKeyTimestamp = hqImageMsg.mediaKeyTimestamp;
                
                payloadExtended.thumbnailHeight = hqImageMsg.height;
                payloadExtended.thumbnailWidth = hqImageMsg.width;
            }

            const mensajeProtobuf = generateWAMessageFromContent(jidReal, {
                extendedTextMessage: payloadExtended
            }, { userJid: whatsappSock.user.id });

            await whatsappSock.relayMessage(jidReal, mensajeProtobuf.message, { messageId: mensajeProtobuf.key.id });

        } else {
            await whatsappSock.sendMessage(jidReal, { text: mensajeFinal });
        }

        await guardarMensajeBD(numero, "TrueWin", mensajeFinal, 'out');
        res.json({ success: true });
    } catch (error) {
        console.error("Fallo al enviar texto manual con Protobuf:", error);
        res.status(500).json({ error: "Fallo al enviar texto" });
    }
});

app.post('/send-image', async (req, res) => {
    const { numero, urlImagen, caption } = req.body;
    if (!whatsappSock) return res.status(500).json({ error: "WhatsApp no inicializado." });
    try {
        const jid = formatearJid(numero);
        
        // 🌟 PIEZA CORRECTORA: Procesamos las llaves del pie de foto en caliente
        const captionFinal = procesarSpintax(caption);

        await whatsappSock.sendPresenceUpdate('composing', jid);
        await delay(Math.floor(Math.random() * 1500) + 2000); 
        
        // Enviamos a Baileys el caption ya procesado y aleatorio
        await whatsappSock.sendMessage(jid, { image: { url: urlImagen }, caption: captionFinal });
        await whatsappSock.sendPresenceUpdate('paused', jid);

        // Guardamos en la base de datos con el texto real que vió el usuario
        await guardarMensajeBD(numero, "TrueWin", captionFinal || "[Imagen enviada]", 'out', null, urlImagen, 'image');
        res.json({ success: true });
    } catch (error) {
        console.error(`[Error] Fallo enviando imagen a ${numero}:`, error);
        res.status(500).json({ error: error.message });
    }
});

// =====================================================================
// 🚀 ENDPOINT DE VIDEO PARCHEADO CON SOPORTE SPINTAX
// =====================================================================
app.post('/send-video', async (req, res) => {
    const { numero, urlVideo, caption } = req.body;
    if (!whatsappSock) return res.status(500).json({ error: "WhatsApp no inicializado." });
    try {
        const jid = formatearJid(numero);
        
        // 🌟 PIEZA CORRECTORA: Procesamos las llaves del pie de video en caliente
        const captionFinal = procesarSpintax(caption);

        await whatsappSock.sendPresenceUpdate('composing', jid);
        await delay(Math.floor(Math.random() * 2000) + 3000); 
        
        // Enviamos a Baileys el caption ya procesado y aleatorio
        await whatsappSock.sendMessage(jid, { video: { url: urlVideo }, caption: captionFinal });
        await whatsappSock.sendPresenceUpdate('paused', jid);

        // Guardamos en la base de datos con el texto real que vió el usuario
        await guardarMensajeBD(numero, "TrueWin", captionFinal || "[Video enviado]", 'out', null, urlVideo, 'video');
        res.json({ success: true });
    } catch (error) {
        console.error(`[Error] Fallo enviando video a ${numero}:`, error);
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

// 🚀 DESPACHADOR ASÍNCRONO EN NUBE: Ejecuta secuencias con pausas humanas anti-ban
async function despacharFlujoDesdeNube(numeroDestino, tpl) {
    const pause = (ms) => new Promise(res => setTimeout(res, ms));
    
    try {
        // Simulación de lectura inicial humana antes de interactuar
        const tiempoLectura = Math.floor(Math.random() * (2200 - 1200 + 1)) + 1200; 
        await pause(tiempoLectura);
    } catch (e) {}

    for (const msj of tpl.secuencia) {
        try {
            let textoOriginal = msj.texto || "";
            let mUrl = msj.url || null;
            let mType = null;

            // 🚀 PROCESAMIENTO ÚNICO: Aquí unificamos la variable maestra de texto
            let textoBurbuja = msj.tipo === 'texto' || msj.tipo === 'media' ? procesarSpintax(textoOriginal) : textoOriginal;

            if (msj.tipo === 'media' && msj.url) {
                mType = msj.url.includes('.mp4') || msj.url.includes('.mov') ? 'video' : 'image';
                if (!textoBurbuja) textoBurbuja = mType === 'video' ? "[Video enviado]" : "[Imagen enviada]";
            } else if (msj.tipo === 'audio') {
                mType = 'audio';
                textoBurbuja = "[Nota de voz enviada]";
            }

            // Telemetría humana dinámica
            try {
                if (msj.tipo === 'audio') {
                    await whatsappSock.sendPresenceUpdate('recording', numeroDestino);
                    await pause(4000); 
                } else {
                    await whatsappSock.sendPresenceUpdate('composing', numeroDestino);
                    
                    // Algoritmo de tipeo realista según el largo del texto real
                    const caracteres = textoBurbuja ? textoBurbuja.length : 20;
                    
                    // 1. Velocidad variable: un humano en celular tarda entre 25ms y 55ms por letra
                    const velocidadPorLetra = Math.floor(Math.random() * (55 - 25 + 1)) + 25; 
                    
                    // 2. Tiempo de reacción: pausa inicial aleatoria antes de empezar a teclear (entre 300ms y 800ms)
                    const tiempoReaccion = Math.floor(Math.random() * (800 - 300 + 1)) + 300;
                    
                    let tiempoTipeo = (caracteres * velocidadPorLetra) + tiempoReaccion;
                    
                    // 3. Límites Dinámicos (Ya no son siempre 1.5s o 4.5s exactos)
                    const limiteMinimo = Math.floor(Math.random() * (1900 - 1200 + 1)) + 1200; // Mínimo entre 1.2s y 1.9s
                    const limiteMaximo = Math.floor(Math.random() * (6500 - 4800 + 1)) + 4800; // Máximo entre 4.8s y 6.5s
                    
                    if (tiempoTipeo < limiteMinimo) tiempoTipeo = limiteMinimo;
                    if (tiempoTipeo > limiteMaximo) tiempoTipeo = limiteMaximo;
                    
                    // Redondeamos para que los logs se vean limpios
                    tiempoTipeo = Math.floor(tiempoTipeo);
                    
                    console.log(`[Anti-Ban] Simulando tipeo por ${(tiempoTipeo / 1000).toFixed(2)}s para un mensaje de ${caracteres} letras.`);
                    await pause(tiempoTipeo);
                }
            } catch (e) { 
                console.warn("No se pudo actualizar la telemetría de presencia:", e.message); 
            }

            // 🚀 DISPARO CORREGIDO: Forzamos a Baileys a usar 'textoBurbuja' obligatoriamente
            if (msj.tipo === 'texto') {
                await whatsappSock.sendMessage(numeroDestino, { text: textoBurbuja });
            } else if (msj.tipo === 'media' && msj.url) {
                if (mType === 'video') {
                    await whatsappSock.sendMessage(numeroDestino, { video: { url: msj.url }, caption: textoBurbuja });
                } else {
                    await whatsappSock.sendMessage(numeroDestino, { image: { url: msj.url }, caption: textoBurbuja });
                }
            } else if (msj.tipo === 'audio' && msj.url) {
                await whatsappSock.sendMessage(numeroDestino, { audio: { url: msj.url }, mimetype: 'audio/ogg; codecs=opus', ptt: true });
            }

            // Apagamos estado de presencia
            try { await whatsappSock.sendPresenceUpdate('paused', numeroDestino); } catch (e) {}

            // Guardamos en el historial de Firestore usando el texto real despachado
            await guardarMensajeBD(numeroDestino, "TrueWin", textoBurbuja, 'out', null, mUrl, mType);

            // Emitimos por WebSockets al CRM visual
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

            // Delay de separación orgánico entre piezas de la secuencia
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