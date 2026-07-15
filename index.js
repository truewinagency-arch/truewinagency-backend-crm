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

// 🚀 GESTOR DE CONTACTOS: Guarda, nombra y extrae metadatos de Grupos/Comunidades
async function registrarContactoInteligente(jid, pushName, esGrupo) {
    if (!whatsappSock || jid.includes('status@broadcast')) return;

    try {
        const docRef = db.collection('crm_contactos').doc(jid);
        const doc = await docRef.get();

        // Si ya existe en la base de datos, solo actualizamos su última actividad
        if (doc.exists) {
            await docRef.update({ ultimaActividad: Date.now() });
            return;
        }

        // Si es un contacto nuevo, extraemos toda su identidad
        let nombreOficial = pushName || "Usuario Desconocido";
        let fotoUrl = null;
        let tipoEntidad = esGrupo ? 'grupo' : 'persona';

        if (esGrupo) {
            try {
                // 🌟 MAGIA: Extraemos el nombre real del grupo y su descripción
                const metadata = await whatsappSock.groupMetadata(jid);
                nombreOficial = metadata.subject || nombreOficial;
                
                // Detectamos si es un canal de avisos de comunidad (Announce)
                if (metadata.announce) {
                    tipoEntidad = 'comunidad_avisos';
                }
            } catch (e) {
                console.warn(`[Contactos] No se pudo obtener metadata del grupo ${jid}`);
            }
        }

        // Intentamos sacar la foto de perfil en segundo plano
        try {
            fotoUrl = await whatsappSock.profilePictureUrl(jid, 'image');
        } catch (e) { /* Si no tiene foto, se queda en null */ }

        // Guardamos el perfil maestro en Firestore
        await docRef.set({
            jid: jid,
            nombreOriginal: nombreOficial,
            nombrePersonalizado: "", // 🌟 Campo vacío listo para que lo edites en tu panel
            tipo: tipoEntidad,
            fotoPerfil: fotoUrl,
            creadoEl: new Date().toISOString(),
            ultimaActividad: Date.now()
        });

        console.log(`[Contactos] Nuevo perfil registrado: ${nombreOficial} (${tipoEntidad})`);
    } catch (error) {
        console.error("[Contactos] Error al registrar entidad:", error);
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

    // =========================================================================
    // 🚀 EXTRACCIÓN DE VERSIÓN OFICIAL Y CONFIGURACIÓN DEL SOCKET
    // =========================================================================
    const { fetchLatestWaWebVersion, Browsers } = require('@whiskeysockets/baileys');
    let versionWaWeb = [2, 3000, 1015901307]; // Versión de respaldo estática
    
    try {
        // Obtenemos la última versión de los servidores de Meta para evitar el Error 405
        const { version, isLatest } = await fetchLatestWaWebVersion();
        versionWaWeb = version;
        console.log(`[TrueWin] Conectando con versión WA Web oficial: ${version.join('.')} (¿Es la última?: ${isLatest})`);
    } catch (vError) {
        console.warn("[TrueWin] No se pudo obtener la versión en vivo de Meta, usando respaldo seguro.");
    }

    whatsappSock = makeWASocket({
        auth: state,
        printQRInTerminal: false,
        version: versionWaWeb, // 🌟 CLAVE: Le dice a Meta que somos un cliente moderno actualizado
        browser: Browsers.ubuntu('Chrome'), // 🌟 CLAVE: Nos camufla como un navegador Linux legítimo
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
                console.log("[Firestore] Ejecutando purga total de la sesión corrupta...");
                
                // Usamos 'coleccionSesion' y 'db' que ya están definidos globalmente en tu index.js
                const snapshot = await coleccionSesion.get();
                
                if (!snapshot.empty) {
                    const batch = db.batch(); // Preparamos un borrado masivo
                    snapshot.docs.forEach((doc) => {
                        batch.delete(doc.ref);
                    });
                    
                    await batch.commit(); // Ejecutamos el borrado de golpe
                    console.log("[Firestore] Sesión antigua eliminada de la base de datos con éxito.");
                } else {
                    console.log("[Firestore] La colección ya estaba limpia.");
                }
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


// =========================================================================
// 🚀 ENDPOINTS DE GESTIÓN DE CONTACTOS Y COMUNIDADES
// =========================================================================

// Obtener toda la lista de contactos para la barra lateral de tu chat
app.get('/api/contactos', async (req, res) => {
    try {
        const snapshot = await db.collection('crm_contactos').orderBy('ultimaActividad', 'desc').get();
        let contactos = [];
        snapshot.forEach(doc => contactos.push(doc.data()));
        res.json(contactos);
    } catch (error) {
        res.status(500).json({ error: "Fallo al obtener contactos" });
    }
});

// Cambiar el nombre a un cliente o comunidad de forma personalizada
app.put('/api/contactos/:jid', async (req, res) => {
    try {
        const { jid } = req.params;
        const { nombrePersonalizado } = req.body;
        
        await db.collection('crm_contactos').doc(jid).update({
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
    const { numero, mensaje, linkData } = req.body; 
    if (!whatsappSock) return res.status(500).json({ error: "No conectado" });

    try {
        const mensajeFinal = procesarSpintax(mensaje);
        const jidReal = formatearJid(numero);

        // 1. Si tu frontend llegara a enviar linkData explícito (por si lo actualizas a futuro)
        if (linkData && linkData.url) {
            await enviarTarjetaEnlace(jidReal, mensajeFinal, linkData);
        } else {
            // 2. 🚀 EL AUTO-RASTREADOR PARA ENVÍOS MANUALES
            // Escaneamos el texto que escribiste en el CRM buscando enlaces
            const urlRegex = /(https?:\/\/[^\s]+|www\.[^\s]+|[a-zA-Z0-9-]+\.[a-zA-Z]{2,}(?:\/[^\s]*)?)/g;
            const urls = mensajeFinal.match(urlRegex);

            if (urls && urls.length > 0) {
                const linkDetectado = urls[0];
                console.log(`[Envío Manual] Link detectado en el texto: ${linkDetectado}. Extrayendo info...`);
                
                // Usamos el scraper que construimos para extraer la portada oficial
                const linkDataInfo = await extraerMetadatos(linkDetectado);
                
                // Despachamos a la fábrica de tarjetas
                await enviarTarjetaEnlace(jidReal, mensajeFinal, linkDataInfo);
            } else {
                // 3. Texto puro sin enlaces (se envía normal)
                await whatsappSock.sendMessage(jidReal, { text: mensajeFinal });
            }
        }

        await guardarMensajeBD(numero, "TrueWin", mensajeFinal, 'out');
        res.json({ success: true });
    } catch (error) {
        console.error("Fallo al enviar texto manual con auto-tarjeta:", error);
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

        const snapshot = await db.collection('crm_mensajes').where('host', '==', hostActivo).get();
        let todosLosMensajes = [];
        snapshot.forEach(doc => todosLosMensajes.push(doc.data()));
        
        todosLosMensajes.sort((a, b) => a.timestamp - b.timestamp);

        const historial = {};
        const nombres = {};
        
        todosLosMensajes.forEach(data => {
            if (!historial[data.numero]) historial[data.numero] = [];
            
            let chatArray = historial[data.numero];
            let ultimoMensaje = chatArray.length > 0 ? chatArray[chatArray.length - 1] : null;

            // 🌟 LÓGICA DE COLLAGE:
            // Si el último mensaje es del mismo tipo (in/out), es una imagen,
            // el mensaje actual también es imagen, y se enviaron con menos de 60 segundos de diferencia...
            if (
                ultimoMensaje && 
                ultimoMensaje.tipo === data.tipo &&
                ultimoMensaje.mediaType === 'image' && 
                data.mediaType === 'image' &&
                (data.timestamp - ultimoMensaje.timestamp) < 60000 // 60 segundos
            ) {
                // Transformamos el mensaje anterior en un Collage (Array de URLs)
                if (!ultimoMensaje.esCollage) {
                    ultimoMensaje.esCollage = true;
                    ultimoMensaje.mediaUrls = [ultimoMensaje.mediaUrl];
                }
                // Añadimos la nueva imagen al paquete
                ultimoMensaje.mediaUrls.push(data.mediaUrl);
                
                // Concatenamos el texto si hay varios pies de foto (opcional)
                if (data.texto && data.texto !== "[Archivo o mensaje interactivo]") {
                    ultimoMensaje.texto = ultimoMensaje.texto + "\n" + data.texto;
                }
            } else {
                // Es un mensaje normal, lo añadimos directamente
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

// =========================================================================
// 🚀 FÁBRICA DE TARJETAS HD (SIN BORDES BLANCOS)
// =========================================================================
async function enviarTarjetaEnlace(jidReal, mensajeFinal, linkData) {
    let thumbnailBuffer = null;

    let textoVisible = mensajeFinal || "";
    if (linkData && linkData.url && !textoVisible.includes(linkData.url)) {
        textoVisible = textoVisible ? `${textoVisible}\n\n🌐 ${linkData.url}` : linkData.url;
    }

    if (linkData && linkData.imageUrl) {
        try {
            console.log(`[Tarjeta] Procesando miniatura para: ${linkData.imageUrl}`);
            const resImagen = await fetch(linkData.imageUrl, { headers: { 'User-Agent': 'Mozilla/5.0' } });
            
            if (resImagen.ok) {
                const originalBuffer = Buffer.from(await resImagen.arrayBuffer());
                let calidad = 75;
                
                thumbnailBuffer = await sharp(originalBuffer)
                    .resize({ width: 300, withoutEnlargement: true })
                    .jpeg({ quality: calidad })
                    .toBuffer();

                while (thumbnailBuffer.length > 14000 && calidad > 10) {
                    calidad -= 10;
                    thumbnailBuffer = await sharp(originalBuffer)
                        .resize({ width: 300, withoutEnlargement: true })
                        .jpeg({ quality: calidad })
                        .toBuffer();
                }
                console.log(`[Tarjeta] Miniatura validada. Peso: ${(thumbnailBuffer.length / 1024).toFixed(2)} KB.`);
            }
        } catch (e) {
            console.warn("[Tarjeta] Fallo al generar miniatura:", e.message);
        }
    }

    const { generateWAMessageFromContent } = require('@whiskeysockets/baileys');

    // Ensamblamos los datos obligatorios
    const payloadExtended = {
        text: textoVisible, 
        matchedText: linkData.url,
        canonicalUrl: linkData.url,
        title: linkData.title || "Enlace",
        description: linkData.description || ""
    };

    // Solo inyectamos la imagen si se generó bien (evita que WhatsApp borre la tarjeta)
    if (thumbnailBuffer) {
        payloadExtended.jpegThumbnail = thumbnailBuffer;
    }

    const mensajeProtobuf = generateWAMessageFromContent(jidReal, {
        extendedTextMessage: payloadExtended
    }, { userJid: whatsappSock.user.id });

    await whatsappSock.relayMessage(jidReal, mensajeProtobuf.message, { messageId: mensajeProtobuf.key.id });
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

            // Procesamiento Spintax
            let textoBurbuja = msj.tipo === 'texto' || msj.tipo === 'media' || msj.tipo === 'enlace' ? procesarSpintax(textoOriginal) : textoOriginal;

            if (msj.tipo === 'media' && msj.url) {
                mType = msj.url.includes('.mp4') || msj.url.includes('.mov') ? 'video' : 'image';
                if (!textoBurbuja) textoBurbuja = mType === 'video' ? "[Video enviado]" : "[Imagen enviada]";
            } else if (msj.tipo === 'audio') {
                mType = 'audio';
                textoBurbuja = "[Nota de voz enviada]";
            } else if (msj.tipo === 'enlace') {
                mType = 'link'; 
                // Extraemos la URL principal para guardarla en el historial
                if (msj.linkData && msj.linkData.url) mUrl = msj.linkData.url; 
            }

            // 🚀 TELEMETRÍA HUMANA (Se mantiene idéntica)
            try {
                if (msj.tipo === 'audio') {
                    await whatsappSock.sendPresenceUpdate('recording', numeroDestino);
                    await pause(4000); 
                } else {
                    await whatsappSock.sendPresenceUpdate('composing', numeroDestino);
                    const caracteres = textoBurbuja ? textoBurbuja.length : 20;
                    const velocidadPorLetra = Math.floor(Math.random() * (55 - 25 + 1)) + 25; 
                    const tiempoReaccion = Math.floor(Math.random() * (800 - 300 + 1)) + 300;
                    let tiempoTipeo = (caracteres * velocidadPorLetra) + tiempoReaccion;
                    
                    const limiteMinimo = Math.floor(Math.random() * (1900 - 1200 + 1)) + 1200; 
                    const limiteMaximo = Math.floor(Math.random() * (6500 - 4800 + 1)) + 4800; 
                    
                    if (tiempoTipeo < limiteMinimo) tiempoTipeo = limiteMinimo;
                    if (tiempoTipeo > limiteMaximo) tiempoTipeo = limiteMaximo;
                    
                    tiempoTipeo = Math.floor(tiempoTipeo);
                    console.log(`[Anti-Ban] Simulando tipeo por ${(tiempoTipeo / 1000).toFixed(2)}s para un mensaje de ${caracteres} letras.`);
                    await pause(tiempoTipeo);
                }
            } catch (e) { }

            // 🚀 DISPARO CORREGIDO: Integración del Motor de Tarjetas
           if (msj.tipo === 'texto') {
                // Escaneamos si el texto puro tiene un link (ej: bingowin.app o https://...)
                const urlRegex = /(https?:\/\/[^\s]+|www\.[^\s]+|[a-zA-Z0-9-]+\.[a-zA-Z]{2,}(?:\/[^\s]*)?)/g;
                const urls = textoBurbuja.match(urlRegex);

                if (urls && urls.length > 0) {
                    const linkDetectado = urls[0];
                    console.log(`[Bot Nube] Link detectado en el texto: ${linkDetectado}. Extrayendo info...`);
                    
                    // 1. Extraemos los datos de la web automáticamente
                    const linkDataInfo = await extraerMetadatos(linkDetectado);
                    
                    // 2. Mandamos la tarjeta usando la Fábrica manual que ya sabemos que funciona
                    await enviarTarjetaEnlace(numeroDestino, textoBurbuja, linkDataInfo);
                } else {
                    // Si es un texto sin enlaces, enviamos normal
                    await whatsappSock.sendMessage(numeroDestino, { text: textoBurbuja });
                }

            // Los demás tipos de mensajes se quedan idénticos
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

            // Guardamos en el historial
            await guardarMensajeBD(numeroDestino, "TrueWin", textoBurbuja, 'out', null, mUrl, mType);

            // Emitimos por WebSockets al CRM
            io.emit('nuevo-mensaje', { 
            numero: identificador, 
            nombre: nombrePerfil, 
            texto: texto, 
            hora: new Date().toISOString(),
            timestamp: Date.now(), // 🚀 NUEVO: Vital para que la lógica de collage en vivo no falle
            remitente: remitenteEspecifico,
            mediaUrl: mediaUrl,
            mediaType: mediaType,
            tipo: tipoMensaje 
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