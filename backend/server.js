/**
 * ==========================================================
 *  My Voice Chat - Backend server
 * ==========================================================
 * สรุปสถาปัตยกรรม (อัปเดตล่าสุดหลังทดสอบจริงแล้วใช้งานได้):
 *
 * เซิร์ฟเวอร์นี้เปิด 2 พอร์ตแยกกัน เพราะ Minecraft (อย่างน้อยในหลาย
 * เครื่อง/เวอร์ชันที่ทดสอบ) เชื่อมต่อแบบเข้ารหัส "wss://" ไม่ได้
 * ใช้ได้แค่ "ws://" ธรรมดา ในขณะที่เว็บไซต์/เบราว์เซอร์ต้องใช้ wss
 * เสมอ (เพราะเว็บเสิร์ฟผ่าน https) เลยต้องแยกกันชัดเจน:
 *
 * 1) พอร์ตหลัก (env: PORT) -> เว็บไซต์ + WebSocket ฝั่งเว็บ path "/site"
 *    ต้อง deploy บนโฮสต์ที่มี HTTPS ให้อัตโนมัติ (เช่น Railway ให้ผ่าน
 *    domain แบบ Generate Domain) เกมไม่ได้ยุ่งกับพอร์ตนี้เลย
 *
 * 2) พอร์ตที่สอง (env: MC_PORT, ค่าเริ่มต้น 3002) -> เฉพาะ Minecraft
 *    เท่านั้น เป็น ws ธรรมดา ไม่มี TLS ต้อง deploy บนโฮสต์ที่เปิด
 *    "TCP Proxy" แบบดิบให้คนนอกต่อเข้ามาได้ตรง ๆ (Render ทำไม่ได้
 *    เพราะบังคับ TLS ทุกการเชื่อมต่อ แต่ Railway.app ทำได้ผ่านฟีเจอร์
 *    Settings -> Network -> "+ TCP Proxy" โดยตั้ง Target Port ให้
 *    ตรงกับค่า MC_PORT นี้)
 *
 * ในเกมต้องพิมพ์ (ใช้ /wsserver หรือ /connect ก็ได้ ชื่อคำสั่งเดียวกัน):
 *    /wsserver ws://<โดเมน-TCP-Proxy-จาก-Railway>:<พอร์ตที่ได้มา>
 * (ห้ามใช้ wss:// และห้ามใช้โดเมนเว็บไซต์หลัก ต้องเป็นที่อยู่จาก
 *  TCP Proxy โดยเฉพาะ)
 *
 * เมื่อเกมต่อเข้ามา เซิร์ฟเวอร์จะสร้างรหัสยืนยัน 6 หลัก, สั่งให้เกม
 * พิมพ์รหัสนั้นในแชท, และ subscribe event ตำแหน่งผู้เล่นไว้คำนวณ
 * proximity ฝั่งเว็บไซต์กรอกรหัสนี้เพื่อจับคู่ จากนั้นสถานะไมค์จะถูก
 * ส่งกลับเข้าเกมเป็นไอคอนต่อท้ายชื่อผู้เล่น และเสียงจริงส่งกันแบบ
 * P2P (WebRTC) ระหว่างเบราว์เซอร์ผู้เล่นแต่ละคน
 *
 * ถ้า Minecraft หลุดการเชื่อมต่อ (ปิดเกม/ลบโลก/ถอด add-on) พอร์ต
 * MC_PORT ฝั่งนั้นจะปิดเองโดยธรรมชาติ เซิร์ฟเวอร์จะแจ้งเว็บไซต์ทุกคน
 * ใน session ให้ขึ้น bubble แจ้งเตือนและเคลียร์ข้อมูลทิ้ง
 *
 * NOTE: ส่วนที่ยังเป็นแค่โครงเริ่มต้น ดูหัวข้อ "สิ่งที่ยังเป็นแค่จุด
 * เริ่มต้น" ใน README.md (proximity ยังไม่เช็ค dimension, เสียงเป็น
 * P2P mesh เหมาะกลุ่มเล็ก ไม่ใช่ SFU)
 * ==========================================================
 */

const path = require("path");
const fs = require("fs");
const http = require("http");
const express = require("express");
const { WebSocketServer } = require("ws");
const crypto = require("crypto");

// ------------------------------------------------------------------
// ระบบเก็บข้อมูลถาวร (persistent storage)
// ------------------------------------------------------------------
// เก็บไฟล์ JSON ไว้ที่ DATA_DIR (ควรชี้ไปที่ Railway Volume เพื่อให้
// ข้อมูลไม่หายตอน redeploy/restart) แมประหว่าง "worldId" (รหัสถาวร
// ที่ behavior pack สร้างและเก็บไว้ในตัวโลก/เซิร์ฟเวอร์) กับรหัส 6
// หลักที่เคยออกให้ + ชื่อผู้เล่นที่เป็นเจ้าของห้อง
//
// worldId จะหายไปเองก็ต่อเมื่อ: ถอด behavior pack ออก, ลบโลกทิ้ง,
// หรือลบเซิร์ฟเวอร์ทิ้ง (เพราะมันถูกเก็บอยู่ในตัวเซฟของโลกนั้นเอง
// ผ่าน world dynamic property) ตราบใดที่ยังไม่ทำ 3 อย่างนี้ รหัส 6
// หลักจะเป็นตัวเดิมเสมอทุกครั้งที่เชื่อมต่อใหม่
const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, "data");
const DATA_FILE = path.join(DATA_DIR, "sessions.json");

function loadPersisted() {
  try {
    if (!fs.existsSync(DATA_FILE)) return {};
    return JSON.parse(fs.readFileSync(DATA_FILE, "utf8"));
  } catch {
    return {};
  }
}

function savePersisted(data) {
  try {
    fs.mkdirSync(DATA_DIR, { recursive: true });
    fs.writeFileSync(DATA_FILE, JSON.stringify(data, null, 2));
  } catch (err) {
    console.error("บันทึกไฟล์ข้อมูลถาวรไม่สำเร็จ:", err.message);
  }
}

// persisted: worldId -> { code, hostName, updatedAt }
let persisted = loadPersisted();

const app = express();
app.use(express.static(path.join(__dirname, "public")));

// เซิร์ฟเวอร์หลัก: เว็บไซต์ + WebSocket ฝั่งเว็บ (/site) -> ใช้ wss ได้ปกติ
// เพราะเบราว์เซอร์รองรับ TLS เต็มรูปแบบอยู่แล้ว ไม่มีปัญหาแบบ Minecraft
const server = app.listen(process.env.PORT || 3000, () => {
  console.log(`My Voice Chat backend (เว็บไซต์) กำลังทำงานที่พอร์ต ${process.env.PORT || 3000}`);
});

// เซิร์ฟเวอร์ที่สอง: เฉพาะ Minecraft เท่านั้น -> เป็น ws ธรรมดา ไม่มี TLS
// MC_PORT ต้องถูกตั้งเป็นพอร์ตที่ TCP Proxy ของโฮสต์ (เช่น Railway) ชี้มาให้
const mcHttpServer = http.createServer();
const MC_PORT = process.env.MC_PORT || 3002;
mcHttpServer.listen(MC_PORT, () => {
  console.log(`My Voice Chat backend (Minecraft) กำลังทำงานที่พอร์ต ${MC_PORT}`);
});

const wssMc = new WebSocketServer({ noServer: true });
const wssSite = new WebSocketServer({ noServer: true });

// การเชื่อมต่อจากเกม (ws ธรรมดา ไม่เข้ารหัส) เข้ามาที่ mcHttpServer โดยตรง
// รับทุก path เผื่อ /connect บางเวอร์ชันไม่ส่ง path ต่อท้ายมาให้ถูกต้อง
mcHttpServer.on("upgrade", (req, socket, head) => {
  wssMc.handleUpgrade(req, socket, head, (ws) => wssMc.emit("connection", ws, req));
});

// การเชื่อมต่อจากเว็บไซต์ (wss ผ่าน TLS ปกติ) เข้ามาที่ server หลัก path /site
server.on("upgrade", (req, socket, head) => {
  if (req.url.startsWith("/site")) {
    wssSite.handleUpgrade(req, socket, head, (ws) => wssSite.emit("connection", ws, req));
  } else {
    socket.destroy();
  }
});


// sessions: key = รหัส 6 หลัก, value = { mcSocket, siteSockets: Map<playerName, ws>, positions: {} }
const sessions = new Map();

function genCode() {
  let code;
  do {
    code = String(crypto.randomInt(0, 1000000)).padStart(6, "0");
  } while (sessions.has(code));
  return code;
}

function sendCommand(mcSocket, commandLine, onResponse) {
  const requestId = crypto.randomUUID();
  if (onResponse) {
    mcSocket._pendingCommands = mcSocket._pendingCommands || new Map();
    mcSocket._pendingCommands.set(requestId, onResponse);
  }
  mcSocket.send(
    JSON.stringify({
      header: { version: 1, requestId, messagePurpose: "commandRequest" },
      body: { version: 1, commandLine, origin: { type: "player" } },
    })
  );
}

function subscribeEvent(mcSocket, eventName) {
  mcSocket.send(
    JSON.stringify({
      header: { version: 1, requestId: crypto.randomUUID(), messagePurpose: "subscribe" },
      body: { eventName },
    })
  );
}

// ดึงข้อมูล worldId + hostName ที่ behavior pack เก็บไว้แบบเงียบ ๆ ใน
// scoreboard objective ชื่อ "vcmc_data" (ไม่ขึ้นแชท ไม่สแปม) โดยยิง
// คำสั่ง "scoreboard objectives list" แล้วอ่านค่าจาก statusMessage ที่
// ตอบกลับมา (ใช้กลไก commandRequest/commandResponse เดียวกับที่พิสูจน์
// แล้วว่าใช้งานได้จริงตลอดโปรเจกต์นี้ - ต่างจากการอ่านจาก PlayerMessage
// ที่พบว่าไม่จับข้อความที่สคริปต์ส่งเอง)
function pollWorldId(mcSocket, onFound) {
  sendCommand(mcSocket, "scoreboard objectives list", (body) => {
    const statusMessage = body?.statusMessage || "";
    console.log("[mc] scoreboard objectives list ->", statusMessage);
    // พยายาม parse หลายรูปแบบ เผื่อรูปแบบข้อความต่างกันไปตามเวอร์ชันเกม
    const patterns = [
      /vcmc_data[^:]*:\s*'([^']*)'/,
      /vcmc_data[^:]*:\s*"([^"]*)"/,
      /vcmc_data\s*=\s*([^\s,]+)/,
    ];
    for (const re of patterns) {
      const m = statusMessage.match(re);
      if (m) {
        const parts = m[1].split("|");
        if (parts.length === 2) {
          onFound(parts[0], parts[1]);
          return;
        }
      }
    }
  });
}

// ------------------------------------------------------------------
// การเชื่อมต่อจากฝั่ง Minecraft (/wsserver ws://.../)
// ------------------------------------------------------------------
wssMc.on("connection", (mcSocket) => {
  // ยังไม่รู้ตัวตนของโลก/เซิร์ฟเวอร์นี้ในตอนแรก ต้อง poll หา worldId
  // จาก scoreboard ก่อน (ดูฟังก์ชัน pollWorldId ด้านบน)
  let session = null;

  console.log("[mc] มีการเชื่อมต่อเข้ามา กำลังรอ worldId...");

  subscribeEvent(mcSocket, "PlayerTravelled");

  sendCommand(
    mcSocket,
    `tellraw @a {"rawtext":[{"text":"§b[MyVoiceChat] §fกำลังเชื่อมต่อ..."}]}`
  );

  function setupSession(worldId, hostName) {
    let entry = persisted[worldId];
    if (!entry) {
      entry = { code: genCode(), hostName, updatedAt: Date.now() };
    } else {
      entry.hostName = hostName;
      entry.updatedAt = Date.now();
    }
    persisted[worldId] = entry;
    savePersisted(persisted);

    session = { mcSocket, siteSockets: new Map(), positions: {}, hostName, worldId };
    sessions.set(entry.code, session);
    mcSocket.sessionCode = entry.code;

    console.log(`[mc] ยืนยัน worldId แล้ว -> รหัส ${entry.code} (เจ้าของห้อง: ${hostName})`);

    sendCommand(
      mcSocket,
      `tellraw @a {"rawtext":[{"text":"§b[MyVoiceChat] §fรหัสเชื่อมต่อเว็บไซต์ของคุณคือ: §e§l${entry.code}"}]}`
    );
  }

  // ยิงถามซ้ำทุก 2 วินาที จนกว่าจะได้ worldId (behavior pack อาจยัง
  // ไม่ทันสร้าง scoreboard ตอนที่เพิ่งเชื่อมต่อ)
  const pollIntervalId = setInterval(() => {
    if (session || mcSocket.readyState !== mcSocket.OPEN) {
      clearInterval(pollIntervalId);
      return;
    }
    pollWorldId(mcSocket, (worldId, hostName) => {
      if (!session) setupSession(worldId, hostName);
      clearInterval(pollIntervalId);
    });
  }, 2000);

  mcSocket.on("message", (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return;
    }

    // ผลตอบกลับของคำสั่งที่เคยส่งไปพร้อม callback (ใช้กับ pollWorldId)
    if (msg.header?.messagePurpose === "commandResponse") {
      const cb = mcSocket._pendingCommands?.get(msg.header.requestId);
      if (cb) {
        mcSocket._pendingCommands.delete(msg.header.requestId);
        cb(msg.body);
      }
      return;
    }

    const eventName = msg.body?.eventName;

    // ตำแหน่งผู้เล่น -> เก็บไว้เพื่อคำนวณ proximity แล้วส่งให้เว็บไซต์
    if (eventName === "PlayerTravelled" && session) {
      const p = msg.body.player;
      if (p?.name) {
        session.positions[p.name] = {
          x: p.position?.x,
          y: p.position?.y,
          z: p.position?.z,
          dimension: msg.body.dimension,
        };
        broadcastToSite(session, { type: "positions", positions: session.positions });
      }
    }
  });

  mcSocket.on("close", () => {
    if (session) {
      console.log(`[mc] หลุดการเชื่อมต่อ -> รหัส ${session.mcSocket.sessionCode}`);
      broadcastToSite(session, { type: "disconnected" });
      sessions.delete(mcSocket.sessionCode);
    }
    // หมายเหตุ: ข้อมูลใน persisted (ไฟล์ถาวร) ไม่ถูกลบตรงนี้ - จะยังอยู่
    // ต่อไปแม้เกมหลุด รอให้เชื่อมต่อใหม่ก็ใช้รหัสเดิมได้เลย (ตราบใดที่
    // worldId เดิมยังอยู่ = ยังไม่ได้ถอด add-on/ลบโลก/ลบเซิร์ฟเวอร์)
  });

  mcSocket.on("error", () => mcSocket.close());
});

// ------------------------------------------------------------------
// การเชื่อมต่อจากฝั่งเว็บไซต์
// ------------------------------------------------------------------
wssSite.on("connection", (siteSocket) => {
  siteSocket.on("message", (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return;
    }

    // ขอรายชื่อห้องที่กำลังออนไลน์อยู่ตอนนี้ (ไว้ทำหน้า "เข้าร่วมห้อง")
    if (msg.type === "list_rooms") {
      const rooms = [...sessions.entries()]
        .filter(([, s]) => s.hostName)
        .map(([code, s]) => ({ code, hostName: s.hostName }));
      siteSocket.send(JSON.stringify({ type: "rooms", rooms }));
      return;
    }

    // ขั้นตอนยืนยันตัวตนด้วยรหัส 6 หลัก + ชื่อผู้เล่น
    if (msg.type === "verify") {
      const session = sessions.get(msg.code);
      if (!session) {
        siteSocket.send(JSON.stringify({ type: "verify_failed", reason: "รหัสไม่ถูกต้องหรือหมดอายุ" }));
        return;
      }
      siteSocket.sessionCode = msg.code;
      siteSocket.playerName = msg.playerName;
      session.siteSockets.set(msg.playerName, siteSocket);

      siteSocket.send(JSON.stringify({ type: "verified", playerName: msg.playerName }));

      // ตั้งสถานะเริ่มต้นเป็น "เชื่อมต่อแล้ว" (state 1)
      sendCommand(session.mcSocket, `scriptevent vcmc:status ${msg.playerName}|1`);

      // บอกทุกคนใน session ว่าตอนนี้มีใครอยู่บ้าง (ไว้เปิด WebRTC หากันเอง)
      broadcastPeerLists(session);
      return;
    }

    const session = sessions.get(siteSocket.sessionCode);
    if (!session) return;

    // ปิด/เปิดไมค์ -> อัปเดตไอคอนในเกม
    if (msg.type === "mute") {
      const state = msg.muted ? 2 : 1;
      sendCommand(session.mcSocket, `scriptevent vcmc:status ${siteSocket.playerName}|${state}`);
      return;
    }

    // กำลังพูดอยู่หรือไม่ (state 3 = พูดอยู่)
    if (msg.type === "talking" && !msg.muted) {
      const state = msg.talking ? 3 : 1;
      sendCommand(session.mcSocket, `scriptevent vcmc:status ${siteSocket.playerName}|${state}`);
      return;
    }

    // สัญญาณ WebRTC (offer/answer/ice) รีเลย์ไปหาผู้เล่นเป้าหมายใน session เดียวกัน
    if (msg.type === "signal") {
      const target = session.siteSockets.get(msg.to);
      if (target) {
        target.send(
          JSON.stringify({ type: "signal", from: siteSocket.playerName, data: msg.data })
        );
      }
      return;
    }
  });

  siteSocket.on("close", () => {
    const session = sessions.get(siteSocket.sessionCode);
    if (session && siteSocket.playerName) {
      session.siteSockets.delete(siteSocket.playerName);
      // แจ้งคนอื่นในกลุ่มว่ามีคนหลุด (ไว้เคลียร์ peer connection ฝั่งเขา)
      broadcastToSite(session, { type: "peer_left", playerName: siteSocket.playerName }, siteSocket);
    }
  });
});

function broadcastToSite(session, payload, exceptSocket) {
  for (const ws of session.siteSockets.values()) {
    if (ws !== exceptSocket && ws.readyState === ws.OPEN) {
      ws.send(JSON.stringify(payload));
    }
  }
}

// ส่งรายชื่อ "เพื่อนในกลุ่มเดียวกัน" ให้แต่ละคน (ไม่รวมตัวเอง)
// frontend ใช้รายชื่อนี้ในการเปิดการเชื่อมต่อ WebRTC หากันเอง (mesh)
function broadcastPeerLists(session) {
  const names = [...session.siteSockets.keys()];
  for (const [name, ws] of session.siteSockets.entries()) {
    if (ws.readyState !== ws.OPEN) continue;
    ws.send(JSON.stringify({ type: "peers", peers: names.filter((n) => n !== name) }));
  }
}

// heartbeat ระดับ TCP/WebSocket มาตรฐาน - ใช้กับ "ฝั่งเว็บไซต์" เท่านั้น
// (เบราว์เซอร์ตอบสนอง ping/pong ตามมาตรฐานแน่นอน)
//
// สำคัญ: ห้ามใช้กลไกนี้กับฝั่ง Minecraft (wssMc) เพราะเอนจินเครือข่าย
// ของเกมไม่ตอบสนอง ping แบบนี้เหมือนเบราว์เซอร์ ถ้าเปิดใช้ด้วยจะโดน
// terminate() ตัดทิ้งทั้งที่จริง ๆ ยังเชื่อมต่ออยู่ปกติ (นี่คือสาเหตุที่
// เคยเจอปัญหา "เชื่อมต่อได้แป๊บนึงแล้วหลุดเอง" มาก่อน) ฝั่ง Minecraft
// ปล่อยให้ event "close"/"error" ที่เกิดขึ้นจริงเป็นตัวบอกแทน
function heartbeat() {
  this.isAlive = true;
}
wssSite.on("connection", (ws) => {
  ws.isAlive = true;
  ws.on("pong", heartbeat);
});
setInterval(() => {
  wssSite.clients.forEach((ws) => {
    if (ws.isAlive === false) return ws.terminate();
    ws.isAlive = false;
    ws.ping();
  });
}, 15000);
