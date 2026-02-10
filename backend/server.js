const path = require("path");
const fs = require("fs");
const express = require("express");
const cors = require("cors");
const Database = require("better-sqlite3");
const multer = require("multer");
const { parseTransactions } = require("../lib/import/parseTransactions");
const { parse: parseCsv } = require("csv-parse/sync");

const PORT = process.env.PORT || 3001;
const DB_PATH = path.join(__dirname, "data.db");
const ALLOWED_COLLECTIONS = new Set([
  "income",
  "expenses",
  "goals",
  "investments",
  "assets",
]);

const app = express();
app.use(cors());
app.use(express.json({ limit: "1mb" }));
const publicDir = path.join(__dirname, "..");
app.use(express.static(publicDir));
const uploadDir = path.join(__dirname, "uploads");
fs.mkdirSync(uploadDir, { recursive: true });
const upload = multer({
  storage: multer.diskStorage({
    destination: (req, file, cb) => cb(null, uploadDir),
    filename: (req, file, cb) => {
      const safeName = file.originalname.replace(/[^a-zA-Z0-9._-]/g, "_");
      cb(null, `${Date.now()}_${safeName}`);
    },
  }),
  limits: { fileSize: 15_000_000 },
});

const db = new Database(DB_PATH);
db.pragma("journal_mode = WAL");
db.exec(`
  CREATE TABLE IF NOT EXISTS entries (
    id TEXT PRIMARY KEY,
    collection TEXT NOT NULL,
    payload TEXT NOT NULL,
    fingerprint TEXT,
    createdAt TEXT NOT NULL,
    updatedAt TEXT NOT NULL
  );
`);
try {
  db.exec(`ALTER TABLE entries ADD COLUMN fingerprint TEXT`);
} catch (error) {
  if (!String(error).includes("duplicate column")) {
    throw error;
  }
}
db.exec(`
  CREATE INDEX IF NOT EXISTS idx_entries_collection ON entries(collection);
  CREATE INDEX IF NOT EXISTS idx_entries_fingerprint ON entries(fingerprint);
  CREATE TABLE IF NOT EXISTS imports (
    id TEXT PRIMARY KEY,
    filename TEXT NOT NULL,
    status TEXT NOT NULL,
    error TEXT,
    createdAt TEXT NOT NULL
  );
  CREATE TABLE IF NOT EXISTS import_transactions (
    id TEXT PRIMARY KEY,
    import_id TEXT NOT NULL,
    description TEXT NOT NULL,
    amount REAL NOT NULL,
    date TEXT NOT NULL,
    type TEXT NOT NULL,
    category TEXT,
    fingerprint TEXT NOT NULL,
    status TEXT NOT NULL,
    raw_line TEXT,
    createdAt TEXT NOT NULL
  );
  CREATE INDEX IF NOT EXISTS idx_import_transactions_import ON import_transactions(import_id);
  CREATE TABLE IF NOT EXISTS category_rules (
    id TEXT PRIMARY KEY,
    pattern TEXT NOT NULL,
    category TEXT NOT NULL,
    createdAt TEXT NOT NULL
  );
`);
try {
  db.exec(`ALTER TABLE imports ADD COLUMN error TEXT`);
} catch (error) {
  if (!String(error).includes("duplicate column")) {
    throw error;
  }
}
try {
  db.exec(`ALTER TABLE import_transactions ADD COLUMN raw_line TEXT`);
} catch (error) {
  if (!String(error).includes("duplicate column")) {
    throw error;
  }
}

const nowIso = () => new Date().toISOString();

const serialize = (row) => ({
  id: row.id,
  ...JSON.parse(row.payload),
  createdAt: row.createdAt,
  updatedAt: row.updatedAt,
});

app.get("/api/health", (req, res) => {
  res.json({ ok: true });
});

app.get("/api/all", (req, res) => {
  const data = {};
  ALLOWED_COLLECTIONS.forEach((collection) => {
    const rows = db
      .prepare("SELECT * FROM entries WHERE collection = ? ORDER BY createdAt DESC")
      .all(collection);
    data[collection] = rows.map(serialize);
  });
  res.json(data);
});

app.get("/api/:collection", (req, res) => {
  const { collection } = req.params;
  if (!ALLOWED_COLLECTIONS.has(collection)) {
    return res.status(404).json({ error: "Unknown collection" });
  }
  const rows = db
    .prepare("SELECT * FROM entries WHERE collection = ? ORDER BY createdAt DESC")
    .all(collection);
  res.json(rows.map(serialize));
});

const monthMap = {
  JAN: "01",
  FEB: "02",
  MAR: "03",
  APR: "04",
  MAY: "05",
  JUN: "06",
  JUL: "07",
  AUG: "08",
  SEP: "09",
  OCT: "10",
  NOV: "11",
  DEC: "12",
};

const parseAmount = (value) => {
  if (!value) return 0;
  return Number(String(value).replace(/,/g, "").trim()) || 0;
};

const categorizeDescription = (description) => {
  const text = description.toLowerCase();
  if (text.includes("uber")) return "Uber";
  if (text.includes("tim hortons") || text.includes("mcdonald") || text.includes("shawarm") || text.includes("burger"))
    return "Eating Out";
  if (text.includes("nofrills") || text.includes("freshco")) return "Groceries";
  if (text.includes("mtge") || text.includes("mortgage")) return "Mortgage";
  if (text.includes("hydro") || text.includes("utilities")) return "Utilities";
  if (text.includes("rogers")) return "Phone";
  if (text.includes("fit4less") || text.includes("gym")) return "Gym";
  if (text.includes("openai") || text.includes("cursor")) return "Subscriptions";
  return "Misc";
};

const parseTdStatement = (text) => {
  const rows = [];
  const lines = text
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);

  const periodLine = lines.find((line) => line.includes("Statement From To"));
  const dateRangeLine = lines.find((line) => line.match(/[A-Z]{3}\s+\d{2}\/\d{2}\s+-\s+[A-Z]{3}\s+\d{2}\/\d{2}/));
  let yearStart = "2025";
  let yearEnd = "2026";
  if (dateRangeLine) {
    const match = dateRangeLine.match(/([A-Z]{3})\s+\d{2}\/(\d{2})\s+-\s+([A-Z]{3})\s+\d{2}\/(\d{2})/);
    if (match) {
      yearStart = `20${match[2]}`;
      yearEnd = `20${match[4]}`;
    }
  }

  lines.forEach((line) => {
    if (!line.includes("|")) return;
    const parts = line
      .split("|")
      .map((part) => part.trim())
      .filter((part) => part.length);
    if (parts.length < 5) return;
    const [description, withdrawals, deposits, date] = parts;
    if (!description || !date) return;
    if (/balance/i.test(description)) return;
    if (/starting/i.test(description)) return;
    if (/closing/i.test(description)) return;
    if (/account\/transaction/i.test(description)) return;
    if (!/^[A-Z]{3}\d{2}/.test(date)) return;

    const month = date.slice(0, 3);
    const day = date.slice(3, 5);
    const year = month === "DEC" ? yearStart : yearEnd;
    const isoDate = `${year}-${monthMap[month]}-${day}`;

    const withdrawalAmount = parseAmount(withdrawals);
    const depositAmount = parseAmount(deposits);
    if (!withdrawalAmount && !depositAmount) return;

    if (withdrawalAmount) {
      rows.push({
        collection: "expenses",
        payload: {
          name: description,
          amount: withdrawalAmount,
          date: isoDate,
          category: categorizeDescription(description),
          recurring: false,
        },
      });
    } else if (depositAmount) {
      rows.push({
        collection: "income",
        payload: {
          name: description,
          amount: depositAmount,
          date: isoDate,
          incomeType: "Other",
          recurring: false,
        },
      });
    }
  });
  return rows;
};

const createParsedRows = (importId, parsed) => {
  const timestamp = nowIso();
  return parsed.map((entry, index) => ({
    id: `${Date.now()}-${Math.random().toString(16).slice(2)}`,
    import_id: importId,
    description: entry.description,
    amount: entry.amount,
    date: entry.date,
    type: "staged",
    category: null,
    fingerprint: `raw-${importId}-${index + 1}`,
    status: "staged",
    raw_line: entry.raw_line,
    createdAt: timestamp,
  }));
};

const createCsvRows = (importId, rows) => {
  const timestamp = nowIso();
  return rows.map((row, index) => ({
    id: `${Date.now()}-${Math.random().toString(16).slice(2)}`,
    import_id: importId,
    description: row.description,
    amount: row.amount,
    date: row.date,
    type: row.type,
    category: row.category || null,
    fingerprint: `csv-${importId}-${index + 1}`,
    status: "staged",
    raw_line: row.raw_line || row.description,
    createdAt: timestamp,
  }));
};
app.post("/api/imports/upload", upload.single("statement"), async (req, res) => {
  let importId = null;
  try {
    if (!req.file) {
      return res.status(400).json({ error: "Missing statement file" });
    }
    if (!req.file.originalname.toLowerCase().endsWith(".csv")) {
      return res.status(400).json({ error: "Only CSV files are supported right now." });
    }
    importId = `${Date.now()}-${Math.random().toString(16).slice(2)}`;
    const filename = req.file.filename;
    const createdAt = nowIso();
    db.prepare(
      "INSERT INTO imports (id, filename, status, createdAt) VALUES (?, ?, ?, ?)"
    ).run(importId, filename, "processing", createdAt);

    const csvText = fs.readFileSync(req.file.path, "utf-8");
    const parsedRows = parseCsvStatement(csvText);
    const rows = createCsvRows(
      importId,
      parsedRows.map((row) => ({
        description: row.payload.name,
        amount: row.payload.amount,
        date: row.payload.date,
        type: row.collection === "income" ? "income" : "expense",
        category: row.payload.category || null,
        raw_line: row.payload.name,
      }))
    );
    const insert = db.prepare(
      "INSERT INTO import_transactions (id, import_id, description, amount, date, type, category, fingerprint, status, raw_line, createdAt) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    );
    const transaction = db.transaction((items) => {
      items.forEach((row) => {
        insert.run(
          row.id,
          row.import_id,
          row.description,
          row.amount,
          row.date,
          row.type,
          row.category,
          row.fingerprint,
          row.status,
          row.raw_line,
          row.createdAt
        );
      });
    });
    transaction(rows);
    db.prepare("UPDATE imports SET status = ? WHERE id = ?").run("staged", importId);
    return res.json({ ok: true, import_id: importId });
  } catch (error) {
    console.error("CSV import failed", error);
    const message = error && error.message ? error.message : "CSV import failed";
    if (importId) {
      db.prepare("UPDATE imports SET status = ?, error = ? WHERE id = ?").run(
        "failed",
        message,
        importId
      );
    }
    return res.status(400).json({ error: message });
  }
});

app.get("/api/imports/:id", (req, res) => {
  const { id } = req.params;
  const importRow = db.prepare("SELECT * FROM imports WHERE id = ?").get(id);
  if (!importRow) {
    return res.status(404).json({ error: "Import not found" });
  }
  const staging = db
    .prepare("SELECT * FROM import_transactions WHERE import_id = ? ORDER BY createdAt DESC")
    .all(id);
  return res.json({ import: importRow, staging });
});

app.post("/api/imports/:id/confirm", (req, res) => {
  const { id } = req.params;
  const importRow = db.prepare("SELECT * FROM imports WHERE id = ?").get(id);
  if (!importRow) {
    return res.status(404).json({ error: "Import not found" });
  }
  const staging = db
    .prepare("SELECT * FROM import_transactions WHERE import_id = ?")
    .all(id);
  if (!staging.length) {
    return res.json({ imported: 0, skipped: 0 });
  }

  const insert = db.prepare(
    "INSERT INTO entries (id, collection, payload, fingerprint, createdAt, updatedAt) VALUES (?, ?, ?, ?, ?, ?)"
  );
  const exists = db.prepare("SELECT 1 FROM entries WHERE fingerprint = ? LIMIT 1");
  const timestamp = nowIso();
  let imported = 0;
  let skipped = 0;
  const transaction = db.transaction((items) => {
    items.forEach((row) => {
      if (exists.get(row.fingerprint)) {
        skipped += 1;
        return;
      }
      const payload =
        row.type === "income"
          ? {
              name: row.description,
              amount: row.amount,
              date: row.date,
              incomeType: "Other",
              recurring: false,
            }
          : {
              name: row.description,
              amount: row.amount,
              date: row.date,
              category: row.category || "Misc",
              recurring: false,
            };
      insert.run(
        `${Date.now()}-${Math.random().toString(16).slice(2)}`,
        row.type === "income" ? "income" : "expenses",
        JSON.stringify(payload),
        row.fingerprint,
        timestamp,
        timestamp
      );
      imported += 1;
    });
  });
  transaction(staging);

  db.prepare("UPDATE imports SET status = ? WHERE id = ?").run("confirmed", id);
  db.prepare("UPDATE import_transactions SET status = ? WHERE import_id = ?").run(
    "confirmed",
    id
  );
  return res.json({ imported, skipped });
});

const parseCsvStatement = (text) => {
  const records = parseCsv(text, {
    columns: true,
    skip_empty_lines: true,
  });

  return records.flatMap((record) => {
    const description =
      record.Description || record.description || record.Memo || record.memo || "";
    const dateRaw = record.Date || record.date || record.Posted || record.posted || "";
    const amountRaw =
      record.Amount || record.amount || record.Debit || record.debit || record.Credit || record.credit || "";

    if (!description || !dateRaw || !amountRaw) return [];
    const amount = parseAmount(amountRaw);
    if (!amount) return [];
    const isoDate = new Date(dateRaw);
    if (Number.isNaN(isoDate.getTime())) return [];
    const date = isoDate.toISOString().slice(0, 10);

    if (String(amountRaw).toLowerCase().includes("credit") || amount > 0) {
      return [
        {
          collection: "income",
          payload: {
            name: description,
            amount: Math.abs(amount),
            date,
            incomeType: "Other",
            recurring: false,
          },
        },
      ];
    }
    return [
      {
        collection: "expenses",
        payload: {
          name: description,
          amount: Math.abs(amount),
          date,
          category: categorizeDescription(description),
          recurring: false,
        },
      },
    ];
  });
};

const insertRows = (rows) => {
  const insert = db.prepare(
    "INSERT INTO entries (id, collection, payload, createdAt, updatedAt) VALUES (?, ?, ?, ?, ?)"
  );
  const timestamp = nowIso();
  const transaction = db.transaction((items) => {
    items.forEach((item) => {
      const id = `${Date.now()}-${Math.random().toString(16).slice(2)}`;
      insert.run(id, item.collection, JSON.stringify(item.payload), timestamp, timestamp);
    });
  });
  transaction(rows);
};

app.post("/api/import/statement", upload.single("statement"), async (req, res) => {
  if (!req.file) {
    return res.status(400).json({ error: "Missing statement file" });
  }
  const filename = req.file.originalname.toLowerCase();
  let rows = [];
  if (filename.endsWith(".pdf")) {
    const parsed = await pdfParse(req.file.buffer);
    rows = parseTdStatement(parsed.text || "");
  } else if (filename.endsWith(".csv")) {
    rows = parseCsvStatement(req.file.buffer.toString("utf-8"));
  } else {
    return res.status(400).json({ error: "Unsupported file type" });
  }
  insertRows(rows);
  return res.json({ imported: rows.length });
});

app.use((err, req, res, next) => {
  if (err) {
    const message = err.message || "Upload failed";
    return res.status(400).json({ error: message });
  }
  return next();
});

app.post("/api/:collection", (req, res) => {
  const { collection } = req.params;
  if (!ALLOWED_COLLECTIONS.has(collection)) {
    return res.status(404).json({ error: "Unknown collection" });
  }
  const payload = req.body || {};
  const id = payload.id || `${Date.now()}-${Math.random().toString(16).slice(2)}`;
  const timestamp = nowIso();
  db.prepare(
    "INSERT INTO entries (id, collection, payload, createdAt, updatedAt) VALUES (?, ?, ?, ?, ?)"
  ).run(id, collection, JSON.stringify(payload), timestamp, timestamp);
  res.json({ id, ...payload, createdAt: timestamp, updatedAt: timestamp });
});

app.put("/api/:collection/:id", (req, res) => {
  const { collection, id } = req.params;
  if (!ALLOWED_COLLECTIONS.has(collection)) {
    return res.status(404).json({ error: "Unknown collection" });
  }
  const payload = req.body || {};
  const timestamp = nowIso();
  const info = db
    .prepare(
      "UPDATE entries SET payload = ?, updatedAt = ? WHERE id = ? AND collection = ?"
    )
    .run(JSON.stringify(payload), timestamp, id, collection);
  if (!info.changes) {
    return res.status(404).json({ error: "Entry not found" });
  }
  res.json({ id, ...payload, updatedAt: timestamp });
});

app.delete("/api/:collection/:id", (req, res) => {
  const { collection, id } = req.params;
  if (!ALLOWED_COLLECTIONS.has(collection)) {
    return res.status(404).json({ error: "Unknown collection" });
  }
  const info = db
    .prepare("DELETE FROM entries WHERE id = ? AND collection = ?")
    .run(id, collection);
  if (!info.changes) {
    return res.status(404).json({ error: "Entry not found" });
  }
  res.json({ ok: true });
});

app.get("*", (req, res) => {
  if (req.path.startsWith("/api/")) {
    return res.status(404).json({ error: "Not found" });
  }
  return res.sendFile(path.join(publicDir, "index.html"));
});

app.listen(PORT, () => {
  console.log(`API running on http://localhost:${PORT}`);
});
