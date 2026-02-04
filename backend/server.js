const path = require("path");
const express = require("express");
const cors = require("cors");
const Database = require("better-sqlite3");

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

const db = new Database(DB_PATH);
db.pragma("journal_mode = WAL");
db.exec(`
  CREATE TABLE IF NOT EXISTS entries (
    id TEXT PRIMARY KEY,
    collection TEXT NOT NULL,
    payload TEXT NOT NULL,
    createdAt TEXT NOT NULL,
    updatedAt TEXT NOT NULL
  );
  CREATE INDEX IF NOT EXISTS idx_entries_collection ON entries(collection);
`);

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

app.listen(PORT, () => {
  console.log(`API running on http://localhost:${PORT}`);
});
