const MONTH_MAP = {
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

const normalizeAmount = (value) => {
  if (!value) return null;
  const trimmed = String(value).replace(/\$/g, "").trim();
  let normalized = trimmed;
  if (trimmed.includes(",") && trimmed.includes(".")) {
    normalized = trimmed.replace(/,/g, "");
  } else if (trimmed.includes(",") && !trimmed.includes(".")) {
    normalized = trimmed.replace(/,/g, ".");
  }
  const numeric = Number(normalized);
  return Number.isNaN(numeric) ? null : numeric;
};

const parseDateToken = (token) => {
  if (!token) return null;
  const isoMatch = token.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (isoMatch) {
    return `${isoMatch[1]}-${isoMatch[2]}-${isoMatch[3]}`;
  }

  const slashMatch = token.match(/^(\d{2})\/(\d{2})\/(\d{4})$/);
  if (slashMatch) {
    return `${slashMatch[3]}-${slashMatch[2]}-${slashMatch[1]}`;
  }

  const monthMatch = token.match(/^([A-Z]{3})\s+(\d{1,2})$/);
  if (monthMatch) {
    const year = new Date().getFullYear();
    const month = MONTH_MAP[monthMatch[1]];
    const day = String(monthMatch[2]).padStart(2, "0");
    if (month) {
      return `${year}-${month}-${day}`;
    }
  }

  return null;
};

const extractAmountToken = (line) => {
  const matches = line.match(/-?\$?\d+(?:[.,]\d{2})?/g);
  if (!matches || !matches.length) return null;
  return matches[matches.length - 1];
};

const extractDateToken = (line) => {
  const iso = line.match(/\b\d{4}-\d{2}-\d{2}\b/);
  if (iso) return iso[0];
  const slash = line.match(/\b\d{2}\/\d{2}\/\d{4}\b/);
  if (slash) return slash[0];
  const spacedMonth = line.match(/\b([A-Z]{3})\s+\d{1,2}\b/);
  if (spacedMonth) return spacedMonth[0];
  const compactMonth = line.match(/\b([A-Z]{3}\d{2})\b/);
  if (compactMonth) return compactMonth[0];
  return null;
};

const extractAmountBeforeDate = (line, dateToken) => {
  if (!dateToken) return null;
  const index = line.indexOf(dateToken);
  if (index <= 0) return null;
  const before = line.slice(0, index);
  const matches = before.match(/-?\$?\d+(?:[.,]\d{2})?/g);
  if (!matches || !matches.length) return null;
  return matches[matches.length - 1];
};

const parseTransactions = (text) => {
  if (!text) return [];
  return text
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean)
    .reduce((results, line) => {
      const dateToken = extractDateToken(line);
      const amountToken =
        extractAmountBeforeDate(line, dateToken) || extractAmountToken(line);
      if (!dateToken || !amountToken) return results;
      const date = parseDateToken(dateToken);
      const amount = normalizeAmount(amountToken);
      if (!date || amount === null) return results;
      const description = line
        .replace(dateToken, "")
        .replace(amountToken, "")
        .replace(/\s{2,}/g, " ")
        .trim();
      results.push({
        date,
        description: description || "Transaction",
        amount,
        raw_line: line,
      });
      return results;
    }, []);
};

module.exports = {
  parseTransactions,
};
