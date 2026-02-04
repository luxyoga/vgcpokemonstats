const API_BASE = "http://localhost:3001/api";

const monthNames = [
  "All",
  "January",
  "February",
  "March",
  "April",
  "May",
  "June",
  "July",
  "August",
  "September",
  "October",
  "November",
  "December",
];

const defaultData = {
  income: [],
  expenses: [],
  goals: [],
  investments: [],
  assets: [],
};

const dom = {
  filterMonth: document.getElementById("filterMonth"),
  filterYear: document.getElementById("filterYear"),
  incomeForm: document.getElementById("incomeForm"),
  expenseForm: document.getElementById("expenseForm"),
  goalForm: document.getElementById("goalForm"),
  investmentForm: document.getElementById("investmentForm"),
  assetForm: document.getElementById("assetForm"),
  incomeTable: document.getElementById("incomeTable"),
  expenseTable: document.getElementById("expenseTable"),
  goalTable: document.getElementById("goalTable"),
  investmentTable: document.getElementById("investmentTable"),
  assetTable: document.getElementById("assetTable"),
  statIncome: document.getElementById("statIncome"),
  statExpenses: document.getElementById("statExpenses"),
  statNet: document.getElementById("statNet"),
  statGoals: document.getElementById("statGoals"),
  statNetWorth: document.getElementById("statNetWorth"),
  statSubscriptions: document.getElementById("statSubscriptions"),
  incomeNote: document.getElementById("incomeNote"),
  expenseNote: document.getElementById("expenseNote"),
  netNote: document.getElementById("netNote"),
  goalNote: document.getElementById("goalNote"),
  netWorthNote: document.getElementById("netWorthNote"),
  subscriptionNote: document.getElementById("subscriptionNote"),
  categoryBreakdown: document.getElementById("categoryBreakdown"),
  goalProgress: document.getElementById("goalProgress"),
  cashflowTimeline: document.getElementById("cashflowTimeline"),
  netWorthTrend: document.getElementById("netWorthTrend"),
  expensePie: document.getElementById("expensePie"),
  expenseLegend: document.getElementById("expenseLegend"),
  incomePie: document.getElementById("incomePie"),
  incomeLegend: document.getElementById("incomeLegend"),
  exportDataBtn: document.getElementById("exportDataBtn"),
  customCategoryField: document.getElementById("customCategoryField"),
  submitLabels: document.querySelectorAll("[data-submit-label]"),
  cancelEditButtons: document.querySelectorAll("[data-cancel-edit]"),
  editBanners: document.querySelectorAll("[data-edit-banner]"),
  refreshPricesBtn: document.getElementById("refreshPricesBtn"),
  priceStatus: document.getElementById("priceStatus"),
  debugPanel: document.getElementById("debugPanel"),
  debugOutput: document.getElementById("debugOutput"),
  debugRunBtn: document.getElementById("debugRunBtn"),
  debugShowDataBtn: document.getElementById("debugShowDataBtn"),
  debugResetFiltersBtn: document.getElementById("debugResetFiltersBtn"),
  debugCloseBtn: document.getElementById("debugCloseBtn"),
};

const editState = {
  income: null,
  expenses: null,
  goals: null,
  investments: null,
  assets: null,
};

const formatter = new Intl.NumberFormat("en-US", {
  style: "currency",
  currency: "USD",
});

const today = new Date();

const safeClone = (value) => {
  if (typeof structuredClone === "function") {
    return structuredClone(value);
  }
  return JSON.parse(JSON.stringify(value));
};

const generateId = () => {
  if (typeof crypto !== "undefined" && crypto.randomUUID) return crypto.randomUUID();
  return `${Date.now()}-${Math.random().toString(16).slice(2)}`;
};

let cachedData = safeClone(defaultData);

const showApiWarning = () => {
  if (document.getElementById("storageWarning")) return;
  const banner = document.createElement("div");
  banner.id = "storageWarning";
  banner.style.cssText =
    "position: sticky; top: 0; z-index: 20; background: #fff4e5; color: #7c3e00; padding: 10px 16px; text-align: center; font-size: 13px; border-bottom: 1px solid #f3d4a6;";
  banner.textContent =
    "Backend API not reachable. Start the server with `npm run dev`.";
  document.body.prepend(banner);
  showDebugPanel();
};

const showDebugPanel = () => {
  if (dom.debugPanel) dom.debugPanel.classList.remove("hidden");
};

const writeDebug = (message) => {
  if (!dom.debugOutput) return;
  dom.debugOutput.textContent = `${message}\n`;
};

const appendDebug = (message) => {
  if (!dom.debugOutput) return;
  dom.debugOutput.textContent += `${message}\n`;
};

window.addEventListener("error", (event) => {
  showDebugPanel();
  appendDebug(`Error: ${event.message}`);
});

window.addEventListener("unhandledrejection", (event) => {
  showDebugPanel();
  appendDebug(`Unhandled promise: ${event.reason}`);
});

const fetchJson = async (url, options = {}) => {
  const response = await fetch(url, options);
  if (!response.ok) {
    throw new Error(`API error: ${response.status}`);
  }
  return response.json();
};

const refreshData = async () => {
  try {
    const data = await fetchJson(`${API_BASE}/all`);
    cachedData = { ...safeClone(defaultData), ...data };
    return cachedData;
  } catch (error) {
    console.error("Failed to fetch data", error);
    showApiWarning();
    return cachedData;
  }
};

const loadData = () => cachedData;

const toISODate = (value) => {
  if (!value) return "";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "";
  return date.toISOString().slice(0, 10);
};

const formatMoney = (value) => formatter.format(Number(value) || 0);

const getExpenseCategory = (expense) => {
  if (expense.category === "Custom" && expense.customCategory) {
    return expense.customCategory;
  }
  if (expense.category === "Subscriptions" && expense.subscriptionName) {
    return `Subscriptions • ${expense.subscriptionName}`;
  }
  return expense.category || "Uncategorized";
};

const getMonthIndex = (isoDate) => {
  if (!isoDate) return null;
  const date = new Date(isoDate);
  return Number.isNaN(date.getTime()) ? null : date.getMonth() + 1;
};

const getYear = (isoDate) => {
  if (!isoDate) return null;
  const date = new Date(isoDate);
  return Number.isNaN(date.getTime()) ? null : date.getFullYear();
};

const buildFilters = (data) => {
  const hasEntries =
    data.income.length ||
    data.expenses.length ||
    data.investments.length ||
    data.assets.length;
  const selectedMonth =
    dom.filterMonth.value ||
    (hasEntries ? "0" : String(today.getMonth() + 1));
  const selectedYear =
    dom.filterYear.value ||
    (hasEntries ? "" : String(today.getFullYear()));

  dom.filterMonth.innerHTML = "";
  monthNames.forEach((name, index) => {
    const option = document.createElement("option");
    option.value = String(index);
    option.textContent = name;
    dom.filterMonth.append(option);
  });
  dom.filterMonth.value = selectedMonth;

  const years = new Set();
  [...data.income, ...data.expenses, ...data.investments].forEach((entry) => {
    const year = getYear(entry.date);
    if (year) years.add(year);
  });
  years.add(today.getFullYear());
  const yearList = Array.from(years).sort((a, b) => b - a);
  dom.filterYear.innerHTML = "";
  yearList.forEach((year) => {
    const option = document.createElement("option");
    option.value = String(year);
    option.textContent = String(year);
    dom.filterYear.append(option);
  });
  const fallbackYear = yearList[0] ? String(yearList[0]) : String(today.getFullYear());
  dom.filterYear.value = yearList.includes(Number(selectedYear))
    ? selectedYear
    : fallbackYear;
};

const getActiveFilter = () => ({
  month: Number(dom.filterMonth.value || 0),
  year: Number(dom.filterYear.value || today.getFullYear()),
});

const matchesFilter = (entry, filter) => {
  if (!entry.date) return false;
  const entryMonth = getMonthIndex(entry.date);
  const entryYear = getYear(entry.date);
  if (!entryMonth || !entryYear) return false;
  const monthOk = filter.month === 0 || entryMonth === filter.month;
  const yearOk = entryYear === filter.year;
  return monthOk && yearOk;
};

const getTotals = (data, filter) => {
  const income = data.income.filter((entry) => matchesFilter(entry, filter));
  const expenses = data.expenses.filter((entry) => matchesFilter(entry, filter));
  const investments = data.investments.filter((entry) => matchesFilter(entry, filter));
  const assets = data.assets;
  const incomeTotal = income.reduce((sum, entry) => sum + Number(entry.amount || 0), 0);
  const expenseTotal = expenses.reduce((sum, entry) => sum + Number(entry.amount || 0), 0);
  const investmentTotal = investments.reduce((sum, entry) => sum + Number(entry.amount || 0), 0);
  const investmentValueTotal = data.investments.reduce(
    (sum, entry) => sum + getInvestmentValue(entry),
    0
  );
  const assetTotal = assets.reduce(
    (sum, asset) =>
      sum + Number(asset.currentValue || 0) - Number(asset.mortgageBalance || 0),
    0
  );
  return {
    income,
    expenses,
    investments,
    assets,
    incomeTotal,
    expenseTotal,
    investmentTotal,
    investmentValueTotal,
    assetTotal,
  };
};

const renderDashboard = (data) => {
  const filter = getActiveFilter();
  const totals = getTotals(data, filter);
  const net = totals.incomeTotal - totals.expenseTotal;
  const savingsRate = totals.incomeTotal
    ? Math.round((net / totals.incomeTotal) * 100)
    : 0;

  dom.statIncome.textContent = formatMoney(totals.incomeTotal);
  dom.statExpenses.textContent = formatMoney(totals.expenseTotal);
  dom.statNet.textContent = formatMoney(net);
  dom.statGoals.textContent = formatMoney(
    data.goals.reduce((sum, goal) => sum + Number(goal.saved || 0), 0)
  );
  const netWorth =
    totals.incomeTotal -
    totals.expenseTotal +
    totals.investmentValueTotal +
    totals.assetTotal;
  dom.statNetWorth.textContent = formatMoney(netWorth);
  const subscriptionExpenses = totals.expenses.filter(
    (expense) => expense.category === "Subscriptions"
  );
  const subscriptionTotal = subscriptionExpenses.reduce(
    (sum, entry) => sum + Number(entry.amount || 0),
    0
  );
  dom.statSubscriptions.textContent = formatMoney(subscriptionTotal);

  dom.incomeNote.textContent = `${totals.income.length} streams`;
  dom.expenseNote.textContent = `${totals.expenses.length} transactions`;
  dom.netNote.textContent = `${savingsRate}% savings rate`;
  dom.goalNote.textContent = `${data.goals.length} active goals`;
  dom.netWorthNote.textContent = `Assets + cash flow`;
  dom.subscriptionNote.textContent = `${subscriptionExpenses.length} active`;

  const categoryTotals = {};
  totals.expenses.forEach((expense) => {
    const category = getExpenseCategory(expense);
    categoryTotals[category] = (categoryTotals[category] || 0) + Number(expense.amount || 0);
  });

  dom.categoryBreakdown.innerHTML = "";
  const categoryList = Object.entries(categoryTotals).sort((a, b) => b[1] - a[1]);
  if (!categoryList.length) {
    dom.categoryBreakdown.append(createEmptyState("Add expenses to see a breakdown."));
  } else {
    categoryList.forEach(([category, amount]) => {
      dom.categoryBreakdown.append(
        createListItem(category, formatMoney(amount), "pill")
      );
    });
  }

  dom.goalProgress.innerHTML = "";
  if (!data.goals.length) {
    dom.goalProgress.append(createEmptyState("Set a goal to see progress here."));
  } else {
    data.goals
      .slice()
      .sort((a, b) => (a.dueDate || "").localeCompare(b.dueDate || ""))
      .forEach((goal) => {
        const progress = goal.target
          ? Math.min(100, Math.round((Number(goal.saved || 0) / goal.target) * 100))
          : 0;
        const subtitle = `${progress}% of ${formatMoney(goal.target)}`;
        dom.goalProgress.append(createListItem(goal.name, subtitle, "badge"));
      });
  }

  renderCharts(data, filter);
  renderMixCharts(totals);
};

const createListItem = (label, value, badgeClass) => {
  const wrapper = document.createElement("div");
  wrapper.className = "list-item";
  const left = document.createElement("span");
  left.textContent = label;
  const right = document.createElement("span");
  right.className = badgeClass;
  right.textContent = value;
  wrapper.append(left, right);
  return wrapper;
};

const createEmptyState = (message) => {
  const wrapper = document.createElement("div");
  wrapper.className = "list-item";
  wrapper.textContent = message;
  return wrapper;
};

const renderTable = (entries, tableBody, formatRow, emptyColSpan = 5) => {
  tableBody.innerHTML = "";
  if (!entries.length) {
    const row = document.createElement("tr");
    const cell = document.createElement("td");
    cell.colSpan = emptyColSpan;
    cell.textContent = "No entries yet.";
    cell.style.color = "#6b7280";
    cell.style.textAlign = "center";
    row.append(cell);
    tableBody.append(row);
    return;
  }
  entries.forEach((entry) => tableBody.append(formatRow(entry)));
};

const formatRowWithDelete = (cells, onEdit, onDelete) => {
  const row = document.createElement("tr");
  cells.forEach((cellContent) => {
    const cell = document.createElement("td");
    cell.append(cellContent);
    row.append(cell);
  });
  const deleteCell = document.createElement("td");
  const editBtn = document.createElement("button");
  editBtn.type = "button";
  editBtn.className = "ghost-button";
  editBtn.textContent = "Edit";
  editBtn.addEventListener("click", onEdit);
  const deleteBtn = document.createElement("button");
  deleteBtn.type = "button";
  deleteBtn.className = "ghost-button";
  deleteBtn.textContent = "Delete";
  deleteBtn.addEventListener("click", onDelete);
  deleteCell.append(editBtn, deleteBtn);
  row.append(deleteCell);
  return row;
};

const renderTables = (data) => {
  const filter = getActiveFilter();
  const totals = getTotals(data, filter);

  renderTable(totals.income, dom.incomeTable, (entry) =>
    formatRowWithDelete(
      [
        document.createTextNode(entry.name),
        document.createTextNode(entry.incomeType || "—"),
        document.createTextNode(entry.date || "—"),
        document.createTextNode(formatMoney(entry.amount)),
      ],
      () => startEdit("income", entry),
      () => removeEntry("income", entry.id)
    )
  );

  renderTable(totals.expenses, dom.expenseTable, (entry) =>
    formatRowWithDelete(
      [
        document.createTextNode(entry.name),
        document.createTextNode(getExpenseCategory(entry)),
        document.createTextNode(entry.date || "—"),
        document.createTextNode(formatMoney(entry.amount)),
      ],
      () => startEdit("expenses", entry),
      () => removeEntry("expenses", entry.id)
    )
  );

  renderTable(data.goals, dom.goalTable, (entry) =>
    formatRowWithDelete(
      [
        document.createTextNode(entry.name),
        document.createTextNode(formatMoney(entry.target)),
        document.createTextNode(formatMoney(entry.saved || 0)),
        document.createTextNode(entry.dueDate || "—"),
      ],
      () => startEdit("goals", entry),
      () => removeEntry("goals", entry.id)
    )
  );

  renderTable(
    totals.investments,
    dom.investmentTable,
    (entry) => {
    const shares = Number(entry.shares || 0);
    const currentPrice = Number(entry.currentPrice || 0);
    const currentValue = shares && currentPrice ? shares * currentPrice : null;
    const updated = entry.lastPriceUpdated
      ? new Date(entry.lastPriceUpdated).toLocaleDateString()
      : "—";
      return formatRowWithDelete(
        [
          document.createTextNode(entry.name),
          document.createTextNode(entry.symbol || "—"),
          document.createTextNode(shares ? shares.toFixed(4) : "—"),
          document.createTextNode(formatMoney(entry.purchasePrice || 0)),
          document.createTextNode(
            entry.currentPrice ? formatMoney(entry.currentPrice) : "—"
          ),
          document.createTextNode(
            currentValue ? formatMoney(currentValue) : formatMoney(entry.amount || 0)
          ),
          document.createTextNode(updated),
        ],
        () => startEdit("investments", entry),
        () => removeEntry("investments", entry.id)
      );
    },
    8
  );
    formatRowWithDelete(
      [
        document.createTextNode(entry.name),
        document.createTextNode(entry.type || "—"),
        document.createTextNode(entry.date || "—"),
        document.createTextNode(formatMoney(entry.amount)),
      ],
      () => startEdit("investments", entry),
      () => removeEntry("investments", entry.id)
    )
  );

  renderTable(
    data.assets,
    dom.assetTable,
    (entry) => {
    const equity =
      Number(entry.currentValue || 0) - Number(entry.mortgageBalance || 0);
      return formatRowWithDelete(
      [
        document.createTextNode(entry.name),
        document.createTextNode(entry.category || "—"),
        document.createTextNode(formatMoney(entry.purchasePrice || 0)),
        document.createTextNode(formatMoney(entry.currentValue || 0)),
        document.createTextNode(formatMoney(entry.mortgageBalance || 0)),
        document.createTextNode(formatMoney(equity)),
      ],
        () => startEdit("assets", entry),
      () => removeEntry("assets", entry.id)
      );
    },
    7
  );
};

const removeEntry = async (collection, id) => {
  try {
    await fetchJson(`${API_BASE}/${collection}/${id}`, { method: "DELETE" });
    const data = loadData();
    data[collection] = data[collection].filter((entry) => entry.id !== id);
    renderAll(data);
  } catch (error) {
    console.error("Failed to delete entry", error);
    showApiWarning();
  }
};

const addEntry = async (collection, payload) => {
  try {
    const entry = await fetchJson(`${API_BASE}/${collection}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    const data = loadData();
    data[collection] = [entry, ...data[collection]];
    renderAll(data);
  } catch (error) {
    console.error("Failed to add entry", error);
    showApiWarning();
  }
};

const updateEntry = async (collection, id, payload) => {
  try {
    const entry = await fetchJson(`${API_BASE}/${collection}/${id}`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    const data = loadData();
    data[collection] = data[collection].map((item) =>
      item.id === id ? { ...item, ...entry } : item
    );
    renderAll(data);
  } catch (error) {
    console.error("Failed to update entry", error);
    showApiWarning();
  }
};

const submitLabels = {
  income: "Add Income",
  expenses: "Add Expense",
  goals: "Add Goal",
  investments: "Add Investment",
  assets: "Add Asset",
};

const setEditMode = (collection, enabled) => {
  dom.editBanners.forEach((banner) => {
    if (banner.dataset.editBanner === collection) {
      banner.classList.toggle("hidden", !enabled);
    }
  });
  dom.cancelEditButtons.forEach((button) => {
    if (button.dataset.cancelEdit === collection) {
      button.classList.toggle("hidden", !enabled);
    }
  });
  dom.submitLabels.forEach((button) => {
    if (button.dataset.submitLabel === collection) {
      button.textContent = enabled ? "Save Changes" : submitLabels[collection];
    }
  });
};

const clearEditState = (collection) => {
  editState[collection] = null;
  setEditMode(collection, false);
  switch (collection) {
    case "income":
      dom.incomeForm.reset();
      dom.incomeForm.date.value = toISODate(today);
      break;
    case "expenses":
      dom.expenseForm.reset();
      dom.expenseForm.date.value = toISODate(today);
      dom.customCategoryField.classList.add("hidden");
      dom.expenseForm.customCategory.required = false;
      break;
    case "goals":
      dom.goalForm.reset();
      break;
    case "investments":
      dom.investmentForm.reset();
      dom.investmentForm.date.value = toISODate(today);
      break;
    case "assets":
      dom.assetForm.reset();
      break;
    default:
      break;
  }
};

const startEdit = (collection, entry) => {
  editState[collection] = entry.id;
  setEditMode(collection, true);
  switch (collection) {
    case "income":
      dom.incomeForm.name.value = entry.name || "";
      dom.incomeForm.incomeType.value = entry.incomeType || "Salary";
      dom.incomeForm.amount.value = entry.amount || "";
      dom.incomeForm.date.value = entry.date || "";
      dom.incomeForm.category.value = entry.category || "";
      dom.incomeForm.recurring.checked = Boolean(entry.recurring);
      dom.incomeForm.scrollIntoView({ behavior: "smooth", block: "center" });
      break;
    case "expenses":
      dom.expenseForm.name.value = entry.name || "";
      dom.expenseForm.category.value = entry.category || "Rent";
      dom.expenseForm.amount.value = entry.amount || "";
      dom.expenseForm.date.value = entry.date || "";
      dom.expenseForm.customCategory.value = entry.customCategory || "";
      dom.expenseForm.subscriptionName.value = entry.subscriptionName || "";
      dom.expenseForm.recurring.checked = Boolean(entry.recurring);
      const isCustom = entry.category === "Custom";
      dom.customCategoryField.classList.toggle("hidden", !isCustom);
      dom.expenseForm.customCategory.required = isCustom;
      dom.expenseForm.scrollIntoView({ behavior: "smooth", block: "center" });
      break;
    case "goals":
      dom.goalForm.name.value = entry.name || "";
      dom.goalForm.target.value = entry.target || "";
      dom.goalForm.saved.value = entry.saved || "";
      dom.goalForm.dueDate.value = entry.dueDate || "";
      dom.goalForm.scrollIntoView({ behavior: "smooth", block: "center" });
      break;
    case "investments":
      dom.investmentForm.name.value = entry.name || "";
      dom.investmentForm.type.value = entry.type || "";
      dom.investmentForm.symbol.value = entry.symbol || "";
      dom.investmentForm.amount.value = entry.amount || "";
      dom.investmentForm.purchasePrice.value = entry.purchasePrice || "";
      dom.investmentForm.shares.value = entry.shares || "";
      dom.investmentForm.currentPrice.value = entry.currentPrice || "";
      dom.investmentForm.date.value = entry.date || "";
      dom.investmentForm.scrollIntoView({ behavior: "smooth", block: "center" });
      break;
    case "assets":
      dom.assetForm.name.value = entry.name || "";
      dom.assetForm.category.value = entry.category || "";
      dom.assetForm.purchasePrice.value = entry.purchasePrice || "";
      dom.assetForm.currentValue.value = entry.currentValue || "";
      dom.assetForm.mortgageBalance.value = entry.mortgageBalance || "";
      dom.assetForm.date.value = entry.date || "";
      dom.assetForm.scrollIntoView({ behavior: "smooth", block: "center" });
      break;
    default:
      break;
  }
};

const handleIncomeSubmit = async (event) => {
  event.preventDefault();
  const formData = new FormData(event.target);
  const payload = {
    name: formData.get("name"),
    incomeType: formData.get("incomeType"),
    amount: Number(formData.get("amount") || 0),
    date: toISODate(formData.get("date") || today),
    category: formData.get("category"),
    recurring: formData.get("recurring") === "on",
  };
  if (editState.income) {
    await updateEntry("income", editState.income, payload);
    clearEditState("income");
    return;
  }
  await addEntry("income", payload);
  event.target.reset();
  event.target.date.value = toISODate(today);
};

const handleExpenseSubmit = async (event) => {
  event.preventDefault();
  const formData = new FormData(event.target);
  const category = formData.get("category");
  const customCategory = category === "Custom" ? formData.get("customCategory") : "";
  const payload = {
    name: formData.get("name"),
    amount: Number(formData.get("amount") || 0),
    date: toISODate(formData.get("date") || today),
    category,
    customCategory,
    subscriptionName: formData.get("subscriptionName"),
    recurring: formData.get("recurring") === "on",
  };
  if (editState.expenses) {
    await updateEntry("expenses", editState.expenses, payload);
    clearEditState("expenses");
    return;
  }
  await addEntry("expenses", payload);
  event.target.reset();
  event.target.date.value = toISODate(today);
};

const handleGoalSubmit = async (event) => {
  event.preventDefault();
  const formData = new FormData(event.target);
  const payload = {
    name: formData.get("name"),
    target: Number(formData.get("target") || 0),
    saved: Number(formData.get("saved") || 0),
    dueDate: toISODate(formData.get("dueDate")),
  };
  if (editState.goals) {
    await updateEntry("goals", editState.goals, payload);
    clearEditState("goals");
    return;
  }
  await addEntry("goals", payload);
  event.target.reset();
};

const handleInvestmentSubmit = async (event) => {
  event.preventDefault();
  const formData = new FormData(event.target);
  const payload = {
    name: formData.get("name"),
    type: formData.get("type"),
    symbol: formData.get("symbol")
      ? String(formData.get("symbol")).trim().toUpperCase()
      : "",
    amount: Number(formData.get("amount") || 0),
    purchasePrice: Number(formData.get("purchasePrice") || 0),
    shares: Number(formData.get("shares") || 0),
    currentPrice: Number(formData.get("currentPrice") || 0),
    date: toISODate(formData.get("date") || today),
  };
  if (editState.investments) {
    await updateEntry("investments", editState.investments, payload);
    clearEditState("investments");
    return;
  }
  await addEntry("investments", payload);
  event.target.reset();
  event.target.date.value = toISODate(today);
};

const handleAssetSubmit = async (event) => {
  event.preventDefault();
  const formData = new FormData(event.target);
  const payload = {
    name: formData.get("name"),
    category: formData.get("category"),
    purchasePrice: Number(formData.get("purchasePrice") || 0),
    currentValue: Number(formData.get("currentValue") || 0),
    mortgageBalance: Number(formData.get("mortgageBalance") || 0),
    date: toISODate(formData.get("date")),
  };
  if (editState.assets) {
    await updateEntry("assets", editState.assets, payload);
    clearEditState("assets");
    return;
  }
  await addEntry("assets", payload);
  event.target.reset();
};

const bindFormDefaults = () => {
  dom.incomeForm.date.value = toISODate(today);
  dom.expenseForm.date.value = toISODate(today);
  dom.investmentForm.date.value = toISODate(today);
};

const bindEvents = () => {
  if (dom.incomeForm) dom.incomeForm.addEventListener("submit", handleIncomeSubmit);
  if (dom.expenseForm) dom.expenseForm.addEventListener("submit", handleExpenseSubmit);
  if (dom.goalForm) dom.goalForm.addEventListener("submit", handleGoalSubmit);
  if (dom.investmentForm)
    dom.investmentForm.addEventListener("submit", handleInvestmentSubmit);
  if (dom.assetForm) dom.assetForm.addEventListener("submit", handleAssetSubmit);
  if (dom.refreshPricesBtn) {
    dom.refreshPricesBtn.addEventListener("click", () => {
      refreshInvestmentPrices();
    });
  }
  if (dom.expenseForm && dom.expenseForm.category) {
    dom.expenseForm.category.addEventListener("change", (event) => {
      const isCustom = event.target.value === "Custom";
      if (dom.customCategoryField) {
        dom.customCategoryField.classList.toggle("hidden", !isCustom);
      }
      if (dom.expenseForm.customCategory) {
        dom.expenseForm.customCategory.required = isCustom;
        if (!isCustom) {
          dom.expenseForm.customCategory.value = "";
        }
      }
    });
  }
  if (dom.filterMonth) dom.filterMonth.addEventListener("change", () => renderAll(loadData()));
  if (dom.filterYear) dom.filterYear.addEventListener("change", () => renderAll(loadData()));
  dom.cancelEditButtons.forEach((button) => {
    button.addEventListener("click", () => {
      const collection = button.dataset.cancelEdit;
      clearEditState(collection);
    });
  });
  dom.exportDataBtn.addEventListener("click", () => {
    const data = loadData();
    const blob = new Blob([JSON.stringify(data, null, 2)], {
      type: "application/json",
    });
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.download = "budget-tracker-data.json";
    link.click();
    URL.revokeObjectURL(url);
  });
  if (dom.debugRunBtn)
    dom.debugRunBtn.addEventListener("click", () => {
    runDiagnostics();
    });
  if (dom.debugShowDataBtn)
    dom.debugShowDataBtn.addEventListener("click", () => {
    const data = loadData();
    showDebugPanel();
    writeDebug(JSON.stringify(data, null, 2));
    });
  if (dom.debugResetFiltersBtn)
    dom.debugResetFiltersBtn.addEventListener("click", () => {
    if (dom.filterMonth) dom.filterMonth.value = "0";
      if (dom.filterYear) {
        const firstYear = dom.filterYear.options[0]
          ? dom.filterYear.options[0].value
          : "";
        dom.filterYear.value = firstYear;
      }
      renderAll(loadData());
    });
  if (dom.debugCloseBtn)
    dom.debugCloseBtn.addEventListener("click", () => {
      if (dom.debugPanel) dom.debugPanel.classList.add("hidden");
    });
};

const runDiagnostics = () => {
  showDebugPanel();
  writeDebug("Running diagnostics...");
  appendDebug(`storageAvailable: ${storageAvailable}`);
  appendDebug(`localStorage accessible: ${typeof localStorage !== "undefined"}`);
  appendDebug(
    `forms: income=${Boolean(dom.incomeForm)}, expense=${Boolean(
      dom.expenseForm
    )}, investment=${Boolean(dom.investmentForm)}`
  );
  appendDebug(
    `filters: month=${Boolean(dom.filterMonth)}, year=${Boolean(dom.filterYear)}`
  );
  const data = loadData();
  appendDebug(
    `counts: income=${data.income.length}, expenses=${data.expenses.length}, investments=${data.investments.length}, assets=${data.assets.length}`
  );
  appendDebug("Diagnostics complete.");
};

const buildMonthSeries = (data, year) => {
  const months = Array.from({ length: 12 }, (_, index) => index + 1);
  return months.map((month) => {
    const filter = { month, year };
    const totals = getTotals(data, filter);
    return {
      month,
      income: totals.incomeTotal,
      expenses: totals.expenseTotal,
      net: totals.incomeTotal - totals.expenseTotal,
      invested: totals.investmentTotal,
    };
  });
};

const renderCharts = (data, filter) => {
  const assetTotal = data.assets.reduce(
    (sum, asset) =>
      sum + Number(asset.currentValue || 0) - Number(asset.mortgageBalance || 0),
    0
  );
  const investmentValueTotal = data.investments.reduce(
    (sum, entry) => sum + getInvestmentValue(entry),
    0
  );
  const series = buildMonthSeries(data, filter.year);
  renderLineChart(
    dom.cashflowTimeline,
    series.map((point) => ({
      label: monthNames[point.month].slice(0, 3),
      value: point.net,
    }))
  );

  let cumulative = 0;
  const netWorthPoints = series.map((point) => {
    cumulative += point.net + point.invested;
    return {
      label: monthNames[point.month].slice(0, 3),
      value: cumulative + assetTotal + investmentValueTotal,
    };
  });
  renderLineChart(dom.netWorthTrend, netWorthPoints);
};

const getInvestmentValue = (entry) => {
  const shares = Number(entry.shares || 0);
  const currentPrice = Number(entry.currentPrice || 0);
  if (shares && currentPrice) return shares * currentPrice;
  return Number(entry.amount || 0);
};

const refreshInvestmentPrices = async () => {
  const data = loadData();
  const symbols = Array.from(
    new Set(
      data.investments
        .map((entry) => (entry.symbol || "").trim().toUpperCase())
        .filter(Boolean)
    )
  );
  if (!symbols.length) {
    dom.priceStatus.textContent = "Add a symbol to fetch prices.";
    return;
  }

  dom.priceStatus.textContent = "Refreshing prices...";
  try {
    const query = encodeURIComponent(symbols.join(","));
    const endpoint = `https://query1.finance.yahoo.com/v7/finance/quote?symbols=${query}`;
    const url = `https://cors.isomorphic-git.org/${endpoint}`;
    const response = await fetch(url);
    if (!response.ok) throw new Error("Price fetch failed");
    const payload = await response.json();
    const quotes =
      payload && payload.quoteResponse && payload.quoteResponse.result
        ? payload.quoteResponse.result
        : [];
    const priceMap = new Map(
      quotes.map((quote) => [
        quote && quote.symbol ? quote.symbol.toUpperCase() : "",
        quote ? quote.regularMarketPrice : null,
      ])
    );

    const updatedAt = new Date().toISOString();
    const updates = [];
    const nextInvestments = data.investments.map((entry) => {
      const symbol = (entry.symbol || "").toUpperCase();
      if (!symbol || !priceMap.has(symbol)) return entry;
      const price = priceMap.get(symbol);
      if (!price) return entry;
      const updatedEntry = {
        ...entry,
        currentPrice: price,
        lastPriceUpdated: updatedAt,
      };
      updates.push(updatedEntry);
      return updatedEntry;
    });
    await Promise.all(
      updates.map((entry) =>
        fetchJson(`${API_BASE}/investments/${entry.id}`, {
          method: "PUT",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(entry),
        })
      )
    );
    data.investments = nextInvestments;
    dom.priceStatus.textContent = "Prices updated.";
    renderAll(data);
  } catch (error) {
    console.error("Failed to refresh prices", error);
    dom.priceStatus.textContent =
      "Price lookup unavailable. Try again later or enter current price manually.";
  }
};

const renderLineChart = (container, data) => {
  container.innerHTML = "";
  if (!data.length) {
    container.append(createEmptyState("Add entries to see the trend."));
    return;
  }

  const width = 600;
  const height = 180;
  const padding = 24;
  const innerWidth = width - padding * 2;
  const innerHeight = height - padding * 2;
  const values = data.map((point) => point.value);
  const min = Math.min(...values, 0);
  const max = Math.max(...values, 1);
  const range = max - min || 1;

  const points = data.map((point, index) => {
    const x = padding + (innerWidth / (data.length - 1 || 1)) * index;
    const y =
      padding + innerHeight - ((point.value - min) / range) * innerHeight;
    return { x, y, value: point.value, label: point.label };
  });

  const linePath = points
    .map((point, index) => `${index === 0 ? "M" : "L"} ${point.x} ${point.y}`)
    .join(" ");

  const zeroY =
    padding + innerHeight - ((0 - min) / range) * innerHeight;

  const svg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
  svg.setAttribute("viewBox", `0 0 ${width} ${height}`);
  svg.classList.add("line-chart");

  const baseline = document.createElementNS("http://www.w3.org/2000/svg", "line");
  baseline.setAttribute("x1", String(padding));
  baseline.setAttribute("x2", String(width - padding));
  baseline.setAttribute("y1", String(zeroY));
  baseline.setAttribute("y2", String(zeroY));
  baseline.setAttribute("class", "line-chart-baseline");
  svg.append(baseline);

  const path = document.createElementNS("http://www.w3.org/2000/svg", "path");
  path.setAttribute("d", linePath);
  path.setAttribute("class", "line-chart-path");
  svg.append(path);

  points.forEach((point) => {
    const dot = document.createElementNS("http://www.w3.org/2000/svg", "circle");
    dot.setAttribute("cx", String(point.x));
    dot.setAttribute("cy", String(point.y));
    dot.setAttribute("r", "4");
    dot.setAttribute("class", "line-chart-dot");
    svg.append(dot);
  });

  const labels = document.createElement("div");
  labels.className = "line-chart-labels";
  points.forEach((point) => {
    const label = document.createElement("div");
    label.className = "line-chart-label";
    label.innerHTML = `<span>${point.label}</span><small>${formatMoney(
      point.value
    )}</small>`;
    labels.append(label);
  });

  container.append(svg, labels);
};

const renderMixCharts = (totals) => {
  const expenseTotals = {};
  totals.expenses.forEach((expense) => {
    const category = getExpenseCategory(expense);
    expenseTotals[category] = (expenseTotals[category] || 0) + Number(expense.amount || 0);
  });
  renderPieChart(
    dom.expensePie,
    dom.expenseLegend,
    Object.entries(expenseTotals).map(([label, value]) => ({ label, value }))
  );

  const incomeTotals = {};
  totals.income.forEach((income) => {
    const label = income.incomeType || "Other";
    incomeTotals[label] = (incomeTotals[label] || 0) + Number(income.amount || 0);
  });
  renderPieChart(
    dom.incomePie,
    dom.incomeLegend,
    Object.entries(incomeTotals).map(([label, value]) => ({ label, value }))
  );
};

const renderPieChart = (pieEl, legendEl, data) => {
  pieEl.innerHTML = "";
  legendEl.innerHTML = "";

  if (!data.length) {
    pieEl.className = "pie-chart pie-chart-empty";
    pieEl.textContent = "No data";
    return;
  }

  const palette = [
    "#2a9187",
    "#7bdcb5",
    "#b7ede2",
    "#f4c095",
    "#f6d186",
    "#c5b4e3",
    "#8ecae6",
    "#f2a7b3",
  ];

  const total = data.reduce((sum, item) => sum + Number(item.value || 0), 0);
  const slices = data
    .filter((item) => Number(item.value) > 0)
    .sort((a, b) => b.value - a.value)
    .map((item, index) => ({
      ...item,
      color: palette[index % palette.length],
      percent: total ? Math.round((item.value / total) * 100) : 0,
    }));

  const gradientStops = [];
  let current = 0;
  slices.forEach((item) => {
    const next = current + (item.value / total) * 360;
    gradientStops.push(`${item.color} ${current}deg ${next}deg`);
    current = next;
  });

  pieEl.className = "pie-chart";
  pieEl.style.background = `conic-gradient(${gradientStops.join(", ")})`;

  slices.forEach((item) => {
    const row = document.createElement("div");
    row.className = "legend-row";
    const swatch = document.createElement("span");
    swatch.className = "legend-swatch";
    swatch.style.background = item.color;
    const label = document.createElement("span");
    label.textContent = item.label;
    const value = document.createElement("span");
    value.className = "legend-value";
    value.textContent = `${item.percent}%`;
    row.append(swatch, label, value);
    legendEl.append(row);
  });
};

const renderAll = (data) => {
  buildFilters(data);
  renderDashboard(data);
  renderTables(data);
};

const init = async () => {
  try {
    bindFormDefaults();
    bindEvents();
    const data = await refreshData();
    renderAll(data);
  } catch (error) {
    console.error("Init failed", error);
    showDebugPanel();
    appendDebug(
      `Init failed: ${
        error && error.message ? error.message : String(error)
      }`
    );
  }
};

init();
