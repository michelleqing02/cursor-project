const datasetOptions = [
  { value: "player", label: "Player Weekly Usage" },
  { value: "team", label: "Team Efficiency" },
  { value: "receiving_efficiency", label: "Receiving Efficiency (NGS + PFR)" },
  { value: "quarterback_efficiency", label: "Quarterback Efficiency (ESPN QBR)" },
];

const questionForm = document.getElementById("question-form");
const questionInput = document.getElementById("question-input");
const answerSection = document.getElementById("answer");
const answerText = document.getElementById("answer-text");
const answerTable = document.getElementById("answer-table");

const filtersForm = document.getElementById("filters-form");
const datasetSelect = document.getElementById("dataset-select");
const seasonSelect = document.getElementById("season-select");
const weekSelect = document.getElementById("week-select");
const teamSelect = document.getElementById("team-select");
const playerInput = document.getElementById("player-filter");
const limitInput = document.getElementById("limit-input");
const sortSelect = document.getElementById("sort-select");

const statsSection = document.getElementById("stats-results");
const statsSummary = document.getElementById("stats-summary");
const statsTable = document.getElementById("stats-table");
const statsStatus = document.getElementById("stats-status");

const refreshButton = document.getElementById("refresh-button");
const refreshStatus = document.getElementById("status-message");

const setRefreshStatus = (message, tone = "info") => {
  refreshStatus.textContent = message;
  refreshStatus.dataset.tone = tone;
};

const setStatsStatus = (message, tone = "info") => {
  statsStatus.textContent = message;
  statsStatus.dataset.tone = tone;
};

const formatLabel = (value) => value.replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());

const formatCell = (value) => {
  if (value === null || value === undefined || Number.isNaN(value)) return "N/A";
  if (typeof value === "number") {
    const rounded = Number.parseFloat(value.toFixed(3));
    return Number.isInteger(rounded) ? rounded.toString() : rounded.toString();
  }
  return value;
};

const renderTable = (rows, container, columns) => {
  if (!rows || rows.length === 0) {
    container.innerHTML = "";
    return;
  }

  const resolvedColumns = columns && columns.length ? columns : Object.keys(rows[0]);
  const table = document.createElement("table");
  const thead = document.createElement("thead");
  const headerRow = document.createElement("tr");

  resolvedColumns.forEach((col) => {
    const th = document.createElement("th");
    th.textContent = formatLabel(col);
    headerRow.appendChild(th);
  });

  thead.appendChild(headerRow);
  table.appendChild(thead);

  const tbody = document.createElement("tbody");
  rows.forEach((row) => {
    const tr = document.createElement("tr");
    resolvedColumns.forEach((col) => {
      const td = document.createElement("td");
      td.textContent = formatCell(row[col]);
      tr.appendChild(td);
    });
    tbody.appendChild(tr);
  });

  table.appendChild(tbody);
  container.innerHTML = "";
  container.appendChild(table);
};

const populateSelect = (select, values, placeholder = "All", formatter = (v) => v) => {
  const preserved = select.value;
  select.innerHTML = "";
  const defaultOption = document.createElement("option");
  defaultOption.value = "";
  defaultOption.textContent = placeholder;
  select.appendChild(defaultOption);

  values.forEach((value) => {
    const option = document.createElement("option");
    option.value = value;
    option.textContent = formatter(value);
    select.appendChild(option);
  });

  if (preserved && values.map((value) => String(value)).includes(preserved)) {
    select.value = preserved;
  }
};

const updateSortOptions = (columns = [], preferred) => {
  if (!sortSelect) return;
  const previous = sortSelect.value;
  sortSelect.innerHTML = "";

  const autoOption = document.createElement("option");
  autoOption.value = "";
  autoOption.textContent = "Auto";
  sortSelect.appendChild(autoOption);

  columns.forEach((column) => {
    const option = document.createElement("option");
    option.value = column;
    option.textContent = formatLabel(column);
    sortSelect.appendChild(option);
  });

  const target = preferred || previous;
  if (target && Array.from(sortSelect.options).some((opt) => opt.value === target)) {
    sortSelect.value = target;
  }
};

const populateDatasetOptions = () => {
  if (!datasetSelect) return;
  datasetOptions.forEach(({ value, label }) => {
    const option = document.createElement("option");
    option.value = value;
    option.textContent = label;
    datasetSelect.appendChild(option);
  });
};

const fetchMetadata = async (dataset) => {
  const url = dataset ? `/api/stats/metadata?dataset=${encodeURIComponent(dataset)}` : "/api/stats/metadata";
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`Metadata request failed (${response.status})`);
  }
  return response.json();
};

const refreshMetadata = async (dataset) => {
  try {
    setStatsStatus("Loading metadata...");
    const meta = await fetchMetadata(dataset);
    const payload = meta[dataset] || meta;
    const seasons = payload.seasons || [];
    const weeks = payload.weeks || [];
    const teams = payload.teams || [];

    populateSelect(seasonSelect, seasons, "All seasons");
    populateSelect(weekSelect, weeks, "All weeks");
    populateSelect(teamSelect, teams, "All teams", (value) => value || "All teams");

    seasonSelect.disabled = seasons.length === 0;
    weekSelect.disabled = weeks.length === 0;
    teamSelect.disabled = teams.length === 0;
    setStatsStatus("Ready", "success");
  } catch (error) {
    console.error(error);
    setStatsStatus("Metadata unavailable", "error");
  }
};

const runStatsQuery = async () => {
  if (!filtersForm) return;
  const dataset = datasetSelect.value;
  if (!dataset) return;

  const params = new URLSearchParams({ dataset });
  if (seasonSelect.value) params.set("season", seasonSelect.value);
  if (weekSelect.value) params.set("week", weekSelect.value);
  if (teamSelect.value) params.set("team", teamSelect.value);
  if (playerInput.value) params.set("player", playerInput.value.trim());
  if (limitInput.value) params.set("limit", limitInput.value);
  if (sortSelect.value) params.set("sort", sortSelect.value);

  setStatsStatus("Loading stats...");
  statsSection.hidden = true;

  try {
    const response = await fetch(`/api/stats?${params.toString()}`);
    if (!response.ok) {
      throw new Error(`Stats request failed (${response.status})`);
    }
    const payload = await response.json();

    if (!payload.rows || payload.rows.length === 0) {
      setStatsStatus("No data for that filter set", "error");
      statsTable.innerHTML = "";
      statsSummary.textContent = "";
      return;
    }

    const datasetLabel = datasetOptions.find((item) => item.value === dataset)?.label || formatLabel(dataset);
    renderTable(payload.rows, statsTable, payload.columns);
    updateSortOptions(payload.columns, payload.filters?.sort);

    const activeFilters = Object.entries(payload.filters || {})
      .filter(([key, value]) => value && !["limit", "sort"].includes(key))
      .map(([key, value]) => `${formatLabel(key)}: ${value}`)
      .join(", ");

    const limit = payload.filters?.limit || payload.rows.length;
    statsSummary.textContent = `Showing ${payload.rows.length} of ${limit} rows from ${datasetLabel}${activeFilters ? ` (${activeFilters})` : ""}.`;

    statsSection.hidden = false;
    setStatsStatus("Ready", "success");
  } catch (error) {
    console.error(error);
    setStatsStatus("Stats lookup failed", "error");
  }
};

filtersForm?.addEventListener("submit", async (event) => {
  event.preventDefault();
  await runStatsQuery();
});

datasetSelect?.addEventListener("change", async () => {
  await refreshMetadata(datasetSelect.value);
  await runStatsQuery();
});

questionForm?.addEventListener("submit", async (event) => {
  event.preventDefault();
  const question = questionInput.value.trim();
  if (!question) return;

  setRefreshStatus("Querying...");
  answerSection.hidden = true;

  try {
    const response = await fetch("/api/ask", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ question }),
    });

    if (!response.ok) {
      throw new Error(`Request failed with status ${response.status}`);
    }

    const payload = await response.json();
    answerText.textContent = payload.answer;
    renderTable(payload.data, answerTable);
    answerSection.hidden = false;
    setRefreshStatus("Ready", "success");
  } catch (error) {
    console.error(error);
    setRefreshStatus("Unable to retrieve an answer. Check the logs and try again.", "error");
  }
});

refreshButton?.addEventListener("click", async () => {
  setRefreshStatus("Refreshing props and stats...");
  refreshButton.disabled = true;
  try {
    const response = await fetch("/api/update-data", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ include_props: true, include_stats: true }),
    });

    if (!response.ok) {
      throw new Error(`Refresh failed with status ${response.status}`);
    }

    const payload = await response.json();
    const statusLines = payload.statuses
      .map((status) => `${status.dataset}: ${status.records} records${status.message ? ` (${status.message})` : ""}`)
      .join(" | ");
    setRefreshStatus(`Refresh complete | ${statusLines}`, "success");
    await refreshMetadata(datasetSelect.value);
    await runStatsQuery();
  } catch (error) {
    console.error(error);
    setRefreshStatus("Refresh failed. See console for details.", "error");
  } finally {
    refreshButton.disabled = false;
  }
});

const bootstrap = async () => {
  populateDatasetOptions();
  if (datasetSelect && datasetSelect.options.length > 0) {
    datasetSelect.value = datasetOptions[0].value;
  }
  await refreshMetadata(datasetSelect.value);
  await runStatsQuery();
  setRefreshStatus("Ready");
  questionInput?.focus();
};

bootstrap().catch((error) => {
  console.error("Failed to bootstrap dashboard", error);
  setStatsStatus("Dashboard failed to initialise", "error");
});
