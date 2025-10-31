const form = document.getElementById("question-form");
const input = document.getElementById("question-input");
const answerSection = document.getElementById("answer");
const answerText = document.getElementById("answer-text");
const answerTable = document.getElementById("answer-table");
const refreshButton = document.getElementById("refresh-button");
const statusMessage = document.getElementById("status-message");

const renderTable = (rows) => {
  if (!rows || rows.length === 0) {
    answerTable.innerHTML = "";
    return;
  }

  const columns = Object.keys(rows[0]);
  const table = document.createElement("table");
  const thead = document.createElement("thead");
  const headerRow = document.createElement("tr");

  columns.forEach((col) => {
    const th = document.createElement("th");
    th.textContent = col.replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
    headerRow.appendChild(th);
  });

  thead.appendChild(headerRow);
  table.appendChild(thead);

  const tbody = document.createElement("tbody");
  rows.forEach((row) => {
    const tr = document.createElement("tr");
    columns.forEach((col) => {
      const td = document.createElement("td");
      const value = row[col];
      td.textContent = value ?? "?";
      tr.appendChild(td);
    });
    tbody.appendChild(tr);
  });

  table.appendChild(tbody);
  answerTable.innerHTML = "";
  answerTable.appendChild(table);
};

const setStatus = (message, tone = "info") => {
  statusMessage.textContent = message;
  statusMessage.dataset.tone = tone;
};

form.addEventListener("submit", async (event) => {
  event.preventDefault();
  const question = input.value.trim();
  if (!question) return;

  setStatus("Querying?");
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
    renderTable(payload.data);
    answerSection.hidden = false;
    setStatus("Ready", "success");
  } catch (error) {
    console.error(error);
    setStatus("Unable to retrieve an answer. Check the logs and try again.", "error");
  }
});

refreshButton.addEventListener("click", async () => {
  setStatus("Refreshing props and stats?");
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
      .join(" ? ");
    setStatus(`Refresh complete ? ${statusLines}`, "success");
  } catch (error) {
    console.error(error);
    setStatus("Refresh failed. See console for details.", "error");
  } finally {
    refreshButton.disabled = false;
  }
});

setStatus("Ready");
input.focus();
