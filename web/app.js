const fileInput = document.querySelector("#file-input");
const status = document.querySelector("#status");
const summaryTitle = document.querySelector("#summary-title");
const summary = document.querySelector("#summary");
const warnings = document.querySelector("#warnings");
const playersTitle = document.querySelector("#players-title");
const players = document.querySelector("#players");
const diagnosesTitle = document.querySelector("#diagnoses-title");
const diagnoses = document.querySelector("#diagnoses");
const rawTitle = document.querySelector("#raw-title");
const rawOutput = document.querySelector("#raw-output");

const reportSummaryEntries = [
  ["Replay ID", (report) => report.meta.replay_id],
  ["Date", (report) => report.meta.date],
  ["Map", (report) => report.meta.map],
  ["Mode", (report) => report.meta.mode],
  ["Score", (report) => `${report.meta.final_score.blue} - ${report.meta.final_score.orange}`],
  ["Winner", (report) => report.meta.winner ?? "draw"],
  ["Parse quality", (report) => report.availability.parse_quality],
];

const batchSummaryEntries = [
  ["Analysis version", (summaryData) => summaryData.analysis_version],
  ["Matches", (summaryData) => summaryData.matches.length],
  ["Team aggregates", (summaryData) => summaryData.team_aggregate.length],
  ["Player aggregates", (summaryData) => summaryData.player_aggregate.length],
];

function setStatus(message, isError = false) {
  status.textContent = message;
  status.classList.toggle("error", isError);
}

function renderSummary(title, entries, source) {
  summaryTitle.textContent = title;
  summary.classList.remove("empty");
  summary.innerHTML = entries
    .map(
      ([label, resolve]) =>
        `<dt>${label}</dt><dd>${String(resolve(source))}</dd>`,
    )
    .join("");
}

function renderWarnings(items) {
  const content = items.length
    ? items.map((warning) => `<li>${warning}</li>`).join("")
    : "<li>No warnings.</li>";
  warnings.classList.remove("empty");
  warnings.innerHTML = content;
}

function formatMetric(metric) {
  if (!metric) {
    return "n/a";
  }
  const value = metric.value == null ? "n/a" : Number(metric.value).toFixed(2);
  return `${value} (${metric.quality})`;
}

function renderReportPlayers(report) {
  playersTitle.textContent = "Players";
  if (!report.player_metrics.length) {
    players.classList.add("empty");
    players.textContent = "No player metrics available.";
    return;
  }

  const rows = report.player_metrics
    .map((player) => {
      const highlights = [
        ["score", player.metrics.score],
        ["goals", player.metrics.goals],
        ["shots", player.metrics.shots],
        ["saves", player.metrics.saves],
        ["avg_speed", player.metrics.avg_speed],
        ["boost_avg", player.metrics.boost_avg],
      ]
        .map(([label, metric]) => `<div><strong>${label}</strong>: ${formatMetric(metric)}</div>`)
        .join("");
      return `
        <tr>
          <td>${player.player_name}</td>
          <td>${player.team}</td>
          <td>${highlights}</td>
        </tr>
      `;
    })
    .join("");

  players.classList.remove("empty");
  players.innerHTML = `
    <table>
      <thead>
        <tr>
          <th>Player</th>
          <th>Team</th>
          <th>Highlights</th>
        </tr>
      </thead>
      <tbody>${rows}</tbody>
    </table>
  `;
}

function renderBatchPlayers(summaryData) {
  playersTitle.textContent = "Player aggregates";
  if (!summaryData.player_aggregate.length) {
    players.classList.add("empty");
    players.textContent = "No player aggregates available.";
    return;
  }

  const rows = summaryData.player_aggregate
    .map((player) => {
      const highlights = [
        ["matches", player.matches ?? "n/a"],
        ["wins", player.wins ?? "n/a"],
        ["score", formatMetric(player.metrics.score)],
        ["goals", formatMetric(player.metrics.goals)],
        ["avg_speed", formatMetric(player.metrics.avg_speed)],
      ]
        .map(([label, value]) => `<div><strong>${label}</strong>: ${value}</div>`)
        .join("");
      return `
        <tr>
          <td>${player.player_name}</td>
          <td>${player.team}</td>
          <td>${highlights}</td>
        </tr>
      `;
    })
    .join("");

  players.classList.remove("empty");
  players.innerHTML = `
    <table>
      <thead>
        <tr>
          <th>Player</th>
          <th>Team</th>
          <th>Highlights</th>
        </tr>
      </thead>
      <tbody>${rows}</tbody>
    </table>
  `;
}

function renderReportDiagnoses(report) {
  diagnosesTitle.textContent = "Concede diagnoses";
  if (!report.concede_diagnoses.length) {
    diagnoses.classList.add("empty");
    diagnoses.textContent = "No concede diagnoses available.";
    return;
  }

  diagnoses.classList.remove("empty");
  diagnoses.innerHTML = report.concede_diagnoses
    .map((diagnosis) => {
      const labels = diagnosis.labels.length
        ? diagnosis.labels
            .map(
              (label) =>
                `<li><span class="pill">${label.label}</span> score ${label.score.toFixed(2)}</li>`,
            )
            .join("")
        : "<li>No labels.</li>";
      return `
        <article>
          <h3>Goal ${diagnosis.goal_index}</h3>
          <p>Window: ${diagnosis.window_start.toFixed(2)}s - ${diagnosis.window_end.toFixed(2)}s</p>
          <ul class="list">${labels}</ul>
        </article>
      `;
    })
    .join("");
}

function renderBatchDiagnoses(summaryData) {
  diagnosesTitle.textContent = "Matches";
  if (!summaryData.matches.length) {
    diagnoses.classList.add("empty");
    diagnoses.textContent = "No matches available.";
    return;
  }

  diagnoses.classList.remove("empty");
  diagnoses.innerHTML = summaryData.matches
    .map(
      (match) => `
        <article>
          <h3>${match.replay_id}</h3>
          <p>${match.date} · ${match.map}</p>
          <p>Score: ${match.final_score.blue} - ${match.final_score.orange}</p>
          <p>Winner: ${match.winner ?? "draw"}</p>
          <p>Diagnoses: ${match.diagnosis_count}</p>
        </article>
      `,
    )
    .join("");
}

function renderRaw(report) {
  rawTitle.textContent = "Raw JSON";
  rawOutput.classList.remove("empty");
  rawOutput.textContent = JSON.stringify(report, null, 2);
}

function isBatchSummary(data) {
  return Array.isArray(data?.matches) && Array.isArray(data?.team_aggregate);
}

function renderBatchSummary(summaryData) {
  renderSummary("Batch summary", batchSummaryEntries, summaryData);
  renderWarnings(summaryData.warnings ?? []);
  renderBatchPlayers(summaryData);
  renderBatchDiagnoses(summaryData);
  renderRaw(summaryData);
}

function renderAnalysisReport(report) {
  renderSummary("Match summary", reportSummaryEntries, report);
  renderWarnings(report.warnings ?? []);
  renderReportPlayers(report);
  renderReportDiagnoses(report);
  renderRaw(report);
}

function main() {
  setStatus("Choose an rl-coach output JSON file to begin.");
  fileInput.addEventListener("change", async (event) => {
    const [file] = event.target.files ?? [];
    if (!file) {
      return;
    }

    setStatus(`Loading ${file.name}...`);
    try {
      const text = await file.text();
      const data = JSON.parse(text);
      if (isBatchSummary(data)) {
        renderBatchSummary(data);
        setStatus(`Loaded batch summary from ${file.name}.`);
      } else {
        renderAnalysisReport(data);
        setStatus(`Loaded match report from ${file.name}.`);
      }
    } catch (error) {
      setStatus(`Failed to load JSON: ${error}`, true);
    }
  });
}

main();
