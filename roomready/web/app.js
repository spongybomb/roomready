"use strict";
// ROOMREADY local demo UI (plain JS, no external dependencies).
const $ = (id) => document.getElementById(id);
let selectedOrder = null;

function esc(v) {
  return String(v === undefined || v === null ? "" : v)
    .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
}

function toast(msg, isError) {
  const t = $("toast");
  t.textContent = msg;
  t.className = isError ? "err" : "";
  t.hidden = false;
  clearTimeout(toast.timer);
  toast.timer = setTimeout(() => { t.hidden = true; }, 4000);
}

async function call(method, path, body, opts) {
  opts = opts || {};
  const headers = { "Content-Type": "application/json", "X-Demo-User": opts.asAdmin ? "admin" : $("user").value };
  if (opts.idempotencyKey) headers["Idempotency-Key"] = opts.idempotencyKey;
  const res = await fetch(path, { method, headers, body: body ? JSON.stringify(body) : undefined });
  const data = await res.json().catch(() => ({}));
  if (!res.ok && !opts.quiet) toast(`${res.status} ${data.error || ""}: ${data.message || ""}`, true);
  return { status: res.status, data };
}

function newKey() {
  $("idemKey").value = "ui-" + Math.random().toString(36).slice(2, 10);
}

function isoWithOffset(date, offsetHours) {
  const shifted = new Date(date.getTime() + offsetHours * 3600 * 1000);
  const sign = offsetHours >= 0 ? "+" : "-";
  const hh = String(Math.abs(offsetHours)).padStart(2, "0");
  return shifted.toISOString().slice(0, 19) + `${sign}${hh}:00`;
}

async function refreshClock() {
  const { data } = await call("GET", "/admin/clock", null, { asAdmin: true, quiet: true });
  if (data.now) {
    $("clock").textContent = `clock: ${data.now}` + (data.nextDueAt ? ` | next due: ${data.nextDueAt}` : "");
    if (!$("checkIn").value) {
      const d = new Date(data.now);
      d.setUTCDate(d.getUTCDate() + 2);
      d.setUTCHours(7, 0, 0, 0); // 15:00 at +08:00
      $("checkIn").value = isoWithOffset(d, 8);
    }
  }
}

async function loadScenarios() {
  const { data } = await call("GET", "/scenarios");
  $("scenario").innerHTML = (data.scenarios || [])
    .map((s) => `<option value="${esc(s.id)}">${esc(s.id)} - ${esc(s.label)}</option>`).join("");
}

async function saveProfile() {
  const body = {
    displayName: $("displayName").value,
    sizes: { slippers: $("sizeSlippers").value, bathrobe: $("sizeBathrobe").value },
    allergyFlags: [...document.querySelectorAll(".allergy:checked")].map((c) => c.value),
    preferredBrands: $("brands").value.split(",").map((s) => s.trim()).filter(Boolean),
  };
  const { status } = await call("POST", "/profile", body);
  if (status === 200) toast(`Profile saved for ${$("user").value}`);
}

async function createOrder() {
  const body = {
    flight: $("flight").value, hotelId: $("hotelId").value, roomReservationRef: $("roomRef").value,
    checkIn: $("checkIn").value, scenario: $("scenario").value,
  };
  const { status, data } = await call("POST", "/orders", body, { idempotencyKey: $("idemKey").value });
  if (status === 201) toast(`Order accepted: ${data.orderId} (due ${data.dueAt})`);
  if (status === 200) toast(`Idempotent replay: same order ${data.orderId}`);
  if (status === 201 || status === 200) { selectedOrder = data.orderId; await refreshAll(); }
}

async function tick(body) {
  const { data } = await call("POST", "/admin/tick", body, { asAdmin: true });
  $("tickResult").textContent = JSON.stringify(data, null, 1);
  await refreshAll();
}

async function refreshOrders() {
  const { data } = await call("GET", "/orders", null, { quiet: true });
  const rows = (data.orders || []).map((o) => `
    <tr class="${o.orderId === selectedOrder ? "selected" : ""}">
      <td><code>${esc(o.orderId.slice(0, 8))}</code></td>
      <td><span class="badge s-${esc(o.status)}">${esc(o.status)}</span>${o.cancelRequested ? " (cancel requested)" : ""}</td>
      <td>${esc(o.checkIn)}</td><td>${esc(o.dueAt)}</td><td>${esc(o.scenario)}</td>
      <td><button class="secondary" data-id="${esc(o.orderId)}">Open</button></td></tr>`);
  $("orders").querySelector("tbody").innerHTML = rows.join("") || `<tr><td colspan="6">No orders for this user.</td></tr>`;
  $("orders").querySelectorAll("button[data-id]").forEach((b) =>
    b.addEventListener("click", () => { selectedOrder = b.dataset.id; refreshAll(); }));
}

async function refreshDetail() {
  if (!selectedOrder) { $("detailCard").hidden = true; return; }
  const { status, data } = await call("GET", `/orders/${selectedOrder}`, null, { quiet: true });
  if (status !== 200) { $("detailCard").hidden = true; return; }
  $("detailCard").hidden = false;
  $("detailId").textContent = data.orderId;
  $("detailStatus").textContent = data.status;
  $("detailStatus").className = `badge s-${data.status}`;
  $("ledger").querySelector("tbody").innerHTML = data.ledger.map((e) => `
    <tr><td>${esc(e.action)}</td><td class="s-${esc(e.status)}">${esc(e.status)}</td><td>${esc(e.attempts)}</td>
    <td>${e.reconciled ? "yes" : ""}</td><td class="mono small">${esc(JSON.stringify(e.providerRef || {}))}</td></tr>`).join("")
    || `<tr><td colspan="5">No provider actions yet.</td></tr>`;
  $("comps").querySelector("tbody").innerHTML = data.compensations.map((c) => `
    <tr><td>${esc(c.slot)}</td><td class="s-${esc(c.status)}">${esc(c.status)}</td><td>${esc(c.method)}</td>
    <td class="small">${esc(c.lastError)}</td></tr>`).join("") || `<tr><td colspan="4">None.</td></tr>`;
  $("detailInfo").innerHTML = `generation ${esc(data.generation)} | cancelRequested ${esc(data.cancelRequested)}`
    + (data.failureReason ? ` | failure: ${esc(data.failureReason)}` : "")
    + (data.unresolved && data.unresolved.length ? ` | unresolved: ${esc(data.unresolved.join("; "))}` : "");
  const audit = await call("GET", `/admin/orders/${selectedOrder}/audit`, null, { asAdmin: true, quiet: true });
  $("audit").innerHTML = (audit.data.audit || []).map((a) => `
    <li><span class="at">${esc(a.at)}</span> <span class="ev">${esc(a.event)}</span> <em>${esc(a.actor)}</em>
    <span class="mono">${esc(JSON.stringify(a.detail))}</span></li>`).join("");
}

async function refreshProviders() {
  const { data } = await call("GET", "/admin/providers", null, { asAdmin: true, quiet: true });
  const fields = { retailer: ["reservationStatus", "paymentStatus", "amountCents"], courier: ["deliveryStatus", "bookingId"], hotel: ["setupStatus", "setupId"] };
  $("providers").innerHTML = Object.keys(fields).map((p) => `
    <div><h3>${esc(p)}</h3><table><thead><tr><th>Order</th>${fields[p].map((f) => `<th>${esc(f)}</th>`).join("")}</tr></thead>
    <tbody>${(data[p] || []).map((r) => `<tr><td><code>${esc(r.orderId.slice(0, 8))}</code></td>${fields[p].map((f) => `<td>${esc(r[f])}</td>`).join("")}</tr>`).join("")
      || `<tr><td colspan="4">No records.</td></tr>`}</tbody></table></div>`).join("");
  $("executions").textContent = (data.executions || [])
    .map((e) => `${e.name}  ${e.status}  end=${e.endState}\n   ${e.states.join(" > ")}`).join("\n") || "No executions yet.";
}

async function refreshAll() {
  await Promise.all([refreshClock(), refreshOrders(), refreshDetail(), refreshProviders()]);
}

$("saveProfile").addEventListener("click", saveProfile);
$("createOrder").addEventListener("click", createOrder);
$("newKey").addEventListener("click", newKey);
$("tick").addEventListener("click", () => tick({}));
$("tickDue").addEventListener("click", () => tick({ fastForwardToDue: true }));
$("tickAdvance").addEventListener("click", () => tick({ advanceHours: Number($("advanceHours").value) || 0 }));
$("refresh").addEventListener("click", refreshAll);
$("refreshProviders").addEventListener("click", refreshProviders);
$("user").addEventListener("change", () => { selectedOrder = null; refreshAll(); });
$("cancel").addEventListener("click", async () => {
  const { status, data } = await call("POST", `/orders/${selectedOrder}/cancel`);
  if (status === 200 || status === 202) toast(`Cancel accepted: status ${data.status}`);
  await refreshAll();
});

newKey();
loadScenarios();
refreshAll();
