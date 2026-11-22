import { Channel, invoke } from "@tauri-apps/api/core";
import { save } from "@tauri-apps/plugin-dialog";

type JobStatus = "queued" | "running" | "completed" | "error";

type ProgressUpdate = {
  downloaded: number;
  total: number | null;
  bytesPerSecond: number;
  activeConnections: number;
  maxConnections: number;
};

type Job = {
  id: number;
  url: string;
  output: string;
  connections: number;
  fixedConnections: boolean;
  status: JobStatus;
  progress?: ProgressUpdate;
  error?: string;
};

function element<T extends HTMLElement>(selector: string): T {
  const found = document.querySelector<T>(selector);
  if (!found) throw new Error(`Falta el elemento ${selector}`);
  return found;
}

const form = element<HTMLFormElement>("#download-form");
const urlInput = element<HTMLInputElement>("#url");
const outputInput = element<HTMLInputElement>("#output");
const connectionInput = element<HTMLInputElement>("#connections");
const connectionValue = element<HTMLOutputElement>("#connection-value");
const fixedInput = element<HTMLInputElement>("#fixed");
const jobList = element<HTMLDivElement>("#job-list");
const emptyState = element<HTMLDivElement>("#empty-state");
const jobCount = element<HTMLSpanElement>("#job-count");

const jobs: Job[] = [];
let nextId = 1;
let processing = false;

function filenameFromUrl(value: string): string {
  try {
    const segment = new URL(value).pathname.split("/").filter(Boolean).at(-1);
    const name = segment ? decodeURIComponent(segment) : "descarga.bin";
    return name.replace(/[<>:"/\\|?*\x00-\x1f]/g, "_") || "descarga.bin";
  } catch {
    return "descarga.bin";
  }
}

function mebibytes(bytes: number): string {
  return `${(bytes / (1024 * 1024)).toFixed(1)} MiB`;
}

function createText(tag: string, className: string, value: string): HTMLElement {
  const node = document.createElement(tag);
  node.className = className;
  node.textContent = value;
  return node;
}

function renderJob(job: Job): HTMLElement {
  const card = document.createElement("article");
  card.className = `job-card status-${job.status}`;

  const top = createText("div", "job-top", "");
  const titleBlock = createText("div", "job-title-block", "");
  titleBlock.append(createText("h3", "job-name", filenameFromUrl(job.url)));
  titleBlock.append(createText("p", "job-source", new URL(job.url).host));
  top.append(titleBlock);

  const statusLabels: Record<JobStatus, string> = {
    queued: "En espera", running: "Descargando", completed: "Completado", error: "Error",
  };
  top.append(createText("span", "job-status", statusLabels[job.status]));
  card.append(top);
  card.append(createText("p", "job-destination", job.output));

  const percent = job.progress?.total
    ? Math.min(100, Math.round(job.progress.downloaded / job.progress.total * 100))
    : job.status === "completed" ? 100 : 0;
  const track = createText("div", "progress-track", "");
  const fill = createText("div", "progress-fill", "");
  fill.style.width = `${percent}%`;
  track.append(fill);
  card.append(track);

  const details = createText("div", "job-details", "");
  if (job.progress) {
    const size = job.progress.total
      ? `${mebibytes(job.progress.downloaded)} / ${mebibytes(job.progress.total)}`
      : mebibytes(job.progress.downloaded);
    details.append(createText("span", "", size));
    details.append(createText("span", "", `${mebibytes(job.progress.bytesPerSecond)}/s · ${job.progress.activeConnections}/${job.progress.maxConnections} conexiones`));
  } else {
    details.append(createText("span", "", job.status === "completed" ? "Archivo guardado" : "Pendiente"));
  }
  card.append(details);

  if (job.error) {
    card.append(createText("p", "job-error", job.error));
    const retry = createText("button", "retry-button", "Reintentar") as HTMLButtonElement;
    retry.type = "button";
    retry.addEventListener("click", () => {
      job.status = "queued";
      job.error = undefined;
      job.progress = undefined;
      render();
      void processQueue();
    });
    card.append(retry);
  }
  return card;
}

function render(): void {
  jobCount.textContent = String(jobs.length);
  emptyState.hidden = jobs.length > 0;
  jobList.replaceChildren(...jobs.map(renderJob));
}

async function chooseDestination(): Promise<string | null> {
  try {
    const filename = filenameFromUrl(urlInput.value);
    const defaultPath = outputInput.value.trim() || await invoke<string>("default_download_path", { filename });
    const selected = await save({
      title: "Guardar descarga como",
      defaultPath,
    });
    if (selected) outputInput.value = selected;
    return selected;
  } catch (error) {
    window.alert(`No se pudo abrir el selector de archivos: ${String(error)}`);
    return null;
  }
}

async function processQueue(): Promise<void> {
  if (processing) return;
  processing = true;
  try {
    while (true) {
      const job = jobs.find((item) => item.status === "queued");
      if (!job) break;
      job.status = "running";
      render();
      const channel = new Channel<ProgressUpdate>();
      channel.onmessage = (progress) => {
        job.progress = progress;
        render();
      };
      try {
        await invoke("start_download", {
          url: job.url,
          output: job.output,
          connections: job.connections,
          fixedConnections: job.fixedConnections,
          onProgress: channel,
        });
        job.status = "completed";
        job.error = undefined;
      } catch (error) {
        job.status = "error";
        job.error = String(error);
      }
      render();
    }
  } finally {
    processing = false;
  }
}

element<HTMLButtonElement>("#browse").addEventListener("click", () => {
  void chooseDestination();
});

connectionInput.addEventListener("input", () => {
  connectionValue.value = connectionInput.value;
  connectionValue.textContent = connectionInput.value;
});

form.addEventListener("submit", (event) => {
  event.preventDefault();
  void (async () => {
    const url = urlInput.value.trim();
    if (!/^https?:\/\//i.test(url)) {
      urlInput.setCustomValidity("Usa un enlace HTTP o HTTPS");
      urlInput.reportValidity();
      return;
    }
    urlInput.setCustomValidity("");
    const output = outputInput.value.trim() || await chooseDestination();
    if (!output) return;
    jobs.unshift({
      id: nextId++, url, output,
      connections: Number(connectionInput.value),
      fixedConnections: fixedInput.checked,
      status: "queued",
    });
    urlInput.value = "";
    outputInput.value = "";
    render();
    void processQueue();
  })();
});

urlInput.addEventListener("input", () => urlInput.setCustomValidity(""));
render();
