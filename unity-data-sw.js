const DATA_PATH = "/Build/build.data";
const DATA_MANIFEST_PATH = "/Build/data-manifest.json";
const CHUNK_PAUSE_MS = 75;
let dataManifestPromise = null;

self.addEventListener("install", (event) => {
  event.waitUntil(self.skipWaiting());
});

self.addEventListener("activate", (event) => {
  event.waitUntil(self.clients.claim());
});

self.addEventListener("fetch", (event) => {
  const url = new URL(event.request.url);
  const handlesUnityData =
    url.origin === self.location.origin &&
    url.pathname === DATA_PATH &&
    (event.request.method === "GET" || event.request.method === "HEAD");

  if (handlesUnityData) {
    event.respondWith(handleUnityDataRequest(event.request));
  }
});

async function handleUnityDataRequest(request) {
  const manifest = await loadDataManifest();
  const headers = new Headers({
    "Content-Type": "application/octet-stream",
    "Content-Length": String(manifest.totalBytes),
    "Cache-Control": "no-store, no-transform",
    "Accept-Ranges": "none",
    "X-Unity-Data-Version": manifest.version || "unknown",
    "X-Unity-Data-Source": "chunk-stream"
  });

  if (request.method === "HEAD") {
    return new Response(null, { status: 200, headers });
  }

  if (typeof ReadableStream === "undefined") {
    return new Response("ReadableStream is not supported in this browser.", {
      status: 503,
      headers: { "Content-Type": "text/plain; charset=utf-8" }
    });
  }

  return new Response(createChunkStream(manifest.chunks), {
    status: 200,
    headers
  });
}

async function loadDataManifest() {
  if (!dataManifestPromise) {
    dataManifestPromise = (async () => {
      const response = await fetch(DATA_MANIFEST_PATH, { cache: "no-cache" });
      if (!response.ok) {
        throw new Error(`Unable to load Unity data manifest: ${response.status}`);
      }

      const manifest = await response.json();
      validateManifest(manifest);
      return manifest;
    })();
  }
  return dataManifestPromise;
}

function validateManifest(manifest) {
  if (!manifest || !Array.isArray(manifest.chunks) || !manifest.chunks.length) {
    throw new Error("Unity data manifest has no chunks.");
  }

  const totalBytes = manifest.chunks.reduce((sum, chunk) => {
    if (!chunk || typeof chunk.path !== "string" || !Number.isFinite(chunk.bytes)) {
      throw new Error("Unity data manifest contains an invalid chunk entry.");
    }
    return sum + chunk.bytes;
  }, 0);

  if (totalBytes !== manifest.totalBytes) {
    throw new Error(
      `Unity data manifest size mismatch: chunks=${totalBytes}, total=${manifest.totalBytes}`
    );
  }
}

function createChunkStream(chunks) {
  let chunkIndex = 0;
  let currentReader = null;
  let currentAbortController = null;

  return new ReadableStream({
    async pull(controller) {
      while (true) {
        if (!currentReader) {
          if (chunkIndex >= chunks.length) {
            controller.close();
            return;
          }

          const chunk = chunks[chunkIndex++];
          currentAbortController = new AbortController();
          const response = await fetch(chunk.path, {
            cache: "default",
            signal: currentAbortController.signal
          });
          if (!response.ok) {
            throw new Error(`Unable to load Unity data chunk ${chunk.path}: ${response.status}`);
          }

          if (!response.body || !response.body.getReader) {
            const buffer = await response.arrayBuffer();
            controller.enqueue(new Uint8Array(buffer));
            return;
          }

          currentReader = response.body.getReader();
        }

        const result = await currentReader.read();
        if (result.done) {
          currentReader = null;
          currentAbortController = null;
          if (chunkIndex < chunks.length) {
            await delay(CHUNK_PAUSE_MS);
          }
          continue;
        }

        controller.enqueue(result.value);
        return;
      }
    },

    cancel() {
      if (currentReader) {
        currentReader.cancel();
      }
      if (currentAbortController) {
        currentAbortController.abort();
      }
    }
  });
}

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
