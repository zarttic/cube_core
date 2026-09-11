// Minimal pub/sub so client.js can signal an authentication failure without
// importing UI or store modules (which would create an import cycle).

const listeners = new Set();

export function onAuthFailure(listener) {
  listeners.add(listener);
  return () => listeners.delete(listener);
}

export function emitAuthFailure(error) {
  listeners.forEach((listener) => {
    try {
      listener(error);
    } catch {
      // A broken listener must never affect the request that triggered it.
    }
  });
}
