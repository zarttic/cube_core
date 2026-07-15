export function createRequestScope() {
  let controller = null;
  let token = 0;

  function cancel() {
    controller?.abort();
    controller = null;
    token += 1;
  }

  return {
    begin() {
      controller?.abort();
      controller = new AbortController();
      token += 1;
      return { token, signal: controller.signal };
    },
    isCurrent(candidate) {
      return candidate === token && !controller?.signal.aborted;
    },
    cancel,
    dispose: cancel,
  };
}
