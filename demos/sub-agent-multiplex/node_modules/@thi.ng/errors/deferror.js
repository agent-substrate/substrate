const defError = (prefix, suffix = (msg) => msg !== void 0 ? ": " + msg : "") => class extends Error {
  origMessage;
  constructor(msg) {
    super(prefix(msg) + suffix(msg));
    this.origMessage = msg !== void 0 ? String(msg) : "";
  }
};
export {
  defError
};
