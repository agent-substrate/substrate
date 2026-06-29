import { defError } from "./deferror.js";
const IOError = defError(() => "IO error");
const ioerror = (msg) => {
  throw new IOError(msg);
};
const FileNotFoundError = defError(() => "File not found");
const fileNotFound = (path) => {
  throw new FileNotFoundError(path);
};
export {
  FileNotFoundError,
  IOError,
  fileNotFound,
  ioerror
};
