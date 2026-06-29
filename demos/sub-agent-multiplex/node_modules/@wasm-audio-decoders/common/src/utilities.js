export const assignNames = (Class, name) => {
  Object.defineProperty(Class, "name", { value: name });
};
