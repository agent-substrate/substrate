import terser from "@rollup/plugin-terser";

export default {
  input: "./src/simple-yenc.js",
  output: [
    {
      file: "dist/esm.js",
      format: "esm",
    },
    {
      file: "dist/index.js",
      format: "cjs",
    },
  ],
  plugins: [
    terser({
      compress: {
        ecma: 2021,
        passes: 5,
        toplevel: true,
        unsafe: true,
        unsafe_methods: true,
        unsafe_arrows: true,
      },
    }),
  ],
};
