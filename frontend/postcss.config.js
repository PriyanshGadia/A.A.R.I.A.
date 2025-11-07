// postcss.config.js
export default {
  plugins: {
    '@tailwindcss/postcss': {}, // <-- This is the new, correct way
    autoprefixer: {},
  },
};