/** @type {import('tailwindcss').Config} */
module.exports = {
  content: [
    "./index.html",
    "./src/**/*.{js,ts,jsx,tsx}",
  ],
  theme: {
    extend: {
      colors: {
        'base-bg': '#0a0a0c', // Very dark background
        // MODIFIED: Alpha changed from 0.7 to 0.85 for opacity
        'panel-bg': 'rgba(15, 15, 25, 0.85)', // More opaque, translucent for panels
        'panel-border': 'rgba(0, 255, 255, 0.4)', // Cyan border
        'text-primary': '#e0e0e0', // Light gray text
        'text-secondary': '#a0a0a0', // Medium gray text
        'glow-primary': 'rgba(0, 255, 255, 0.2)', // Light cyan glow for backgrounds
        'glow-secondary': '#00ffff', // Bright cyan for highlights and text
        'glow-accent': '#ff00ff', // Magenta accent for errors/alerts
      },
      boxShadow: {
        // Outer glow for panels
        'glow': '0 0 15px rgba(0, 255, 255, 0.4), inset 0 0 8px rgba(0, 255, 255, 0.2)',
        'glow-light': '0 0 8px rgba(0, 255, 255, 0.2)',
        // Inner glow for panel content areas
        'panel-inner-glow': 'inset 0 0 10px rgba(0, 255, 255, 0.1)',
      },
      keyframes: {
        'border-pulse': {
          '0%, 100%': { borderColor: 'rgba(0, 255, 255, 0.4)' },
          '50%': { borderColor: 'rgba(0, 255, 255, 0.8)' },
        }
      },
      animation: {
        'border-pulse': 'border-pulse 4s infinite ease-in-out',
      }
    },
  },
  plugins: [],
}