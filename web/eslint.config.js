import js from '@eslint/js'
import reactHooks from 'eslint-plugin-react-hooks'
import reactRefresh from 'eslint-plugin-react-refresh'
import globals from 'globals'
import tseslint from 'typescript-eslint'

export default tseslint.config(
  { ignores: ['dist', 'node_modules', '../internal/web/dist'] },

  {
    files: ['**/*.{ts,tsx}'],
    extends: [js.configs.recommended, ...tseslint.configs.recommended],
    languageOptions: {
      ecmaVersion: 2023,
      globals: globals.browser,
    },
    plugins: {
      'react-hooks': reactHooks,
      'react-refresh': reactRefresh,
    },
    rules: {
      ...reactHooks.configs.recommended.rules,
      'react-refresh/only-export-components': ['warn', { allowConstantExport: true }],

      // Unused args prefixed with _ are a deliberate signal, not an oversight.
      '@typescript-eslint/no-unused-vars': [
        'error',
        { argsIgnorePattern: '^_', varsIgnorePattern: '^_' },
      ],

      // The dashboard consumes a WebSocket protocol with a versioned envelope.
      // `any` there means the protocol types are not being enforced at exactly
      // the boundary where enforcing them matters most.
      '@typescript-eslint/no-explicit-any': 'error',

      // A floating promise in a realtime UI is a silently swallowed reconnect.
      '@typescript-eslint/no-floating-promises': 'off', // needs type-aware linting; enabled in Phase 5
    },
  },

  {
    files: ['scripts/**/*.mjs', 'vite.config.ts'],
    languageOptions: { globals: globals.node },
  },
)
