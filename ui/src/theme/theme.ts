// Mantine theme configuration

import { createTheme } from '@mantine/core';

export const theme = createTheme({
  primaryColor: 'blue',
  fontFamily: 'Inter, system-ui, Avenir, Helvetica, Arial, sans-serif',
  fontFamilyMonospace: 'ui-monospace, SFMono-Regular, Monaco, Consolas, monospace',

  colors: {
    // You can add custom colors here if needed
  },

  headings: {
    fontWeight: '600',
    sizes: {
      h1: { fontSize: '2rem', lineHeight: '2.5rem' },
      h2: { fontSize: '1.5rem', lineHeight: '2rem' },
      h3: { fontSize: '1.25rem', lineHeight: '1.75rem' },
    },
  },

  components: {
    Button: {
      defaultProps: {
        size: 'sm',
      },
    },
    TextInput: {
      defaultProps: {
        size: 'sm',
      },
    },
    Select: {
      defaultProps: {
        size: 'sm',
      },
    },
  },
});
