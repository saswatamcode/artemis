// Main application component with routing

import { BrowserRouter, Routes, Route } from 'react-router-dom';
import { MantineProvider } from '@mantine/core';
import '@mantine/core/styles.css';

import { theme } from './theme/theme';
import { AppShell } from './components/layout/AppShell';
import { ErrorBoundary } from './components/common/ErrorBoundary';
import { QueryProvider } from './contexts/QueryContext';
import { MetricsPage } from './pages/MetricsPage';
import { TracePage } from './pages/TracePage';

function App() {
  return (
    <MantineProvider theme={theme}>
      <ErrorBoundary>
        <BrowserRouter>
          <QueryProvider>
            <AppShell>
              <Routes>
                <Route path="/" element={<MetricsPage />} />
                <Route path="/trace/:traceID" element={<TracePage />} />
              </Routes>
            </AppShell>
          </QueryProvider>
        </BrowserRouter>
      </ErrorBoundary>
    </MantineProvider>
  );
}

export default App;
