// The sample suite's configuration, and deliberately an ordinary one: nothing
// here is Rainier-specific, because the point of the qualification is that a
// project's own unmodified Playwright configuration works in a session.
//
// In particular there is no `channel`, no `executablePath`, or launch
// argument. The browser sandbox is required for this baseline. Playwright resolves the browser
// from PLAYWRIGHT_BROWSERS_PATH, which the image points at the workspace
// cache; the image seeds that cache with links to its pinned baseline, so this
// runs with nothing downloaded and no network at all.
const { defineConfig, devices } = require('@playwright/test')

const PORT = Number(process.env.PORT || 8973)
const baseURL = `http://127.0.0.1:${PORT}`

module.exports = defineConfig({
  testDir: './tests',
  // CI here means "this is a qualification run": no `.only` may slip through,
  // and one worker, because the thing being measured is the image rather than
  // the host's core count.
  forbidOnly: !!process.env.CI,
  workers: 1,
  retries: 0,
  reporter: [['list'], ['html', { open: 'never' }]],
  // The artifacts a failure has to leave behind, and the reason ffmpeg is in
  // the image: a trace to open, a screenshot to look at, and a video of the
  // run. All three are retained only on failure, so a green run writes almost
  // nothing to the workspace volume.
  use: {
    baseURL,
    trace: 'retain-on-failure',
    screenshot: 'only-on-failure',
    video: 'retain-on-failure',
  },
  projects: [
    {
      name: 'desktop',
      use: { ...devices['Desktop Chrome'], viewport: { width: 1280, height: 800 }, launchOptions: { chromiumSandbox: true } },
    },
    {
      name: 'phone',
      // A real phone descriptor, not just a narrow window: device scale
      // factor, touch, and the mobile user agent all change what the page
      // does, and a layout assertion that ignored them would be measuring
      // something nobody has.
      use: { ...devices['Pixel 7'], launchOptions: { chromiumSandbox: true } },
    },
  ],
  // Playwright starts and stops this itself, which is half of what the
  // qualification is checking: a session must not be left with a listener on
  // loopback after the suite exits.
  webServer: {
    command: 'node server.js',
    url: `${baseURL}/healthz`,
    reuseExistingServer: false,
    timeout: 60_000,
    stdout: 'pipe',
    stderr: 'pipe',
  },
})
