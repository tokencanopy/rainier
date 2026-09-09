// What a session has to be able to do out of the box: reach a loopback server
// it started itself, drive a real Chromium, assert on what rendered, and write
// a screenshot somebody can look at. Everything it names is synthetic.
const { test, expect } = require('@playwright/test')
const fs = require('node:fs')
const path = require('node:path')

test('renders the page and asserts on what the browser actually laid out', async ({ page }, testInfo) => {
  await page.goto('/')
  await expect(page).toHaveTitle('Rainier browser sample')
  await expect(page.getByRole('heading', { name: 'Sessions', level: 1 })).toBeVisible()
  await expect(page.getByText('sample@rainier.test')).toBeVisible()

  // A real layout question, which is the only kind worth a browser: the cards
  // are a three-column grid on a desktop viewport and a single column on a
  // phone. jsdom cannot answer this.
  const cards = page.locator('.card')
  await expect(cards).toHaveCount(3)
  const boxes = await cards.evaluateAll((els) => els.map((el) => el.getBoundingClientRect().top))
  const sameRow = boxes.every((top) => Math.abs(top - boxes[0]) < 1)
  expect(sameRow).toBe(testInfo.project.name === 'desktop')

  // And a real interaction: a click that has to reach the page and run its
  // handler, not a synthetic event dispatched into a DOM implementation.
  await page.getByRole('button', { name: 'Resume' }).click()
  await expect(page.locator('#count')).toHaveText('1')

  const screenshot = testInfo.outputPath(`${testInfo.project.name}.png`)
  await page.screenshot({ path: screenshot, fullPage: true })
  expect(fs.statSync(screenshot).size).toBeGreaterThan(1000)
  await testInfo.attach(`${testInfo.project.name} screenshot`, { path: screenshot, contentType: 'image/png' })
})

test('has no horizontal overflow at its own viewport', async ({ page }) => {
  await page.goto('/')
  const overflow = await page.evaluate(() => {
    const root = document.documentElement
    return root.scrollWidth - root.clientWidth
  })
  expect(overflow).toBeLessThanOrEqual(0)
})

// The version-matching evidence, asserted rather than described. A
// preinstalled browser is only worth anything if the project own Playwright is
// what picked it, from the path the image advertises, at the revision that
// Playwright version pins. If any of those three stops being true this test
// says which one.
//
// It reads /proc rather than asking Playwright for a path, because
// `chromium.executablePath()` answers for the full Chrome for Testing build
// while `headless: true` actually launches chrome-headless-shell. The running
// process is the only unambiguous answer to "which binary is this".
test('runs the preinstalled baseline, resolved by the project own Playwright', async ({ browser, page }) => {
  await page.goto('/')

  const browsersPath = process.env.PLAYWRIGHT_BROWSERS_PATH
  expect(browsersPath, 'PLAYWRIGHT_BROWSERS_PATH').toBeTruthy()

  const running = fs
    .readdirSync('/proc')
    .filter((entry) => /^\d+$/.test(entry))
    .map((pid) => {
      try {
        return fs.readlinkSync(`/proc/${pid}/exe`)
      } catch {
        return null
      }
    })
    .filter((exe) => exe && /chrome-headless-shell|chrome-linux/.test(exe))

  expect(running.length, 'a browser process').toBeGreaterThan(0)

  // /proc/<pid>/exe is already resolved, so this is the real file behind the
  // workspace cache link — which is how the test tells "the image preinstalled
  // it" apart from "this workspace downloaded it". Both are legitimate; only
  // the first is what a fresh session is being qualified for.
  const exe = running[0]
  expect(exe.startsWith('/usr/local/lib/rainier-browsers/'), `browser executable ${exe}`).toBe(true)

  // The revision directory is the one this project own Playwright pins, which
  // is the whole of "the versions match".
  const registry = require(
    path.join(path.dirname(require.resolve('playwright-core')), 'browsers.json'),
  )
  const pinned = registry.browsers.find((b) => b.name === 'chromium-headless-shell')
  expect(exe).toContain(`chromium_headless_shell-${pinned.revision}`)
  expect(browser.version()).toBe(pinned.browserVersion)
})
