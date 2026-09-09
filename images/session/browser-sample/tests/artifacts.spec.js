// A failing test, run on purpose by scripts/session-image-browser-e2e.sh with
// RAINIER_BROWSER_SMOKE_FAIL=1, because "a failure emits a trace, a screenshot
// and a video" is a claim about the image and the configuration together and
// cannot be checked by a suite that only ever passes.
//
// It is skipped in every other run, so an ordinary `npx playwright test` in
// this directory is green.
const { test, expect } = require('@playwright/test')

test('deliberately fails so the run has artifacts to emit', async ({ page }) => {
  test.skip(process.env.RAINIER_BROWSER_SMOKE_FAIL !== '1', 'artifact probe; set RAINIER_BROWSER_SMOKE_FAIL=1')
  await page.goto('/')
  await expect(page.getByRole('heading', { level: 1 })).toHaveText('this heading does not exist')
})
