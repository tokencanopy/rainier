// The loopback web server the sample suite drives. Deliberately node's own
// http and nothing else: a session image qualification must not depend on a
// second package resolving, and the point of the exercise is the browser.
//
// It binds 127.0.0.1 inside the session's own network namespace, which is the
// only interface a test server should ever be on. Nothing outside the session
// container can reach it even if the container has egress.
const http = require('node:http')

const PORT = Number(process.env.PORT || 8973)

// One page, written out here rather than read from disk, so the served bytes
// and the assertions live next to each other. Everything it names is
// synthetic: no account, workspace or session is behind any of it.
const page = `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Rainier browser sample</title>
  <style>
    :root { color-scheme: light }
    * { box-sizing: border-box }
    body { margin: 0; font-family: Arial, Helvetica, sans-serif; color: #10221f }
    header { padding: 16px 24px; background: #0f3d3a; color: #fff }
    main { padding: 24px; max-width: 960px }
    .cards { display: grid; grid-template-columns: repeat(3, 1fr); gap: 16px }
    .card { border: 1px solid #cfdad8; border-radius: 8px; padding: 16px }
    #count { font-variant-numeric: tabular-nums }
    button { min-width: 44px; min-height: 44px; font: inherit }
    @media (max-width: 600px) {
      .cards { grid-template-columns: 1fr }
      main { padding: 16px }
    }
  </style>
</head>
<body>
  <header><h1>Sessions</h1></header>
  <main>
    <p>Signed in as <strong>sample@rainier.test</strong> on <code>runner.invalid</code>.</p>
    <div class="cards">
      <div class="card"><h2>box1</h2><p>running</p></div>
      <div class="card"><h2>box2</h2><p>suspended</p></div>
      <div class="card"><h2>box3</h2><p>stopped</p></div>
    </div>
    <p>Resumed <span id="count">0</span> times.</p>
    <button id="resume" type="button">Resume</button>
  </main>
  <script>
    const count = document.getElementById('count')
    document.getElementById('resume').addEventListener('click', () => {
      count.textContent = String(Number(count.textContent) + 1)
    })
  </script>
</body>
</html>
`

const server = http.createServer((req, res) => {
  if (req.url === '/healthz') {
    res.writeHead(200, { 'content-type': 'text/plain' })
    res.end('ok')
    return
  }
  res.writeHead(200, { 'content-type': 'text/html; charset=utf-8' })
  res.end(page)
})

// Loopback only, explicitly. A server that bound 0.0.0.0 would be reachable
// from anything sharing this network namespace, and "nothing does" is a
// property of the deployment rather than of this file.
server.listen(PORT, '127.0.0.1', () => {
  console.log(`sample server on http://127.0.0.1:${PORT}`)
})

for (const signal of ['SIGINT', 'SIGTERM']) {
  process.on(signal, () => server.close(() => process.exit(0)))
}
