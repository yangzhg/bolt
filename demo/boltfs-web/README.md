# BoltFS Web Demo

Minimal LAN demo for BoltFS in the browser.

## Start

```bash
cd demo/boltfs-web
npm install
node server.mjs
```

Default bind:

- host: `0.0.0.0`
- port: `8080`

Optional overrides:

```bash
HOST=0.0.0.0 PORT=9000 node server.mjs
```

If the BoltFS binary is not at the default release path, override it:

```bash
BOLTFS_BINARY=/abs/path/to/boltfs node server.mjs
```

## LAN Access

On the machine running the demo:

```bash
ifconfig | grep 'inet '
```

Then open in a browser from another machine on the same LAN:

```text
http://<your-ip>:8080
```

The page connects directly to a BoltFS REPL session and does not expose a general-purpose shell.
