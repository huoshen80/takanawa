# takanawa-node

Node.js and Electron bindings for Takanawa, built with napi-rs and wrapped with
the shared Takanawa TypeScript API.

## Platforms

- Linux x64 (glibc)
- macOS 11 or newer on Apple silicon
- Windows x64

## TODO

- [ ] Build, test, and publish the Linux ARM64 GNU/glibc addon (`aarch64-unknown-linux-gnu`).
- [ ] Build, test, and publish the Intel macOS x64 addon (`x86_64-apple-darwin`).

## Development

```sh
pnpm --filter takanawa-node build
```

The package builds the native Node-API addon first, then emits the TypeScript wrapper as both ESM and CommonJS.

## Usage

```ts
import { DownloadTask, downloadToCompletion } from 'takanawa-node'

await downloadToCompletion({
  url: 'https://example.com/file.zip',
  targetPath: '/tmp/file.zip',
  parallelism: 4,
  hash: {
    kind: 'sha256',
    expected: '...64 hex characters...'
  }
})

const task = new DownloadTask({
  url: 'https://example.com/file.zip',
  targetPath: '/tmp/file.zip'
})

const progress = await task.addProgressListener((snapshot) => {
  console.log(snapshot.phase, snapshot.downloadedBytes, snapshot.contentLen)
})
const speed = await task.addSpeedListener((snapshot) => {
  console.log(snapshot.bytesPerSecond, snapshot.receivedBytes)
})

await task.start()
console.log(await task.snapshot())

await progress.remove()
await speed.remove()
await task.close()
```

The public API matches the other Takanawa npm targets. Task methods return
promises, and large byte counts are exposed as `bigint` so callers do not lose
precision.

`hash` supports `sha1`, `sha256`, `sha512`, `md5`, and `crc32` expected
digests. You can also pass a compact string such as `sha512:<hex>`. The legacy
`sha256` option remains available for existing callers.
