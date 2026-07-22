import test from 'node:test'
import assert from 'node:assert/strict'

import { DownloadTask, downloadToCompletion } from '../dist/index.mjs'

test('exports public API', () => {
  assert.equal(typeof DownloadTask, 'function')
  assert.equal(typeof downloadToCompletion, 'function')
  for (const method of [
    'start',
    'pause',
    'cancel',
    'snapshot',
    'bitmap',
    'close',
    'addProgressListener',
    'addSpeedListener'
  ]) {
    assert.equal(typeof DownloadTask.prototype[method], 'function')
  }
})

test('calls the native binding', async () => {
  const task = new DownloadTask({
    url: 'http://127.0.0.1:1/file',
    targetPath: 'unused'
  })

  try {
    const snapshot = await task.snapshot()
    assert.equal(snapshot.phase, 'created')
  } finally {
    await task.close()
  }
})
