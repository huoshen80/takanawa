import { createRequire } from 'node:module'
import type { TakanawaStatusCode } from 'takanawa-js-core'

const require = createRequire(import.meta.url)

export interface NativeDownloadOptions {
  url: string
  targetPath: string
  chunkSize?: string
  parallelism?: number
  maxParallelChunks?: number
  maxIo?: number
  maxRetries?: number
  backoffInitialMs?: number
  backoffMaxMs?: number
  connectTimeoutMs?: number
  readTimeoutMs?: number
  totalTimeoutMs?: number
  bytesPerSecondLimit?: string
  hash?: NativeHashConfig
  sha256?: string
}

export interface NativeHashConfig {
  kind: string
  expected: string
}

export interface NativeDownloadSnapshot {
  phase: string
  contentLen: string
  downloadedBytes: string
  chunkSize: string
  chunkCount: string
  completedChunks: string
  activeIo: number
  lastError?: string
  lastErrorCode?: TakanawaStatusCode
}

export interface NativeDownloadSpeedSnapshot {
  phase: string
  contentLen: string
  receivedBytes: string
  intervalBytes: string
  elapsedMillis: string
  bytesPerSecond: number
  activeIo: number
}

export interface NativeDownloadTask {
  start(): void
  pause(): void
  cancel(): void
  snapshot(): NativeDownloadSnapshot
  speedSnapshot(): NativeDownloadSpeedSnapshot
  bitmap(): Uint8Array
}

export interface TakanawaNativeBinding {
  nativeDownloadToCompletion(options: NativeDownloadOptions): Promise<NativeDownloadSnapshot>
  NativeDownloadTask: new (options: NativeDownloadOptions) => NativeDownloadTask
}

export const nativeBinding = require('../index.js') as TakanawaNativeBinding
