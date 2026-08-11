mod common;

use std::fs;
use std::time::Duration;

use sha2::{Digest, Sha256};
use takanawa_core::{HashConfig, PartFile, RemoteInfo, TakanawaError};
use takanawa_http::{
    DEFAULT_MAX_IO, DownloadConfig, DownloadEngine, DownloadHandle, DownloadPhase, RetryConfig,
    TimeoutConfig, download_to_completion,
};
use tempfile::TempDir;
use tokio::runtime::Runtime;

use common::{RangeServer, wait_for_phase};

#[tokio::test]
async fn downloads_file_with_public_api_and_sha256() {
    let data = b"abcdefghijklmnopqrstuvwxyz".to_vec();
    let expected_hash: [u8; 32] = Sha256::digest(&data).into();
    let server = RangeServer::spawn(data.clone());
    let dir = TempDir::new().expect("temp dir should be created");
    let target = dir.path().join("out.bin");

    let snapshot = download_to_completion(
        DownloadEngine::new(DEFAULT_MAX_IO).expect("engine should be created"),
        download_config(
            server.url(),
            target.clone(),
            HashConfig::Sha256(expected_hash),
        ),
    )
    .await
    .expect("download should complete");

    assert_eq!(snapshot.phase, DownloadPhase::Completed);
    assert_eq!(snapshot.content_len, data.len() as u64);
    assert_eq!(
        fs::read(target).expect("downloaded file should be readable"),
        data
    );
}

#[tokio::test]
async fn resumes_from_existing_part_file() {
    let data = b"abcdefghijklmnopqrstuvwxyz".to_vec();
    let server = RangeServer::spawn(data.clone());
    let url = server.url();
    let dir = TempDir::new().expect("temp dir should be created");
    let target = dir.path().join("out.bin");

    let mut part = PartFile::open_or_create(
        &target,
        &url,
        &RemoteInfo {
            content_len: data.len() as u64,
            etag: None,
            last_modified: None,
        },
        5,
        HashConfig::None,
    )
    .expect("part file should be created");
    part.write_chunk(0, &data[..5])
        .expect("first chunk should be committed");
    drop(part);

    let snapshot = download_to_completion(
        DownloadEngine::new(DEFAULT_MAX_IO).expect("engine should be created"),
        download_config(url, target.clone(), HashConfig::None),
    )
    .await
    .expect("download should resume and complete");

    assert_eq!(snapshot.phase, DownloadPhase::Completed);
    assert_eq!(snapshot.completed_chunks, snapshot.chunk_count);
    assert_eq!(
        fs::read(target).expect("downloaded file should be readable"),
        data
    );
}

#[tokio::test]
async fn reports_hash_mismatch_before_finalizing_target() {
    let data = b"abcdef".to_vec();
    let server = RangeServer::spawn(data);
    let dir = TempDir::new().expect("temp dir should be created");
    let target = dir.path().join("out.bin");

    let err = download_to_completion(
        DownloadEngine::new(DEFAULT_MAX_IO).expect("engine should be created"),
        download_config(server.url(), target.clone(), HashConfig::Sha256([0; 32])),
    )
    .await
    .expect_err("download should fail hash verification");

    assert!(matches!(err, TakanawaError::HashMismatch));
    assert!(!target.exists());
}

#[tokio::test]
async fn rejects_server_that_ignores_ranges() {
    let server = RangeServer::spawn_ignoring_ranges(b"abcdef".to_vec());
    let dir = TempDir::new().expect("temp dir should be created");
    let target = dir.path().join("out.bin");

    let err = download_to_completion(
        DownloadEngine::new(DEFAULT_MAX_IO).expect("engine should be created"),
        download_config(server.url(), target, HashConfig::None),
    )
    .await
    .expect_err("download should reject non-range response");

    assert!(matches!(
        err,
        TakanawaError::RangeNotHonored { status: 200 }
    ));
}

#[tokio::test]
async fn retries_when_server_temporarily_ignores_range() {
    let data = b"abcdefghijklmnopqrstuvwxyz".to_vec();
    let server = RangeServer::spawn_ignoring_first_range(data.clone());
    let dir = TempDir::new().expect("temp dir should be created");
    let target = dir.path().join("out.bin");
    let mut config = download_config(server.url(), target.clone(), HashConfig::None);
    config.retry.max_retries = 1;

    let snapshot = download_to_completion(
        DownloadEngine::new(DEFAULT_MAX_IO).expect("engine should be created"),
        config,
    )
    .await
    .expect("download should retry and complete");

    assert_eq!(snapshot.phase, DownloadPhase::Completed);
    assert_eq!(
        fs::read(target).expect("downloaded file should be readable"),
        data
    );
}

#[test]
fn cancel_transitions_background_download_to_cancelled() {
    let server = RangeServer::spawn_delayed_chunks(
        b"abcdefghijklmnopqrstuvwxyz".to_vec(),
        Duration::from_millis(250),
    );
    let dir = TempDir::new().expect("temp dir should be created");
    let target = dir.path().join("out.bin");
    let runtime = Runtime::new().expect("runtime should be created");
    let download = DownloadHandle::new(
        DownloadEngine::new(DEFAULT_MAX_IO).expect("engine should be created"),
        download_config(server.url(), target, HashConfig::None),
    );

    download
        .start_on(&runtime)
        .expect("download should start on runtime");
    wait_for_phase(&download, DownloadPhase::Running);
    download.cancel().expect("cancel should be requested");

    let snapshot = wait_for_phase(&download, DownloadPhase::Cancelled);

    assert_eq!(snapshot.phase, DownloadPhase::Cancelled);
}

fn download_config(
    url: String,
    target_path: std::path::PathBuf,
    hash: HashConfig,
) -> DownloadConfig {
    DownloadConfig {
        url,
        target_path,
        chunk_size: 5,
        parallelism: 2,
        max_parallel_chunks: 0,
        retry: RetryConfig {
            max_retries: 0,
            backoff_initial: Duration::from_millis(1),
            backoff_max: Duration::from_millis(1),
        },
        timeout: TimeoutConfig::default(),
        bytes_per_second_limit: 0,
        hash,
    }
}
