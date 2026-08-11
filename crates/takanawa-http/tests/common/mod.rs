use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::Duration;

use takanawa_http::{DownloadHandle, DownloadPhase, DownloadSnapshot};

#[derive(Clone, Copy)]
enum ServerMode {
    Range,
    IgnoreRange,
    IgnoreFirstRange,
    DelayChunks(Duration),
}

pub struct RangeServer {
    addr: SocketAddr,
}

impl RangeServer {
    pub fn spawn(data: Vec<u8>) -> Self {
        spawn_server(Arc::new(data), ServerMode::Range)
    }

    pub fn spawn_ignoring_ranges(data: Vec<u8>) -> Self {
        spawn_server(Arc::new(data), ServerMode::IgnoreRange)
    }

    pub fn spawn_ignoring_first_range(data: Vec<u8>) -> Self {
        spawn_server(Arc::new(data), ServerMode::IgnoreFirstRange)
    }

    pub fn spawn_delayed_chunks(data: Vec<u8>, delay: Duration) -> Self {
        spawn_server(Arc::new(data), ServerMode::DelayChunks(delay))
    }

    pub fn url(&self) -> String {
        format!("http://{}/file", self.addr)
    }
}

pub fn wait_for_phase(download: &DownloadHandle, phase: DownloadPhase) -> DownloadSnapshot {
    for _ in 0..150 {
        let snapshot = download.snapshot();
        if snapshot.phase == phase {
            return snapshot;
        }
        thread::sleep(Duration::from_millis(20));
    }
    download.snapshot()
}

fn spawn_server(data: Arc<Vec<u8>>, mode: ServerMode) -> RangeServer {
    let listener = TcpListener::bind("127.0.0.1:0").expect("test server should bind");
    let addr = listener
        .local_addr()
        .expect("test server should have an address");
    let request_count = Arc::new(AtomicUsize::new(0));
    thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            let data = Arc::clone(&data);
            let request_count = Arc::clone(&request_count);
            thread::spawn(move || handle_connection(stream, &data, mode, &request_count));
        }
    });
    RangeServer { addr }
}

fn handle_connection(
    mut stream: TcpStream,
    data: &[u8],
    mode: ServerMode,
    request_count: &AtomicUsize,
) {
    let mut buffer = [0; 4096];
    let read = stream.read(&mut buffer).unwrap_or(0);
    let request = String::from_utf8_lossy(&buffer[..read]);
    let range = request_range(&request);

    let request_number = request_count.fetch_add(1, Ordering::Relaxed);
    if matches!(mode, ServerMode::IgnoreRange)
        || matches!(mode, ServerMode::IgnoreFirstRange) && request_number == 0
    {
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            data.len()
        );
        stream
            .write_all(response.as_bytes())
            .expect("test server should write headers");
        stream
            .write_all(data)
            .expect("test server should write body");
        return;
    }

    let Some((start, end)) = range else {
        stream
            .write_all(
                b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            )
            .expect("test server should write bad request response");
        return;
    };
    let Some(body) = range_body(data, start, end, &mut stream) else {
        return;
    };

    if let ServerMode::DelayChunks(delay) = mode {
        if !(start == 0 && body.len() == 1) {
            thread::sleep(delay);
        }
    }

    let response = format!(
        "HTTP/1.1 206 Partial Content\r\nContent-Range: bytes {start}-{}/{}\r\nContent-Length: {}\r\nAccept-Ranges: bytes\r\nConnection: close\r\n\r\n",
        start + body.len() - 1,
        data.len(),
        body.len()
    );
    stream
        .write_all(response.as_bytes())
        .expect("test server should write headers");
    stream
        .write_all(body)
        .expect("test server should write body");
}

fn range_body<'a>(
    data: &'a [u8],
    start: usize,
    end: usize,
    stream: &mut TcpStream,
) -> Option<&'a [u8]> {
    if start >= data.len() {
        let response = format!(
            "HTTP/1.1 416 Range Not Satisfiable\r\nContent-Range: bytes */{}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            data.len()
        );
        stream
            .write_all(response.as_bytes())
            .expect("test server should write range error");
        return None;
    }
    let end = end.min(data.len() - 1);
    Some(&data[start..=end])
}

fn request_range(request: &str) -> Option<(usize, usize)> {
    let range = request.lines().find_map(|line| {
        let (name, value) = line.split_once(':')?;
        if name.eq_ignore_ascii_case("range") {
            value.trim().strip_prefix("bytes=")
        } else {
            None
        }
    })?;
    let (start, end) = range.split_once('-')?;
    Some((start.parse().ok()?, end.parse().ok()?))
}
