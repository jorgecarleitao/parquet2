// Special thanks to Alice for the help: https://users.rust-lang.org/t/63019/6
use std::io::{Result, SeekFrom};
use std::pin::Pin;

use futures::{
    future::BoxFuture,
    io::{AsyncRead, AsyncSeek},
    Future,
};
use s3::Bucket;

pub struct RangedStreamer {
    pos: u64,
    length: u64, // total size
    state: State,
    path: String,
    min_request_size: usize, // requests have at least this size
}

impl RangedStreamer {
    pub fn new(length: usize, bucket: Bucket, path: String, min_request_size: usize) -> Self {
        let length = length as u64;
        Self {
            pos: 0,
            length,
            state: State::HasChunk(SeekOutput {
                start: 0,
                bucket,
                data: vec![],
            }),
            path,
            min_request_size,
        }
    }
}

async fn read_s3(start: u64, length: usize, bucket: Bucket, path: String) -> Result<SeekOutput> {
    let (mut data, _) = bucket
        .get_object_range(&path, start, Some(start + length as u64))
        .await
        .map_err(|x| std::io::Error::new(std::io::ErrorKind::Other, x.to_string()))?;

    data.truncate(length);
    Ok(SeekOutput {
        start,
        bucket,
        data,
    })
}

enum State {
    HasChunk(SeekOutput),
    Seeking(BoxFuture<'static, std::io::Result<SeekOutput>>),
}

struct SeekOutput {
    start: u64,
    bucket: Bucket,
    data: Vec<u8>,
}

// whether `test_interval` is inside `a` (start, length).
fn range_includes(a: (usize, usize), test_interval: (usize, usize)) -> bool {
    if test_interval.0 < a.0 {
        return false;
    }
    let test_end = test_interval.0 + test_interval.1;
    let a_end = a.0 + a.1;
    if test_end > a_end {
        return false;
    }
    true
}

impl AsyncRead for RangedStreamer {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut [u8],
    ) -> std::task::Poll<Result<usize>> {
        let requested_range = (self.pos as usize, buf.len());
        let min_request_size = self.min_request_size;
        match &mut self.state {
            State::HasChunk(output) => {
                //let _ = self.process(buf, &output);
                let existing_range = (output.start as usize, output.data.len());
                if range_includes(existing_range, requested_range) {
                    let offset = requested_range.0 - existing_range.0;
                    buf.copy_from_slice(&output.data[offset..offset + buf.len()]);
                    self.pos += buf.len() as u64;
                    std::task::Poll::Ready(Ok(buf.len()))
                } else {
                    let future = read_s3(
                        requested_range.0 as u64,
                        std::cmp::max(min_request_size, requested_range.1),
                        output.bucket.clone(),
                        self.path.clone(),
                    );
                    self.state = State::Seeking(Box::pin(future));
                    self.poll_read(cx, buf)
                }
            }
            State::Seeking(ref mut future) => match Pin::new(future).poll(cx) {
                std::task::Poll::Ready(v) => {
                    match v {
                        Ok(output) => self.state = State::HasChunk(output),
                        Err(e) => return std::task::Poll::Ready(Err(e)),
                    };
                    self.poll_read(cx, buf)
                }
                std::task::Poll::Pending => std::task::Poll::Pending,
            },
        }
    }
}

impl AsyncSeek for RangedStreamer {
    fn poll_seek(
        mut self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
        pos: std::io::SeekFrom,
    ) -> std::task::Poll<Result<u64>> {
        match pos {
            SeekFrom::Start(pos) => self.pos = pos,
            SeekFrom::End(pos) => self.pos = (self.length as i64 + pos) as u64,
            SeekFrom::Current(pos) => self.pos = (self.pos as i64 + pos) as u64,
        };
        std::task::Poll::Ready(Ok(self.pos))
    }
}
