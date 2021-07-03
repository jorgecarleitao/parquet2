use crate::encoding::{bitpacking, get_length, hybrid_rle, log2};

#[inline]
pub fn get_bit_width(max_level: i16) -> u32 {
    log2(max_level as u64)
}

enum DecoderState<'a> {
    Bitpacking(bitpacking::Decoder<'a>),
    Rle { value: u32, length: usize },
    Finished,
}

/// Decoder of RLE-encoded levels (rep and def). It is an iterator of `u32`.
pub struct RLEDecoder<'a> {
    runner: hybrid_rle::Decoder<'a>,
    state: DecoderState<'a>,
    num_bits: u32,
    length: usize,
    length_so_far: usize,
}

impl<'a> RLEDecoder<'a> {
    pub fn new(values: &'a [u8], num_bits: u32, length: u32) -> Self {
        let runner = hybrid_rle::Decoder::new(values, num_bits);
        let mut this = Self {
            length: length as usize,
            num_bits,
            state: DecoderState::Finished,
            runner,
            length_so_far: 0,
        };
        this.load_run();
        this
    }

    fn load_run(&mut self) {
        let run = self.runner.next();
        if let Some(run) = run {
            match run {
                hybrid_rle::HybridEncoded::Bitpacked(compressed) => {
                    let pack_length = (compressed.len() as usize / self.num_bits as usize) * 8;
                    let additional = std::cmp::min(
                        self.length_so_far + pack_length,
                        self.length as usize - self.length_so_far,
                    );
                    self.state = DecoderState::Bitpacking(bitpacking::Decoder::new(
                        compressed,
                        self.num_bits as u8,
                        additional,
                    ));
                }
                hybrid_rle::HybridEncoded::Rle(pack, items) => {
                    let mut bytes = [0u8; std::mem::size_of::<u32>()];
                    pack.iter()
                        .enumerate()
                        .for_each(|(i, byte)| bytes[i] = *byte);
                    let value = u32::from_le_bytes(bytes);
                    self.state = DecoderState::Rle {
                        value,
                        length: items,
                    };
                }
            }
        } else {
            assert_eq!(self.length_so_far, self.length);
            self.state = DecoderState::Finished;
        }
    }
}

impl<'a> Iterator for RLEDecoder<'a> {
    type Item = u32;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        let next = match &mut self.state {
            DecoderState::Finished => return None,
            DecoderState::Bitpacking(decoder) => decoder.next(),
            DecoderState::Rle { value, length } => {
                if *length == 0 {
                    None
                } else {
                    *length -= 1;
                    Some(*value)
                }
            }
        };
        if next.is_none() {
            self.load_run()
        }

        self.length_so_far += 1;
        next
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.length, Some(self.length))
    }
}

impl<'a> ExactSizeIterator for RLEDecoder<'a> {}

/// returns slices corresponding to (rep, def, values) for v1 pages
#[inline]
pub fn split_buffer_v1(buffer: &[u8], has_rep: bool, has_def: bool) -> (&[u8], &[u8], &[u8]) {
    let (rep, buffer) = if has_rep {
        let level_buffer_length = get_length(buffer) as usize;
        (
            &buffer[4..4 + level_buffer_length],
            &buffer[4 + level_buffer_length..],
        )
    } else {
        (&[] as &[u8], buffer)
    };

    let (def, buffer) = if has_def {
        let level_buffer_length = get_length(buffer) as usize;
        (
            &buffer[4..4 + level_buffer_length],
            &buffer[4 + level_buffer_length..],
        )
    } else {
        (&[] as &[u8], buffer)
    };

    (rep, def, buffer)
}

/// returns slices corresponding to (rep, def, values) for v2 pages
pub fn split_buffer_v2(
    buffer: &[u8],
    rep_level_buffer_length: usize,
    def_level_buffer_length: usize,
) -> (&[u8], &[u8], &[u8]) {
    (
        &buffer[..rep_level_buffer_length],
        &buffer[rep_level_buffer_length..rep_level_buffer_length + def_level_buffer_length],
        &buffer[rep_level_buffer_length + def_level_buffer_length..],
    )
}
