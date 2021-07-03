use parquet_format::Encoding;

use crate::encoding::{bitpacking, get_length, hybrid_rle, log2};

#[inline]
pub fn get_bit_width(max_level: i16) -> u32 {
    log2(max_level as u64)
}

#[inline]
pub fn decode(values: &[u8], length: u32, encoding: (&Encoding, i16)) -> Vec<u32> {
    match encoding {
        (_, 0) => vec![0; length as usize], // no levels => required => all zero
        (Encoding::Rle, max_length) => {
            let bit_width = get_bit_width(max_length);
            rle_decode(&values, bit_width as u32, length)
        }
        (Encoding::BitPacked, _) => {
            todo!()
        }
        _ => unreachable!(),
    }
}

enum DecoderState<'a> {
    Bitpacking(bitpacking::Decoder<'a>),
    Rle { value: u32, length: usize },
    Finished,
}

pub struct RLEDecoder<'a> {
    runner: hybrid_rle::Decoder<'a>,
    state: DecoderState<'a>,
    num_bits: u32,
    length: u32,
    length_so_far: usize,
}

impl<'a> RLEDecoder<'a> {
    pub fn new(values: &'a [u8], num_bits: u32, length: u32) -> Self {
        let runner = hybrid_rle::Decoder::new(&values, num_bits);
        let mut this = Self {
            length,
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
            self.state = DecoderState::Finished;
        }
    }
}

impl<'a> Iterator for RLEDecoder<'a> {
    type Item = u32;

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
}

#[inline]
pub fn rle_decode(values: &[u8], num_bits: u32, length: u32) -> Vec<u32> {
    let length = length as usize;
    let runner = hybrid_rle::Decoder::new(&values, num_bits);

    let mut values = Vec::with_capacity(length);
    runner.for_each(|run| match run {
        hybrid_rle::HybridEncoded::Bitpacked(compressed) => {
            let previous_len = values.len();
            let pack_length = (compressed.len() as usize / num_bits as usize) * 8;
            let additional = std::cmp::min(previous_len + pack_length, length - previous_len);
            values.extend(bitpacking::Decoder::new(
                compressed,
                num_bits as u8,
                additional,
            ));
            debug_assert_eq!(previous_len + additional, values.len());
        }
        hybrid_rle::HybridEncoded::Rle(pack, items) => {
            let mut bytes = [0u8; std::mem::size_of::<u32>()];
            pack.iter()
                .enumerate()
                .for_each(|(i, byte)| bytes[i] = *byte);
            let value = u32::from_le_bytes(bytes);
            values.extend(std::iter::repeat(value).take(items))
        }
    });
    values
}

/// returns slices corresponding to (rep, def, values)
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
