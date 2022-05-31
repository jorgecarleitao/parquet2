# Changelog

## [v0.13.0](https://github.com/jorgecarleitao/parquet2/tree/v0.13.0) (2022-05-31)

[Full Changelog](https://github.com/jorgecarleitao/parquet2/compare/v0.12.1...v0.13.0)

**Breaking changes:**

- Removed unused cargo feature [\#145](https://github.com/jorgecarleitao/parquet2/pull/145) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Fix potential misuse of FileWriter API's \(sync + async\) [\#138](https://github.com/jorgecarleitao/parquet2/pull/138) ([TurnOfACard](https://github.com/TurnOfACard))

**New features:**

- Added new\_with\_page\_meta to PageReader [\#136](https://github.com/jorgecarleitao/parquet2/pull/136) ([ygf11](https://github.com/ygf11))
- Added compression options/levels for GZIP and BROTLI codecs. [\#132](https://github.com/jorgecarleitao/parquet2/pull/132) ([TurnOfACard](https://github.com/TurnOfACard))

**Fixed bugs:**

- Async FileStreamer does not write statistics [\#139](https://github.com/jorgecarleitao/parquet2/issues/139)
- Fixed error in compressing lz4raw with large offsets [\#140](https://github.com/jorgecarleitao/parquet2/pull/140) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Enhancements:**

- Improved read of metadata [\#143](https://github.com/jorgecarleitao/parquet2/pull/143) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Simplified async metadata read [\#137](https://github.com/jorgecarleitao/parquet2/pull/137) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Testing updates:**

- Lifted duplicated code to a function [\#141](https://github.com/jorgecarleitao/parquet2/pull/141) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Improved Integration test documentation and expanded tests [\#133](https://github.com/jorgecarleitao/parquet2/pull/133) ([TurnOfACard](https://github.com/TurnOfACard))

## [v0.12.1](https://github.com/jorgecarleitao/parquet2/tree/v0.12.1) (2022-05-15)

[Full Changelog](https://github.com/jorgecarleitao/parquet2/compare/v0.12.0...v0.12.1)

**Enhancements:**

- Pass only necessary data when create PageReader [\#135](https://github.com/jorgecarleitao/parquet2/issues/135)

## [v0.12.0](https://github.com/jorgecarleitao/parquet2/tree/v0.12.0) (2022-04-22)

[Full Changelog](https://github.com/jorgecarleitao/parquet2/compare/v0.11.0...v0.12.0)

**Breaking changes:**

- Add `CompressionOptions`, which allows for zstd compression levels. [\#128](https://github.com/jorgecarleitao/parquet2/pull/128) ([TurnOfACard](https://github.com/TurnOfACard))

**Enhancements:**

- Improved performance of RLE decoding \(-18%\) [\#130](https://github.com/jorgecarleitao/parquet2/pull/130) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Improved perf of bitpacking decoding \(3.5x\) [\#129](https://github.com/jorgecarleitao/parquet2/pull/129) ([jorgecarleitao](https://github.com/jorgecarleitao))

## [v0.11.0](https://github.com/jorgecarleitao/parquet2/tree/v0.11.0) (2022-04-15)

[Full Changelog](https://github.com/jorgecarleitao/parquet2/compare/v0.10.3...v0.11.0)

**Breaking changes:**

- Renamed `ParquetError` to `Error` [\#109](https://github.com/jorgecarleitao/parquet2/issues/109)
- Made `.end` not consume the parquet `FileWriter` [\#127](https://github.com/jorgecarleitao/parquet2/pull/127) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Removed `compression` from `WriteOptions` [\#125](https://github.com/jorgecarleitao/parquet2/pull/125) ([kornholi](https://github.com/kornholi))
- Simplified API and converted some panics on read to errors [\#112](https://github.com/jorgecarleitao/parquet2/pull/112) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Improved typing to reduce clones and use of unwraps [\#106](https://github.com/jorgecarleitao/parquet2/pull/106) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Simplified `PageIterator` [\#103](https://github.com/jorgecarleitao/parquet2/pull/103) ([jorgecarleitao](https://github.com/jorgecarleitao))

**New features:**

- Added support for page-level filter pushdown \(indexes\) [\#102](https://github.com/jorgecarleitao/parquet2/issues/102)
- Added support for bloom filters [\#98](https://github.com/jorgecarleitao/parquet2/issues/98)
- Added optional support for LZ4 via LZ4-flex crate \(thus enabling wasm\) [\#124](https://github.com/jorgecarleitao/parquet2/pull/124) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added support for page-level filter pushdown \(column and offset indexes\) [\#107](https://github.com/jorgecarleitao/parquet2/pull/107) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added support to read column and page indexes [\#100](https://github.com/jorgecarleitao/parquet2/pull/100) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Fixed bugs:**

- Fixed minimum version for LZ4 [\#122](https://github.com/jorgecarleitao/parquet2/pull/122) ([kornholi](https://github.com/kornholi))
- Fixed Lz4Raw compression error \(if input is tiny\)  [\#118](https://github.com/jorgecarleitao/parquet2/pull/118) ([dantengsky](https://github.com/dantengsky))
- Fixed LZ4 [\#95](https://github.com/jorgecarleitao/parquet2/pull/95) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Enhancements:**

- Made offsets be always written [\#123](https://github.com/jorgecarleitao/parquet2/pull/123) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added specialized deserialization of one-level filtered pages [\#120](https://github.com/jorgecarleitao/parquet2/pull/120) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added support to read and use bloom filters [\#99](https://github.com/jorgecarleitao/parquet2/pull/99) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added `ordinal` and `total_compressed_size` to column meta [\#96](https://github.com/jorgecarleitao/parquet2/pull/96) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added non-consuming function to get values of delta-decoder [\#94](https://github.com/jorgecarleitao/parquet2/pull/94) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Disabled bitpacking default-features and upgraded to edition 2021 [\#93](https://github.com/jorgecarleitao/parquet2/pull/93) ([light4](https://github.com/light4))

**Documentation updates:**

- Fix deployment of guide [\#115](https://github.com/jorgecarleitao/parquet2/pull/115) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Testing updates:**

- Added tests for reducing statistics [\#116](https://github.com/jorgecarleitao/parquet2/pull/116) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Simplified tests [\#104](https://github.com/jorgecarleitao/parquet2/pull/104) ([jorgecarleitao](https://github.com/jorgecarleitao))

## [v0.10.3](https://github.com/jorgecarleitao/parquet2/tree/v0.10.3) (2022-03-03)

[Full Changelog](https://github.com/jorgecarleitao/parquet2/compare/v0.10.2...v0.10.3)

**Fixed bugs:**

- write ColumnMetaData instead of ColumnChunk after pages. [\#90](https://github.com/jorgecarleitao/parquet2/pull/90) ([youngsofun](https://github.com/youngsofun))

## [v0.10.2](https://github.com/jorgecarleitao/parquet2/tree/v0.10.2) (2022-02-14)

[Full Changelog](https://github.com/jorgecarleitao/parquet2/compare/v0.10.1...v0.10.2)

**Fixed bugs:**

- Raise error when writing a page that is too large [\#84](https://github.com/jorgecarleitao/parquet2/pull/84) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Testing updates:**

- fix fmt and typo [\#83](https://github.com/jorgecarleitao/parquet2/pull/83) ([youngsofun](https://github.com/youngsofun))

## [v0.10.1](https://github.com/jorgecarleitao/parquet2/tree/v0.10.1) (2022-02-12)

[Full Changelog](https://github.com/jorgecarleitao/parquet2/compare/v0.10.0...v0.10.1)

**Enhancements:**

- Update zstd dependency [\#82](https://github.com/jorgecarleitao/parquet2/pull/82) ([jhorstmann](https://github.com/jhorstmann))
- Added file offset [\#81](https://github.com/jorgecarleitao/parquet2/pull/81) ([barrotsteindev](https://github.com/barrotsteindev))

## [v0.10.0](https://github.com/jorgecarleitao/parquet2/tree/v0.10.0) (2022-02-02)

[Full Changelog](https://github.com/jorgecarleitao/parquet2/compare/v0.9.2...v0.10.0)

**Breaking changes:**

- Simplified API to write files [\#78](https://github.com/jorgecarleitao/parquet2/pull/78) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Fixed bugs:**

- Fixed panic in reading empty values in hybrid-RLE [\#80](https://github.com/jorgecarleitao/parquet2/pull/80) ([jorgecarleitao](https://github.com/jorgecarleitao))

## [v0.9.2](https://github.com/jorgecarleitao/parquet2/tree/v0.9.2) (2022-01-25)

[Full Changelog](https://github.com/jorgecarleitao/parquet2/compare/v0.9.0...v0.9.2)

**Fixed bugs:**

- Fixed panic in reading empty values in hybrid-RLE [\#80](https://github.com/jorgecarleitao/parquet2/pull/80) ([jorgecarleitao](https://github.com/jorgecarleitao))

## [v0.9.0](https://github.com/jorgecarleitao/parquet2/tree/v0.9.0) (2022-01-11)

[Full Changelog](https://github.com/jorgecarleitao/parquet2/compare/v0.8.0...v0.9.0)

**Breaking changes:**

- Changed stream of groups to stream of futures of groups [\#71](https://github.com/jorgecarleitao/parquet2/pull/71) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Enhancements:**

- bitpacking: use stack-allocated temporary buffer [\#76](https://github.com/jorgecarleitao/parquet2/pull/76) ([danburkert](https://github.com/danburkert))
- Added constructor to `RowGroupMetaData` and `ColumnChunkMetaData` [\#74](https://github.com/jorgecarleitao/parquet2/pull/74) ([yjshen](https://github.com/yjshen))
- Improved performance of reading multiple pages [\#73](https://github.com/jorgecarleitao/parquet2/pull/73) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Fixed bugs:**

- Fixed error in declaring size of compressed dict page. [\#72](https://github.com/jorgecarleitao/parquet2/pull/72) ([jorgecarleitao](https://github.com/jorgecarleitao))

## [v0.8.1](https://github.com/jorgecarleitao/parquet2/tree/v0.8.1) (2021-12-09)

[Full Changelog](https://github.com/jorgecarleitao/parquet2/compare/v0.8.0...v0.8.1)

**Fixed bugs:**

- Fixed error in declaring size of compressed dict page. [\#72](https://github.com/jorgecarleitao/parquet2/pull/72) ([jorgecarleitao](https://github.com/jorgecarleitao))

## [v0.8.0](https://github.com/jorgecarleitao/parquet2/tree/v0.8.0) (2021-11-24)

[Full Changelog](https://github.com/jorgecarleitao/parquet2/compare/v0.7.0...v0.8.0)

**Breaking changes:**

- Improved error message when a feature is not active [\#69](https://github.com/jorgecarleitao/parquet2/pull/69) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Fixed bugs:**

- Fixed error in finishing iterator. [\#68](https://github.com/jorgecarleitao/parquet2/pull/68) ([jorgecarleitao](https://github.com/jorgecarleitao))

## [v0.7.0](https://github.com/jorgecarleitao/parquet2/tree/v0.7.0) (2021-11-13)

[Full Changelog](https://github.com/jorgecarleitao/parquet2/compare/v0.6.0...v0.7.0)

**Breaking changes:**

- Use `i64`s for delta-bitpacked's interface [\#67](https://github.com/jorgecarleitao/parquet2/pull/67) ([kornholi](https://github.com/kornholi))

**New features:**

- Added basic support to read nested types [\#64](https://github.com/jorgecarleitao/parquet2/pull/64) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Fixed bugs:**

- Fix off-by-one error in delta-bitpacked decoder [\#66](https://github.com/jorgecarleitao/parquet2/pull/66) ([kornholi](https://github.com/kornholi))

## [v0.6.0](https://github.com/jorgecarleitao/parquet2/tree/v0.6.0) (2021-10-18)

[Full Changelog](https://github.com/jorgecarleitao/parquet2/compare/v0.5.0...v0.6.0)

**Breaking changes:**

- Improved performance of codec initialization [\#63](https://github.com/jorgecarleitao/parquet2/pull/63) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Made `PageFilter` `Send` [\#62](https://github.com/jorgecarleitao/parquet2/pull/62) ([dantengsky](https://github.com/dantengsky))
- Alowed reusing compression buffer [\#60](https://github.com/jorgecarleitao/parquet2/pull/60) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Fixed bugs:**

- Fixed delta-bitpacked mini-block decoding [\#56](https://github.com/jorgecarleitao/parquet2/pull/56) ([kornholi](https://github.com/kornholi))
- Add descriptor to `FixedLenStatistics` [\#54](https://github.com/jorgecarleitao/parquet2/pull/54) ([potter420](https://github.com/potter420))
- Fixed error in reading zero-width bit from hybrid RLE. [\#53](https://github.com/jorgecarleitao/parquet2/pull/53) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Enhancements:**

- Added writing reduced statistics for `FixedLenByteArray` [\#55](https://github.com/jorgecarleitao/parquet2/pull/55) ([potter420](https://github.com/potter420))

## [v0.5.2](https://github.com/jorgecarleitao/parquet2/tree/v0.5.2) (2021-10-06)

[Full Changelog](https://github.com/jorgecarleitao/parquet2/compare/v0.5.1...v0.5.2)

**Fixed bugs:**

- Fixed delta-bitpacked mini-block decoding [\#56](https://github.com/jorgecarleitao/parquet2/pull/56) ([kornholi](https://github.com/kornholi))
- Add descriptor to `FixedLenStatistics` [\#54](https://github.com/jorgecarleitao/parquet2/pull/54) ([potter420](https://github.com/potter420))

**Enhancements:**

- Added writing reduced statistics for `FixedLenByteArray` [\#55](https://github.com/jorgecarleitao/parquet2/pull/55) ([potter420](https://github.com/potter420))

## [v0.5.1](https://github.com/jorgecarleitao/parquet2/tree/v0.5.1) (2021-09-29)

[Full Changelog](https://github.com/jorgecarleitao/parquet2/compare/v0.5.0...v0.5.1)

**Fixed bugs:**

- Fixed error in reading zero-width bit from hybrid RLE. [\#53](https://github.com/jorgecarleitao/parquet2/pull/53) ([jorgecarleitao](https://github.com/jorgecarleitao))

## [v0.5.0](https://github.com/jorgecarleitao/parquet2/tree/v0.5.0) (2021-09-18)

[Full Changelog](https://github.com/jorgecarleitao/parquet2/compare/v0.4.0...v0.5.0)

**Breaking changes:**

- Renamed `Compression::Zsld` to `Compression::Zstd` \(typo\) [\#48](https://github.com/jorgecarleitao/parquet2/pull/48) ([vincev](https://github.com/vincev))

**Enhancements:**

- Add `null_count` method to trait `Statistics` [\#49](https://github.com/jorgecarleitao/parquet2/pull/49) ([yjshen](https://github.com/yjshen))

## [v0.4.0](https://github.com/jorgecarleitao/parquet2/tree/v0.4.0) (2021-08-28)

[Full Changelog](https://github.com/jorgecarleitao/parquet2/compare/v0.3.0...v0.4.0)

**Breaking changes:**

- Make `write_*` return the number of written bytes. [\#45](https://github.com/jorgecarleitao/parquet2/issues/45)
- move `HybridRleDecoder` from `read::levels` to `encoding::hybrid_rle` [\#41](https://github.com/jorgecarleitao/parquet2/issues/41)
- Simplified split of page buffer [\#37](https://github.com/jorgecarleitao/parquet2/pull/37) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Simplified API to get page iterator [\#36](https://github.com/jorgecarleitao/parquet2/pull/36) ([jorgecarleitao](https://github.com/jorgecarleitao))

**New features:**

- Added support to write to async writers. [\#35](https://github.com/jorgecarleitao/parquet2/pull/35) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Fixed bugs:**

- Fixed edge case of a small bitpacked. [\#43](https://github.com/jorgecarleitao/parquet2/pull/43) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Fixed error in decoding RLE-hybrid. [\#40](https://github.com/jorgecarleitao/parquet2/pull/40) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Enhancements:**

- Removed requirement of "Seek" on write. [\#44](https://github.com/jorgecarleitao/parquet2/pull/44) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Documentation updates:**

- Added guide to read [\#38](https://github.com/jorgecarleitao/parquet2/pull/38) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Testing updates:**

- Made tests deserializer use the correct decoder. [\#46](https://github.com/jorgecarleitao/parquet2/pull/46) ([jorgecarleitao](https://github.com/jorgecarleitao))

## [v0.3.0](https://github.com/jorgecarleitao/parquet2/tree/v0.3.0) (2021-08-09)

[Full Changelog](https://github.com/jorgecarleitao/parquet2/compare/v0.2.0...v0.3.0)

**Breaking changes:**

- Added option to apply filter pushdown to data pages. [\#34](https://github.com/jorgecarleitao/parquet2/pull/34) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Add support to read async [\#33](https://github.com/jorgecarleitao/parquet2/pull/33) ([jorgecarleitao](https://github.com/jorgecarleitao))

**New features:**

- Add support for async read [\#32](https://github.com/jorgecarleitao/parquet2/issues/32)
- Added option to apply filter pushdown to data pages. [\#34](https://github.com/jorgecarleitao/parquet2/pull/34) ([jorgecarleitao](https://github.com/jorgecarleitao))

## [v0.2.0](https://github.com/jorgecarleitao/parquet2/tree/v0.2.0) (2021-08-03)

[Full Changelog](https://github.com/jorgecarleitao/parquet2/compare/v0.1.0...v0.2.0)

**Enhancements:**

- Add support to write dictionary-encoded pages [\#29](https://github.com/jorgecarleitao/parquet2/issues/29)
- Upgrade zstd to ^0.9 [\#31](https://github.com/jorgecarleitao/parquet2/pull/31) ([Dandandan](https://github.com/Dandandan))



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
