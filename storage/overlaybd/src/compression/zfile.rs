use crate::io::vfile_io::{read_exact, CtxRead, DirectRead, FileReader};
use crate::io::virtual_file::{IoCtx, LocalBoxFuture, VirtualFile};
use anyhow::{bail, ensure, Context, Result};
use async_trait::async_trait;
use bytes::Bytes;
use crc::{Algorithm, Crc};
use futures_util::stream::{self, StreamExt};
use std::cmp::min;
use std::sync::Arc;

pub const MAX_READ_SIZE: usize = 65_536;

const HEADER_TRAILER_SPACE: usize = 512;
const HEADER_TRAILER_BODY_SIZE: usize = 96;
const BUF_SIZE: usize = 512;
const NOI_WELL_KNOWN_PRIME: u32 = 100_007;
const ZSTD_C_LEVEL: i32 = 3;
const JUMP_TABLE_READ_CHUNK_SIZE: usize = 1 << 20;
const JUMP_TABLE_READ_PARALLELISM: usize = 32;
const CRC32C_RAW_ALG: Algorithm<u32> = Algorithm {
    width: 32,
    poly: 0x1EDC6F41,
    init: 0,
    refin: true,
    refout: true,
    xorout: 0,
    check: 0,
    residue: 0,
};
const CRC32C_RAW: Crc<u32> = Crc::<u32>::new(&CRC32C_RAW_ALG);

fn crc32c(data: &[u8]) -> u32 {
    CRC32C_RAW.checksum(data)
}

fn crc32c_salt(data: &[u8]) -> u32 {
    // Upstream crc32c_extend() consumes the running CRC state directly.
    // `crc`'s digest_with_initial() applies reflected init preprocessing,
    // so reverse the seed bits to reconstruct the same internal state.
    let mut digest = CRC32C_RAW.digest_with_initial(NOI_WELL_KNOWN_PRIME.reverse_bits());
    digest.update(data);
    digest.finalize()
}

/// Thin wrapper around [`read_exact`] preserving the historical
/// `(file, buf, offset)` argument order used by zfile parsing helpers.
async fn read_exact_at(file: &dyn VirtualFile, buf: &mut [u8], offset: u64) -> Result<()> {
    read_exact(&DirectRead, file, offset, buf).await
}

async fn write_all_at(
    file: &(dyn VirtualFile + Send + Sync),
    mut buf: &[u8],
    mut offset: u64,
) -> Result<()> {
    while !buf.is_empty() {
        let n = file.write_at(offset, buf).await?;
        ensure!(n != 0, "short write");
        buf = &buf[n..];
        offset = offset.checked_add(n as u64).context("offset overflow")?;
    }
    Ok(())
}

async fn read_exact_chunks_at(
    file: Arc<dyn VirtualFile>,
    offset: u64,
    len: usize,
) -> Result<Vec<u8>> {
    if len == 0 {
        return Ok(Vec::new());
    }
    if len <= JUMP_TABLE_READ_CHUNK_SIZE {
        let mut buf = vec![0u8; len];
        read_exact_at(file.as_ref(), &mut buf, offset).await?;
        return Ok(buf);
    }

    let chunk_count = len.div_ceil(JUMP_TABLE_READ_CHUNK_SIZE);
    let chunks = stream::iter(0..chunk_count)
        .map(|chunk_index| {
            let file = file.clone();
            async move {
                let chunk_offset = chunk_index * JUMP_TABLE_READ_CHUNK_SIZE;
                let chunk_len = min(JUMP_TABLE_READ_CHUNK_SIZE, len - chunk_offset);
                let mut buf = vec![0u8; chunk_len];
                read_exact_at(file.as_ref(), &mut buf, offset + chunk_offset as u64).await?;
                Ok::<_, anyhow::Error>((chunk_index, buf))
            }
        })
        .buffer_unordered(chunk_count.min(JUMP_TABLE_READ_PARALLELISM))
        .collect::<Vec<_>>()
        .await;

    let mut out = vec![0u8; len];
    for chunk in chunks {
        let (chunk_index, buf) = chunk?;
        let chunk_offset = chunk_index * JUMP_TABLE_READ_CHUNK_SIZE;
        out[chunk_offset..chunk_offset + buf.len()].copy_from_slice(&buf);
    }
    Ok(out)
}

fn read_u32_le(buf: &[u8], offset: usize) -> Result<u32> {
    let end = offset.checked_add(4).context("offset overflow")?;
    let bytes = buf.get(offset..end).context("buffer too small")?;
    Ok(u32::from_le_bytes(bytes.try_into().expect("u32 length")))
}

fn read_u64_le(buf: &[u8], offset: usize) -> Result<u64> {
    let end = offset.checked_add(8).context("offset overflow")?;
    let bytes = buf.get(offset..end).context("buffer too small")?;
    Ok(u64::from_le_bytes(bytes.try_into().expect("u64 length")))
}

fn write_u32_le(buf: &mut [u8], offset: usize, value: u32) {
    let end = offset + 4;
    buf[offset..end].copy_from_slice(&value.to_le_bytes());
}

fn write_u64_le(buf: &mut [u8], offset: usize, value: u64) {
    let end = offset + 8;
    buf[offset..end].copy_from_slice(&value.to_le_bytes());
}

fn u64_to_usize(value: u64, field: &str) -> Result<usize> {
    usize::try_from(value).with_context(|| format!("{field} cannot fit in usize"))
}

fn mul_u64_to_usize(a: u64, b: u64, field: &str) -> Result<usize> {
    let product = a
        .checked_mul(b)
        .with_context(|| format!("{field} multiplication overflow"))?;
    u64_to_usize(product, field)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CompressOptions {
    pub block_size: u32,
    pub algo: u8,
    pub level: u8,
    pub use_dict: u8,
    pub reserved: u32,
    pub dict_size: u32,
    pub verify: u8,
}

impl CompressOptions {
    pub const MINI_LZO: u8 = 0;
    pub const LZ4: u8 = 1;
    pub const ZSTD: u8 = 2;
    pub const DEFAULT_BLOCK_SIZE: u32 = 4096;

    pub fn new(algo: u8, block_size: u32, verify: u8) -> Self {
        Self {
            block_size,
            algo,
            verify,
            ..Self::default()
        }
    }

    fn decode_from(buf: &[u8]) -> Result<Self> {
        ensure!(buf.len() >= 24, "compress options buffer too small");
        Ok(Self {
            block_size: read_u32_le(buf, 0)?,
            algo: buf[4],
            level: buf[5],
            use_dict: buf[6],
            reserved: read_u32_le(buf, 8)?,
            dict_size: read_u32_le(buf, 12)?,
            verify: buf[16],
        })
    }

    fn encode_into(&self, buf: &mut [u8]) -> Result<()> {
        ensure!(buf.len() >= 24, "compress options buffer too small");
        buf[..24].fill(0);
        write_u32_le(buf, 0, self.block_size);
        buf[4] = self.algo;
        buf[5] = self.level;
        buf[6] = self.use_dict;
        write_u32_le(buf, 8, self.reserved);
        write_u32_le(buf, 12, self.dict_size);
        buf[16] = self.verify;
        Ok(())
    }
}

impl Default for CompressOptions {
    fn default() -> Self {
        Self {
            block_size: Self::DEFAULT_BLOCK_SIZE,
            algo: Self::LZ4,
            level: 0,
            use_dict: 0,
            reserved: 0,
            dict_size: 0,
            verify: 0,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct CompressArgs {
    pub opt: CompressOptions,
    pub overwrite_header: bool,
    pub workers: usize,
}

impl CompressArgs {
    pub fn new(opt: CompressOptions) -> Self {
        Self {
            opt,
            overwrite_header: false,
            workers: 1,
        }
    }
}

impl Default for CompressArgs {
    fn default() -> Self {
        Self::new(CompressOptions::default())
    }
}

pub trait Compressor: Send {
    fn max_dst_size(&self) -> usize;
    fn src_block_size(&self) -> usize;

    fn compress(&mut self, src: &[u8], dst: &mut [u8]) -> Result<usize>;
    fn decompress(&mut self, src: &[u8], dst: &mut [u8]) -> Result<usize>;

    fn nbatch(&self) -> usize {
        1
    }

    fn compress_batch(
        &mut self,
        src: &[u8],
        src_chunk_len: &[usize],
        dst: &mut [u8],
        dst_chunk_len: &mut [usize],
        n: usize,
    ) -> Result<()> {
        if n == 0 {
            return Ok(());
        }
        ensure!(
            src_chunk_len.len() >= n && dst_chunk_len.len() >= n,
            "chunk length arrays are smaller than n"
        );
        let each_dst_capacity = dst.len() / n;
        ensure!(
            each_dst_capacity >= self.max_dst_size(),
            "destination chunk capacity ({each_dst_capacity}) smaller than max_dst_size ({})",
            self.max_dst_size()
        );

        let mut src_offset = 0usize;
        for i in 0..n {
            let chunk_len = src_chunk_len[i];
            let end = src_offset
                .checked_add(chunk_len)
                .context("source offset overflow")?;
            ensure!(end <= src.len(), "source chunk exceeds source buffer");
            let dst_begin = i
                .checked_mul(each_dst_capacity)
                .context("destination offset overflow")?;
            let dst_end = dst_begin
                .checked_add(each_dst_capacity)
                .context("destination offset overflow")?;

            let nout = self.compress(&src[src_offset..end], &mut dst[dst_begin..dst_end])?;
            dst_chunk_len[i] = nout;
            src_offset = end;
        }
        Ok(())
    }

    fn decompress_batch(
        &mut self,
        src: &[u8],
        src_chunk_len: &[usize],
        dst: &mut [u8],
        dst_chunk_len: &mut [usize],
        n: usize,
    ) -> Result<()> {
        if n == 0 {
            return Ok(());
        }
        ensure!(
            src_chunk_len.len() >= n && dst_chunk_len.len() >= n,
            "chunk length arrays are smaller than n"
        );
        let each_dst_capacity = dst.len() / n;
        ensure!(
            each_dst_capacity >= self.src_block_size(),
            "destination chunk capacity ({each_dst_capacity}) smaller than src_block_size ({})",
            self.src_block_size()
        );

        let mut src_offset = 0usize;
        for i in 0..n {
            let chunk_len = src_chunk_len[i];
            let end = src_offset
                .checked_add(chunk_len)
                .context("source offset overflow")?;
            ensure!(end <= src.len(), "source chunk exceeds source buffer");
            let dst_begin = i
                .checked_mul(each_dst_capacity)
                .context("destination offset overflow")?;
            let dst_end = dst_begin
                .checked_add(each_dst_capacity)
                .context("destination offset overflow")?;

            let nout = self.decompress(&src[src_offset..end], &mut dst[dst_begin..dst_end])?;
            dst_chunk_len[i] = nout;
            src_offset = end;
        }
        Ok(())
    }
}

fn lz4_compress_bound(src_size: usize) -> usize {
    src_size + (src_size / 255) + 16
}

fn zstd_compress_bound(src_size: usize) -> usize {
    src_size
        + (src_size >> 8)
        + if src_size < (128 << 10) {
            ((128 << 10) - src_size) >> 11
        } else {
            0
        }
}

struct Lz4Compressor {
    max_dst_size: usize,
    src_blk_size: usize,
}

impl Lz4Compressor {
    fn new(args: &CompressArgs) -> Result<Self> {
        ensure!(
            args.opt.algo == CompressOptions::LZ4,
            "compression type invalid, expected LZ4"
        );
        let src_blk_size = usize::try_from(args.opt.block_size).context("invalid block_size")?;
        ensure!(src_blk_size != 0, "block_size must be > 0");

        Ok(Self {
            max_dst_size: lz4_compress_bound(src_blk_size),
            src_blk_size,
        })
    }
}

impl Compressor for Lz4Compressor {
    fn max_dst_size(&self) -> usize {
        self.max_dst_size
    }

    fn src_block_size(&self) -> usize {
        self.src_blk_size
    }

    fn compress(&mut self, src: &[u8], dst: &mut [u8]) -> Result<usize> {
        ensure!(
            dst.len() >= self.max_dst_size,
            "destination buffer too small ({}), expected at least {}",
            dst.len(),
            self.max_dst_size
        );
        let compressed = lz4_flex::block::compress(src);
        ensure!(!compressed.is_empty(), "lz4 returned empty output");
        ensure!(
            compressed.len() <= dst.len(),
            "compressed data larger than destination buffer"
        );
        dst[..compressed.len()].copy_from_slice(&compressed);
        Ok(compressed.len())
    }

    fn decompress(&mut self, src: &[u8], dst: &mut [u8]) -> Result<usize> {
        let out_len =
            lz4_flex::block::decompress_into(src, dst).context("lz4 decompress failed")?;
        ensure!(out_len != 0, "lz4 decompress returned zero bytes");
        Ok(out_len)
    }
}

struct ZstdCompressor {
    max_dst_size: usize,
    src_blk_size: usize,
}

impl ZstdCompressor {
    fn new(args: &CompressArgs) -> Result<Self> {
        ensure!(
            args.opt.algo == CompressOptions::ZSTD,
            "compression type invalid, expected ZSTD"
        );

        let src_blk_size = usize::try_from(args.opt.block_size).context("invalid block_size")?;
        ensure!(src_blk_size != 0, "block_size must be > 0");

        Ok(Self {
            max_dst_size: zstd_compress_bound(src_blk_size),
            src_blk_size,
        })
    }
}

impl Compressor for ZstdCompressor {
    fn max_dst_size(&self) -> usize {
        self.max_dst_size
    }

    fn src_block_size(&self) -> usize {
        self.src_blk_size
    }

    fn compress(&mut self, src: &[u8], dst: &mut [u8]) -> Result<usize> {
        ensure!(
            dst.len() >= self.max_dst_size,
            "destination buffer too small ({}), expected at least {}",
            dst.len(),
            self.max_dst_size
        );
        let compressed = zstd::bulk::compress(src, ZSTD_C_LEVEL).context("zstd compress failed")?;
        ensure!(!compressed.is_empty(), "zstd returned empty output");
        ensure!(
            compressed.len() <= dst.len(),
            "compressed data larger than destination buffer"
        );
        dst[..compressed.len()].copy_from_slice(&compressed);
        Ok(compressed.len())
    }

    fn decompress(&mut self, src: &[u8], dst: &mut [u8]) -> Result<usize> {
        ensure!(
            dst.len() >= self.src_blk_size,
            "destination buffer too small ({}), expected at least {}",
            dst.len(),
            self.src_blk_size
        );
        let decompressed =
            zstd::bulk::decompress(src, dst.len()).context("zstd decompress failed")?;
        ensure!(!decompressed.is_empty(), "zstd returned empty output");
        ensure!(
            decompressed.len() <= dst.len(),
            "decompressed data larger than destination buffer"
        );
        dst[..decompressed.len()].copy_from_slice(&decompressed);
        Ok(decompressed.len())
    }
}

pub fn create_compressor(args: &CompressArgs) -> Result<Box<dyn Compressor>> {
    match args.opt.algo {
        CompressOptions::LZ4 => Ok(Box::new(Lz4Compressor::new(args)?)),
        CompressOptions::ZSTD => Ok(Box::new(ZstdCompressor::new(args)?)),
        _ => bail!("invalid compression algorithm"),
    }
}

#[derive(Clone, Debug)]
struct HeaderTrailer {
    magic0: u64,
    magic1: [u8; 16],
    size: u32,
    digest: u32,
    flags: u64,
    index_offset: u64,
    index_size: u64,
    original_file_size: u64,
    index_crc: u32,
    reserved_0: u32,
    opt: CompressOptions,
}

impl HeaderTrailer {
    const MAGIC0: u64 = u64::from_le_bytes(*b"ZFile\0\x01\0");
    const MAGIC1: [u8; 16] = [
        0x74, 0x75, 0x6a, 0x69, 0x2e, 0x79, 0x79, 0x66, 0x40, 0x41, 0x6c, 0x69, 0x62, 0x61, 0x62,
        0x61,
    ];

    const FLAG_SHIFT_HEADER: u32 = 0;
    const FLAG_SHIFT_TYPE: u32 = 1;
    const FLAG_SHIFT_SEALED: u32 = 2;
    const FLAG_SHIFT_HEADER_OVERWRITE: u32 = 3;
    const FLAG_SHIFT_CALC_DIGEST: u32 = 4;
    const FLAG_SHIFT_IDX_COMP: u32 = 5;

    fn new() -> Self {
        Self {
            magic0: Self::MAGIC0,
            magic1: Self::MAGIC1,
            size: HEADER_TRAILER_BODY_SIZE as u32,
            digest: 0,
            flags: 0,
            index_offset: 0,
            index_size: 0,
            original_file_size: 0,
            index_crc: 0,
            reserved_0: 0,
            opt: CompressOptions::default(),
        }
    }

    fn decode_from_512(buf: &[u8]) -> Result<Self> {
        ensure!(
            buf.len() >= HEADER_TRAILER_SPACE,
            "header/trailer buffer too small"
        );
        Ok(Self {
            magic0: read_u64_le(buf, 0)?,
            magic1: buf[8..24].try_into().context("invalid magic1")?,
            size: read_u32_le(buf, 24)?,
            digest: read_u32_le(buf, 28)?,
            flags: read_u64_le(buf, 32)?,
            index_offset: read_u64_le(buf, 40)?,
            index_size: read_u64_le(buf, 48)?,
            original_file_size: read_u64_le(buf, 56)?,
            index_crc: read_u32_le(buf, 64)?,
            reserved_0: read_u32_le(buf, 68)?,
            opt: CompressOptions::decode_from(&buf[72..96])?,
        })
    }

    fn encode_512(&self) -> Result<[u8; HEADER_TRAILER_SPACE]> {
        let mut buf = [0u8; HEADER_TRAILER_SPACE];
        write_u64_le(&mut buf, 0, self.magic0);
        buf[8..24].copy_from_slice(&self.magic1);
        write_u32_le(&mut buf, 24, self.size);
        write_u32_le(&mut buf, 28, self.digest);
        write_u64_le(&mut buf, 32, self.flags);
        write_u64_le(&mut buf, 40, self.index_offset);
        write_u64_le(&mut buf, 48, self.index_size);
        write_u64_le(&mut buf, 56, self.original_file_size);
        write_u32_le(&mut buf, 64, self.index_crc);
        write_u32_le(&mut buf, 68, self.reserved_0);
        self.opt.encode_into(&mut buf[72..96])?;
        Ok(buf)
    }

    fn verify_magic(&self) -> bool {
        self.magic0 == Self::MAGIC0 && self.magic1 == Self::MAGIC1
    }

    fn get_flag_bit(&self, shift: u32) -> bool {
        (self.flags & (1u64 << shift)) != 0
    }

    fn set_flag_bit(&mut self, shift: u32) {
        self.flags |= 1u64 << shift;
    }

    fn clr_flag_bit(&mut self, shift: u32) {
        self.flags &= !(1u64 << shift);
    }

    fn is_header(&self) -> bool {
        self.get_flag_bit(Self::FLAG_SHIFT_HEADER)
    }

    fn is_header_overwrite(&self) -> bool {
        self.get_flag_bit(Self::FLAG_SHIFT_HEADER_OVERWRITE)
    }

    fn is_trailer(&self) -> bool {
        !self.is_header()
    }

    fn is_data_file(&self) -> bool {
        self.get_flag_bit(Self::FLAG_SHIFT_TYPE)
    }

    fn is_sealed(&self) -> bool {
        self.get_flag_bit(Self::FLAG_SHIFT_SEALED)
    }

    fn is_digest_enabled(&self) -> bool {
        self.get_flag_bit(Self::FLAG_SHIFT_CALC_DIGEST)
    }

    fn is_valid(&self, bytes: &[u8; HEADER_TRAILER_SPACE]) -> bool {
        if !self.is_digest_enabled() {
            return true;
        }
        let mut tmp = *bytes;
        tmp[28..32].fill(0);
        crc32c(&tmp) == self.digest
    }

    fn set_header(&mut self) {
        self.set_flag_bit(Self::FLAG_SHIFT_HEADER);
    }

    fn set_trailer(&mut self) {
        self.clr_flag_bit(Self::FLAG_SHIFT_HEADER);
    }

    fn set_data_file(&mut self) {
        self.set_flag_bit(Self::FLAG_SHIFT_TYPE);
    }

    fn set_index_file(&mut self) {
        self.clr_flag_bit(Self::FLAG_SHIFT_TYPE);
    }

    fn set_sealed(&mut self) {
        self.set_flag_bit(Self::FLAG_SHIFT_SEALED);
    }

    fn clr_sealed(&mut self) {
        self.clr_flag_bit(Self::FLAG_SHIFT_SEALED);
    }

    fn set_header_overwrite(&mut self) {
        self.set_flag_bit(Self::FLAG_SHIFT_HEADER_OVERWRITE);
    }

    fn set_digest_enable(&mut self) {
        self.set_flag_bit(Self::FLAG_SHIFT_CALC_DIGEST);
    }

    #[allow(dead_code)]
    fn set_compress_index(&mut self) {
        self.set_flag_bit(Self::FLAG_SHIFT_IDX_COMP);
    }

    fn set_compress_option(&mut self, opt: CompressOptions) {
        self.opt = opt;
    }
}

#[derive(Clone, Debug, Default)]
struct JumpTable {
    group_size: usize,
    partial_offset: Vec<u64>,
    deltas: Vec<u16>,
}

impl JumpTable {
    fn size(&self) -> usize {
        self.deltas.len()
    }

    fn offset_at(&self, idx: usize) -> Result<u64> {
        ensure!(self.group_size != 0, "jump table not initialized");
        ensure!(
            idx < self.deltas.len(),
            "jump table index out of range: idx={idx}, size={}",
            self.deltas.len()
        );

        let part_idx = idx / self.group_size;
        let inner_idx = idx % self.group_size;
        let part_offset = *self.partial_offset.get(part_idx).with_context(|| {
            format!(
                "partial_offset index out of range: idx={part_idx}, size={}",
                self.partial_offset.len()
            )
        })?;

        if inner_idx == 0 {
            return Ok(part_offset);
        }
        Ok(part_offset + u64::from(self.deltas[idx]))
    }

    fn build(
        &mut self,
        ibuf: &[u32],
        offset_begin: u64,
        block_size: u32,
        enable_crc: bool,
    ) -> Result<()> {
        ensure!(block_size != 0, "block_size cannot be zero");

        let block_size = block_size as usize;
        let group_size = ((u16::MAX as usize) + 1) / block_size;
        ensure!(
            group_size != 0,
            "block_size too large for jump table grouping: {block_size}"
        );

        self.group_size = group_size;
        self.partial_offset.clear();
        self.deltas.clear();

        self.partial_offset.reserve(ibuf.len() / group_size + 1);
        self.deltas.reserve(ibuf.len() + 1);

        let mut raw_offset = offset_begin;
        self.partial_offset.push(raw_offset);
        self.deltas.push(0);

        let min_block_size = if enable_crc {
            std::mem::size_of::<u32>()
        } else {
            0
        };
        for i in 1..=ibuf.len() {
            let blk = ibuf[i - 1] as usize;
            ensure!(
                blk > min_block_size,
                "unexpected block size: idx={}, size={}",
                i - 1,
                ibuf[i - 1]
            );

            raw_offset = raw_offset
                .checked_add(ibuf[i - 1] as u64)
                .context("jump table offset overflow")?;

            if i % group_size == 0 {
                self.partial_offset.push(raw_offset);
                self.deltas.push(0);
                continue;
            }

            let prev = u64::from(self.deltas[i - 1]);
            let next = prev
                .checked_add(ibuf[i - 1] as u64)
                .context("delta overflow")?;
            ensure!(
                next < u16::MAX as u64,
                "build block length failed idx={} delta={} blk={} exceeds {}",
                i - 1,
                prev,
                ibuf[i - 1],
                u16::MAX
            );
            self.deltas.push(next as u16);
        }

        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ValidMode {
    Normal,
    CrcOnly,
}

pub struct ZFileRO {
    file: Arc<dyn VirtualFile>,
    ht: HeaderTrailer,
    jump_table: JumpTable,
    compressor: Box<dyn Compressor>,
    valid_mode: ValidMode,
    data_verify: bool,
}

impl ZFileRO {
    pub fn original_size(&self) -> u64 {
        self.ht.original_file_size
    }

    pub fn options(&self) -> CompressOptions {
        self.ht.opt
    }

    pub fn set_crc_check_only(&mut self) {
        self.valid_mode = ValidMode::CrcOnly;
    }

    pub async fn pread(&mut self, buf: &mut [u8], offset: u64) -> Result<usize> {
        self.pread_generic(&DirectRead, buf, offset).await
    }

    /// Same as [`Self::pread`], but compressed-block reads from the underlying
    /// file go through `read_at_into_with_ctx`, allowing a ublk queue to drive
    /// the disk IO submission on its own io_uring thread. The decompression
    /// path is unchanged.
    pub async fn pread_with_ctx<'a>(
        &'a mut self,
        ctx: IoCtx<'a>,
        buf: &mut [u8],
        offset: u64,
    ) -> Result<usize> {
        self.pread_generic(&CtxRead { ctx }, buf, offset).await
    }

    /// Body shared by [`Self::pread`] and [`Self::pread_with_ctx`]; the
    /// per-block reader read is provided by `reader`. Send-ness of the returned
    /// future is monomorphised from `R`: [`DirectRead`] yields a `Send`
    /// future for background callers; [`CtxRead`] yields a `!Send`
    /// future for ublk-queue callers.
    async fn pread_generic<R: FileReader>(
        &mut self,
        reader: &R,
        buf: &mut [u8],
        offset: u64,
    ) -> Result<usize> {
        let block_size = usize::try_from(self.ht.opt.block_size).context("invalid block_size")?;
        ensure!(
            block_size <= MAX_READ_SIZE,
            "block_size {block_size} > MAX_READ_SIZE {MAX_READ_SIZE}"
        );

        if offset >= self.ht.original_file_size {
            return Ok(0);
        }

        let clamped = min(buf.len() as u64, self.ht.original_file_size - offset) as usize;
        if clamped == 0 {
            return Ok(0);
        }

        let begin_idx = (offset / block_size as u64) as usize;
        let end = offset + clamped as u64 - 1;
        let end_idx = (end / block_size as u64) as usize + 1;

        let mut written = 0usize;
        let mut compressed = Vec::new();
        let mut raw = vec![0u8; block_size];

        for idx in begin_idx..end_idx {
            let block_begin = self.jump_table.offset_at(idx)?;
            let block_end = self.jump_table.offset_at(idx + 1)?;
            let encoded_len_u64 = block_end
                .checked_sub(block_begin)
                .context("invalid block offsets")?;
            let encoded_len = u64_to_usize(encoded_len_u64, "encoded_len")?;

            let compressed_size = if self.data_verify {
                encoded_len
                    .checked_sub(std::mem::size_of::<u32>())
                    .context("encoded block smaller than crc32 trailer")?
            } else {
                encoded_len
            };

            let cp_begin = if idx == begin_idx {
                (offset % block_size as u64) as usize
            } else {
                0
            };
            let mut cp_len = if idx == end_idx - 1 {
                (end % block_size as u64) as usize + 1
            } else {
                block_size
            };
            cp_len = cp_len.checked_sub(cp_begin).context("invalid cp_len")?;

            let mut retry = 3;
            loop {
                compressed.resize(encoded_len, 0);
                read_exact(reader, self.file.as_ref(), block_begin, &mut compressed).await?;

                if self.data_verify {
                    let expected = u32::from_le_bytes(
                        compressed[compressed_size..compressed_size + 4]
                            .try_into()
                            .expect("crc32 size"),
                    );
                    let got = crc32c_salt(&compressed[..compressed_size]);
                    if got != expected {
                        if retry > 0 {
                            retry -= 1;
                            self.file.evict_range(block_begin, encoded_len_u64).await?;
                            continue;
                        }
                        bail!(
                            "checksum verification failed: idx={idx}, got={got:#010x}, expected={expected:#010x}"
                        );
                    }
                }

                if self.valid_mode == ValidMode::CrcOnly {
                    break;
                }

                let decomp = if cp_begin == 0 && cp_len == block_size {
                    let dst_slice = &mut buf[written..written + cp_len];
                    let n = self
                        .compressor
                        .decompress(&compressed[..compressed_size], dst_slice)?;
                    if n != cp_len {
                        Err(anyhow::anyhow!(
                            "unexpected decompressed size: got {n}, expected {cp_len}"
                        ))
                    } else {
                        Ok(())
                    }
                } else {
                    let n = self
                        .compressor
                        .decompress(&compressed[..compressed_size], &mut raw)?;
                    let end_pos = cp_begin
                        .checked_add(cp_len)
                        .context("copy range overflow")?;
                    if end_pos > n {
                        Err(anyhow::anyhow!(
                            "decompressed range out of bounds: n={n}, begin={cp_begin}, len={cp_len}"
                        ))
                    } else {
                        buf[written..written + cp_len].copy_from_slice(&raw[cp_begin..end_pos]);
                        Ok(())
                    }
                };

                match decomp {
                    Ok(()) => break,
                    Err(e) => {
                        if retry > 0 {
                            retry -= 1;
                            self.file.evict_range(block_begin, encoded_len_u64).await?;
                            continue;
                        }
                        return Err(e);
                    }
                }
            }

            written += cp_len;
        }

        Ok(written)
    }
}

#[async_trait]
impl VirtualFile for tokio::sync::Mutex<ZFileRO> {
    async fn read_at(&self, offset: u64, len: usize) -> Result<Bytes> {
        let mut ro = self.lock().await;
        let mut buf = vec![0u8; len];
        let n = ro.pread(&mut buf, offset).await?;
        buf.truncate(n);
        Ok(Bytes::from(buf))
    }

    async fn read_at_into(&self, offset: u64, dst: &mut [u8]) -> Result<usize> {
        let mut ro = self.lock().await;
        ro.pread(dst, offset).await
    }

    async fn write_at(&self, _offset: u64, _data: &[u8]) -> Result<usize> {
        bail!("zfile reader is read-only")
    }

    async fn size(&self) -> Result<u64> {
        let ro = self.lock().await;
        Ok(ro.original_size())
    }

    fn read_at_with_ctx<'a>(
        &'a self,
        ctx: IoCtx<'a>,
        offset: u64,
        len: usize,
    ) -> LocalBoxFuture<'a, Result<Bytes>> {
        Box::pin(async move {
            let mut ro = self.lock().await;
            let mut buf = vec![0u8; len];
            let n = ro.pread_with_ctx(ctx, &mut buf, offset).await?;
            buf.truncate(n);
            Ok(Bytes::from(buf))
        })
    }

    fn read_at_into_with_ctx<'a>(
        &'a self,
        ctx: IoCtx<'a>,
        offset: u64,
        dst: &'a mut [u8],
    ) -> LocalBoxFuture<'a, Result<usize>> {
        Box::pin(async move {
            let mut ro = self.lock().await;
            ro.pread_with_ctx(ctx, dst, offset).await
        })
    }
}

async fn load_jump_table(file: Arc<dyn VirtualFile>) -> Result<(HeaderTrailer, JumpTable)> {
    let mut buf = [0u8; HEADER_TRAILER_SPACE];
    read_exact_at(file.as_ref(), &mut buf, 0).await?;
    let header = HeaderTrailer::decode_from_512(&buf)?;

    ensure!(
        header.verify_magic() && header.is_header(),
        "header magic/type mismatch"
    );
    ensure!(header.is_valid(&buf), "header digest validation failed");

    let st_size = file.size().await?;
    let mut selected = header.clone();
    let index_bytes: usize;

    if !header.is_header_overwrite() {
        ensure!(header.is_data_file(), "unsupported zfile type");
        ensure!(
            st_size >= HEADER_TRAILER_SPACE as u64,
            "file too small for trailer"
        );

        let trailer_offset = st_size - HEADER_TRAILER_SPACE as u64;
        read_exact_at(file.as_ref(), &mut buf, trailer_offset).await?;
        let trailer = HeaderTrailer::decode_from_512(&buf)?;
        ensure!(
            trailer.verify_magic()
                && trailer.is_trailer()
                && trailer.is_data_file()
                && trailer.is_sealed(),
            "trailer magic/type/sealed mismatch"
        );

        index_bytes = mul_u64_to_usize(trailer.index_size, 4, "index_size")?;
        ensure!(
            trailer.index_offset <= trailer_offset,
            "invalid index_offset beyond trailer"
        );
        let max_index_bytes = trailer_offset - trailer.index_offset;
        ensure!(index_bytes as u64 <= max_index_bytes, "invalid index bytes");
        selected = trailer;
    } else {
        index_bytes = mul_u64_to_usize(header.index_size, 4, "index_size")?;
    }

    let index_raw = read_exact_chunks_at(file.clone(), selected.index_offset, index_bytes).await?;

    if selected.is_digest_enabled() {
        let crc = crc32c(&index_raw);
        ensure!(
            crc == selected.index_crc,
            "jump table checksum mismatch: got {crc:#010x}, expected {:#010x}",
            selected.index_crc
        );
    }

    let nindex = u64_to_usize(selected.index_size, "index_size")?;
    ensure!(nindex != 0, "empty index is not a valid zfile");

    ensure!(
        index_raw.len() == nindex * std::mem::size_of::<u32>(),
        "index byte length mismatch"
    );

    let mut ibuf = Vec::with_capacity(nindex);
    for chunk in index_raw.chunks_exact(4) {
        ibuf.push(u32::from_le_bytes(chunk.try_into().expect("u32 chunk")));
    }

    let mut jump_table = JumpTable::default();
    jump_table.build(
        &ibuf,
        HEADER_TRAILER_SPACE as u64 + selected.opt.dict_size as u64,
        selected.opt.block_size,
        selected.opt.verify != 0,
    )?;

    ensure!(
        jump_table.size() == nindex + 1,
        "jump table entry count mismatch"
    );

    Ok((selected, jump_table))
}

pub async fn zfile_open_ro(file: Arc<dyn VirtualFile>, verify: bool) -> Result<ZFileRO> {
    let mut retry = 2;
    let (ht, jump_table) = loop {
        match load_jump_table(file.clone()).await {
            Ok(v) => break v,
            Err(e) => {
                if verify && retry > 0 {
                    retry -= 1;
                    file.evict_all().await?;
                    continue;
                }
                return Err(e);
            }
        }
    };
    let args = CompressArgs::new(ht.opt);
    let compressor = create_compressor(&args)?;
    let data_verify = ht.opt.verify != 0;

    Ok(ZFileRO {
        file,
        ht,
        jump_table,
        compressor,
        valid_mode: ValidMode::Normal,
        data_verify,
    })
}

pub async fn zfile_open_ro_vfile(
    file: Arc<dyn VirtualFile>,
    verify: bool,
) -> Result<Arc<dyn VirtualFile>> {
    let ro = zfile_open_ro(file, verify).await?;
    Ok(Arc::new(tokio::sync::Mutex::new(ro)))
}

async fn write_header_trailer(
    file: &(dyn VirtualFile + Send + Sync),
    is_header: bool,
    is_sealed: bool,
    is_data_file: bool,
    overwrite_header: bool,
    ht: &mut HeaderTrailer,
    offset: u64,
) -> Result<()> {
    if is_header {
        ht.set_header();
    } else {
        ht.set_trailer();
    }

    if is_sealed {
        ht.set_sealed();
    } else {
        ht.clr_sealed();
    }

    if is_data_file {
        ht.set_data_file();
    } else {
        ht.set_index_file();
    }

    if overwrite_header {
        ht.set_header_overwrite();
    }

    ht.set_digest_enable();
    ht.digest = 0;
    let mut encoded = ht.encode_512()?;
    encoded[28..32].fill(0);
    ht.digest = crc32c(&encoded);
    write_u32_le(&mut encoded, 28, ht.digest);

    write_all_at(file, &encoded, offset).await
}

fn compress_data(
    compressor: &mut dyn Compressor,
    src: &[u8],
    dst: &mut [u8],
    gen_crc: bool,
) -> Result<usize> {
    let mut compressed_len = compressor.compress(src, dst)?;
    ensure!(compressed_len != 0, "compress returned zero");
    if gen_crc {
        let end = compressed_len
            .checked_add(std::mem::size_of::<u32>())
            .context("crc length overflow")?;
        ensure!(
            end <= dst.len(),
            "destination buffer cannot hold crc32 trailer"
        );
        let crc = crc32c_salt(&dst[..compressed_len]);
        write_u32_le(dst, compressed_len, crc);
        compressed_len = end;
    }
    Ok(compressed_len)
}

pub struct ZFileBuilder {
    dest: Arc<dyn VirtualFile>,
    args: CompressArgs,
    opt: CompressOptions,
    compressor: Box<dyn Compressor>,
    block_len: Vec<u32>,
    compressed_data: Vec<u8>,
    reserved_buf: Vec<u8>,
    reserved_size: usize,
    raw_data_size: u64,
    moffset: u64,
    ht: HeaderTrailer,
    finished: bool,
}

impl ZFileBuilder {
    pub async fn new(file: Arc<dyn VirtualFile>, args: &CompressArgs) -> Result<Self> {
        let mut ht = HeaderTrailer::new();
        ht.set_compress_option(args.opt);
        write_header_trailer(file.as_ref(), true, false, true, false, &mut ht, 0).await?;

        let block_size = usize::try_from(args.opt.block_size).context("invalid block_size")?;
        ensure!(block_size != 0, "block_size must be > 0");

        let buf_size = block_size
            .checked_add(BUF_SIZE)
            .context("buffer size overflow")?;

        Ok(Self {
            dest: file,
            args: *args,
            opt: args.opt,
            compressor: create_compressor(args)?,
            block_len: Vec::new(),
            compressed_data: vec![0u8; buf_size],
            reserved_buf: vec![0u8; buf_size],
            reserved_size: 0,
            raw_data_size: 0,
            moffset: HEADER_TRAILER_SPACE as u64 + args.opt.dict_size as u64,
            ht,
            finished: false,
        })
    }

    async fn write_buffer(&mut self, src: &[u8]) -> Result<()> {
        let compressed_len = compress_data(
            self.compressor.as_mut(),
            src,
            &mut self.compressed_data,
            self.opt.verify != 0,
        )?;

        write_all_at(
            self.dest.as_ref(),
            &self.compressed_data[..compressed_len],
            self.moffset,
        )
        .await?;
        self.block_len.push(
            u32::try_from(compressed_len)
                .context("compressed_len cannot fit in u32 jump table entry")?,
        );
        self.moffset = self
            .moffset
            .checked_add(compressed_len as u64)
            .context("moffset overflow")?;
        Ok(())
    }

    async fn write_blocks_parallel(&mut self, blocks: Vec<Vec<u8>>) -> Result<()> {
        if blocks.is_empty() {
            return Ok(());
        }

        if self.args.workers <= 1 || blocks.len() == 1 {
            for blk in blocks {
                self.write_buffer(&blk).await?;
            }
            return Ok(());
        }

        let mut tasks = Vec::with_capacity(blocks.len());
        let opt = self.opt;
        let gen_crc = self.opt.verify != 0;
        let buf_size = usize::try_from(opt.block_size)
            .context("invalid block_size")?
            .checked_add(BUF_SIZE)
            .context("buffer size overflow")?;

        for blk in blocks {
            tasks.push(tokio::task::spawn_blocking(move || -> Result<Vec<u8>> {
                let args = CompressArgs::new(opt);
                let mut compressor = create_compressor(&args)?;
                let mut out = vec![0u8; buf_size];
                let n = compress_data(compressor.as_mut(), &blk, &mut out, gen_crc)?;
                out.truncate(n);
                Ok(out)
            }));
        }

        for task in tasks {
            let compressed = task.await.context("compress worker join failed")??;
            write_all_at(self.dest.as_ref(), &compressed, self.moffset).await?;
            self.block_len.push(
                u32::try_from(compressed.len())
                    .context("compressed_len cannot fit in u32 jump table entry")?,
            );
            self.moffset = self
                .moffset
                .checked_add(compressed.len() as u64)
                .context("moffset overflow")?;
        }
        Ok(())
    }

    pub async fn write(&mut self, mut buf: &[u8]) -> Result<usize> {
        ensure!(!self.finished, "builder already closed");

        self.raw_data_size = self
            .raw_data_size
            .checked_add(buf.len() as u64)
            .context("raw_data_size overflow")?;

        let expected = buf.len();
        let block_size = self.opt.block_size as usize;
        let worker_batch = self.args.workers.max(1);

        if self.reserved_size != 0 {
            let needed = block_size - self.reserved_size;
            if buf.len() < needed {
                self.reserved_buf[self.reserved_size..self.reserved_size + buf.len()]
                    .copy_from_slice(buf);
                self.reserved_size += buf.len();
                return Ok(expected);
            }

            self.reserved_buf[self.reserved_size..self.reserved_size + needed]
                .copy_from_slice(&buf[..needed]);
            let chunk = self.reserved_buf[..block_size].to_vec();
            self.write_buffer(&chunk).await?;
            self.reserved_size = 0;
            buf = &buf[needed..];
        }

        while buf.len() >= block_size {
            let full_blocks = buf.len() / block_size;
            let batch_blocks = min(full_blocks, worker_batch);
            let batch_bytes = batch_blocks
                .checked_mul(block_size)
                .context("batch_bytes overflow")?;
            let (head, tail) = buf.split_at(batch_bytes);
            let mut blocks = Vec::with_capacity(batch_blocks);
            for chunk in head.chunks_exact(block_size) {
                blocks.push(chunk.to_vec());
            }
            self.write_blocks_parallel(blocks).await?;
            buf = tail;
        }

        if !buf.is_empty() {
            self.reserved_buf[..buf.len()].copy_from_slice(buf);
            self.reserved_size = buf.len();
        }

        Ok(expected)
    }

    pub async fn finish(&mut self) -> Result<()> {
        if self.finished {
            return Ok(());
        }

        if self.reserved_size != 0 {
            let chunk = self.reserved_buf[..self.reserved_size].to_vec();
            self.write_buffer(&chunk).await?;
            self.reserved_size = 0;
        }

        let index_offset = self.moffset;
        let index_size = self.block_len.len() as u64;
        let mut index_raw = Vec::with_capacity(self.block_len.len() * std::mem::size_of::<u32>());
        for len in &self.block_len {
            index_raw.extend_from_slice(&len.to_le_bytes());
        }

        write_all_at(self.dest.as_ref(), &index_raw, index_offset).await?;

        self.ht.index_crc = crc32c(&index_raw);
        self.ht.index_offset = index_offset;
        self.ht.index_size = index_size;
        self.ht.original_file_size = self.raw_data_size;

        let trailer_offset = index_offset
            .checked_add(index_raw.len() as u64)
            .context("trailer offset overflow")?;
        write_header_trailer(
            self.dest.as_ref(),
            false,
            true,
            true,
            false,
            &mut self.ht,
            trailer_offset,
        )
        .await?;
        if self.args.overwrite_header {
            write_header_trailer(self.dest.as_ref(), true, false, true, true, &mut self.ht, 0)
                .await?;
        }

        self.finished = true;
        Ok(())
    }

    pub async fn close(mut self) -> Result<Arc<dyn VirtualFile>> {
        self.finish().await?;
        Ok(self.dest.clone())
    }
}

pub async fn new_zfile_builder(
    file: Arc<dyn VirtualFile>,
    args: &CompressArgs,
) -> Result<ZFileBuilder> {
    ZFileBuilder::new(file, args).await
}

pub async fn zfile_compress(
    src: Arc<dyn VirtualFile>,
    dst: Arc<dyn VirtualFile>,
    args: &CompressArgs,
) -> Result<()> {
    let opt = args.opt;
    let mut compressor = create_compressor(args)?;

    let mut ht = HeaderTrailer::new();
    ht.set_compress_option(opt);
    write_header_trailer(dst.as_ref(), true, false, true, false, &mut ht, 0).await?;

    let block_size = usize::try_from(opt.block_size).context("invalid block_size")?;
    ensure!(block_size != 0, "block_size must be > 0");

    let buf_size = block_size
        .checked_add(BUF_SIZE)
        .context("buffer size overflow")?;

    let nbatch = compressor.nbatch().max(1);

    let mut block_len: Vec<u32> = Vec::new();
    let mut moffset = HEADER_TRAILER_SPACE as u64 + opt.dict_size as u64;
    let mut infile_size = 0u64;
    let src_size = src.size().await?;
    let mut src_offset = 0u64;

    let mut raw_data = vec![0u8; block_size * nbatch];
    let mut compressed_data = vec![0u8; buf_size * nbatch];

    while src_offset < src_size {
        let remain = (src_size - src_offset) as usize;
        let to_read = min(remain, raw_data.len());
        let mut readn = 0usize;
        while readn < to_read {
            let chunk = src
                .read_at(src_offset + readn as u64, to_read - readn)
                .await?;
            if chunk.is_empty() {
                break;
            }
            let n = chunk.len();
            raw_data[readn..readn + n].copy_from_slice(&chunk);
            readn += n;
        }
        if readn == 0 {
            break;
        }
        src_offset = src_offset
            .checked_add(readn as u64)
            .context("src_offset overflow")?;
        infile_size = infile_size
            .checked_add(readn as u64)
            .context("infile_size overflow")?;

        let mut raw_chunk_len = Vec::with_capacity(nbatch);
        let mut remain = readn;
        while remain > 0 {
            let chunk = min(remain, block_size);
            raw_chunk_len.push(chunk);
            remain -= chunk;
        }
        let n = raw_chunk_len.len();

        let mut compressed_len = vec![0usize; n];
        compressor.compress_batch(
            &raw_data[..readn],
            &raw_chunk_len,
            &mut compressed_data[..n * buf_size],
            &mut compressed_len,
            n,
        )?;

        for (j, compressed) in compressed_len.iter().copied().enumerate() {
            let begin = j * buf_size;
            let end = begin + compressed;
            write_all_at(dst.as_ref(), &compressed_data[begin..end], moffset).await?;
            let mut written = compressed;

            if opt.verify != 0 {
                let crc = crc32c_salt(&compressed_data[begin..end]);
                write_all_at(
                    dst.as_ref(),
                    &crc.to_le_bytes(),
                    moffset
                        .checked_add(written as u64)
                        .context("crc offset overflow")?,
                )
                .await?;
                written += std::mem::size_of::<u32>();
            }

            block_len.push(u32::try_from(written).context("compressed length cannot fit in u32")?);
            moffset = moffset
                .checked_add(written as u64)
                .context("moffset overflow")?;
        }
    }

    let index_offset = moffset;
    let index_size = block_len.len() as u64;
    let mut index_raw = Vec::with_capacity(block_len.len() * std::mem::size_of::<u32>());
    for len in &block_len {
        index_raw.extend_from_slice(&len.to_le_bytes());
    }

    write_all_at(dst.as_ref(), &index_raw, index_offset).await?;

    ht.index_crc = crc32c(&index_raw);
    ht.index_offset = index_offset;
    ht.index_size = index_size;
    ht.original_file_size = infile_size;

    let trailer_offset = index_offset
        .checked_add(index_raw.len() as u64)
        .context("trailer offset overflow")?;
    write_header_trailer(
        dst.as_ref(),
        false,
        true,
        true,
        false,
        &mut ht,
        trailer_offset,
    )
    .await?;
    if args.overwrite_header {
        write_header_trailer(dst.as_ref(), true, false, true, true, &mut ht, 0).await?;
    }

    Ok(())
}

pub async fn zfile_decompress(src: Arc<dyn VirtualFile>, dst: Arc<dyn VirtualFile>) -> Result<()> {
    let mut zf = zfile_open_ro(src, true).await?;
    let raw_data_size = zf.original_size();
    let block_size = zf.options().block_size as usize;

    let mut raw_buf = vec![0u8; block_size];
    let mut offset = 0u64;
    while offset < raw_data_size {
        let len = min(block_size as u64, raw_data_size - offset) as usize;
        let readn = zf.pread(&mut raw_buf[..len], offset).await?;
        ensure!(
            readn == len,
            "failed to read decompressed data: offset={offset}, expect={len}, got={readn}"
        );
        write_all_at(dst.as_ref(), &raw_buf[..readn], offset).await?;
        offset += len as u64;
    }

    Ok(())
}

pub async fn zfile_validation_check(src: Arc<dyn VirtualFile>) -> Result<()> {
    let mut zf = zfile_open_ro(src, true).await?;
    ensure!(
        zf.options().verify != 0,
        "source file does not have data checksum"
    );

    zf.set_crc_check_only();

    let raw_data_size = zf.original_size();
    let block_size = zf.options().block_size as usize;
    let mut scratch = vec![0u8; block_size];

    let mut offset = 0u64;
    while offset < raw_data_size {
        let len = min(block_size as u64, raw_data_size - offset) as usize;
        let readn = zf.pread(&mut scratch[..len], offset).await?;
        ensure!(
            readn == len,
            "crc check failed in block {}",
            offset / block_size as u64
        );
        offset += len as u64;
    }

    Ok(())
}

pub async fn is_zfile(file: Arc<dyn VirtualFile>) -> Result<i32> {
    let mut buf = [0u8; HEADER_TRAILER_SPACE];
    read_exact_at(file.as_ref(), &mut buf, 0).await?;

    let ht = HeaderTrailer::decode_from_512(&buf)?;
    if !ht.verify_magic() || !ht.is_header() {
        return Ok(0);
    }
    ensure!(
        ht.is_valid(&buf),
        "file is zfile but header digest is invalid"
    );
    Ok(1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::local::LocalFile;
    use crate::test_utils::test_io_ring;
    use rand::{rngs::StdRng, Rng, RngExt, SeedableRng};
    use std::sync::Arc;
    use tempfile::NamedTempFile;

    const VERIFY_DATA_SIZE: usize = 1024 * 1024 + 333;

    async fn write_all(file: &Arc<dyn VirtualFile>, data: &[u8]) {
        write_all_at(file.as_ref(), data, 0)
            .await
            .expect("write source");
    }

    async fn read_all(file: &Arc<dyn VirtualFile>) -> Vec<u8> {
        let size = file.size().await.expect("size read") as usize;
        let mut out = vec![0u8; size];
        if size > 0 {
            read_exact_at(file.as_ref(), &mut out, 0)
                .await
                .expect("read all");
        }
        out
    }

    async fn new_local_vfile() -> Arc<dyn VirtualFile> {
        let tmp = NamedTempFile::new().expect("create temp file");
        let path = tmp.path().to_path_buf();
        drop(tmp);
        Arc::new(
            LocalFile::new(path, test_io_ring())
                .await
                .expect("create local vfile"),
        )
    }

    fn sample_data(size: usize) -> Vec<u8> {
        let mut rng = StdRng::seed_from_u64(0x5a5a_1122_7788_ccdd);
        let mut data = vec![0u8; size];
        rng.fill_bytes(&mut data);
        data
    }

    async fn seqread_compare(src: &[u8], ro: &mut ZFileRO) {
        let mut buf = vec![0u8; 16 * 1024];
        let mut offset = 0usize;
        while offset < src.len() {
            let len = min(buf.len(), src.len() - offset);
            let readn = ro
                .pread(&mut buf[..len], offset as u64)
                .await
                .expect("sequential pread");
            assert_eq!(readn, len, "sequential read length mismatch at {offset}");
            assert_eq!(
                &buf[..len],
                &src[offset..offset + len],
                "sequential read content mismatch at {offset}"
            );
            offset += len;
        }
    }

    async fn randread_compare(src: &[u8], ro: &mut ZFileRO, seed: u64) {
        if src.len() < 512 {
            return;
        }
        let mut rng = StdRng::seed_from_u64(seed);
        let counts = src.len() / 512;
        let mut small_buf = vec![0u8; 16 * 1024];

        for _ in 0..400 {
            let offset_blk = (rng.next_u32() as usize) % counts;
            let mut len_blk = min(counts - offset_blk, (rng.next_u32() as usize) % 32);
            if len_blk == 0 {
                len_blk = 1;
            }

            let offset = offset_blk * 512;
            let len = min(len_blk * 512, src.len() - offset);
            let readn = ro
                .pread(&mut small_buf[..len], offset as u64)
                .await
                .expect("random pread");
            assert_eq!(readn, len, "random read length mismatch at {offset}");
            assert_eq!(
                &small_buf[..len],
                &src[offset..offset + len],
                "random read content mismatch at {offset}"
            );
        }

        let large_len = MAX_READ_SIZE * 2;
        if src.len() <= large_len {
            return;
        }
        let mut large_buf = vec![0u8; large_len];
        for _ in 0..120 {
            let max_offset = src.len() - large_len;
            let offset = rng.random_range(0..=max_offset);
            let readn = ro
                .pread(&mut large_buf, offset as u64)
                .await
                .expect("large pread");
            assert_eq!(readn, large_len, "large read length mismatch at {offset}");
            assert_eq!(
                &large_buf[..],
                &src[offset..offset + large_len],
                "large read content mismatch at {offset}"
            );
        }
    }

    async fn build_zfile(data: &[u8], workers: usize, seed: u64) -> Arc<dyn VirtualFile> {
        let dst = new_local_vfile().await;
        let args = CompressArgs {
            opt: CompressOptions::new(CompressOptions::LZ4, 4096, 1),
            overwrite_header: false,
            workers,
        };
        let mut rng = StdRng::seed_from_u64(seed);

        {
            let mut builder = new_zfile_builder(dst.clone(), &args)
                .await
                .expect("create builder");
            let mut offset = 0usize;
            while offset < data.len() {
                let step = ((rng.next_u32() as usize) % 8192) + 1;
                let chunk = min(step, data.len() - offset);
                let n = builder
                    .write(&data[offset..offset + chunk])
                    .await
                    .expect("builder write");
                assert_eq!(n, chunk);
                offset += chunk;
            }
            builder.finish().await.expect("builder finish");
        }

        dst
    }

    #[tokio::test]
    async fn test_verify_compression_matrix() {
        let data = sample_data(VERIFY_DATA_SIZE);

        for enable_crc in [0u8, 1u8] {
            for algo in [CompressOptions::LZ4, CompressOptions::ZSTD] {
                for bs_shift in 12..=16 {
                    let block_size = 1u32 << bs_shift;
                    let case_tag = format!("crc={enable_crc},algo={algo},bs={block_size}");

                    let src = new_local_vfile().await;
                    let dst = new_local_vfile().await;
                    let dec = new_local_vfile().await;
                    write_all(&src, &data).await;

                    let args = CompressArgs {
                        opt: CompressOptions::new(algo, block_size, enable_crc),
                        overwrite_header: false,
                        workers: 1,
                    };
                    zfile_compress(src.clone(), dst.clone(), &args)
                        .await
                        .unwrap_or_else(|e| panic!("compress failed for {case_tag}: {e}"));

                    assert_eq!(
                        is_zfile(dst.clone()).await.expect("is_zfile"),
                        1,
                        "dst should be zfile for {case_tag}"
                    );

                    let mut ro = zfile_open_ro(dst.clone(), enable_crc != 0)
                        .await
                        .unwrap_or_else(|e| panic!("open ro failed for {case_tag}: {e}"));
                    seqread_compare(&data, &mut ro).await;
                    randread_compare(
                        &data,
                        &mut ro,
                        0x77aa_5511 + block_size as u64 + algo as u64,
                    )
                    .await;

                    zfile_decompress(dst.clone(), dec.clone())
                        .await
                        .unwrap_or_else(|e| panic!("decompress failed for {case_tag}: {e}"));
                    assert_eq!(
                        read_all(&dec).await,
                        data,
                        "decompress compare failed for {case_tag}"
                    );
                    assert_eq!(
                        is_zfile(dec.clone()).await.expect("is_zfile decompressed"),
                        0,
                        "decompressed file should not be zfile for {case_tag}"
                    );
                }
            }
        }
    }

    #[tokio::test]
    async fn test_validation_check() {
        let data = sample_data(2 * 1024 * 1024 + 517);

        let src = new_local_vfile().await;
        let dst = new_local_vfile().await;
        write_all(&src, &data).await;

        let args = CompressArgs {
            opt: CompressOptions::new(CompressOptions::LZ4, 4096, 1),
            overwrite_header: false,
            workers: 1,
        };
        zfile_compress(src.clone(), dst.clone(), &args)
            .await
            .expect("compress");
        zfile_validation_check(dst.clone())
            .await
            .expect("validation before corruption");

        let corruption = vec![0xEEu8; 8192];
        write_all_at(dst.as_ref(), &corruption, 8192)
            .await
            .expect("corrupt data blocks");
        assert!(zfile_validation_check(dst.clone()).await.is_err());
    }

    #[tokio::test]
    async fn test_ht_check() {
        let data = sample_data(1024 * 1024 + 257);

        let src = new_local_vfile().await;
        let dst = new_local_vfile().await;
        write_all(&src, &data).await;

        let args = CompressArgs {
            opt: CompressOptions::new(CompressOptions::LZ4, 4096, 1),
            overwrite_header: false,
            workers: 1,
        };
        zfile_compress(src.clone(), dst.clone(), &args)
            .await
            .expect("compress");

        let bad_header = 2324u32.to_le_bytes();
        write_all_at(dst.as_ref(), &bad_header, 400)
            .await
            .expect("corrupt header");
        assert!(zfile_validation_check(dst.clone()).await.is_err());
        assert!(is_zfile(dst.clone()).await.is_err());
    }

    #[tokio::test]
    async fn test_verify_builder() {
        let data = sample_data(1024 * 1024 + 99);

        let zfile_mp = build_zfile(&data, 4, 0x4567_9001).await;
        let zfile_sp = build_zfile(&data, 1, 0x9753_1110).await;

        let size_mp = zfile_mp.size().await.expect("metadata mp");
        let size_sp = zfile_sp.size().await.expect("metadata sp");
        assert_eq!(size_mp, size_sp, "builder output file size mismatch");

        let bytes_mp = read_all(&zfile_mp).await;
        let bytes_sp = read_all(&zfile_sp).await;
        assert_eq!(bytes_mp, bytes_sp, "builder output byte mismatch");

        let mut ro = zfile_open_ro(zfile_mp.clone(), true)
            .await
            .expect("open mp zfile");
        seqread_compare(&data, &mut ro).await;
        randread_compare(&data, &mut ro, 0x9abc_dd01).await;
    }

    #[tokio::test]
    async fn test_overwrite_header_path() {
        let data = sample_data(1024 * 1024 + 123);
        let src = new_local_vfile().await;
        let dst = new_local_vfile().await;
        write_all(&src, &data).await;

        let args = CompressArgs {
            opt: CompressOptions::new(CompressOptions::LZ4, 4096, 1),
            overwrite_header: true,
            workers: 1,
        };
        zfile_compress(src.clone(), dst.clone(), &args)
            .await
            .expect("compress with overwrite_header");

        let size = dst.size().await.expect("zfile size");
        assert!(size > HEADER_TRAILER_SPACE as u64);
        let trailer_offset = size - HEADER_TRAILER_SPACE as u64;
        // Corrupt trailer deliberately: open path should still rely on overwritten header.
        write_all_at(dst.as_ref(), &[0xEE; 32], trailer_offset)
            .await
            .expect("corrupt trailer");

        let mut ro = zfile_open_ro(dst.clone(), true)
            .await
            .expect("open zfile by overwritten header");
        seqread_compare(&data, &mut ro).await;
        randread_compare(&data, &mut ro, 0x5566_aabb).await;
    }

    #[tokio::test]
    async fn test_crc32c_consistency() {
        assert_eq!(crc32c(b""), 0x0000_0000);
        assert_eq!(crc32c(b"123456789"), 0x58e3_fa20);
        assert_eq!(crc32c_salt(b"123456789"), 0x3c9b_d4e0);

        let mut rng = StdRng::seed_from_u64(0x8000_22dd_3388_12ab);
        let mut buf = [0u8; 1024];
        for _ in 0..3000 {
            rng.fill_bytes(&mut buf);
            let direct = crc32c(&buf);

            let split = (rng.next_u32() as usize) % buf.len();
            let mut digest = CRC32C_RAW.digest();
            digest.update(&buf[..split]);
            digest.update(&buf[split..]);
            let incremental = digest.finalize();

            assert_eq!(direct, incremental);
        }
    }

    #[tokio::test]
    async fn test_compress_block_sizes() {
        let data = sample_data(128 * 1024);
        for block_size in [4096, 8192, 16384, 32768, 65536] {
            let src = new_local_vfile().await;
            let dst = new_local_vfile().await;
            write_all(&src, &data).await;

            let args = CompressArgs {
                opt: CompressOptions::new(CompressOptions::LZ4, block_size, 1),
                overwrite_header: false,
                workers: 1,
            };
            zfile_compress(src, dst.clone(), &args).await.unwrap();
            let mut ro = zfile_open_ro(dst, false).await.unwrap();
            seqread_compare(&data, &mut ro).await;
        }
    }

    #[tokio::test]
    async fn test_zstd_compression() {
        let data = sample_data(64 * 1024);
        let src = new_local_vfile().await;
        let dst = new_local_vfile().await;
        write_all(&src, &data).await;

        let args = CompressArgs {
            opt: CompressOptions::new(CompressOptions::ZSTD, 4096, 1),
            overwrite_header: false,
            workers: 1,
        };
        zfile_compress(src, dst.clone(), &args).await.unwrap();
        let mut ro = zfile_open_ro(dst, false).await.unwrap();
        seqread_compare(&data, &mut ro).await;
    }

    #[tokio::test]
    async fn test_single_block() {
        let data = sample_data(2048);
        let src = new_local_vfile().await;
        let dst = new_local_vfile().await;
        write_all(&src, &data).await;

        let args = CompressArgs {
            opt: CompressOptions::new(CompressOptions::LZ4, 4096, 1),
            overwrite_header: false,
            workers: 1,
        };
        zfile_compress(src, dst.clone(), &args).await.unwrap();
        let mut ro = zfile_open_ro(dst, false).await.unwrap();
        seqread_compare(&data, &mut ro).await;
    }

    #[tokio::test]
    async fn test_is_zfile_detection() {
        let vf = new_local_vfile().await;
        let data = vec![0u8; 1024];
        write_all(&vf, &data).await;
        let result = is_zfile(vf).await.unwrap();
        assert_eq!(result, 0);
    }

    #[tokio::test]
    async fn test_zfile_open_ro_vfile() {
        let data = sample_data(8192);
        let src = new_local_vfile().await;
        let dst = new_local_vfile().await;
        write_all(&src, &data).await;
        let args = CompressArgs {
            opt: CompressOptions::new(CompressOptions::LZ4, 4096, 1),
            overwrite_header: false,
            workers: 1,
        };
        zfile_compress(src, dst.clone(), &args).await.unwrap();
        let _ro = zfile_open_ro_vfile(dst, false).await.unwrap();
    }

    #[tokio::test]
    async fn test_zfile_decompress() {
        let data = sample_data(8192);
        let src = new_local_vfile().await;
        let compressed = new_local_vfile().await;
        let decompressed = new_local_vfile().await;
        write_all(&src, &data).await;
        let args = CompressArgs {
            opt: CompressOptions::new(CompressOptions::LZ4, 4096, 1),
            overwrite_header: false,
            workers: 1,
        };
        zfile_compress(src, compressed.clone(), &args)
            .await
            .unwrap();
        zfile_decompress(compressed, decompressed.clone())
            .await
            .unwrap();
        let size = decompressed.size().await.unwrap();
        assert_eq!(size, 8192);
    }

    #[tokio::test]
    async fn test_zfile_builder_write() {
        let dst = new_local_vfile().await;
        let args = CompressArgs {
            opt: CompressOptions::new(CompressOptions::LZ4, 4096, 1),
            overwrite_header: false,
            workers: 1,
        };
        let mut builder = new_zfile_builder(dst.clone(), &args).await.unwrap();
        let data = vec![0xAB; 2048];
        builder.write(&data).await.unwrap();
        builder.finish().await.unwrap();
        let _file = builder.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_zfile_pread() {
        let data = sample_data(8192);
        let src = new_local_vfile().await;
        let dst = new_local_vfile().await;
        write_all(&src, &data).await;
        let args = CompressArgs {
            opt: CompressOptions::new(CompressOptions::LZ4, 4096, 1),
            overwrite_header: false,
            workers: 1,
        };
        zfile_compress(src, dst.clone(), &args).await.unwrap();
        let mut ro = zfile_open_ro(dst, false).await.unwrap();
        let mut buf = vec![0u8; 1024];
        let n = ro.pread(&mut buf, 1024).await.unwrap();
        assert_eq!(n, 1024);
        assert_eq!(buf, &data[1024..2048]);
    }
}
