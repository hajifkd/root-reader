use crate::internal::*;
use crate::{RootIoError, VER_THRESHOLD_KEY};
use byteorder::{BigEndian, ReadBytesExt};
use std::io::{Read, Seek, SeekFrom};

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum StreamKind {
    Uncompressed,
    ZlibNew,
    ZlibOld,
    Lzma,
    Zstd,
}

const HEADER_SIZE: usize = 9;

impl StreamKind {}

#[derive(Debug)]
pub struct RootKey {
    pub begin: u64,
    pub meta_data: Vec<u8>,
    pub obj_begin: u64,
    pub nbytes: u32,
    pub version: u16,
    pub obj_len: u32,
    pub datime: u32,
    pub key_len: u16,
    pub cycle: u16,
    pub seek_key: u64,
    pub seek_pdir: u64,
    pub class_name: String,
    pub name: String,
    pub title: String,
}

impl RootKey {
    pub(crate) fn new(reader: &mut (impl Read + Seek), begin: u64) -> Result<Self, RootIoError> {
        reader.seek(SeekFrom::Start(begin))?;
        read_u32!(reader, nbytes);
        read_u16!(reader, version);
        read_u32!(reader, obj_len, datime);
        read_u16!(reader, key_len, cycle);
        read_u64_val!(
            // NOT written in the document.
            // Use the source. https://root.cern.ch/doc/master/TFile_8cxx_source.html
            version > VER_THRESHOLD_KEY || begin >= (1u64 << 31),
            reader,
            seek_key,
            seek_pdir
        );
        if begin != seek_key {
            return Err(RootIoError::InvalidFormatError);
        }
        let class_name = read_string(reader)?;
        let name = read_string(reader)?;
        let title = read_string(reader)?;
        let obj_begin = begin + key_len as u64;
        let meta_begin = reader.seek(SeekFrom::Current(0))?;
        let mut meta_data = vec![0; (obj_begin - meta_begin) as usize];
        reader.read(&mut meta_data)?;

        // TODO
        // parse compression header according to https://github.com/root-project/root/blob/master/js/scripts/JSRoot.io.js#L189
        Ok(Self {
            begin,
            meta_data,
            obj_begin,
            nbytes,
            version,
            obj_len,
            datime,
            key_len,
            cycle,
            seek_key,
            seek_pdir,
            class_name,
            name,
            title,
        })
    }

    pub(crate) fn next_position(&self) -> u64 {
        self.begin + self.nbytes as u64
    }

    pub(crate) fn read_raw_buffer(
        &self,
        reader: &mut (impl Read + Seek),
    ) -> Result<Vec<u8>, RootIoError> {
        reader.seek(SeekFrom::Start(self.obj_begin))?;
        let mut buf = vec![0; self.obj_len as usize];
        reader.read(&mut buf)?;
        Ok(buf)
    }

    pub(crate) fn detect_stream_kind(
        &self,
        reader: &mut (impl Read + Seek),
    ) -> Result<StreamKind, RootIoError> {
        reader.seek(SeekFrom::Start(self.obj_begin))?;

        if self.nbytes == self.obj_len + self.key_len as u32 {
            return Ok(StreamKind::Uncompressed);
        }
        let mut header = [0; HEADER_SIZE];
        reader.read(&mut header)?;

        match &header[..2] {
            b"ZL" => {
                if header[2] != 8 {
                    Err(RootIoError::InvalidFormatError)
                } else {
                    Ok(StreamKind::ZlibNew)
                }
            }
            b"CS" => {
                if header[2] != 8 {
                    Err(RootIoError::InvalidFormatError)
                } else {
                    Ok(StreamKind::ZlibOld)
                }
            }
            b"XZ" => {
                if header[2] != 0 {
                    Err(RootIoError::InvalidFormatError)
                } else {
                    Ok(StreamKind::Lzma)
                }
            }
            b"ZS" => {
                if header[2] != 0 {
                    Err(RootIoError::InvalidFormatError)
                } else {
                    Ok(StreamKind::Zstd)
                }
            }
            _ => Err(RootIoError::InvalidFormatError),
        }
    }

    pub(crate) fn decompress<'a>(
        &'a self,
        reader: &'a mut (impl Read + Seek),
    ) -> Result<Box<dyn Read + 'a>, RootIoError> {
        let kind = self.detect_stream_kind(reader)?;

        match kind {
            StreamKind::Uncompressed => {
                reader.seek(SeekFrom::Start(self.obj_begin))?;
                let result = reader.by_ref();
                Ok(Box::new(result.take(self.obj_len as u64)))
            }
            StreamKind::ZlibNew | StreamKind::ZlibOld => {
                let zlib_offset: u64 = if kind == StreamKind::ZlibNew { 2 } else { 0 };
                reader.seek(SeekFrom::Start(
                    self.obj_begin + HEADER_SIZE as u64 + zlib_offset,
                ))?;
                let result = reader.by_ref();

                let content = result.take(
                    self.nbytes as u64 - self.key_len as u64 - HEADER_SIZE as u64 - zlib_offset,
                );
                Ok(Box::new(flate2::read::DeflateDecoder::new(content)))
            }

            _ => Err(RootIoError::Unimplemented(format!(
                "Compression format {:?}",
                kind
            ))),
        }
    }
}
