use byteorder::{BigEndian, ReadBytesExt};
use std::io::{Read, Seek};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum RootIoError {
    #[error("Invalid file format")]
    InvalidFormatError,

    #[error("{0} is not implemented")]
    Unimplemented(String),

    #[error(transparent)]
    IOError(#[from] std::io::Error),
}

pub(crate) const VER_THRESHOLD: u32 = 1000000;
pub(crate) const VER_THRESHOLD_KEY: u16 = 1000;

pub(crate) fn read_as_u64(cond: bool, reader: &mut impl Read) -> Result<u64, RootIoError> {
    Ok(if cond {
        reader.read_u64::<BigEndian>()?
    } else {
        reader.read_u32::<BigEndian>()? as _
    })
}

pub(crate) fn read_string(reader: &mut impl Read) -> Result<String, RootIoError> {
    let len = reader.read_u8()?;
    let mut vec = vec![0u8; len as usize];
    reader.read(&mut vec)?;
    Ok(String::from_utf8_lossy(&vec).to_string())
}

macro_rules! read_u16 {
    ( $reader: expr, $( $x:ident ),* ) => {
        $(
            let $x = $reader.read_u16::<BigEndian>()?;
        )*
    };
}

macro_rules! read_u32 {
    ( $reader: expr, $( $x:ident ),* ) => {
        $(
            let $x = $reader.read_u32::<BigEndian>()?;
        )*
    };
}

macro_rules! read_u64_val {
    ( $cond: expr, $reader: expr, $( $x:ident ),* ) => {
        $(
            let $x = read_as_u64($cond, $reader)?;
        )*
    };
}

mod entry;
use entry::RootKey;

pub(crate) mod internal {
    pub(crate) use super::{read_as_u64, read_string};
}

#[derive(Debug)]
pub struct RootFile<T: Read + Seek> {
    reader: T,
    version: u32,
    begin: u64,
    end: u64,
    seek_free: u64,
    nbytes_free: u32,
    nfree: u32,
    nbytes_name: u32,
    units: u8,
    compress: u32,
    seek_info: u64,
    nbytes_info: u32,
    uuid: [u8; 18],
    keys: Vec<RootKey>,
}

impl<T: Read + Seek> RootFile<T> {
    pub fn new(reader: T) -> Result<Self, RootIoError> {
        let mut reader = reader;
        let mut header = [0u8; 4];

        reader.read(&mut header)?;
        if &header != b"root" {
            return Err(RootIoError::InvalidFormatError);
        }

        read_u32!(reader, version, begin);
        let begin = begin as u64;
        read_u64_val!(version >= VER_THRESHOLD, &mut reader, end, seek_free);
        read_u32!(reader, nbytes_free, nfree, nbytes_name);
        let units = reader.read_u8()?;
        read_u32!(reader, compress);
        read_u64_val!(version >= VER_THRESHOLD, &mut reader, seek_info);
        read_u32!(reader, nbytes_info);

        let mut uuid = [0u8; 18];
        reader.read(&mut uuid)?;

        let mut pointer = begin;
        let mut keys = vec![];

        let mut tot_len = 0;

        while pointer < end {
            let key = RootKey::new(&mut reader, pointer)?;
            pointer = key.next_position();
            if key.name == "Particle_size" {
                dbg!(keys.len());
                dbg!(&key);
                tot_len += key.obj_len;
            }
            keys.push(key);
        }

        dbg!(tot_len / 4);

        /*let mut v = vec![];
        keys[3]
            .decompress(&mut reader)
            .unwrap()
            .read_to_end(&mut v)
            .unwrap();
        dbg!(&keys[3]);
        dbg!(&v[..100]);*/
        keys = vec![];

        Ok(RootFile {
            reader,
            version,
            begin,
            end,
            seek_free,
            nbytes_free,
            nfree,
            nbytes_name,
            units,
            compress,
            seek_info,
            nbytes_info,
            uuid,
            keys,
        })
    }

    pub fn is_large_file(&self) -> bool {
        self.version >= VER_THRESHOLD
    }
}

#[cfg(test)]
mod tests {
    use super::RootFile;
    #[test]
    fn open_file() {
        let file = std::fs::File::open("delphes.root").unwrap();
        let root = RootFile::new(file);
        assert!(root.is_ok());
        let root = root.unwrap();
        dbg!(&root);
        assert!(root.is_large_file());
    }
}
