use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};

use crate::node::Node;

const PAGE_SIZE: usize = 4096;
const HEADER_SIZE: usize = PAGE_SIZE;
pub(crate) const STARTUP_OFFSET: usize = HEADER_SIZE + 20;

pub(crate) type Offset = usize;

pub(crate) trait PageOperator {
    fn next_offset(&self) -> usize;
    fn read(&mut self, offset: usize) -> anyhow::Result<Node>;
    fn write(&mut self, node: &Node) -> anyhow::Result<usize>;
    fn write_at(&mut self, node: &Node, offset: usize) -> anyhow::Result<()>;
}

pub(crate) struct Pager {
    file: File,
    cursor: usize,
    bincode_config: bincode::config::Configuration,
}

impl Pager {
    pub(crate) fn new(file: File, startup_offset: usize) -> Self {
        Self {
            file,
            cursor: startup_offset,
            bincode_config: bincode::config::standard(),
        }
    }
}

impl PageOperator for Pager {
    fn next_offset(&self) -> usize {
        self.cursor
    }

    fn read(&mut self, offset: usize) -> anyhow::Result<Node> {
        self.file.seek(SeekFrom::Start(offset as u64))?;
        let mut buffer: [u8; PAGE_SIZE] = [0x00; PAGE_SIZE];
        let _ = self.file.read(&mut buffer[..])?;
        let (node, _) = bincode::decode_from_slice(&buffer, self.bincode_config)?;
        Ok(node)
    }

    fn write(&mut self, node: &Node) -> anyhow::Result<usize> {
        let offset = self.file.seek(SeekFrom::Start((self.cursor) as u64))?;
        let data: Vec<u8> = bincode::encode_to_vec(node, self.bincode_config)?;
        self.file.write_all(data.as_slice())?;
        self.cursor += PAGE_SIZE;
        Ok(offset as usize)
    }

    fn write_at(&mut self, node: &Node, offset: usize) -> anyhow::Result<()> {
        let _ = self.file.seek(SeekFrom::Start(offset as u64))?;
        let data: Vec<u8> = bincode::encode_to_vec(node, self.bincode_config)?;
        self.file.write_all(data.as_slice())?;
        Ok(())
    }
}