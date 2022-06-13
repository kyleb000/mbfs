use std::{collections::{HashMap, LinkedList}, thread::JoinHandle, sync::mpsc::{Sender, Receiver, self, RecvError}};

struct FileHandle {
    path: String,
    offset: usize,
    iostream: (Sender<Vec<u8>>, Receiver<Vec<u8>>),
    filesystem: Sender<ReadRequest>,
}

struct ReadRequest {
    path: String,
    offset: usize,
    amount: usize,
    dest: Sender<Vec<u8>>,
}

struct MessageBasedFileSystem {
    buffers: HashMap<String, Vec<u8>>,
    requests: LinkedList<ReadRequest>,
    readers: Vec<JoinHandle<()>>,
}

impl FileHandle {
    fn new(path: String, filesystem: Sender<ReadRequest>) -> Self {
        FileHandle { path, offset: 0, iostream: mpsc::channel(), filesystem }
    }

    fn read(&mut self) -> Result<Vec<u8>, RecvError> {
        self.filesystem.send(ReadRequest {
            path: self.path.clone(),
            offset: self.offset,
            amount: 4096,
            dest: self.iostream.0.clone()
        }).unwrap();
        let res = self.iostream.1.recv()?;
        self.offset += res.len();
        Ok(res)
    }
}