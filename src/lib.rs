use std::{
    thread::{ JoinHandle, self },
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc
    },
    io::{ self, BufReader, BufRead },
    fs::File, time::Duration, collections::LinkedList
};

use crossbeam_channel::{Sender, Receiver, RecvError};
use chashmap::CHashMap;


/// Handle to a file. Keeps track of the offset of the file.
/// When a read() request is made, this object will send a request to the
/// main file reader, and will receive a response from the reader.
pub struct FileHandle {
    path: String,
    offset: usize,
    iostream: (Sender<io::Result<Vec<u8>>>, Receiver<io::Result<Vec<u8>>>),
    filesystem: Sender<ReadRequest>,
}

/// Read request for file data, along with the desintation to send the data to.
pub struct ReadRequest {
    path: String,
    dest: Sender<io::Result<Vec<u8>>>,
}

/// Contains the next set of file chunks that are cached in memory, so
/// when the next read request is made, the read operation will happen
/// quickly.
pub struct FilePageEntry {
    pages: LinkedList<Vec<u8>>,
    page_size: usize,
    max_pages: usize,
}

/// This object is responsible for the actual handling of requests and
/// reading of files.
pub struct MessageBasedFileSystem {
    readers: Vec<(JoinHandle<()>, Sender<ReadRequest>)>,
    request_stream: (Sender<ReadRequest>, Receiver<ReadRequest>),
    run: Arc<AtomicBool>,
    max_messages: usize,
}

impl FilePageEntry {
    pub fn new(path: String, page_size: usize, max_pages: usize) -> io::Result<Self> {
        let mut file_pages = LinkedList::new();
        match Self::populate_pages(path, &mut file_pages, max_pages, page_size) {
            Ok(_) => Ok(FilePageEntry{pages: file_pages, page_size, max_pages}),
            Err(err) => Err(err)
        }
    }

    pub fn read(&mut self, path: String) -> io::Result<Option<Vec<u8>>> {
        if self.pages.is_empty() {
            // Attempt to repopulate the pages.
            let res = Self::populate_pages(path, &mut self.pages, self.max_pages, self.page_size);
            match res {
                Ok(_) => {
                    if self.pages.is_empty() {
                        Ok(None)
                    } else {
                        Ok(self.pages.pop_front())
                    }
                },
                Err(err) => Err(err)                
            }
        } else {
            Ok(self.pages.pop_front())
        }
    }

    /// Attempts to populate a list of pages by reading file data into pages.
    /// If successful, an empty Ok() variant will be returned, otherwise an Error.
    fn populate_pages(path: String, file_pages: &mut LinkedList<Vec<u8>>, max_pages: usize, page_size: usize) -> io::Result<()> {
        match File::open(path) {
            Ok(file) => {
                let mut reader = BufReader::with_capacity(page_size, file);
                let mut i = 0;
                while i < max_pages {
                    let buffer = reader.fill_buf()?;
                    let length = buffer.len();
                    if length == 0 {
                        break;
                    }
                    file_pages.push_back(buffer.to_vec());
                    reader.consume(length);
                    i += 1;
                }
                Ok(())
            },
            Err(err) => Err(err)
        }
    }
}

impl FileHandle {
    fn new(path: String, filesystem: Sender<ReadRequest>, max_messages: usize) -> Self {
        FileHandle { path, offset: 0, iostream: crossbeam_channel::bounded(max_messages), filesystem }
    }

    /// Sends a read request to the file reader.
    /// # Returns
    /// - `Ok(Vec<u8>)` - A chunk of file data.
    /// - `Err(Error)` - Error when reading file.
    pub fn read(&mut self) -> io::Result<Vec<u8>> {
        self.filesystem.send(ReadRequest {
            path: self.path.clone(),
            dest: self.iostream.0.clone()
        }).unwrap();
        let res = self.iostream.1.recv().unwrap()?;
        self.offset += res.len();
        Ok(res)
    }

    /// Get the name of the file.
    /// # Returns
    /// - `String` - The name of the file.
    pub fn get_name(&self) -> String {
        self.path.clone()
    }
}

impl MessageBasedFileSystem {

    /// Instantiates a new MessageBasedFileSystem.
    pub fn new(max_pages: usize, page_size: usize, max_messages: usize) -> Self {
        let mut readers = Vec::new();
        let to_run = Arc::new(AtomicBool::new(true));
        let pages = Arc::new(CHashMap::new());
        for _ in 0..10 {
            let run = to_run.clone();
            let pages_ref = pages.clone();
            let (sender, receiver) = crossbeam_channel::unbounded();
            readers.push((thread::spawn(move || {
                while run.load(Ordering::Relaxed) {
                    let req: ReadRequest = receiver.recv().unwrap();
                    if !pages_ref.contains_key(&req.path) {
                        match FilePageEntry::new(req.path.clone(), page_size, max_pages) {
                            Ok(val) => pages_ref.insert_new(req.path.clone(), val),
                            Err(val) => panic!("{}", val)
                        }                        
                    }

                    let to_send = match pages_ref.get_mut(&req.path).unwrap().read(req.path.clone()) {
                        Ok(val) => {
                            match val {
                                Some(v) => Ok(v),
                                None => Ok(Vec::new())
                            }
                        },
                        Err(val) => Err(val)
                    };
                    let _ = req.dest.send(to_send).unwrap();
                }
            }), sender));
        }
        MessageBasedFileSystem { readers, request_stream: crossbeam_channel::unbounded(), run:to_run, max_messages }
    }

    /// Returns a new file handle.
    /// # Returns
    /// - `FileHandle` - A new file handle object.
    pub fn open(&self, path: String) -> FileHandle {
        FileHandle::new(path, self.request_stream.0.clone(), self.max_messages)
    }

    /// Provides a signal to use to stop the system.
    /// # Returns
    /// - `Arc<AtomicBool>` - Stop signal.
    pub fn get_run_signal(&self) -> Arc<AtomicBool> {
        self.run.clone()
    }

    /// Runs the main file reader.
    pub fn run(&self) -> Result<(), RecvError> {
        for reader in self.readers.iter() {
            let req_result = self.request_stream.1.recv_timeout(Duration::from_millis(1));
            if let Ok(request) = req_result {
                reader.1.send(request).unwrap();
            }
        }
        Ok(())
    }
}