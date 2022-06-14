use std::{
    thread::{ JoinHandle, self },
    sync::{
        mpsc::{ Sender, Receiver, self, RecvError },
        atomic::{AtomicBool, Ordering},
        Arc
    },
    io::{ self, BufReader, BufRead },
    fs::File, time::Duration
};

const CAPACITY:usize = 4096;

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
    offset: usize,
    dest: Sender<io::Result<Vec<u8>>>,
}

/// This object is responsible for the actual handling of requests and
/// reading of files.
pub struct MessageBasedFileSystem {
    readers: Vec<(JoinHandle<()>, Sender<ReadRequest>)>,
    request_stream: (Sender<ReadRequest>, Receiver<ReadRequest>),
    run: Arc<AtomicBool>,
}

impl FileHandle {
    fn new(path: String, filesystem: Sender<ReadRequest>) -> Self {
        FileHandle { path, offset: 0, iostream: mpsc::channel(), filesystem }
    }

    /// Sends a read request to the file reader.
    /// # Returns
    /// - `Ok(Vec<u8>)` - A chunk of file data.
    /// - `Err(Error)` - Error when reading file.
    pub fn read(&mut self) -> io::Result<Vec<u8>> {
        self.filesystem.send(ReadRequest {
            path: self.path.clone(),
            offset: self.offset,
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
    pub fn new() -> Self {
        let mut readers = Vec::new();
        let to_run = Arc::new(AtomicBool::new(true));
        for _ in 0..10 {
            let run = to_run.clone();
            let (sender, receiver) = mpsc::channel();
            readers.push((thread::spawn(move || {
                while run.load(Ordering::Relaxed) {
                    let req: ReadRequest = receiver.recv().unwrap();
                    let to_send = match File::open(&req.path) {
                        Ok(file) => {
                            let mut reader = BufReader::with_capacity(CAPACITY, file);
                            if let Err(val) = reader.seek_relative(req.offset.try_into().unwrap()) {
                                Err(val)
                            } else {
                                let buffer = reader.fill_buf();
                                if buffer.is_err() {
                                    Err(buffer.err().unwrap())
                                } else {
                                    Ok(buffer.unwrap().to_vec())
                                }
                            }
                        },
                        Err(err) => Err(err)
                    };
                    let _ = req.dest.send(to_send).unwrap();
                }
            }), sender));
        }
        MessageBasedFileSystem { readers, request_stream: mpsc::channel(), run:to_run }
    }

    /// Returns a new file handle.
    /// # Returns
    /// - `FileHandle` - A new file handle object.
    pub fn open(&self, path: String) -> FileHandle {
        FileHandle::new(path, self.request_stream.0.clone())
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