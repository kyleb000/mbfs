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

#[cfg(test)]
#[path = "lib_test.rs"]
mod lib_test;

/// Record for a file somewhere on the file system,
/// along with channels for reading file data and sending
/// read requests to the MBFS kernel.
pub struct FileHandle {
    path: String,
    iostream: (Sender<io::Result<Vec<u8>>>, Receiver<io::Result<Vec<u8>>>),
    filesystem: Sender<ReadRequest>,
}

/// Read request for file data, along with the desintation to send the data to.
pub struct ReadRequest {
    path: String,
    dest: Sender<io::Result<Vec<u8>>>,
}

/// Contains a list of file pages.
/// 
/// A page is a chunk of data containing data from the file. Each
/// page correspons to a slice of data from a file.
struct FilePageEntry {
    pages: LinkedList<Vec<u8>>,
    page_size: usize,
    max_pages: usize,
    current_page: usize,
}

/// This is the primary struct for running a message based file system.
/// Upon construction, this struct will create a set of workers, each
/// listening for new read requests.
/// 
/// When a worker receives a read request, it will first check to see if
/// there are any existing pages for the file. If no pages exists, then a
/// new entry is made. Otherwise, the head of the list will be returned.
/// 
/// Finally, this struct also contains a flag wich can be used to stop all workers.
/// Usually this is done when the program must terminate.
pub struct MessageBasedFileSystem {
    readers: Vec<(JoinHandle<()>, Sender<ReadRequest>)>,
    request_stream: (Sender<ReadRequest>, Receiver<ReadRequest>),
    run: Arc<AtomicBool>,
    max_messages: usize,
}

impl FilePageEntry {
    /// Instantiate a new FilePageEntry struct.
    /// # Parameters
    /// - `path`: The file path to read from.
    /// - `page_size`: The size of each page.
    /// - `max_pages`: The maximum number of pages each entry may hold.
    /// 
    /// # Returns
    /// - `Ok(FilePageEntry)`: If a file is successfully opened and read from, the struct is returned.
    /// - `Err`: Reasons as to why constructing this struct failed.
    /// 
    /// # Examples
    /// 
    /// ```
    /// let path = "/test/file.txt".to_owned();
    /// let entry = mbfs::FilePageEntry::new(path, 4096, 16);
    /// match entry {
    ///     Ok(_) => println!("Entry created!"),
    ///     Err(reason) => panic!("Failed to create entry: {}", reason)
    /// }
    /// ```
    fn new(path: String, page_size: usize, max_pages: usize) -> io::Result<Self> {
        let mut file_pages = LinkedList::new();
        match Self::populate_pages(path, &mut file_pages, max_pages, page_size, 0) {
            Ok(_) => Ok(FilePageEntry{pages: file_pages, page_size, max_pages, current_page: 0}),
            Err(err) => Err(err)
        }
    }

    /// Fetch the next page. If there are no more pages, open the file, move
    /// by total_pages and read in the next set of pages before returning the first one.
    /// 
    /// # Parameters
    /// - `path`: The file to read from if new pages are needed.
    /// 
    /// # Returns
    /// - `Ok(Option)`: Potential file data. If the result is `Some`, there is file data. If `None`, we've reached EOF.
    /// - `Err(reason)`: Reason as to why the operation failed.
    /// 
    /// # Examples
    /// 
    /// ```
    /// let res = entry.unwrap().read(path);
    /// match res {
    ///     Ok(option) => {
    ///         if option.is_some() {
    ///             println!("File data read");
    ///         } else {
    ///             println!("EOF reached");
    ///         }
    ///     },
    ///     Err(reason) => panic!("Failed read data: {}", reason)
    /// }
    /// ```
    fn read(&mut self, path: String) -> io::Result<Option<Vec<u8>>> {
        if self.pages.is_empty() {
            // Attempt to repopulate the pages.
            Self::populate_pages(path, &mut self.pages, self.max_pages, self.page_size, self.current_page)?;
        }
        if self.pages.is_empty() {
            Ok(None)
        } else {
            self.current_page += 1;
            Ok(self.pages.pop_front())
        }
    }

    /// Attempts to populate a list of pages by reading file data into pages.
    /// If successful, an empty Ok() variant will be returned, otherwise an Error.
    /// 
    /// # Parameters:
    /// - `path`: Path to the file to read.
    /// - `file_pages`: The list to populate.
    /// - `max_pages`: The maximum number of pages that can be populated from the file.
    /// - `page_size`: The size of each page.
    /// - `current_page`: Offset from where the pages should be populated.
    /// 
    /// # Returns:
    /// `io::Result<Vec<u8>>`
    /// 
    /// # Examples:
    /// 
    /// ```
    /// let mut file_pages = LinkedList::new();
    /// populate_pages("/test/file.txt".to_owned(), &mut file_pages, 5, 4096, 0);
    /// ```
    fn populate_pages(path: String,
        file_pages: &mut LinkedList<Vec<u8>>,
        max_pages: usize,
        page_size: usize,
        current_page: usize) -> io::Result<()> 
    {
        let file = File::open(path)?;
        let mut reader = BufReader::with_capacity(page_size, file);
        reader.seek_relative((current_page * page_size).try_into().unwrap())?;
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
    }
}

impl FileHandle {
    /// Creates a new file handle.
    /// 
    /// # Parameters
    /// - `path`: The file path.
    /// - `filesystem`: Channel with which to send read requests.
    /// - `max_messages`: Limit to how many read requests can be made.
    fn new(path: String, filesystem: Sender<ReadRequest>, max_messages: usize) -> Self {
        FileHandle { path, iostream: crossbeam_channel::bounded(max_messages), filesystem }
    }

    /// Sends a read request to the file reader.
    /// 
    /// # Returns
    /// - `Ok(Vec<u8>)` - A chunk of file data.
    /// - `Err(Error)` - Error when reading file.
    /// 
    /// # Examples
    /// ```
    /// let file_reader = Arc::new(Mutex::new(MessageBasedFileSystem::new()));
    /// /* Spawn thread to run mbfs */
    /// let file = file_reader.lock().unwrap().open("/test/file.txt".to_owned());
    /// let vec = file.read()?;
    /// ```
    pub fn read(&mut self) -> io::Result<Vec<u8>> {
        self.filesystem.send(ReadRequest {
            path: self.path.clone(),
            dest: self.iostream.0.clone()
        }).unwrap();
        let res = self.iostream.1.recv().unwrap()?;
        Ok(res)
    }

    /// Get the name of the file.
    /// 
    /// # Returns
    /// - `String` - The name of the file.
    pub fn get_name(&self) -> String {
        self.path.clone()
    }
}

impl MessageBasedFileSystem {

    /// Instantiates a new MessageBasedFileSystem.
    /// 
    /// # Parameters:
    /// - `max_pages` - The maximum number of pages each file reference may hold.
    /// - `page_size`: The size of each page.
    /// - `max_messages`: The maximum capacity of the request queue. Additional requests will be
    /// blocked until there is space in the queue.
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
                    let req_result = receiver.recv();
                    if req_result.is_err() {
                        continue;
                    }
                    let req: ReadRequest = req_result.unwrap();
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
    /// 
    /// # Parameters
    /// - `path`: The path to the file.
    /// 
    /// # Returns
    /// - `FileHandle` - A new file handle object.
    /// 
    /// # Examples
    /// ```
    /// let file_reader = Arc::new(Mutex::new(MessageBasedFileSystem::new()));
    /// /* Spawn thread to run mbfs */
    /// let file = file_reader.lock().unwrap().open("/test/file.txt".to_owned());
    /// ```
    pub fn open(&self, path: String) -> FileHandle {
        FileHandle::new(path, self.request_stream.0.clone(), self.max_messages)
    }

    /// Provides a signal to use to stop the system.
    /// # Returns
    /// - `Arc<AtomicBool>` - Stop signal.
    pub fn get_run_signal(&self) -> Arc<AtomicBool> {
        self.run.clone()
    }

    /// Runs the message based file system.
    /// 
    /// # Examples
    /// ```
    /// let file_reader = Arc::new(Mutex::new(MessageBasedFileSystem::new()));
    /// let fr2 = file_reader.clone();
    /// let read_thr = thread::spawn(move || {
    ///     loop {
    ///         {
    ///             fr2.lock().unwrap().run().unwrap();
    ///         }
    ///         thread::sleep(Duration::from_millis(1));
    ///     }
    /// });
    /// // Do work with `file_reader`
    /// read_thr.join()?;
    /// ```
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