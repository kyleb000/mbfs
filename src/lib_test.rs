#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::super::*;
    #[test]
    fn create_file_entry_success() {
        let entry = FilePageEntry::new("test_data/test1.txt".to_owned(), 1, 5);
        assert!(entry.is_ok());
    }

    #[test]
    fn create_file_entry_failure() {
        let entry = FilePageEntry::new("test_data/none.txt".to_owned(), 1, 5);
        assert!(entry.is_err());
    }

    #[test]
    fn read_from_file_entry_success() {
        let mut entry = FilePageEntry::new("test_data/test2.txt".to_owned(), 1, 3).unwrap();

        let mut i: io::Result<Option<Vec<u8>>>; 

        for item in ['1', '2', '3'].into_iter() {
            i = entry.read("test_data/test2.txt".to_owned());
            assert!(i.is_ok());

            let j = i.unwrap();
            assert_eq!(*(j.unwrap().get(0).unwrap()), item.try_into().unwrap());
        }

        i = entry.read("test_data/test2.txt".to_owned());
        assert!(i.is_ok());
        assert!(i.unwrap().is_none());
    }

    #[test]
    fn read_file_repopulate_entry() {
        let mut entry = FilePageEntry::new("test_data/test3.txt".to_owned(), 1, 3).unwrap();

        let mut i: io::Result<Option<Vec<u8>>>; 

        for item in ['1', '2', '3', '4', '5', '6'].into_iter() {
            i = entry.read("test_data/test3.txt".to_owned());
            assert!(i.is_ok());

            let j = i.unwrap();
            assert_eq!(*(j.unwrap().get(0).unwrap()), item.try_into().unwrap());
        }

        i = entry.read("test_data/test3.txt".to_owned());
        assert!(i.is_ok());
        assert!(i.unwrap().is_none());
    }

    #[test]
    fn create_file_handle() {
        let (tx, _) = crossbeam_channel::unbounded();
        let handle = FileHandle::new("test_data/test1.txt".to_owned(), tx, 2);

        assert_eq!(handle.get_name(), "test_data/test1.txt".to_owned())
    }

    #[test]
    fn file_handle_read() {
        let (tx, rx) = crossbeam_channel::unbounded();
        let mut handle = FileHandle::new("test_data/test1.txt".to_owned(), tx, 2);

        let _ = handle.iostream.0.send(Ok([1_u8, 2_u8].to_vec()));

        let read_res = handle.read();
        assert!(read_res.is_ok());

        let recv = rx.recv().unwrap();
        assert_eq!(recv.path, "test_data/test1.txt".to_owned());
    }

    #[test]
    fn create_mbfs() {
        let file_reader = Arc::new(Mutex::new(MessageBasedFileSystem::new(3,1,2)));
        let fr2 = file_reader.clone();
        let mut handle = file_reader.lock().unwrap().open("test_data/test2.txt".to_owned());

        let run_sig = file_reader.lock().unwrap().get_run_signal();

        let read_thr = thread::spawn(move || {
            loop {
                let sig = fr2.lock().unwrap().get_run_signal();
                if !sig.load(Ordering::Relaxed) {
                    break;
                }
                fr2.lock().unwrap().run().unwrap();
                thread::sleep(Duration::from_millis(200));
            }
        });
        let data = handle.read().unwrap();
        assert_eq!(*(data.get(0).unwrap()), '1'.try_into().unwrap());
        run_sig.store(false, Ordering::Relaxed);
        read_thr.join().unwrap();
    }
}