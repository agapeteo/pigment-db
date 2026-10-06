//! Process SIGKILL after Physical ACK; this is not machine power-loss proof.
use pigment_db::key_value_store::{
    ConditionalAction as Action, ConditionalResult, DurableKeyValueStore,
};
use pigment_db::{DurabilityPolicy, DurableStoreOptions};
use std::io::Write;

#[test]
#[ignore = "subprocess entrypoint, invoked by physical_ack_survives_process_sigkill"]
fn physical_child() {
    let directory =
        std::env::var_os("CONDITIONAL_KEY_CRASH_DIRECTORY").expect("test-owned directory");
    let options = DurableStoreOptions::default().with_durability_policy(DurabilityPolicy::Physical);
    let store = DurableKeyValueStore::try_init_new_with_options(directory, options)
        .unwrap()
        .into_store();
    assert_eq!(
        store
            .try_compare_exchange_one(b"key".to_vec(), None, Action::Put(b"first".to_vec()))
            .unwrap(),
        ConditionalResult::Applied
    );
    assert_eq!(
        store
            .try_compare_exchange_one(
                b"key".to_vec(),
                Some(b"first"),
                Action::Put(b"last".to_vec())
            )
            .unwrap(),
        ConditionalResult::Applied
    );
    assert_eq!(
        store
            .try_compare_exchange_one(b"gone".to_vec(), None, Action::Put(vec![]))
            .unwrap(),
        ConditionalResult::Applied
    );
    assert_eq!(
        store
            .try_compare_exchange_one(b"gone".to_vec(), Some(b""), Action::Delete)
            .unwrap(),
        ConditionalResult::Applied
    );
    println!("CONDITIONAL_PHYSICAL_ACK");
    std::io::stdout().flush().unwrap();
    // Keep the actual store open until the parent terminates this exact child.
    loop {
        std::thread::park();
    }
}

#[cfg(unix)]
#[test]
fn physical_ack_survives_process_sigkill() {
    use std::io::BufRead;
    use std::os::unix::process::ExitStatusExt;
    use std::process::{Child, Command, Stdio};
    use std::sync::mpsc;
    use std::time::Duration;
    struct OwnedChild(Child);
    impl Drop for OwnedChild {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let directory = tempfile::tempdir().unwrap();
    let mut child = OwnedChild(
        Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "physical_child", "--ignored", "--nocapture"])
            .env("CONDITIONAL_KEY_CRASH_DIRECTORY", directory.path())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap(),
    );
    let stdout = child.0.stdout.take().unwrap();
    let (tx, rx) = mpsc::channel();
    let reader = std::thread::spawn(move || {
        for line in std::io::BufReader::new(stdout).lines() {
            if line.unwrap() == "CONDITIONAL_PHYSICAL_ACK" {
                tx.send(()).unwrap();
                return;
            }
        }
    });
    rx.recv_timeout(Duration::from_secs(30))
        .expect("real child reaches Physical ACK");
    child.0.kill().unwrap();
    let status = child.0.wait().unwrap();
    assert_eq!(status.signal(), Some(9));
    reader.join().unwrap();
    let options = DurableStoreOptions::default().with_durability_policy(DurabilityPolicy::Physical);
    let store = DurableKeyValueStore::try_init_new_with_options(directory.path(), options)
        .unwrap()
        .into_store();
    assert_eq!(store.get(b"key"), Some(b"last".to_vec()));
    assert_eq!(store.get(b"gone"), None);
    assert_eq!(store.size(), 1);
}
