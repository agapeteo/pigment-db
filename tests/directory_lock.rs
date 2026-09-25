//! Cross-process ownership of a store directory (specs/011).
//!
//! Each test re-executes this test binary as a child process that plays one role. The role is
//! chosen by environment variables set only on the child's command, and the role entry is an
//! ignored test, so it never runs on its own. A child reports through a file it renames into
//! place and exits with `CHILD_DONE`; any other exit code, or a missing report, is a harness
//! failure rather than an answer.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use pigment_db::key_map_store::DurableKeyMapStore;
use pigment_db::key_set_store::DurableKeySetStore;
use pigment_db::key_value_store::DurableKeyValueStore;
use pigment_db::model::SearchKey;
use pigment_db::{
    compact_directory_in_place, ClosedCompactionOptions, CompactionError, RecoveryError,
    RecoveryOperation,
};

const ROLE_ENV: &str = "PIGMENT_DIRECTORY_LOCK_ROLE";
const STORE_ENV: &str = "PIGMENT_DIRECTORY_LOCK_STORE";
const SIGNAL_ENV: &str = "PIGMENT_DIRECTORY_LOCK_SIGNAL";
const CHILD_ENTRY: &str = "child_entry";
const WATCHDOG: Duration = Duration::from_secs(60);

/// A role that ran to completion and wrote its report.
const CHILD_DONE: i32 = 86;
/// A holder whose release never came.
const HOLDER_TIMED_OUT: i32 = 99;

struct Fixture {
    base: tempfile::TempDir,
}

impl Fixture {
    fn new() -> Self {
        let base = tempfile::tempdir().expect("create fixture base");
        fs::create_dir_all(base.path().join("root").join("store")).expect("create store");
        fs::create_dir(base.path().join("signal")).expect("create signal directory");
        Self { base }
    }

    /// The directory whose whole contents a refused open must leave alone: the store and every
    /// sibling beside it.
    fn root(&self) -> PathBuf {
        self.base.path().join("root")
    }

    fn store(&self) -> PathBuf {
        self.root().join("store")
    }

    /// Outside `root`, so the handshake never shows in a snapshot.
    fn signal(&self) -> PathBuf {
        self.base.path().join("signal")
    }

    fn inner_lock(&self) -> PathBuf {
        fs::canonicalize(self.store())
            .expect("canonical store")
            .join(".pigment-lock")
    }

    fn snapshot(&self) -> Vec<(PathBuf, Option<Vec<u8>>)> {
        let mut entries = Vec::new();
        collect(&self.root(), &self.root(), &mut entries);
        entries.sort();
        entries
    }
}

fn collect(root: &Path, dir: &Path, out: &mut Vec<(PathBuf, Option<Vec<u8>>)>) {
    for entry in fs::read_dir(dir).expect("list fixture directory") {
        let path = entry.expect("read fixture entry").path();
        let relative = path.strip_prefix(root).expect("inside root").to_path_buf();
        if path.is_dir() {
            out.push((relative, None));
            collect(root, &path, out);
        } else if cfg!(windows)
            && path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.ends_with(".pigment-lock"))
        {
            // Windows refuses every read of a locked range, even through the holder's own second
            // handle, so a lock file is recorded by its presence there.
            out.push((relative, Some(b"<lock file>".to_vec())));
        } else {
            let bytes = fs::read(&path)
                .unwrap_or_else(|error| panic!("read fixture file {}: {error}", path.display()));
            out.push((relative, Some(bytes)));
        }
    }
}

struct Stores {
    kv: DurableKeyValueStore<fs::File>,
    set: DurableKeySetStore<fs::File>,
    map: DurableKeyMapStore<fs::File>,
}

fn open_all(store: &Path) -> Stores {
    let kv = DurableKeyValueStore::try_init_new(store)
        .expect("open key/value store")
        .into_parts()
        .0;
    let set = DurableKeySetStore::try_init_new(store)
        .expect("open key/set store")
        .into_parts()
        .0;
    let map = DurableKeyMapStore::try_init_new(store)
        .expect("open key/sorted-map store")
        .into_parts()
        .0;
    Stores { kv, set, map }
}

fn seed(stores: &Stores) {
    stores.kv.put(b"key".to_vec(), b"value".to_vec());
    stores.set.append(b"set".to_vec(), b"member".to_vec());
    stores
        .map
        .put(b"map".to_vec(), SearchKey::from("entry"), b"value".to_vec());
}

/// A child process that is killed and reaped if the test ends while it is still running.
struct ChildGuard(Option<Child>);

impl ChildGuard {
    fn id(&self) -> u32 {
        self.0.as_ref().expect("child present").id()
    }

    fn wait_for_exit(&mut self) -> i32 {
        let child = self.0.as_mut().expect("child present");
        let started = Instant::now();
        loop {
            if let Some(status) = child.try_wait().expect("poll child") {
                self.0 = None;
                return status.code().unwrap_or(-1);
            }
            if started.elapsed() >= WATCHDOG {
                panic!("child timed out");
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    fn kill_and_reap(&mut self) {
        let mut child = self.0.take().expect("child present");
        child.kill().expect("kill child");
        child.wait().expect("reap child");
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if let Some(mut child) = self.0.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

fn spawn_role(role: &str, fixture: &Fixture, store: &Path) -> ChildGuard {
    let log = fs::File::create(fixture.signal().join(format!("{role}.log"))).expect("child log");
    let child = Command::new(std::env::current_exe().expect("locate test executable"))
        .args([
            CHILD_ENTRY,
            "--exact",
            "--ignored",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(ROLE_ENV, role)
        .env(STORE_ENV, store)
        .env(SIGNAL_ENV, fixture.signal())
        .stdin(Stdio::null())
        .stdout(log.try_clone().expect("clone child log"))
        .stderr(log)
        .spawn()
        .expect("spawn role child");
    ChildGuard(Some(child))
}

/// Runs a role to completion and returns its report, one line per operation.
fn run_role(role: &str, fixture: &Fixture, store: &Path) -> Vec<String> {
    let _ = fs::remove_file(fixture.signal().join("report"));
    let mut child = spawn_role(role, fixture, store);
    let code = child.wait_for_exit();
    let log = fs::read_to_string(fixture.signal().join(format!("{role}.log"))).unwrap_or_default();
    assert_eq!(code, CHILD_DONE, "role {role} did not complete: {log}");
    fs::read_to_string(fixture.signal().join("report"))
        .unwrap_or_else(|error| panic!("role {role} wrote no report ({error}): {log}"))
        .lines()
        .map(str::to_owned)
        .collect()
}

/// Starts a child that opens every family and holds them until released or killed.
fn spawn_holder(fixture: &Fixture) -> ChildGuard {
    let mut child = spawn_role("hold", fixture, &fixture.store());
    let ready = fixture.signal().join("ready");
    let started = Instant::now();
    while !ready.exists() {
        if let Some(status) = child.0.as_mut().expect("holder").try_wait().expect("poll") {
            let log = fs::read_to_string(fixture.signal().join("hold.log")).unwrap_or_default();
            panic!("holder exited before it was ready ({status}): {log}");
        }
        assert!(started.elapsed() < WATCHDOG, "holder never became ready");
        std::thread::sleep(Duration::from_millis(10));
    }
    child
}

fn release(mut holder: ChildGuard, fixture: &Fixture) {
    fs::write(fixture.signal().join("release"), b"").expect("signal release");
    assert_eq!(
        holder.wait_for_exit(),
        CHILD_DONE,
        "holder did not release cleanly"
    );
}

/// One family's open, as a report line: `refused <operation> <message>`, `opened`, or `other ...`.
fn describe<T>(result: Result<T, RecoveryError>) -> String {
    match result {
        Ok(_) => "opened".to_owned(),
        Err(RecoveryError::Io {
            operation, source, ..
        }) if source.kind() == std::io::ErrorKind::WouldBlock => {
            format!("refused {operation:?} {source}")
        }
        Err(error) => format!("other {error:?}"),
    }
}

fn write_report(signal: &Path, lines: &[String]) {
    let staged = signal.join("report.staged");
    fs::write(&staged, lines.join("\n")).expect("write report");
    fs::rename(staged, signal.join("report")).expect("publish report");
}

#[test]
#[ignore = "a child-process role for this file's tests; run by its parent"]
fn child_entry() {
    let Ok(role) = std::env::var(ROLE_ENV) else {
        return;
    };
    let store = PathBuf::from(std::env::var_os(STORE_ENV).expect("role store directory"));
    let signal = PathBuf::from(std::env::var_os(SIGNAL_ENV).expect("role signal directory"));
    match role.as_str() {
        "open-each" => {
            let lines = vec![
                format!(
                    "kv {}",
                    describe(DurableKeyValueStore::try_init_new(&store))
                ),
                format!("set {}", describe(DurableKeySetStore::try_init_new(&store))),
                format!("map {}", describe(DurableKeyMapStore::try_init_new(&store))),
                format!(
                    "kv {}",
                    describe(DurableKeyValueStore::try_init_new(&store))
                ),
            ];
            write_report(&signal, &lines);
        }
        "init-new" => {
            let store_text = store.to_str().expect("utf-8 store path").to_owned();
            let outcome = std::panic::catch_unwind(|| DurableKeyValueStore::init_new(&store_text));
            let line = match outcome {
                Ok(_) => "opened".to_owned(),
                Err(payload) => {
                    let message = payload
                        .downcast_ref::<String>()
                        .cloned()
                        .or_else(|| {
                            payload
                                .downcast_ref::<&str>()
                                .map(|text| (*text).to_owned())
                        })
                        .unwrap_or_default();
                    format!("panicked {message}")
                }
            };
            write_report(&signal, &[line]);
        }
        "compact" => {
            let line = match compact_directory_in_place(&store, ClosedCompactionOptions::default())
            {
                Err(CompactionError::FailedClosed { detail }) => format!("refused {detail}"),
                Ok(_) => "compacted".to_owned(),
                Err(error) => format!("other {error:?}"),
            };
            write_report(&signal, &[line]);
        }
        "hold" => {
            let stores = open_all(&store);
            fs::write(signal.join("ready"), std::process::id().to_string()).expect("ready");
            let started = Instant::now();
            while !signal.join("release").exists() {
                if started.elapsed() >= WATCHDOG {
                    std::process::exit(HOLDER_TIMED_OUT);
                }
                std::thread::sleep(Duration::from_millis(10));
            }
            drop(stores);
        }
        other => panic!("unknown role {other}"),
    }
    std::process::exit(CHILD_DONE);
}

fn assert_refused_naming(line: &str, family: &str, lock: &Path) {
    let expected = format!("{family} refused {:?} ", RecoveryOperation::Inspect);
    assert!(
        line.starts_with(&expected),
        "{family} must be refused: {line}"
    );
    assert!(
        line.contains(&lock.display().to_string()),
        "{family}'s refusal must name {}: {line}",
        lock.display()
    );
}

/// The families the "open-each" role opens, in order: every family, then the first again.
const OPEN_EACH: [&str; 4] = ["kv", "set", "map", "kv"];

/// Pairs each line of an "open-each" report with the family it reports, requiring one line per
/// open: a child that reports fewer is a harness failure, not a pass.
fn open_each_lines(lines: &[String]) -> impl Iterator<Item = (&String, &'static str)> {
    assert_eq!(
        lines.len(),
        OPEN_EACH.len(),
        "one report line per open: {lines:?}"
    );
    lines.iter().zip(OPEN_EACH)
}

/// A1: while this process holds one family, another process's open of every family is refused,
/// before it reads or writes anything, naming the directory's lock file.
#[test]
fn another_process_cannot_open_a_directory_this_process_holds() {
    let fixture = Fixture::new();
    let seeded = open_all(&fixture.store());
    seed(&seeded);
    drop(seeded);
    let held = DurableKeyValueStore::try_init_new(fixture.store())
        .expect("hold one family")
        .into_parts()
        .0;
    let before = fixture.snapshot();

    let lines = run_role("open-each", &fixture, &fixture.store());

    let lock = fixture.inner_lock();
    for (line, family) in open_each_lines(&lines) {
        assert_refused_naming(line, family, &lock);
    }
    assert_eq!(
        fixture.snapshot(),
        before,
        "a refused open must change nothing"
    );
    // Read once released: Windows refuses reads of a locked range, and release keeps the file.
    drop(held);
    assert_eq!(
        fs::read_to_string(&lock).expect("lock file"),
        format!("{}\n", std::process::id()),
        "the lock file records its owner"
    );
}

/// A2: the refusal names the lock file and, where the owner's record can be read, the owning
/// process; once that owner exits, the directory opens.
#[test]
fn a_refusal_names_the_lock_file_and_the_owning_process() {
    let fixture = Fixture::new();
    let holder = spawn_holder(&fixture);
    let holder_id = holder.id();

    let refusal = match DurableKeyValueStore::try_init_new(fixture.store()) {
        Err(RecoveryError::Io { source, .. })
            if source.kind() == std::io::ErrorKind::WouldBlock =>
        {
            source.to_string()
        }
        Err(error) => panic!("expected a WouldBlock refusal, got {error:?}"),
        Ok(_) => panic!("a directory another process holds must not open"),
    };
    release(holder, &fixture);

    assert!(
        refusal.contains(&fixture.inner_lock().display().to_string()),
        "the refusal must name the lock file: {refusal}"
    );
    // Windows forbids reading a region another process has locked, so no owner is named there.
    #[cfg(unix)]
    assert!(
        refusal.contains(&format!("process {holder_id}")),
        "the refusal must name the owning process {holder_id}: {refusal}"
    );
    #[cfg(not(unix))]
    let _ = holder_id;
    open_all(&fixture.store());
}

/// A4: one process opens all three families of a directory together; once it has dropped them,
/// another process opens it, and the lock file stays for the next owner.
#[test]
fn one_process_opens_every_family_and_releases_the_directory_on_drop() {
    let fixture = Fixture::new();
    let stores = open_all(&fixture.store());
    seed(&stores);
    drop(stores);

    let lines = run_role("open-each", &fixture, &fixture.store());

    assert!(
        open_each_lines(&lines).all(|(line, family)| *line == format!("{family} opened")),
        "a released directory must open in another process: {lines:?}"
    );
    assert!(
        fixture.inner_lock().is_file(),
        "the lock file is never deleted on release"
    );
}

/// A5 (guard): an owner killed without running any destructor leaves no ownership behind.
#[test]
fn a_killed_owner_leaves_the_directory_openable() {
    let fixture = Fixture::new();
    let mut holder = spawn_holder(&fixture);
    holder.kill_and_reap();

    // Windows releases a terminated process's locks asynchronously, so it is allowed a moment.
    // Elsewhere the lock is gone once the owner is reaped, and the first open must succeed.
    let patience = if cfg!(windows) {
        Duration::from_secs(5)
    } else {
        Duration::ZERO
    };
    let started = Instant::now();
    let stores = loop {
        match DurableKeyValueStore::try_init_new(fixture.store()) {
            Ok(outcome) => break outcome.into_parts().0,
            Err(error) if started.elapsed() < patience => {
                let _ = error;
                std::thread::sleep(Duration::from_millis(50));
            }
            Err(error) => panic!("a killed owner left the directory owned: {error:?}"),
        }
    };
    stores.put(b"key".to_vec(), b"value".to_vec());
}

/// X1: ownership belongs to the process, not to the first open: dropping one family keeps the
/// directory owned while another family is open.
#[test]
fn the_directory_stays_owned_until_the_last_family_is_dropped() {
    let fixture = Fixture::new();
    let kv = DurableKeyValueStore::try_init_new(fixture.store())
        .expect("open key/value store")
        .into_parts()
        .0;
    let set = DurableKeySetStore::try_init_new(fixture.store())
        .expect("open key/set store")
        .into_parts()
        .0;
    drop(kv);

    let while_set_open = run_role("open-each", &fixture, &fixture.store());
    drop(set);
    let after_both = run_role("open-each", &fixture, &fixture.store());

    for (line, family) in open_each_lines(&while_set_open) {
        assert_refused_naming(line, family, &fixture.inner_lock());
    }
    assert!(
        open_each_lines(&after_both).all(|(line, family)| *line == format!("{family} opened")),
        "once every family is dropped the directory must open: {after_both:?}"
    );
}

/// X2: a symlink alias reaches the same directory, so it reaches the same lock.
#[cfg(unix)]
#[test]
fn an_alias_of_a_held_directory_is_refused() {
    let fixture = Fixture::new();
    let alias = fixture.base.path().join("alias");
    std::os::unix::fs::symlink(fixture.store(), &alias).expect("create alias");
    let held = DurableKeyValueStore::try_init_new(fixture.store())
        .expect("hold the directory")
        .into_parts()
        .0;

    let lines = run_role("open-each", &fixture, &alias);

    for (line, family) in open_each_lines(&lines) {
        assert_refused_naming(line, family, &fixture.inner_lock());
    }
    drop(held);
}

/// X3 (guard): the lock file's contents never decide ownership: a foreign process id or bytes
/// that do not parse, with no one holding the lock, do not refuse an open.
#[test]
fn a_lock_file_nobody_holds_opens_whatever_it_records() {
    for contents in [&b"1\n"[..], b"not a process id \xff\n"] {
        let fixture = Fixture::new();
        fs::write(fixture.store().join(".pigment-lock"), contents).expect("prewrite lock file");

        let stores = open_all(&fixture.store());
        seed(&stores);
    }
}

/// `init_new`, the historical panic-on-error API, panics with the refusal naming the lock file.
#[test]
fn init_new_panics_naming_the_lock_file() {
    let fixture = Fixture::new();
    let held = DurableKeyValueStore::try_init_new(fixture.store())
        .expect("hold the directory")
        .into_parts()
        .0;

    let lines = run_role("init-new", &fixture, &fixture.store());

    assert_eq!(lines.len(), 1, "{lines:?}");
    assert!(
        lines[0].starts_with("panicked "),
        "init_new must panic: {lines:?}"
    );
    assert!(
        lines[0].contains(&fixture.inner_lock().display().to_string()),
        "the panic must name the lock file: {lines:?}"
    );
    drop(held);
}

/// Makes a directory read-only for the duration of a test, and restores it on drop. Fails the
/// test where permissions are not enforced for this user (for example root), because every
/// scenario that needs it would otherwise pass without reaching what it tests.
#[cfg(unix)]
struct ReadOnlyDirectory(PathBuf);

#[cfg(unix)]
impl ReadOnlyDirectory {
    fn new(path: &Path) -> Self {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o555)).expect("make read-only");
        let guard = Self(path.to_path_buf());
        let probe = path.join("probe");
        if fs::write(&probe, b"").is_ok() {
            let _ = fs::remove_file(probe);
            panic!(
                "permissions are not enforced for this user, so this scenario cannot be staged; \
                 run the suite as an unprivileged user"
            );
        }
        guard
    }
}

#[cfg(unix)]
impl Drop for ReadOnlyDirectory {
    fn drop(&mut self) {
        use std::os::unix::fs::PermissionsExt;
        let _ = fs::set_permissions(&self.0, fs::Permissions::from_mode(0o755));
    }
}

/// A7: a store directory this process cannot write, holding no lock file, is refused naming the
/// lock file it would need, and nothing is created.
#[cfg(unix)]
#[test]
fn a_store_that_cannot_hold_its_lock_file_is_refused_naming_it() {
    let fixture = Fixture::new();
    let stores = open_all(&fixture.store());
    seed(&stores);
    drop(stores);
    let _ = fs::remove_file(fixture.store().join(".pigment-lock"));
    let before = fixture.snapshot();
    let read_only = ReadOnlyDirectory::new(&fixture.store());

    let error = match DurableKeyValueStore::try_init_new(fixture.store()) {
        Ok(_) => panic!("a store that cannot hold its lock file must be refused"),
        Err(error) => error,
    };

    drop(read_only);
    match &error {
        RecoveryError::Io {
            operation: RecoveryOperation::Inspect,
            source,
            ..
        } => {
            assert_eq!(
                source.kind(),
                std::io::ErrorKind::PermissionDenied,
                "{error}"
            );
            assert!(
                source.to_string().contains(".pigment-lock"),
                "the refusal must name the lock file: {error}"
            );
        }
        other => panic!("expected an Inspect refusal, got {other:?}"),
    }
    assert_eq!(fixture.snapshot(), before, "nothing may be created");
}

/// A8: a child process spawned by another thread keeps a copy of every open descriptor, the
/// lock's included, until it execs. A directory dropped and reopened in that window must still
/// open: closing the lock file alone would leave the lock held by the child.
#[cfg(unix)]
#[test]
fn a_reopen_is_not_refused_while_another_thread_spawns_children() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    let fixture = Fixture::new();
    let stop = Arc::new(AtomicBool::new(false));
    let spawner = {
        let stop = stop.clone();
        std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                let _ = Command::new("true").status();
            }
        })
    };
    let mut refused = Vec::new();
    for _ in 0..400 {
        match DurableKeyValueStore::try_init_new(fixture.store()) {
            Ok(outcome) => drop(outcome),
            Err(RecoveryError::Io { source, .. })
                if source.kind() == std::io::ErrorKind::WouldBlock =>
            {
                refused.push(source.to_string())
            }
            Err(error) => panic!("unexpected open failure: {error:?}"),
        }
    }
    stop.store(true, Ordering::Relaxed);
    spawner.join().expect("join spawner");

    assert!(
        refused.is_empty(),
        "{} of 400 reopens were refused by this process's own children, e.g. {:?}",
        refused.len(),
        refused.first()
    );
}

/// A3: closed maintenance run by another process is refused while this process holds the
/// directory, naming the lock file, and changes nothing.
#[test]
fn another_process_cannot_compact_a_directory_this_process_holds() {
    let fixture = Fixture::new();
    let stores = open_all(&fixture.store());
    seed(&stores);
    let before = fixture.snapshot();

    let lines = run_role("compact", &fixture, &fixture.store());

    assert_eq!(lines.len(), 1, "{lines:?}");
    assert!(
        lines[0].starts_with("refused "),
        "closed compaction must be refused: {lines:?}"
    );
    assert!(
        lines[0].contains(".pigment-lock"),
        "the refusal must name a lock file: {lines:?}"
    );
    assert_eq!(
        fixture.snapshot(),
        before,
        "a refused compaction must change nothing"
    );
    drop(stores);
}

/// A6: a store directory this process cannot write, holding a read-only `.pigment-lock` created
/// in advance, opens: the lock is taken through a read-only descriptor and records nothing, and
/// it still excludes another process.
#[cfg(unix)]
#[test]
fn a_read_only_lock_file_created_in_advance_serves_a_read_only_store_directory() {
    use std::os::unix::fs::PermissionsExt;

    let fixture = Fixture::new();
    let stores = open_all(&fixture.store());
    seed(&stores);
    drop(stores);
    let lock = fixture.store().join(".pigment-lock");
    fs::write(&lock, b"").expect("reset lock file");
    fs::set_permissions(&lock, fs::Permissions::from_mode(0o444)).expect("read-only lock file");
    let read_only = ReadOnlyDirectory::new(&fixture.store());

    let stores = open_all(&fixture.store());
    let recorded = fs::read(&lock).expect("read lock file");
    let lines = run_role("open-each", &fixture, &fixture.store());

    drop(stores);
    drop(read_only);
    assert!(
        recorded.is_empty(),
        "a read-only lock records nothing: {recorded:?}"
    );
    for (line, family) in open_each_lines(&lines) {
        assert_refused_naming(line, family, &fixture.inner_lock());
    }
}

/// A lock path that is not a regular file is refused rather than followed: a symlink's target
/// would otherwise be locked in its place and overwritten with the owner record.
#[cfg(unix)]
#[test]
fn a_lock_path_that_is_a_symlink_is_refused_and_its_target_untouched() {
    let fixture = Fixture::new();
    let victim = fixture.base.path().join("victim.txt");
    fs::write(&victim, b"keep me").expect("create victim");
    std::os::unix::fs::symlink(&victim, fixture.store().join(".pigment-lock"))
        .expect("symlink lock path");

    let result = DurableKeyValueStore::try_init_new(fixture.store());

    assert_eq!(fs::read(&victim).expect("read victim"), b"keep me");
    match result {
        Err(RecoveryError::Io {
            operation: RecoveryOperation::Inspect,
            source,
            ..
        }) => assert!(
            source.to_string().contains("not a regular file"),
            "the refusal must say why: {source}"
        ),
        Err(error) => panic!("expected an Inspect refusal, got {error:?}"),
        Ok(_) => panic!("a symlinked lock path must be refused"),
    }
}
