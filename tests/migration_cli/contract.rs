//! Successful migration executable contracts.

#[test]
fn executable_help_smoke_contract() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_pigment-db-migrate"))
        .arg("--help")
        .output()
        .expect("migration executable must launch");

    assert!(output.status.success());
    assert!(String::from_utf8(output.stdout)
        .unwrap()
        .starts_with("Usage: pigment-db-migrate"));
    assert!(output.stderr.is_empty());
}
