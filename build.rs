use std::process::Command;

fn main() {
    println!("cargo::rustc-check-cfg=cfg(rhel8)");
    let output = Command::new("cat")
        .arg("/etc/os-release")
        .output()
        .expect("Failed to execute command");

    let os_info = String::from_utf8(output.stdout).unwrap();

    if os_info.contains("platform:el8") {
        println!("cargo::rustc-cfg=rhel8");
    }
}
