use std::process::Command;

fn main() {
    let output = Command::new("cat")
        .arg("/etc/os-release")
        .output()
        .expect("Failed to execute command");

    let os_info = String::from_utf8(output.stdout).unwrap();

    if os_info.contains("Red Hat Enterprise Linux 8") || os_info.contains("AlmaLinux 8") {
        println!("cargo:rustc-cfg=rhel8");
    }
}
