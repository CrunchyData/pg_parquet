use std::process::Command;

use cfg_aliases::cfg_aliases;

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

    cfg_aliases! {
        pre_pg18: { any(feature = "pg14", feature = "pg15", feature = "pg16", feature = "pg17") },
        pre_pg17: { any(feature = "pg14", feature = "pg15", feature = "pg16") },
        pre_pg16: { any(feature = "pg14", feature = "pg15")  },
        pre_pg15: { any(feature = "pg14")  },
    }
}
