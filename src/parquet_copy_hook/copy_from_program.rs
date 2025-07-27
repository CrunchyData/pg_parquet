use std::{ffi::CStr, io::pipe, process::Command};

use crate::arrow_parquet::uri_utils::{uri_as_string, ParsedUriInfo};

pub(crate) unsafe fn copy_program_to_file(uri_info: &ParsedUriInfo) {
    let program = CStr::from_ptr(uri_info.program.expect("Program pointer is null"))
        .to_str()
        .unwrap_or_else(|e| panic!("Failed to convert program pointer to string: {e}"))
        .to_string();

    let (mut pipe_in, pipe_out) =
        pipe().unwrap_or_else(|e| panic!("Failed to create command pipe: {e}"));

    #[cfg(unix)]
    let mut command = Command::new("/bin/sh")
        .arg("-lc")
        .arg(program)
        .stdout(pipe_out)
        .spawn()
        .unwrap_or_else(|e| panic!("Failed to spawn command: {e}"));

    #[cfg(windows)]
    let mut command = Command::new("cmd")
        .arg("/C")
        .arg(program)
        .stdout(pipe_out)
        .spawn()
        .unwrap_or_else(|e| panic!("Failed to spawn command: {e}"));

    command
        .wait()
        .unwrap_or_else(|e| panic!("Failed to wait for command: {e}"));

    // Write input pipe to the file
    let path = uri_as_string(&uri_info.uri);

    // create or overwrite the local file
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .truncate(true)
        .create(true)
        .open(&path)
        .unwrap_or_else(|e| panic!("{}", e));

    std::io::copy(&mut pipe_in, &mut file)
        .unwrap_or_else(|e| panic!("Failed to copy command stdout to file: {e}"));
}
