use std::{
    ffi::{c_char, CStr},
    fs::File,
    io::pipe,
    process::Command,
};

use crate::arrow_parquet::uri_utils::{uri_as_string, ParsedUriInfo};

pub(crate) unsafe fn copy_file_to_program(uri_info: ParsedUriInfo, program: *mut c_char) {
    let (pipe_in, mut pipe_out) =
        pipe().unwrap_or_else(|e| panic!("Failed to create command pipe: {e}"));

    let program = CStr::from_ptr(program).to_string_lossy().into_owned();

    #[cfg(unix)]
    let mut command = Command::new("/bin/sh")
        .arg("-lc")
        .arg(program)
        .stdin(pipe_in)
        .spawn()
        .unwrap_or_else(|e| panic!("Failed to spawn command: {e}"));

    #[cfg(windows)]
    let mut command = Command::new("cmd")
        .arg("/C")
        .arg(program)
        .stdin(pipe_in)
        .spawn()
        .unwrap_or_else(|e| panic!("Failed to spawn command: {e}"));

    // Write the file to output pipe
    let path = uri_as_string(&uri_info.uri);

    let mut file = File::open(path).unwrap_or_else(|e| {
        panic!("could not open temp file: {e}");
    });

    std::io::copy(&mut file, &mut pipe_out)
        .unwrap_or_else(|e| panic!("Failed to copy file to command stdin: {e}"));

    // close output pipe to unblock the command
    drop(pipe_out);

    command
        .wait()
        .unwrap_or_else(|e| panic!("Failed to wait for command: {e}"));
}
