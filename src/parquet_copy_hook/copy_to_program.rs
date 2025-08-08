use std::fs::File;

use crate::arrow_parquet::uri_utils::{uri_as_string, ParsedUriInfo};

pub(crate) unsafe fn copy_file_to_program(uri_info: &mut ParsedUriInfo, program: &str) {
    // get tmp file
    let path = uri_as_string(&uri_info.uri);

    let mut file = File::open(path).unwrap_or_else(|e| {
        panic!("could not open temp file: {e}");
    });

    // open and then get pipe file
    let copy_from = false;
    let mut pipe_file = uri_info.open_program_pipe(program, copy_from);

    // Write temp file to pipe file
    std::io::copy(&mut file, &mut pipe_file)
        .unwrap_or_else(|e| panic!("Failed to copy file to command stdin: {e}"));
}
