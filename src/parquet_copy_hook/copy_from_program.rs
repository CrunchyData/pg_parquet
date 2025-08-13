use crate::arrow_parquet::uri_utils::{uri_as_string, ParsedUriInfo};

pub(crate) unsafe fn copy_program_to_file(uri_info: &mut ParsedUriInfo, program: &str) {
    // get tmp file
    let path = uri_as_string(&uri_info.uri);

    // open and then get pipe file
    let copy_from = true;
    let mut pipe_file = uri_info.open_program_pipe(program, copy_from);

    // create or overwrite the local file
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .truncate(true)
        .create(true)
        .open(&path)
        .unwrap_or_else(|e| panic!("{}", e));

    // Write pipe file to temp file
    std::io::copy(&mut pipe_file, &mut file)
        .unwrap_or_else(|e| panic!("Failed to copy command stdout to file: {e}"));
}
