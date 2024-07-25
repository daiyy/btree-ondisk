mod mt;
use std::time::Instant;

const DEFAULT_FILE_SIZE: usize = 100 * 1024 * 1024 * 1024;

#[tokio::main]
async fn main() {
    let file_size = DEFAULT_FILE_SIZE;
    let mut file = mt::File::new(file_size);

    tokio::task::spawn(async move {
        file.build().await;
    });
}
