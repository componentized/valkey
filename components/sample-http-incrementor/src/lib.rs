#![no_main]

use exports::wasi::http::handler::Guest;
use wasi::http::types::{ErrorCode, Fields, Request, Response};
use wasmcloud::keyvalue::{atomics, store, types::Error};

#[derive(Debug, Clone)]
struct SampleHttpIncrementor;

impl SampleHttpIncrementor {
    async fn increment(path: &str) -> Result<i64, Error> {
        let bucket = store::open("http-incrementor".to_string()).await?;
        atomics::increment(&bucket, path.to_string(), 1).await
    }
}

impl Guest for SampleHttpIncrementor {
    async fn handle(request: Request) -> Result<Response, ErrorCode> {
        let path_with_query = request.get_path_with_query().unwrap_or_default();
        let path = path_with_query.split('?').next().unwrap_or_default();

        let count = Self::increment(path)
            .await
            .map_err(|err| ErrorCode::InternalError(Some(err.to_string())))?;

        let (mut contents, contents_reader) = wit_stream::new();
        // the response has no trailers, the dropped writer resolves the future to the default
        let (_, trailers) = wit_future::new(|| Ok(None));
        let (response, _transmitted) =
            Response::new(Fields::new(), Some(contents_reader), trailers);

        // the host reads the body once the response is returned, the body is written after
        wit_bindgen::spawn_local(async move {
            contents.write_all(format!("{count}\n").into_bytes()).await;
        });

        Ok(response)
    }
}

wit_bindgen::generate!({
    world: "sample-http-incrementor",
    path: "../wit",
    generate_all
});

export!(SampleHttpIncrementor);
