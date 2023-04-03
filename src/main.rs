use std::{error, io::Cursor};

use aws_lambda_events::s3::object_lambda::{GetObjectContext, S3ObjectLambdaEvent};
use aws_sdk_s3::Client as S3Client;
use lambda_runtime::{run, service_fn, Error, LambdaEvent};
use s3::{GetFile, PutFile};
use thumbnailer::{create_thumbnails, ThumbnailSize};

mod s3;

/**
This s3 object lambda handler
    * downloads the asked file
    * creates a PNG thumbnail from it
    * forward it to the browser
*/
pub(crate) async fn function_handler<T: PutFile + GetFile>(
    event: LambdaEvent<S3ObjectLambdaEvent>,
    size: u32,
    client: &T,
) -> Result<(), Box<dyn error::Error>> {
    tracing::info!("handler starts");

    let context: GetObjectContext = event.payload.get_object_context.unwrap();

    let image = client.get_file(context.input_s3_url)?;

    let thumbnail = get_thumbnail(image, size);
    tracing::info!("thumbnail created");

    match client.put_file(context.output_route, context.output_token, thumbnail).await {
        Ok(msg) => tracing::info!(msg),
        Err(msg) => tracing::info!(msg)
    };
 
    tracing::info!("handler ends");

    Ok(())
}

fn get_thumbnail(vec: Vec<u8>, size: u32) -> Vec<u8> {
    let reader = Cursor::new(vec);
    let mut thumbnails = create_thumbnails(reader, mime::IMAGE_PNG, [ThumbnailSize::Custom((size, size))]).unwrap();

    let thumbnail = thumbnails.pop().unwrap();
    let mut buf = Cursor::new(Vec::new());
    thumbnail.write_png(&mut buf).unwrap();

    buf.into_inner()
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    // required to enable CloudWatch error logging by the runtime
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        // disable printing the name of the module in every log line.
        .with_target(false)
        // this needs to be set to false, otherwise ANSI color codes will
        // show up in a confusing manner in CloudWatch logs.
        .with_ansi(false)
        // disabling time is handy because CloudWatch will add the ingestion time.
        .without_time()
        .init();

    let shared_config = aws_config::load_from_env().await;
    let client = S3Client::new(&shared_config);
    let client_ref = &client;

    let func = service_fn(move |event| async move { function_handler(event, 128, client_ref).await });

    let _ = run(func).await;

    Ok(())
}
