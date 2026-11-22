use clap::Parser;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    meiloader::run(meiloader::Args::parse(), None).await
}
