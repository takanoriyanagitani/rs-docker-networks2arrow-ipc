use std::io;
use std::sync::Arc;

use io::BufWriter;
use io::Write;

use arrow::record_batch::RecordBatch;

use bollard::Docker;
use bollard::errors::Error;
use bollard::models::Network;
use bollard::query_parameters::ListNetworksOptions;

pub async fn list_networks(
    d: &Docker,

    opts: Option<ListNetworksOptions>,
) -> Result<Vec<Network>, Error> {
    d.list_networks(opts).await
}

use arrow::array::ArrayRef;
use arrow::array::builder::{BooleanBuilder, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};

pub fn network_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("created", DataType::Utf8, true),
        Field::new("scope", DataType::Utf8, true),
        Field::new("driver", DataType::Utf8, true),
        Field::new("enable_ipv6", DataType::Boolean, true),
        Field::new("internal", DataType::Boolean, true),
        Field::new("attachable", DataType::Boolean, true),
        Field::new("ingress", DataType::Boolean, true),
    ]))
}

pub fn networks2batch(
    networks: Vec<Network>,
    schema: Arc<Schema>,
) -> Result<RecordBatch, io::Error> {
    let mut id_builder = StringBuilder::new();
    let mut name_builder = StringBuilder::new();
    let mut created_builder = StringBuilder::new();
    let mut scope_builder = StringBuilder::new();
    let mut driver_builder = StringBuilder::new();
    let mut enable_ipv6_builder = BooleanBuilder::new();
    let mut internal_builder = BooleanBuilder::new();
    let mut attachable_builder = BooleanBuilder::new();
    let mut ingress_builder = BooleanBuilder::new();

    for network in networks {
        id_builder.append_option(network.id);
        name_builder.append_option(network.name);
        created_builder.append_option(network.created);
        scope_builder.append_option(network.scope);
        driver_builder.append_option(network.driver);
        enable_ipv6_builder.append_option(network.enable_ipv6);
        internal_builder.append_option(network.internal);
        attachable_builder.append_option(network.attachable);
        ingress_builder.append_option(network.ingress);
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(id_builder.finish()),
        Arc::new(name_builder.finish()),
        Arc::new(created_builder.finish()),
        Arc::new(scope_builder.finish()),
        Arc::new(driver_builder.finish()),
        Arc::new(enable_ipv6_builder.finish()),
        Arc::new(internal_builder.finish()),
        Arc::new(attachable_builder.finish()),
        Arc::new(ingress_builder.finish()),
    ];

    RecordBatch::try_new(schema, columns).map_err(io::Error::other)
}

pub struct IpcStreamWriter<W>(pub arrow::ipc::writer::StreamWriter<BufWriter<W>>)
where
    W: Write;

impl<W> IpcStreamWriter<W>
where
    W: Write,
{
    pub fn finish(&mut self) -> Result<(), io::Error> {
        self.0.finish().map_err(io::Error::other)
    }

    pub fn flush(&mut self) -> Result<(), io::Error> {
        self.0.get_mut().flush()
    }

    pub fn write_batch(&mut self, b: &RecordBatch) -> Result<(), io::Error> {
        self.0.write(b).map_err(io::Error::other)
    }
}

pub fn batch2writer<W>(b: &RecordBatch, mut wtr: W, sch: &Schema) -> Result<(), io::Error>
where
    W: Write,
{
    let swtr = arrow::ipc::writer::StreamWriter::try_new_buffered(&mut wtr, sch)
        .map_err(io::Error::other)?;
    let mut iw = IpcStreamWriter(swtr);
    iw.write_batch(b)?;
    iw.flush()?;
    iw.finish()?;

    drop(iw);

    wtr.flush()
}

pub fn networks_to_writer<W>(
    networks: Vec<Network>,
    mut wtr: W,
    sch: Arc<Schema>,
) -> Result<(), io::Error>
where
    W: Write,
{
    let batch = networks2batch(networks, sch.clone())?;
    batch2writer(&batch, &mut wtr, &sch)
}

pub async fn networks2writer<W>(
    d: &Docker,
    mut wtr: W,
    opts: Option<ListNetworksOptions>,
) -> Result<(), io::Error>
where
    W: Write,
{
    let networks = list_networks(d, opts).await.map_err(io::Error::other)?;
    let schema = network_schema();
    networks_to_writer(networks, &mut wtr, schema)
}

use bollard::ClientVersion;

pub fn unix2docker(
    sock_path: &str,
    timeout_seconds: u64,
    client_version: &ClientVersion,
) -> Result<Docker, io::Error> {
    Docker::connect_with_unix(sock_path, timeout_seconds, client_version).map_err(io::Error::other)
}

pub const DOCKER_UNIX_PATH_DEFAULT: &str = "/var/run/docker.sock";
pub const DOCKER_CON_TIMEOUT_SECONDS_DEFAULT: u64 = 120;
pub const DOCKER_CLIENT_VERSION_DEFAULT: &ClientVersion = bollard::API_DEFAULT_VERSION;
