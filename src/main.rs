use log::*;
use rumqttc::{EventLoop, Incoming, MqttOptions, QoS, Request, Subscribe};

use serde::{Deserialize, Serialize};
use simple_logger::SimpleLogger;
use std::fs;
use std::io::Write;
use std::path::PathBuf;
use std::time::SystemTime;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "mqtt-recorder", about = "mqtt recorder written in rust")]

struct Opt {
    #[structopt(short, long, default_value = "1")]
    verbose: u32,

    #[structopt(short, long, default_value = "localhost")]
    address: String,

    #[structopt(short, long, default_value = "1883")]
    port: u16,

    #[structopt(parse(from_os_str))]
    outputfile: Option<PathBuf>,
}

#[derive(Serialize, Deserialize)]
struct MqttMessage {
    time: f64,
    qos: u8,
    retain: bool,
    topic: String,
    msg_b64: String,
}

#[tokio::main]
async fn main() {
    let opt = Opt::from_args();

    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_millis();

    let servername = format!("{}-{}", "mqtt-recorder-rs", now);

    let outputfile = if let Some(outputfile) = opt.outputfile {
        outputfile
    } else {
        format!("mqttlog-{}", now).into()
    };

    match opt.verbose {
        1 => {
            let _e = SimpleLogger::new().with_level(LevelFilter::Info).init();
        }
        2 => {
            let _e = SimpleLogger::new().with_level(LevelFilter::Debug).init();
        }
        3 => {
            let _e = SimpleLogger::new().with_level(LevelFilter::Trace).init();
        }
        0 | _ => {}
    }

    let mut log_file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&outputfile)
        .unwrap();

    let mut mqttoptions = MqttOptions::new(servername, &opt.address, opt.port);

    mqttoptions.set_keep_alive(5);
    let mut eventloop = EventLoop::new(mqttoptions, 20 as usize);
    let requests_tx = eventloop.requests_tx.clone();

    loop {
        let res = eventloop.poll().await;
        if let Ok((Some(Incoming::ConnAck(_)), _)) = res {
            info!("Connected to: {}:{}", opt.address, opt.port);

            let subscription = Subscribe::new("#", QoS::AtLeastOnce);
            let _ = requests_tx.send(Request::Subscribe(subscription)).await;

            while let Ok((incoming, _outgoing)) = eventloop.poll().await {
                if let Some(Incoming::Publish(publish)) = incoming {
                    let qos = match publish.qos {
                        QoS::AtMostOnce => 0,
                        QoS::AtLeastOnce => 1,
                        QoS::ExactlyOnce => 2,
                    };

                    let msg = MqttMessage {
                        time: SystemTime::now()
                            .duration_since(SystemTime::UNIX_EPOCH)
                            .unwrap()
                            .as_secs_f64(),
                        retain: publish.retain,
                        topic: publish.topic.clone(),
                        msg_b64: base64::encode(&*publish.payload),
                        qos,
                    };

                    let serialized = serde_json::to_string(&msg).unwrap();
                    writeln!(log_file, "{}", serialized).unwrap();

                    debug!("{:?}", publish);
                }
            }
        }
        info!("Stream cancelled");
    }
}
