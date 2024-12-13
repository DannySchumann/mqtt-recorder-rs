use log::*;
use rumqttc::{ConnectionError, TlsConfiguration, Transport};
use rumqttc::{Event, EventLoop, Incoming, MqttOptions, Publish, QoS, Request, Subscribe};
use serde::{Deserialize, Serialize};
use simple_logger::SimpleLogger;
use std::{
    fs,
    io::{self, BufRead, Read, Write},
    path::PathBuf,
    time::SystemTime,
};
use std::ops::Add;
use std::time::Duration;
use bytes::Bytes;
use structopt::StructOpt;
use tokio::time::Instant;

const MIN_SLEEP: Duration = Duration::from_millis(1);

#[derive(Debug, StructOpt)]
#[structopt(name = "mqtt-recorder", about = "mqtt recorder written in rust")]

struct Opt {
    //// The verbosity of the program
    #[structopt(short, long, default_value = "1")]
    verbose: u32,

    /// The address to connect to
    #[structopt(short, long, default_value = "localhost")]
    address: String,

    /// The port to connect to
    #[structopt(short, long, default_value = "1883")]
    port: u16,

    /// certificate of trusted CA
    #[structopt(short, long)]
    cafile: Option<PathBuf>,

    /// Mode to run software in
    #[structopt(subcommand)]
    mode: Mode,
}

#[derive(Debug, StructOpt)]
pub enum Mode {
    // Records values from an MQTT Stream
    #[structopt(name = "record")]
    Record(RecordOptions),

    // Replay values from an input file
    #[structopt(name = "replay")]
    Replay(ReplayOtions),
}

#[derive(Debug, StructOpt)]
pub struct RecordOptions {
    #[structopt(short, long, default_value = "#")]
    // Topic to record, can be used multiple times for a set of topics
    topic: Vec<String>,
    // The file to write mqtt messages to
    #[structopt(short, long, parse(from_os_str))]
    filename: PathBuf,
}

#[derive(Debug, StructOpt)]
pub struct ReplayOtions {
    #[structopt(short, long, default_value = "1.0")]
    // Speed of the playback, 2.0 makes it twice as fast
    speed: f64,

    // The file to read replay values from
    #[structopt(short, long, parse(from_os_str))]
    filename: PathBuf,

    #[structopt(
        name = "loop",
        short,
        long,
        parse(try_from_str),
        default_value = "false"
    )]
    loop_replay: bool,
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

    let mut mqttoptions = MqttOptions::new(servername, &opt.address, opt.port);
    mqttoptions.set_max_packet_size(1000 * 1024, 1000 * 1024);

    if let Some(cafile) = opt.cafile {
        let mut file = fs::OpenOptions::new();
        let mut file = file.read(true).create_new(false).open(&cafile).unwrap();
        let mut vec = Vec::new();
        let _ = file.read_to_end(&mut vec).unwrap();

        let tlsconfig = TlsConfiguration::Simple {
            ca: vec,
            alpn: None,
            client_auth: None,
        };

        let transport = Transport::Tls(tlsconfig);
        mqttoptions.set_transport(transport);
    }

    mqttoptions.set_keep_alive(5);
    let mut eventloop = EventLoop::new(mqttoptions, 20 as usize);
    let requests_tx = eventloop.requests_tx.clone();

    // Enter recording mode and open file readonly
    match opt.mode {
        Mode::Replay(replay) => {
            let (stop_tx, stop_rx) = std::sync::mpsc::channel();
            let mut accumulated_durations = Duration::from_millis(0);
            let mut start_replay = Instant::now();
            let mut first_message_time = 0.0;
            // Sends the recorded messages
            tokio::spawn(async move {
                // text
                loop {
                    let mut previous = -1.0;
                    let mut file = fs::OpenOptions::new();
                    debug!("{:?}", replay.filename);
                    let file = file
                        .read(true)
                        .create_new(false)
                        .open(&replay.filename)
                        .unwrap();
                    for line in io::BufReader::new(&file).lines() {
                        if let Ok(line) = line {
                            let msg = serde_json::from_str::<MqttMessage>(&line);

                            if let Ok(msg) = msg {
                                if previous < 0.0 {
                                    start_replay = Instant::now();
                                    first_message_time = msg.time;
                                    previous = msg.time;
                                }
                                let now = Instant::now();
                                let replay_elapsed = Duration::from_secs_f64(
                                    (msg.time - first_message_time) / replay.speed
                                );

                                let last_packet_elapsed =  Duration::from_secs_f64((msg.time - previous) / replay.speed);
                                
                                let real_time_elapsed = start_replay.elapsed();
                                let drift = replay_elapsed.as_millis() as i128 - real_time_elapsed.as_millis() as i128;
                                println!("time: {:?}, msg.time: {:?}, previous: {:?}, replay elapsed: {:?}, realtime elapsed: {:?}, drift {:?} ms", now, msg.time, previous, replay_elapsed, real_time_elapsed, drift);
                                if drift > last_packet_elapsed.as_millis() as i128 {
                                    let before_sleep = Instant::now();
                                    tokio::time::sleep(last_packet_elapsed).await;
                                    println!("sleep {:?} to compensate drift (actual sleep {:?})", last_packet_elapsed, before_sleep.elapsed());
                                }
                                
                                previous = msg.time;

                                let qos = match msg.qos {
                                    0 => QoS::AtMostOnce,
                                    1 => QoS::AtLeastOnce,
                                    2 => QoS::ExactlyOnce,
                                    _ => QoS::AtMostOnce,
                                };

                                let base64 = base64::decode(msg.msg_b64).unwrap();

                                let publish = Publish {
                                    dup: false,
                                    qos,
                                    retain: msg.retain,
                                    pkid: 0,
                                    topic: msg.topic.into(),
                                    payload: Bytes::from(base64),
                                };

                                let _e = requests_tx.send(publish.into()).await;
                            }
                        }
                    }

                    if !replay.loop_replay {
                        let _e = stop_tx.send(());
                        break;
                    }
                }
            });

            // run the eventloop forever
            while let Err(std::sync::mpsc::TryRecvError::Empty) = stop_rx.try_recv() {
                let _res = eventloop.poll().await.unwrap();
            }
        }
        // Enter recording mode and open file writeable
        Mode::Record(record) => {
            let mut file = fs::OpenOptions::new();
            let mut file = file
                .write(true)
                .create_new(true)
                .open(&record.filename)
                .unwrap();

            loop {
                let res = eventloop.poll().await;

                match res {
                    Ok(Event::Incoming(Incoming::Publish(publish))) => {
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
                        writeln!(file, "{}", serialized).unwrap();

                        debug!("{:?}", publish);
                    }
                    Ok(Event::Incoming(Incoming::ConnAck(_connect))) => {
                        info!("Connected to: {}:{}", opt.address, opt.port);

                        for topic in &record.topic {
                            let subscription = Subscribe::new(topic, QoS::AtLeastOnce);
                            let _ = requests_tx.send(Request::Subscribe(subscription)).await;
                        }
                    }
                    Err(e) => {
                        error!("{:?}", e);
                        if let ConnectionError::Network(_e) = e {
                            break;
                        }
                    }
                    _ => {}
                }
            }
        }
    }
}
