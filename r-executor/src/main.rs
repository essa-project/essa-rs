#![warn(missing_docs)]
use essa_common::{
    essa_default_zenoh_prefix, executor_run_function_subscribe_topic, executor_run_module_topic,
};
use anna::{
    anna_default_zenoh_prefix,
    lattice::Lattice,
    nodes::{request_cluster_info, ClientNode},
    store::LatticeValue,
    topics::RoutingThread,
    ClientKey,
};
use anyhow::{bail, Context};
use uuid::Uuid;
use extendr_api::prelude::*;
use extendr_api::{io::PstreamFormat, io::Save, io::Load, Robj};
use extendr_api::serializer::to_robj;
use extendr_api::deserializer::from_robj;
use zenoh::{prelude::r#async::*, query::Reply, queryable::Query};
use serde::Serialize;
use zenoh::prelude::r#async::*;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

pub async fn r_executor(
    zenoh: Arc<zenoh::Session>,
    zenoh_prefix: String,) -> anyhow::Result<()> {
    log::info!("Starting R executor!");
    let reply = zenoh.declare_queryable("essa/*/run_r/*")
        .res()
        .await
        .map_err(|e| anyhow::anyhow!(e))
        .context("failed to declare subscriber r-executor")?;

    let mut anna_client = new_anna_client(zenoh).await.unwrap();

    loop {
        match reply.recv_async().await {
            Ok(query) => {
                let mut topic_split = query.key_expr().as_str().split('/');
                let args = topic_split
                    .next_back()
                    .context("no args key in topic")?
                    .to_owned();
                let _run_r = topic_split
                    .next_back()
                    .context("no run_r in topic")?
                    .to_owned();
                let func = topic_split
                    .next_back()
                    .context("no func in topic")?
                    .to_owned();

                let func = kvs_get(func.into(), &mut anna_client)?;
                let args = kvs_get(args.into(), &mut anna_client)?;
                let mut serialized_result: Vec<u8> = Vec::new();

                {
                    let func = func.into_lww()
                                   .map_err(eyre_to_anyhow)
                                   .context("func is not a LWW lattice")?
                                   .into_revealed()
                                   .into_value();
                    let args = args.into_lww()
                                   .map_err(eyre_to_anyhow)
                                   .context("args is not a LWW lattice")?
                                   .into_revealed()
                                   .into_value();

                    let args
                        = essa_api::bincode::deserialize(&args);

                    let args: essa_common::Rargs = args.unwrap();
                    let func = String::from_utf8(func)?;
                    // this is a problem, because there is single_threaded instruction.
                    let func = eval_string(&func).expect("erro parse");

                    let res: Robj = func.call(Pairlist::from_pairs(args.args.unwrap())).expect("failed to call func");
                    let res: essa_common::Rreturn =
                        essa_common::Rreturn { result: from_robj(&res).unwrap() };

                    serialized_result = essa_api::bincode::serialize(&res).expect("erro serialize res");
                }

                query
                    .reply(Ok(Sample::new(query.key_expr().clone(), serialized_result)))
                    .res()
                    .await
                    .expect("failed to send sample back");
            },
            Err(e) => {
                log::debug!("zenoh error {e:?}");
                break;
            }
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let zenoh = Arc::new(
        zenoh::open(zenoh::config::Config::default())
            .res()
            .await
            .map_err(|e| anyhow::anyhow!(e))
            .context("Failed to connect to zenoh")?,
    );

    let zenoh_prefix = essa_default_zenoh_prefix().to_owned();

    extendr_engine::start_r();
    r_executor(zenoh, zenoh_prefix).await;
    extendr_engine::end_r();

    Ok(())
}

// /// Store the given value in the key-value store.
// fn kvs_put(key: ClientKey, value: LatticeValue, anna: &mut ClientNode) -> anyhow::Result<()> {
//     let start = Instant::now();

//     smol::block_on(anna.put(key, value))
//         .map_err(eyre_to_anyhow)
//         .context("put failed")?;

//     let put_latency = (Instant::now() - start).as_millis();
//     if put_latency >= 100 {
//         log::trace!("high kvs_put latency: {}ms", put_latency);
//     }

//     Ok(())
// }

/// Get the given value in the key-value store.
///
/// Returns an error if the requested key does not exist in the KVS.
fn kvs_get(key: ClientKey, anna: &mut ClientNode) -> anyhow::Result<LatticeValue> {
    smol::block_on(anna.get(key))
        .map_err(eyre_to_anyhow)
        .context("get failed")
}

async fn new_anna_client(zenoh: Arc<zenoh::Session>) -> anyhow::Result<ClientNode> {
    let zenoh_prefix = anna_default_zenoh_prefix().to_owned();

    let cluster_info = request_cluster_info(&zenoh, &zenoh_prefix)
        .await
        .map_err(|e| anyhow::anyhow!(e))
        .context("Failed to request cluster info from seed node")?;

    let routing_threads: Vec<_> = cluster_info
        .routing_node_ids
        .into_iter()
        .map(|node_id| RoutingThread {
            node_id,
            // TODO: use anna config file to get number of threads per
            // routing node
            thread_id: 0,
        })
        .collect();

    // connect to anna as a new client node
    let mut anna = ClientNode::new(
        Uuid::new_v4().to_string(),
        0,
        routing_threads,
        Duration::from_secs(10),
        zenoh,
        zenoh_prefix,
    )
    .map_err(eyre_to_anyhow)
    .context("failed to connect to anna")?;

    anna.init_tcp_connections()
        .await
        .map_err(eyre_to_anyhow)
        .context("Failed to init TCP connections in anna client")?;

    Ok(anna)
}
/// Transforms an [`eyre::Report`] to an [`anyhow::Error`].
fn eyre_to_anyhow(err: eyre::Report) -> anyhow::Error {
    let err = Box::<dyn std::error::Error + 'static + Send + Sync>::from(err);
    anyhow::anyhow!(err)
}



// fn main() -> anyhow::Result<()> {

//     extendr_engine::start_r();
//     // An R object with a single string "hello"
//     let character = r!("hello");
//     println!("{:?}", character);
//     let character = r!(["hello", "goodbye"]);
//     println!("{:?}", character);

//     let function = R!("function(x, y) {x+y}").expect("cannot create a `function`");
//     println!("{:?}", function.call(pairlist!(1, 2)));

//     let env: Environment = new_env(global_env(), true, 10).try_into().unwrap();
//     env.set_local(sym!(x), "hello");
//     assert_eq!(env.local(sym!(x)), Ok(r!("hello")));

//     let _env = new_env(current_env(), false, 10);

//     let names_and_values = (0..100).map(|i| (format!("n{}", i), i));
//     let env = Environment::from_pairs(global_env(), names_and_values);
//     let expr = env.clone();
//     assert_eq!(expr.len(), 100);
//     let env2 = expr.as_environment().unwrap();
//     assert_eq!(env2.len(), 100);

//     println!("env: {:?}", env);
//     println!("ls env: {:?}", lang!("ls", env.clone()).eval());


//     let mut w = Vec::new();
//     // to entire sure that this can use asciiformat
//     function.to_writer(&mut w, PstreamFormat::AsciiFormat, 3, None).unwrap();

//     let c = String::from_utf8(w)?;
//     let mut c = std::io::Cursor::new(c);
//     let res = Robj::from_reader(&mut c, PstreamFormat::AsciiFormat, None).unwrap();
//     assert_eq!(res.call(pairlist!(1, 2)).unwrap(), r!(3));

//     let promise = Promise::from_parts(r!(1), global_env()).unwrap();
//     assert!(promise.value().is_unbound_value());
//     assert_eq!(promise.eval_promise().unwrap(), r!(1));
//     assert_eq!(promise.value(), r!(1));

//     #[derive(Serialize)]
//     struct Rfloat2(Rfloat);
//     let s = Rfloat2(Rfloat::na());
//     let expected = r!(());
//     assert_eq!(to_robj(&s).unwrap(), Robj::from(expected));

//     #[derive(Serialize)]
//     struct Test<'a> {
//         int: i32,
//         seq: Vec<&'a str>,
//     }

//     let test = Test {
//         int: 1,
//         seq: vec!["a", "b"],
//     };

//     let expected = list!(int=1, seq=list!("a", "b"));
//     assert_eq!(to_robj(&test).unwrap(), Robj::from(expected));

//     //let result = function.call(pairlist!(sym!("a"), sym!("b")));
//     //println!("result: {:?}", result);

//     // let list = list!(a = 1, b = 2);
//     // println!("{:?}", list);

//     // let confint1 = R!("confint(lm(weight ~ group - 1, PlantGrowth))").unwrap();
//     // println!("{:?}", confint1);

//     // assert_eq!(call!("`+`", 1, 2).expect("deu erro"), r!(3));

//     // let formula = lang!("~", sym!(weight), lang!("-", sym!(group), 1.0)).set_class(["formula"]).unwrap();
//     // let plant_growth = global!(PlantGrowth).unwrap();
//     // let model = call!("lm", formula, plant_growth).unwrap();
//     // let confint2 = call!("confint", model).unwrap();

//     // assert_eq!(confint1.as_real_vector(), confint2.as_real_vector());

//     let file = R!("source('../test-function/a.R')");
//     // only the last one is available
//     println!("{:?}", file);

//     let func = &file.unwrap().as_list().unwrap().values().collect::<Vec<_>>()[0];
//     println!("{:?}", func);

//     println!("{:?}", func.call(pairlist!(1, 2)));

//     extendr_engine::end_r();
//     Ok(())
// }
