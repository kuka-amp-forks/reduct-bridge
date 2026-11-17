//! The ROS 2 graph and subscriptions, spoken over Zenoh the way `rmw_zenoh`
//! speaks it (through hiroz): no rcl, and no ROS installation at build or run
//! time. Only the `.msg` files the parser reads come from a ROS install.
//!
//! The session connects as `rmw_zenoh_cpp`'s own sessions do (hiroz's
//! default): a client of the router at `tcp/localhost:7447`, or the session
//! config `ZENOH_SESSION_CONFIG_URI` names, with `ZENOH_CONFIG_OVERRIDE`'s
//! `key=value;...` applied on top.

use super::instance::{Ros2Config, Ros2Durability, Ros2QosConfig, Ros2Reliability};
use anyhow::{Error, anyhow, bail};
use hiroz::Builder;
use hiroz::attachment::Attachment;
use hiroz::context::{ZContext, ZContextBuilder};
use hiroz::dynamic::DynamicMessage;
use hiroz::entity::{EndpointKind, TypeInfo, entity_get_endpoint};
use hiroz::node::ZNode;
use hiroz::pubsub::ZSub;
use hiroz::qos::{QosDurability, QosHistory, QosProfile, QosReliability};
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::time::Duration;
use zenoh::sample::Sample;

/// How long the graph must go without a change to count as settled.
const GRAPH_QUIET: Duration = Duration::from_millis(500);
/// The longest startup waits for it. With nothing else on the graph yet, the
/// worker goes on to look for topics as they appear.
const GRAPH_SETTLE_TIMEOUT: Duration = Duration::from_secs(5);

pub(super) struct Session {
    _context: ZContext,
    node: ZNode,
}

/// A subscription that keeps what arrives until [`Subscription::drain`].
pub(super) struct Subscription {
    sub: ZSub<DynamicMessage, Sample, <DynamicMessage as hiroz::msg::ZMessage>::Serdes>,
}

impl Session {
    pub(super) fn new(cfg: &Ros2Config) -> Result<Self, Error> {
        let mut builder = ZContextBuilder::default();
        if let Some(domain_id) = cfg.domain_id {
            builder = builder.with_domain_id(domain_id);
        }
        let context = builder
            .build()
            .map_err(|err| anyhow!("Failed to open the ROS2 Zenoh session: {}", err))?;
        let node = context
            .create_node(cfg.node_name.as_str())
            .without_parameters()
            .build()
            .map_err(|err| anyhow!("Failed to create ROS2 node '{}': {}", cfg.node_name, err))?;
        // The graph fills in as liveliness tokens arrive. Let it settle before
        // the first look, or a wildcard resolves against half of it.
        let own = [context.session().zid()];
        tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()?
            .block_on(node.graph().wait_for_external_settled(
                &own,
                GRAPH_QUIET,
                GRAPH_SETTLE_TIMEOUT,
            ));
        Ok(Self {
            _context: context,
            node,
        })
    }

    /// Every topic something publishes, with its type as `pkg/msg/Type`. A
    /// topic only subscribed to has nothing to record, and no publisher to
    /// take a type hash from.
    pub(super) fn topic_names_and_types(&self) -> HashMap<String, Vec<String>> {
        let graph = self.node.graph();
        let mut topics: HashMap<String, Vec<String>> = HashMap::new();
        for (topic, type_name) in graph.get_topic_names_and_types() {
            if graph.count(EndpointKind::Publisher, &topic) == 0 {
                continue;
            }
            let type_name = ros_type_name(&type_name);
            let types = topics.entry(topic).or_default();
            if !types.contains(&type_name) {
                types.push(type_name);
            }
        }
        topics
    }

    /// Subscribe to `topic` as `topic_type` publishes it. The key expression
    /// carries the type's hash, taken from a publisher the graph knows.
    pub(super) fn subscribe(
        &self,
        topic: &str,
        topic_type: &str,
        qos: &Ros2QosConfig,
        queue_size: u32,
    ) -> Result<Subscription, Error> {
        let type_info = self
            .node
            .graph()
            .get_entities_by_topic(EndpointKind::Publisher, topic)
            .iter()
            .filter_map(|entity| entity_get_endpoint(entity)?.type_info.clone())
            .find(|info| ros_type_name(&info.name) == topic_type)
            .ok_or_else(|| {
                anyhow!(
                    "No publisher of '{}' [{}] to take its type hash from",
                    topic,
                    topic_type
                )
            })?;
        let sub = self
            .node
            .create_sub_impl::<DynamicMessage>(
                topic,
                Some(TypeInfo::new(&type_info.name, type_info.hash)),
            )
            .with_qos(profile(qos, queue_size)?)
            .build()
            .map_err(|err| anyhow!("{}", err))?;
        Ok(Subscription { sub })
    }
}

impl Subscription {
    /// Hand every message received so far to `handle`, with its publisher's
    /// timestamp in microseconds when the sample carries one.
    pub(super) fn drain(&self, mut handle: impl FnMut(Vec<u8>, Option<u64>)) -> usize {
        let mut count = 0;
        while self.sub.is_ready() {
            let Ok(sample) = self.sub.recv_serialized() else {
                break;
            };
            handle(
                sample.payload().to_bytes().into_owned(),
                source_timestamp_us(&sample),
            );
            count += 1;
        }
        count
    }
}

/// `rmw_zenoh` stamps each sample with its publisher's clock, in nanoseconds.
fn source_timestamp_us(sample: &Sample) -> Option<u64> {
    let attachment = Attachment::try_from(sample.attachment()?).ok()?;
    u64::try_from(attachment.source_timestamp)
        .ok()
        .filter(|ns| *ns > 0)
        .map(|ns| ns / 1_000)
}

/// `std_msgs::msg::dds_::String_`, as `rmw_zenoh` names types on the wire,
/// to `std_msgs/msg/String`.
pub(super) fn ros_type_name(type_name: &str) -> String {
    if !type_name.contains("::") {
        return type_name.to_string();
    }
    let name = type_name.replace("::dds_::", "::");
    let name = name.strip_suffix('_').unwrap_or(&name);
    name.replace("::", "/")
}

fn profile(qos: &Ros2QosConfig, queue_size: u32) -> Result<QosProfile, Error> {
    let depth = qos.depth.unwrap_or(queue_size) as usize;
    let Some(depth) = NonZeroUsize::new(depth) else {
        bail!("ROS2 QoS depth must be at least 1");
    };
    Ok(QosProfile {
        reliability: match qos.reliability {
            Some(Ros2Reliability::BestEffort) => QosReliability::BestEffort,
            Some(Ros2Reliability::Reliable) | None => QosReliability::Reliable,
        },
        durability: match qos.durability {
            Some(Ros2Durability::TransientLocal) => QosDurability::TransientLocal,
            Some(Ros2Durability::Volatile) | None => QosDurability::Volatile,
        },
        history: QosHistory::KeepLast(depth),
        ..QosProfile::default()
    })
}

#[cfg(test)]
mod tests {
    use super::{profile, ros_type_name};
    use crate::input::ros2::instance::{Ros2Durability, Ros2QosConfig, Ros2Reliability};
    use hiroz::qos::{QosDurability, QosHistory, QosReliability};

    #[test]
    fn ros_type_name_demangles_dds_names() {
        assert_eq!(
            ros_type_name("std_msgs::msg::dds_::String_"),
            "std_msgs/msg/String"
        );
        assert_eq!(
            ros_type_name("std_msgs::msg::String"),
            "std_msgs/msg/String"
        );
        assert_eq!(ros_type_name("std_msgs/msg/String"), "std_msgs/msg/String");
    }

    #[test]
    fn profile_defaults_to_reliable_volatile_queue_depth() {
        let qos = profile(&Ros2QosConfig::default(), 16).unwrap();
        assert_eq!(qos.reliability, QosReliability::Reliable);
        assert_eq!(qos.durability, QosDurability::Volatile);
        assert_eq!(qos.history, QosHistory::KeepLast(16.try_into().unwrap()));
    }

    #[test]
    fn profile_takes_the_topic_settings() {
        let cfg = Ros2QosConfig {
            reliability: Some(Ros2Reliability::BestEffort),
            durability: Some(Ros2Durability::TransientLocal),
            depth: Some(1),
        };
        let qos = profile(&cfg, 16).unwrap();
        assert_eq!(qos.reliability, QosReliability::BestEffort);
        assert_eq!(qos.durability, QosDurability::TransientLocal);
        assert_eq!(qos.history, QosHistory::KeepLast(1.try_into().unwrap()));
        assert!(
            profile(
                &Ros2QosConfig {
                    depth: Some(0),
                    ..cfg
                },
                16
            )
            .is_err()
        );
    }
}
