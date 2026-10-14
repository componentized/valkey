//! The admin commands: ACL, CLIENT, CLUSTER, COMMANDLOG, FAILOVER, LATENCY, MODULE, ROLE, SHUTDOWN
//! and the like.

use crate::command::{Flag, IntoCommand, NonEmpty, Words};
use crate::types::{
    AclChannelRule, AclCommandRule, AclDbRule, AclDryrunResult, AclKeyRule, AclListEntry,
    AclLogEntry, AclRule, AclSelectorRule, AclUserRule, BgsaveOperation, ClientFilter,
    ClientPauseMode, ClientType, ClientUnblockType, ClusterBumpepochResult, ClusterFailoverMode,
    ClusterGetslotmigrationsEntry, ClusterGetslotmigrationsOperation, ClusterMigrateslotsGroup,
    ClusterResetMode, ClusterSetslotState, CommandlogEntry, CommandlogType, Error, FailoverTarget,
    Integer, LatencyHistogramEntry, LatencyLatestEntry, ModuleListEntry, RespValue, RolePrimary,
    RolePrimaryReplica, RoleReplica, RoleReply, ShutdownSave,
};
use crate::value::{Fields, FlatPairs, FromValue, items, parse_string};

use wit_bindgen::rt::Map;

impl IntoCommand for AclRule {
    fn into_command(self) -> Result<Words, Error> {
        match self {
            AclRule::Keys(rule) => rule.into_command(),
            AclRule::Channel(rule) => rule.into_command(),
            AclRule::Command(rule) => rule.into_command(),
            AclRule::Db(rule) => rule.into_command(),
            AclRule::User(rule) => rule.into_command(),
            AclRule::Other(rule) => rule.into_command(),
        }
    }
}

impl IntoCommand for AclSelectorRule {
    fn into_command(self) -> Result<Words, Error> {
        match self {
            AclSelectorRule::Keys(rule) => rule.into_command(),
            AclSelectorRule::Channel(rule) => rule.into_command(),
            AclSelectorRule::Command(rule) => rule.into_command(),
            AclSelectorRule::Db(rule) => rule.into_command(),
            AclSelectorRule::Other(rule) => rule.into_command(),
        }
    }
}

impl IntoCommand for AclKeyRule {
    fn into_command(self) -> Result<Words, Error> {
        match self {
            AclKeyRule::Add(pattern) => format!("~{pattern}"),
            AclKeyRule::AddRead(pattern) => format!("%R~{pattern}"),
            AclKeyRule::AddWrite(pattern) => format!("%W~{pattern}"),
            AclKeyRule::AddReadWrite(pattern) => format!("%RW~{pattern}"),
            AclKeyRule::All => "allkeys".to_string(),
            AclKeyRule::Reset => "resetkeys".to_string(),
        }
        .into_command()
    }
}

impl IntoCommand for AclChannelRule {
    fn into_command(self) -> Result<Words, Error> {
        match self {
            AclChannelRule::Add(pattern) => format!("&{pattern}"),
            AclChannelRule::All => "allchannels".to_string(),
            AclChannelRule::Reset => "resetchannels".to_string(),
        }
        .into_command()
    }
}

impl IntoCommand for AclCommandRule {
    fn into_command(self) -> Result<Words, Error> {
        match self {
            AclCommandRule::Add(command) => format!("+{command}"),
            AclCommandRule::AddCategory(category) => format!("+@{category}"),
            AclCommandRule::AddFirstArg((command, arg)) => format!("+{command}|{arg}"),
            AclCommandRule::All => "allcommands".to_string(),
            AclCommandRule::Remove(command) => format!("-{command}"),
            AclCommandRule::RemoveCategory(category) => format!("-@{category}"),
            AclCommandRule::Reset => "nocommands".to_string(),
        }
        .into_command()
    }
}

/// `db=` requires at least one database.
impl IntoCommand for AclDbRule {
    fn into_command(self) -> Result<Words, Error> {
        match self {
            AclDbRule::Db(dbs) if dbs.is_empty() => {
                return Err(Error::Client("db requires at least one item".to_string()));
            }
            AclDbRule::Db(dbs) => format!(
                "db={}",
                dbs.iter()
                    .map(|db| db.to_string())
                    .collect::<Vec<_>>()
                    .join(",")
            ),
            AclDbRule::All => "alldbs".to_string(),
            AclDbRule::Reset => "resetdbs".to_string(),
        }
        .into_command()
    }
}

impl IntoCommand for AclUserRule {
    fn into_command(self) -> Result<Words, Error> {
        match self {
            AclUserRule::On => "on".into_command(),
            AclUserRule::Off => "off".into_command(),
            AclUserRule::Nopass => "nopass".into_command(),
            AclUserRule::SanitizePayload => "sanitize-payload".into_command(),
            AclUserRule::SkipSanitizePayload => "skip-sanitize-payload".into_command(),
            AclUserRule::Password(password) => format!(">{password}").into_command(),
            AclUserRule::HashedPassword(hash) => format!("#{hash}").into_command(),
            AclUserRule::RemovePassword(password) => format!("<{password}").into_command(),
            AclUserRule::RemoveHashedPassword(hash) => format!("!{hash}").into_command(),
            AclUserRule::Selector(rules) => ("(", rules, ")").into_command(),
            AclUserRule::Clearselectors => "clearselectors".into_command(),
            AclUserRule::Reset => "reset".into_command(),
        }
    }
}

/// `OK` when the user may run the command, otherwise why not.
impl FromValue for AclDryrunResult {
    fn from_value(value: RespValue) -> Result<Self, Error> {
        match value {
            RespValue::String(reply) if reply == "OK" => Ok(AclDryrunResult::Allowed),
            value => String::from_value(value).map(AclDryrunResult::Denied),
        }
    }
}

/// The rules of ACL GETUSER: the flags, the hashed passwords, the root selector's rules, then the
/// selectors.
pub struct AclUserRules(pub Vec<AclRule>);

impl FromValue for AclUserRules {
    fn from_value(value: RespValue) -> Result<Self, Error> {
        let mut fields = Fields::from_value(value)?;
        let flags: Vec<String> = fields.required("flags")?;
        let passwords: Vec<String> = fields.required("passwords")?;
        let mut rules: Vec<AclRule> = flags.iter().map(|flag| rule(flag)).collect();
        rules.extend(
            passwords
                .into_iter()
                .map(|hash| AclRule::User(AclUserRule::HashedPassword(hash))),
        );
        rules.extend(selector_rules(&mut fields)?.into_iter().map(root_rule));
        let selectors: Vec<Fields> = fields.required("selectors")?;
        for mut selector in selectors {
            let selector = selector_rules(&mut selector)?;
            rules.push(AclRule::User(AclUserRule::Selector(selector)));
        }
        Ok(AclUserRules(rules))
    }
}

/// The rules of a selector, from its `keys`, `channels`, `databases` and `commands` fields, each
/// rules separated by spaces.
///
/// The channels are preceded by `resetchannels`, as ACL LIST writes them, so the server's
/// `acl-pubsub-default` does not add to them.
fn selector_rules(fields: &mut Fields) -> Result<Vec<AclSelectorRule>, Error> {
    let keys: String = fields.required("keys")?;
    let channels: String = fields.required("channels")?;
    // since Valkey 9.1
    let databases: Option<String> = fields.optional("databases")?;
    let commands: String = fields.required("commands")?;
    let mut rules: Vec<AclSelectorRule> = keys.split_whitespace().map(key_rule).collect();
    rules.push(AclSelectorRule::Channel(AclChannelRule::Reset));
    rules.extend(channels.split_whitespace().map(channel_rule));
    rules.extend(
        databases
            .iter()
            .flat_map(|databases| databases.split_whitespace().map(db_rule)),
    );
    rules.extend(commands.split_whitespace().map(command_rule));
    Ok(rules)
}

fn key_rule(rule: &str) -> AclSelectorRule {
    let key_rule = match rule {
        "allkeys" => AclKeyRule::All,
        "resetkeys" => AclKeyRule::Reset,
        _ => {
            if let Some(pattern) = rule.strip_prefix("%RW~") {
                AclKeyRule::AddReadWrite(pattern.to_string())
            } else if let Some(pattern) = rule.strip_prefix("%R~") {
                AclKeyRule::AddRead(pattern.to_string())
            } else if let Some(pattern) = rule.strip_prefix("%W~") {
                AclKeyRule::AddWrite(pattern.to_string())
            } else if let Some(pattern) = rule.strip_prefix('~') {
                AclKeyRule::Add(pattern.to_string())
            } else {
                return AclSelectorRule::Other(rule.to_string());
            }
        }
    };
    AclSelectorRule::Keys(key_rule)
}

fn channel_rule(rule: &str) -> AclSelectorRule {
    let channel_rule = match rule {
        "allchannels" => AclChannelRule::All,
        "resetchannels" => AclChannelRule::Reset,
        _ => match rule.strip_prefix('&') {
            Some(pattern) => AclChannelRule::Add(pattern.to_string()),
            None => return AclSelectorRule::Other(rule.to_string()),
        },
    };
    AclSelectorRule::Channel(channel_rule)
}

fn db_rule(rule: &str) -> AclSelectorRule {
    let db_rule = match rule {
        "alldbs" => AclDbRule::All,
        "resetdbs" => AclDbRule::Reset,
        _ => {
            let dbs = rule
                .strip_prefix("db=")
                .map(|dbs| dbs.split(',').map(str::parse).collect::<Result<_, _>>());
            match dbs {
                Some(Ok(dbs)) => AclDbRule::Db(dbs),
                _ => return AclSelectorRule::Other(rule.to_string()),
            }
        }
    };
    AclSelectorRule::Db(db_rule)
}

/// `+config|get` is a subcommand, or a first argument, either way it is sent back as is.
fn command_rule(rule: &str) -> AclSelectorRule {
    let command_rule = match rule {
        "allcommands" | "+@all" => AclCommandRule::All,
        "nocommands" | "-@all" => AclCommandRule::Reset,
        _ => {
            if let Some(category) = rule.strip_prefix("+@") {
                AclCommandRule::AddCategory(category.to_string())
            } else if let Some(category) = rule.strip_prefix("-@") {
                AclCommandRule::RemoveCategory(category.to_string())
            } else if let Some(command) = rule.strip_prefix('+') {
                AclCommandRule::Add(command.to_string())
            } else if let Some(command) = rule.strip_prefix('-') {
                AclCommandRule::Remove(command.to_string())
            } else {
                return AclSelectorRule::Other(rule.to_string());
            }
        }
    };
    AclSelectorRule::Command(command_rule)
}

/// The lines of ACL LIST, each `user <username> <rule> ...`, with selectors as `(<rule> ...)`.
pub struct AclList(pub Vec<AclListEntry>);

impl FromValue for AclList {
    fn from_value(value: RespValue) -> Result<Self, Error> {
        let lines: Vec<String> = FromValue::from_value(value)?;
        lines
            .iter()
            .map(|line| acl_list_entry(line))
            .collect::<Result<_, _>>()
            .map(AclList)
    }
}

fn acl_list_entry(line: &str) -> Result<AclListEntry, Error> {
    let invalid = || Error::Client(format!("unexpected ACL LIST line: {line:?}"));
    let mut words = line
        .strip_prefix("user ")
        .ok_or_else(invalid)?
        .split_whitespace();
    let username = words.next().ok_or_else(invalid)?.to_string();
    let mut rules = vec![];
    while let Some(word) = words.next() {
        if !word.starts_with('(') {
            rules.push(rule(word));
            continue;
        }
        // a selector, its rules up to the word that closes it
        let mut selector = vec![word];
        while !selector.last().is_some_and(|word| word.ends_with(')')) {
            selector.push(words.next().ok_or_else(invalid)?);
        }
        let selector = selector.join(" ");
        let selector = selector
            .strip_prefix('(')
            .and_then(|selector| selector.strip_suffix(')'))
            .ok_or_else(invalid)?;
        let selector = selector.split_whitespace().map(selector_rule).collect();
        rules.push(AclRule::User(AclUserRule::Selector(selector)));
    }
    Ok(AclListEntry { username, rules })
}

/// A rule of the user or its root selector, e.g. `on`, `#<hash>` or `~app:*`.
fn rule(word: &str) -> AclRule {
    let user_rule = match word {
        "on" => AclUserRule::On,
        "off" => AclUserRule::Off,
        "nopass" => AclUserRule::Nopass,
        "sanitize-payload" => AclUserRule::SanitizePayload,
        "skip-sanitize-payload" => AclUserRule::SkipSanitizePayload,
        "clearselectors" => AclUserRule::Clearselectors,
        "reset" => AclUserRule::Reset,
        _ => {
            if let Some(hash) = word.strip_prefix('#') {
                AclUserRule::HashedPassword(hash.to_string())
            } else if let Some(hash) = word.strip_prefix('!') {
                AclUserRule::RemoveHashedPassword(hash.to_string())
            } else if let Some(password) = word.strip_prefix('>') {
                AclUserRule::Password(password.to_string())
            } else if let Some(password) = word.strip_prefix('<') {
                AclUserRule::RemovePassword(password.to_string())
            } else {
                return root_rule(selector_rule(word));
            }
        }
    };
    AclRule::User(user_rule)
}

/// A rule of a selector, e.g. `~app:*`, `&news:*`, `db=0` or `+get`.
fn selector_rule(word: &str) -> AclSelectorRule {
    match word {
        "allkeys" | "resetkeys" => key_rule(word),
        "allchannels" | "resetchannels" => channel_rule(word),
        "alldbs" | "resetdbs" => db_rule(word),
        "allcommands" | "nocommands" => command_rule(word),
        _ if word.starts_with('~') || word.starts_with('%') => key_rule(word),
        _ if word.starts_with('&') => channel_rule(word),
        _ if word.starts_with("db=") => db_rule(word),
        _ if word.starts_with('+') || word.starts_with('-') => command_rule(word),
        _ => AclSelectorRule::Other(word.to_string()),
    }
}

/// A rule of the root selector.
fn root_rule(rule: AclSelectorRule) -> AclRule {
    match rule {
        AclSelectorRule::Keys(rule) => AclRule::Keys(rule),
        AclSelectorRule::Channel(rule) => AclRule::Channel(rule),
        AclSelectorRule::Command(rule) => AclRule::Command(rule),
        AclSelectorRule::Db(rule) => AclRule::Db(rule),
        AclSelectorRule::Other(rule) => AclRule::Other(rule),
    }
}

impl FromValue for AclLogEntry {
    fn from_value(value: RespValue) -> Result<Self, Error> {
        let mut fields = Fields::from_value(value)?;
        Ok(Self {
            count: fields.required("count")?,
            reason: fields.required("reason")?,
            context: fields.required("context")?,
            object: fields.required("object")?,
            username: fields.required("username")?,
            age_seconds: fields.required("age-seconds")?,
            client_info: fields.required("client-info")?,
            // since Valkey 7.2
            entry_id: fields.optional("entry-id")?,
            timestamp_created: fields.optional("timestamp-created")?,
            timestamp_last_updated: fields.optional("timestamp-last-updated")?,
        })
    }
}

impl IntoCommand for BgsaveOperation {
    fn into_command(self) -> Result<Words, Error> {
        match self {
            BgsaveOperation::Schedule => "SCHEDULE",
            BgsaveOperation::Cancel => "CANCEL",
        }
        .into_command()
    }
}

/// `master` rather than `primary`, which servers before Valkey 8.0 do not know.
impl IntoCommand for ClientType {
    fn into_command(self) -> Result<Words, Error> {
        match self {
            ClientType::Normal => "normal",
            ClientType::Primary => "master",
            ClientType::Replica => "replica",
            ClientType::Pubsub => "pubsub",
        }
        .into_command()
    }
}

impl IntoCommand for ClientFilter {
    fn into_command(self) -> Result<Words, Error> {
        match self {
            ClientFilter::Id(ids) => ("ID", NonEmpty::new("id", ids)).into_command(),
            ClientFilter::Type(client_type) => ("TYPE", client_type).into_command(),
            ClientFilter::User(user) => ("USER", user).into_command(),
            ClientFilter::Addr(addr) => ("ADDR", addr).into_command(),
            ClientFilter::Laddr(laddr) => ("LADDR", laddr).into_command(),
            ClientFilter::Skipme(skip) => {
                ("SKIPME", if skip { "YES" } else { "NO" }).into_command()
            }
            ClientFilter::Maxage(age) => ("MAXAGE", age).into_command(),
            ClientFilter::Name(name) => ("NAME", name).into_command(),
            ClientFilter::Idle(idle) => ("IDLE", idle).into_command(),
            ClientFilter::Flags(flags) => ("FLAGS", flags).into_command(),
            ClientFilter::LibName(name) => ("LIB-NAME", name).into_command(),
            ClientFilter::LibVer(version) => ("LIB-VER", version).into_command(),
            ClientFilter::Db(db) => ("DB", db).into_command(),
            ClientFilter::Capa(capability) => ("CAPA", capability).into_command(),
            ClientFilter::Ip(ip) => ("IP", ip).into_command(),
            ClientFilter::NotId(ids) => ("NOT-ID", NonEmpty::new("not-id", ids)).into_command(),
            ClientFilter::NotType(client_type) => ("NOT-TYPE", client_type).into_command(),
            ClientFilter::NotUser(user) => ("NOT-USER", user).into_command(),
            ClientFilter::NotAddr(addr) => ("NOT-ADDR", addr).into_command(),
            ClientFilter::NotLaddr(laddr) => ("NOT-LADDR", laddr).into_command(),
            ClientFilter::NotName(name) => ("NOT-NAME", name).into_command(),
            ClientFilter::NotFlags(flags) => ("NOT-FLAGS", flags).into_command(),
            ClientFilter::NotLibName(name) => ("NOT-LIB-NAME", name).into_command(),
            ClientFilter::NotLibVer(version) => ("NOT-LIB-VER", version).into_command(),
            ClientFilter::NotDb(db) => ("NOT-DB", db).into_command(),
            ClientFilter::NotCapa(capability) => ("NOT-CAPA", capability).into_command(),
            ClientFilter::NotIp(ip) => ("NOT-IP", ip).into_command(),
        }
    }
}

/// The lines of CLIENT LIST, each of the fields of a connection, `name=value` separated by spaces.
#[derive(Debug)]
pub struct ClientList(pub Vec<Map<String, String>>);

impl FromValue for ClientList {
    fn from_value(value: RespValue) -> Result<Self, Error> {
        let text = String::from_value(value)?;
        let clients = text
            .lines()
            .filter(|line| !line.is_empty())
            .map(|line| {
                line.split(' ')
                    .filter_map(|field| field.split_once('='))
                    .map(|(name, value)| (name.to_string(), value.to_string()))
                    .collect()
            })
            .collect();
        Ok(ClientList(clients))
    }
}

impl IntoCommand for ClientPauseMode {
    fn into_command(self) -> Result<Words, Error> {
        match self {
            ClientPauseMode::Write => "WRITE",
            ClientPauseMode::All => "ALL",
        }
        .into_command()
    }
}

impl IntoCommand for ClientUnblockType {
    fn into_command(self) -> Result<Words, Error> {
        match self {
            ClientUnblockType::Timeout => "TIMEOUT",
            ClientUnblockType::Error => "ERROR",
        }
        .into_command()
    }
}

/// `BUMPED epoch` or `STILL epoch`.
impl FromValue for ClusterBumpepochResult {
    fn from_value(value: RespValue) -> Result<Self, Error> {
        let reply = String::from_value(value)?;
        let invalid = || Error::Client(format!("unexpected reply: {reply:?}"));
        let (outcome, epoch) = reply.split_once(' ').ok_or_else(invalid)?;
        let epoch = epoch.parse().map_err(|_| invalid())?;
        match outcome {
            "BUMPED" => Ok(ClusterBumpepochResult::Bumped(epoch)),
            "STILL" => Ok(ClusterBumpepochResult::Still(epoch)),
            _ => Err(invalid()),
        }
    }
}

impl IntoCommand for ClusterFailoverMode {
    fn into_command(self) -> Result<Words, Error> {
        match self {
            ClusterFailoverMode::Force => "FORCE",
            ClusterFailoverMode::Takeover => "TAKEOVER",
        }
        .into_command()
    }
}

impl FromValue for ClusterGetslotmigrationsOperation {
    fn from_value(value: RespValue) -> Result<Self, Error> {
        match String::from_value(value)?.as_str() {
            "IMPORT" => Ok(ClusterGetslotmigrationsOperation::Import),
            "EXPORT" => Ok(ClusterGetslotmigrationsOperation::Export),
            operation => Err(Error::Client(format!(
                "unexpected operation: {operation:?}"
            ))),
        }
    }
}

/// Ranges of slots, `start-end start-end ...`.
pub struct SlotRanges(pub Vec<(u16, u16)>);

impl FromValue for SlotRanges {
    fn from_value(value: RespValue) -> Result<Self, Error> {
        let ranges = String::from_value(value)?;
        let invalid = || Error::Client(format!("unexpected slot ranges: {ranges:?}"));
        ranges
            .split_whitespace()
            .map(|range| {
                let (start, end) = range.split_once('-').ok_or_else(invalid)?;
                Ok((
                    start.parse().map_err(|_| invalid())?,
                    end.parse().map_err(|_| invalid())?,
                ))
            })
            .collect::<Result<_, _>>()
            .map(SlotRanges)
    }
}

impl FromValue for ClusterGetslotmigrationsEntry {
    fn from_value(value: RespValue) -> Result<Self, Error> {
        let mut fields = Fields::from_value(value)?;
        let slot_ranges: SlotRanges = fields.required("slot_ranges")?;
        Ok(Self {
            link_name: fields.required("link_name")?,
            operation: fields.required("operation")?,
            slot_ranges: slot_ranges.0,
            target_node: fields.required("target_node")?,
            source_node: fields.required("source_node")?,
            create_time: fields.required("create_time")?,
            last_update_time: fields.required("last_update_time")?,
            last_ack_time: fields.required("last_ack_time")?,
            state: fields.required("state")?,
            message: fields.required("message")?,
            cow_size: fields.required("cow_size")?,
        })
    }
}

/// `SLOTSRANGE start-slot end-slot [ start-slot end-slot ... ] NODE node-id [ AUTH username password ]`
impl IntoCommand for ClusterMigrateslotsGroup {
    fn into_command(self) -> Result<Words, Error> {
        (
            "SLOTSRANGE",
            NonEmpty::new("ranges", self.ranges),
            "NODE",
            self.node_id,
            self.auth
                .map(|(username, password)| ("AUTH", username, password)),
        )
            .into_command()
    }
}

impl IntoCommand for ClusterResetMode {
    fn into_command(self) -> Result<Words, Error> {
        match self {
            ClusterResetMode::Hard => "HARD",
            ClusterResetMode::Soft => "SOFT",
        }
        .into_command()
    }
}

impl IntoCommand for ClusterSetslotState {
    fn into_command(self) -> Result<Words, Error> {
        match self {
            ClusterSetslotState::Importing(node_id) => ("IMPORTING", node_id).into_command(),
            ClusterSetslotState::Migrating(node_id) => ("MIGRATING", node_id).into_command(),
            ClusterSetslotState::Node(node_id) => ("NODE", node_id).into_command(),
            ClusterSetslotState::Stable => "STABLE".into_command(),
        }
    }
}

impl IntoCommand for CommandlogType {
    fn into_command(self) -> Result<Words, Error> {
        match self {
            CommandlogType::Slow => "slow",
            CommandlogType::LargeRequest => "large-request",
            CommandlogType::LargeReply => "large-reply",
        }
        .into_command()
    }
}

/// `[id, timestamp, value, [argument ...], client-address, client-name]`, of SLOWLOG GET and
/// COMMANDLOG GET.
impl FromValue for CommandlogEntry {
    fn from_value(value: RespValue) -> Result<Self, Error> {
        match <[RespValue; 6]>::try_from(items(value)?) {
            Ok([id, timestamp, value, arguments, client_address, client_name]) => Ok(Self {
                id: FromValue::from_value(id)?,
                timestamp: FromValue::from_value(timestamp)?,
                value: FromValue::from_value(value)?,
                arguments: FromValue::from_value(arguments)?,
                client_address: FromValue::from_value(client_address)?,
                client_name: FromValue::from_value(client_name)?,
            }),
            Err(items) => Err(Error::Client(format!(
                "unexpected number of items, expected 6: {}",
                items.len()
            ))),
        }
    }
}

/// `TO host port [ FORCE ]`
impl IntoCommand for FailoverTarget {
    fn into_command(self) -> Result<Words, Error> {
        ("TO", self.host, self.port, Flag("FORCE", self.force)).into_command()
    }
}

impl FromValue for LatencyHistogramEntry {
    fn from_value(value: RespValue) -> Result<Self, Error> {
        let mut fields = Fields::from_value(value)?;
        // a map of each bucket to its cumulative count
        let histogram: FlatPairs<Integer, Integer> = fields.required("histogram_usec")?;
        Ok(Self {
            calls: fields.required("calls")?,
            histogram_usec: histogram.0,
        })
    }
}

/// `[event, timestamp, latest, max, sum, count]`
impl FromValue for LatencyLatestEntry {
    fn from_value(value: RespValue) -> Result<Self, Error> {
        match <[RespValue; 6]>::try_from(items(value)?) {
            Ok([event, timestamp, latest, max, sum, count]) => Ok(Self {
                event: FromValue::from_value(event)?,
                timestamp: FromValue::from_value(timestamp)?,
                latest: FromValue::from_value(latest)?,
                max: FromValue::from_value(max)?,
                sum: FromValue::from_value(sum)?,
                count: FromValue::from_value(count)?,
            }),
            Err(items) => Err(Error::Client(format!(
                "unexpected number of items, expected 6: {}",
                items.len()
            ))),
        }
    }
}

impl FromValue for ModuleListEntry {
    fn from_value(value: RespValue) -> Result<Self, Error> {
        let mut fields = Fields::from_value(value)?;
        Ok(Self {
            name: fields.required("name")?,
            ver: fields.required("ver")?,
            path: fields.required("path")?,
            args: fields.required("args")?,
        })
    }
}

/// `[ip, port, offset]`, the port and offset are strings.
impl FromValue for RolePrimaryReplica {
    fn from_value(value: RespValue) -> Result<Self, Error> {
        let (ip, port, offset) = FromValue::from_value(value)?;
        Ok(Self {
            ip,
            port: parse_string(port)?,
            offset: parse_string(offset)?,
        })
    }
}

/// `["master", offset, [replica ...]]`, `["slave", host, port, state, offset]` or
/// `["sentinel", [primary ...]]`.
impl FromValue for RoleReply {
    fn from_value(value: RespValue) -> Result<Self, Error> {
        let mut items = items(value)?.into_iter();
        let role = String::from_value(items.next().unwrap_or(RespValue::Null))?;
        let mut next = || {
            items
                .next()
                .ok_or_else(|| Error::Client(format!("too few items for the role {role:?}")))
        };
        match role.as_str() {
            "master" => Ok(RoleReply::Primary(RolePrimary {
                offset: FromValue::from_value(next()?)?,
                replicas: FromValue::from_value(next()?)?,
            })),
            "slave" => Ok(RoleReply::Replica(RoleReplica {
                primary_host: FromValue::from_value(next()?)?,
                primary_port: u64::from_value(next()?)?
                    .try_into()
                    .map_err(|_| Error::Client("unexpected port".to_string()))?,
                state: FromValue::from_value(next()?)?,
                offset: FromValue::from_value(next()?)?,
            })),
            "sentinel" => Ok(RoleReply::Sentinel(FromValue::from_value(next()?)?)),
            role => Err(Error::Client(format!("unexpected role: {role:?}"))),
        }
    }
}

impl IntoCommand for ShutdownSave {
    fn into_command(self) -> Result<Words, Error> {
        match self {
            ShutdownSave::Nosave => "NOSAVE",
            ShutdownSave::Save => "SAVE",
        }
        .into_command()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::{
        array, bulk, bulks, double, fields, from, from_error, int, ok, simple, words, words_error,
    };
    use crate::types::{
        DurationMilliseconds, DurationSeconds, UnixTimeMilliseconds, UnixTimeSeconds,
    };

    fn key(rule: AclKeyRule) -> AclSelectorRule {
        AclSelectorRule::Keys(rule)
    }

    fn channel(rule: AclChannelRule) -> AclSelectorRule {
        AclSelectorRule::Channel(rule)
    }

    fn command(rule: AclCommandRule) -> AclSelectorRule {
        AclSelectorRule::Command(rule)
    }

    fn user(rule: AclUserRule) -> AclRule {
        AclRule::User(rule)
    }

    #[test]
    fn acl_key_rules() {
        assert_eq!(
            words(vec![
                AclKeyRule::Add("a:*".to_string()),
                AclKeyRule::AddRead("r:*".to_string()),
                AclKeyRule::AddWrite("w:*".to_string()),
                AclKeyRule::AddReadWrite("rw:*".to_string()),
                AclKeyRule::All,
                AclKeyRule::Reset,
            ]),
            [
                "~a:*",
                "%R~r:*",
                "%W~w:*",
                "%RW~rw:*",
                "allkeys",
                "resetkeys"
            ]
        );
    }

    #[test]
    fn acl_channel_rules() {
        assert_eq!(
            words(vec![
                AclChannelRule::Add("news:*".to_string()),
                AclChannelRule::All,
                AclChannelRule::Reset,
            ]),
            ["&news:*", "allchannels", "resetchannels"]
        );
    }

    #[test]
    fn acl_command_rules() {
        assert_eq!(
            words(vec![
                AclCommandRule::Add("get".to_string()),
                AclCommandRule::AddCategory("read".to_string()),
                AclCommandRule::AddFirstArg(("config".to_string(), "get".to_string())),
                AclCommandRule::All,
                AclCommandRule::Remove("del".to_string()),
                AclCommandRule::RemoveCategory("dangerous".to_string()),
                AclCommandRule::Reset,
            ]),
            [
                "+get",
                "+@read",
                "+config|get",
                "allcommands",
                "-del",
                "-@dangerous",
                "nocommands"
            ]
        );
    }

    #[test]
    fn acl_db_rules() {
        assert_eq!(
            words(vec![
                AclDbRule::Db(vec![0, 2]),
                AclDbRule::All,
                AclDbRule::Reset
            ]),
            ["db=0,2", "alldbs", "resetdbs"]
        );
        assert_eq!(
            words_error(AclDbRule::Db(vec![])),
            "db requires at least one item"
        );
    }

    #[test]
    fn acl_user_rules() {
        assert_eq!(
            words(vec![
                AclUserRule::On,
                AclUserRule::Off,
                AclUserRule::Nopass,
                AclUserRule::SanitizePayload,
                AclUserRule::SkipSanitizePayload,
                AclUserRule::Password("secret".to_string()),
                AclUserRule::HashedPassword("abc".to_string()),
                AclUserRule::RemovePassword("old".to_string()),
                AclUserRule::RemoveHashedPassword("def".to_string()),
                AclUserRule::Clearselectors,
                AclUserRule::Reset,
            ]),
            [
                "on",
                "off",
                "nopass",
                "sanitize-payload",
                "skip-sanitize-payload",
                ">secret",
                "#abc",
                "<old",
                "!def",
                "clearselectors",
                "reset"
            ]
        );
    }

    #[test]
    fn acl_selector() {
        let selector = AclUserRule::Selector(vec![
            key(AclKeyRule::Add("app:*".to_string())),
            command(AclCommandRule::Add("get".to_string())),
        ]);
        assert_eq!(words(selector), ["(", "~app:*", "+get", ")"]);
    }

    #[test]
    fn acl_rules() {
        assert_eq!(
            words(vec![
                AclRule::Keys(AclKeyRule::All),
                AclRule::Channel(AclChannelRule::All),
                AclRule::Command(AclCommandRule::All),
                AclRule::Db(AclDbRule::All),
                AclRule::User(AclUserRule::On),
                AclRule::Other("newer".to_string()),
            ]),
            [
                "allkeys",
                "allchannels",
                "allcommands",
                "alldbs",
                "on",
                "newer"
            ]
        );
        assert_eq!(
            words(AclSelectorRule::Other("newer".to_string())),
            ["newer"]
        );
    }

    #[test]
    fn acl_dryrun_result() {
        assert_eq!(from::<AclDryrunResult>(ok()), AclDryrunResult::Allowed);
        assert_eq!(
            from::<AclDryrunResult>(bulk("User app has no permissions to run the 'set' command")),
            AclDryrunResult::Denied(
                "User app has no permissions to run the 'set' command".to_string()
            )
        );
    }

    #[test]
    fn acl_getuser() {
        let value = fields(vec![
            ("flags", bulks(&["on", "sanitize-payload"])),
            ("passwords", bulks(&["abc"])),
            ("commands", bulk("+@read -debug")),
            ("keys", bulk("~app:* %R~ro:*")),
            ("channels", bulk("&news:*")),
            ("databases", bulk("db=0,1")),
            (
                "selectors",
                array(vec![fields(vec![
                    ("commands", bulk("+get")),
                    ("keys", bulk("%W~w:*")),
                    ("channels", bulk("")),
                ])]),
            ),
        ]);
        assert_eq!(
            from::<AclUserRules>(value).0,
            [
                user(AclUserRule::On),
                user(AclUserRule::SanitizePayload),
                user(AclUserRule::HashedPassword("abc".to_string())),
                AclRule::Keys(AclKeyRule::Add("app:*".to_string())),
                AclRule::Keys(AclKeyRule::AddRead("ro:*".to_string())),
                AclRule::Channel(AclChannelRule::Reset),
                AclRule::Channel(AclChannelRule::Add("news:*".to_string())),
                AclRule::Db(AclDbRule::Db(vec![0, 1])),
                AclRule::Command(AclCommandRule::AddCategory("read".to_string())),
                AclRule::Command(AclCommandRule::Remove("debug".to_string())),
                user(AclUserRule::Selector(vec![
                    key(AclKeyRule::AddWrite("w:*".to_string())),
                    channel(AclChannelRule::Reset),
                    command(AclCommandRule::Add("get".to_string())),
                ])),
            ]
        );
    }

    #[test]
    fn acl_getuser_missing_field() {
        let value = fields(vec![("flags", bulks(&["on"]))]);
        assert_eq!(
            from_error::<AclUserRules>(value),
            "missing field: passwords"
        );
    }

    #[test]
    fn acl_list() {
        let value = bulks(&[
            "user default on nopass sanitize-payload ~* &* +@all",
            "user app off #abc resetchannels db=3 -@all +get (%R~x resetchannels +get) (~y +set)",
        ]);
        let entries = from::<AclList>(value).0;
        assert_eq!(
            entries,
            [
                AclListEntry {
                    username: "default".to_string(),
                    rules: vec![
                        user(AclUserRule::On),
                        user(AclUserRule::Nopass),
                        user(AclUserRule::SanitizePayload),
                        AclRule::Keys(AclKeyRule::Add("*".to_string())),
                        AclRule::Channel(AclChannelRule::Add("*".to_string())),
                        AclRule::Command(AclCommandRule::All),
                    ],
                },
                AclListEntry {
                    username: "app".to_string(),
                    rules: vec![
                        user(AclUserRule::Off),
                        user(AclUserRule::HashedPassword("abc".to_string())),
                        AclRule::Channel(AclChannelRule::Reset),
                        AclRule::Db(AclDbRule::Db(vec![3])),
                        AclRule::Command(AclCommandRule::Reset),
                        AclRule::Command(AclCommandRule::Add("get".to_string())),
                        user(AclUserRule::Selector(vec![
                            key(AclKeyRule::AddRead("x".to_string())),
                            channel(AclChannelRule::Reset),
                            command(AclCommandRule::Add("get".to_string())),
                        ])),
                        user(AclUserRule::Selector(vec![
                            key(AclKeyRule::Add("y".to_string())),
                            command(AclCommandRule::Add("set".to_string())),
                        ])),
                    ],
                },
            ]
        );
    }

    #[test]
    fn acl_list_unknown_rules() {
        let entries = from::<AclList>(bulks(&["user app on ?newer db=x"])).0;
        assert_eq!(
            entries[0].rules,
            [
                user(AclUserRule::On),
                AclRule::Other("?newer".to_string()),
                AclRule::Other("db=x".to_string()),
            ]
        );
    }

    #[test]
    fn acl_list_invalid() {
        assert_eq!(
            from_error::<AclList>(bulks(&["bogus"])),
            r#"unexpected ACL LIST line: "bogus""#
        );
        assert_eq!(
            from_error::<AclList>(bulks(&["user app (~x"])),
            r#"unexpected ACL LIST line: "user app (~x""#
        );
    }

    #[test]
    fn acl_list_round_trip() {
        let line =
            "user app on #abc ~app:* resetchannels &news:* -@all +get (%R~x resetchannels +get)";
        let entries = from::<AclList>(bulks(&[line])).0;
        let rules = entries.into_iter().next().unwrap().rules;
        // selectors are sent as words between parentheses
        assert_eq!(
            words(rules)
                .iter()
                .map(|word| String::from_utf8_lossy(word.as_bytes()))
                .collect::<Vec<_>>()
                .join(" "),
            "on #abc ~app:* resetchannels &news:* nocommands +get ( %R~x resetchannels +get )"
        );
    }

    #[test]
    fn acl_log_entry() {
        let mut entry = vec![
            ("count", int(1)),
            ("reason", bulk("command")),
            ("context", bulk("toplevel")),
            ("object", bulk("get")),
            ("username", bulk("app")),
            ("age-seconds", double(1.5)),
            ("client-info", bulk("id=3 addr=127.0.0.1:5000")),
        ];
        let expected = AclLogEntry {
            count: 1,
            reason: "command".to_string(),
            context: "toplevel".to_string(),
            object: "get".to_string(),
            username: "app".to_string(),
            age_seconds: 1.5,
            client_info: "id=3 addr=127.0.0.1:5000".to_string(),
            entry_id: None,
            timestamp_created: None,
            timestamp_last_updated: None,
        };
        // before Valkey 7.2
        assert_eq!(from::<AclLogEntry>(fields(entry.clone())), expected);
        entry.extend([
            ("entry-id", int(7)),
            ("timestamp-created", int(1_700_000_000_000)),
            ("timestamp-last-updated", int(1_700_000_000_500)),
        ]);
        assert_eq!(
            from::<AclLogEntry>(fields(entry)),
            AclLogEntry {
                entry_id: Some(7),
                timestamp_created: Some(UnixTimeMilliseconds {
                    pxat: 1_700_000_000_000
                }),
                timestamp_last_updated: Some(UnixTimeMilliseconds {
                    pxat: 1_700_000_000_500
                }),
                ..expected
            }
        );
    }

    #[test]
    fn keywords() {
        assert_eq!(
            words([BgsaveOperation::Schedule, BgsaveOperation::Cancel]),
            ["SCHEDULE", "CANCEL"]
        );
        assert_eq!(
            words([
                ClientType::Normal,
                ClientType::Primary,
                ClientType::Replica,
                ClientType::Pubsub
            ]),
            ["normal", "master", "replica", "pubsub"]
        );
        assert_eq!(
            words([ClientPauseMode::Write, ClientPauseMode::All]),
            ["WRITE", "ALL"]
        );
        assert_eq!(
            words([ClientUnblockType::Timeout, ClientUnblockType::Error]),
            ["TIMEOUT", "ERROR"]
        );
        assert_eq!(
            words([ClusterFailoverMode::Force, ClusterFailoverMode::Takeover]),
            ["FORCE", "TAKEOVER"]
        );
        assert_eq!(
            words([ClusterResetMode::Hard, ClusterResetMode::Soft]),
            ["HARD", "SOFT"]
        );
        assert_eq!(
            words([
                CommandlogType::Slow,
                CommandlogType::LargeRequest,
                CommandlogType::LargeReply
            ]),
            ["slow", "large-request", "large-reply"]
        );
        assert_eq!(
            words([ShutdownSave::Nosave, ShutdownSave::Save]),
            ["NOSAVE", "SAVE"]
        );
    }

    #[test]
    fn client_filter() {
        let filters = vec![
            ClientFilter::Id(vec![1, 2]),
            ClientFilter::Type(ClientType::Primary),
            ClientFilter::User("app".to_string()),
            ClientFilter::Addr("127.0.0.1:5000".to_string()),
            ClientFilter::Laddr("127.0.0.1:6379".to_string()),
            ClientFilter::Skipme(false),
            ClientFilter::Maxage(DurationSeconds { ex: 60 }),
            ClientFilter::Name("worker".to_string()),
            ClientFilter::Idle(DurationSeconds { ex: 5 }),
            ClientFilter::Flags("N".to_string()),
            ClientFilter::LibName("lib".to_string()),
            ClientFilter::LibVer("1.0".to_string()),
            ClientFilter::Db(1),
            ClientFilter::Capa("redirect".to_string()),
            ClientFilter::Ip("127.0.0.1".to_string()),
        ];
        assert_eq!(
            words(filters),
            [
                "ID",
                "1",
                "2",
                "TYPE",
                "master",
                "USER",
                "app",
                "ADDR",
                "127.0.0.1:5000",
                "LADDR",
                "127.0.0.1:6379",
                "SKIPME",
                "NO",
                "MAXAGE",
                "60",
                "NAME",
                "worker",
                "IDLE",
                "5",
                "FLAGS",
                "N",
                "LIB-NAME",
                "lib",
                "LIB-VER",
                "1.0",
                "DB",
                "1",
                "CAPA",
                "redirect",
                "IP",
                "127.0.0.1"
            ]
        );
        assert_eq!(words(ClientFilter::Skipme(true)), ["SKIPME", "YES"]);
        assert_eq!(
            words_error(ClientFilter::Id(vec![])),
            "id requires at least one item"
        );
    }

    #[test]
    fn client_filter_negated() {
        let filters = vec![
            ClientFilter::NotId(vec![3]),
            ClientFilter::NotType(ClientType::Pubsub),
            ClientFilter::NotUser("app".to_string()),
            ClientFilter::NotAddr("a".to_string()),
            ClientFilter::NotLaddr("l".to_string()),
            ClientFilter::NotName("n".to_string()),
            ClientFilter::NotFlags("f".to_string()),
            ClientFilter::NotLibName("ln".to_string()),
            ClientFilter::NotLibVer("lv".to_string()),
            ClientFilter::NotDb(2),
            ClientFilter::NotCapa("c".to_string()),
            ClientFilter::NotIp("i".to_string()),
        ];
        assert_eq!(
            words(filters),
            [
                "NOT-ID",
                "3",
                "NOT-TYPE",
                "pubsub",
                "NOT-USER",
                "app",
                "NOT-ADDR",
                "a",
                "NOT-LADDR",
                "l",
                "NOT-NAME",
                "n",
                "NOT-FLAGS",
                "f",
                "NOT-LIB-NAME",
                "ln",
                "NOT-LIB-VER",
                "lv",
                "NOT-DB",
                "2",
                "NOT-CAPA",
                "c",
                "NOT-IP",
                "i"
            ]
        );
        assert_eq!(
            words_error(ClientFilter::NotId(vec![])),
            "not-id requires at least one item"
        );
    }

    #[test]
    fn client_list() {
        let value = bulk("id=3 addr=127.0.0.1:5000 name=\nid=4 addr=127.0.0.1:5001 name=worker\n");
        let clients = from::<ClientList>(value).0;
        let client = |id: &str, addr: &str, name: &str| -> Map<String, String> {
            [("id", id), ("addr", addr), ("name", name)]
                .into_iter()
                .map(|(field, value)| (field.to_string(), value.to_string()))
                .collect()
        };
        assert_eq!(
            clients,
            [
                client("3", "127.0.0.1:5000", ""),
                client("4", "127.0.0.1:5001", "worker")
            ]
        );
        assert!(from::<ClientList>(bulk("")).0.is_empty());
    }

    #[test]
    fn cluster_bumpepoch_result() {
        assert_eq!(
            from::<ClusterBumpepochResult>(simple("BUMPED 5")),
            ClusterBumpepochResult::Bumped(5)
        );
        assert_eq!(
            from::<ClusterBumpepochResult>(simple("STILL 3")),
            ClusterBumpepochResult::Still(3)
        );
        assert_eq!(
            from_error::<ClusterBumpepochResult>(simple("STILL")),
            r#"unexpected reply: "STILL""#
        );
        assert_eq!(
            from_error::<ClusterBumpepochResult>(simple("OTHER 1")),
            r#"unexpected reply: "OTHER 1""#
        );
    }

    #[test]
    fn cluster_getslotmigrations_entry() {
        let value = fields(vec![
            ("link_name", bulk("abc")),
            ("operation", bulk("EXPORT")),
            ("slot_ranges", bulk("0-100 200-200")),
            ("target_node", bulk("target")),
            ("source_node", bulk("source")),
            ("create_time", int(1_700_000_000)),
            ("last_update_time", int(1_700_000_001)),
            ("last_ack_time", int(1_700_000_002)),
            ("state", bulk("success")),
            ("message", bulk("")),
            ("cow_size", int(0)),
        ]);
        assert_eq!(
            from::<ClusterGetslotmigrationsEntry>(value),
            ClusterGetslotmigrationsEntry {
                link_name: "abc".to_string(),
                operation: ClusterGetslotmigrationsOperation::Export,
                slot_ranges: vec![(0, 100), (200, 200)],
                target_node: "target".to_string(),
                source_node: "source".to_string(),
                create_time: UnixTimeSeconds {
                    exat: 1_700_000_000
                },
                last_update_time: UnixTimeSeconds {
                    exat: 1_700_000_001
                },
                last_ack_time: UnixTimeSeconds {
                    exat: 1_700_000_002
                },
                state: "success".to_string(),
                message: String::new(),
                cow_size: 0,
            }
        );
    }

    #[test]
    fn cluster_getslotmigrations_operation() {
        assert_eq!(
            from::<ClusterGetslotmigrationsOperation>(bulk("IMPORT")),
            ClusterGetslotmigrationsOperation::Import
        );
        assert_eq!(
            from_error::<ClusterGetslotmigrationsOperation>(bulk("MOVE")),
            r#"unexpected operation: "MOVE""#
        );
    }

    #[test]
    fn slot_ranges() {
        assert_eq!(from::<SlotRanges>(bulk("")).0, []);
        assert_eq!(
            from_error::<SlotRanges>(bulk("0-100 200")),
            r#"unexpected slot ranges: "0-100 200""#
        );
        assert_eq!(
            from_error::<SlotRanges>(bulk("0-x")),
            r#"unexpected slot ranges: "0-x""#
        );
    }

    #[test]
    fn cluster_migrateslots_group() {
        let group = ClusterMigrateslotsGroup {
            ranges: vec![(0, 100), (200, 300)],
            node_id: "node".to_string(),
            auth: Some(("user".to_string(), "secret".to_string())),
        };
        assert_eq!(
            words(group),
            [
                "SLOTSRANGE",
                "0",
                "100",
                "200",
                "300",
                "NODE",
                "node",
                "AUTH",
                "user",
                "secret"
            ]
        );
        let group = ClusterMigrateslotsGroup {
            ranges: vec![],
            node_id: "node".to_string(),
            auth: None,
        };
        assert_eq!(words_error(group), "ranges requires at least one item");
    }

    #[test]
    fn cluster_setslot_state() {
        assert_eq!(
            words(vec![
                ClusterSetslotState::Importing("a".to_string()),
                ClusterSetslotState::Migrating("b".to_string()),
                ClusterSetslotState::Node("c".to_string()),
                ClusterSetslotState::Stable,
            ]),
            ["IMPORTING", "a", "MIGRATING", "b", "NODE", "c", "STABLE"]
        );
    }

    #[test]
    fn commandlog_entry() {
        let value = array(vec![
            int(1),
            int(1_700_000_000),
            int(15),
            bulks(&["GET", "key"]),
            bulk("127.0.0.1:5000"),
            bulk(""),
        ]);
        assert_eq!(
            from::<CommandlogEntry>(value),
            CommandlogEntry {
                id: 1,
                timestamp: UnixTimeSeconds {
                    exat: 1_700_000_000
                },
                value: 15,
                arguments: vec!["GET".to_string(), "key".to_string()],
                client_address: "127.0.0.1:5000".to_string(),
                client_name: String::new(),
            }
        );
        assert_eq!(
            from_error::<CommandlogEntry>(array(vec![int(1)])),
            "unexpected number of items, expected 6: 1"
        );
    }

    #[test]
    fn failover_target() {
        let target = |force| FailoverTarget {
            host: "replica".to_string(),
            port: 6380,
            force,
        };
        assert_eq!(words(target(true)), ["TO", "replica", "6380", "FORCE"]);
        assert_eq!(words(target(false)), ["TO", "replica", "6380"]);
    }

    #[test]
    fn latency_histogram_entry() {
        let value = fields(vec![
            ("calls", int(3)),
            (
                "histogram_usec",
                crate::test_util::map(vec![(int(1), int(2)), (int(4), int(3))]),
            ),
        ]);
        assert_eq!(
            from::<LatencyHistogramEntry>(value),
            LatencyHistogramEntry {
                calls: 3,
                histogram_usec: vec![(1, 2), (4, 3)],
            }
        );
    }

    #[test]
    fn latency_latest_entry() {
        let value = array(vec![
            bulk("command"),
            int(1_700_000_000),
            int(250),
            int(500),
            int(1000),
            int(4),
        ]);
        assert_eq!(
            from::<LatencyLatestEntry>(value),
            LatencyLatestEntry {
                event: "command".to_string(),
                timestamp: UnixTimeSeconds {
                    exat: 1_700_000_000
                },
                latest: DurationMilliseconds { px: 250 },
                max: DurationMilliseconds { px: 500 },
                sum: DurationMilliseconds { px: 1000 },
                count: 4,
            }
        );
        assert_eq!(
            from_error::<LatencyLatestEntry>(array(vec![])),
            "unexpected number of items, expected 6: 0"
        );
    }

    #[test]
    fn module_list_entry() {
        let value = fields(vec![
            ("name", bulk("lua")),
            ("ver", int(1)),
            ("path", bulk("lua")),
            ("args", array(vec![])),
        ]);
        assert_eq!(
            from::<ModuleListEntry>(value),
            ModuleListEntry {
                name: "lua".to_string(),
                ver: 1,
                path: "lua".to_string(),
                args: vec![],
            }
        );
    }

    #[test]
    fn role_primary() {
        let value = array(vec![
            bulk("master"),
            int(3129659),
            array(vec![bulks(&["127.0.0.1", "9001", "3129242"])]),
        ]);
        assert_eq!(
            from::<RoleReply>(value),
            RoleReply::Primary(RolePrimary {
                offset: 3129659,
                replicas: vec![RolePrimaryReplica {
                    ip: "127.0.0.1".to_string(),
                    port: 9001,
                    offset: 3129242,
                }],
            })
        );
    }

    #[test]
    fn role_replica() {
        let value = array(vec![
            bulk("slave"),
            bulk("127.0.0.1"),
            int(9000),
            bulk("connected"),
            int(3167038),
        ]);
        assert_eq!(
            from::<RoleReply>(value),
            RoleReply::Replica(RoleReplica {
                primary_host: "127.0.0.1".to_string(),
                primary_port: 9000,
                state: "connected".to_string(),
                offset: 3167038,
            })
        );
    }

    #[test]
    fn role_sentinel() {
        let value = array(vec![bulk("sentinel"), bulks(&["primary"])]);
        assert_eq!(
            from::<RoleReply>(value),
            RoleReply::Sentinel(vec!["primary".to_string()])
        );
    }

    #[test]
    fn role_invalid() {
        assert_eq!(
            from_error::<RoleReply>(array(vec![bulk("master"), int(1)])),
            r#"too few items for the role "master""#
        );
        assert_eq!(
            from_error::<RoleReply>(array(vec![bulk("leader")])),
            r#"unexpected role: "leader""#
        );
        assert_eq!(
            from_error::<RoleReply>(array(vec![
                bulk("slave"),
                bulk("h"),
                int(70000),
                bulk("connected"),
                int(1),
            ])),
            "unexpected port"
        );
        assert_eq!(
            from_error::<RolePrimaryReplica>(bulks(&["127.0.0.1", "port", "1"])),
            r#"unexpected number: "port""#
        );
    }
}
