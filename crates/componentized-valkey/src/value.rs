use crate::types::{
    AclChannelRule, AclCommandRule, AclDbRule, AclDryrunResult, AclKeyRule, AclListEntry,
    AclLogEntry, AclRule, AclSelectorRule, AclUserRule, ClientTrackingRedirect,
    ClientTrackinginfoReply, ClusterBumpepochResult, ClusterGetslotmigrationsEntry,
    ClusterGetslotmigrationsOperation, CommandDocsArgument, CommandDocsArgumentType,
    CommandDocsReply, CommandInfoReply, CommandKeySpec, CommandKeySpecBeginSearch,
    CommandKeySpecFindKeys, CommandKeySpecKeynum, CommandKeySpecKeyword, CommandKeySpecRange,
    CommandlogEntry, Cursor, Double, DurationMilliseconds, DurationSeconds, Error, ExpiretimeReply,
    FunctionListFunction, FunctionListLibrary, FunctionStatsEngine, FunctionStatsReply,
    FunctionStatsRunning, GeoCoordinates, GeosearchWithResult, HexpireResult, HpersistResult,
    Integer, KeyType, LatencyHistogramEntry, LatencyLatestEntry, LcsIdxMatch, LcsIdxReply,
    LinsertResult, ModuleListEntry, PexpiretimeReply, PttlReply, RespNestedValue, RespValue,
    RolePrimary, RolePrimaryReplica, RoleReplica, RoleReply, StreamId, TtlReply,
    UnixTimeMilliseconds, UnixTimeSeconds, XinfoConsumersEntry, XinfoGroupsEntry, XinfoStreamReply,
    XinfoStreamReplyFull, XinfoStreamReplyFullConsumer, XinfoStreamReplyFullGroup,
    XpendingExtendedEntry, XpendingSummary,
};

use wit_bindgen::rt::Map;

/// Converts a reply into a Rust type, e.g. `Option<String>` for a bulk string or null.
pub trait FromValue: Sized {
    fn from_value(value: RespValue) -> Result<Self, Error>;
}

/// The reply itself, e.g. for a field of mixed types.
impl FromValue for RespValue {
    fn from_value(value: RespValue) -> Result<Self, Error> {
        Ok(value)
    }
}

/// A simple string `OK`.
impl FromValue for () {
    fn from_value(value: RespValue) -> Result<Self, Error> {
        match &value {
            RespValue::String(reply) if reply == "OK" => Ok(()),
            _ => Err(unexpected(&value)),
        }
    }
}

impl FromValue for Integer {
    fn from_value(value: RespValue) -> Result<Self, Error> {
        match value {
            RespValue::Integer(value) => Ok(value),
            value => Err(unexpected(&value)),
        }
    }
}

/// A double, or a bulk string holding one, e.g. the reply of INCRBYFLOAT, even with RESP3.
impl FromValue for Double {
    fn from_value(value: RespValue) -> Result<Self, Error> {
        match value {
            RespValue::Double(value) => Ok(value),
            RespValue::BulkString(text) => text
                .parse()
                .map_err(|_| Error::Client(format!("unexpected reply, not a double: {text:?}"))),
            value => Err(unexpected(&value)),
        }
    }
}

/// A non-negative integer.
impl FromValue for u64 {
    fn from_value(value: RespValue) -> Result<Self, Error> {
        let value = Integer::from_value(value)?;
        u64::try_from(value)
            .map_err(|_| Error::Client(format!("unexpected negative reply: {value}")))
    }
}

/// An integer `0` or `1`, or a boolean.
impl FromValue for bool {
    fn from_value(value: RespValue) -> Result<Self, Error> {
        match value {
            RespValue::Integer(0) => Ok(false),
            RespValue::Integer(1) => Ok(true),
            RespValue::Boolean(value) => Ok(value),
            value => Err(unexpected(&value)),
        }
    }
}

/// A bulk, simple or verbatim string.
impl FromValue for String {
    fn from_value(value: RespValue) -> Result<Self, Error> {
        match value {
            RespValue::BulkString(value) | RespValue::String(value) => Ok(value),
            RespValue::VerbatimString((_, value)) => Ok(value),
            value => Err(unexpected(&value)),
        }
    }
}

/// Null, or a value.
impl<T: FromValue> FromValue for Option<T> {
    fn from_value(value: RespValue) -> Result<Self, Error> {
        match value {
            RespValue::Null => Ok(None),
            value => T::from_value(value).map(Some),
        }
    }
}

/// An array, or a set.
impl<T: FromValue> FromValue for Vec<T> {
    fn from_value(value: RespValue) -> Result<Self, Error> {
        items(value)?.into_iter().map(T::from_value).collect()
    }
}

/// An array of exactly as many items as the tuple.
macro_rules! from_value_tuples {
    ($(($len:literal; $($item:ident),+)),*) => {
        $(
            impl<$($item: FromValue),+> FromValue for ($($item,)+) {
                #[allow(non_snake_case)]
                fn from_value(value: RespValue) -> Result<Self, Error> {
                    match <[RespValue; $len]>::try_from(items(value)?) {
                        Ok([$($item),+]) => Ok(($($item::from_value($item)?,)+)),
                        Err(items) => Err(Error::Client(format!(
                            "unexpected number of items, expected {}: {}",
                            $len,
                            items.len()
                        ))),
                    }
                }
            }
        )*
    };
}

from_value_tuples!((2; A, B), (3; A, B, C), (4; A, B, C, D));

/// A map, or a flat array of keys and values.
impl<K: FromValue + Ord, V: FromValue> FromValue for Map<K, V> {
    fn from_value(value: RespValue) -> Result<Self, Error> {
        Ok(entries(value)?.into_iter().collect())
    }
}

/// The fields of a map, by name, e.g. to build a record.
///
/// Fields that are not taken are ignored, so newer servers may add fields.
pub struct Fields(Vec<(String, RespValue)>);

impl FromValue for Fields {
    fn from_value(value: RespValue) -> Result<Self, Error> {
        entries(value).map(Fields)
    }
}

impl Fields {
    /// Take the named field, if present.
    pub fn optional<T: FromValue>(&mut self, name: &str) -> Result<Option<T>, Error> {
        match self.0.iter().position(|(key, _)| key == name) {
            Some(index) => T::from_value(self.0.swap_remove(index).1).map(Some),
            None => Ok(None),
        }
    }

    /// Take the named field, failing if it is missing.
    pub fn required<T: FromValue>(&mut self, name: &str) -> Result<T, Error> {
        self.optional(name)?
            .ok_or_else(|| Error::Client(format!("missing field: {name}")))
    }
}

/// Whole seconds, never negative.
impl FromValue for DurationSeconds {
    fn from_value(value: RespValue) -> Result<Self, Error> {
        u64::from_value(value).map(|ex| DurationSeconds { ex })
    }
}

/// Whole milliseconds, never negative.
impl FromValue for DurationMilliseconds {
    fn from_value(value: RespValue) -> Result<Self, Error> {
        u64::from_value(value).map(|px| DurationMilliseconds { px })
    }
}

/// Whole seconds since the Unix epoch.
impl FromValue for UnixTimeSeconds {
    fn from_value(value: RespValue) -> Result<Self, Error> {
        Integer::from_value(value).map(|exat| UnixTimeSeconds { exat })
    }
}

/// Whole milliseconds since the Unix epoch.
impl FromValue for UnixTimeMilliseconds {
    fn from_value(value: RespValue) -> Result<Self, Error> {
        Integer::from_value(value).map(|pxat| UnixTimeMilliseconds { pxat })
    }
}

/// A value, or `-1` for none, e.g. the `active-time` of a consumer that has never been active.
pub struct OrNever<T>(pub Option<T>);

impl<T: FromValue> FromValue for OrNever<T> {
    fn from_value(value: RespValue) -> Result<Self, Error> {
        match value {
            RespValue::Integer(-1) => Ok(OrNever(None)),
            value => T::from_value(value).map(|value| OrNever(Some(value))),
        }
    }
}

/// A stream ID, or `0-0` for none, e.g. the start of the next XAUTOCLAIM, `0-0` once the scan is
/// complete.
pub struct OrZeroId(pub Option<StreamId>);

pub const ZERO_ID: &str = "0-0";

impl FromValue for OrZeroId {
    fn from_value(value: RespValue) -> Result<Self, Error> {
        let id = StreamId::from_value(value)?;
        Ok(OrZeroId((id != ZERO_ID).then_some(id)))
    }
}

/// `[count, lowest-id, highest-id, [[consumer, count]...]]`, the IDs and consumers are null when
/// nothing is pending, and each consumer's count is a bulk string.
impl FromValue for XpendingSummary {
    fn from_value(value: RespValue) -> Result<Self, Error> {
        let (count, lowest_id, highest_id, consumers): (_, _, _, Option<Vec<(String, String)>>) =
            FromValue::from_value(value)?;
        let consumers = consumers
            .unwrap_or_default()
            .into_iter()
            .map(|(consumer, count)| {
                let count = count.parse().map_err(|_| {
                    Error::Client(format!(
                        "unexpected pending count, not an integer: {count:?}"
                    ))
                })?;
                Ok((consumer, count))
            })
            .collect::<Result<_, Error>>()?;
        Ok(Self {
            count,
            lowest_id,
            highest_id,
            consumers,
        })
    }
}

/// `[id, consumer, milliseconds-since-delivery, delivery-count]`.
impl FromValue for XpendingExtendedEntry {
    fn from_value(value: RespValue) -> Result<Self, Error> {
        let (id, name, since_delivery, delivery_count) = FromValue::from_value(value)?;
        Ok(Self {
            id,
            name,
            since_delivery,
            delivery_count,
        })
    }
}

/// Members and their scores, e.g. from ZPOPMIN, which replies with one pair, or none, without a
/// count, and a list of pairs with one.
pub struct ScoredMembers(pub Vec<(String, Double)>);

impl FromValue for ScoredMembers {
    fn from_value(value: RespValue) -> Result<Self, Error> {
        let items = items(value)?;
        match items.first() {
            None => Ok(ScoredMembers(vec![])),
            Some(RespValue::Array(_)) => items
                .into_iter()
                .map(FromValue::from_value)
                .collect::<Result<_, _>>()
                .map(ScoredMembers),
            Some(_) => match <[RespValue; 2]>::try_from(items) {
                Ok([member, score]) => Ok(ScoredMembers(vec![(
                    String::from_value(member)?,
                    Double::from_value(score)?,
                )])),
                Err(items) => Err(Error::Client(format!(
                    "unexpected number of items, expected a member and its score: {}",
                    items.len()
                ))),
            },
        }
    }
}

/// Pairs in a flat array, `[key, value, key, value...]`, e.g. the members and scores of ZSCAN.
pub struct FlatPairs<K, V>(pub Vec<(K, V)>);

impl<K: FromValue, V: FromValue> FromValue for FlatPairs<K, V> {
    fn from_value(value: RespValue) -> Result<Self, Error> {
        entries(value).map(FlatPairs)
    }
}

/// The codes of HTTL and the like, `-2` when the field does not exist, `-1` when it has no
/// expiration, otherwise the time.
macro_rules! from_value_field_times {
    ($($variant:ident($time:ident) => $case:ident),*) => {
        $(
            impl FromValue for $variant {
                fn from_value(value: RespValue) -> Result<Self, Error> {
                    match value {
                        RespValue::Integer(-2) => Ok($variant::DoesNotExist),
                        RespValue::Integer(-1) => Ok($variant::NoExpiration),
                        value => $time::from_value(value).map($variant::$case),
                    }
                }
            }
        )*
    };
}

from_value_field_times!(
    TtlReply(DurationSeconds) => ExpiresIn,
    PttlReply(DurationMilliseconds) => ExpiresIn,
    ExpiretimeReply(UnixTimeSeconds) => ExpiresAt,
    PexpiretimeReply(UnixTimeMilliseconds) => ExpiresAt
);

impl FromValue for HpersistResult {
    fn from_value(value: RespValue) -> Result<Self, Error> {
        match Integer::from_value(value)? {
            -2 => Ok(Self::DoesNotExist),
            -1 => Ok(Self::NoExpiration),
            1 => Ok(Self::Removed),
            code => Err(Error::Client(format!("unexpected reply: {code}"))),
        }
    }
}

/// The cursor a scan returns to resume from, none once the scan is complete, `0`.
pub struct NextCursor(pub Option<Cursor>);

impl FromValue for NextCursor {
    fn from_value(value: RespValue) -> Result<Self, Error> {
        let cursor = String::from_value(value)?;
        Ok(NextCursor(
            (cursor != "0").then_some(Cursor::Resume(cursor)),
        ))
    }
}

/// The length of the list after the insert, `0` when the key does not exist, `-1` when the pivot
/// was not found.
impl FromValue for LinsertResult {
    fn from_value(value: RespValue) -> Result<Self, Error> {
        match Integer::from_value(value)? {
            -1 => Ok(Self::PivotNotFound),
            0 => Ok(Self::NoSuchKey),
            length if length > 0 => Ok(Self::Inserted(length)),
            code => Err(Error::Client(format!("unexpected reply: {code}"))),
        }
    }
}

/// `[longitude, latitude]`
impl FromValue for GeoCoordinates {
    fn from_value(value: RespValue) -> Result<Self, Error> {
        let (longitude, latitude) = FromValue::from_value(value)?;
        Ok(GeoCoordinates {
            longitude,
            latitude,
        })
    }
}

/// The member, or with any of WITHDIST, WITHHASH and WITHCOORD, an array of the member then the
/// details, in that order, told apart by their types.
impl FromValue for GeosearchWithResult {
    fn from_value(value: RespValue) -> Result<Self, Error> {
        let mut result = GeosearchWithResult {
            member: String::new(),
            distance: None,
            hash: None,
            coordinates: None,
        };
        let items = match value {
            RespValue::Array(_) => items(value)?,
            // without details, the member alone
            value => vec![value],
        };
        let mut items = items.into_iter();
        result.member = String::from_value(items.next().unwrap_or(RespValue::Null))?;
        for item in items {
            match item {
                // a bulk string, as RESP3 has no other way to send a fixed precision double
                RespValue::BulkString(_) => result.distance = Some(Double::from_value(item)?),
                RespValue::Integer(hash) => result.hash = Some(hash),
                RespValue::Array(_) => result.coordinates = Some(GeoCoordinates::from_value(item)?),
                item => return Err(unexpected(&item)),
            }
        }
        Ok(result)
    }
}

impl FromValue for FunctionListFunction {
    fn from_value(value: RespValue) -> Result<Self, Error> {
        let mut fields = Fields::from_value(value)?;
        Ok(Self {
            name: fields.required("name")?,
            description: fields.required("description")?,
            flags: fields.required("flags")?,
        })
    }
}

impl FromValue for FunctionListLibrary {
    fn from_value(value: RespValue) -> Result<Self, Error> {
        let mut fields = Fields::from_value(value)?;
        Ok(Self {
            library_name: fields.required("library_name")?,
            engine: fields.required("engine")?,
            functions: fields.required("functions")?,
            // only with WITHCODE
            library_code: fields.optional("library_code")?,
        })
    }
}

impl FromValue for FunctionStatsRunning {
    fn from_value(value: RespValue) -> Result<Self, Error> {
        let mut fields = Fields::from_value(value)?;
        Ok(Self {
            name: fields.required("name")?,
            command: fields.required("command")?,
            duration: fields.required("duration_ms")?,
        })
    }
}

impl FromValue for FunctionStatsEngine {
    fn from_value(value: RespValue) -> Result<Self, Error> {
        let mut fields = Fields::from_value(value)?;
        Ok(Self {
            libraries_count: fields.required("libraries_count")?,
            functions_count: fields.required("functions_count")?,
        })
    }
}

impl FromValue for FunctionStatsReply {
    fn from_value(value: RespValue) -> Result<Self, Error> {
        let mut fields = Fields::from_value(value)?;
        Ok(Self {
            running_script: fields.required("running_script")?,
            engines: fields.required("engines")?,
        })
    }
}

/// The type of a key, by name, none for `none`, a key that does not exist.
pub struct OptionalKeyType(pub Option<KeyType>);

impl FromValue for OptionalKeyType {
    fn from_value(value: RespValue) -> Result<Self, Error> {
        // `none` rather than null, though the reply schema has null
        let name: Option<String> = FromValue::from_value(value)?;
        Ok(OptionalKeyType(match name.as_deref() {
            None | Some("none") => None,
            Some("string") => Some(KeyType::String),
            Some("list") => Some(KeyType::List),
            Some("set") => Some(KeyType::Set),
            Some("zset") => Some(KeyType::Zset),
            Some("hash") => Some(KeyType::Hash),
            Some("stream") => Some(KeyType::Stream),
            Some(_) => name.map(KeyType::Other),
        }))
    }
}

/// `OK` when keys were moved, `NOKEY` when none of them exist.
pub struct Migrated(pub bool);

impl FromValue for Migrated {
    fn from_value(value: RespValue) -> Result<Self, Error> {
        match &value {
            RespValue::String(reply) if reply == "OK" => Ok(Migrated(true)),
            RespValue::String(reply) if reply == "NOKEY" => Ok(Migrated(false)),
            _ => Err(unexpected(&value)),
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

/// A number sent as a string, e.g. the port of a replica in ROLE.
fn parse_string<T: std::str::FromStr>(value: RespValue) -> Result<T, Error> {
    let text = String::from_value(value)?;
    text.parse()
        .map_err(|_| Error::Client(format!("unexpected number: {text:?}")))
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

/// `-1` when tracking is off, `0` when not redirected, otherwise the ID of the connection.
impl FromValue for ClientTrackingRedirect {
    fn from_value(value: RespValue) -> Result<Self, Error> {
        match Integer::from_value(value)? {
            -1 => Ok(ClientTrackingRedirect::NotTracking),
            0 => Ok(ClientTrackingRedirect::NotRedirected),
            id if id > 0 => Ok(ClientTrackingRedirect::Redirected(id)),
            code => Err(Error::Client(format!("unexpected reply: {code}"))),
        }
    }
}

impl FromValue for ClientTrackinginfoReply {
    fn from_value(value: RespValue) -> Result<Self, Error> {
        let mut fields = Fields::from_value(value)?;
        Ok(Self {
            flags: fields.required("flags")?,
            redirect: fields.required("redirect")?,
            prefixes: fields.required("prefixes")?,
        })
    }
}

/// The entries of COMMAND and COMMAND INFO, each followed by its subcommands, nulls, for commands
/// that do not exist, left out.
pub struct CommandInfoList(pub Vec<CommandInfoReply>);

impl FromValue for CommandInfoList {
    fn from_value(value: RespValue) -> Result<Self, Error> {
        let mut commands = vec![];
        for command in items(value)? {
            command_info(command, &mut commands)?;
        }
        Ok(CommandInfoList(commands))
    }
}

/// `[name, arity, flags, first-key, last-key, step, acl-categories, tips, key-specs, subcommands]`,
/// the ACL categories since 6.0, the rest since 7.0.
fn command_info(value: RespValue, commands: &mut Vec<CommandInfoReply>) -> Result<(), Error> {
    if value == RespValue::Null {
        return Ok(());
    }
    let mut items = items(value)?.into_iter();
    let mut next = || {
        items
            .next()
            .ok_or_else(|| Error::Client("too few items for a command".to_string()))
    };
    let info = CommandInfoReply {
        name: FromValue::from_value(next()?)?,
        arity: FromValue::from_value(next()?)?,
        flags: FromValue::from_value(next()?)?,
        first_key: FromValue::from_value(next()?)?,
        last_key: FromValue::from_value(next()?)?,
        step: FromValue::from_value(next()?)?,
        acl_categories: next().map_or(Ok(vec![]), FromValue::from_value)?,
        tips: next().map_or(Ok(vec![]), FromValue::from_value)?,
        key_specs: next().map_or(Ok(vec![]), FromValue::from_value)?,
    };
    let subcommands: Vec<RespValue> = next().map_or(Ok(vec![]), FromValue::from_value)?;
    commands.push(info);
    for subcommand in subcommands {
        command_info(subcommand, commands)?;
    }
    Ok(())
}

impl FromValue for CommandKeySpec {
    fn from_value(value: RespValue) -> Result<Self, Error> {
        let mut fields = Fields::from_value(value)?;
        let mut begin_search: Fields = fields.required("begin_search")?;
        let mut find_keys: Fields = fields.required("find_keys")?;
        Ok(Self {
            notes: fields.optional("notes")?,
            flags: fields.required("flags")?,
            begin_search: begin_search_spec(&mut begin_search)?,
            find_keys: find_keys_spec(&mut find_keys)?,
        })
    }
}

/// `{type, spec}`, a type this interface does not know is `unknown`.
fn begin_search_spec(fields: &mut Fields) -> Result<CommandKeySpecBeginSearch, Error> {
    let kind: String = fields.required("type")?;
    let mut spec: Fields = fields.required("spec")?;
    Ok(match kind.as_str() {
        "index" => CommandKeySpecBeginSearch::Index(spec.required("index")?),
        "keyword" => CommandKeySpecBeginSearch::Keyword(CommandKeySpecKeyword {
            keyword: spec.required("keyword")?,
            startfrom: spec.required("startfrom")?,
        }),
        _ => CommandKeySpecBeginSearch::Unknown,
    })
}

/// `{type, spec}`, a type this interface does not know is `unknown`.
fn find_keys_spec(fields: &mut Fields) -> Result<CommandKeySpecFindKeys, Error> {
    let kind: String = fields.required("type")?;
    let mut spec: Fields = fields.required("spec")?;
    Ok(match kind.as_str() {
        "range" => CommandKeySpecFindKeys::Range(CommandKeySpecRange {
            lastkey: spec.required("lastkey")?,
            keystep: spec.required("keystep")?,
            limit: spec.required("limit")?,
        }),
        "keynum" => CommandKeySpecFindKeys::Keynum(CommandKeySpecKeynum {
            keynumidx: spec.required("keynumidx")?,
            firstkey: spec.required("firstkey")?,
            keystep: spec.required("keystep")?,
        }),
        _ => CommandKeySpecFindKeys::Unknown,
    })
}

/// The documentation of COMMAND DOCS, a map by name, each followed by its subcommands.
pub struct CommandDocsList(pub Vec<CommandDocsReply>);

impl FromValue for CommandDocsList {
    fn from_value(value: RespValue) -> Result<Self, Error> {
        let mut commands = vec![];
        for (name, doc) in entries::<String, Fields>(value)? {
            command_doc(name, doc, &mut commands)?;
        }
        Ok(CommandDocsList(commands))
    }
}

fn command_doc(
    name: String,
    mut fields: Fields,
    commands: &mut Vec<CommandDocsReply>,
) -> Result<(), Error> {
    let mut arguments = vec![];
    let nested: Option<Vec<Fields>> = fields.optional("arguments")?;
    command_arguments(nested.unwrap_or_default(), None, &mut arguments)?;
    commands.push(CommandDocsReply {
        name,
        summary: fields.required("summary")?,
        since: fields.required("since")?,
        group: fields.required("group")?,
        complexity: fields.optional("complexity")?,
        module: fields.optional("module")?,
        doc_flags: fields.optional("doc_flags")?.unwrap_or_default(),
        deprecated_since: fields.optional("deprecated_since")?,
        replaced_by: fields.optional("replaced_by")?,
        history: fields.optional("history")?.unwrap_or_default(),
        arguments,
    });
    let subcommands: Option<Vec<(String, Fields)>> = fields
        .optional::<FlatPairs<String, Fields>>("subcommands")?
        .map(|subcommands| subcommands.0);
    for (name, doc) in subcommands.unwrap_or_default() {
        command_doc(name, doc, commands)?;
    }
    Ok(())
}

/// The arguments, depth first, each followed by those it contains, which point to it as their
/// parent.
fn command_arguments(
    nested: Vec<Fields>,
    parent: Option<u32>,
    arguments: &mut Vec<CommandDocsArgument>,
) -> Result<(), Error> {
    for mut fields in nested {
        let index = u32::try_from(arguments.len())
            .map_err(|_| Error::Client("too many arguments".to_string()))?;
        let kind: String = fields.required("type")?;
        arguments.push(CommandDocsArgument {
            parent,
            name: fields.required("name")?,
            type_: argument_type(kind),
            display_text: fields.optional("display_text")?,
            key_spec_index: fields.optional("key_spec_index")?,
            token: fields.optional("token")?,
            summary: fields.optional("summary")?,
            since: fields.optional("since")?,
            deprecated_since: fields.optional("deprecated_since")?,
            flags: fields.optional("flags")?.unwrap_or_default(),
        });
        let contained: Option<Vec<Fields>> = fields.optional("arguments")?;
        command_arguments(contained.unwrap_or_default(), Some(index), arguments)?;
    }
    Ok(())
}

fn argument_type(kind: String) -> CommandDocsArgumentType {
    match kind.as_str() {
        "string" => CommandDocsArgumentType::String,
        "integer" => CommandDocsArgumentType::Integer,
        "double" => CommandDocsArgumentType::Double,
        "key" => CommandDocsArgumentType::Key,
        "pattern" => CommandDocsArgumentType::Pattern,
        "unix-time" => CommandDocsArgumentType::UnixTime,
        "pure-token" => CommandDocsArgumentType::PureToken,
        "oneof" => CommandDocsArgumentType::Oneof,
        "block" => CommandDocsArgumentType::Block,
        _ => CommandDocsArgumentType::Other(kind),
    }
}

impl FromValue for HexpireResult {
    fn from_value(value: RespValue) -> Result<Self, Error> {
        match Integer::from_value(value)? {
            -2 => Ok(Self::DoesNotExist),
            0 => Ok(Self::ConditionNotMet),
            1 => Ok(Self::Applied),
            2 => Ok(Self::ZeroSeconds),
            code => Err(Error::Client(format!("unexpected reply: {code}"))),
        }
    }
}

/// A map of the matches and the length of the longest common subsequence.
impl FromValue for LcsIdxReply {
    fn from_value(value: RespValue) -> Result<Self, Error> {
        let mut fields = Fields::from_value(value)?;
        Ok(Self {
            matches: fields.required("matches")?,
            len: fields.required("len")?,
        })
    }
}

/// The inclusive ranges of a match in each key and, with `WITHMATCHLEN`, its length.
impl FromValue for LcsIdxMatch {
    fn from_value(value: RespValue) -> Result<Self, Error> {
        let mut items = items(value)?.into_iter();
        let (Some(key1_range), Some(key2_range), len, None) =
            (items.next(), items.next(), items.next(), items.next())
        else {
            return Err(Error::Client(
                "unexpected LCS match, expected 2 or 3 items".to_string(),
            ));
        };
        Ok(Self {
            key1_range: FromValue::from_value(key1_range)?,
            key2_range: FromValue::from_value(key2_range)?,
            len: len.map(FromValue::from_value).transpose()?,
        })
    }
}

impl FromValue for XinfoConsumersEntry {
    fn from_value(value: RespValue) -> Result<Self, Error> {
        let mut fields = Fields::from_value(value)?;
        Ok(Self {
            name: fields.required("name")?,
            pending: fields.required("pending")?,
            idle: fields.required("idle")?,
            inactive: fields
                .required::<OrNever<DurationMilliseconds>>("inactive")?
                .0,
        })
    }
}

impl FromValue for XinfoGroupsEntry {
    fn from_value(value: RespValue) -> Result<Self, Error> {
        let mut fields = Fields::from_value(value)?;
        Ok(Self {
            name: fields.required("name")?,
            consumers: fields.required("consumers")?,
            pending: fields.required("pending")?,
            last_delivered_id: fields.required("last-delivered-id")?,
            entries_read: fields.required("entries-read")?,
            lag: fields.required("lag")?,
        })
    }
}

impl FromValue for XinfoStreamReply {
    fn from_value(value: RespValue) -> Result<Self, Error> {
        let mut fields = Fields::from_value(value)?;
        Ok(Self {
            length: fields.required("length")?,
            radix_tree_keys: fields.required("radix-tree-keys")?,
            radix_tree_nodes: fields.required("radix-tree-nodes")?,
            groups: fields.required("groups")?,
            last_generated_id: fields.required::<OrZeroId>("last-generated-id")?.0,
            max_deleted_entry_id: fields.required::<OrZeroId>("max-deleted-entry-id")?.0,
            entries_added: fields.required("entries-added")?,
            first_entry: fields.required("first-entry")?,
            last_entry: fields.required("last-entry")?,
        })
    }
}

impl FromValue for XinfoStreamReplyFull {
    fn from_value(value: RespValue) -> Result<Self, Error> {
        let mut fields = Fields::from_value(value)?;
        Ok(Self {
            length: fields.required("length")?,
            radix_tree_keys: fields.required("radix-tree-keys")?,
            radix_tree_nodes: fields.required("radix-tree-nodes")?,
            last_generated_id: fields.required::<OrZeroId>("last-generated-id")?.0,
            max_deleted_entry_id: fields.required::<OrZeroId>("max-deleted-entry-id")?.0,
            entries_added: fields.required("entries-added")?,
            entries: fields.required("entries")?,
            groups: fields.required("groups")?,
        })
    }
}

impl FromValue for XinfoStreamReplyFullGroup {
    fn from_value(value: RespValue) -> Result<Self, Error> {
        let mut fields = Fields::from_value(value)?;
        Ok(Self {
            name: fields.required("name")?,
            last_delivered_id: fields.required("last-delivered-id")?,
            entries_read: fields.required("entries-read")?,
            lag: fields.required("lag")?,
            pel_count: fields.required("pel-count")?,
            pending: fields.required("pending")?,
            consumers: fields.required("consumers")?,
        })
    }
}

impl FromValue for XinfoStreamReplyFullConsumer {
    fn from_value(value: RespValue) -> Result<Self, Error> {
        let mut fields = Fields::from_value(value)?;
        Ok(Self {
            name: fields.required("name")?,
            seen_time: fields.required("seen-time")?,
            active_time: fields
                .required::<OrNever<UnixTimeMilliseconds>>("active-time")?
                .0,
            pel_count: fields.required("pel-count")?,
            pending: fields.required("pending")?,
        })
    }
}

fn unexpected(reply: &RespValue) -> Error {
    Error::Client(format!("unexpected reply: {reply:?}"))
}

fn decode(value: RespNestedValue) -> Result<RespValue, Error> {
    crate::resp::decode(value).map_err(Error::Resp)
}

/// The nested values of an array, or a set.
fn items(reply: RespValue) -> Result<Vec<RespValue>, Error> {
    match reply {
        RespValue::Array(items) | RespValue::Set(items) => items.into_iter().map(decode).collect(),
        reply => Err(unexpected(&reply)),
    }
}

/// The entries of a map, or of a flat array of keys and values.
pub fn entries<K: FromValue, V: FromValue>(reply: RespValue) -> Result<Vec<(K, V)>, Error> {
    let entry = |key, value| Ok((K::from_value(decode(key)?)?, V::from_value(decode(value)?)?));
    match reply {
        RespValue::Map(entries) => entries
            .into_iter()
            .map(|(key, value)| entry(key, value))
            .collect(),
        RespValue::Array(items) => {
            if items.len() % 2 != 0 {
                return Err(Error::Client(format!(
                    "unexpected odd number of keys and values: {}",
                    items.len()
                )));
            }
            let mut items = items.into_iter();
            let mut entries = vec![];
            while let (Some(key), Some(value)) = (items.next(), items.next()) {
                entries.push(entry(key, value)?);
            }
            Ok(entries)
        }
        reply => Err(unexpected(&reply)),
    }
}
