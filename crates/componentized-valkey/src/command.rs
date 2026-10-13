use crate::types::{
    AclChannelRule, AclCommandRule, AclDbRule, AclKeyRule, AclRule, AclSelectorRule, AclUserRule,
    BgsaveOperation, BitRange, BitfieldEncoding, BitfieldGet, BitfieldIncrby, BitfieldOffset,
    BitfieldOperation, BitfieldOverflow, BitfieldSet, ClientFilter, ClientPauseMode,
    ClientSetinfoAttr, ClientTrackingMode, ClientTrackingOptions, ClientType, ClientUnblockType,
    ClusterFailoverMode, ClusterMigrateslotsGroup, ClusterResetMode, ClusterSetslotState,
    CommandListFilter, CommandlogType, Cursor, DurationMilliseconds, DurationSeconds, Error,
    FailoverTarget, FlushMode, GeoCoordinates, GeoUnit, GeoaddCondition, GeosearchCount,
    GeosearchFrom, GeosearchShape, GetexTtl, HsetexCondition, HsetexFieldCondition, KeyType,
    LexBound, Limit, LinsertPosition, ListSide, MigrateAuth, MsetexPrecondition, ScoreBound,
    SetPrecondition, SetTtl, ShutdownSave, SortOpts, SortOrder, StreamTrim, StreamTrimEfficiency,
    TtlMode, UnixTimeMilliseconds, UnixTimeSeconds, XclaimOpts, ZaddComparison, ZaddCondition,
    ZmpopEnd, ZrangeBy, ZsetAggregate,
};
use crate::value::{OrZeroId, ZERO_ID};

/// Converts a Rust type into the words of a command, e.g. `Option<String>` into no word, or one.
///
/// The impls compose, a tuple is the words of each of its items in order, so a command is written
/// as one value, e.g. `("SET", key, value, precondition, ttl)`.
///
/// A value the command does not accept, e.g. an empty list where it requires an item, fails with a
/// client error, and the command is not sent.
pub trait IntoCommand {
    fn into_command(self) -> Result<Vec<String>, Error>;
}

impl IntoCommand for &str {
    fn into_command(self) -> Result<Vec<String>, Error> {
        Ok(vec![self.to_string()])
    }
}

impl IntoCommand for String {
    fn into_command(self) -> Result<Vec<String>, Error> {
        Ok(vec![self])
    }
}

impl IntoCommand for &String {
    fn into_command(self) -> Result<Vec<String>, Error> {
        Ok(vec![self.clone()])
    }
}

/// Numbers, without a unit, see `DurationSeconds` and the other records for times.
macro_rules! into_command_numbers {
    ($($number:ty),*) => {
        $(
            impl IntoCommand for $number {
                fn into_command(self) -> Result<Vec<String>, Error> {
                    Ok(vec![self.to_string()])
                }
            }
        )*
    };
}

into_command_numbers!(i32, i64, u8, u16, u32, u64, usize, f64);

/// No word for none.
impl<T: IntoCommand> IntoCommand for Option<T> {
    fn into_command(self) -> Result<Vec<String>, Error> {
        self.map_or(Ok(vec![]), IntoCommand::into_command)
    }
}

impl<T: IntoCommand> IntoCommand for Vec<T> {
    fn into_command(self) -> Result<Vec<String>, Error> {
        let mut command = vec![];
        for item in self {
            command.extend(item.into_command()?);
        }
        Ok(command)
    }
}

impl<T: IntoCommand, const N: usize> IntoCommand for [T; N] {
    fn into_command(self) -> Result<Vec<String>, Error> {
        let mut command = vec![];
        for item in self {
            command.extend(item.into_command()?);
        }
        Ok(command)
    }
}

/// The words of each item, in order.
macro_rules! into_command_tuples {
    ($(($($item:ident),+)),*) => {
        $(
            impl<$($item: IntoCommand),+> IntoCommand for ($($item,)+) {
                #[allow(non_snake_case)]
                fn into_command(self) -> Result<Vec<String>, Error> {
                    let ($($item,)+) = self;
                    let mut command = vec![];
                    $(command.extend($item.into_command()?);)+
                    Ok(command)
                }
            }
        )*
    };
}

into_command_tuples!(
    (A),
    (A, B),
    (A, B, C),
    (A, B, C, D),
    (A, B, C, D, E),
    (A, B, C, D, E, F),
    (A, B, C, D, E, F, G),
    (A, B, C, D, E, F, G, H),
    (A, B, C, D, E, F, G, H, I),
    (A, B, C, D, E, F, G, H, I, J)
);

/// A word that is present when true, e.g. `Flag("WITHMATCHLEN", with_match_len)`.
///
/// A `bool` has no impl of its own, the word it stands for depends on the command.
pub struct Flag(pub &'static str, pub bool);

impl IntoCommand for Flag {
    fn into_command(self) -> Result<Vec<String>, Error> {
        let Flag(word, present) = self;
        Ok(if present {
            vec![word.to_string()]
        } else {
            vec![]
        })
    }
}

pub struct NamedOption<T>(pub &'static str, pub Option<T>);

impl<T: IntoCommand> IntoCommand for NamedOption<T> {
    fn into_command(self) -> Result<Vec<String>, Error> {
        let NamedOption(name, value) = self;
        match value {
            Some(_) => (name, value).into_command(),
            None => Ok(vec![]),
        }
    }
}

/// `0-0` for none, e.g. to start XAUTOCLAIM from the beginning.
impl IntoCommand for OrZeroId {
    fn into_command(self) -> Result<Vec<String>, Error> {
        Ok(vec![self.0.unwrap_or_else(|| ZERO_ID.to_string())])
    }
}

/// Whole seconds, e.g. for EX.
impl IntoCommand for DurationSeconds {
    fn into_command(self) -> Result<Vec<String>, Error> {
        self.ex.into_command()
    }
}

/// Whole milliseconds, e.g. for PX.
impl IntoCommand for DurationMilliseconds {
    fn into_command(self) -> Result<Vec<String>, Error> {
        self.px.into_command()
    }
}

/// Whole seconds since the Unix epoch, e.g. for EXAT.
impl IntoCommand for UnixTimeSeconds {
    fn into_command(self) -> Result<Vec<String>, Error> {
        self.exat.into_command()
    }
}

/// Whole milliseconds since the Unix epoch, e.g. for PXAT.
impl IntoCommand for UnixTimeMilliseconds {
    fn into_command(self) -> Result<Vec<String>, Error> {
        self.pxat.into_command()
    }
}

impl IntoCommand for AclRule {
    fn into_command(self) -> Result<Vec<String>, Error> {
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
    fn into_command(self) -> Result<Vec<String>, Error> {
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
    fn into_command(self) -> Result<Vec<String>, Error> {
        Ok(vec![match self {
            AclKeyRule::Add(pattern) => format!("~{pattern}"),
            AclKeyRule::AddRead(pattern) => format!("%R~{pattern}"),
            AclKeyRule::AddWrite(pattern) => format!("%W~{pattern}"),
            AclKeyRule::AddReadWrite(pattern) => format!("%RW~{pattern}"),
            AclKeyRule::All => "allkeys".to_string(),
            AclKeyRule::Reset => "resetkeys".to_string(),
        }])
    }
}

impl IntoCommand for AclChannelRule {
    fn into_command(self) -> Result<Vec<String>, Error> {
        Ok(vec![match self {
            AclChannelRule::Add(pattern) => format!("&{pattern}"),
            AclChannelRule::All => "allchannels".to_string(),
            AclChannelRule::Reset => "resetchannels".to_string(),
        }])
    }
}

impl IntoCommand for AclCommandRule {
    fn into_command(self) -> Result<Vec<String>, Error> {
        Ok(vec![match self {
            AclCommandRule::Add(command) => format!("+{command}"),
            AclCommandRule::AddCategory(category) => format!("+@{category}"),
            AclCommandRule::AddFirstArg((command, arg)) => format!("+{command}|{arg}"),
            AclCommandRule::All => "allcommands".to_string(),
            AclCommandRule::Remove(command) => format!("-{command}"),
            AclCommandRule::RemoveCategory(category) => format!("-@{category}"),
            AclCommandRule::Reset => "nocommands".to_string(),
        }])
    }
}

/// `db=` requires at least one database.
impl IntoCommand for AclDbRule {
    fn into_command(self) -> Result<Vec<String>, Error> {
        Ok(vec![match self {
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
        }])
    }
}

impl IntoCommand for AclUserRule {
    fn into_command(self) -> Result<Vec<String>, Error> {
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

impl IntoCommand for TtlMode {
    fn into_command(self) -> Result<Vec<String>, Error> {
        match self {
            TtlMode::Nx => "NX",
            TtlMode::Xx => "XX",
            TtlMode::Gt => "GT",
            TtlMode::Lt => "LT",
        }
        .into_command()
    }
}

impl IntoCommand for GetexTtl {
    fn into_command(self) -> Result<Vec<String>, Error> {
        match self {
            GetexTtl::Ex(duration) => ("EX", duration).into_command(),
            GetexTtl::Px(duration) => ("PX", duration).into_command(),
            GetexTtl::Exat(unix_time) => ("EXAT", unix_time).into_command(),
            GetexTtl::Pxat(unix_time) => ("PXAT", unix_time).into_command(),
            GetexTtl::Persist => "PERSIST".into_command(),
        }
    }
}

/// Also `MsetexTtl`, the same type.
impl IntoCommand for SetTtl {
    fn into_command(self) -> Result<Vec<String>, Error> {
        match self {
            SetTtl::Ex(duration) => ("EX", duration).into_command(),
            SetTtl::Px(duration) => ("PX", duration).into_command(),
            SetTtl::Exat(unix_time) => ("EXAT", unix_time).into_command(),
            SetTtl::Pxat(unix_time) => ("PXAT", unix_time).into_command(),
            SetTtl::Keepttl => "KEEPTTL".into_command(),
        }
    }
}

impl IntoCommand for SetPrecondition {
    fn into_command(self) -> Result<Vec<String>, Error> {
        match self {
            SetPrecondition::Nx => "NX".into_command(),
            SetPrecondition::Xx => "XX".into_command(),
            SetPrecondition::Ifeq(value) => ("IFEQ", value).into_command(),
            SetPrecondition::Ifne(value) => ("IFNE", value).into_command(),
        }
    }
}

impl IntoCommand for MsetexPrecondition {
    fn into_command(self) -> Result<Vec<String>, Error> {
        match self {
            MsetexPrecondition::Nx => "NX".into_command(),
            MsetexPrecondition::Xx => "XX".into_command(),
        }
    }
}

impl IntoCommand for StreamTrim {
    fn into_command(self) -> Result<Vec<String>, Error> {
        //〈 MAXLEN | MINID 〉 [ = | ~ ] threshold [ LIMIT count ]
        match self {
            StreamTrim::Maxlen(trim) => (
                "MAXLEN",
                trim.efficiency,
                trim.threshold,
                NamedOption("LIMIT", trim.limit),
            )
                .into_command(),
            StreamTrim::Minid(trim) => (
                "MINID",
                trim.efficiency,
                trim.threshold,
                NamedOption("LIMIT", trim.limit),
            )
                .into_command(),
        }
    }
}

impl IntoCommand for StreamTrimEfficiency {
    fn into_command(self) -> Result<Vec<String>, Error> {
        //  = | ~
        (match self {
            StreamTrimEfficiency::Exact => "=",
            StreamTrimEfficiency::MearlyExact => "~",
        })
        .into_command()
    }
}

impl IntoCommand for XclaimOpts {
    fn into_command(self) -> Result<Vec<String>, Error> {
        // [ IDLE ms ] [ TIME unix-time-milliseconds ] [ RETRYCOUNT count ] [ FORCE ] [ LASTID lastid ]
        (
            NamedOption("IDLE", self.idle),
            NamedOption("TIME", self.time),
            NamedOption("RETRYCOUNT", self.retrycount),
            Flag("FORCE", self.force),
            NamedOption("LASTID", self.lastid),
        )
            .into_command()
    }
}

/// Milliseconds as seconds with a fraction, e.g. the timeout of BZPOPMIN, `1500` as `1.500`.
pub struct DecimalSeconds(pub DurationMilliseconds);

impl IntoCommand for DecimalSeconds {
    fn into_command(self) -> Result<Vec<String>, Error> {
        let milliseconds = self.0.px;
        Ok(vec![format!(
            "{}.{:03}",
            milliseconds / 1_000,
            milliseconds % 1_000
        )])
    }
}

/// A list the command requires at least one item of, e.g. the keys of DEL, named for the error when
/// it is empty.
pub struct NonEmpty<T>(&'static str, Vec<T>);

impl<T> NonEmpty<T> {
    pub fn new(name: &'static str, items: Vec<T>) -> Self {
        NonEmpty(name, items)
    }

    /// A list the command allows to be left out, none rather than empty.
    pub fn optional(name: &'static str, items: Option<Vec<T>>) -> Option<Self> {
        items.map(|items| NonEmpty(name, items))
    }

    pub fn len(&self) -> usize {
        self.1.len()
    }
}

impl<T: IntoCommand> IntoCommand for NonEmpty<T> {
    fn into_command(self) -> Result<Vec<String>, Error> {
        let NonEmpty(name, items) = self;
        if items.is_empty() {
            return Err(Error::Client(format!("{name} requires at least one item")));
        }
        items.into_command()
    }
}

/// The items preceded by how many there are, e.g. the keys of ZINTER, or the keys and values of
/// MSETEX, a pair counts as one.
pub struct NumKeys<T>(pub NonEmpty<T>);

impl<T: IntoCommand> IntoCommand for NumKeys<T> {
    fn into_command(self) -> Result<Vec<String>, Error> {
        (self.0.len(), self.0).into_command()
    }
}

/// `FIELDS numfields field [ field ... ]`, e.g. for HEXPIRE, a field and its value counts as one.
pub struct Fields<T>(pub NonEmpty<T>);

impl<T: IntoCommand> IntoCommand for Fields<T> {
    fn into_command(self) -> Result<Vec<String>, Error> {
        ("FIELDS", NumKeys(self.0)).into_command()
    }
}

impl IntoCommand for Limit {
    fn into_command(self) -> Result<Vec<String>, Error> {
        ("LIMIT", self.offset, self.count).into_command()
    }
}

/// The start and stop of a range of scores, as given, the start is the maximum with REV.
pub struct ScoreRange {
    pub start: ScoreBound,
    pub stop: ScoreBound,
    pub rev: bool,
}

impl IntoCommand for ScoreRange {
    fn into_command(self) -> Result<Vec<String>, Error> {
        let bound = |bound, unbounded: &str| match bound {
            ScoreBound::Inclusive(score) => score.to_string(),
            ScoreBound::Exclusive(score) => format!("({score}"),
            ScoreBound::Unbounded => unbounded.to_string(),
        };
        let (low, high) = ("-inf", "+inf");
        let (start, stop) = if self.rev { (high, low) } else { (low, high) };
        Ok(vec![bound(self.start, start), bound(self.stop, stop)])
    }
}

/// The start and stop of a range of members, as given, the start is the maximum with REV.
pub struct LexRange {
    pub start: LexBound,
    pub stop: LexBound,
    pub rev: bool,
}

impl IntoCommand for LexRange {
    fn into_command(self) -> Result<Vec<String>, Error> {
        let bound = |bound, unbounded: &str| match bound {
            LexBound::Inclusive(member) => format!("[{member}"),
            LexBound::Exclusive(member) => format!("({member}"),
            LexBound::Unbounded => unbounded.to_string(),
        };
        let (low, high) = ("-", "+");
        let (start, stop) = if self.rev { (high, low) } else { (low, high) };
        Ok(vec![bound(self.start, start), bound(self.stop, stop)])
    }
}

/// ZRANGE and ZRANGESTORE, `start stop [ BYSCORE | BYLEX ] [ REV ] [ LIMIT offset count ]`.
pub struct Zrange {
    pub range: ZrangeBy,
    pub rev: bool,
    pub limit: Option<Limit>,
}

impl IntoCommand for Zrange {
    fn into_command(self) -> Result<Vec<String>, Error> {
        let Zrange { range, rev, limit } = self;
        let (range, by) = match range {
            ZrangeBy::Rank((start, stop)) => ((start, stop).into_command()?, None),
            ZrangeBy::Score((start, stop)) => (
                ScoreRange { start, stop, rev }.into_command()?,
                Some("BYSCORE"),
            ),
            ZrangeBy::Lex((start, stop)) => {
                (LexRange { start, stop, rev }.into_command()?, Some("BYLEX"))
            }
        };
        (range, by, Flag("REV", rev), limit).into_command()
    }
}

impl IntoCommand for ZsetAggregate {
    fn into_command(self) -> Result<Vec<String>, Error> {
        let aggregate = match self {
            ZsetAggregate::Sum => "SUM",
            ZsetAggregate::Min => "MIN",
            ZsetAggregate::Max => "MAX",
        };
        ("AGGREGATE", aggregate).into_command()
    }
}

impl IntoCommand for ZmpopEnd {
    fn into_command(self) -> Result<Vec<String>, Error> {
        match self {
            ZmpopEnd::Min => "MIN",
            ZmpopEnd::Max => "MAX",
        }
        .into_command()
    }
}

impl IntoCommand for ZaddCondition {
    fn into_command(self) -> Result<Vec<String>, Error> {
        match self {
            ZaddCondition::Nx => "NX",
            ZaddCondition::Xx => "XX",
        }
        .into_command()
    }
}

impl IntoCommand for ZaddComparison {
    fn into_command(self) -> Result<Vec<String>, Error> {
        match self {
            ZaddComparison::Gt => "GT",
            ZaddComparison::Lt => "LT",
        }
        .into_command()
    }
}

impl IntoCommand for SortOrder {
    fn into_command(self) -> Result<Vec<String>, Error> {
        match self {
            SortOrder::Asc => "ASC",
            SortOrder::Desc => "DESC",
        }
        .into_command()
    }
}

/// `[ BY pattern ] [ LIMIT offset count ] [ GET pattern ... ] [ ASC | DESC ] [ ALPHA ]`
impl IntoCommand for SortOpts {
    fn into_command(self) -> Result<Vec<String>, Error> {
        (
            NamedOption("BY", self.by),
            self.limit,
            NonEmpty::optional("get", self.get).map(|NonEmpty(name, patterns)| {
                let patterns = patterns.into_iter().map(|pattern| ("GET", pattern));
                NonEmpty(name, patterns.collect())
            }),
            self.order,
            Flag("ALPHA", self.alpha),
        )
            .into_command()
    }
}

impl IntoCommand for HsetexCondition {
    fn into_command(self) -> Result<Vec<String>, Error> {
        match self {
            HsetexCondition::Nx => "NX",
            HsetexCondition::Xx => "XX",
        }
        .into_command()
    }
}

impl IntoCommand for HsetexFieldCondition {
    fn into_command(self) -> Result<Vec<String>, Error> {
        match self {
            HsetexFieldCondition::Fnx => "FNX",
            HsetexFieldCondition::Fxx => "FXX",
        }
        .into_command()
    }
}

/// `0` to start, failing to resume from `0`, which would restart the scan.
impl IntoCommand for Cursor {
    fn into_command(self) -> Result<Vec<String>, Error> {
        match self {
            Cursor::Start => "0".into_command(),
            Cursor::Resume(cursor) if cursor == "0" => Err(Error::Client(
                "resume requires the cursor a scan returned, use start to begin a scan".to_string(),
            )),
            Cursor::Resume(cursor) => cursor.into_command(),
        }
    }
}

impl IntoCommand for ListSide {
    fn into_command(self) -> Result<Vec<String>, Error> {
        match self {
            ListSide::Left => "LEFT",
            ListSide::Right => "RIGHT",
        }
        .into_command()
    }
}

impl IntoCommand for LinsertPosition {
    fn into_command(self) -> Result<Vec<String>, Error> {
        match self {
            LinsertPosition::Before => "BEFORE",
            LinsertPosition::After => "AFTER",
        }
        .into_command()
    }
}

/// `start [ end [ BIT ] ]`, BYTE is the default, and left out for servers before it existed.
impl IntoCommand for BitRange {
    fn into_command(self) -> Result<Vec<String>, Error> {
        match self {
            BitRange::From(start) => start.into_command(),
            BitRange::Bytes((start, end)) => (start, end).into_command(),
            BitRange::Bits((start, end)) => (start, end, "BIT").into_command(),
        }
    }
}

/// `i1` to `i64`, `u1` to `u63`, other widths fail.
impl IntoCommand for BitfieldEncoding {
    fn into_command(self) -> Result<Vec<String>, Error> {
        match self {
            BitfieldEncoding::Signed(width @ 1..=64) => format!("i{width}").into_command(),
            BitfieldEncoding::Unsigned(width @ 1..=63) => format!("u{width}").into_command(),
            BitfieldEncoding::Signed(width) => Err(Error::Client(format!(
                "a signed bitfield is 1 to 64 bits wide, not {width}"
            ))),
            BitfieldEncoding::Unsigned(width) => Err(Error::Client(format!(
                "an unsigned bitfield is 1 to 63 bits wide, not {width}"
            ))),
        }
    }
}

impl IntoCommand for BitfieldOffset {
    fn into_command(self) -> Result<Vec<String>, Error> {
        match self {
            BitfieldOffset::Bit(offset) => offset.into_command(),
            BitfieldOffset::Field(field) => format!("#{field}").into_command(),
        }
    }
}

impl IntoCommand for BitfieldOverflow {
    fn into_command(self) -> Result<Vec<String>, Error> {
        let overflow = match self {
            BitfieldOverflow::Wrap => "WRAP",
            BitfieldOverflow::Sat => "SAT",
            BitfieldOverflow::Fail => "FAIL",
        };
        ("OVERFLOW", overflow).into_command()
    }
}

impl IntoCommand for BitfieldGet {
    fn into_command(self) -> Result<Vec<String>, Error> {
        ("GET", self.encoding, self.offset).into_command()
    }
}

impl IntoCommand for BitfieldSet {
    fn into_command(self) -> Result<Vec<String>, Error> {
        ("SET", self.encoding, self.offset, self.value).into_command()
    }
}

impl IntoCommand for BitfieldIncrby {
    fn into_command(self) -> Result<Vec<String>, Error> {
        ("INCRBY", self.encoding, self.offset, self.increment).into_command()
    }
}

/// A write is preceded by its overflow mode, when given.
impl IntoCommand for BitfieldOperation {
    fn into_command(self) -> Result<Vec<String>, Error> {
        match self {
            BitfieldOperation::Get(get) => get.into_command(),
            BitfieldOperation::Set((overflow, set)) => (overflow, set).into_command(),
            BitfieldOperation::Incrby((overflow, incrby)) => (overflow, incrby).into_command(),
        }
    }
}

/// A bit, `1` or `0`, e.g. for SETBIT.
pub struct Bit(pub bool);

impl IntoCommand for Bit {
    fn into_command(self) -> Result<Vec<String>, Error> {
        if self.0 { "1" } else { "0" }.into_command()
    }
}

/// `longitude latitude`
impl IntoCommand for GeoCoordinates {
    fn into_command(self) -> Result<Vec<String>, Error> {
        (self.longitude, self.latitude).into_command()
    }
}

impl IntoCommand for GeoUnit {
    fn into_command(self) -> Result<Vec<String>, Error> {
        match self {
            GeoUnit::M => "m",
            GeoUnit::Km => "km",
            GeoUnit::Ft => "ft",
            GeoUnit::Mi => "mi",
        }
        .into_command()
    }
}

impl IntoCommand for GeoaddCondition {
    fn into_command(self) -> Result<Vec<String>, Error> {
        match self {
            GeoaddCondition::Nx => "NX",
            GeoaddCondition::Xx => "XX",
        }
        .into_command()
    }
}

impl IntoCommand for GeosearchFrom {
    fn into_command(self) -> Result<Vec<String>, Error> {
        match self {
            GeosearchFrom::Member(member) => ("FROMMEMBER", member).into_command(),
            GeosearchFrom::Lonlat(coordinates) => ("FROMLONLAT", coordinates).into_command(),
        }
    }
}

/// The center, when the shape has one, then the shape.
impl IntoCommand for GeosearchShape {
    fn into_command(self) -> Result<Vec<String>, Error> {
        match self {
            GeosearchShape::Radius(circle) => {
                (circle.from, "BYRADIUS", circle.radius, circle.unit).into_command()
            }
            GeosearchShape::Box(rectangle) => (
                rectangle.from,
                "BYBOX",
                rectangle.width,
                rectangle.height,
                rectangle.unit,
            )
                .into_command(),
            GeosearchShape::Polygon(vertices) => {
                ("BYPOLYGON", NumKeys(NonEmpty::new("vertices", vertices))).into_command()
            }
        }
    }
}

/// `COUNT count [ ANY ]`
impl IntoCommand for GeosearchCount {
    fn into_command(self) -> Result<Vec<String>, Error> {
        ("COUNT", self.count, Flag("ANY", self.any)).into_command()
    }
}

impl IntoCommand for FlushMode {
    fn into_command(self) -> Result<Vec<String>, Error> {
        match self {
            FlushMode::Async => "ASYNC",
            FlushMode::Sync => "SYNC",
        }
        .into_command()
    }
}

/// `numkeys [ key ... ] [ arg ... ]`, e.g. for EVAL, `0` without keys.
pub struct KeysAndArgs(pub Option<Vec<String>>, pub Option<Vec<String>>);

impl IntoCommand for KeysAndArgs {
    fn into_command(self) -> Result<Vec<String>, Error> {
        let KeysAndArgs(keys, args) = self;
        let numkeys = keys.as_ref().map_or(0, Vec::len);
        (
            numkeys,
            NonEmpty::optional("keys", keys),
            NonEmpty::optional("args", args),
        )
            .into_command()
    }
}

/// The name of the type, e.g. for SCAN's TYPE.
impl IntoCommand for KeyType {
    fn into_command(self) -> Result<Vec<String>, Error> {
        match self {
            KeyType::String => "string".to_string(),
            KeyType::List => "list".to_string(),
            KeyType::Set => "set".to_string(),
            KeyType::Zset => "zset".to_string(),
            KeyType::Hash => "hash".to_string(),
            KeyType::Stream => "stream".to_string(),
            KeyType::Other(name) => name,
        }
        .into_command()
    }
}

impl IntoCommand for MigrateAuth {
    fn into_command(self) -> Result<Vec<String>, Error> {
        match self {
            MigrateAuth::Password(password) => ("AUTH", password).into_command(),
            MigrateAuth::UsernamePassword((username, password)) => {
                ("AUTH2", username, password).into_command()
            }
        }
    }
}

impl IntoCommand for BgsaveOperation {
    fn into_command(self) -> Result<Vec<String>, Error> {
        match self {
            BgsaveOperation::Schedule => "SCHEDULE",
            BgsaveOperation::Cancel => "CANCEL",
        }
        .into_command()
    }
}

/// `master` rather than `primary`, which servers before Valkey 8.0 do not know.
impl IntoCommand for ClientType {
    fn into_command(self) -> Result<Vec<String>, Error> {
        match self {
            ClientType::Normal => "normal",
            ClientType::Primary => "master",
            ClientType::Replica => "replica",
            ClientType::Pubsub => "pubsub",
        }
        .into_command()
    }
}

impl IntoCommand for ClientPauseMode {
    fn into_command(self) -> Result<Vec<String>, Error> {
        match self {
            ClientPauseMode::Write => "WRITE",
            ClientPauseMode::All => "ALL",
        }
        .into_command()
    }
}

impl IntoCommand for ClientUnblockType {
    fn into_command(self) -> Result<Vec<String>, Error> {
        match self {
            ClientUnblockType::Timeout => "TIMEOUT",
            ClientUnblockType::Error => "ERROR",
        }
        .into_command()
    }
}

impl IntoCommand for ClusterFailoverMode {
    fn into_command(self) -> Result<Vec<String>, Error> {
        match self {
            ClusterFailoverMode::Force => "FORCE",
            ClusterFailoverMode::Takeover => "TAKEOVER",
        }
        .into_command()
    }
}

impl IntoCommand for ClusterResetMode {
    fn into_command(self) -> Result<Vec<String>, Error> {
        match self {
            ClusterResetMode::Hard => "HARD",
            ClusterResetMode::Soft => "SOFT",
        }
        .into_command()
    }
}

impl IntoCommand for CommandlogType {
    fn into_command(self) -> Result<Vec<String>, Error> {
        match self {
            CommandlogType::Slow => "slow",
            CommandlogType::LargeRequest => "large-request",
            CommandlogType::LargeReply => "large-reply",
        }
        .into_command()
    }
}

impl IntoCommand for ShutdownSave {
    fn into_command(self) -> Result<Vec<String>, Error> {
        match self {
            ShutdownSave::Nosave => "NOSAVE",
            ShutdownSave::Save => "SAVE",
        }
        .into_command()
    }
}

impl IntoCommand for ClientFilter {
    fn into_command(self) -> Result<Vec<String>, Error> {
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

impl IntoCommand for ClusterSetslotState {
    fn into_command(self) -> Result<Vec<String>, Error> {
        match self {
            ClusterSetslotState::Importing(node_id) => ("IMPORTING", node_id).into_command(),
            ClusterSetslotState::Migrating(node_id) => ("MIGRATING", node_id).into_command(),
            ClusterSetslotState::Node(node_id) => ("NODE", node_id).into_command(),
            ClusterSetslotState::Stable => "STABLE".into_command(),
        }
    }
}

/// `TO host port [ FORCE ]`
impl IntoCommand for FailoverTarget {
    fn into_command(self) -> Result<Vec<String>, Error> {
        ("TO", self.host, self.port, Flag("FORCE", self.force)).into_command()
    }
}

/// `SLOTSRANGE start-slot end-slot [ start-slot end-slot ... ] NODE node-id [ AUTH username password ]`
impl IntoCommand for ClusterMigrateslotsGroup {
    fn into_command(self) -> Result<Vec<String>, Error> {
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

impl IntoCommand for ClientSetinfoAttr {
    fn into_command(self) -> Result<Vec<String>, Error> {
        match self {
            ClientSetinfoAttr::LibName(name) => ("LIB-NAME", name).into_command(),
            ClientSetinfoAttr::LibVer(version) => ("LIB-VER", version).into_command(),
        }
    }
}

impl IntoCommand for ClientTrackingMode {
    fn into_command(self) -> Result<Vec<String>, Error> {
        match self {
            ClientTrackingMode::Optin => "OPTIN",
            ClientTrackingMode::Optout => "OPTOUT",
        }
        .into_command()
    }
}

/// `[ REDIRECT client-id ] [ PREFIX prefix [ PREFIX prefix ... ] ] [ BCAST ] [ OPTIN | OPTOUT ] [ NOLOOP ]`
impl IntoCommand for ClientTrackingOptions {
    fn into_command(self) -> Result<Vec<String>, Error> {
        // PREFIX before each prefix
        let prefixes = self.prefixes.map(|prefixes| {
            let prefixes = prefixes.into_iter().map(|prefix| ("PREFIX", prefix));
            prefixes.collect::<Vec<_>>()
        });
        (
            NamedOption("REDIRECT", self.redirect),
            NonEmpty::optional("prefixes", prefixes),
            Flag("BCAST", self.bcast),
            self.mode,
            Flag("NOLOOP", self.noloop),
        )
            .into_command()
    }
}

/// `FILTERBY 〈 MODULE module-name | ACLCAT category | PATTERN pattern 〉`
impl IntoCommand for CommandListFilter {
    fn into_command(self) -> Result<Vec<String>, Error> {
        let (filter, value) = match self {
            CommandListFilter::Module(name) => ("MODULE", name),
            CommandListFilter::Aclcat(category) => ("ACLCAT", category),
            CommandListFilter::Pattern(pattern) => ("PATTERN", pattern),
        };
        ("FILTERBY", filter, value).into_command()
    }
}

/// `ON` or `OFF`, e.g. for CLIENT NO-TOUCH.
pub struct OnOff(pub bool);

impl IntoCommand for OnOff {
    fn into_command(self) -> Result<Vec<String>, Error> {
        if self.0 { "ON" } else { "OFF" }.into_command()
    }
}
