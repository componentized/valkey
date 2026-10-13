use crate::exports::componentized::valkey::commands::{
    Aggregate, BitRange, BitfieldEncoding, BitfieldGet, BitfieldIncrby, BitfieldOffset,
    BitfieldOperation, BitfieldOverflow, BitfieldSet, ChannelRule, CommandRule, Connection, Cursor,
    DbRule, DurationMilliseconds, DurationSeconds, Error, GetexTtl, HsetexCondition,
    HsetexFieldCondition, KeyRule, LexBound, Limit, LinsertPosition, ListSide, MsetexPrecondition,
    PopEnd, Rule, RuleSelector, ScoreBound, SetPrecondition, SetTtl, SortOpts, SortOrder,
    StreamTrim, StreamTrimEfficiency, TtlMode, UnixTimeMilliseconds, UnixTimeSeconds, UserRule,
    Value, XclaimOpts, ZaddComparison, ZaddCondition, ZrangeBy,
};
use crate::value::{FromValue, OrZeroId, ZERO_ID};

/// Send a command, its words as bulk strings, and wait for the reply.
///
/// An error reply fails the command, see `connection.exchange`.
async fn exchange(connection: &Connection, command: impl IntoCommand) -> Result<Value, Error> {
    connection.exchange(command.into_command()?).await
}

/// Send a command, its words as bulk strings, without waiting for a reply.
///
/// An error reply fails the command, see `connection.emit`.
pub(crate) async fn emit(connection: &Connection, command: impl IntoCommand) -> Result<(), Error> {
    connection.emit(command.into_command()?).await
}

/// Send a command, see `exchange`, and convert its reply.
pub(crate) async fn query<T: FromValue>(
    connection: &Connection,
    command: impl IntoCommand,
) -> Result<T, Error> {
    T::from_value(exchange(connection, command).await?)
}

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

into_command_numbers!(i32, i64, u8, u32, u64, usize, f64);

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

impl IntoCommand for Rule {
    fn into_command(self) -> Result<Vec<String>, Error> {
        match self {
            Rule::Keys(rule) => rule.into_command(),
            Rule::Channel(rule) => rule.into_command(),
            Rule::Command(rule) => rule.into_command(),
            Rule::Db(rule) => rule.into_command(),
            Rule::User(rule) => rule.into_command(),
        }
    }
}

impl IntoCommand for RuleSelector {
    fn into_command(self) -> Result<Vec<String>, Error> {
        match self {
            RuleSelector::Keys(rule) => rule.into_command(),
            RuleSelector::Channel(rule) => rule.into_command(),
            RuleSelector::Command(rule) => rule.into_command(),
            RuleSelector::Db(rule) => rule.into_command(),
        }
    }
}

impl IntoCommand for KeyRule {
    fn into_command(self) -> Result<Vec<String>, Error> {
        Ok(vec![match self {
            KeyRule::Add(pattern) => format!("~{pattern}"),
            KeyRule::AddRead(pattern) => format!("%R~{pattern}"),
            KeyRule::AddWrite(pattern) => format!("%W~{pattern}"),
            KeyRule::AddReadWrite(pattern) => format!("%RW~{pattern}"),
            KeyRule::All => "allkeys".to_string(),
            KeyRule::Reset => "resetkeys".to_string(),
        }])
    }
}

impl IntoCommand for ChannelRule {
    fn into_command(self) -> Result<Vec<String>, Error> {
        Ok(vec![match self {
            ChannelRule::Add(pattern) => format!("&{pattern}"),
            ChannelRule::All => "allchannels".to_string(),
            ChannelRule::Reset => "resetchannels".to_string(),
        }])
    }
}

impl IntoCommand for CommandRule {
    fn into_command(self) -> Result<Vec<String>, Error> {
        Ok(vec![match self {
            CommandRule::Add(command) => format!("+{command}"),
            CommandRule::AddCategory(category) => format!("+@{category}"),
            CommandRule::AddFirstArg((command, arg)) => format!("+{command}|{arg}"),
            CommandRule::All => "allcommands".to_string(),
            CommandRule::Remove(command) => format!("-{command}"),
            CommandRule::RemoveCategory(category) => format!("-@{category}"),
            CommandRule::Reset => "nocommands".to_string(),
        }])
    }
}

/// `db=` requires at least one database.
impl IntoCommand for DbRule {
    fn into_command(self) -> Result<Vec<String>, Error> {
        Ok(vec![match self {
            DbRule::Db(dbs) if dbs.is_empty() => {
                return Err(Error::Client("db requires at least one item".to_string()));
            }
            DbRule::Db(dbs) => format!(
                "db={}",
                dbs.iter()
                    .map(|db| db.to_string())
                    .collect::<Vec<_>>()
                    .join(",")
            ),
            DbRule::All => "alldbs".to_string(),
            DbRule::Reset => "resetdbs".to_string(),
        }])
    }
}

impl IntoCommand for UserRule {
    fn into_command(self) -> Result<Vec<String>, Error> {
        match self {
            UserRule::On => "on".into_command(),
            UserRule::Off => "off".into_command(),
            UserRule::Nopass => "nopass".into_command(),
            UserRule::Password(password) => format!(">{password}").into_command(),
            UserRule::HashedPassword(hash) => format!("#{hash}").into_command(),
            UserRule::RemovePassword(password) => format!("<{password}").into_command(),
            UserRule::RemoveHashedPassword(hash) => format!("!{hash}").into_command(),
            UserRule::Selector(rules) => ("(", rules, ")").into_command(),
            UserRule::Clearselectors => "clearselectors".into_command(),
            UserRule::Reset => "reset".into_command(),
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

impl IntoCommand for Aggregate {
    fn into_command(self) -> Result<Vec<String>, Error> {
        let aggregate = match self {
            Aggregate::Sum => "SUM",
            Aggregate::Min => "MIN",
            Aggregate::Max => "MAX",
        };
        ("AGGREGATE", aggregate).into_command()
    }
}

impl IntoCommand for PopEnd {
    fn into_command(self) -> Result<Vec<String>, Error> {
        match self {
            PopEnd::Min => "MIN",
            PopEnd::Max => "MAX",
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
