use crate::exports::componentized::valkey::commands::{
    ChannelRule, CommandRule, Connection, DbRule, Duration, Error, GetexTtl, Instant, KeyRule,
    MsetexPrecondition, Rule, RuleSelector, SetPrecondition, SetTtl, StreamTrim,
    StreamTrimEfficiency, TtlMode, UserRule, Value, XclaimOpts,
};
use crate::value::{FromValue, OrZeroId, ZERO_ID};

/// Send a command, its words as bulk strings, and wait for the reply.
///
/// An error reply fails the command, see `connection.exchange`.
async fn exchange(connection: &Connection, command: impl IntoCommand) -> Result<Value, Error> {
    connection.exchange(command.into_command()).await
}

/// Send a command, its words as bulk strings, without waiting for a reply.
///
/// An error reply fails the command, see `connection.emit`.
pub(crate) async fn emit(connection: &Connection, command: impl IntoCommand) -> Result<(), Error> {
    connection.emit(command.into_command()).await
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
pub trait IntoCommand {
    fn into_command(self) -> Vec<String>;
}

impl IntoCommand for &str {
    fn into_command(self) -> Vec<String> {
        vec![self.to_string()]
    }
}

impl IntoCommand for String {
    fn into_command(self) -> Vec<String> {
        vec![self]
    }
}

impl IntoCommand for &String {
    fn into_command(self) -> Vec<String> {
        vec![self.clone()]
    }
}

/// Numbers, without a unit, a `Duration` is a `u64` of nanoseconds, see `Seconds` and
/// `Milliseconds`.
macro_rules! into_command_numbers {
    ($($number:ty),*) => {
        $(
            impl IntoCommand for $number {
                fn into_command(self) -> Vec<String> {
                    vec![self.to_string()]
                }
            }
        )*
    };
}

into_command_numbers!(i32, i64, u8, u32, u64, usize, f64);

/// No word for none.
impl<T: IntoCommand> IntoCommand for Option<T> {
    fn into_command(self) -> Vec<String> {
        self.map(IntoCommand::into_command).unwrap_or_default()
    }
}

impl<T: IntoCommand> IntoCommand for Vec<T> {
    fn into_command(self) -> Vec<String> {
        self.into_iter()
            .flat_map(IntoCommand::into_command)
            .collect()
    }
}

impl<T: IntoCommand, const N: usize> IntoCommand for [T; N] {
    fn into_command(self) -> Vec<String> {
        self.into_iter()
            .flat_map(IntoCommand::into_command)
            .collect()
    }
}

/// The words of each item, in order.
macro_rules! into_command_tuples {
    ($(($($item:ident),+)),*) => {
        $(
            impl<$($item: IntoCommand),+> IntoCommand for ($($item,)+) {
                #[allow(non_snake_case)]
                fn into_command(self) -> Vec<String> {
                    let ($($item,)+) = self;
                    let mut command = vec![];
                    $(command.extend($item.into_command());)+
                    command
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
    fn into_command(self) -> Vec<String> {
        let Flag(word, present) = self;
        if present {
            vec![word.to_string()]
        } else {
            vec![]
        }
    }
}

pub struct NamedOption<T>(pub &'static str, pub Option<T>);

impl<T: IntoCommand> IntoCommand for NamedOption<T> {
    fn into_command(self) -> Vec<String> {
        let NamedOption(name, value) = self;
        match value {
            Some(_) => (name, value).into_command(),
            None => vec![],
        }
    }
}

/// `0-0` for none, e.g. to start XAUTOCLAIM from the beginning.
impl IntoCommand for OrZeroId {
    fn into_command(self) -> Vec<String> {
        vec![self.0.unwrap_or_else(|| ZERO_ID.to_string())]
    }
}

/// A duration or instant in whole seconds, e.g. for EX and EXAT.
pub struct Seconds<T>(pub T);

/// A duration or instant in whole milliseconds, e.g. for PX and PXAT.
pub struct Milliseconds<T>(pub T);

impl IntoCommand for Seconds<Duration> {
    fn into_command(self) -> Vec<String> {
        vec![duration_seconds(self.0)]
    }
}

impl IntoCommand for Milliseconds<Duration> {
    fn into_command(self) -> Vec<String> {
        vec![duration_milliseconds(self.0)]
    }
}

impl IntoCommand for Seconds<Instant> {
    fn into_command(self) -> Vec<String> {
        vec![instant_seconds(self.0)]
    }
}

impl IntoCommand for Milliseconds<Instant> {
    fn into_command(self) -> Vec<String> {
        vec![instant_milliseconds(self.0)]
    }
}

impl IntoCommand for Rule {
    fn into_command(self) -> Vec<String> {
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
    fn into_command(self) -> Vec<String> {
        match self {
            RuleSelector::Keys(rule) => rule.into_command(),
            RuleSelector::Channel(rule) => rule.into_command(),
            RuleSelector::Command(rule) => rule.into_command(),
            RuleSelector::Db(rule) => rule.into_command(),
        }
    }
}

impl IntoCommand for KeyRule {
    fn into_command(self) -> Vec<String> {
        vec![match self {
            KeyRule::Add(pattern) => format!("~{pattern}"),
            KeyRule::AddRead(pattern) => format!("%R~{pattern}"),
            KeyRule::AddWrite(pattern) => format!("%W~{pattern}"),
            KeyRule::AddReadWrite(pattern) => format!("%RW~{pattern}"),
            KeyRule::All => "allkeys".to_string(),
            KeyRule::Reset => "resetkeys".to_string(),
        }]
    }
}

impl IntoCommand for ChannelRule {
    fn into_command(self) -> Vec<String> {
        vec![match self {
            ChannelRule::Add(pattern) => format!("&{pattern}"),
            ChannelRule::All => "allchannels".to_string(),
            ChannelRule::Reset => "resetchannels".to_string(),
        }]
    }
}

impl IntoCommand for CommandRule {
    fn into_command(self) -> Vec<String> {
        vec![match self {
            CommandRule::Add(command) => format!("+{command}"),
            CommandRule::AddCategory(category) => format!("+@{category}"),
            CommandRule::AddFirstArg((command, arg)) => format!("+{command}|{arg}"),
            CommandRule::All => "allcommands".to_string(),
            CommandRule::Remove(command) => format!("-{command}"),
            CommandRule::RemoveCategory(category) => format!("-@{category}"),
            CommandRule::Reset => "nocommands".to_string(),
        }]
    }
}

impl IntoCommand for DbRule {
    fn into_command(self) -> Vec<String> {
        vec![match self {
            DbRule::Db(dbs) => format!(
                "db={}",
                dbs.iter()
                    .map(|db| db.to_string())
                    .collect::<Vec<_>>()
                    .join(",")
            ),
            DbRule::All => "alldbs".to_string(),
            DbRule::Reset => "resetdbs".to_string(),
        }]
    }
}

impl IntoCommand for UserRule {
    fn into_command(self) -> Vec<String> {
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
    fn into_command(self) -> Vec<String> {
        vec![
            match self {
                TtlMode::Nx => "NX",
                TtlMode::Xx => "XX",
                TtlMode::Gt => "GT",
                TtlMode::Lt => "LT",
            }
            .to_string(),
        ]
    }
}

impl IntoCommand for GetexTtl {
    fn into_command(self) -> Vec<String> {
        match self {
            GetexTtl::Ex(duration) => ("EX", Seconds(duration)).into_command(),
            GetexTtl::Px(duration) => ("PX", Milliseconds(duration)).into_command(),
            GetexTtl::Exat(instant) => ("EXAT", Seconds(instant)).into_command(),
            GetexTtl::Pxat(instant) => ("PXAT", Milliseconds(instant)).into_command(),
            GetexTtl::Persist => "PERSIST".into_command(),
        }
    }
}

/// Also `MsetexTtl`, the same type.
impl IntoCommand for SetTtl {
    fn into_command(self) -> Vec<String> {
        match self {
            SetTtl::Ex(duration) => ("EX", Seconds(duration)).into_command(),
            SetTtl::Px(duration) => ("PX", Milliseconds(duration)).into_command(),
            SetTtl::Exat(instant) => ("EXAT", Seconds(instant)).into_command(),
            SetTtl::Pxat(instant) => ("PXAT", Milliseconds(instant)).into_command(),
            SetTtl::Keepttl => "KEEPTTL".into_command(),
        }
    }
}

impl IntoCommand for SetPrecondition {
    fn into_command(self) -> Vec<String> {
        match self {
            SetPrecondition::Nx => "NX".into_command(),
            SetPrecondition::Xx => "XX".into_command(),
            SetPrecondition::Ifeq(value) => ("IFEQ", value).into_command(),
            SetPrecondition::Ifne(value) => ("IFNE", value).into_command(),
        }
    }
}

impl IntoCommand for MsetexPrecondition {
    fn into_command(self) -> Vec<String> {
        match self {
            MsetexPrecondition::Nx => "NX".into_command(),
            MsetexPrecondition::Xx => "XX".into_command(),
        }
    }
}

impl IntoCommand for StreamTrim {
    fn into_command(self) -> Vec<String> {
        //〈 MAXLEN | MINID 〉 [ = | ~ ] threshold [ LIMIT count ]
        match self {
            StreamTrim::Maxlen(trim) => (
                "MAXLEN",
                trim.efficiency,
                trim.threshold,
                trim.limit.map(|l| ("LIMIT", l).into_command()),
            )
                .into_command(),
            StreamTrim::Minid(trim) => (
                "MINID",
                trim.efficiency,
                trim.threshold,
                trim.limit.map(|l| ("LIMIT", l).into_command()),
            )
                .into_command(),
        }
    }
}

impl IntoCommand for StreamTrimEfficiency {
    fn into_command(self) -> Vec<String> {
        //  = | ~
        (match self {
            StreamTrimEfficiency::Exact => "=",
            StreamTrimEfficiency::MearlyExact => "~",
        })
        .into_command()
    }
}

/// A duration, in nanoseconds, as whole seconds, the precision of EX.
pub fn duration_seconds(nanoseconds: u64) -> String {
    nanoseconds.div_ceil(1_000_000_000).to_string()
}

/// A duration, in nanoseconds, as whole milliseconds, the precision of PX.
pub fn duration_milliseconds(nanoseconds: u64) -> String {
    nanoseconds.div_ceil(1_000_000).to_string()
}

/// An instant as seconds since the unix epoch, the precision of EXAT.
pub fn instant_seconds(instant: Instant) -> String {
    (instant.seconds + instant.nanoseconds.div_ceil(1_000_000_000) as i64).to_string()
}

/// An instant as milliseconds since the unix epoch, the precision of PXAT.
pub fn instant_milliseconds(instant: Instant) -> String {
    ((instant.seconds * 1_000) + instant.nanoseconds.div_ceil(1_000_000) as i64).to_string()
}

impl IntoCommand for XclaimOpts {
    fn into_command(self) -> Vec<String> {
        // [ IDLE ms ] [ TIME unix-time-milliseconds ] [ RETRYCOUNT count ] [ FORCE ] [ LASTID lastid ]
        (
            NamedOption("IDLE", self.idle.map(Milliseconds)),
            NamedOption("TIME", self.time.map(Milliseconds)),
            NamedOption("RETRYCOUNT", self.retrycount),
            Flag("FORCE", self.force),
            NamedOption("LASTID", self.lastid),
        )
            .into_command()
    }
}
