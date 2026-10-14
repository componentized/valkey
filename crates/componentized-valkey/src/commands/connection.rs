//! The connection commands: CLIENT of the connection itself, and COMMAND.

use crate::command::{Flag, IntoCommand, NonEmpty};
use crate::types::{
    ClientSetinfoAttr, ClientTrackingMode, ClientTrackingOptions, ClientTrackinginfoRedirect,
    ClientTrackinginfoReply, CommandDocsArgument, CommandDocsArgumentType, CommandDocsReply,
    CommandInfoReply, CommandKeySpec, CommandKeySpecBeginSearch, CommandKeySpecFindKeys,
    CommandKeySpecKeynum, CommandKeySpecKeyword, CommandKeySpecRange, CommandListFilter, Error,
    Integer, RespValue,
};
use crate::value::{Fields, FlatPairs, FromValue, entries, items};

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

/// `[ PREFIX prefix [ PREFIX prefix ... ] ] [ BCAST ] [ OPTIN | OPTOUT ] [ NOLOOP ]`
impl IntoCommand for ClientTrackingOptions {
    fn into_command(self) -> Result<Vec<String>, Error> {
        // PREFIX before each prefix
        let prefixes = self.prefixes.map(|prefixes| {
            let prefixes = prefixes.into_iter().map(|prefix| ("PREFIX", prefix));
            prefixes.collect::<Vec<_>>()
        });
        (
            NonEmpty::optional("prefixes", prefixes),
            Flag("BCAST", self.bcast),
            self.mode,
            Flag("NOLOOP", self.noloop),
        )
            .into_command()
    }
}

/// `-1` when tracking is off, `0` when not redirected, otherwise the ID of the connection.
impl FromValue for ClientTrackinginfoRedirect {
    fn from_value(value: RespValue) -> Result<Self, Error> {
        match Integer::from_value(value)? {
            -1 => Ok(ClientTrackinginfoRedirect::NotTracking),
            0 => Ok(ClientTrackinginfoRedirect::NotRedirected),
            id if id > 0 => Ok(ClientTrackinginfoRedirect::Redirected(id)),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::{
        array, bulk, bulks, fields, from, from_error, int, map, words, words_error,
    };

    #[test]
    fn client_setinfo_attr() {
        assert_eq!(
            words(ClientSetinfoAttr::LibName("lib".to_string())),
            ["LIB-NAME", "lib"]
        );
        assert_eq!(
            words(ClientSetinfoAttr::LibVer("1.0".to_string())),
            ["LIB-VER", "1.0"]
        );
    }

    #[test]
    fn client_tracking_options() {
        let options = ClientTrackingOptions {
            prefixes: Some(vec!["a:".to_string(), "b:".to_string()]),
            bcast: true,
            mode: Some(ClientTrackingMode::Optin),
            noloop: true,
        };
        assert_eq!(
            words(options),
            ["PREFIX", "a:", "PREFIX", "b:", "BCAST", "OPTIN", "NOLOOP"]
        );
        let options = ClientTrackingOptions {
            prefixes: None,
            bcast: false,
            mode: Some(ClientTrackingMode::Optout),
            noloop: false,
        };
        assert_eq!(words(options), ["OPTOUT"]);
        let options = ClientTrackingOptions {
            prefixes: Some(vec![]),
            bcast: true,
            mode: None,
            noloop: false,
        };
        assert_eq!(words_error(options), "prefixes requires at least one item");
    }

    #[test]
    fn client_trackinginfo_redirect() {
        assert_eq!(
            from::<ClientTrackinginfoRedirect>(int(-1)),
            ClientTrackinginfoRedirect::NotTracking
        );
        assert_eq!(
            from::<ClientTrackinginfoRedirect>(int(0)),
            ClientTrackinginfoRedirect::NotRedirected
        );
        assert_eq!(
            from::<ClientTrackinginfoRedirect>(int(7)),
            ClientTrackinginfoRedirect::Redirected(7)
        );
        assert_eq!(
            from_error::<ClientTrackinginfoRedirect>(int(-2)),
            "unexpected reply: -2"
        );
    }

    #[test]
    fn client_trackinginfo_reply() {
        let value = fields(vec![
            ("flags", bulks(&["on", "bcast"])),
            ("redirect", int(0)),
            ("prefixes", bulks(&["a:"])),
        ]);
        assert_eq!(
            from::<ClientTrackinginfoReply>(value),
            ClientTrackinginfoReply {
                flags: vec!["on".to_string(), "bcast".to_string()],
                redirect: ClientTrackinginfoRedirect::NotRedirected,
                prefixes: vec!["a:".to_string()],
            }
        );
    }

    #[test]
    fn command_list_filter() {
        assert_eq!(
            words(CommandListFilter::Module("search".to_string())),
            ["FILTERBY", "MODULE", "search"]
        );
        assert_eq!(
            words(CommandListFilter::Aclcat("read".to_string())),
            ["FILTERBY", "ACLCAT", "read"]
        );
        assert_eq!(
            words(CommandListFilter::Pattern("x*".to_string())),
            ["FILTERBY", "PATTERN", "x*"]
        );
    }

    #[test]
    fn on_off() {
        assert_eq!(words([OnOff(true), OnOff(false)]), ["ON", "OFF"]);
    }

    fn get_key_spec() -> RespValue {
        fields(vec![
            ("flags", bulks(&["RO", "access"])),
            (
                "begin_search",
                fields(vec![
                    ("type", bulk("index")),
                    ("spec", fields(vec![("index", int(1))])),
                ]),
            ),
            (
                "find_keys",
                fields(vec![
                    ("type", bulk("range")),
                    (
                        "spec",
                        fields(vec![
                            ("lastkey", int(0)),
                            ("keystep", int(1)),
                            ("limit", int(0)),
                        ]),
                    ),
                ]),
            ),
        ])
    }

    fn command_info_value(
        name: &str,
        key_specs: Vec<RespValue>,
        subcommands: Vec<RespValue>,
    ) -> RespValue {
        array(vec![
            bulk(name),
            int(2),
            bulks(&["readonly"]),
            int(1),
            int(1),
            int(1),
            bulks(&["@read"]),
            array(vec![]),
            array(key_specs),
            array(subcommands),
        ])
    }

    fn command_info_reply(name: &str, key_specs: Vec<CommandKeySpec>) -> CommandInfoReply {
        CommandInfoReply {
            name: name.to_string(),
            arity: 2,
            flags: vec!["readonly".to_string()],
            first_key: 1,
            last_key: 1,
            step: 1,
            acl_categories: vec!["@read".to_string()],
            tips: vec![],
            key_specs,
        }
    }

    #[test]
    fn command_info_list() {
        let value = array(vec![
            command_info_value("get", vec![get_key_spec()], vec![]),
            // a command that does not exist
            RespValue::Null,
            command_info_value(
                "config",
                vec![],
                vec![command_info_value("config|get", vec![], vec![])],
            ),
        ]);
        let get_key_spec = CommandKeySpec {
            notes: None,
            flags: vec!["RO".to_string(), "access".to_string()],
            begin_search: CommandKeySpecBeginSearch::Index(1),
            find_keys: CommandKeySpecFindKeys::Range(CommandKeySpecRange {
                lastkey: 0,
                keystep: 1,
                limit: 0,
            }),
        };
        assert_eq!(
            from::<CommandInfoList>(value).0,
            [
                command_info_reply("get", vec![get_key_spec]),
                command_info_reply("config", vec![]),
                command_info_reply("config|get", vec![]),
            ]
        );
    }

    #[test]
    fn command_info_before_valkey_7() {
        let value = array(vec![array(vec![
            bulk("get"),
            int(2),
            bulks(&["readonly"]),
            int(1),
            int(1),
            int(1),
        ])]);
        assert_eq!(
            from::<CommandInfoList>(value).0,
            [CommandInfoReply {
                acl_categories: vec![],
                ..command_info_reply("get", vec![])
            }]
        );
        assert_eq!(
            from_error::<CommandInfoList>(array(vec![array(vec![bulk("get")])])),
            "too few items for a command"
        );
    }

    #[test]
    fn command_key_spec_kinds() {
        let value = fields(vec![
            ("notes", bulk("a note")),
            ("flags", bulks(&["RW"])),
            (
                "begin_search",
                fields(vec![
                    ("type", bulk("keyword")),
                    (
                        "spec",
                        fields(vec![("keyword", bulk("STREAMS")), ("startfrom", int(1))]),
                    ),
                ]),
            ),
            (
                "find_keys",
                fields(vec![
                    ("type", bulk("keynum")),
                    (
                        "spec",
                        fields(vec![
                            ("keynumidx", int(0)),
                            ("firstkey", int(1)),
                            ("keystep", int(1)),
                        ]),
                    ),
                ]),
            ),
        ]);
        assert_eq!(
            from::<CommandKeySpec>(value),
            CommandKeySpec {
                notes: Some("a note".to_string()),
                flags: vec!["RW".to_string()],
                begin_search: CommandKeySpecBeginSearch::Keyword(CommandKeySpecKeyword {
                    keyword: "STREAMS".to_string(),
                    startfrom: 1,
                }),
                find_keys: CommandKeySpecFindKeys::Keynum(CommandKeySpecKeynum {
                    keynumidx: 0,
                    firstkey: 1,
                    keystep: 1,
                }),
            }
        );
    }

    #[test]
    fn command_key_spec_unknown_kinds() {
        let unknown = || fields(vec![("type", bulk("newer")), ("spec", fields(vec![]))]);
        let value = fields(vec![
            ("flags", array(vec![])),
            ("begin_search", unknown()),
            ("find_keys", unknown()),
        ]);
        assert_eq!(
            from::<CommandKeySpec>(value),
            CommandKeySpec {
                notes: None,
                flags: vec![],
                begin_search: CommandKeySpecBeginSearch::Unknown,
                find_keys: CommandKeySpecFindKeys::Unknown,
            }
        );
    }

    fn doc(name: &str) -> CommandDocsReply {
        CommandDocsReply {
            name: name.to_string(),
            summary: format!("about {name}"),
            since: "1.0.0".to_string(),
            group: "server".to_string(),
            complexity: None,
            module: None,
            doc_flags: vec![],
            deprecated_since: None,
            replaced_by: None,
            history: vec![],
            arguments: vec![],
        }
    }

    fn argument(
        parent: Option<u32>,
        name: &str,
        type_: CommandDocsArgumentType,
    ) -> CommandDocsArgument {
        CommandDocsArgument {
            parent,
            name: name.to_string(),
            type_,
            display_text: None,
            key_spec_index: None,
            token: None,
            summary: None,
            since: None,
            deprecated_since: None,
            flags: vec![],
        }
    }

    #[test]
    fn command_docs_list() {
        let value = map(vec![
            (
                bulk("set"),
                fields(vec![
                    ("summary", bulk("about set")),
                    ("since", bulk("1.0.0")),
                    ("group", bulk("server")),
                    ("complexity", bulk("O(1)")),
                    (
                        "history",
                        array(vec![bulks(&[
                            "2.6.12",
                            "Added the EX, PX, NX and XX options.",
                        ])]),
                    ),
                    (
                        "arguments",
                        array(vec![
                            fields(vec![
                                ("name", bulk("key")),
                                ("type", bulk("key")),
                                ("display_text", bulk("key")),
                                ("key_spec_index", int(0)),
                            ]),
                            fields(vec![
                                ("name", bulk("condition")),
                                ("type", bulk("oneof")),
                                ("since", bulk("2.6.12")),
                                ("flags", bulks(&["optional"])),
                                (
                                    "arguments",
                                    array(vec![fields(vec![
                                        ("name", bulk("nx")),
                                        ("type", bulk("pure-token")),
                                        ("token", bulk("NX")),
                                    ])]),
                                ),
                            ]),
                            fields(vec![("name", bulk("newer")), ("type", bulk("vector"))]),
                        ]),
                    ),
                ]),
            ),
            (
                bulk("config"),
                fields(vec![
                    ("summary", bulk("about config")),
                    ("since", bulk("1.0.0")),
                    ("group", bulk("server")),
                    ("doc_flags", bulks(&["syscmd"])),
                    (
                        "subcommands",
                        map(vec![(
                            bulk("config|get"),
                            fields(vec![
                                ("summary", bulk("about config|get")),
                                ("since", bulk("1.0.0")),
                                ("group", bulk("server")),
                                ("deprecated_since", bulk("9.0.0")),
                                ("replaced_by", bulk("`OTHER`")),
                            ]),
                        )]),
                    ),
                ]),
            ),
        ]);
        assert_eq!(
            from::<CommandDocsList>(value).0,
            [
                CommandDocsReply {
                    complexity: Some("O(1)".to_string()),
                    history: vec![(
                        "2.6.12".to_string(),
                        "Added the EX, PX, NX and XX options.".to_string()
                    )],
                    arguments: vec![
                        CommandDocsArgument {
                            display_text: Some("key".to_string()),
                            key_spec_index: Some(0),
                            ..argument(None, "key", CommandDocsArgumentType::Key)
                        },
                        CommandDocsArgument {
                            since: Some("2.6.12".to_string()),
                            flags: vec!["optional".to_string()],
                            ..argument(None, "condition", CommandDocsArgumentType::Oneof)
                        },
                        CommandDocsArgument {
                            token: Some("NX".to_string()),
                            ..argument(Some(1), "nx", CommandDocsArgumentType::PureToken)
                        },
                        argument(
                            None,
                            "newer",
                            CommandDocsArgumentType::Other("vector".to_string())
                        ),
                    ],
                    ..doc("set")
                },
                CommandDocsReply {
                    doc_flags: vec!["syscmd".to_string()],
                    ..doc("config")
                },
                CommandDocsReply {
                    deprecated_since: Some("9.0.0".to_string()),
                    replaced_by: Some("`OTHER`".to_string()),
                    ..doc("config|get")
                },
            ]
        );
    }

    #[test]
    fn command_docs_argument_types() {
        let kinds = [
            ("string", CommandDocsArgumentType::String),
            ("integer", CommandDocsArgumentType::Integer),
            ("double", CommandDocsArgumentType::Double),
            ("key", CommandDocsArgumentType::Key),
            ("pattern", CommandDocsArgumentType::Pattern),
            ("unix-time", CommandDocsArgumentType::UnixTime),
            ("pure-token", CommandDocsArgumentType::PureToken),
            ("oneof", CommandDocsArgumentType::Oneof),
            ("block", CommandDocsArgumentType::Block),
        ];
        for (kind, expected) in kinds {
            assert_eq!(argument_type(kind.to_string()), expected);
        }
    }

    #[test]
    fn command_docs_missing_field() {
        let value = map(vec![(bulk("get"), fields(vec![("summary", bulk("s"))]))]);
        assert_eq!(from_error::<CommandDocsList>(value), "missing field: since");
    }
}
