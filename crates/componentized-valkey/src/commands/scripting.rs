//! The scripting commands: EVAL, FCALL, FUNCTION and SCRIPT, and the FLUSH modes they share.

use crate::command::{IntoCommand, NonEmpty, Words};
use crate::types::{
    ErrorCode, FlushMode, FunctionListFunction, FunctionListLibrary, FunctionRestorePolicy,
    FunctionStatsEngine, FunctionStatsReply, FunctionStatsRunning, RespValue, StringOrBytes,
};
use crate::value::{Fields, FromValue};

impl IntoCommand for FlushMode {
    fn into_command(self) -> Result<Words, ErrorCode> {
        match self {
            FlushMode::Async => "ASYNC",
            FlushMode::Sync => "SYNC",
        }
        .into_command()
    }
}

impl IntoCommand for FunctionRestorePolicy {
    fn into_command(self) -> Result<Words, ErrorCode> {
        match self {
            FunctionRestorePolicy::Flush => "FLUSH",
            FunctionRestorePolicy::Append => "APPEND",
            FunctionRestorePolicy::Replace => "REPLACE",
        }
        .into_command()
    }
}

/// `numkeys [ key ... ] [ arg ... ]`, e.g. for EVAL, `0` without keys.
pub struct KeysAndArgs(pub Option<Vec<String>>, pub Option<Vec<StringOrBytes>>);

impl IntoCommand for KeysAndArgs {
    fn into_command(self) -> Result<Words, ErrorCode> {
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

impl FromValue for FunctionListFunction {
    fn from_value(value: RespValue) -> Result<Self, ErrorCode> {
        let mut fields = Fields::from_value(value)?;
        Ok(Self {
            name: fields.required("name")?,
            description: fields.required("description")?,
            flags: fields.required("flags")?,
        })
    }
}

impl FromValue for FunctionListLibrary {
    fn from_value(value: RespValue) -> Result<Self, ErrorCode> {
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
    fn from_value(value: RespValue) -> Result<Self, ErrorCode> {
        let mut fields = Fields::from_value(value)?;
        Ok(Self {
            name: fields.required("name")?,
            command: fields.required("command")?,
            duration: fields.required("duration_ms")?,
        })
    }
}

impl FromValue for FunctionStatsEngine {
    fn from_value(value: RespValue) -> Result<Self, ErrorCode> {
        let mut fields = Fields::from_value(value)?;
        Ok(Self {
            libraries_count: fields.required("libraries_count")?,
            functions_count: fields.required("functions_count")?,
        })
    }
}

impl FromValue for FunctionStatsReply {
    fn from_value(value: RespValue) -> Result<Self, ErrorCode> {
        let mut fields = Fields::from_value(value)?;
        Ok(Self {
            running_script: fields.required("running_script")?,
            engines: fields.required("engines")?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::{array, bulk, bulks, fields, from, from_error, int, words, words_error};
    use crate::types::DurationMilliseconds;

    #[test]
    fn function_restore_policy() {
        assert_eq!(
            words([
                FunctionRestorePolicy::Flush,
                FunctionRestorePolicy::Append,
                FunctionRestorePolicy::Replace,
            ]),
            ["FLUSH", "APPEND", "REPLACE"]
        );
    }

    #[test]
    fn flush_mode() {
        assert_eq!(
            words([FlushMode::Async, FlushMode::Sync]),
            ["ASYNC", "SYNC"]
        );
    }

    #[test]
    fn keys_and_args() {
        assert_eq!(words(KeysAndArgs(None, None)), ["0"]);
        assert_eq!(
            words(KeysAndArgs(
                Some(vec!["a".to_string(), "b".to_string()]),
                Some(vec!["x".into()])
            )),
            ["2", "a", "b", "x"]
        );
        assert_eq!(words(KeysAndArgs(None, Some(vec!["x".into()]))), ["0", "x"]);
        assert_eq!(
            words_error(KeysAndArgs(Some(vec![]), None)),
            "keys requires at least one item"
        );
        assert_eq!(
            words_error(KeysAndArgs(None, Some(vec![]))),
            "args requires at least one item"
        );
    }

    #[test]
    fn function_list_library() {
        let function = |name: &str, description: RespValue| {
            fields(vec![
                ("name", bulk(name)),
                ("description", description),
                ("flags", bulks(&["no-writes"])),
            ])
        };
        let value = fields(vec![
            ("library_name", bulk("mylib")),
            ("engine", bulk("LUA")),
            (
                "functions",
                array(vec![
                    function("first", bulk("the first")),
                    function("second", RespValue::Null),
                ]),
            ),
        ]);
        let expected = FunctionListLibrary {
            library_name: "mylib".to_string(),
            engine: "LUA".to_string(),
            functions: vec![
                FunctionListFunction {
                    name: "first".to_string(),
                    description: Some("the first".to_string()),
                    flags: vec!["no-writes".to_string()],
                },
                FunctionListFunction {
                    name: "second".to_string(),
                    description: None,
                    flags: vec!["no-writes".to_string()],
                },
            ],
            library_code: None,
        };
        assert_eq!(from::<FunctionListLibrary>(value), expected);
        // WITHCODE
        let value = fields(vec![
            ("library_name", bulk("mylib")),
            ("engine", bulk("LUA")),
            ("functions", array(vec![])),
            ("library_code", bulk("#!lua name=mylib")),
        ]);
        assert_eq!(
            from::<FunctionListLibrary>(value),
            FunctionListLibrary {
                functions: vec![],
                library_code: Some("#!lua name=mylib".to_string()),
                ..expected
            }
        );
    }

    #[test]
    fn function_stats_reply() {
        let engines = fields(vec![(
            "LUA",
            fields(vec![
                ("libraries_count", int(2)),
                ("functions_count", int(3)),
            ]),
        )]);
        let value = fields(vec![
            ("running_script", RespValue::Null),
            ("engines", engines.clone()),
        ]);
        let stats = from::<FunctionStatsReply>(value);
        assert_eq!(stats.running_script, None);
        assert_eq!(
            stats.engines.get("LUA"),
            Some(&FunctionStatsEngine {
                libraries_count: 2,
                functions_count: 3,
            })
        );
        let running = fields(vec![
            ("name", bulk("slow")),
            ("command", bulks(&["FCALL", "slow", "0"])),
            ("duration_ms", int(1500)),
        ]);
        let value = fields(vec![("running_script", running), ("engines", engines)]);
        assert_eq!(
            from::<FunctionStatsReply>(value).running_script,
            Some(FunctionStatsRunning {
                name: "slow".to_string(),
                command: vec!["FCALL".to_string(), "slow".to_string(), "0".to_string()],
                duration: DurationMilliseconds { px: 1500 },
            })
        );
        assert_eq!(
            from_error::<FunctionStatsReply>(fields(vec![("engines", fields(vec![]))])),
            "missing field: running_script"
        );
    }
}
