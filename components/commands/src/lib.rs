use crate::exports::componentized::valkey::commands::{
    AclDryrunResult, AclListEntry, AclLogEntry, AclRule, BgsaveOperation, BitRange, BitfieldGet,
    BitfieldOperation, BitopOperation, Bytes, BzmpopEnd, ClientFilter, ClientPauseMode,
    ClientSetinfoAttr, ClientTrackingOptions, ClientTrackinginfoReply, ClientUnblockType,
    ClusterBumpepochResult, ClusterFailoverMode, ClusterGetslotmigrationsEntry,
    ClusterMigrateslotsGroup, ClusterResetMode, ClusterSetslotState, CommandDocsReply,
    CommandInfoReply, CommandListFilter, CommandlogEntry, CommandlogType, Connection, Cursor,
    Double, DurationMilliseconds, DurationSeconds, Error, ExpiretimeReply, FailoverTarget,
    FlushMode, FunctionListLibrary, FunctionRestorePolicy, FunctionStatsReply, GeoCoordinates,
    GeoUnit, GeoaddCondition, GeosearchCount, GeosearchShape, GeosearchWithFlags,
    GeosearchWithResult, GetexTtl, Guest, HexpireResult, HgetexTtl, HpersistResult,
    HsetexCondition, HsetexFieldCondition, HsetexTtl, Integer, KeyType, LatencyHistogramEntry,
    LatencyLatestEntry, LcsIdxReply, LexBound, Limit, LinsertPosition, LinsertResult, ListSide,
    MigrateAuth, MigrateFlags, ModuleListEntry, MsetexPrecondition, MsetexTtl, PexpiretimeReply,
    PttlReply, RespValue, RestoreTtl, RoleReply, ScoreBound, SetPrecondition, SetTtl,
    ShutdownFlags, ShutdownSave, SortOpts, SortOrder, SortRoOpts, StreamId, StreamTrim,
    StringOrBytes, TtlMode, TtlReply, UnixTimeMilliseconds, UnixTimeSeconds, XclaimOpts,
    XinfoConsumersEntry, XinfoGroupsEntry, XinfoStreamReply, XinfoStreamReplyFull,
    XpendingExtendedEntry, XpendingSummary, ZaddComparison, ZaddCondition, ZmpopEnd, ZrangeBy,
    ZrangeWithscoresBy, ZsetAggregate,
};
use componentized_valkey::command::{Flag, NamedOption, NonEmpty, NumKeys};
use componentized_valkey::commands::admin::{AclList, AclUserRules, ClientList};
use componentized_valkey::commands::bitmap::Bit;
use componentized_valkey::commands::connection::{CommandDocsList, CommandInfoList, OnOff};
use componentized_valkey::commands::hash::Fields;
use componentized_valkey::commands::keyspace::{Migrated, OptionalKeyType};
use componentized_valkey::commands::scripting::KeysAndArgs;
use componentized_valkey::commands::shared::{DecimalSeconds, NextCursor};
use componentized_valkey::commands::sortedset::{LexRange, ScoreRange, ScoredMembers, Zrange};
use componentized_valkey::commands::stream::OrZeroId;
use componentized_valkey::value::{Binary, FlatPairs, OrNever};

use wit_bindgen::rt::Map;

use componentized_valkey::{FromValue, IntoCommand};

/// Send a command, its words as bulk strings, and convert its reply.
///
/// An error reply fails the command, see `connection.exchange`.
async fn exchange<T: FromValue>(
    connection: &Connection,
    command: impl IntoCommand,
) -> Result<T, Error> {
    let reply = connection.exchange(command.into_command()?).await?;
    T::from_value(reply)
}

struct Commands;

/// The rows of SORT's values, `row` values each.
fn rows(values: Vec<Option<StringOrBytes>>, row: usize) -> Vec<Vec<Option<StringOrBytes>>> {
    let mut values = values.into_iter();
    std::iter::from_fn(|| Some(values.by_ref().take(row).collect::<Vec<_>>()))
        .take_while(|row| !row.is_empty())
        .collect()
}

/// The words of RESTORE's ttl, `0` for none, and whether it is absolute, ABSTTL.
fn restore_ttl(ttl: Option<RestoreTtl>) -> (StringOrBytes, bool) {
    match ttl {
        None => ("0".into(), false),
        Some(RestoreTtl::ExpireIn(duration)) => (duration.px.to_string().into(), false),
        Some(RestoreTtl::ExpireAt(unix_time)) => (unix_time.pxat.to_string().into(), true),
    }
}

/// The number of values SORT returns for each element, one for each GET pattern, or the element.
fn sort_row_len(opts: &Option<SortOpts>) -> usize {
    opts.as_ref()
        .and_then(|opts| opts.get.as_ref())
        .map_or(1, Vec::len)
}

impl Guest for Commands {
    #[doc = " Deletes ACL users, and terminates their connections."]
    #[doc = ""]
    #[doc = " ACL DELUSER username [ username ... ]"]
    #[doc = " https://valkey.io/commands/acl-deluser/"]
    #[allow(async_fn_in_trait)]
    async fn acl_deluser(
        connection: &Connection,
        usernames: Vec<String>,
    ) -> Result<Integer, Error> {
        exchange(
            connection,
            ("ACL", "DELUSER", NonEmpty::new("usernames", usernames)),
        )
        .await
    }

    #[doc = " Generates a pseudorandom, secure password that can be used to identify ACL users."]
    #[doc = ""]
    #[doc = " ACL GENPASS [ bits ]"]
    #[doc = " https://valkey.io/commands/acl-genpass/"]
    #[allow(async_fn_in_trait)]
    async fn acl_genpass(connection: &Connection, bits: Option<Integer>) -> Result<String, Error> {
        exchange(connection, ("ACL", "GENPASS", bits)).await
    }

    #[doc = " Creates and modifies an ACL user and its rules."]
    #[doc = ""]
    #[doc = " ACL SETUSER username [ rule ] [ [ rule ] ... ]"]
    #[doc = " https://valkey.io/commands/acl-setuser/"]
    #[allow(async_fn_in_trait)]
    async fn acl_setuser(
        connection: &Connection,
        username: String,
        rules: Option<Vec<AclRule>>,
    ) -> Result<(), Error> {
        exchange(
            connection,
            (
                "ACL",
                "SETUSER",
                username,
                NonEmpty::optional("rules", rules),
            ),
        )
        .await
    }

    #[doc = " Lists the ACL categories, or the commands inside a category."]
    #[doc = ""]
    #[doc = " ACL CAT [ category ]"]
    #[doc = " https://valkey.io/commands/acl-cat/"]
    #[allow(async_fn_in_trait)]
    async fn acl_cat(
        connection: &Connection,
        category: Option<String>,
    ) -> Result<Vec<String>, Error> {
        exchange(connection, ("ACL", "CAT", category)).await
    }

    #[doc = " Simulates the execution of a command by a user, without executing the command."]
    #[doc = ""]
    #[doc = " ACL DRYRUN username command [ arg [ arg ... ] ]"]
    #[doc = " https://valkey.io/commands/acl-dryrun/"]
    #[allow(async_fn_in_trait)]
    async fn acl_dryrun(
        connection: &Connection,
        username: String,
        command: Vec<String>,
    ) -> Result<AclDryrunResult, Error> {
        exchange(
            connection,
            ("ACL", "DRYRUN", username, NonEmpty::new("command", command)),
        )
        .await
    }

    #[doc = " Lists the ACL rules of a user."]
    #[doc = ""]
    #[doc = " None when the user does not exist. The rules are as the server normalized them, e.g."]
    #[doc = " `allkeys` is `~*`. Applied with `acl-setuser`, after `reset`, they recreate the user."]
    #[doc = ""]
    #[doc = " ACL GETUSER username"]
    #[doc = " https://valkey.io/commands/acl-getuser/"]
    #[allow(async_fn_in_trait)]
    async fn acl_getuser(
        connection: &Connection,
        username: String,
    ) -> Result<Option<Vec<AclRule>>, Error> {
        let rules: Option<AclUserRules> =
            exchange(connection, ("ACL", "GETUSER", username)).await?;
        Ok(rules.map(|rules| rules.0))
    }

    #[doc = " Dumps the effective rules in ACL file format."]
    #[doc = ""]
    #[doc = " ACL LIST"]
    #[doc = " https://valkey.io/commands/acl-list/"]
    #[allow(async_fn_in_trait)]
    async fn acl_list(connection: &Connection) -> Result<Vec<AclListEntry>, Error> {
        let users: AclList = exchange(connection, ("ACL", "LIST")).await?;
        Ok(users.0)
    }

    #[doc = " Reloads the rules from the configured ACL file."]
    #[doc = ""]
    #[doc = " ACL LOAD"]
    #[doc = " https://valkey.io/commands/acl-load/"]
    #[allow(async_fn_in_trait)]
    async fn acl_load(connection: &Connection) -> Result<(), Error> {
        exchange(connection, ("ACL", "LOAD")).await
    }

    #[doc = " Lists recent security events generated due to ACL rules."]
    #[doc = ""]
    #[doc = " See also `acl-log-reset`."]
    #[doc = ""]
    #[doc = " ACL LOG [ count ]"]
    #[doc = " https://valkey.io/commands/acl-log/"]
    #[allow(async_fn_in_trait)]
    async fn acl_log(
        connection: &Connection,
        count: Option<Integer>,
    ) -> Result<Vec<AclLogEntry>, Error> {
        exchange(connection, ("ACL", "LOG", count)).await
    }

    #[doc = " Lists recent security events generated due to ACL rules."]
    #[doc = ""]
    #[doc = " The `RESET` form, clearing the security events."]
    #[doc = ""]
    #[doc = " See also `acl-log`."]
    #[doc = ""]
    #[doc = " ACL LOG RESET"]
    #[doc = " https://valkey.io/commands/acl-log/"]
    #[allow(async_fn_in_trait)]
    async fn acl_log_reset(connection: &Connection) -> Result<(), Error> {
        exchange(connection, ("ACL", "LOG", "RESET")).await
    }

    #[doc = " Saves the effective ACL rules in the configured ACL file."]
    #[doc = ""]
    #[doc = " ACL SAVE"]
    #[doc = " https://valkey.io/commands/acl-save/"]
    #[allow(async_fn_in_trait)]
    async fn acl_save(connection: &Connection) -> Result<(), Error> {
        exchange(connection, ("ACL", "SAVE")).await
    }

    #[doc = " Lists all ACL users."]
    #[doc = ""]
    #[doc = " ACL USERS"]
    #[doc = " https://valkey.io/commands/acl-users/"]
    #[allow(async_fn_in_trait)]
    async fn acl_users(connection: &Connection) -> Result<Vec<String>, Error> {
        exchange(connection, ("ACL", "USERS")).await
    }

    #[doc = " Returns the authenticated username of the current connection."]
    #[doc = ""]
    #[doc = " ACL WHOAMI"]
    #[doc = " https://valkey.io/commands/acl-whoami/"]
    #[allow(async_fn_in_trait)]
    async fn acl_whoami(connection: &Connection) -> Result<String, Error> {
        exchange(connection, ("ACL", "WHOAMI")).await
    }

    #[doc = " Asynchronously rewrites the append-only file to disk."]
    #[doc = ""]
    #[doc = " BGREWRITEAOF"]
    #[doc = " https://valkey.io/commands/bgrewriteaof/"]
    #[allow(async_fn_in_trait)]
    async fn bgrewriteaof(connection: &Connection) -> Result<String, Error> {
        exchange(connection, "BGREWRITEAOF").await
    }

    #[doc = " Asynchronously saves the database(s) to disk."]
    #[doc = ""]
    #[doc = " BGSAVE [ SCHEDULE | CANCEL ]"]
    #[doc = " https://valkey.io/commands/bgsave/"]
    #[allow(async_fn_in_trait)]
    async fn bgsave(
        connection: &Connection,
        operation: Option<BgsaveOperation>,
    ) -> Result<String, Error> {
        exchange(connection, ("BGSAVE", operation)).await
    }

    #[doc = " Terminates open connections."]
    #[doc = ""]
    #[doc = " CLIENT KILL filter [ filter ... ]"]
    #[doc = " https://valkey.io/commands/client-kill/"]
    #[allow(async_fn_in_trait)]
    async fn client_kill(
        connection: &Connection,
        filters: Vec<ClientFilter>,
    ) -> Result<Integer, Error> {
        exchange(
            connection,
            ("CLIENT", "KILL", NonEmpty::new("filters", filters)),
        )
        .await
    }

    #[doc = " Lists open connections."]
    #[doc = ""]
    #[doc = " Each connection is a map of its fields, parsed from the server's text, e.g. `id`, `addr` and"]
    #[doc = " `name`."]
    #[doc = ""]
    #[doc = " CLIENT LIST [ filter [ filter ... ] ]"]
    #[doc = " https://valkey.io/commands/client-list/"]
    #[allow(async_fn_in_trait)]
    async fn client_list(
        connection: &Connection,
        filters: Option<Vec<ClientFilter>>,
    ) -> Result<Vec<Map<String, String>>, Error> {
        let command = ("CLIENT", "LIST", NonEmpty::optional("filters", filters));
        let clients: ClientList = exchange(connection, command).await?;
        Ok(clients.0)
    }

    #[doc = " Sets the client eviction mode of the connection."]
    #[doc = ""]
    #[doc = " It applies to the connection, so to every caller sharing it."]
    #[doc = ""]
    #[doc = " CLIENT NO-EVICT 〈 ON | OFF 〉"]
    #[doc = " https://valkey.io/commands/client-no-evict/"]
    #[allow(async_fn_in_trait)]
    async fn client_no_evict(connection: &Connection, enabled: bool) -> Result<(), Error> {
        exchange(
            connection,
            ("CLIENT", "NO-EVICT", if enabled { "ON" } else { "OFF" }),
        )
        .await
    }

    #[doc = " Suspends commands processing."]
    #[doc = ""]
    #[doc = " CLIENT PAUSE timeout [ WRITE | ALL ]"]
    #[doc = " https://valkey.io/commands/client-pause/"]
    #[allow(async_fn_in_trait)]
    async fn client_pause(
        connection: &Connection,
        timeout: DurationMilliseconds,
        mode: Option<ClientPauseMode>,
    ) -> Result<(), Error> {
        exchange(connection, ("CLIENT", "PAUSE", timeout, mode)).await
    }

    #[doc = " Unblocks a client blocked by a blocking command from a different connection."]
    #[doc = ""]
    #[doc = " CLIENT UNBLOCK client-id [ TIMEOUT | ERROR ]"]
    #[doc = " https://valkey.io/commands/client-unblock/"]
    #[allow(async_fn_in_trait)]
    async fn client_unblock(
        connection: &Connection,
        client_id: Integer,
        unblock_type: Option<ClientUnblockType>,
    ) -> Result<bool, Error> {
        exchange(connection, ("CLIENT", "UNBLOCK", client_id, unblock_type)).await
    }

    #[doc = " Resumes processing commands from paused clients."]
    #[doc = ""]
    #[doc = " CLIENT UNPAUSE"]
    #[doc = " https://valkey.io/commands/client-unpause/"]
    #[allow(async_fn_in_trait)]
    async fn client_unpause(connection: &Connection) -> Result<(), Error> {
        exchange(connection, ("CLIENT", "UNPAUSE")).await
    }

    #[doc = " Assigns new hash slots to a node."]
    #[doc = ""]
    #[doc = " CLUSTER ADDSLOTS slot [ slot ... ]"]
    #[doc = " https://valkey.io/commands/cluster-addslots/"]
    #[allow(async_fn_in_trait)]
    async fn cluster_addslots(connection: &Connection, slots: Vec<u16>) -> Result<(), Error> {
        exchange(
            connection,
            ("CLUSTER", "ADDSLOTS", NonEmpty::new("slots", slots)),
        )
        .await
    }

    #[doc = " Assigns new hash slot ranges to a node."]
    #[doc = ""]
    #[doc = " CLUSTER ADDSLOTSRANGE start-slot end-slot [ start-slot end-slot ... ]"]
    #[doc = " https://valkey.io/commands/cluster-addslotsrange/"]
    #[allow(async_fn_in_trait)]
    async fn cluster_addslotsrange(
        connection: &Connection,
        ranges: Vec<(u16, u16)>,
    ) -> Result<(), Error> {
        exchange(
            connection,
            ("CLUSTER", "ADDSLOTSRANGE", NonEmpty::new("ranges", ranges)),
        )
        .await
    }

    #[doc = " Advances the cluster config epoch."]
    #[doc = ""]
    #[doc = " CLUSTER BUMPEPOCH"]
    #[doc = " https://valkey.io/commands/cluster-bumpepoch/"]
    #[allow(async_fn_in_trait)]
    async fn cluster_bumpepoch(connection: &Connection) -> Result<ClusterBumpepochResult, Error> {
        exchange(connection, ("CLUSTER", "BUMPEPOCH")).await
    }

    #[doc = " Cancel all current ongoing slot migration operations."]
    #[doc = ""]
    #[doc = " CLUSTER CANCELSLOTMIGRATIONS"]
    #[doc = " https://valkey.io/commands/cluster-cancelslotmigrations/"]
    #[allow(async_fn_in_trait)]
    async fn cluster_cancelslotmigrations(connection: &Connection) -> Result<(), Error> {
        exchange(connection, ("CLUSTER", "CANCELSLOTMIGRATIONS")).await
    }

    #[doc = " Returns the number of active failure reports for a node."]
    #[doc = ""]
    #[doc = " CLUSTER COUNT-FAILURE-REPORTS node-id"]
    #[doc = " https://valkey.io/commands/cluster-count-failure-reports/"]
    #[allow(async_fn_in_trait)]
    async fn cluster_count_failure_reports(
        connection: &Connection,
        node_id: String,
    ) -> Result<Integer, Error> {
        exchange(connection, ("CLUSTER", "COUNT-FAILURE-REPORTS", node_id)).await
    }

    #[doc = " Sets hash slots as unbound for a node."]
    #[doc = ""]
    #[doc = " CLUSTER DELSLOTS slot [ slot ... ]"]
    #[doc = " https://valkey.io/commands/cluster-delslots/"]
    #[allow(async_fn_in_trait)]
    async fn cluster_delslots(connection: &Connection, slots: Vec<u16>) -> Result<(), Error> {
        exchange(
            connection,
            ("CLUSTER", "DELSLOTS", NonEmpty::new("slots", slots)),
        )
        .await
    }

    #[doc = " Sets hash slot ranges as unbound for a node."]
    #[doc = ""]
    #[doc = " CLUSTER DELSLOTSRANGE start-slot end-slot [ start-slot end-slot ... ]"]
    #[doc = " https://valkey.io/commands/cluster-delslotsrange/"]
    #[allow(async_fn_in_trait)]
    async fn cluster_delslotsrange(
        connection: &Connection,
        ranges: Vec<(u16, u16)>,
    ) -> Result<(), Error> {
        exchange(
            connection,
            ("CLUSTER", "DELSLOTSRANGE", NonEmpty::new("ranges", ranges)),
        )
        .await
    }

    #[doc = " Forces a replica to perform a manual failover of its primary."]
    #[doc = ""]
    #[doc = " CLUSTER FAILOVER [ FORCE | TAKEOVER ]"]
    #[doc = " https://valkey.io/commands/cluster-failover/"]
    #[allow(async_fn_in_trait)]
    async fn cluster_failover(
        connection: &Connection,
        mode: Option<ClusterFailoverMode>,
    ) -> Result<(), Error> {
        exchange(connection, ("CLUSTER", "FAILOVER", mode)).await
    }

    #[doc = " Deletes all slots information from a node."]
    #[doc = ""]
    #[doc = " CLUSTER FLUSHSLOTS"]
    #[doc = " https://valkey.io/commands/cluster-flushslots/"]
    #[allow(async_fn_in_trait)]
    async fn cluster_flushslots(connection: &Connection) -> Result<(), Error> {
        exchange(connection, ("CLUSTER", "FLUSHSLOTS")).await
    }

    #[doc = " Removes a node from the nodes table."]
    #[doc = ""]
    #[doc = " CLUSTER FORGET node-id"]
    #[doc = " https://valkey.io/commands/cluster-forget/"]
    #[allow(async_fn_in_trait)]
    async fn cluster_forget(connection: &Connection, node_id: String) -> Result<(), Error> {
        exchange(connection, ("CLUSTER", "FORGET", node_id)).await
    }

    #[doc = " Gets the status of ongoing and recently finished slot import and export operations."]
    #[doc = ""]
    #[doc = " CLUSTER GETSLOTMIGRATIONS"]
    #[doc = " https://valkey.io/commands/cluster-getslotmigrations/"]
    #[allow(async_fn_in_trait)]
    async fn cluster_getslotmigrations(
        connection: &Connection,
    ) -> Result<Vec<ClusterGetslotmigrationsEntry>, Error> {
        exchange(connection, ("CLUSTER", "GETSLOTMIGRATIONS")).await
    }

    #[doc = " Forces a node to handshake with another node."]
    #[doc = ""]
    #[doc = " CLUSTER MEET ip port [ cluster-bus-port ]"]
    #[doc = " https://valkey.io/commands/cluster-meet/"]
    #[allow(async_fn_in_trait)]
    async fn cluster_meet(
        connection: &Connection,
        ip: String,
        port: u16,
        cluster_bus_port: Option<u16>,
    ) -> Result<(), Error> {
        exchange(connection, ("CLUSTER", "MEET", ip, port, cluster_bus_port)).await
    }

    #[doc = " Migrate the given slots from this node to the specified nodes."]
    #[doc = ""]
    #[doc = " CLUSTER MIGRATESLOTS SLOTSRANGE start-slot end-slot [ start-slot end-slot ... ] NODE node-id [ AUTH username password ] [ SLOTSRANGE ... ]"]
    #[doc = " https://valkey.io/commands/cluster-migrateslots/"]
    #[allow(async_fn_in_trait)]
    async fn cluster_migrateslots(
        connection: &Connection,
        migrations: Vec<ClusterMigrateslotsGroup>,
    ) -> Result<(), Error> {
        exchange(
            connection,
            (
                "CLUSTER",
                "MIGRATESLOTS",
                NonEmpty::new("migrations", migrations),
            ),
        )
        .await
    }

    #[doc = " Lists the replica nodes of a primary node."]
    #[doc = ""]
    #[doc = " CLUSTER REPLICAS node-id"]
    #[doc = " https://valkey.io/commands/cluster-replicas/"]
    #[allow(async_fn_in_trait)]
    async fn cluster_replicas(
        connection: &Connection,
        node_id: String,
    ) -> Result<Vec<String>, Error> {
        exchange(connection, ("CLUSTER", "REPLICAS", node_id)).await
    }

    #[doc = " Configures a node as replica of a primary node or detaches a replica from its primary."]
    #[doc = ""]
    #[doc = " None detaches the node from its primary, `NO ONE`."]
    #[doc = ""]
    #[doc = " CLUSTER REPLICATE 〈 node-id | NO ONE 〉"]
    #[doc = " https://valkey.io/commands/cluster-replicate/"]
    #[allow(async_fn_in_trait)]
    async fn cluster_replicate(
        connection: &Connection,
        node_id: Option<String>,
    ) -> Result<(), Error> {
        // NO ONE detaches the replica from its primary
        let node = match node_id {
            Some(node_id) => vec![node_id],
            None => vec!["NO".to_string(), "ONE".to_string()],
        };
        exchange(connection, ("CLUSTER", "REPLICATE", node)).await
    }

    #[doc = " Resets a node."]
    #[doc = ""]
    #[doc = " CLUSTER RESET [ HARD | SOFT ]"]
    #[doc = " https://valkey.io/commands/cluster-reset/"]
    #[allow(async_fn_in_trait)]
    async fn cluster_reset(
        connection: &Connection,
        mode: Option<ClusterResetMode>,
    ) -> Result<(), Error> {
        exchange(connection, ("CLUSTER", "RESET", mode)).await
    }

    #[doc = " Forces a node to save the cluster configuration to disk."]
    #[doc = ""]
    #[doc = " CLUSTER SAVECONFIG"]
    #[doc = " https://valkey.io/commands/cluster-saveconfig/"]
    #[allow(async_fn_in_trait)]
    async fn cluster_saveconfig(connection: &Connection) -> Result<(), Error> {
        exchange(connection, ("CLUSTER", "SAVECONFIG")).await
    }

    #[doc = " Sets the configuration epoch for a new node."]
    #[doc = ""]
    #[doc = " CLUSTER SET-CONFIG-EPOCH config-epoch"]
    #[doc = " https://valkey.io/commands/cluster-set-config-epoch/"]
    #[allow(async_fn_in_trait)]
    async fn cluster_set_config_epoch(
        connection: &Connection,
        config_epoch: Integer,
    ) -> Result<(), Error> {
        exchange(connection, ("CLUSTER", "SET-CONFIG-EPOCH", config_epoch)).await
    }

    #[doc = " Binds a hash slot to a node."]
    #[doc = ""]
    #[doc = " CLUSTER SETSLOT slot 〈 IMPORTING node-id | MIGRATING node-id | NODE node-id | STABLE 〉 [ TIMEOUT timeout ]"]
    #[doc = " https://valkey.io/commands/cluster-setslot/"]
    #[allow(async_fn_in_trait)]
    async fn cluster_setslot(
        connection: &Connection,
        slot: u16,
        state: ClusterSetslotState,
        timeout: Option<DurationMilliseconds>,
    ) -> Result<(), Error> {
        exchange(
            connection,
            (
                "CLUSTER",
                "SETSLOT",
                slot,
                state,
                NamedOption("TIMEOUT", timeout),
            ),
        )
        .await
    }

    #[doc = " Returns the specified command log's entries."]
    #[doc = ""]
    #[doc = " COMMANDLOG GET count 〈 slow | large-request | large-reply 〉"]
    #[doc = " https://valkey.io/commands/commandlog-get/"]
    #[allow(async_fn_in_trait)]
    async fn commandlog_get(
        connection: &Connection,
        count: Integer,
        type_: CommandlogType,
    ) -> Result<Vec<CommandlogEntry>, Error> {
        exchange(connection, ("COMMANDLOG", "GET", count, type_)).await
    }

    #[doc = " Returns the number of entries in the specified type of command log."]
    #[doc = ""]
    #[doc = " COMMANDLOG LEN 〈 slow | large-request | large-reply 〉"]
    #[doc = " https://valkey.io/commands/commandlog-len/"]
    #[allow(async_fn_in_trait)]
    async fn commandlog_len(
        connection: &Connection,
        type_: CommandlogType,
    ) -> Result<Integer, Error> {
        exchange(connection, ("COMMANDLOG", "LEN", type_)).await
    }

    #[doc = " Clears all entries from the specified type of command log."]
    #[doc = ""]
    #[doc = " COMMANDLOG RESET 〈 slow | large-request | large-reply 〉"]
    #[doc = " https://valkey.io/commands/commandlog-reset/"]
    #[allow(async_fn_in_trait)]
    async fn commandlog_reset(connection: &Connection, type_: CommandlogType) -> Result<(), Error> {
        exchange(connection, ("COMMANDLOG", "RESET", type_)).await
    }

    #[doc = " Returns the effective values of configuration parameters."]
    #[doc = ""]
    #[doc = " CONFIG GET parameter [ parameter ... ]"]
    #[doc = " https://valkey.io/commands/config-get/"]
    #[allow(async_fn_in_trait)]
    async fn config_get(
        connection: &Connection,
        parameters: Vec<String>,
    ) -> Result<Map<String, String>, Error> {
        exchange(
            connection,
            ("CONFIG", "GET", NonEmpty::new("parameters", parameters)),
        )
        .await
    }

    #[doc = " Resets the server's statistics."]
    #[doc = ""]
    #[doc = " CONFIG RESETSTAT"]
    #[doc = " https://valkey.io/commands/config-resetstat/"]
    #[allow(async_fn_in_trait)]
    async fn config_resetstat(connection: &Connection) -> Result<(), Error> {
        exchange(connection, ("CONFIG", "RESETSTAT")).await
    }

    #[doc = " Persists the effective configuration to file."]
    #[doc = ""]
    #[doc = " CONFIG REWRITE"]
    #[doc = " https://valkey.io/commands/config-rewrite/"]
    #[allow(async_fn_in_trait)]
    async fn config_rewrite(connection: &Connection) -> Result<(), Error> {
        exchange(connection, ("CONFIG", "REWRITE")).await
    }

    #[doc = " Sets configuration parameters in-flight."]
    #[doc = ""]
    #[doc = " CONFIG SET parameter value [ parameter value ... ]"]
    #[doc = " https://valkey.io/commands/config-set/"]
    #[allow(async_fn_in_trait)]
    async fn config_set(
        connection: &Connection,
        parameters_and_values: Vec<(String, String)>,
    ) -> Result<(), Error> {
        let parameters_and_values = NonEmpty::new("parameters-and-values", parameters_and_values);
        exchange(connection, ("CONFIG", "SET", parameters_and_values)).await
    }

    #[doc = " Starts a coordinated failover from a server to one of its replicas."]
    #[doc = ""]
    #[doc = " FAILOVER [ TO host port [ FORCE ] ] [ ABORT ] [ TIMEOUT milliseconds ]"]
    #[doc = " https://valkey.io/commands/failover/"]
    #[allow(async_fn_in_trait)]
    async fn failover(
        connection: &Connection,
        target: Option<FailoverTarget>,
        abort: bool,
        timeout: Option<DurationMilliseconds>,
    ) -> Result<(), Error> {
        let command = (
            "FAILOVER",
            target,
            Flag("ABORT", abort),
            NamedOption("TIMEOUT", timeout),
        );
        exchange(connection, command).await
    }

    #[doc = " Returns the Unix timestamp of the last successful save to disk."]
    #[doc = ""]
    #[doc = " LASTSAVE"]
    #[doc = " https://valkey.io/commands/lastsave/"]
    #[allow(async_fn_in_trait)]
    async fn lastsave(connection: &Connection) -> Result<UnixTimeSeconds, Error> {
        exchange(connection, "LASTSAVE").await
    }

    #[doc = " Returns a human-readable latency analysis report."]
    #[doc = ""]
    #[doc = " LATENCY DOCTOR"]
    #[doc = " https://valkey.io/commands/latency-doctor/"]
    #[allow(async_fn_in_trait)]
    async fn latency_doctor(connection: &Connection) -> Result<String, Error> {
        exchange(connection, ("LATENCY", "DOCTOR")).await
    }

    #[doc = " Returns a latency graph for an event."]
    #[doc = ""]
    #[doc = " LATENCY GRAPH event"]
    #[doc = " https://valkey.io/commands/latency-graph/"]
    #[allow(async_fn_in_trait)]
    async fn latency_graph(connection: &Connection, event: String) -> Result<String, Error> {
        exchange(connection, ("LATENCY", "GRAPH", event)).await
    }

    #[doc = " Returns the cumulative distribution of latencies of a subset or all commands."]
    #[doc = ""]
    #[doc = " LATENCY HISTOGRAM [ command [ command ... ] ]"]
    #[doc = " https://valkey.io/commands/latency-histogram/"]
    #[allow(async_fn_in_trait)]
    async fn latency_histogram(
        connection: &Connection,
        commands: Option<Vec<String>>,
    ) -> Result<Map<String, LatencyHistogramEntry>, Error> {
        exchange(
            connection,
            (
                "LATENCY",
                "HISTOGRAM",
                NonEmpty::optional("commands", commands),
            ),
        )
        .await
    }

    #[doc = " Returns timestamp-latency samples for an event."]
    #[doc = ""]
    #[doc = " LATENCY HISTORY event"]
    #[doc = " https://valkey.io/commands/latency-history/"]
    #[allow(async_fn_in_trait)]
    async fn latency_history(
        connection: &Connection,
        event: String,
    ) -> Result<Vec<(UnixTimeSeconds, DurationMilliseconds)>, Error> {
        exchange(connection, ("LATENCY", "HISTORY", event)).await
    }

    #[doc = " Returns the latest latency samples for all events."]
    #[doc = ""]
    #[doc = " LATENCY LATEST"]
    #[doc = " https://valkey.io/commands/latency-latest/"]
    #[allow(async_fn_in_trait)]
    async fn latency_latest(connection: &Connection) -> Result<Vec<LatencyLatestEntry>, Error> {
        exchange(connection, ("LATENCY", "LATEST")).await
    }

    #[doc = " Resets the latency data for one or more events."]
    #[doc = ""]
    #[doc = " LATENCY RESET [ event [ event ... ] ]"]
    #[doc = " https://valkey.io/commands/latency-reset/"]
    #[allow(async_fn_in_trait)]
    async fn latency_reset(
        connection: &Connection,
        events: Option<Vec<String>>,
    ) -> Result<Integer, Error> {
        exchange(
            connection,
            ("LATENCY", "RESET", NonEmpty::optional("events", events)),
        )
        .await
    }

    #[doc = " Returns all loaded modules."]
    #[doc = ""]
    #[doc = " MODULE LIST"]
    #[doc = " https://valkey.io/commands/module-list/"]
    #[allow(async_fn_in_trait)]
    async fn module_list(connection: &Connection) -> Result<Vec<ModuleListEntry>, Error> {
        exchange(connection, ("MODULE", "LIST")).await
    }

    #[doc = " Loads a module."]
    #[doc = ""]
    #[doc = " MODULE LOAD path [ arg [ arg ... ] ]"]
    #[doc = " https://valkey.io/commands/module-load/"]
    #[allow(async_fn_in_trait)]
    async fn module_load(
        connection: &Connection,
        path: String,
        args: Option<Vec<String>>,
    ) -> Result<(), Error> {
        exchange(
            connection,
            ("MODULE", "LOAD", path, NonEmpty::optional("args", args)),
        )
        .await
    }

    #[doc = " Loads a module using extended parameters."]
    #[doc = ""]
    #[doc = " MODULE LOADEX path [ CONFIG name value [ CONFIG name value ... ] ] [ ARGS args [ args ... ] ]"]
    #[doc = " https://valkey.io/commands/module-loadex/"]
    #[allow(async_fn_in_trait)]
    async fn module_loadex(
        connection: &Connection,
        path: String,
        configs: Option<Vec<(String, String)>>,
        args: Option<Vec<String>>,
    ) -> Result<(), Error> {
        // CONFIG before each name and value, ARGS once before the arguments
        let configs = configs.map(|configs| {
            let configs = configs
                .into_iter()
                .map(|(name, value)| ("CONFIG", name, value));
            configs.collect::<Vec<_>>()
        });
        let configs = NonEmpty::optional("configs", configs);
        let args = NonEmpty::optional("args", args).map(|args| ("ARGS", args));
        exchange(connection, ("MODULE", "LOADEX", path, configs, args)).await
    }

    #[doc = " Unloads a module."]
    #[doc = ""]
    #[doc = " MODULE UNLOAD name"]
    #[doc = " https://valkey.io/commands/module-unload/"]
    #[allow(async_fn_in_trait)]
    async fn module_unload(connection: &Connection, name: String) -> Result<(), Error> {
        exchange(connection, ("MODULE", "UNLOAD", name)).await
    }

    #[doc = " Configures a server as replica of another, or promotes it to a primary."]
    #[doc = ""]
    #[doc = " None makes the server a primary, `NO ONE`."]
    #[doc = ""]
    #[doc = " REPLICAOF 〈 host port | NO ONE 〉"]
    #[doc = " https://valkey.io/commands/replicaof/"]
    #[allow(async_fn_in_trait)]
    async fn replicaof(
        connection: &Connection,
        primary: Option<(String, u16)>,
    ) -> Result<String, Error> {
        // NO ONE makes the server a primary
        let primary = match primary {
            Some((host, port)) => vec![host, port.to_string()],
            None => vec!["NO".to_string(), "ONE".to_string()],
        };
        exchange(connection, ("REPLICAOF", primary)).await
    }

    #[doc = " Returns the replication role."]
    #[doc = ""]
    #[doc = " ROLE"]
    #[doc = " https://valkey.io/commands/role/"]
    #[allow(async_fn_in_trait)]
    async fn role(connection: &Connection) -> Result<RoleReply, Error> {
        exchange(connection, "ROLE").await
    }

    #[doc = " Synchronously saves the database(s) to disk."]
    #[doc = ""]
    #[doc = " SAVE"]
    #[doc = " https://valkey.io/commands/save/"]
    #[allow(async_fn_in_trait)]
    async fn save(connection: &Connection) -> Result<(), Error> {
        exchange(connection, "SAVE").await
    }

    #[doc = " Synchronously saves the database(s) to disk and shuts down the server."]
    #[doc = ""]
    #[doc = " The server closes the connection once it stops, so a shutdown that succeeds fails the call"]
    #[doc = " with a closed error."]
    #[doc = ""]
    #[doc = " See also `shutdown-abort`."]
    #[doc = ""]
    #[doc = " SHUTDOWN [ NOSAVE | SAVE ] [ NOW ] [ FORCE ] [ SAFE ] [ FAILOVER ]"]
    #[doc = " https://valkey.io/commands/shutdown/"]
    #[allow(async_fn_in_trait)]
    async fn shutdown(
        connection: &Connection,
        save: Option<ShutdownSave>,
        modifiers: ShutdownFlags,
    ) -> Result<(), Error> {
        let command = (
            "SHUTDOWN",
            save,
            Flag("NOW", modifiers.contains(ShutdownFlags::NOW)),
            Flag("FORCE", modifiers.contains(ShutdownFlags::FORCE)),
            Flag("SAFE", modifiers.contains(ShutdownFlags::SAFE)),
            Flag("FAILOVER", modifiers.contains(ShutdownFlags::FAILOVER)),
        );
        exchange(connection, command).await
    }

    #[doc = " Synchronously saves the database(s) to disk and shuts down the server."]
    #[doc = ""]
    #[doc = " The `ABORT` form, aborting a shutdown in progress."]
    #[doc = ""]
    #[doc = " See also `shutdown`."]
    #[doc = ""]
    #[doc = " SHUTDOWN ABORT"]
    #[doc = " https://valkey.io/commands/shutdown/"]
    #[allow(async_fn_in_trait)]
    async fn shutdown_abort(connection: &Connection) -> Result<(), Error> {
        exchange(connection, ("SHUTDOWN", "ABORT")).await
    }

    #[doc = " Returns the slow log's entries."]
    #[doc = ""]
    #[doc = " SLOWLOG GET [ count ]"]
    #[doc = " https://valkey.io/commands/slowlog-get/"]
    #[allow(async_fn_in_trait)]
    async fn slowlog_get(
        connection: &Connection,
        count: Option<Integer>,
    ) -> Result<Vec<CommandlogEntry>, Error> {
        exchange(connection, ("SLOWLOG", "GET", count)).await
    }

    #[doc = " Returns the number of entries in the slow log."]
    #[doc = ""]
    #[doc = " SLOWLOG LEN"]
    #[doc = " https://valkey.io/commands/slowlog-len/"]
    #[allow(async_fn_in_trait)]
    async fn slowlog_len(connection: &Connection) -> Result<Integer, Error> {
        exchange(connection, ("SLOWLOG", "LEN")).await
    }

    #[doc = " Clears all entries from the slow log."]
    #[doc = ""]
    #[doc = " SLOWLOG RESET"]
    #[doc = " https://valkey.io/commands/slowlog-reset/"]
    #[allow(async_fn_in_trait)]
    async fn slowlog_reset(connection: &Connection) -> Result<(), Error> {
        exchange(connection, ("SLOWLOG", "RESET")).await
    }

    #[doc = " Counts the number of set bits (population counting) in a string."]
    #[doc = ""]
    #[doc = " BITCOUNT key [ start [ end [ BYTE | BIT ] ] ]"]
    #[doc = " https://valkey.io/commands/bitcount/"]
    #[allow(async_fn_in_trait)]
    async fn bitcount(
        connection: &Connection,
        key: String,
        range: Option<BitRange>,
    ) -> Result<Integer, Error> {
        exchange(connection, ("BITCOUNT", key, range)).await
    }

    #[doc = " Performs arbitrary bitfield integer operations on strings."]
    #[doc = ""]
    #[doc = " None for a write that failed with `OVERFLOW FAIL`."]
    #[doc = ""]
    #[doc = " BITFIELD key [ GET encoding offset | [ OVERFLOW WRAP | SAT | FAIL ] 〈 SET encoding offset value | INCRBY encoding offset increment 〉 [ GET encoding offset | [ OVERFLOW WRAP | SAT | FAIL ] 〈 SET encoding offset value | INCRBY encoding offset increment 〉 ... ] ]"]
    #[doc = " https://valkey.io/commands/bitfield/"]
    #[allow(async_fn_in_trait)]
    async fn bitfield(
        connection: &Connection,
        key: String,
        operations: Option<Vec<BitfieldOperation>>,
    ) -> Result<Vec<Option<Integer>>, Error> {
        exchange(
            connection,
            (
                "BITFIELD",
                key,
                NonEmpty::optional("operations", operations),
            ),
        )
        .await
    }

    #[doc = " Performs arbitrary read-only bitfield integer operations on strings."]
    #[doc = ""]
    #[doc = " BITFIELD_RO key [ GET encoding offset [ GET encoding offset ... ] ]"]
    #[doc = " https://valkey.io/commands/bitfield_ro/"]
    #[allow(async_fn_in_trait)]
    async fn bitfield_ro(
        connection: &Connection,
        key: String,
        gets: Option<Vec<BitfieldGet>>,
    ) -> Result<Vec<Integer>, Error> {
        exchange(
            connection,
            ("BITFIELD_RO", key, NonEmpty::optional("gets", gets)),
        )
        .await
    }

    #[doc = " Performs bitwise operations on multiple strings, and stores the result."]
    #[doc = ""]
    #[doc = " BITOP 〈 AND | OR | XOR | NOT 〉 destkey key [ key ... ]"]
    #[doc = " https://valkey.io/commands/bitop/"]
    #[allow(async_fn_in_trait)]
    async fn bitop(
        connection: &Connection,
        destination: String,
        operation: BitopOperation,
    ) -> Result<Integer, Error> {
        // the operation and its keys are either side of the destination
        let (operation, keys) = match operation {
            BitopOperation::And(keys) => ("AND", keys),
            BitopOperation::Or(keys) => ("OR", keys),
            BitopOperation::Xor(keys) => ("XOR", keys),
            BitopOperation::Not(key) => ("NOT", vec![key]),
        };
        exchange(
            connection,
            ("BITOP", operation, destination, NonEmpty::new("keys", keys)),
        )
        .await
    }

    #[doc = " Finds the first set (1) or clear (0) bit in a string."]
    #[doc = ""]
    #[doc = " None when no bit matches, the server's `-1`."]
    #[doc = ""]
    #[doc = " BITPOS key bit [ start [ end [ BYTE | BIT ] ] ]"]
    #[doc = " https://valkey.io/commands/bitpos/"]
    #[allow(async_fn_in_trait)]
    async fn bitpos(
        connection: &Connection,
        key: String,
        bit: bool,
        range: Option<BitRange>,
    ) -> Result<Option<Integer>, Error> {
        // -1 when there is no such bit
        let position: OrNever<Integer> =
            exchange(connection, ("BITPOS", key, Bit(bit), range)).await?;
        Ok(position.0)
    }

    #[doc = " Returns a bit value by offset."]
    #[doc = ""]
    #[doc = " GETBIT key offset"]
    #[doc = " https://valkey.io/commands/getbit/"]
    #[allow(async_fn_in_trait)]
    async fn getbit(connection: &Connection, key: String, offset: u64) -> Result<bool, Error> {
        exchange(connection, ("GETBIT", key, offset)).await
    }

    #[doc = " Sets or clears the bit at offset of the string value."]
    #[doc = ""]
    #[doc = " SETBIT key offset value"]
    #[doc = " https://valkey.io/commands/setbit/"]
    #[allow(async_fn_in_trait)]
    async fn setbit(
        connection: &Connection,
        key: String,
        offset: u64,
        value: bool,
    ) -> Result<bool, Error> {
        exchange(connection, ("SETBIT", key, offset, Bit(value))).await
    }

    #[doc = " Authenticates the connection."]
    #[doc = ""]
    #[doc = " AUTH [ username ] password"]
    #[doc = " https://valkey.io/commands/auth/"]
    #[allow(async_fn_in_trait)]
    async fn auth(
        connection: &Connection,
        username: Option<String>,
        password: String,
    ) -> Result<(), Error> {
        exchange(connection, ("AUTH", username, password)).await
    }

    #[doc = " Returns the server's liveliness response."]
    #[doc = ""]
    #[doc = " PING [ message ]"]
    #[doc = " https://valkey.io/commands/ping/"]
    #[allow(async_fn_in_trait)]
    async fn ping(connection: &Connection, message: Option<StringOrBytes>) -> Result<(), Error> {
        let reply: StringOrBytes = exchange(connection, ("PING", message.clone())).await?;
        if reply == message.unwrap_or_else(|| "PONG".into()) {
            Ok(())
        } else {
            Err(Error::Client(format!("unexpected reply: {reply:?}")))
        }
    }

    #[doc = " Signals that a cluster client is following an -ASK redirect."]
    #[doc = ""]
    #[doc = " It applies to the next command on the connection, which may be another caller's when the"]
    #[doc = " connection is shared. Use a connection of your own."]
    #[doc = ""]
    #[doc = " ASKING"]
    #[doc = " https://valkey.io/commands/asking/"]
    #[allow(async_fn_in_trait)]
    async fn asking(connection: &Connection) -> Result<(), Error> {
        exchange(connection, "ASKING").await
    }

    #[doc = " Instructs the server whether to track the keys in the next request."]
    #[doc = ""]
    #[doc = " It applies to the next command on the connection, which may be another caller's when the"]
    #[doc = " connection is shared. Use a connection of your own."]
    #[doc = ""]
    #[doc = " CLIENT CACHING 〈 YES | NO 〉"]
    #[doc = " https://valkey.io/commands/client-caching/"]
    #[allow(async_fn_in_trait)]
    async fn client_caching(connection: &Connection, enabled: bool) -> Result<(), Error> {
        exchange(
            connection,
            ("CLIENT", "CACHING", if enabled { "YES" } else { "NO" }),
        )
        .await
    }

    #[doc = " A client claims its capability."]
    #[doc = ""]
    #[doc = " CLIENT CAPA capability [ capability ... ]"]
    #[doc = " https://valkey.io/commands/client-capa/"]
    #[allow(async_fn_in_trait)]
    async fn client_capa(connection: &Connection, capabilities: Vec<String>) -> Result<(), Error> {
        exchange(
            connection,
            (
                "CLIENT",
                "CAPA",
                NonEmpty::new("capabilities", capabilities),
            ),
        )
        .await
    }

    #[doc = " Returns the name of the connection."]
    #[doc = ""]
    #[doc = " CLIENT GETNAME"]
    #[doc = " https://valkey.io/commands/client-getname/"]
    #[allow(async_fn_in_trait)]
    async fn client_getname(connection: &Connection) -> Result<Option<String>, Error> {
        exchange(connection, ("CLIENT", "GETNAME")).await
    }

    #[doc = " Returns the unique client ID of the connection."]
    #[doc = ""]
    #[doc = " CLIENT ID"]
    #[doc = " https://valkey.io/commands/client-id/"]
    #[allow(async_fn_in_trait)]
    async fn client_id(connection: &Connection) -> Result<Integer, Error> {
        exchange(connection, ("CLIENT", "ID")).await
    }

    #[doc = " Marks this client as an import source when the server is in import mode."]
    #[doc = ""]
    #[doc = " CLIENT IMPORT-SOURCE 〈 ON | OFF 〉"]
    #[doc = " https://valkey.io/commands/client-import-source/"]
    #[allow(async_fn_in_trait)]
    async fn client_import_source(connection: &Connection, enabled: bool) -> Result<(), Error> {
        exchange(connection, ("CLIENT", "IMPORT-SOURCE", OnOff(enabled))).await
    }

    #[doc = " Returns information about the connection."]
    #[doc = ""]
    #[doc = " A map of the fields, parsed from the server's text, e.g. `id`, `addr` and `name`."]
    #[doc = ""]
    #[doc = " CLIENT INFO"]
    #[doc = " https://valkey.io/commands/client-info/"]
    #[allow(async_fn_in_trait)]
    async fn client_info(connection: &Connection) -> Result<Map<String, String>, Error> {
        // a line of CLIENT LIST
        let clients: ClientList = exchange(connection, ("CLIENT", "INFO")).await?;
        clients
            .0
            .into_iter()
            .next()
            .ok_or_else(|| Error::Client("unexpected empty reply".to_string()))
    }

    #[doc = " Controls whether commands sent by the client affect the LRU/LFU of accessed keys."]
    #[doc = ""]
    #[doc = " It applies to the connection, so to every caller sharing it."]
    #[doc = ""]
    #[doc = " CLIENT NO-TOUCH 〈 ON | OFF 〉"]
    #[doc = " https://valkey.io/commands/client-no-touch/"]
    #[allow(async_fn_in_trait)]
    async fn client_no_touch(connection: &Connection, enabled: bool) -> Result<(), Error> {
        exchange(connection, ("CLIENT", "NO-TOUCH", OnOff(enabled))).await
    }

    #[doc = " Sets information specific to the client or connection."]
    #[doc = ""]
    #[doc = " CLIENT SETINFO 〈 LIB-NAME libname | LIB-VER libver 〉"]
    #[doc = " https://valkey.io/commands/client-setinfo/"]
    #[allow(async_fn_in_trait)]
    async fn client_setinfo(connection: &Connection, attr: ClientSetinfoAttr) -> Result<(), Error> {
        exchange(connection, ("CLIENT", "SETINFO", attr)).await
    }

    #[doc = " Sets the connection name."]
    #[doc = ""]
    #[doc = " CLIENT SETNAME connection-name"]
    #[doc = " https://valkey.io/commands/client-setname/"]
    #[allow(async_fn_in_trait)]
    async fn client_setname(connection: &Connection, connection_name: String) -> Result<(), Error> {
        exchange(connection, ("CLIENT", "SETNAME", connection_name)).await
    }

    #[doc = " Controls server-assisted client-side caching for the connection."]
    #[doc = ""]
    #[doc = " Invalidations arrive as push messages, see `connection.dead-letters`. It applies to the"]
    #[doc = " connection, so to every caller sharing it."]
    #[doc = ""]
    #[doc = " CLIENT TRACKING 〈 ON | OFF 〉 [ PREFIX prefix [ PREFIX prefix ... ] ] [ BCAST ] [ OPTIN ] [ OPTOUT ] [ NOLOOP ]"]
    #[doc = " https://valkey.io/commands/client-tracking/"]
    #[allow(async_fn_in_trait)]
    async fn client_tracking(
        connection: &Connection,
        options: Option<ClientTrackingOptions>,
    ) -> Result<(), Error> {
        // ON with options, OFF without
        let status = OnOff(options.is_some());
        exchange(connection, ("CLIENT", "TRACKING", status, options)).await
    }

    #[doc = " Returns information about server-assisted client-side caching for the connection."]
    #[doc = ""]
    #[doc = " CLIENT TRACKINGINFO"]
    #[doc = " https://valkey.io/commands/client-trackinginfo/"]
    #[allow(async_fn_in_trait)]
    async fn client_trackinginfo(
        connection: &Connection,
    ) -> Result<ClientTrackinginfoReply, Error> {
        exchange(connection, ("CLIENT", "TRACKINGINFO")).await
    }

    #[doc = " Returns a count of commands."]
    #[doc = ""]
    #[doc = " COMMAND COUNT"]
    #[doc = " https://valkey.io/commands/command-count/"]
    #[allow(async_fn_in_trait)]
    async fn command_count(connection: &Connection) -> Result<Integer, Error> {
        exchange(connection, ("COMMAND", "COUNT")).await
    }

    #[doc = " Returns detailed information about all commands."]
    #[doc = ""]
    #[doc = " COMMAND"]
    #[doc = " https://valkey.io/commands/command/"]
    #[allow(async_fn_in_trait)]
    async fn command(connection: &Connection) -> Result<Vec<CommandInfoReply>, Error> {
        let commands: CommandInfoList = exchange(connection, "COMMAND").await?;
        Ok(commands.0)
    }

    #[doc = " Returns documentary information about one, multiple or all commands."]
    #[doc = ""]
    #[doc = " A container is followed by its subcommands. Commands that do not exist are left out."]
    #[doc = ""]
    #[doc = " COMMAND DOCS [ command-name [ command-name ... ] ]"]
    #[doc = " https://valkey.io/commands/command-docs/"]
    #[allow(async_fn_in_trait)]
    async fn command_docs(
        connection: &Connection,
        command_names: Option<Vec<String>>,
    ) -> Result<Vec<CommandDocsReply>, Error> {
        let command_names = NonEmpty::optional("command-names", command_names);
        let commands: CommandDocsList =
            exchange(connection, ("COMMAND", "DOCS", command_names)).await?;
        Ok(commands.0)
    }

    #[doc = " Returns information about one, multiple or all commands."]
    #[doc = ""]
    #[doc = " A container is followed by its subcommands. Commands that do not exist are left out."]
    #[doc = ""]
    #[doc = " COMMAND INFO [ command-name [ command-name ... ] ]"]
    #[doc = " https://valkey.io/commands/command-info/"]
    #[allow(async_fn_in_trait)]
    async fn command_info(
        connection: &Connection,
        command_names: Option<Vec<String>>,
    ) -> Result<Vec<CommandInfoReply>, Error> {
        let command_names = NonEmpty::optional("command-names", command_names);
        let commands: CommandInfoList =
            exchange(connection, ("COMMAND", "INFO", command_names)).await?;
        Ok(commands.0)
    }

    #[doc = " Extracts the key names from an arbitrary command."]
    #[doc = ""]
    #[doc = " COMMAND GETKEYS command [ arg [ arg ... ] ]"]
    #[doc = " https://valkey.io/commands/command-getkeys/"]
    #[allow(async_fn_in_trait)]
    async fn command_getkeys(
        connection: &Connection,
        command: Vec<String>,
    ) -> Result<Vec<String>, Error> {
        exchange(
            connection,
            ("COMMAND", "GETKEYS", NonEmpty::new("command", command)),
        )
        .await
    }

    #[doc = " Extracts the key names and access flags for an arbitrary command."]
    #[doc = ""]
    #[doc = " COMMAND GETKEYSANDFLAGS command [ arg [ arg ... ] ]"]
    #[doc = " https://valkey.io/commands/command-getkeysandflags/"]
    #[allow(async_fn_in_trait)]
    async fn command_getkeysandflags(
        connection: &Connection,
        command: Vec<String>,
    ) -> Result<Vec<(String, Vec<String>)>, Error> {
        exchange(
            connection,
            (
                "COMMAND",
                "GETKEYSANDFLAGS",
                NonEmpty::new("command", command),
            ),
        )
        .await
    }

    #[doc = " Returns a list of command names."]
    #[doc = ""]
    #[doc = " COMMAND LIST [ FILTERBY 〈 MODULE module-name | ACLCAT category | PATTERN pattern 〉 ]"]
    #[doc = " https://valkey.io/commands/command-list/"]
    #[allow(async_fn_in_trait)]
    async fn command_list(
        connection: &Connection,
        filter: Option<CommandListFilter>,
    ) -> Result<Vec<String>, Error> {
        exchange(connection, ("COMMAND", "LIST", filter)).await
    }

    #[doc = " Returns the given string."]
    #[doc = ""]
    #[doc = " ECHO message"]
    #[doc = " https://valkey.io/commands/echo/"]
    #[allow(async_fn_in_trait)]
    async fn echo(connection: &Connection, message: StringOrBytes) -> Result<StringOrBytes, Error> {
        exchange(connection, ("ECHO", message)).await
    }

    #[doc = " Enables read-only queries for a connection to a Valkey replica node."]
    #[doc = ""]
    #[doc = " It applies to the connection, so to every caller sharing it."]
    #[doc = ""]
    #[doc = " READONLY"]
    #[doc = " https://valkey.io/commands/readonly/"]
    #[allow(async_fn_in_trait)]
    async fn readonly(connection: &Connection) -> Result<(), Error> {
        exchange(connection, "READONLY").await
    }

    #[doc = " Enables read-write queries for a connection to a Valkey replica node."]
    #[doc = ""]
    #[doc = " It applies to the connection, so to every caller sharing it."]
    #[doc = ""]
    #[doc = " READWRITE"]
    #[doc = " https://valkey.io/commands/readwrite/"]
    #[allow(async_fn_in_trait)]
    async fn readwrite(connection: &Connection) -> Result<(), Error> {
        exchange(connection, "READWRITE").await
    }

    #[doc = " Changes the selected database."]
    #[doc = ""]
    #[doc = " It applies to the connection, so to every caller sharing it."]
    #[doc = ""]
    #[doc = " SELECT index"]
    #[doc = " https://valkey.io/commands/select/"]
    #[allow(async_fn_in_trait)]
    async fn select(connection: &Connection, index: Integer) -> Result<(), Error> {
        exchange(connection, ("SELECT", index)).await
    }

    #[doc = " Blocks until the asynchronous replication of all preceding write commands sent by the connection is completed."]
    #[doc = ""]
    #[doc = " The connection waits too, replies to commands sent after it on the connection wait behind"]
    #[doc = " it. Use a connection of your own."]
    #[doc = ""]
    #[doc = " WAIT numreplicas timeout"]
    #[doc = " https://valkey.io/commands/wait/"]
    #[allow(async_fn_in_trait)]
    async fn wait(
        connection: &Connection,
        numreplicas: Integer,
        timeout: DurationMilliseconds,
    ) -> Result<Integer, Error> {
        exchange(connection, ("WAIT", numreplicas, timeout)).await
    }

    #[doc = " Blocks until all of the preceding write commands sent by the connection are written to the append-only file of the primary and/or replicas."]
    #[doc = ""]
    #[doc = " The connection waits too, replies to commands sent after it on the connection wait behind"]
    #[doc = " it. Use a connection of your own."]
    #[doc = ""]
    #[doc = " WAITAOF numlocal numreplicas timeout"]
    #[doc = " https://valkey.io/commands/waitaof/"]
    #[allow(async_fn_in_trait)]
    async fn waitaof(
        connection: &Connection,
        numlocal: Integer,
        numreplicas: Integer,
        timeout: DurationMilliseconds,
    ) -> Result<(Integer, Integer), Error> {
        exchange(connection, ("WAITAOF", numlocal, numreplicas, timeout)).await
    }

    #[doc = " Adds one or more members to a geospatial index."]
    #[doc = ""]
    #[doc = " GEOADD key [ NX | XX ] [ CH ] longitude latitude member [ longitude latitude member ... ]"]
    #[doc = " https://valkey.io/commands/geoadd/"]
    #[allow(async_fn_in_trait)]
    async fn geoadd(
        connection: &Connection,
        key: String,
        condition: Option<GeoaddCondition>,
        changed: bool,
        coordinates_and_members: Vec<(GeoCoordinates, StringOrBytes)>,
    ) -> Result<Integer, Error> {
        exchange(
            connection,
            (
                "GEOADD",
                key,
                condition,
                Flag("CH", changed),
                NonEmpty::new("coordinates-and-members", coordinates_and_members),
            ),
        )
        .await
    }

    #[doc = " Returns the distance between two members of a geospatial index."]
    #[doc = ""]
    #[doc = " GEODIST key member1 member2 [ m | km | ft | mi ]"]
    #[doc = " https://valkey.io/commands/geodist/"]
    #[allow(async_fn_in_trait)]
    async fn geodist(
        connection: &Connection,
        key: String,
        member1: StringOrBytes,
        member2: StringOrBytes,
        unit: Option<GeoUnit>,
    ) -> Result<Option<Double>, Error> {
        exchange(connection, ("GEODIST", key, member1, member2, unit)).await
    }

    #[doc = " Returns members from a geospatial index as geohash strings."]
    #[doc = ""]
    #[doc = " GEOHASH key [ member [ member ... ] ]"]
    #[doc = " https://valkey.io/commands/geohash/"]
    #[allow(async_fn_in_trait)]
    async fn geohash(
        connection: &Connection,
        key: String,
        members: Option<Vec<StringOrBytes>>,
    ) -> Result<Vec<Option<String>>, Error> {
        exchange(
            connection,
            ("GEOHASH", key, NonEmpty::optional("members", members)),
        )
        .await
    }

    #[doc = " Returns the longitude and latitude of members from a geospatial index."]
    #[doc = ""]
    #[doc = " GEOPOS key [ member [ member ... ] ]"]
    #[doc = " https://valkey.io/commands/geopos/"]
    #[allow(async_fn_in_trait)]
    async fn geopos(
        connection: &Connection,
        key: String,
        members: Option<Vec<StringOrBytes>>,
    ) -> Result<Vec<Option<GeoCoordinates>>, Error> {
        exchange(
            connection,
            ("GEOPOS", key, NonEmpty::optional("members", members)),
        )
        .await
    }

    #[doc = " Queries a geospatial index for members inside an area of a box, circle, or a polygon."]
    #[doc = ""]
    #[doc = " See also `geosearch-with`."]
    #[doc = ""]
    #[doc = " GEOSEARCH key [ FROMMEMBER member | FROMLONLAT longitude latitude ] 〈 BYRADIUS radius 〈 m | km | ft | mi 〉 | BYBOX width height 〈 m | km | ft | mi 〉 | BYPOLYGON num-vertices longitude latitude [ longitude latitude ... ] 〉 [ ASC | DESC ] [ COUNT count [ ANY ] ]"]
    #[doc = " https://valkey.io/commands/geosearch/"]
    #[allow(async_fn_in_trait)]
    async fn geosearch(
        connection: &Connection,
        key: String,
        shape: GeosearchShape,
        order: Option<SortOrder>,
        count: Option<GeosearchCount>,
    ) -> Result<Vec<StringOrBytes>, Error> {
        exchange(connection, ("GEOSEARCH", key, shape, order, count)).await
    }

    #[doc = " Queries a geospatial index for members inside an area of a box, circle, or a polygon."]
    #[doc = ""]
    #[doc = " Returns each member with the details asked for, `WITHCOORD`, `WITHDIST` and `WITHHASH`."]
    #[doc = ""]
    #[doc = " See also `geosearch`."]
    #[doc = ""]
    #[doc = " GEOSEARCH key [ FROMMEMBER member | FROMLONLAT longitude latitude ] 〈 BYRADIUS radius 〈 m | km | ft | mi 〉 | BYBOX width height 〈 m | km | ft | mi 〉 | BYPOLYGON num-vertices longitude latitude [ longitude latitude ... ] 〉 [ ASC | DESC ] [ COUNT count [ ANY ] ] [ WITHCOORD ] [ WITHDIST ] [ WITHHASH ]"]
    #[doc = " https://valkey.io/commands/geosearch/"]
    #[allow(async_fn_in_trait)]
    async fn geosearch_with(
        connection: &Connection,
        key: String,
        shape: GeosearchShape,
        order: Option<SortOrder>,
        count: Option<GeosearchCount>,
        details: GeosearchWithFlags,
    ) -> Result<Vec<GeosearchWithResult>, Error> {
        let command = (
            "GEOSEARCH",
            key,
            shape,
            order,
            count,
            Flag("WITHCOORD", details.contains(GeosearchWithFlags::WITHCOORD)),
            Flag("WITHDIST", details.contains(GeosearchWithFlags::WITHDIST)),
            Flag("WITHHASH", details.contains(GeosearchWithFlags::WITHHASH)),
        );
        exchange(connection, command).await
    }

    #[doc = " Queries a geospatial index for members inside an area of a box, a circle, or a polygon, optionally stores the result."]
    #[doc = ""]
    #[doc = " GEOSEARCHSTORE destination source [ FROMMEMBER member | FROMLONLAT longitude latitude ] 〈 BYRADIUS radius 〈 m | km | ft | mi 〉 | BYBOX width height 〈 m | km | ft | mi 〉 | BYPOLYGON num-vertices longitude latitude [ longitude latitude ... ] 〉 [ ASC | DESC ] [ COUNT count [ ANY ] ] [ STOREDIST ]"]
    #[doc = " https://valkey.io/commands/geosearchstore/"]
    #[allow(async_fn_in_trait)]
    async fn geosearchstore(
        connection: &Connection,
        destination: String,
        source: String,
        shape: GeosearchShape,
        order: Option<SortOrder>,
        count: Option<GeosearchCount>,
        storedist: bool,
    ) -> Result<Integer, Error> {
        exchange(
            connection,
            (
                "GEOSEARCHSTORE",
                destination,
                source,
                shape,
                order,
                count,
                Flag("STOREDIST", storedist),
            ),
        )
        .await
    }

    #[doc = " Deletes one or more fields and their values from a hash."]
    #[doc = ""]
    #[doc = " HDEL key field [ field ... ]"]
    #[doc = " https://valkey.io/commands/hdel/"]
    #[allow(async_fn_in_trait)]
    async fn hdel(
        connection: &Connection,
        key: String,
        fields: Vec<String>,
    ) -> Result<Integer, Error> {
        exchange(connection, ("HDEL", key, NonEmpty::new("fields", fields))).await
    }

    #[doc = " Determines whether a field exists in a hash."]
    #[doc = ""]
    #[doc = " HEXISTS key field"]
    #[doc = " https://valkey.io/commands/hexists/"]
    #[allow(async_fn_in_trait)]
    async fn hexists(connection: &Connection, key: String, field: String) -> Result<bool, Error> {
        exchange(connection, ("HEXISTS", key, field)).await
    }

    #[doc = " Returns Unix timestamps in seconds since the epoch at which the given key's field(s) will expire."]
    #[doc = ""]
    #[doc = " HEXPIRETIME key FIELDS numfields field [ field ... ]"]
    #[doc = " https://valkey.io/commands/hexpiretime/"]
    #[allow(async_fn_in_trait)]
    async fn hexpiretime(
        connection: &Connection,
        key: String,
        fields: Vec<String>,
    ) -> Result<Vec<ExpiretimeReply>, Error> {
        exchange(
            connection,
            ("HEXPIRETIME", key, Fields(NonEmpty::new("fields", fields))),
        )
        .await
    }

    #[doc = " Returns the values of one or more fields and deletes them from a hash."]
    #[doc = ""]
    #[doc = " HGETDEL key FIELDS numfields field [ field ... ]"]
    #[doc = " https://valkey.io/commands/hgetdel/"]
    #[allow(async_fn_in_trait)]
    async fn hgetdel(
        connection: &Connection,
        key: String,
        fields: Vec<String>,
    ) -> Result<Vec<Option<StringOrBytes>>, Error> {
        exchange(
            connection,
            ("HGETDEL", key, Fields(NonEmpty::new("fields", fields))),
        )
        .await
    }

    #[doc = " Gets the value of one or more fields of a given hash key, and optionally sets their expiration time or time-to-live (TTL)."]
    #[doc = ""]
    #[doc = " HGETEX key [ EX seconds | PX milliseconds | EXAT unix-time-seconds | PXAT unix-time-milliseconds | PERSIST ] FIELDS numfields field [ field ... ]"]
    #[doc = " https://valkey.io/commands/hgetex/"]
    #[allow(async_fn_in_trait)]
    async fn hgetex(
        connection: &Connection,
        key: String,
        ttl: Option<HgetexTtl>,
        fields: Vec<String>,
    ) -> Result<Vec<Option<StringOrBytes>>, Error> {
        exchange(
            connection,
            ("HGETEX", key, ttl, Fields(NonEmpty::new("fields", fields))),
        )
        .await
    }

    #[doc = " Remove the existing expiration on a hash key's field(s)."]
    #[doc = ""]
    #[doc = " HPERSIST key FIELDS numfields field [ field ... ]"]
    #[doc = " https://valkey.io/commands/hpersist/"]
    #[allow(async_fn_in_trait)]
    async fn hpersist(
        connection: &Connection,
        key: String,
        fields: Vec<String>,
    ) -> Result<Vec<HpersistResult>, Error> {
        exchange(
            connection,
            ("HPERSIST", key, Fields(NonEmpty::new("fields", fields))),
        )
        .await
    }

    #[doc = " Sets the expiration time of one or more hash fields in milliseconds."]
    #[doc = ""]
    #[doc = " HPEXPIRE key milliseconds [ NX | XX | GT | LT ] FIELDS numfields field [ field ... ]"]
    #[doc = " https://valkey.io/commands/hpexpire/"]
    #[allow(async_fn_in_trait)]
    async fn hpexpire(
        connection: &Connection,
        key: String,
        milliseconds: DurationMilliseconds,
        mode: Option<TtlMode>,
        fields: Vec<String>,
    ) -> Result<Vec<HexpireResult>, Error> {
        let command = (
            "HPEXPIRE",
            key,
            milliseconds,
            mode,
            Fields(NonEmpty::new("fields", fields)),
        );
        exchange(connection, command).await
    }

    #[doc = " Sets the expiration time of one or more hash fields to a Unix timestamp in milliseconds."]
    #[doc = ""]
    #[doc = " HPEXPIREAT key unix-time-milliseconds [ NX | XX | GT | LT ] FIELDS numfields field [ field ... ]"]
    #[doc = " https://valkey.io/commands/hpexpireat/"]
    #[allow(async_fn_in_trait)]
    async fn hpexpireat(
        connection: &Connection,
        key: String,
        milliseconds: UnixTimeMilliseconds,
        mode: Option<TtlMode>,
        fields: Vec<String>,
    ) -> Result<Vec<HexpireResult>, Error> {
        let command = (
            "HPEXPIREAT",
            key,
            milliseconds,
            mode,
            Fields(NonEmpty::new("fields", fields)),
        );
        exchange(connection, command).await
    }

    #[doc = " Returns the Unix timestamp in milliseconds since Unix epoch at which the given key's field(s) will expire."]
    #[doc = ""]
    #[doc = " HPEXPIRETIME key FIELDS numfields field [ field ... ]"]
    #[doc = " https://valkey.io/commands/hpexpiretime/"]
    #[allow(async_fn_in_trait)]
    async fn hpexpiretime(
        connection: &Connection,
        key: String,
        fields: Vec<String>,
    ) -> Result<Vec<PexpiretimeReply>, Error> {
        exchange(
            connection,
            ("HPEXPIRETIME", key, Fields(NonEmpty::new("fields", fields))),
        )
        .await
    }

    #[doc = " Returns the remaining time to live in milliseconds of a hash key's field(s) that have an associated expiration."]
    #[doc = ""]
    #[doc = " HPTTL key FIELDS numfields field [ field ... ]"]
    #[doc = " https://valkey.io/commands/hpttl/"]
    #[allow(async_fn_in_trait)]
    async fn hpttl(
        connection: &Connection,
        key: String,
        fields: Vec<String>,
    ) -> Result<Vec<PttlReply>, Error> {
        exchange(
            connection,
            ("HPTTL", key, Fields(NonEmpty::new("fields", fields))),
        )
        .await
    }

    #[doc = " Sets the value of one or more fields of a given hash key, and optionally sets their expiration time."]
    #[doc = ""]
    #[doc = " HSETEX key [ NX | XX ] [ FNX | FXX ] [ EX seconds | PX milliseconds | EXAT unix-time-seconds | PXAT unix-time-milliseconds | KEEPTTL ] FIELDS numfields field value [ field value ... ]"]
    #[doc = " https://valkey.io/commands/hsetex/"]
    #[allow(async_fn_in_trait)]
    async fn hsetex(
        connection: &Connection,
        key: String,
        condition: Option<HsetexCondition>,
        field_condition: Option<HsetexFieldCondition>,
        ttl: Option<HsetexTtl>,
        field_and_values: Vec<(String, StringOrBytes)>,
    ) -> Result<bool, Error> {
        let command = (
            "HSETEX",
            key,
            condition,
            field_condition,
            ttl,
            Fields(NonEmpty::new("field-and-values", field_and_values)),
        );
        exchange(connection, command).await
    }

    #[doc = " Returns the remaining time to live (in seconds) of a hash key's field(s) that have an associated expiration."]
    #[doc = ""]
    #[doc = " HTTL key FIELDS numfields field [ field ... ]"]
    #[doc = " https://valkey.io/commands/httl/"]
    #[allow(async_fn_in_trait)]
    async fn httl(
        connection: &Connection,
        key: String,
        fields: Vec<String>,
    ) -> Result<Vec<TtlReply>, Error> {
        exchange(
            connection,
            ("HTTL", key, Fields(NonEmpty::new("fields", fields))),
        )
        .await
    }

    #[doc = " Sets the expiration time of one or more hash fields in seconds."]
    #[doc = ""]
    #[doc = " HEXPIRE key seconds [ NX | XX | GT | LT ] FIELDS numfields field [ field ... ]"]
    #[doc = " https://valkey.io/commands/hexpire/"]
    #[allow(async_fn_in_trait)]
    async fn hexpire(
        connection: &Connection,
        key: String,
        seconds: DurationSeconds,
        mode: Option<TtlMode>,
        fields: Vec<String>,
    ) -> Result<Vec<HexpireResult>, Error> {
        exchange(
            connection,
            (
                "HEXPIRE",
                key,
                seconds,
                mode,
                Fields(NonEmpty::new("fields", fields)),
            ),
        )
        .await
    }

    #[doc = " Sets the expiration time of one or more hash fields to a Unix timestamp."]
    #[doc = ""]
    #[doc = " HEXPIREAT key unix-time-seconds [ NX | XX | GT | LT ] FIELDS numfields field [ field ... ]"]
    #[doc = " https://valkey.io/commands/hexpireat/"]
    #[allow(async_fn_in_trait)]
    async fn hexpireat(
        connection: &Connection,
        key: String,
        seconds: UnixTimeSeconds,
        mode: Option<TtlMode>,
        fields: Vec<String>,
    ) -> Result<Vec<HexpireResult>, Error> {
        exchange(
            connection,
            (
                "HEXPIREAT",
                key,
                seconds,
                mode,
                Fields(NonEmpty::new("fields", fields)),
            ),
        )
        .await
    }

    #[doc = " Returns the value of a field in a hash."]
    #[doc = ""]
    #[doc = " HGET key field"]
    #[doc = " https://valkey.io/commands/hget/"]
    #[allow(async_fn_in_trait)]
    async fn hget(
        connection: &Connection,
        key: String,
        field: String,
    ) -> Result<Option<StringOrBytes>, Error> {
        exchange(connection, ("HGET", key, field)).await
    }

    #[doc = " Returns all fields and values in a hash."]
    #[doc = ""]
    #[doc = " HGETALL key"]
    #[doc = " https://valkey.io/commands/hgetall/"]
    #[allow(async_fn_in_trait)]
    async fn hgetall(
        connection: &Connection,
        key: String,
    ) -> Result<Map<String, StringOrBytes>, Error> {
        exchange(connection, ("HGETALL", key)).await
    }

    #[doc = " Increments the integer value of a field in a hash by a number."]
    #[doc = ""]
    #[doc = " HINCRBY key field increment"]
    #[doc = " https://valkey.io/commands/hincrby/"]
    #[allow(async_fn_in_trait)]
    async fn hincrby(
        connection: &Connection,
        key: String,
        field: String,
        increment: Integer,
    ) -> Result<Integer, Error> {
        exchange(connection, ("HINCRBY", key, field, increment)).await
    }

    #[doc = " Increments the floating point value of a field by a number."]
    #[doc = ""]
    #[doc = " HINCRBYFLOAT key field increment"]
    #[doc = " https://valkey.io/commands/hincrbyfloat/"]
    #[allow(async_fn_in_trait)]
    async fn hincrbyfloat(
        connection: &Connection,
        key: String,
        field: String,
        increment: Double,
    ) -> Result<Double, Error> {
        exchange(connection, ("HINCRBYFLOAT", key, field, increment)).await
    }

    #[doc = " Returns all fields in a hash."]
    #[doc = ""]
    #[doc = " HKEYS key"]
    #[doc = " https://valkey.io/commands/hkeys/"]
    #[allow(async_fn_in_trait)]
    async fn hkeys(connection: &Connection, key: String) -> Result<Vec<String>, Error> {
        exchange(connection, ("HKEYS", key)).await
    }

    #[doc = " Returns the number of fields in a hash."]
    #[doc = ""]
    #[doc = " HLEN key"]
    #[doc = " https://valkey.io/commands/hlen/"]
    #[allow(async_fn_in_trait)]
    async fn hlen(connection: &Connection, key: String) -> Result<Integer, Error> {
        exchange(connection, ("HLEN", key)).await
    }

    #[doc = " Returns the values of all fields in a hash."]
    #[doc = ""]
    #[doc = " HMGET key field [ field ... ]"]
    #[doc = " https://valkey.io/commands/hmget/"]
    #[allow(async_fn_in_trait)]
    async fn hmget(
        connection: &Connection,
        key: String,
        fields: Vec<String>,
    ) -> Result<Vec<Option<StringOrBytes>>, Error> {
        exchange(connection, ("HMGET", key, NonEmpty::new("fields", fields))).await
    }

    #[doc = " Sets the values of multiple fields."]
    #[doc = ""]
    #[doc = " HMSET key field value [ field value ... ]"]
    #[doc = " https://valkey.io/commands/hmset/"]
    #[allow(async_fn_in_trait)]
    async fn hmset(
        connection: &Connection,
        key: String,
        field_and_values: Vec<(String, StringOrBytes)>,
    ) -> Result<(), Error> {
        exchange(
            connection,
            (
                "HMSET",
                key,
                NonEmpty::new("field-and-values", field_and_values),
            ),
        )
        .await
    }

    #[doc = " Returns one or more random fields from a hash."]
    #[doc = ""]
    #[doc = " Without a count, at most one field."]
    #[doc = ""]
    #[doc = " See also `hrandfield-withvalues`."]
    #[doc = ""]
    #[doc = " HRANDFIELD key [ count ]"]
    #[doc = " https://valkey.io/commands/hrandfield/"]
    #[allow(async_fn_in_trait)]
    async fn hrandfield(
        connection: &Connection,
        key: String,
        count: Option<Integer>,
    ) -> Result<Vec<String>, Error> {
        if count.is_some() {
            return exchange(connection, ("HRANDFIELD", key, count)).await;
        }
        // without a count the reply is the field itself, or null
        let field: Option<String> = exchange(connection, ("HRANDFIELD", key)).await?;
        Ok(field.into_iter().collect())
    }

    #[doc = " Returns one or more random fields from a hash."]
    #[doc = ""]
    #[doc = " The `WITHVALUES` form, the fields and their values."]
    #[doc = ""]
    #[doc = " See also `hrandfield`."]
    #[doc = ""]
    #[doc = " HRANDFIELD key count WITHVALUES"]
    #[doc = " https://valkey.io/commands/hrandfield/"]
    #[allow(async_fn_in_trait)]
    async fn hrandfield_withvalues(
        connection: &Connection,
        key: String,
        count: Integer,
    ) -> Result<Vec<(String, StringOrBytes)>, Error> {
        exchange(connection, ("HRANDFIELD", key, count, "WITHVALUES")).await
    }

    #[doc = " Iterates over fields and values of a hash."]
    #[doc = ""]
    #[doc = " See also `hscan-novalues`."]
    #[doc = ""]
    #[doc = " HSCAN key cursor [ MATCH pattern ] [ COUNT count ]"]
    #[doc = " https://valkey.io/commands/hscan/"]
    #[allow(async_fn_in_trait)]
    async fn hscan(
        connection: &Connection,
        key: String,
        cursor: Cursor,
        match_: Option<String>,
        count: Option<Integer>,
    ) -> Result<(Option<Cursor>, Map<String, StringOrBytes>), Error> {
        let (cursor, items): (NextCursor, Map<String, StringOrBytes>) = exchange(
            connection,
            (
                "HSCAN",
                key,
                cursor,
                match_.map(|m| ("MATCH", m)),
                count.map(|c| ("COUNT", c)),
            ),
        )
        .await?;
        Ok((cursor.0, items))
    }

    #[doc = " Iterates over fields and values of a hash."]
    #[doc = ""]
    #[doc = " The `NOVALUES` form, the fields only."]
    #[doc = ""]
    #[doc = " See also `hscan`."]
    #[doc = ""]
    #[doc = " HSCAN key cursor [ MATCH pattern ] [ COUNT count ] NOVALUES"]
    #[doc = " https://valkey.io/commands/hscan/"]
    #[allow(async_fn_in_trait)]
    async fn hscan_novalues(
        connection: &Connection,
        key: String,
        cursor: Cursor,
        match_: Option<String>,
        count: Option<Integer>,
    ) -> Result<(Option<Cursor>, Vec<String>), Error> {
        let (cursor, items): (NextCursor, Vec<String>) = exchange(
            connection,
            (
                "HSCAN",
                key,
                cursor,
                match_.map(|m| ("MATCH", m)),
                count.map(|c| ("COUNT", c)),
                "NOVALUES",
            ),
        )
        .await?;
        Ok((cursor.0, items))
    }

    #[doc = " Creates or modifies the value of a field in a hash."]
    #[doc = ""]
    #[doc = " HSET key field value [ field value ... ]"]
    #[doc = " https://valkey.io/commands/hset/"]
    #[allow(async_fn_in_trait)]
    async fn hset(
        connection: &Connection,
        key: String,
        field_and_values: Vec<(String, StringOrBytes)>,
    ) -> Result<Integer, Error> {
        exchange(
            connection,
            (
                "HSET",
                key,
                NonEmpty::new("field-and-values", field_and_values),
            ),
        )
        .await
    }

    #[doc = " Sets the value of a field in a hash only when the field doesn't exist."]
    #[doc = ""]
    #[doc = " HSETNX key field value"]
    #[doc = " https://valkey.io/commands/hsetnx/"]
    #[allow(async_fn_in_trait)]
    async fn hsetnx(
        connection: &Connection,
        key: String,
        field: String,
        value: StringOrBytes,
    ) -> Result<bool, Error> {
        exchange(connection, ("HSETNX", key, field, value)).await
    }

    #[doc = " Returns the length of the value of a field."]
    #[doc = ""]
    #[doc = " HSTRLEN key field"]
    #[doc = " https://valkey.io/commands/hstrlen/"]
    #[allow(async_fn_in_trait)]
    async fn hstrlen(
        connection: &Connection,
        key: String,
        field: String,
    ) -> Result<Integer, Error> {
        exchange(connection, ("HSTRLEN", key, field)).await
    }

    #[doc = " Returns all values in a hash."]
    #[doc = ""]
    #[doc = " HVALS key"]
    #[doc = " https://valkey.io/commands/hvals/"]
    #[allow(async_fn_in_trait)]
    async fn hvals(connection: &Connection, key: String) -> Result<Vec<StringOrBytes>, Error> {
        exchange(connection, ("HVALS", key)).await
    }

    #[doc = " Adds elements to a HyperLogLog key."]
    #[doc = ""]
    #[doc = " PFADD key [ element [ element ... ] ]"]
    #[doc = " https://valkey.io/commands/pfadd/"]
    #[allow(async_fn_in_trait)]
    async fn pfadd(
        connection: &Connection,
        key: String,
        elements: Option<Vec<StringOrBytes>>,
    ) -> Result<bool, Error> {
        exchange(
            connection,
            ("PFADD", key, NonEmpty::optional("elements", elements)),
        )
        .await
    }

    #[doc = " Returns the approximated cardinality of the set(s) observed by the HyperLogLog key(s)."]
    #[doc = ""]
    #[doc = " PFCOUNT key [ key ... ]"]
    #[doc = " https://valkey.io/commands/pfcount/"]
    #[allow(async_fn_in_trait)]
    async fn pfcount(connection: &Connection, keys: Vec<String>) -> Result<Integer, Error> {
        exchange(connection, ("PFCOUNT", NonEmpty::new("keys", keys))).await
    }

    #[doc = " Merges one or more HyperLogLog values into a single key."]
    #[doc = ""]
    #[doc = " PFMERGE destkey [ sourcekey [ sourcekey ... ] ]"]
    #[doc = " https://valkey.io/commands/pfmerge/"]
    #[allow(async_fn_in_trait)]
    async fn pfmerge(
        connection: &Connection,
        destination: String,
        sources: Option<Vec<String>>,
    ) -> Result<(), Error> {
        exchange(
            connection,
            (
                "PFMERGE",
                destination,
                NonEmpty::optional("sources", sources),
            ),
        )
        .await
    }

    #[doc = " Pops an element from a list, pushes it to another list and returns it."]
    #[doc = ""]
    #[doc = " Returns none when the timeout passed. The connection waits too, replies to commands sent"]
    #[doc = " after it on the connection wait behind it. Use a connection of your own."]
    #[doc = ""]
    #[doc = " BLMOVE source destination 〈 LEFT | RIGHT 〉 〈 LEFT | RIGHT 〉 timeout"]
    #[doc = " https://valkey.io/commands/blmove/"]
    #[allow(async_fn_in_trait)]
    async fn blmove(
        connection: &Connection,
        source: String,
        destination: String,
        wherefrom: ListSide,
        whereto: ListSide,
        timeout: DurationMilliseconds,
    ) -> Result<Option<StringOrBytes>, Error> {
        exchange(
            connection,
            (
                "BLMOVE",
                source,
                destination,
                wherefrom,
                whereto,
                DecimalSeconds(timeout),
            ),
        )
        .await
    }

    #[doc = " Pops the first element from one of multiple lists."]
    #[doc = ""]
    #[doc = " Returns none when the timeout passed. The connection waits too, replies to commands sent"]
    #[doc = " after it on the connection wait behind it. Use a connection of your own."]
    #[doc = ""]
    #[doc = " BLMPOP timeout numkeys key [ key ... ] 〈 LEFT | RIGHT 〉 [ COUNT count ]"]
    #[doc = " https://valkey.io/commands/blmpop/"]
    #[allow(async_fn_in_trait)]
    async fn blmpop(
        connection: &Connection,
        timeout: DurationMilliseconds,
        keys: Vec<String>,
        end: ListSide,
        count: Option<Integer>,
    ) -> Result<Option<(String, Vec<StringOrBytes>)>, Error> {
        exchange(
            connection,
            (
                "BLMPOP",
                DecimalSeconds(timeout),
                NumKeys(NonEmpty::new("keys", keys)),
                end,
                NamedOption("COUNT", count),
            ),
        )
        .await
    }

    #[doc = " Removes and returns the first element in a list."]
    #[doc = ""]
    #[doc = " Returns none when the timeout passed. The connection waits too, replies to commands sent"]
    #[doc = " after it on the connection wait behind it. Use a connection of your own."]
    #[doc = ""]
    #[doc = " BLPOP key [ key ... ] timeout"]
    #[doc = " https://valkey.io/commands/blpop/"]
    #[allow(async_fn_in_trait)]
    async fn blpop(
        connection: &Connection,
        keys: Vec<String>,
        timeout: DurationMilliseconds,
    ) -> Result<Option<(String, StringOrBytes)>, Error> {
        exchange(
            connection,
            (
                "BLPOP",
                NonEmpty::new("keys", keys),
                DecimalSeconds(timeout),
            ),
        )
        .await
    }

    #[doc = " Removes and returns the last element in a list."]
    #[doc = ""]
    #[doc = " Returns none when the timeout passed. The connection waits too, replies to commands sent"]
    #[doc = " after it on the connection wait behind it. Use a connection of your own."]
    #[doc = ""]
    #[doc = " BRPOP key [ key ... ] timeout"]
    #[doc = " https://valkey.io/commands/brpop/"]
    #[allow(async_fn_in_trait)]
    async fn brpop(
        connection: &Connection,
        keys: Vec<String>,
        timeout: DurationMilliseconds,
    ) -> Result<Option<(String, StringOrBytes)>, Error> {
        exchange(
            connection,
            (
                "BRPOP",
                NonEmpty::new("keys", keys),
                DecimalSeconds(timeout),
            ),
        )
        .await
    }

    #[doc = " Pops an element from a list, pushes it to another list and returns it."]
    #[doc = ""]
    #[doc = " Returns none when the timeout passed. The connection waits too, replies to commands sent"]
    #[doc = " after it on the connection wait behind it. Use a connection of your own."]
    #[doc = ""]
    #[doc = " BRPOPLPUSH source destination timeout"]
    #[doc = " https://valkey.io/commands/brpoplpush/"]
    #[allow(async_fn_in_trait)]
    async fn brpoplpush(
        connection: &Connection,
        source: String,
        destination: String,
        timeout: DurationMilliseconds,
    ) -> Result<Option<StringOrBytes>, Error> {
        exchange(
            connection,
            ("BRPOPLPUSH", source, destination, DecimalSeconds(timeout)),
        )
        .await
    }

    #[doc = " Returns an element from a list by its index."]
    #[doc = ""]
    #[doc = " LINDEX key index"]
    #[doc = " https://valkey.io/commands/lindex/"]
    #[allow(async_fn_in_trait)]
    async fn lindex(
        connection: &Connection,
        key: String,
        index: Integer,
    ) -> Result<Option<StringOrBytes>, Error> {
        exchange(connection, ("LINDEX", key, index)).await
    }

    #[doc = " Inserts an element before or after another element in a list."]
    #[doc = ""]
    #[doc = " LINSERT key 〈 BEFORE | AFTER 〉 pivot element"]
    #[doc = " https://valkey.io/commands/linsert/"]
    #[allow(async_fn_in_trait)]
    async fn linsert(
        connection: &Connection,
        key: String,
        position: LinsertPosition,
        pivot: StringOrBytes,
        element: StringOrBytes,
    ) -> Result<LinsertResult, Error> {
        exchange(connection, ("LINSERT", key, position, pivot, element)).await
    }

    #[doc = " Returns the length of a list."]
    #[doc = ""]
    #[doc = " LLEN key"]
    #[doc = " https://valkey.io/commands/llen/"]
    #[allow(async_fn_in_trait)]
    async fn llen(connection: &Connection, key: String) -> Result<Integer, Error> {
        exchange(connection, ("LLEN", key)).await
    }

    #[doc = " Returns an element after popping it from one list and pushing it to another."]
    #[doc = ""]
    #[doc = " LMOVE source destination 〈 LEFT | RIGHT 〉 〈 LEFT | RIGHT 〉"]
    #[doc = " https://valkey.io/commands/lmove/"]
    #[allow(async_fn_in_trait)]
    async fn lmove(
        connection: &Connection,
        source: String,
        destination: String,
        wherefrom: ListSide,
        whereto: ListSide,
    ) -> Result<Option<StringOrBytes>, Error> {
        exchange(
            connection,
            ("LMOVE", source, destination, wherefrom, whereto),
        )
        .await
    }

    #[doc = " Returns multiple elements from a list after removing them."]
    #[doc = ""]
    #[doc = " LMPOP numkeys key [ key ... ] 〈 LEFT | RIGHT 〉 [ COUNT count ]"]
    #[doc = " https://valkey.io/commands/lmpop/"]
    #[allow(async_fn_in_trait)]
    async fn lmpop(
        connection: &Connection,
        keys: Vec<String>,
        end: ListSide,
        count: Option<Integer>,
    ) -> Result<Option<(String, Vec<StringOrBytes>)>, Error> {
        let command = (
            "LMPOP",
            NumKeys(NonEmpty::new("keys", keys)),
            end,
            NamedOption("COUNT", count),
        );
        exchange(connection, command).await
    }

    #[doc = " Returns and removes one or more elements from the beginning of a list."]
    #[doc = ""]
    #[doc = " Without a count, at most one element."]
    #[doc = ""]
    #[doc = " LPOP key [ count ]"]
    #[doc = " https://valkey.io/commands/lpop/"]
    #[allow(async_fn_in_trait)]
    async fn lpop(
        connection: &Connection,
        key: String,
        count: Option<Integer>,
    ) -> Result<Vec<StringOrBytes>, Error> {
        match count {
            Some(count) => {
                // null when the key does not exist
                let elements: Option<Vec<StringOrBytes>> =
                    exchange(connection, ("LPOP", key, count)).await?;
                Ok(elements.unwrap_or_default())
            }
            None => {
                // without a count the reply is the element itself, or null
                let element: Option<StringOrBytes> = exchange(connection, ("LPOP", key)).await?;
                Ok(element.into_iter().collect())
            }
        }
    }

    #[doc = " Returns the index of matching elements in a list."]
    #[doc = ""]
    #[doc = " Without a count, at most one position."]
    #[doc = ""]
    #[doc = " LPOS key element [ RANK rank ] [ COUNT num-matches ] [ MAXLEN len ]"]
    #[doc = " https://valkey.io/commands/lpos/"]
    #[allow(async_fn_in_trait)]
    async fn lpos(
        connection: &Connection,
        key: String,
        element: StringOrBytes,
        rank: Option<Integer>,
        count: Option<Integer>,
        maxlen: Option<Integer>,
    ) -> Result<Vec<Integer>, Error> {
        let command = (
            "LPOS",
            key,
            element,
            NamedOption("RANK", rank),
            NamedOption("COUNT", count),
            NamedOption("MAXLEN", maxlen),
        );

        match count {
            Some(_) => exchange(connection, command).await,
            None => {
                // without a count the reply is the position itself, or null
                let position: Option<Integer> = exchange(connection, command).await?;
                Ok(position.into_iter().collect())
            }
        }
    }

    #[doc = " Prepends one or more elements to a list."]
    #[doc = ""]
    #[doc = " LPUSH key element [ element ... ]"]
    #[doc = " https://valkey.io/commands/lpush/"]
    #[allow(async_fn_in_trait)]
    async fn lpush(
        connection: &Connection,
        key: String,
        elements: Vec<StringOrBytes>,
    ) -> Result<Integer, Error> {
        exchange(
            connection,
            ("LPUSH", key, NonEmpty::new("elements", elements)),
        )
        .await
    }

    #[doc = " Prepends one or more elements to a list only when the list exists."]
    #[doc = ""]
    #[doc = " LPUSHX key element [ element ... ]"]
    #[doc = " https://valkey.io/commands/lpushx/"]
    #[allow(async_fn_in_trait)]
    async fn lpushx(
        connection: &Connection,
        key: String,
        elements: Vec<StringOrBytes>,
    ) -> Result<Integer, Error> {
        exchange(
            connection,
            ("LPUSHX", key, NonEmpty::new("elements", elements)),
        )
        .await
    }

    #[doc = " Returns a range of elements from a list."]
    #[doc = ""]
    #[doc = " LRANGE key start stop"]
    #[doc = " https://valkey.io/commands/lrange/"]
    #[allow(async_fn_in_trait)]
    async fn lrange(
        connection: &Connection,
        key: String,
        start: Integer,
        stop: Integer,
    ) -> Result<Vec<StringOrBytes>, Error> {
        exchange(connection, ("LRANGE", key, start, stop)).await
    }

    #[doc = " Removes elements from a list."]
    #[doc = ""]
    #[doc = " LREM key count element"]
    #[doc = " https://valkey.io/commands/lrem/"]
    #[allow(async_fn_in_trait)]
    async fn lrem(
        connection: &Connection,
        key: String,
        count: Integer,
        element: StringOrBytes,
    ) -> Result<Integer, Error> {
        exchange(connection, ("LREM", key, count, element)).await
    }

    #[doc = " Sets the value of an element in a list by its index."]
    #[doc = ""]
    #[doc = " LSET key index element"]
    #[doc = " https://valkey.io/commands/lset/"]
    #[allow(async_fn_in_trait)]
    async fn lset(
        connection: &Connection,
        key: String,
        index: Integer,
        element: StringOrBytes,
    ) -> Result<(), Error> {
        exchange(connection, ("LSET", key, index, element)).await
    }

    #[doc = " Removes elements from both ends of a list."]
    #[doc = ""]
    #[doc = " LTRIM key start stop"]
    #[doc = " https://valkey.io/commands/ltrim/"]
    #[allow(async_fn_in_trait)]
    async fn ltrim(
        connection: &Connection,
        key: String,
        start: Integer,
        stop: Integer,
    ) -> Result<(), Error> {
        exchange(connection, ("LTRIM", key, start, stop)).await
    }

    #[doc = " Returns and removes one or more elements from the end of a list."]
    #[doc = ""]
    #[doc = " Without a count, at most one element."]
    #[doc = ""]
    #[doc = " RPOP key [ count ]"]
    #[doc = " https://valkey.io/commands/rpop/"]
    #[allow(async_fn_in_trait)]
    async fn rpop(
        connection: &Connection,
        key: String,
        count: Option<Integer>,
    ) -> Result<Vec<StringOrBytes>, Error> {
        match count {
            Some(count) => {
                // null when the key does not exist
                let elements: Option<Vec<StringOrBytes>> =
                    exchange(connection, ("RPOP", key, count)).await?;
                Ok(elements.unwrap_or_default())
            }
            None => {
                // without a count the reply is the element itself, or null
                let element: Option<StringOrBytes> = exchange(connection, ("RPOP", key)).await?;
                Ok(element.into_iter().collect())
            }
        }
    }

    #[doc = " Returns the last element of a list after removing and pushing it to another list."]
    #[doc = ""]
    #[doc = " RPOPLPUSH source destination"]
    #[doc = " https://valkey.io/commands/rpoplpush/"]
    #[allow(async_fn_in_trait)]
    async fn rpoplpush(
        connection: &Connection,
        source: String,
        destination: String,
    ) -> Result<Option<StringOrBytes>, Error> {
        exchange(connection, ("RPOPLPUSH", source, destination)).await
    }

    #[doc = " Appends one or more elements to a list."]
    #[doc = ""]
    #[doc = " RPUSH key element [ element ... ]"]
    #[doc = " https://valkey.io/commands/rpush/"]
    #[allow(async_fn_in_trait)]
    async fn rpush(
        connection: &Connection,
        key: String,
        elements: Vec<StringOrBytes>,
    ) -> Result<Integer, Error> {
        exchange(
            connection,
            ("RPUSH", key, NonEmpty::new("elements", elements)),
        )
        .await
    }

    #[doc = " Appends one or more elements to a list only when the list exists."]
    #[doc = ""]
    #[doc = " RPUSHX key element [ element ... ]"]
    #[doc = " https://valkey.io/commands/rpushx/"]
    #[allow(async_fn_in_trait)]
    async fn rpushx(
        connection: &Connection,
        key: String,
        elements: Vec<StringOrBytes>,
    ) -> Result<Integer, Error> {
        exchange(
            connection,
            ("RPUSHX", key, NonEmpty::new("elements", elements)),
        )
        .await
    }

    #[doc = " Deletes one or more keys."]
    #[doc = ""]
    #[doc = " DEL key [ key ... ]"]
    #[doc = " https://valkey.io/commands/del/"]
    #[allow(async_fn_in_trait)]
    async fn del(connection: &Connection, keys: Vec<String>) -> Result<Integer, Error> {
        exchange(connection, ("DEL", NonEmpty::new("keys", keys))).await
    }

    #[doc = " Returns a serialized representation of the value stored at a key."]
    #[doc = ""]
    #[doc = " DUMP key"]
    #[doc = " https://valkey.io/commands/dump/"]
    #[allow(async_fn_in_trait)]
    async fn dump(connection: &Connection, key: String) -> Result<Option<Bytes>, Error> {
        let serialized: Option<Binary> = exchange(connection, ("DUMP", key)).await?;
        Ok(serialized.map(|serialized| serialized.0))
    }

    #[doc = " Determines whether one or more keys exist."]
    #[doc = ""]
    #[doc = " EXISTS key [ key ... ]"]
    #[doc = " https://valkey.io/commands/exists/"]
    #[allow(async_fn_in_trait)]
    async fn exists(connection: &Connection, keys: Vec<String>) -> Result<Integer, Error> {
        exchange(connection, ("EXISTS", NonEmpty::new("keys", keys))).await
    }

    #[doc = " Sets the expiration time of a key in seconds."]
    #[doc = ""]
    #[doc = " EXPIRE key seconds [ NX | XX | GT | LT ]"]
    #[doc = " https://valkey.io/commands/expire/"]
    #[allow(async_fn_in_trait)]
    async fn expire(
        connection: &Connection,
        key: String,
        seconds: DurationSeconds,
        mode: Option<TtlMode>,
    ) -> Result<bool, Error> {
        exchange(connection, ("EXPIRE", key, seconds, mode)).await
    }

    #[doc = " Sets the expiration time of a key to a Unix timestamp."]
    #[doc = ""]
    #[doc = " EXPIREAT key unix-time-seconds [ NX | XX | GT | LT ]"]
    #[doc = " https://valkey.io/commands/expireat/"]
    #[allow(async_fn_in_trait)]
    async fn expireat(
        connection: &Connection,
        key: String,
        seconds: UnixTimeSeconds,
        mode: Option<TtlMode>,
    ) -> Result<bool, Error> {
        exchange(connection, ("EXPIREAT", key, seconds, mode)).await
    }

    #[doc = " Returns all key names that match a pattern."]
    #[doc = ""]
    #[doc = " KEYS pattern"]
    #[doc = " https://valkey.io/commands/keys/"]
    #[allow(async_fn_in_trait)]
    async fn keys(connection: &Connection, pattern: String) -> Result<Vec<String>, Error> {
        exchange(connection, ("KEYS", pattern)).await
    }

    #[doc = " Removes all keys from the target slot."]
    #[doc = ""]
    #[doc = " CLUSTER FLUSHSLOT slot [ ASYNC | SYNC ]"]
    #[doc = " https://valkey.io/commands/cluster-flushslot/"]
    #[allow(async_fn_in_trait)]
    async fn cluster_flushslot(
        connection: &Connection,
        slot: u16,
        mode: Option<FlushMode>,
    ) -> Result<(), Error> {
        exchange(connection, ("CLUSTER", "FLUSHSLOT", slot, mode)).await
    }

    #[doc = " Iterates over the keys in the cluster."]
    #[doc = ""]
    #[doc = " The cursor carries the slot, for clients to route each call to the node that owns it."]
    #[doc = ""]
    #[doc = " CLUSTERSCAN cursor [ MATCH match-pattern ] [ COUNT count ] [ TYPE type ] [ SLOT slot ]"]
    #[doc = " https://valkey.io/commands/clusterscan/"]
    #[allow(async_fn_in_trait)]
    async fn clusterscan(
        connection: &Connection,
        cursor: Cursor,
        match_: Option<String>,
        count: Option<Integer>,
        type_: Option<KeyType>,
        slot: Option<u16>,
    ) -> Result<(Option<Cursor>, Vec<String>), Error> {
        let command = (
            "CLUSTERSCAN",
            cursor,
            NamedOption("MATCH", match_),
            NamedOption("COUNT", count),
            NamedOption("TYPE", type_),
            NamedOption("SLOT", slot),
        );
        let (cursor, keys): (NextCursor, Vec<String>) = exchange(connection, command).await?;
        Ok((cursor.0, keys))
    }

    #[doc = " Copies the value of a key to a new key."]
    #[doc = ""]
    #[doc = " COPY source destination [ DB destination-db ] [ REPLACE ]"]
    #[doc = " https://valkey.io/commands/copy/"]
    #[allow(async_fn_in_trait)]
    async fn copy(
        connection: &Connection,
        source: String,
        destination: String,
        destination_db: Option<Integer>,
        replace: bool,
    ) -> Result<bool, Error> {
        exchange(
            connection,
            (
                "COPY",
                source,
                destination,
                NamedOption("DB", destination_db),
                Flag("REPLACE", replace),
            ),
        )
        .await
    }

    #[doc = " Returns the number of keys in the database."]
    #[doc = ""]
    #[doc = " DBSIZE"]
    #[doc = " https://valkey.io/commands/dbsize/"]
    #[allow(async_fn_in_trait)]
    async fn dbsize(connection: &Connection) -> Result<Integer, Error> {
        exchange(connection, "DBSIZE").await
    }

    #[doc = " Returns the expiration time of a key as a Unix timestamp."]
    #[doc = ""]
    #[doc = " EXPIRETIME key"]
    #[doc = " https://valkey.io/commands/expiretime/"]
    #[allow(async_fn_in_trait)]
    async fn expiretime(connection: &Connection, key: String) -> Result<ExpiretimeReply, Error> {
        exchange(connection, ("EXPIRETIME", key)).await
    }

    #[doc = " Removes all keys from all databases."]
    #[doc = ""]
    #[doc = " FLUSHALL [ ASYNC | SYNC ]"]
    #[doc = " https://valkey.io/commands/flushall/"]
    #[allow(async_fn_in_trait)]
    async fn flushall(connection: &Connection, mode: Option<FlushMode>) -> Result<(), Error> {
        exchange(connection, ("FLUSHALL", mode)).await
    }

    #[doc = " Removes all keys from the current database."]
    #[doc = ""]
    #[doc = " FLUSHDB [ ASYNC | SYNC ]"]
    #[doc = " https://valkey.io/commands/flushdb/"]
    #[allow(async_fn_in_trait)]
    async fn flushdb(connection: &Connection, mode: Option<FlushMode>) -> Result<(), Error> {
        exchange(connection, ("FLUSHDB", mode)).await
    }

    #[doc = " Atomically transfers a key from one instance to another."]
    #[doc = ""]
    #[doc = " False when none of the keys exist, the server's `NOKEY`."]
    #[doc = ""]
    #[doc = " MIGRATE host port \"\" destination-db timeout [ COPY ] [ REPLACE ] [ AUTH password | AUTH2 username password ] KEYS key [ key ... ]"]
    #[doc = " https://valkey.io/commands/migrate/"]
    #[allow(async_fn_in_trait)]
    async fn migrate(
        connection: &Connection,
        host: String,
        port: u16,
        destination_db: Integer,
        timeout: DurationMilliseconds,
        options: MigrateFlags,
        auth: Option<MigrateAuth>,
        keys: Vec<String>,
    ) -> Result<bool, Error> {
        // "" in place of the key, the keys follow KEYS
        let command = (
            "MIGRATE",
            host,
            port,
            "",
            destination_db,
            timeout,
            Flag("COPY", options.contains(MigrateFlags::COPY)),
            Flag("REPLACE", options.contains(MigrateFlags::REPLACE)),
            auth,
            ("KEYS", NonEmpty::new("keys", keys)),
        );
        let migrated: Migrated = exchange(connection, command).await?;
        Ok(migrated.0)
    }

    #[doc = " Moves a key to another database."]
    #[doc = ""]
    #[doc = " MOVE key db [ REPLACE ]"]
    #[doc = " https://valkey.io/commands/move/"]
    #[allow(async_fn_in_trait)]
    async fn move_(
        connection: &Connection,
        key: String,
        db: Integer,
        replace: bool,
    ) -> Result<bool, Error> {
        exchange(connection, ("MOVE", key, db, Flag("REPLACE", replace))).await
    }

    #[doc = " Returns the internal encoding of an object."]
    #[doc = ""]
    #[doc = " OBJECT ENCODING key"]
    #[doc = " https://valkey.io/commands/object-encoding/"]
    #[allow(async_fn_in_trait)]
    async fn object_encoding(
        connection: &Connection,
        key: String,
    ) -> Result<Option<String>, Error> {
        exchange(connection, ("OBJECT", "ENCODING", key)).await
    }

    #[doc = " Returns the logarithmic access frequency counter of an object."]
    #[doc = ""]
    #[doc = " OBJECT FREQ key"]
    #[doc = " https://valkey.io/commands/object-freq/"]
    #[allow(async_fn_in_trait)]
    async fn object_freq(connection: &Connection, key: String) -> Result<Option<Integer>, Error> {
        exchange(connection, ("OBJECT", "FREQ", key)).await
    }

    #[doc = " Returns the time since the last access to an object."]
    #[doc = ""]
    #[doc = " OBJECT IDLETIME key"]
    #[doc = " https://valkey.io/commands/object-idletime/"]
    #[allow(async_fn_in_trait)]
    async fn object_idletime(
        connection: &Connection,
        key: String,
    ) -> Result<Option<DurationSeconds>, Error> {
        exchange(connection, ("OBJECT", "IDLETIME", key)).await
    }

    #[doc = " Returns the reference count of a value of a key."]
    #[doc = ""]
    #[doc = " OBJECT REFCOUNT key"]
    #[doc = " https://valkey.io/commands/object-refcount/"]
    #[allow(async_fn_in_trait)]
    async fn object_refcount(
        connection: &Connection,
        key: String,
    ) -> Result<Option<Integer>, Error> {
        exchange(connection, ("OBJECT", "REFCOUNT", key)).await
    }

    #[doc = " Removes the expiration time of a key."]
    #[doc = ""]
    #[doc = " PERSIST key"]
    #[doc = " https://valkey.io/commands/persist/"]
    #[allow(async_fn_in_trait)]
    async fn persist(connection: &Connection, key: String) -> Result<bool, Error> {
        exchange(connection, ("PERSIST", key)).await
    }

    #[doc = " Sets the expiration time of a key in milliseconds."]
    #[doc = ""]
    #[doc = " PEXPIRE key milliseconds [ NX | XX | GT | LT ]"]
    #[doc = " https://valkey.io/commands/pexpire/"]
    #[allow(async_fn_in_trait)]
    async fn pexpire(
        connection: &Connection,
        key: String,
        milliseconds: DurationMilliseconds,
        mode: Option<TtlMode>,
    ) -> Result<bool, Error> {
        exchange(connection, ("PEXPIRE", key, milliseconds, mode)).await
    }

    #[doc = " Sets the expiration time of a key to a Unix milliseconds timestamp."]
    #[doc = ""]
    #[doc = " PEXPIREAT key unix-time-milliseconds [ NX | XX | GT | LT ]"]
    #[doc = " https://valkey.io/commands/pexpireat/"]
    #[allow(async_fn_in_trait)]
    async fn pexpireat(
        connection: &Connection,
        key: String,
        milliseconds: UnixTimeMilliseconds,
        mode: Option<TtlMode>,
    ) -> Result<bool, Error> {
        exchange(connection, ("PEXPIREAT", key, milliseconds, mode)).await
    }

    #[doc = " Returns the expiration time of a key as a Unix milliseconds timestamp."]
    #[doc = ""]
    #[doc = " PEXPIRETIME key"]
    #[doc = " https://valkey.io/commands/pexpiretime/"]
    #[allow(async_fn_in_trait)]
    async fn pexpiretime(connection: &Connection, key: String) -> Result<PexpiretimeReply, Error> {
        exchange(connection, ("PEXPIRETIME", key)).await
    }

    #[doc = " Returns the expiration time in milliseconds of a key."]
    #[doc = ""]
    #[doc = " PTTL key"]
    #[doc = " https://valkey.io/commands/pttl/"]
    #[allow(async_fn_in_trait)]
    async fn pttl(connection: &Connection, key: String) -> Result<PttlReply, Error> {
        exchange(connection, ("PTTL", key)).await
    }

    #[doc = " Returns a random key name from the database."]
    #[doc = ""]
    #[doc = " RANDOMKEY"]
    #[doc = " https://valkey.io/commands/randomkey/"]
    #[allow(async_fn_in_trait)]
    async fn randomkey(connection: &Connection) -> Result<Option<String>, Error> {
        exchange(connection, "RANDOMKEY").await
    }

    #[doc = " Renames a key and overwrites the destination."]
    #[doc = ""]
    #[doc = " RENAME key newkey"]
    #[doc = " https://valkey.io/commands/rename/"]
    #[allow(async_fn_in_trait)]
    async fn rename(connection: &Connection, key: String, newkey: String) -> Result<(), Error> {
        exchange(connection, ("RENAME", key, newkey)).await
    }

    #[doc = " Renames a key only when the target key name doesn't exist."]
    #[doc = ""]
    #[doc = " RENAMENX key newkey"]
    #[doc = " https://valkey.io/commands/renamenx/"]
    #[allow(async_fn_in_trait)]
    async fn renamenx(connection: &Connection, key: String, newkey: String) -> Result<bool, Error> {
        exchange(connection, ("RENAMENX", key, newkey)).await
    }

    #[doc = " Creates a key from the serialized representation of a value."]
    #[doc = ""]
    #[doc = " A `ttl` of none restores the key without an expiration, `0`, `expire-at` adds `ABSTTL`."]
    #[doc = ""]
    #[doc = " RESTORE key ttl serialized-value [ REPLACE ] [ ABSTTL ] [ IDLETIME seconds ] [ FREQ frequency ]"]
    #[doc = " https://valkey.io/commands/restore/"]
    #[allow(async_fn_in_trait)]
    async fn restore(
        connection: &Connection,
        key: String,
        ttl: Option<RestoreTtl>,
        serialized_value: Bytes,
        replace: bool,
        idletime: Option<DurationSeconds>,
        freq: Option<Integer>,
    ) -> Result<(), Error> {
        let (ttl, absttl) = restore_ttl(ttl);
        exchange(
            connection,
            (
                "RESTORE",
                key,
                ttl,
                StringOrBytes::Bytes(serialized_value),
                Flag("REPLACE", replace),
                Flag("ABSTTL", absttl),
                NamedOption("IDLETIME", idletime),
                NamedOption("FREQ", freq),
            ),
        )
        .await
    }

    #[doc = " An internal command for migrating keys in a cluster."]
    #[doc = ""]
    #[doc = " A `ttl` of none restores the key without an expiration, `0`, `expire-at` adds `ABSTTL`."]
    #[doc = ""]
    #[doc = " RESTORE-ASKING key ttl serialized-value [ REPLACE ] [ ABSTTL ] [ IDLETIME seconds ] [ FREQ frequency ]"]
    #[doc = " https://valkey.io/commands/restore-asking/"]
    #[allow(async_fn_in_trait)]
    async fn restore_asking(
        connection: &Connection,
        key: String,
        ttl: Option<RestoreTtl>,
        serialized_value: Bytes,
        replace: bool,
        idletime: Option<DurationSeconds>,
        freq: Option<Integer>,
    ) -> Result<(), Error> {
        let (ttl, absttl) = restore_ttl(ttl);
        exchange(
            connection,
            (
                "RESTORE-ASKING",
                key,
                ttl,
                StringOrBytes::Bytes(serialized_value),
                Flag("REPLACE", replace),
                Flag("ABSTTL", absttl),
                NamedOption("IDLETIME", idletime),
                NamedOption("FREQ", freq),
            ),
        )
        .await
    }

    #[doc = " Iterates over the key names in the database."]
    #[doc = ""]
    #[doc = " SCAN cursor [ MATCH pattern ] [ COUNT count ] [ TYPE type ]"]
    #[doc = " https://valkey.io/commands/scan/"]
    #[allow(async_fn_in_trait)]
    async fn scan(
        connection: &Connection,
        cursor: Cursor,
        match_: Option<String>,
        count: Option<Integer>,
        type_: Option<KeyType>,
    ) -> Result<(Option<Cursor>, Vec<String>), Error> {
        let command = (
            "SCAN",
            cursor,
            NamedOption("MATCH", match_),
            NamedOption("COUNT", count),
            NamedOption("TYPE", type_),
        );
        let (cursor, keys): (NextCursor, Vec<String>) = exchange(connection, command).await?;
        Ok((cursor.0, keys))
    }

    #[doc = " Swaps two databases."]
    #[doc = ""]
    #[doc = " SWAPDB index1 index2"]
    #[doc = " https://valkey.io/commands/swapdb/"]
    #[allow(async_fn_in_trait)]
    async fn swapdb(
        connection: &Connection,
        index1: Integer,
        index2: Integer,
    ) -> Result<(), Error> {
        exchange(connection, ("SWAPDB", index1, index2)).await
    }

    #[doc = " Returns the number of existing keys out of those specified after updating the time they were last accessed."]
    #[doc = ""]
    #[doc = " TOUCH key [ key ... ]"]
    #[doc = " https://valkey.io/commands/touch/"]
    #[allow(async_fn_in_trait)]
    async fn touch(connection: &Connection, keys: Vec<String>) -> Result<Integer, Error> {
        exchange(connection, ("TOUCH", NonEmpty::new("keys", keys))).await
    }

    #[doc = " Returns the expiration time in seconds of a key."]
    #[doc = ""]
    #[doc = " TTL key"]
    #[doc = " https://valkey.io/commands/ttl/"]
    #[allow(async_fn_in_trait)]
    async fn ttl(connection: &Connection, key: String) -> Result<TtlReply, Error> {
        exchange(connection, ("TTL", key)).await
    }

    #[doc = " Determines the type of value stored at a key."]
    #[doc = ""]
    #[doc = " None when the key does not exist, the server's `none`."]
    #[doc = ""]
    #[doc = " TYPE key"]
    #[doc = " https://valkey.io/commands/type/"]
    #[allow(async_fn_in_trait)]
    async fn type_(connection: &Connection, key: String) -> Result<Option<KeyType>, Error> {
        let key_type: OptionalKeyType = exchange(connection, ("TYPE", key)).await?;
        Ok(key_type.0)
    }

    #[doc = " Asynchronously deletes one or more keys."]
    #[doc = ""]
    #[doc = " UNLINK key [ key ... ]"]
    #[doc = " https://valkey.io/commands/unlink/"]
    #[allow(async_fn_in_trait)]
    async fn unlink(connection: &Connection, keys: Vec<String>) -> Result<Integer, Error> {
        exchange(connection, ("UNLINK", NonEmpty::new("keys", keys))).await
    }

    #[doc = " Posts a message to a channel."]
    #[doc = ""]
    #[doc = " PUBLISH channel message"]
    #[doc = " https://valkey.io/commands/publish/"]
    #[allow(async_fn_in_trait)]
    async fn publish(
        connection: &Connection,
        channel: String,
        message: StringOrBytes,
    ) -> Result<Integer, Error> {
        exchange(connection, ("PUBLISH", channel, message)).await
    }

    #[doc = " Returns the active channels."]
    #[doc = ""]
    #[doc = " PUBSUB CHANNELS [ pattern ]"]
    #[doc = " https://valkey.io/commands/pubsub-channels/"]
    #[allow(async_fn_in_trait)]
    async fn pubsub_channels(
        connection: &Connection,
        pattern: Option<String>,
    ) -> Result<Vec<String>, Error> {
        exchange(connection, ("PUBSUB", "CHANNELS", pattern)).await
    }

    #[doc = " Returns a count of unique pattern subscriptions."]
    #[doc = ""]
    #[doc = " PUBSUB NUMPAT"]
    #[doc = " https://valkey.io/commands/pubsub-numpat/"]
    #[allow(async_fn_in_trait)]
    async fn pubsub_numpat(connection: &Connection) -> Result<Integer, Error> {
        exchange(connection, ("PUBSUB", "NUMPAT")).await
    }

    #[doc = " Returns a count of subscribers to channels."]
    #[doc = ""]
    #[doc = " PUBSUB NUMSUB [ channel ] [ [ channel ] ... ]"]
    #[doc = " https://valkey.io/commands/pubsub-numsub/"]
    #[allow(async_fn_in_trait)]
    async fn pubsub_numsub(
        connection: &Connection,
        channels: Option<Vec<String>>,
    ) -> Result<Map<String, Integer>, Error> {
        exchange(
            connection,
            ("PUBSUB", "NUMSUB", NonEmpty::optional("channels", channels)),
        )
        .await
    }

    #[doc = " Returns the active shard channels."]
    #[doc = ""]
    #[doc = " PUBSUB SHARDCHANNELS [ pattern ]"]
    #[doc = " https://valkey.io/commands/pubsub-shardchannels/"]
    #[allow(async_fn_in_trait)]
    async fn pubsub_shardchannels(
        connection: &Connection,
        pattern: Option<String>,
    ) -> Result<Vec<String>, Error> {
        exchange(connection, ("PUBSUB", "SHARDCHANNELS", pattern)).await
    }

    #[doc = " Returns the count of subscribers of shard channels."]
    #[doc = ""]
    #[doc = " PUBSUB SHARDNUMSUB [ shardchannel [ shardchannel ... ] ]"]
    #[doc = " https://valkey.io/commands/pubsub-shardnumsub/"]
    #[allow(async_fn_in_trait)]
    async fn pubsub_shardnumsub(
        connection: &Connection,
        shardchannels: Option<Vec<String>>,
    ) -> Result<Map<String, Integer>, Error> {
        exchange(
            connection,
            (
                "PUBSUB",
                "SHARDNUMSUB",
                NonEmpty::optional("shardchannels", shardchannels),
            ),
        )
        .await
    }

    #[doc = " Executes a server-side Lua script."]
    #[doc = ""]
    #[doc = " An error the script returns fails the call."]
    #[doc = ""]
    #[doc = " EVAL script numkeys [ key [ key ... ] ] [ arg [ arg ... ] ]"]
    #[doc = " https://valkey.io/commands/eval/"]
    #[allow(async_fn_in_trait)]
    async fn eval(
        connection: &Connection,
        script: String,
        keys: Option<Vec<String>>,
        args: Option<Vec<StringOrBytes>>,
    ) -> Result<RespValue, Error> {
        exchange(connection, ("EVAL", script, KeysAndArgs(keys, args))).await
    }

    #[doc = " Executes a read-only server-side Lua script."]
    #[doc = ""]
    #[doc = " An error the script returns fails the call."]
    #[doc = ""]
    #[doc = " EVAL_RO script numkeys [ key [ key ... ] ] [ arg [ arg ... ] ]"]
    #[doc = " https://valkey.io/commands/eval_ro/"]
    #[allow(async_fn_in_trait)]
    async fn eval_ro(
        connection: &Connection,
        script: String,
        keys: Option<Vec<String>>,
        args: Option<Vec<StringOrBytes>>,
    ) -> Result<RespValue, Error> {
        exchange(connection, ("EVAL_RO", script, KeysAndArgs(keys, args))).await
    }

    #[doc = " Executes a server-side Lua script by SHA1 digest."]
    #[doc = ""]
    #[doc = " An error the script returns fails the call."]
    #[doc = ""]
    #[doc = " EVALSHA sha1 numkeys [ key [ key ... ] ] [ arg [ arg ... ] ]"]
    #[doc = " https://valkey.io/commands/evalsha/"]
    #[allow(async_fn_in_trait)]
    async fn evalsha(
        connection: &Connection,
        sha1: String,
        keys: Option<Vec<String>>,
        args: Option<Vec<StringOrBytes>>,
    ) -> Result<RespValue, Error> {
        exchange(connection, ("EVALSHA", sha1, KeysAndArgs(keys, args))).await
    }

    #[doc = " Executes a read-only server-side Lua script by SHA1 digest."]
    #[doc = ""]
    #[doc = " An error the script returns fails the call."]
    #[doc = ""]
    #[doc = " EVALSHA_RO sha1 numkeys [ key [ key ... ] ] [ arg [ arg ... ] ]"]
    #[doc = " https://valkey.io/commands/evalsha_ro/"]
    #[allow(async_fn_in_trait)]
    async fn evalsha_ro(
        connection: &Connection,
        sha1: String,
        keys: Option<Vec<String>>,
        args: Option<Vec<StringOrBytes>>,
    ) -> Result<RespValue, Error> {
        exchange(connection, ("EVALSHA_RO", sha1, KeysAndArgs(keys, args))).await
    }

    #[doc = " Invokes a function."]
    #[doc = ""]
    #[doc = " An error the function returns fails the call."]
    #[doc = ""]
    #[doc = " FCALL function numkeys [ key [ key ... ] ] [ arg [ arg ... ] ]"]
    #[doc = " https://valkey.io/commands/fcall/"]
    #[allow(async_fn_in_trait)]
    async fn fcall(
        connection: &Connection,
        function: String,
        keys: Option<Vec<String>>,
        args: Option<Vec<StringOrBytes>>,
    ) -> Result<RespValue, Error> {
        exchange(connection, ("FCALL", function, KeysAndArgs(keys, args))).await
    }

    #[doc = " Invokes a read-only function."]
    #[doc = ""]
    #[doc = " An error the function returns fails the call."]
    #[doc = ""]
    #[doc = " FCALL_RO function numkeys [ key [ key ... ] ] [ arg [ arg ... ] ]"]
    #[doc = " https://valkey.io/commands/fcall_ro/"]
    #[allow(async_fn_in_trait)]
    async fn fcall_ro(
        connection: &Connection,
        function: String,
        keys: Option<Vec<String>>,
        args: Option<Vec<StringOrBytes>>,
    ) -> Result<RespValue, Error> {
        exchange(connection, ("FCALL_RO", function, KeysAndArgs(keys, args))).await
    }

    #[doc = " Deletes a library and its functions."]
    #[doc = ""]
    #[doc = " FUNCTION DELETE library-name"]
    #[doc = " https://valkey.io/commands/function-delete/"]
    #[allow(async_fn_in_trait)]
    async fn function_delete(connection: &Connection, library_name: String) -> Result<(), Error> {
        exchange(connection, ("FUNCTION", "DELETE", library_name)).await
    }

    #[doc = " Dumps all libraries into a serialized binary payload."]
    #[doc = ""]
    #[doc = " FUNCTION DUMP"]
    #[doc = " https://valkey.io/commands/function-dump/"]
    #[allow(async_fn_in_trait)]
    async fn function_dump(connection: &Connection) -> Result<Bytes, Error> {
        let serialized: Binary = exchange(connection, ("FUNCTION", "DUMP")).await?;
        Ok(serialized.0)
    }

    #[doc = " Deletes all libraries and functions."]
    #[doc = ""]
    #[doc = " FUNCTION FLUSH [ ASYNC | SYNC ]"]
    #[doc = " https://valkey.io/commands/function-flush/"]
    #[allow(async_fn_in_trait)]
    async fn function_flush(connection: &Connection, mode: Option<FlushMode>) -> Result<(), Error> {
        exchange(connection, ("FUNCTION", "FLUSH", mode)).await
    }

    #[doc = " Terminates a function during execution."]
    #[doc = ""]
    #[doc = " FUNCTION KILL"]
    #[doc = " https://valkey.io/commands/function-kill/"]
    #[allow(async_fn_in_trait)]
    async fn function_kill(connection: &Connection) -> Result<(), Error> {
        exchange(connection, ("FUNCTION", "KILL")).await
    }

    #[doc = " Returns information about all libraries."]
    #[doc = ""]
    #[doc = " FUNCTION LIST [ LIBRARYNAME library-name-pattern ] [ WITHCODE ]"]
    #[doc = " https://valkey.io/commands/function-list/"]
    #[allow(async_fn_in_trait)]
    async fn function_list(
        connection: &Connection,
        library_name_pattern: Option<String>,
        withcode: bool,
    ) -> Result<Vec<FunctionListLibrary>, Error> {
        exchange(
            connection,
            (
                "FUNCTION",
                "LIST",
                NamedOption("LIBRARYNAME", library_name_pattern),
                Flag("WITHCODE", withcode),
            ),
        )
        .await
    }

    #[doc = " Creates a library."]
    #[doc = ""]
    #[doc = " FUNCTION LOAD [ REPLACE ] function-code"]
    #[doc = " https://valkey.io/commands/function-load/"]
    #[allow(async_fn_in_trait)]
    async fn function_load(
        connection: &Connection,
        replace: bool,
        function_code: String,
    ) -> Result<String, Error> {
        exchange(
            connection,
            ("FUNCTION", "LOAD", Flag("REPLACE", replace), function_code),
        )
        .await
    }

    #[doc = " Restores all libraries from a payload."]
    #[doc = ""]
    #[doc = " FUNCTION RESTORE serialized-value [ FLUSH | APPEND | REPLACE ]"]
    #[doc = " https://valkey.io/commands/function-restore/"]
    #[allow(async_fn_in_trait)]
    async fn function_restore(
        connection: &Connection,
        serialized_value: Bytes,
        policy: Option<FunctionRestorePolicy>,
    ) -> Result<(), Error> {
        exchange(
            connection,
            (
                "FUNCTION",
                "RESTORE",
                StringOrBytes::Bytes(serialized_value),
                policy,
            ),
        )
        .await
    }

    #[doc = " Returns information about a function during execution."]
    #[doc = ""]
    #[doc = " FUNCTION STATS"]
    #[doc = " https://valkey.io/commands/function-stats/"]
    #[allow(async_fn_in_trait)]
    async fn function_stats(connection: &Connection) -> Result<FunctionStatsReply, Error> {
        exchange(connection, ("FUNCTION", "STATS")).await
    }

    #[doc = " Determines whether server-side Lua scripts exist in the script cache."]
    #[doc = ""]
    #[doc = " SCRIPT EXISTS sha1 [ sha1 ... ]"]
    #[doc = " https://valkey.io/commands/script-exists/"]
    #[allow(async_fn_in_trait)]
    async fn script_exists(
        connection: &Connection,
        sha1s: Vec<String>,
    ) -> Result<Vec<bool>, Error> {
        exchange(
            connection,
            ("SCRIPT", "EXISTS", NonEmpty::new("sha1s", sha1s)),
        )
        .await
    }

    #[doc = " Removes all server-side Lua scripts from the script cache."]
    #[doc = ""]
    #[doc = " SCRIPT FLUSH [ ASYNC | SYNC ]"]
    #[doc = " https://valkey.io/commands/script-flush/"]
    #[allow(async_fn_in_trait)]
    async fn script_flush(connection: &Connection, mode: Option<FlushMode>) -> Result<(), Error> {
        exchange(connection, ("SCRIPT", "FLUSH", mode)).await
    }

    #[doc = " Terminates a server-side Lua script during execution."]
    #[doc = ""]
    #[doc = " SCRIPT KILL"]
    #[doc = " https://valkey.io/commands/script-kill/"]
    #[allow(async_fn_in_trait)]
    async fn script_kill(connection: &Connection) -> Result<(), Error> {
        exchange(connection, ("SCRIPT", "KILL")).await
    }

    #[doc = " Loads a server-side Lua script to the script cache."]
    #[doc = ""]
    #[doc = " SCRIPT LOAD script"]
    #[doc = " https://valkey.io/commands/script-load/"]
    #[allow(async_fn_in_trait)]
    async fn script_load(connection: &Connection, script: String) -> Result<String, Error> {
        exchange(connection, ("SCRIPT", "LOAD", script)).await
    }

    #[doc = " Show server-side Lua script in the script cache."]
    #[doc = ""]
    #[doc = " SCRIPT SHOW sha1"]
    #[doc = " https://valkey.io/commands/script-show/"]
    #[allow(async_fn_in_trait)]
    async fn script_show(connection: &Connection, sha1: String) -> Result<String, Error> {
        exchange(connection, ("SCRIPT", "SHOW", sha1)).await
    }

    #[doc = " Posts a message to a shard channel."]
    #[doc = ""]
    #[doc = " SPUBLISH shardchannel message"]
    #[doc = " https://valkey.io/commands/spublish/"]
    #[allow(async_fn_in_trait)]
    async fn spublish(
        connection: &Connection,
        shardchannel: String,
        message: StringOrBytes,
    ) -> Result<Integer, Error> {
        exchange(connection, ("SPUBLISH", shardchannel, message)).await
    }

    #[doc = " Appends a string to the value of a key."]
    #[doc = ""]
    #[doc = " APPEND key value"]
    #[doc = " https://valkey.io/commands/append/"]
    #[allow(async_fn_in_trait)]
    async fn append(
        connection: &Connection,
        key: String,
        value: StringOrBytes,
    ) -> Result<Integer, Error> {
        exchange(connection, ("APPEND", key, value)).await
    }

    #[doc = " Decrements the integer value of a key by one."]
    #[doc = ""]
    #[doc = " DECR key"]
    #[doc = " https://valkey.io/commands/decr/"]
    #[allow(async_fn_in_trait)]
    async fn decr(connection: &Connection, key: String) -> Result<Integer, Error> {
        exchange(connection, ("DECR", key)).await
    }

    #[doc = " Decrements the integer value of a key by a number."]
    #[doc = ""]
    #[doc = " DECRBY key decrement"]
    #[doc = " https://valkey.io/commands/decrby/"]
    #[allow(async_fn_in_trait)]
    async fn decrby(
        connection: &Connection,
        key: String,
        decrement: Integer,
    ) -> Result<Integer, Error> {
        exchange(connection, ("DECRBY", key, decrement)).await
    }

    #[doc = " Delete key if value matches string."]
    #[doc = ""]
    #[doc = " DELIFEQ key value"]
    #[doc = " https://valkey.io/commands/delifeq/"]
    #[allow(async_fn_in_trait)]
    async fn delifeq(
        connection: &Connection,
        key: String,
        value: StringOrBytes,
    ) -> Result<bool, Error> {
        exchange(connection, ("DELIFEQ", key, value)).await
    }

    #[doc = " Returns the string value of a key."]
    #[doc = ""]
    #[doc = " GET key"]
    #[doc = " https://valkey.io/commands/get/"]
    #[allow(async_fn_in_trait)]
    async fn get(connection: &Connection, key: String) -> Result<Option<StringOrBytes>, Error> {
        exchange(connection, ("GET", key)).await
    }

    #[doc = " Returns the string value of a key after deleting the key."]
    #[doc = ""]
    #[doc = " GETDEL key"]
    #[doc = " https://valkey.io/commands/getdel/"]
    #[allow(async_fn_in_trait)]
    async fn getdel(connection: &Connection, key: String) -> Result<Option<StringOrBytes>, Error> {
        exchange(connection, ("GETDEL", key)).await
    }

    #[doc = " Returns the string value of a key after setting its expiration time."]
    #[doc = ""]
    #[doc = " GETEX key [ EX seconds | PX milliseconds | EXAT unix-time-seconds | PXAT unix-time-milliseconds | PERSIST ]"]
    #[doc = " https://valkey.io/commands/getex/"]
    #[allow(async_fn_in_trait)]
    async fn getex(
        connection: &Connection,
        key: String,
        ttl: Option<GetexTtl>,
    ) -> Result<Option<StringOrBytes>, Error> {
        exchange(connection, ("GETEX", key, ttl)).await
    }

    #[doc = " Returns a substring of the string stored at a key."]
    #[doc = ""]
    #[doc = " GETRANGE key start end"]
    #[doc = " https://valkey.io/commands/getrange/"]
    #[allow(async_fn_in_trait)]
    async fn getrange(
        connection: &Connection,
        key: String,
        start: Integer,
        end: Integer,
    ) -> Result<StringOrBytes, Error> {
        exchange(connection, ("GETRANGE", key, start, end)).await
    }

    #[doc = " Returns the previous string value of a key after setting it to a new value."]
    #[doc = ""]
    #[doc = " GETSET key value"]
    #[doc = " https://valkey.io/commands/getset/"]
    #[allow(async_fn_in_trait)]
    async fn getset(
        connection: &Connection,
        key: String,
        value: StringOrBytes,
    ) -> Result<Option<StringOrBytes>, Error> {
        exchange(connection, ("GETSET", key, value)).await
    }

    #[doc = " Increments the integer value of a key by one."]
    #[doc = ""]
    #[doc = " INCR key"]
    #[doc = " https://valkey.io/commands/incr/"]
    #[allow(async_fn_in_trait)]
    async fn incr(connection: &Connection, key: String) -> Result<Integer, Error> {
        exchange(connection, ("INCR", key)).await
    }

    #[doc = " Increments the integer value of a key by a number."]
    #[doc = ""]
    #[doc = " INCRBY key increment"]
    #[doc = " https://valkey.io/commands/incrby/"]
    #[allow(async_fn_in_trait)]
    async fn incrby(
        connection: &Connection,
        key: String,
        increment: Integer,
    ) -> Result<Integer, Error> {
        exchange(connection, ("INCRBY", key, increment)).await
    }

    #[doc = " Increments the floating point value of a key by a number."]
    #[doc = ""]
    #[doc = " INCRBYFLOAT key increment"]
    #[doc = " https://valkey.io/commands/incrbyfloat/"]
    #[allow(async_fn_in_trait)]
    async fn incrbyfloat(
        connection: &Connection,
        key: String,
        increment: Double,
    ) -> Result<Double, Error> {
        exchange(connection, ("INCRBYFLOAT", key, increment)).await
    }

    #[doc = " Finds the longest common substring."]
    #[doc = ""]
    #[doc = " See also `lcs-len` and `lcs-idx`."]
    #[doc = ""]
    #[doc = " LCS key1 key2"]
    #[doc = " https://valkey.io/commands/lcs/"]
    #[allow(async_fn_in_trait)]
    async fn lcs(
        connection: &Connection,
        key1: String,
        key2: String,
    ) -> Result<StringOrBytes, Error> {
        exchange(connection, ("LCS", key1, key2)).await
    }

    #[doc = " Finds the longest common substring."]
    #[doc = ""]
    #[doc = " The `LEN` form, the length only."]
    #[doc = ""]
    #[doc = " See also `lcs` and `lcs-idx`."]
    #[doc = ""]
    #[doc = " LCS key1 key2 LEN"]
    #[doc = " https://valkey.io/commands/lcs/"]
    #[allow(async_fn_in_trait)]
    async fn lcs_len(
        connection: &Connection,
        key1: String,
        key2: String,
    ) -> Result<Integer, Error> {
        exchange(connection, ("LCS", key1, key2, "LEN")).await
    }

    #[doc = " Finds the longest common substring."]
    #[doc = ""]
    #[doc = " The `IDX` form, the ranges of the matches."]
    #[doc = ""]
    #[doc = " See also `lcs` and `lcs-len`."]
    #[doc = ""]
    #[doc = " LCS key1 key2 IDX [ MINMATCHLEN min-match-len ] [ WITHMATCHLEN ]"]
    #[doc = " https://valkey.io/commands/lcs/"]
    #[allow(async_fn_in_trait)]
    async fn lcs_idx(
        connection: &Connection,
        key1: String,
        key2: String,
        min_match_len: Option<Integer>,
        with_match_len: bool,
    ) -> Result<LcsIdxReply, Error> {
        exchange(
            connection,
            (
                "LCS",
                key1,
                key2,
                "IDX",
                min_match_len.map(|len| ("MINMATCHLEN", len)),
                Flag("WITHMATCHLEN", with_match_len),
            ),
        )
        .await
    }

    #[doc = " Atomically returns the string values of one or more keys."]
    #[doc = ""]
    #[doc = " MGET key [ key ... ]"]
    #[doc = " https://valkey.io/commands/mget/"]
    #[allow(async_fn_in_trait)]
    async fn mget(
        connection: &Connection,
        keys: Vec<String>,
    ) -> Result<Vec<Option<StringOrBytes>>, Error> {
        exchange(connection, ("MGET", NonEmpty::new("keys", keys))).await
    }

    #[doc = " Atomically creates or modifies the string values of one or more keys."]
    #[doc = ""]
    #[doc = " MSET key value [ key value ... ]"]
    #[doc = " https://valkey.io/commands/mset/"]
    #[allow(async_fn_in_trait)]
    async fn mset(
        connection: &Connection,
        keys_and_values: Vec<(String, StringOrBytes)>,
    ) -> Result<(), Error> {
        exchange(
            connection,
            ("MSET", NonEmpty::new("keys-and-values", keys_and_values)),
        )
        .await
    }

    #[doc = " Atomically creates or modifies the string values of one or more keys, and optionally set their expiration."]
    #[doc = ""]
    #[doc = " MSETEX numkeys key value [ key value ... ] [ NX | XX ] [ EX seconds | PX milliseconds | EXAT unix-time-seconds | PXAT unix-time-milliseconds | KEEPTTL ]"]
    #[doc = " https://valkey.io/commands/msetex/"]
    #[allow(async_fn_in_trait)]
    async fn msetex(
        connection: &Connection,
        keys_and_values: Vec<(String, StringOrBytes)>,
        precondition: Option<MsetexPrecondition>,
        ttl: Option<MsetexTtl>,
    ) -> Result<bool, Error> {
        exchange(
            connection,
            (
                "MSETEX",
                NumKeys(NonEmpty::new("keys-and-values", keys_and_values)),
                precondition,
                ttl,
            ),
        )
        .await
    }

    #[doc = " Atomically modifies the string values of one or more keys only when all keys don't exist."]
    #[doc = ""]
    #[doc = " MSETNX key value [ key value ... ]"]
    #[doc = " https://valkey.io/commands/msetnx/"]
    #[allow(async_fn_in_trait)]
    async fn msetnx(
        connection: &Connection,
        keys_and_values: Vec<(String, StringOrBytes)>,
    ) -> Result<bool, Error> {
        exchange(
            connection,
            ("MSETNX", NonEmpty::new("keys-and-values", keys_and_values)),
        )
        .await
    }

    #[doc = " Sets both string value and expiration time in milliseconds of a key."]
    #[doc = ""]
    #[doc = " PSETEX key milliseconds value"]
    #[doc = " https://valkey.io/commands/psetex/"]
    #[allow(async_fn_in_trait)]
    async fn psetex(
        connection: &Connection,
        key: String,
        milliseconds: DurationMilliseconds,
        value: StringOrBytes,
    ) -> Result<(), Error> {
        exchange(connection, ("PSETEX", key, milliseconds, value)).await
    }

    #[doc = " Sets the string value of a key, ignoring its type."]
    #[doc = ""]
    #[doc = " Returns false when the precondition is not met, the key is unchanged."]
    #[doc = ""]
    #[doc = " See also `set-get`."]
    #[doc = ""]
    #[doc = " SET key value [ NX | XX | IFEQ comparison-value | IFNE comparison-not-equal ] [ EX seconds | PX milliseconds | EXAT unix-time-seconds | PXAT unix-time-milliseconds | KEEPTTL ]"]
    #[doc = " https://valkey.io/commands/set/"]
    #[allow(async_fn_in_trait)]
    async fn set(
        connection: &Connection,
        key: String,
        value: StringOrBytes,
        precondition: Option<SetPrecondition>,
        ttl: Option<SetTtl>,
    ) -> Result<bool, Error> {
        // OK once set, null when the precondition is not met
        let reply: Option<()> =
            exchange(connection, ("SET", key, value, precondition, ttl)).await?;
        Ok(reply.is_some())
    }

    #[doc = " Sets the string value of a key, ignoring its type."]
    #[doc = ""]
    #[doc = " The `GET` form, returning the previous value, none when the key did not exist."]
    #[doc = ""]
    #[doc = " See also `set`."]
    #[doc = ""]
    #[doc = " SET key value [ NX | XX | IFEQ comparison-value | IFNE comparison-not-equal ] GET [ EX seconds | PX milliseconds | EXAT unix-time-seconds | PXAT unix-time-milliseconds | KEEPTTL ]"]
    #[doc = " https://valkey.io/commands/set/"]
    #[allow(async_fn_in_trait)]
    async fn set_get(
        connection: &Connection,
        key: String,
        value: StringOrBytes,
        precondition: Option<SetPrecondition>,
        ttl: Option<SetTtl>,
    ) -> Result<Option<StringOrBytes>, Error> {
        exchange(connection, ("SET", key, value, precondition, "GET", ttl)).await
    }

    #[doc = " Sets the string value and expiration time of a key."]
    #[doc = ""]
    #[doc = " SETEX key seconds value"]
    #[doc = " https://valkey.io/commands/setex/"]
    #[allow(async_fn_in_trait)]
    async fn setex(
        connection: &Connection,
        key: String,
        seconds: DurationSeconds,
        value: StringOrBytes,
    ) -> Result<(), Error> {
        exchange(connection, ("SETEX", key, seconds, value)).await
    }

    #[doc = " Sets the string value of a key only when the key doesn't exist."]
    #[doc = ""]
    #[doc = " SETNX key value"]
    #[doc = " https://valkey.io/commands/setnx/"]
    #[allow(async_fn_in_trait)]
    async fn setnx(
        connection: &Connection,
        key: String,
        value: StringOrBytes,
    ) -> Result<bool, Error> {
        exchange(connection, ("SETNX", key, value)).await
    }

    #[doc = " Overwrites a part of a string value with another by an offset."]
    #[doc = ""]
    #[doc = " SETRANGE key offset value"]
    #[doc = " https://valkey.io/commands/setrange/"]
    #[allow(async_fn_in_trait)]
    async fn setrange(
        connection: &Connection,
        key: String,
        offset: u32,
        value: StringOrBytes,
    ) -> Result<Integer, Error> {
        exchange(connection, ("SETRANGE", key, offset, value)).await
    }

    #[doc = " Returns the length of a string value."]
    #[doc = ""]
    #[doc = " STRLEN key"]
    #[doc = " https://valkey.io/commands/strlen/"]
    #[allow(async_fn_in_trait)]
    async fn strlen(connection: &Connection, key: String) -> Result<Integer, Error> {
        exchange(connection, ("STRLEN", key)).await
    }

    #[doc = " Returns a substring from a string value."]
    #[doc = ""]
    #[doc = " SUBSTR key start end"]
    #[doc = " https://valkey.io/commands/substr/"]
    #[allow(async_fn_in_trait)]
    async fn substr(
        connection: &Connection,
        key: String,
        start: Integer,
        end: Integer,
    ) -> Result<StringOrBytes, Error> {
        exchange(connection, ("SUBSTR", key, start, end)).await
    }

    #[doc = " Returns the number of messages that were successfully acknowledged by the consumer group member of a stream."]
    #[doc = ""]
    #[doc = " XACK key group ID [ ID ... ]"]
    #[doc = " https://valkey.io/commands/xack/"]
    #[allow(async_fn_in_trait)]
    async fn xack(
        connection: &Connection,
        key: String,
        group: String,
        ids: Vec<StreamId>,
    ) -> Result<Integer, Error> {
        exchange(connection, ("XACK", key, group, NonEmpty::new("ids", ids))).await
    }

    #[doc = " Appends a new message to a stream."]
    #[doc = ""]
    #[doc = " XADD key [ NOMKSTREAM ] [ 〈 MAXLEN | MINID 〉 [ = | ~ ] threshold [ LIMIT count ] ] 〈 * | id 〉 field value [ field value ... ]"]
    #[doc = " https://valkey.io/commands/xadd/"]
    #[allow(async_fn_in_trait)]
    async fn xadd(
        connection: &Connection,
        key: String,
        nomkstream: bool,
        trim: Option<StreamTrim>,
        id: StreamId,
        field_and_values: Vec<(String, StringOrBytes)>,
    ) -> Result<Option<StreamId>, Error> {
        exchange(
            connection,
            (
                "XADD",
                key,
                Flag("NOMKSTREAM", nomkstream),
                trim,
                id,
                NonEmpty::new("field-and-values", field_and_values),
            ),
        )
        .await
    }

    #[doc = " Changes, or acquires, ownership of messages in a consumer group, as if the messages were delivered to a consumer group member."]
    #[doc = ""]
    #[doc = " A start of none scans from the beginning, `0-0`. The next start is returned, none once the"]
    #[doc = " scan is complete."]
    #[doc = ""]
    #[doc = " See also `xautoclaim-justid`."]
    #[doc = ""]
    #[doc = " XAUTOCLAIM key group consumer min-idle-time start [ COUNT count ]"]
    #[doc = " https://valkey.io/commands/xautoclaim/"]
    #[allow(async_fn_in_trait)]
    async fn xautoclaim(
        connection: &Connection,
        key: String,
        group: String,
        consumer: String,
        min_idle_time: DurationMilliseconds,
        start: Option<StreamId>,
        count: Option<Integer>,
    ) -> Result<
        (
            Option<StreamId>,
            Vec<(StreamId, Map<String, StringOrBytes>)>,
            Vec<StreamId>,
        ),
        Error,
    > {
        let (next, claimed, deleted): (OrZeroId, _, _) = exchange(
            connection,
            (
                "XAUTOCLAIM",
                key,
                group,
                consumer,
                min_idle_time,
                OrZeroId(start),
                NamedOption("COUNT", count),
            ),
        )
        .await?;
        Ok((next.0, claimed, deleted))
    }

    #[doc = " Changes, or acquires, ownership of messages in a consumer group, as if the messages were delivered to a consumer group member."]
    #[doc = ""]
    #[doc = " The `JUSTID` form, the IDs only. A start of none scans from the beginning, `0-0`. The next"]
    #[doc = " start is returned, none once the scan is complete."]
    #[doc = ""]
    #[doc = " See also `xautoclaim`."]
    #[doc = ""]
    #[doc = " XAUTOCLAIM key group consumer min-idle-time start [ COUNT count ] JUSTID"]
    #[doc = " https://valkey.io/commands/xautoclaim/"]
    #[allow(async_fn_in_trait)]
    async fn xautoclaim_justid(
        connection: &Connection,
        key: String,
        group: String,
        consumer: String,
        min_idle_time: DurationMilliseconds,
        start: Option<StreamId>,
        count: Option<Integer>,
    ) -> Result<(Option<StreamId>, Vec<StreamId>, Vec<StreamId>), Error> {
        let (next, claimed, deleted): (OrZeroId, _, _) = exchange(
            connection,
            (
                "XAUTOCLAIM",
                key,
                group,
                consumer,
                min_idle_time,
                OrZeroId(start),
                NamedOption("COUNT", count),
                "JUSTID",
            ),
        )
        .await?;
        Ok((next.0, claimed, deleted))
    }

    #[doc = " Changes, or acquires, ownership of a message in a consumer group, as if the message was delivered to a consumer group member."]
    #[doc = ""]
    #[doc = " See also `xclaim-justid`."]
    #[doc = ""]
    #[doc = " XCLAIM key group consumer min-idle-time ID [ ID ... ] [ IDLE ms ] [ TIME unix-time-milliseconds ] [ RETRYCOUNT count ] [ FORCE ] [ LASTID lastid ]"]
    #[doc = " https://valkey.io/commands/xclaim/"]
    #[allow(async_fn_in_trait)]
    async fn xclaim(
        connection: &Connection,
        key: String,
        group: String,
        consumer: String,
        min_idle_time: DurationMilliseconds,
        ids: Vec<StreamId>,
        opts: Option<XclaimOpts>,
    ) -> Result<Vec<(StreamId, Map<String, StringOrBytes>)>, Error> {
        exchange(
            connection,
            (
                "XCLAIM",
                key,
                group,
                consumer,
                min_idle_time,
                NonEmpty::new("ids", ids),
                opts,
            ),
        )
        .await
    }

    #[doc = " Changes, or acquires, ownership of a message in a consumer group, as if the message was delivered to a consumer group member."]
    #[doc = ""]
    #[doc = " The `JUSTID` form, the IDs only."]
    #[doc = ""]
    #[doc = " See also `xclaim`."]
    #[doc = ""]
    #[doc = " XCLAIM key group consumer min-idle-time ID [ ID ... ] [ IDLE ms ] [ TIME unix-time-milliseconds ] [ RETRYCOUNT count ] [ FORCE ] JUSTID [ LASTID lastid ]"]
    #[doc = " https://valkey.io/commands/xclaim/"]
    #[allow(async_fn_in_trait)]
    async fn xclaim_justid(
        connection: &Connection,
        key: String,
        group: String,
        consumer: String,
        min_idle_time: DurationMilliseconds,
        ids: Vec<StreamId>,
        opts: Option<XclaimOpts>,
    ) -> Result<Vec<StreamId>, Error> {
        exchange(
            connection,
            (
                "XCLAIM",
                key,
                group,
                consumer,
                min_idle_time,
                NonEmpty::new("ids", ids),
                opts,
            ),
        )
        .await
    }

    #[doc = " Returns the number of messages after removing them from a stream."]
    #[doc = ""]
    #[doc = " XDEL key ID [ ID ... ]"]
    #[doc = " https://valkey.io/commands/xdel/"]
    #[allow(async_fn_in_trait)]
    async fn xdel(
        connection: &Connection,
        key: String,
        ids: Vec<StreamId>,
    ) -> Result<Integer, Error> {
        exchange(connection, ("XDEL", key, NonEmpty::new("ids", ids))).await
    }

    #[doc = " Creates a consumer group."]
    #[doc = ""]
    #[doc = " XGROUP CREATE key group 〈 id | $ 〉 [ MKSTREAM ] [ ENTRIESREAD entriesread ]"]
    #[doc = " https://valkey.io/commands/xgroup-create/"]
    #[allow(async_fn_in_trait)]
    async fn xgroup_create(
        connection: &Connection,
        key: String,
        group: String,
        id: StreamId,
        mkstream: bool,
        entriesread: Option<Integer>,
    ) -> Result<(), Error> {
        exchange(
            connection,
            (
                "XGROUP",
                "CREATE",
                key,
                group,
                id,
                Flag("MKSTREAM", mkstream),
                NamedOption("ENTRIESREAD", entriesread),
            ),
        )
        .await
    }

    #[doc = " Creates a consumer in a consumer group."]
    #[doc = ""]
    #[doc = " XGROUP CREATECONSUMER key group consumer"]
    #[doc = " https://valkey.io/commands/xgroup-createconsumer/"]
    #[allow(async_fn_in_trait)]
    async fn xgroup_createconsumer(
        connection: &Connection,
        key: String,
        group: String,
        consumer: String,
    ) -> Result<bool, Error> {
        exchange(
            connection,
            ("XGROUP", "CREATECONSUMER", key, group, consumer),
        )
        .await
    }

    #[doc = " Deletes a consumer from a consumer group."]
    #[doc = ""]
    #[doc = " XGROUP DELCONSUMER key group consumer"]
    #[doc = " https://valkey.io/commands/xgroup-delconsumer/"]
    #[allow(async_fn_in_trait)]
    async fn xgroup_delconsumer(
        connection: &Connection,
        key: String,
        group: String,
        consumer: String,
    ) -> Result<Integer, Error> {
        exchange(connection, ("XGROUP", "DELCONSUMER", key, group, consumer)).await
    }

    #[doc = " Destroys a consumer group."]
    #[doc = ""]
    #[doc = " XGROUP DESTROY key group"]
    #[doc = " https://valkey.io/commands/xgroup-destroy/"]
    #[allow(async_fn_in_trait)]
    async fn xgroup_destroy(
        connection: &Connection,
        key: String,
        group: String,
    ) -> Result<bool, Error> {
        exchange(connection, ("XGROUP", "DESTROY", key, group)).await
    }

    #[doc = " Sets the last-delivered ID of a consumer group."]
    #[doc = ""]
    #[doc = " XGROUP SETID key group 〈 id | $ 〉 [ ENTRIESREAD entriesread ]"]
    #[doc = " https://valkey.io/commands/xgroup-setid/"]
    #[allow(async_fn_in_trait)]
    async fn xgroup_setid(
        connection: &Connection,
        key: String,
        group: String,
        id: StreamId,
        entriesread: Option<Integer>,
    ) -> Result<(), Error> {
        exchange(
            connection,
            (
                "XGROUP",
                "SETID",
                key,
                group,
                id,
                NamedOption("ENTRIESREAD", entriesread),
            ),
        )
        .await
    }

    #[doc = " Returns a list of the consumers in a consumer group."]
    #[doc = ""]
    #[doc = " XINFO CONSUMERS key group"]
    #[doc = " https://valkey.io/commands/xinfo-consumers/"]
    #[allow(async_fn_in_trait)]
    async fn xinfo_consumers(
        connection: &Connection,
        key: String,
        group: String,
    ) -> Result<Vec<XinfoConsumersEntry>, Error> {
        exchange(connection, ("XINFO", "CONSUMERS", key, group)).await
    }

    #[doc = " Returns a list of the consumer groups of a stream."]
    #[doc = ""]
    #[doc = " XINFO GROUPS key"]
    #[doc = " https://valkey.io/commands/xinfo-groups/"]
    #[allow(async_fn_in_trait)]
    async fn xinfo_groups(
        connection: &Connection,
        key: String,
    ) -> Result<Vec<XinfoGroupsEntry>, Error> {
        exchange(connection, ("XINFO", "GROUPS", key)).await
    }

    #[doc = " Returns information about a stream."]
    #[doc = ""]
    #[doc = " See also `xinfo-stream-full`."]
    #[doc = ""]
    #[doc = " XINFO STREAM key"]
    #[doc = " https://valkey.io/commands/xinfo-stream/"]
    #[allow(async_fn_in_trait)]
    async fn xinfo_stream(connection: &Connection, key: String) -> Result<XinfoStreamReply, Error> {
        exchange(connection, ("XINFO", "STREAM", key)).await
    }

    #[doc = " Returns information about a stream."]
    #[doc = ""]
    #[doc = " The `FULL` form, with the entries, groups and consumers."]
    #[doc = ""]
    #[doc = " See also `xinfo-stream`."]
    #[doc = ""]
    #[doc = " XINFO STREAM key FULL [ COUNT count ]"]
    #[doc = " https://valkey.io/commands/xinfo-stream/"]
    #[allow(async_fn_in_trait)]
    async fn xinfo_stream_full(
        connection: &Connection,
        key: String,
        count: Option<Integer>,
    ) -> Result<XinfoStreamReplyFull, Error> {
        exchange(
            connection,
            ("XINFO", "STREAM", key, "FULL", NamedOption("COUNT", count)),
        )
        .await
    }

    #[doc = " Returns the number of messages in a stream."]
    #[doc = ""]
    #[doc = " XLEN key"]
    #[doc = " https://valkey.io/commands/xlen/"]
    #[allow(async_fn_in_trait)]
    async fn xlen(connection: &Connection, key: String) -> Result<Integer, Error> {
        exchange(connection, ("XLEN", key)).await
    }

    #[doc = " Returns the information and entries from a stream consumer group's pending entries list."]
    #[doc = ""]
    #[doc = " The summary form."]
    #[doc = ""]
    #[doc = " See also `xpending-extended`."]
    #[doc = ""]
    #[doc = " XPENDING key group"]
    #[doc = " https://valkey.io/commands/xpending/"]
    #[allow(async_fn_in_trait)]
    async fn xpending(
        connection: &Connection,
        key: String,
        group: String,
    ) -> Result<XpendingSummary, Error> {
        exchange(connection, ("XPENDING", key, group)).await
    }

    #[doc = " Returns the information and entries from a stream consumer group's pending entries list."]
    #[doc = ""]
    #[doc = " The extended form, the pending entries in the range."]
    #[doc = ""]
    #[doc = " See also `xpending`."]
    #[doc = ""]
    #[doc = " XPENDING key group [ IDLE min-idle-time ] start end count [ consumer ]"]
    #[doc = " https://valkey.io/commands/xpending/"]
    #[allow(async_fn_in_trait)]
    async fn xpending_extended(
        connection: &Connection,
        key: String,
        group: String,
        min_idle_time: Option<DurationMilliseconds>,
        start: StreamId,
        end: StreamId,
        count: Integer,
        consumer: Option<String>,
    ) -> Result<Vec<XpendingExtendedEntry>, Error> {
        exchange(
            connection,
            (
                "XPENDING",
                key,
                group,
                NamedOption("IDLE", min_idle_time),
                start,
                end,
                count,
                consumer,
            ),
        )
        .await
    }

    #[doc = " Returns the messages from a stream within a range of IDs."]
    #[doc = ""]
    #[doc = " XRANGE key start end [ COUNT count ]"]
    #[doc = " https://valkey.io/commands/xrange/"]
    #[allow(async_fn_in_trait)]
    async fn xrange(
        connection: &Connection,
        key: String,
        start: StreamId,
        end: StreamId,
        count: Option<Integer>,
    ) -> Result<Vec<(StreamId, Map<String, StringOrBytes>)>, Error> {
        exchange(
            connection,
            ("XRANGE", key, start, end, NamedOption("COUNT", count)),
        )
        .await
    }

    #[doc = " Returns messages from multiple streams with IDs greater than the ones requested."]
    #[doc = ""]
    #[doc = " With `block`, returns none when the timeout passed. The connection waits too, replies to"]
    #[doc = " commands sent after it on the connection wait behind it. Use a connection of your own."]
    #[doc = ""]
    #[doc = " XREAD [ COUNT count ] [ BLOCK milliseconds ] STREAMS key [ key ... ] ID [ ID ... ]"]
    #[doc = " https://valkey.io/commands/xread/"]
    #[allow(async_fn_in_trait)]
    async fn xread(
        connection: &Connection,
        count: Option<Integer>,
        block: Option<DurationMilliseconds>,
        keys: Vec<String>,
        ids: Vec<StreamId>,
    ) -> Result<Option<Map<String, Vec<(StreamId, Map<String, StringOrBytes>)>>>, Error> {
        exchange(
            connection,
            (
                "XREAD",
                NamedOption("COUNT", count),
                NamedOption("BLOCK", block),
                "STREAMS",
                NonEmpty::new("keys", keys),
                NonEmpty::new("ids", ids),
            ),
        )
        .await
    }

    #[doc = " Returns new or historical messages from a stream for a consumer in a group."]
    #[doc = ""]
    #[doc = " The fields of an entry are none when it was deleted after it was delivered, e.g. when"]
    #[doc = " reading the pending entries with the ID `0`."]
    #[doc = ""]
    #[doc = " With `block`, returns none when the timeout passed. The connection waits too, replies to"]
    #[doc = " commands sent after it on the connection wait behind it. Use a connection of your own."]
    #[doc = ""]
    #[doc = " XREADGROUP GROUP group consumer [ COUNT count ] [ BLOCK milliseconds ] [ NOACK ] STREAMS key [ key ... ] ID [ ID ... ]"]
    #[doc = " https://valkey.io/commands/xreadgroup/"]
    #[allow(async_fn_in_trait)]
    async fn xreadgroup(
        connection: &Connection,
        group: String,
        consumer: String,
        count: Option<Integer>,
        block: Option<DurationMilliseconds>,
        noack: bool,
        keys: Vec<String>,
        ids: Vec<StreamId>,
    ) -> Result<Option<Map<String, Vec<(StreamId, Option<Map<String, StringOrBytes>>)>>>, Error>
    {
        exchange(
            connection,
            (
                "XREADGROUP",
                "GROUP",
                group,
                consumer,
                NamedOption("COUNT", count),
                NamedOption("BLOCK", block),
                Flag("NOACK", noack),
                "STREAMS",
                NonEmpty::new("keys", keys),
                NonEmpty::new("ids", ids),
            ),
        )
        .await
    }

    #[doc = " Returns the messages from a stream within a range of IDs in reverse order."]
    #[doc = ""]
    #[doc = " XREVRANGE key end start [ COUNT count ]"]
    #[doc = " https://valkey.io/commands/xrevrange/"]
    #[allow(async_fn_in_trait)]
    async fn xrevrange(
        connection: &Connection,
        key: String,
        end: StreamId,
        start: StreamId,
        count: Option<Integer>,
    ) -> Result<Vec<(StreamId, Map<String, StringOrBytes>)>, Error> {
        exchange(
            connection,
            ("XREVRANGE", key, end, start, NamedOption("COUNT", count)),
        )
        .await
    }

    #[doc = " An internal command for replicating stream values."]
    #[doc = ""]
    #[doc = " XSETID key last-id [ ENTRIESADDED entries-added ] [ MAXDELETEDID max-deleted-id ]"]
    #[doc = " https://valkey.io/commands/xsetid/"]
    #[allow(async_fn_in_trait)]
    async fn xsetid(
        connection: &Connection,
        key: String,
        last_id: StreamId,
        entries_added: Option<Integer>,
        max_deleted_id: Option<StreamId>,
    ) -> Result<(), Error> {
        exchange(
            connection,
            (
                "XSETID",
                key,
                last_id,
                NamedOption("ENTRIESADDED", entries_added),
                NamedOption("MAXDELETEDID", max_deleted_id),
            ),
        )
        .await
    }

    #[doc = " Deletes messages from the beginning of a stream."]
    #[doc = ""]
    #[doc = " XTRIM key 〈 MAXLEN | MINID 〉 [ = | ~ ] threshold [ LIMIT count ]"]
    #[doc = " https://valkey.io/commands/xtrim/"]
    #[allow(async_fn_in_trait)]
    async fn xtrim(
        connection: &Connection,
        key: String,
        trim: StreamTrim,
    ) -> Result<Integer, Error> {
        exchange(connection, ("XTRIM", key, trim)).await
    }

    #[doc = " Adds one or more members to a set."]
    #[doc = ""]
    #[doc = " SADD key member [ member ... ]"]
    #[doc = " https://valkey.io/commands/sadd/"]
    #[allow(async_fn_in_trait)]
    async fn sadd(
        connection: &Connection,
        key: String,
        members: Vec<StringOrBytes>,
    ) -> Result<Integer, Error> {
        exchange(connection, ("SADD", key, NonEmpty::new("members", members))).await
    }

    #[doc = " Returns the number of members in a set."]
    #[doc = ""]
    #[doc = " SCARD key"]
    #[doc = " https://valkey.io/commands/scard/"]
    #[allow(async_fn_in_trait)]
    async fn scard(connection: &Connection, key: String) -> Result<Integer, Error> {
        exchange(connection, ("SCARD", key)).await
    }

    #[doc = " Returns the difference of multiple sets."]
    #[doc = ""]
    #[doc = " SDIFF key [ key ... ]"]
    #[doc = " https://valkey.io/commands/sdiff/"]
    #[allow(async_fn_in_trait)]
    async fn sdiff(
        connection: &Connection,
        keys: Vec<String>,
    ) -> Result<Vec<StringOrBytes>, Error> {
        exchange(connection, ("SDIFF", NonEmpty::new("keys", keys))).await
    }

    #[doc = " Stores the difference of multiple sets in a key."]
    #[doc = ""]
    #[doc = " SDIFFSTORE destination key [ key ... ]"]
    #[doc = " https://valkey.io/commands/sdiffstore/"]
    #[allow(async_fn_in_trait)]
    async fn sdiffstore(
        connection: &Connection,
        destination: String,
        keys: Vec<String>,
    ) -> Result<Integer, Error> {
        exchange(
            connection,
            ("SDIFFSTORE", destination, NonEmpty::new("keys", keys)),
        )
        .await
    }

    #[doc = " Returns the intersect of multiple sets."]
    #[doc = ""]
    #[doc = " SINTER key [ key ... ]"]
    #[doc = " https://valkey.io/commands/sinter/"]
    #[allow(async_fn_in_trait)]
    async fn sinter(
        connection: &Connection,
        keys: Vec<String>,
    ) -> Result<Vec<StringOrBytes>, Error> {
        exchange(connection, ("SINTER", NonEmpty::new("keys", keys))).await
    }

    #[doc = " Returns the number of members of the intersect of multiple sets."]
    #[doc = ""]
    #[doc = " SINTERCARD numkeys key [ key ... ] [ LIMIT limit ]"]
    #[doc = " https://valkey.io/commands/sintercard/"]
    #[allow(async_fn_in_trait)]
    async fn sintercard(
        connection: &Connection,
        keys: Vec<String>,
        limit: Option<Integer>,
    ) -> Result<Integer, Error> {
        exchange(
            connection,
            (
                "SINTERCARD",
                NumKeys(NonEmpty::new("keys", keys)),
                NamedOption("LIMIT", limit),
            ),
        )
        .await
    }

    #[doc = " Stores the intersect of multiple sets in a key."]
    #[doc = ""]
    #[doc = " SINTERSTORE destination key [ key ... ]"]
    #[doc = " https://valkey.io/commands/sinterstore/"]
    #[allow(async_fn_in_trait)]
    async fn sinterstore(
        connection: &Connection,
        destination: String,
        keys: Vec<String>,
    ) -> Result<Integer, Error> {
        exchange(
            connection,
            ("SINTERSTORE", destination, NonEmpty::new("keys", keys)),
        )
        .await
    }

    #[doc = " Determines whether a member belongs to a set."]
    #[doc = ""]
    #[doc = " False when the key does not exist."]
    #[doc = ""]
    #[doc = " See also `sismember-xx`."]
    #[doc = ""]
    #[doc = " SISMEMBER key member"]
    #[doc = " https://valkey.io/commands/sismember/"]
    #[allow(async_fn_in_trait)]
    async fn sismember(
        connection: &Connection,
        key: String,
        member: StringOrBytes,
    ) -> Result<bool, Error> {
        exchange(connection, ("SISMEMBER", key, member)).await
    }

    #[doc = " Determines whether a member belongs to a set."]
    #[doc = ""]
    #[doc = " The `XX` form, none when the key does not exist, rather than false."]
    #[doc = ""]
    #[doc = " See also `sismember`."]
    #[doc = ""]
    #[doc = " SISMEMBER key member XX"]
    #[doc = " https://valkey.io/commands/sismember/"]
    #[allow(async_fn_in_trait)]
    async fn sismember_xx(
        connection: &Connection,
        key: String,
        member: StringOrBytes,
    ) -> Result<Option<bool>, Error> {
        // -1 when the key does not exist
        let member: OrNever<bool> = exchange(connection, ("SISMEMBER", key, member, "XX")).await?;
        Ok(member.0)
    }

    #[doc = " Returns all members of a set."]
    #[doc = ""]
    #[doc = " SMEMBERS key"]
    #[doc = " https://valkey.io/commands/smembers/"]
    #[allow(async_fn_in_trait)]
    async fn smembers(connection: &Connection, key: String) -> Result<Vec<StringOrBytes>, Error> {
        exchange(connection, ("SMEMBERS", key)).await
    }

    #[doc = " Determines whether multiple members belong to a set."]
    #[doc = ""]
    #[doc = " SMISMEMBER key member [ member ... ]"]
    #[doc = " https://valkey.io/commands/smismember/"]
    #[allow(async_fn_in_trait)]
    async fn smismember(
        connection: &Connection,
        key: String,
        members: Vec<StringOrBytes>,
    ) -> Result<Vec<bool>, Error> {
        exchange(
            connection,
            ("SMISMEMBER", key, NonEmpty::new("members", members)),
        )
        .await
    }

    #[doc = " Moves a member from one set to another."]
    #[doc = ""]
    #[doc = " SMOVE source destination member"]
    #[doc = " https://valkey.io/commands/smove/"]
    #[allow(async_fn_in_trait)]
    async fn smove(
        connection: &Connection,
        source: String,
        destination: String,
        member: StringOrBytes,
    ) -> Result<bool, Error> {
        exchange(connection, ("SMOVE", source, destination, member)).await
    }

    #[doc = " Returns one or more random members from a set after removing them."]
    #[doc = ""]
    #[doc = " Without a count, at most one member."]
    #[doc = ""]
    #[doc = " SPOP key [ count ]"]
    #[doc = " https://valkey.io/commands/spop/"]
    #[allow(async_fn_in_trait)]
    async fn spop(
        connection: &Connection,
        key: String,
        count: Option<Integer>,
    ) -> Result<Vec<StringOrBytes>, Error> {
        if count.is_some() {
            return exchange(connection, ("SPOP", key, count)).await;
        }
        // without a count the reply is the member itself, or null
        let member: Option<StringOrBytes> = exchange(connection, ("SPOP", key)).await?;
        Ok(member.into_iter().collect())
    }

    #[doc = " Gets one or multiple random members from a set."]
    #[doc = ""]
    #[doc = " Without a count, at most one member."]
    #[doc = ""]
    #[doc = " SRANDMEMBER key [ count ]"]
    #[doc = " https://valkey.io/commands/srandmember/"]
    #[allow(async_fn_in_trait)]
    async fn srandmember(
        connection: &Connection,
        key: String,
        count: Option<Integer>,
    ) -> Result<Vec<StringOrBytes>, Error> {
        if count.is_some() {
            return exchange(connection, ("SRANDMEMBER", key, count)).await;
        }
        // without a count the reply is the member itself, or null
        let member: Option<StringOrBytes> = exchange(connection, ("SRANDMEMBER", key)).await?;
        Ok(member.into_iter().collect())
    }

    #[doc = " Removes one or more members from a set."]
    #[doc = ""]
    #[doc = " SREM key member [ member ... ]"]
    #[doc = " https://valkey.io/commands/srem/"]
    #[allow(async_fn_in_trait)]
    async fn srem(
        connection: &Connection,
        key: String,
        members: Vec<StringOrBytes>,
    ) -> Result<Integer, Error> {
        exchange(connection, ("SREM", key, NonEmpty::new("members", members))).await
    }

    #[doc = " Iterates over members of a set."]
    #[doc = ""]
    #[doc = " SSCAN key cursor [ MATCH pattern ] [ COUNT count ]"]
    #[doc = " https://valkey.io/commands/sscan/"]
    #[allow(async_fn_in_trait)]
    async fn sscan(
        connection: &Connection,
        key: String,
        cursor: Cursor,
        match_: Option<String>,
        count: Option<Integer>,
    ) -> Result<(Option<Cursor>, Vec<StringOrBytes>), Error> {
        let (cursor, items): (NextCursor, Vec<StringOrBytes>) = exchange(
            connection,
            (
                "SSCAN",
                key,
                cursor,
                NamedOption("MATCH", match_),
                NamedOption("COUNT", count),
            ),
        )
        .await?;
        Ok((cursor.0, items))
    }

    #[doc = " Returns the union of multiple sets."]
    #[doc = ""]
    #[doc = " SUNION key [ key ... ]"]
    #[doc = " https://valkey.io/commands/sunion/"]
    #[allow(async_fn_in_trait)]
    async fn sunion(
        connection: &Connection,
        keys: Vec<String>,
    ) -> Result<Vec<StringOrBytes>, Error> {
        exchange(connection, ("SUNION", NonEmpty::new("keys", keys))).await
    }

    #[doc = " Stores the union of multiple sets in a key."]
    #[doc = ""]
    #[doc = " SUNIONSTORE destination key [ key ... ]"]
    #[doc = " https://valkey.io/commands/sunionstore/"]
    #[allow(async_fn_in_trait)]
    async fn sunionstore(
        connection: &Connection,
        destination: String,
        keys: Vec<String>,
    ) -> Result<Integer, Error> {
        exchange(
            connection,
            ("SUNIONSTORE", destination, NonEmpty::new("keys", keys)),
        )
        .await
    }

    #[doc = " Sorts the elements in a list, a set, or a sorted set, optionally storing the result."]
    #[doc = ""]
    #[doc = " Each row is the element, or with `get`, the value of each pattern, none when its key does"]
    #[doc = " not exist."]
    #[doc = ""]
    #[doc = " See also `sort-store`."]
    #[doc = ""]
    #[doc = " SORT key [ BY pattern ] [ LIMIT offset count ] [ GET pattern ... ] [ ASC | DESC ] [ ALPHA ]"]
    #[doc = " https://valkey.io/commands/sort/"]
    #[allow(async_fn_in_trait)]
    async fn sort(
        connection: &Connection,
        key: String,
        opts: Option<SortOpts>,
    ) -> Result<Vec<Vec<Option<StringOrBytes>>>, Error> {
        let row = sort_row_len(&opts);
        let values: Vec<Option<StringOrBytes>> = exchange(connection, ("SORT", key, opts)).await?;
        Ok(rows(values, row))
    }

    #[doc = " Sorts the elements in a list, a set, or a sorted set, optionally storing the result."]
    #[doc = ""]
    #[doc = " The `STORE` form, storing the elements as a list, returning how many."]
    #[doc = ""]
    #[doc = " See also `sort`."]
    #[doc = ""]
    #[doc = " SORT key [ BY pattern ] [ LIMIT offset count ] [ GET pattern ... ] [ ASC | DESC ] [ ALPHA ] STORE destination"]
    #[doc = " https://valkey.io/commands/sort/"]
    #[allow(async_fn_in_trait)]
    async fn sort_store(
        connection: &Connection,
        key: String,
        opts: Option<SortOpts>,
        destination: String,
    ) -> Result<Integer, Error> {
        exchange(connection, ("SORT", key, opts, "STORE", destination)).await
    }

    #[doc = " Returns the sorted elements of a list, a set, or a sorted set."]
    #[doc = ""]
    #[doc = " Each row is the element, or with `get`, the value of each pattern, none when its key does"]
    #[doc = " not exist."]
    #[doc = ""]
    #[doc = " SORT_RO key [ BY pattern ] [ LIMIT offset count ] [ GET pattern ... ] [ ASC | DESC ] [ ALPHA ]"]
    #[doc = " https://valkey.io/commands/sort_ro/"]
    #[allow(async_fn_in_trait)]
    async fn sort_ro(
        connection: &Connection,
        key: String,
        opts: Option<SortRoOpts>,
    ) -> Result<Vec<Vec<Option<StringOrBytes>>>, Error> {
        let row = sort_row_len(&opts);
        let values: Vec<Option<StringOrBytes>> =
            exchange(connection, ("SORT_RO", key, opts)).await?;
        Ok(rows(values, row))
    }

    #[doc = " Removes and returns a member by score from one or more sorted sets."]
    #[doc = ""]
    #[doc = " Returns none when the timeout passed. The connection waits too, replies to commands sent"]
    #[doc = " after it on the connection wait behind it. Use a connection of your own."]
    #[doc = ""]
    #[doc = " BZMPOP timeout numkeys key [ key ... ] 〈 MIN | MAX 〉 [ COUNT count ]"]
    #[doc = " https://valkey.io/commands/bzmpop/"]
    #[allow(async_fn_in_trait)]
    async fn bzmpop(
        connection: &Connection,
        timeout: DurationMilliseconds,
        keys: Vec<String>,
        end: BzmpopEnd,
        count: Option<Integer>,
    ) -> Result<Option<(String, Vec<(StringOrBytes, Double)>)>, Error> {
        let command = (
            "BZMPOP",
            DecimalSeconds(timeout),
            NumKeys(NonEmpty::new("keys", keys)),
            end,
            NamedOption("COUNT", count),
        );
        exchange(connection, command).await
    }

    #[doc = " Removes and returns the member with the highest score from one or more sorted sets."]
    #[doc = ""]
    #[doc = " Returns none when the timeout passed. The connection waits too, replies to commands sent"]
    #[doc = " after it on the connection wait behind it. Use a connection of your own."]
    #[doc = ""]
    #[doc = " BZPOPMAX key [ key ... ] timeout"]
    #[doc = " https://valkey.io/commands/bzpopmax/"]
    #[allow(async_fn_in_trait)]
    async fn bzpopmax(
        connection: &Connection,
        keys: Vec<String>,
        timeout: DurationMilliseconds,
    ) -> Result<Option<(String, StringOrBytes, Double)>, Error> {
        exchange(
            connection,
            (
                "BZPOPMAX",
                NonEmpty::new("keys", keys),
                DecimalSeconds(timeout),
            ),
        )
        .await
    }

    #[doc = " Removes and returns the member with the lowest score from one or more sorted sets."]
    #[doc = ""]
    #[doc = " Returns none when the timeout passed. The connection waits too, replies to commands sent"]
    #[doc = " after it on the connection wait behind it. Use a connection of your own."]
    #[doc = ""]
    #[doc = " BZPOPMIN key [ key ... ] timeout"]
    #[doc = " https://valkey.io/commands/bzpopmin/"]
    #[allow(async_fn_in_trait)]
    async fn bzpopmin(
        connection: &Connection,
        keys: Vec<String>,
        timeout: DurationMilliseconds,
    ) -> Result<Option<(String, StringOrBytes, Double)>, Error> {
        exchange(
            connection,
            (
                "BZPOPMIN",
                NonEmpty::new("keys", keys),
                DecimalSeconds(timeout),
            ),
        )
        .await
    }

    #[doc = " Adds one or more members to a sorted set, or updates their scores."]
    #[doc = ""]
    #[doc = " See also `zadd-incr`."]
    #[doc = ""]
    #[doc = " ZADD key [ NX | XX ] [ GT | LT ] [ CH ] score member [ score member ... ]"]
    #[doc = " https://valkey.io/commands/zadd/"]
    #[allow(async_fn_in_trait)]
    async fn zadd(
        connection: &Connection,
        key: String,
        condition: Option<ZaddCondition>,
        comparison: Option<ZaddComparison>,
        changed: bool,
        score_and_members: Vec<(Double, StringOrBytes)>,
    ) -> Result<Option<Integer>, Error> {
        let command = (
            "ZADD",
            key,
            condition,
            comparison,
            Flag("CH", changed),
            NonEmpty::new("score-and-members", score_and_members),
        );
        exchange(connection, command).await
    }

    #[doc = " Adds one or more members to a sorted set, or updates their scores."]
    #[doc = ""]
    #[doc = " The `INCR` form, returning the new score, none when the condition or comparison prevented"]
    #[doc = " it."]
    #[doc = ""]
    #[doc = " See also `zadd`."]
    #[doc = ""]
    #[doc = " ZADD key [ NX | XX ] [ GT | LT ] INCR score member"]
    #[doc = " https://valkey.io/commands/zadd/"]
    #[allow(async_fn_in_trait)]
    async fn zadd_incr(
        connection: &Connection,
        key: String,
        condition: Option<ZaddCondition>,
        comparison: Option<ZaddComparison>,
        increment: Double,
        member: StringOrBytes,
    ) -> Result<Option<Double>, Error> {
        let command = (
            "ZADD", key, condition, comparison, "INCR", increment, member,
        );
        exchange(connection, command).await
    }

    #[doc = " Returns the number of members in a sorted set."]
    #[doc = ""]
    #[doc = " ZCARD key"]
    #[doc = " https://valkey.io/commands/zcard/"]
    #[allow(async_fn_in_trait)]
    async fn zcard(connection: &Connection, key: String) -> Result<Integer, Error> {
        exchange(connection, ("ZCARD", key)).await
    }

    #[doc = " Returns the count of members in a sorted set that have scores within a range."]
    #[doc = ""]
    #[doc = " ZCOUNT key min max"]
    #[doc = " https://valkey.io/commands/zcount/"]
    #[allow(async_fn_in_trait)]
    async fn zcount(
        connection: &Connection,
        key: String,
        min: ScoreBound,
        max: ScoreBound,
    ) -> Result<Integer, Error> {
        exchange(
            connection,
            (
                "ZCOUNT",
                key,
                ScoreRange {
                    start: min,
                    stop: max,
                    rev: false,
                },
            ),
        )
        .await
    }

    #[doc = " Returns the difference between multiple sorted sets."]
    #[doc = ""]
    #[doc = " See also `zdiff-withscores`."]
    #[doc = ""]
    #[doc = " ZDIFF numkeys key [ key ... ]"]
    #[doc = " https://valkey.io/commands/zdiff/"]
    #[allow(async_fn_in_trait)]
    async fn zdiff(
        connection: &Connection,
        keys: Vec<String>,
    ) -> Result<Vec<StringOrBytes>, Error> {
        exchange(connection, ("ZDIFF", NumKeys(NonEmpty::new("keys", keys)))).await
    }

    #[doc = " Returns the difference between multiple sorted sets."]
    #[doc = ""]
    #[doc = " The `WITHSCORES` form, the members and their scores."]
    #[doc = ""]
    #[doc = " See also `zdiff`."]
    #[doc = ""]
    #[doc = " ZDIFF numkeys key [ key ... ] WITHSCORES"]
    #[doc = " https://valkey.io/commands/zdiff/"]
    #[allow(async_fn_in_trait)]
    async fn zdiff_withscores(
        connection: &Connection,
        keys: Vec<String>,
    ) -> Result<Vec<(StringOrBytes, Double)>, Error> {
        exchange(
            connection,
            ("ZDIFF", NumKeys(NonEmpty::new("keys", keys)), "WITHSCORES"),
        )
        .await
    }

    #[doc = " Stores the difference of multiple sorted sets in a key."]
    #[doc = ""]
    #[doc = " ZDIFFSTORE destination numkeys key [ key ... ]"]
    #[doc = " https://valkey.io/commands/zdiffstore/"]
    #[allow(async_fn_in_trait)]
    async fn zdiffstore(
        connection: &Connection,
        destination: String,
        keys: Vec<String>,
    ) -> Result<Integer, Error> {
        exchange(
            connection,
            (
                "ZDIFFSTORE",
                destination,
                NumKeys(NonEmpty::new("keys", keys)),
            ),
        )
        .await
    }

    #[doc = " Increments the score of a member in a sorted set."]
    #[doc = ""]
    #[doc = " ZINCRBY key increment member"]
    #[doc = " https://valkey.io/commands/zincrby/"]
    #[allow(async_fn_in_trait)]
    async fn zincrby(
        connection: &Connection,
        key: String,
        increment: Double,
        member: StringOrBytes,
    ) -> Result<Double, Error> {
        exchange(connection, ("ZINCRBY", key, increment, member)).await
    }

    #[doc = " Returns the intersect of multiple sorted sets."]
    #[doc = ""]
    #[doc = " See also `zinter-withscores`."]
    #[doc = ""]
    #[doc = " ZINTER numkeys key [ key ... ] [ WEIGHTS weight [ weight ... ] ] [ AGGREGATE SUM | MIN | MAX ]"]
    #[doc = " https://valkey.io/commands/zinter/"]
    #[allow(async_fn_in_trait)]
    async fn zinter(
        connection: &Connection,
        keys: Vec<String>,
        weights: Option<Vec<Double>>,
        aggregate: Option<ZsetAggregate>,
    ) -> Result<Vec<StringOrBytes>, Error> {
        exchange(
            connection,
            (
                "ZINTER",
                NumKeys(NonEmpty::new("keys", keys)),
                NamedOption("WEIGHTS", NonEmpty::optional("weights", weights)),
                aggregate,
            ),
        )
        .await
    }

    #[doc = " Returns the intersect of multiple sorted sets."]
    #[doc = ""]
    #[doc = " The `WITHSCORES` form, the members and their combined scores."]
    #[doc = ""]
    #[doc = " See also `zinter`."]
    #[doc = ""]
    #[doc = " ZINTER numkeys key [ key ... ] [ WEIGHTS weight [ weight ... ] ] [ AGGREGATE SUM | MIN | MAX ] WITHSCORES"]
    #[doc = " https://valkey.io/commands/zinter/"]
    #[allow(async_fn_in_trait)]
    async fn zinter_withscores(
        connection: &Connection,
        keys: Vec<String>,
        weights: Option<Vec<Double>>,
        aggregate: Option<ZsetAggregate>,
    ) -> Result<Vec<(StringOrBytes, Double)>, Error> {
        let command = (
            "ZINTER",
            NumKeys(NonEmpty::new("keys", keys)),
            NamedOption("WEIGHTS", NonEmpty::optional("weights", weights)),
            aggregate,
            "WITHSCORES",
        );
        exchange(connection, command).await
    }

    #[doc = " Returns the number of members of the intersect of multiple sorted sets."]
    #[doc = ""]
    #[doc = " ZINTERCARD numkeys key [ key ... ] [ LIMIT limit ]"]
    #[doc = " https://valkey.io/commands/zintercard/"]
    #[allow(async_fn_in_trait)]
    async fn zintercard(
        connection: &Connection,
        keys: Vec<String>,
        limit: Option<Integer>,
    ) -> Result<Integer, Error> {
        exchange(
            connection,
            (
                "ZINTERCARD",
                NumKeys(NonEmpty::new("keys", keys)),
                NamedOption("LIMIT", limit),
            ),
        )
        .await
    }

    #[doc = " Stores the intersect of multiple sorted sets in a key."]
    #[doc = ""]
    #[doc = " ZINTERSTORE destination numkeys key [ key ... ] [ WEIGHTS weight [ weight ... ] ] [ AGGREGATE SUM | MIN | MAX ]"]
    #[doc = " https://valkey.io/commands/zinterstore/"]
    #[allow(async_fn_in_trait)]
    async fn zinterstore(
        connection: &Connection,
        destination: String,
        keys: Vec<String>,
        weights: Option<Vec<Double>>,
        aggregate: Option<ZsetAggregate>,
    ) -> Result<Integer, Error> {
        let command = (
            "ZINTERSTORE",
            destination,
            NumKeys(NonEmpty::new("keys", keys)),
            NamedOption("WEIGHTS", NonEmpty::optional("weights", weights)),
            aggregate,
        );
        exchange(connection, command).await
    }

    #[doc = " Returns the number of members in a sorted set within a lexicographical range."]
    #[doc = ""]
    #[doc = " ZLEXCOUNT key min max"]
    #[doc = " https://valkey.io/commands/zlexcount/"]
    #[allow(async_fn_in_trait)]
    async fn zlexcount(
        connection: &Connection,
        key: String,
        min: LexBound,
        max: LexBound,
    ) -> Result<Integer, Error> {
        exchange(
            connection,
            (
                "ZLEXCOUNT",
                key,
                LexRange {
                    start: min,
                    stop: max,
                    rev: false,
                },
            ),
        )
        .await
    }

    #[doc = " Returns the highest- or lowest-scoring members from one or more sorted sets after removing them."]
    #[doc = ""]
    #[doc = " ZMPOP numkeys key [ key ... ] 〈 MIN | MAX 〉 [ COUNT count ]"]
    #[doc = " https://valkey.io/commands/zmpop/"]
    #[allow(async_fn_in_trait)]
    async fn zmpop(
        connection: &Connection,
        keys: Vec<String>,
        end: ZmpopEnd,
        count: Option<Integer>,
    ) -> Result<Option<(String, Vec<(StringOrBytes, Double)>)>, Error> {
        exchange(
            connection,
            (
                "ZMPOP",
                NumKeys(NonEmpty::new("keys", keys)),
                end,
                NamedOption("COUNT", count),
            ),
        )
        .await
    }

    #[doc = " Returns the score of one or more members in a sorted set."]
    #[doc = ""]
    #[doc = " ZMSCORE key member [ member ... ]"]
    #[doc = " https://valkey.io/commands/zmscore/"]
    #[allow(async_fn_in_trait)]
    async fn zmscore(
        connection: &Connection,
        key: String,
        members: Vec<StringOrBytes>,
    ) -> Result<Vec<Option<Double>>, Error> {
        exchange(
            connection,
            ("ZMSCORE", key, NonEmpty::new("members", members)),
        )
        .await
    }

    #[doc = " Returns the highest-scoring members from a sorted set after removing them."]
    #[doc = ""]
    #[doc = " Without a count, at most one member."]
    #[doc = ""]
    #[doc = " ZPOPMAX key [ count ]"]
    #[doc = " https://valkey.io/commands/zpopmax/"]
    #[allow(async_fn_in_trait)]
    async fn zpopmax(
        connection: &Connection,
        key: String,
        count: Option<Integer>,
    ) -> Result<Vec<(StringOrBytes, Double)>, Error> {
        let members: ScoredMembers = exchange(connection, ("ZPOPMAX", key, count)).await?;
        Ok(members.0)
    }

    #[doc = " Returns the lowest-scoring members from a sorted set after removing them."]
    #[doc = ""]
    #[doc = " Without a count, at most one member."]
    #[doc = ""]
    #[doc = " ZPOPMIN key [ count ]"]
    #[doc = " https://valkey.io/commands/zpopmin/"]
    #[allow(async_fn_in_trait)]
    async fn zpopmin(
        connection: &Connection,
        key: String,
        count: Option<Integer>,
    ) -> Result<Vec<(StringOrBytes, Double)>, Error> {
        let members: ScoredMembers = exchange(connection, ("ZPOPMIN", key, count)).await?;
        Ok(members.0)
    }

    #[doc = " Returns one or more random members from a sorted set."]
    #[doc = ""]
    #[doc = " Without a count, at most one member."]
    #[doc = ""]
    #[doc = " See also `zrandmember-withscores`."]
    #[doc = ""]
    #[doc = " ZRANDMEMBER key [ count ]"]
    #[doc = " https://valkey.io/commands/zrandmember/"]
    #[allow(async_fn_in_trait)]
    async fn zrandmember(
        connection: &Connection,
        key: String,
        count: Option<Integer>,
    ) -> Result<Vec<StringOrBytes>, Error> {
        if count.is_some() {
            return exchange(connection, ("ZRANDMEMBER", key, count)).await;
        }
        // without a count the reply is the member itself, or null
        let member: Option<StringOrBytes> = exchange(connection, ("ZRANDMEMBER", key)).await?;
        Ok(member.into_iter().collect())
    }

    #[doc = " Returns one or more random members from a sorted set."]
    #[doc = ""]
    #[doc = " The `WITHSCORES` form, the members and their scores."]
    #[doc = ""]
    #[doc = " See also `zrandmember`."]
    #[doc = ""]
    #[doc = " ZRANDMEMBER key count WITHSCORES"]
    #[doc = " https://valkey.io/commands/zrandmember/"]
    #[allow(async_fn_in_trait)]
    async fn zrandmember_withscores(
        connection: &Connection,
        key: String,
        count: Integer,
    ) -> Result<Vec<(StringOrBytes, Double)>, Error> {
        exchange(connection, ("ZRANDMEMBER", key, count, "WITHSCORES")).await
    }

    #[doc = " Returns members in a sorted set within a range of indexes."]
    #[doc = ""]
    #[doc = " Also covers ZRANGEBYLEX, ZRANGEBYSCORE, ZREVRANGE, ZREVRANGEBYLEX and ZREVRANGEBYSCORE. A"]
    #[doc = " limit fails the command unless the range is by score or by lex. With `xx`, returns none when"]
    #[doc = " the key does not exist."]
    #[doc = ""]
    #[doc = " See also `zrange-withscores`."]
    #[doc = ""]
    #[doc = " ZRANGE key start stop [ BYSCORE | BYLEX ] [ REV ] [ LIMIT offset count ] [ XX ]"]
    #[doc = " https://valkey.io/commands/zrange/"]
    #[allow(async_fn_in_trait)]
    async fn zrange(
        connection: &Connection,
        key: String,
        range: ZrangeBy,
        rev: bool,
        limit: Option<Limit>,
        xx: bool,
    ) -> Result<Option<Vec<StringOrBytes>>, Error> {
        let range = Zrange { range, rev, limit };
        exchange(connection, ("ZRANGE", key, range, Flag("XX", xx))).await
    }

    #[doc = " Returns members in a sorted set within a range of indexes."]
    #[doc = ""]
    #[doc = " The `WITHSCORES` form, the members and their scores. A limit fails the command unless the"]
    #[doc = " range is by score. With `xx`, returns none when the key does not exist."]
    #[doc = ""]
    #[doc = " See also `zrange`."]
    #[doc = ""]
    #[doc = " ZRANGE key start stop [ BYSCORE ] [ REV ] [ LIMIT offset count ] WITHSCORES [ XX ]"]
    #[doc = " https://valkey.io/commands/zrange/"]
    #[allow(async_fn_in_trait)]
    async fn zrange_withscores(
        connection: &Connection,
        key: String,
        range: ZrangeWithscoresBy,
        rev: bool,
        limit: Option<Limit>,
        xx: bool,
    ) -> Result<Option<Vec<(StringOrBytes, Double)>>, Error> {
        let range = match range {
            ZrangeWithscoresBy::Rank(rank) => ZrangeBy::Rank(rank),
            ZrangeWithscoresBy::Score(score) => ZrangeBy::Score(score),
        };
        let range = Zrange { range, rev, limit };
        let command = ("ZRANGE", key, range, "WITHSCORES", Flag("XX", xx));
        exchange(connection, command).await
    }

    #[doc = " Stores a range of members from sorted set in a key."]
    #[doc = ""]
    #[doc = " A limit fails the command unless the range is by score or by lex."]
    #[doc = ""]
    #[doc = " ZRANGESTORE dst src min max [ BYSCORE | BYLEX ] [ REV ] [ LIMIT offset count ]"]
    #[doc = " https://valkey.io/commands/zrangestore/"]
    #[allow(async_fn_in_trait)]
    async fn zrangestore(
        connection: &Connection,
        destination: String,
        source: String,
        range: ZrangeBy,
        rev: bool,
        limit: Option<Limit>,
    ) -> Result<Integer, Error> {
        let range = Zrange { range, rev, limit };
        exchange(connection, ("ZRANGESTORE", destination, source, range)).await
    }

    #[doc = " Returns the index of a member in a sorted set ordered by ascending scores."]
    #[doc = ""]
    #[doc = " See also `zrank-withscore`."]
    #[doc = ""]
    #[doc = " ZRANK key member"]
    #[doc = " https://valkey.io/commands/zrank/"]
    #[allow(async_fn_in_trait)]
    async fn zrank(
        connection: &Connection,
        key: String,
        member: StringOrBytes,
    ) -> Result<Option<Integer>, Error> {
        exchange(connection, ("ZRANK", key, member)).await
    }

    #[doc = " Returns the index of a member in a sorted set ordered by ascending scores."]
    #[doc = ""]
    #[doc = " The `WITHSCORE` form, the position and the score."]
    #[doc = ""]
    #[doc = " See also `zrank`."]
    #[doc = ""]
    #[doc = " ZRANK key member WITHSCORE"]
    #[doc = " https://valkey.io/commands/zrank/"]
    #[allow(async_fn_in_trait)]
    async fn zrank_withscore(
        connection: &Connection,
        key: String,
        member: StringOrBytes,
    ) -> Result<Option<(Integer, Double)>, Error> {
        exchange(connection, ("ZRANK", key, member, "WITHSCORE")).await
    }

    #[doc = " Removes one or more members from a sorted set."]
    #[doc = ""]
    #[doc = " ZREM key member [ member ... ]"]
    #[doc = " https://valkey.io/commands/zrem/"]
    #[allow(async_fn_in_trait)]
    async fn zrem(
        connection: &Connection,
        key: String,
        members: Vec<StringOrBytes>,
    ) -> Result<Integer, Error> {
        exchange(connection, ("ZREM", key, NonEmpty::new("members", members))).await
    }

    #[doc = " Removes members in a sorted set within a lexicographical range."]
    #[doc = ""]
    #[doc = " ZREMRANGEBYLEX key min max"]
    #[doc = " https://valkey.io/commands/zremrangebylex/"]
    #[allow(async_fn_in_trait)]
    async fn zremrangebylex(
        connection: &Connection,
        key: String,
        min: LexBound,
        max: LexBound,
    ) -> Result<Integer, Error> {
        exchange(
            connection,
            (
                "ZREMRANGEBYLEX",
                key,
                LexRange {
                    start: min,
                    stop: max,
                    rev: false,
                },
            ),
        )
        .await
    }

    #[doc = " Removes members in a sorted set within a range of indexes."]
    #[doc = ""]
    #[doc = " ZREMRANGEBYRANK key start stop"]
    #[doc = " https://valkey.io/commands/zremrangebyrank/"]
    #[allow(async_fn_in_trait)]
    async fn zremrangebyrank(
        connection: &Connection,
        key: String,
        start: Integer,
        stop: Integer,
    ) -> Result<Integer, Error> {
        exchange(connection, ("ZREMRANGEBYRANK", key, start, stop)).await
    }

    #[doc = " Removes members in a sorted set within a range of scores."]
    #[doc = ""]
    #[doc = " ZREMRANGEBYSCORE key min max"]
    #[doc = " https://valkey.io/commands/zremrangebyscore/"]
    #[allow(async_fn_in_trait)]
    async fn zremrangebyscore(
        connection: &Connection,
        key: String,
        min: ScoreBound,
        max: ScoreBound,
    ) -> Result<Integer, Error> {
        exchange(
            connection,
            (
                "ZREMRANGEBYSCORE",
                key,
                ScoreRange {
                    start: min,
                    stop: max,
                    rev: false,
                },
            ),
        )
        .await
    }

    #[doc = " Returns the index of a member in a sorted set ordered by descending scores."]
    #[doc = ""]
    #[doc = " See also `zrevrank-withscore`."]
    #[doc = ""]
    #[doc = " ZREVRANK key member"]
    #[doc = " https://valkey.io/commands/zrevrank/"]
    #[allow(async_fn_in_trait)]
    async fn zrevrank(
        connection: &Connection,
        key: String,
        member: StringOrBytes,
    ) -> Result<Option<Integer>, Error> {
        exchange(connection, ("ZREVRANK", key, member)).await
    }

    #[doc = " Returns the index of a member in a sorted set ordered by descending scores."]
    #[doc = ""]
    #[doc = " The `WITHSCORE` form, the position and the score."]
    #[doc = ""]
    #[doc = " See also `zrevrank`."]
    #[doc = ""]
    #[doc = " ZREVRANK key member WITHSCORE"]
    #[doc = " https://valkey.io/commands/zrevrank/"]
    #[allow(async_fn_in_trait)]
    async fn zrevrank_withscore(
        connection: &Connection,
        key: String,
        member: StringOrBytes,
    ) -> Result<Option<(Integer, Double)>, Error> {
        exchange(connection, ("ZREVRANK", key, member, "WITHSCORE")).await
    }

    #[doc = " Iterates over members and scores of a sorted set."]
    #[doc = ""]
    #[doc = " See also `zscan-noscores`."]
    #[doc = ""]
    #[doc = " ZSCAN key cursor [ MATCH pattern ] [ COUNT count ]"]
    #[doc = " https://valkey.io/commands/zscan/"]
    #[allow(async_fn_in_trait)]
    async fn zscan(
        connection: &Connection,
        key: String,
        cursor: Cursor,
        match_: Option<String>,
        count: Option<Integer>,
    ) -> Result<(Option<Cursor>, Vec<(StringOrBytes, Double)>), Error> {
        // the members and scores are a flat array, the scores bulk strings
        let (cursor, members): (NextCursor, FlatPairs<StringOrBytes, Double>) = exchange(
            connection,
            (
                "ZSCAN",
                key,
                cursor,
                NamedOption("MATCH", match_),
                NamedOption("COUNT", count),
            ),
        )
        .await?;
        Ok((cursor.0, members.0))
    }

    #[doc = " Iterates over members and scores of a sorted set."]
    #[doc = ""]
    #[doc = " The `NOSCORES` form, the members only."]
    #[doc = ""]
    #[doc = " See also `zscan`."]
    #[doc = ""]
    #[doc = " ZSCAN key cursor [ MATCH pattern ] [ COUNT count ] NOSCORES"]
    #[doc = " https://valkey.io/commands/zscan/"]
    #[allow(async_fn_in_trait)]
    async fn zscan_noscores(
        connection: &Connection,
        key: String,
        cursor: Cursor,
        match_: Option<String>,
        count: Option<Integer>,
    ) -> Result<(Option<Cursor>, Vec<StringOrBytes>), Error> {
        let (cursor, items): (NextCursor, Vec<StringOrBytes>) = exchange(
            connection,
            (
                "ZSCAN",
                key,
                cursor,
                NamedOption("MATCH", match_),
                NamedOption("COUNT", count),
                "NOSCORES",
            ),
        )
        .await?;
        Ok((cursor.0, items))
    }

    #[doc = " Returns the score of a member in a sorted set."]
    #[doc = ""]
    #[doc = " ZSCORE key member"]
    #[doc = " https://valkey.io/commands/zscore/"]
    #[allow(async_fn_in_trait)]
    async fn zscore(
        connection: &Connection,
        key: String,
        member: StringOrBytes,
    ) -> Result<Option<Double>, Error> {
        exchange(connection, ("ZSCORE", key, member)).await
    }

    #[doc = " Returns the union of multiple sorted sets."]
    #[doc = ""]
    #[doc = " See also `zunion-withscores`."]
    #[doc = ""]
    #[doc = " ZUNION numkeys key [ key ... ] [ WEIGHTS weight [ weight ... ] ] [ AGGREGATE SUM | MIN | MAX ]"]
    #[doc = " https://valkey.io/commands/zunion/"]
    #[allow(async_fn_in_trait)]
    async fn zunion(
        connection: &Connection,
        keys: Vec<String>,
        weights: Option<Vec<Double>>,
        aggregate: Option<ZsetAggregate>,
    ) -> Result<Vec<StringOrBytes>, Error> {
        exchange(
            connection,
            (
                "ZUNION",
                NumKeys(NonEmpty::new("keys", keys)),
                NamedOption("WEIGHTS", NonEmpty::optional("weights", weights)),
                aggregate,
            ),
        )
        .await
    }

    #[doc = " Returns the union of multiple sorted sets."]
    #[doc = ""]
    #[doc = " The `WITHSCORES` form, the members and their combined scores."]
    #[doc = ""]
    #[doc = " See also `zunion`."]
    #[doc = ""]
    #[doc = " ZUNION numkeys key [ key ... ] [ WEIGHTS weight [ weight ... ] ] [ AGGREGATE SUM | MIN | MAX ] WITHSCORES"]
    #[doc = " https://valkey.io/commands/zunion/"]
    #[allow(async_fn_in_trait)]
    async fn zunion_withscores(
        connection: &Connection,
        keys: Vec<String>,
        weights: Option<Vec<Double>>,
        aggregate: Option<ZsetAggregate>,
    ) -> Result<Vec<(StringOrBytes, Double)>, Error> {
        let command = (
            "ZUNION",
            NumKeys(NonEmpty::new("keys", keys)),
            NamedOption("WEIGHTS", NonEmpty::optional("weights", weights)),
            aggregate,
            "WITHSCORES",
        );
        exchange(connection, command).await
    }

    #[doc = " Stores the union of multiple sorted sets in a key."]
    #[doc = ""]
    #[doc = " ZUNIONSTORE destination numkeys key [ key ... ] [ WEIGHTS weight [ weight ... ] ] [ AGGREGATE SUM | MIN | MAX ]"]
    #[doc = " https://valkey.io/commands/zunionstore/"]
    #[allow(async_fn_in_trait)]
    async fn zunionstore(
        connection: &Connection,
        destination: String,
        keys: Vec<String>,
        weights: Option<Vec<Double>>,
        aggregate: Option<ZsetAggregate>,
    ) -> Result<Integer, Error> {
        let command = (
            "ZUNIONSTORE",
            destination,
            NumKeys(NonEmpty::new("keys", keys)),
            NamedOption("WEIGHTS", NonEmpty::optional("weights", weights)),
            aggregate,
        );
        exchange(connection, command).await
    }
}

wit_bindgen::generate!({
    path: "../wit",
    world: "commands",
    // the resp types shared by every component
    with: {
        "componentized:valkey/types@0.3.0-dev": componentized_valkey::types,
    },
    generate_all,
});

export!(Commands);
