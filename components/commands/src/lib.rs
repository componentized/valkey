use crate::command::{
    Bit, DecimalSeconds, Fields, Flag, KeysAndArgs, LexRange, NamedOption, NonEmpty, NumKeys,
    OnOff, ScoreRange, Zrange, emit, query,
};
use crate::exports::componentized::valkey::commands::{
    AclDryrunResult, AclListEntry, AclLogEntry, Aggregate, BgsaveOperation, BitRange, BitfieldGet,
    BitfieldOperation, BitopOperation, ClientFilter, ClientPauseMode, ClientSetinfoAttr,
    ClientTrackingOptions, ClientTrackinginfoReply, ClientUnblockType, ClusterBumpepochResult,
    ClusterFailoverMode, ClusterResetMode, ClusterSetslotState, CommandDocsReply, CommandInfoReply,
    CommandListFilter, CommandlogType, Connection, Cursor, Double, DurationMilliseconds,
    DurationSeconds, Error, ExpiretimeReply, FailoverTarget, FieldPersistResult, FlushMode,
    FunctionStatsReply, GeoCoordinates, GeoCount, GeoSearchResult, GeoShape, GeoUnit,
    GeoaddCondition, GetexTtl, Guest, HsetexCondition, HsetexFieldCondition, Integer, KeyType,
    LatencyHistogramEntry, LatencyLatestEntry, LcsIdxReply, LexBound, Library, Limit,
    LinsertPosition, LinsertResult, ListSide, LogEntry, MigrateAuth, ModuleInfo,
    MsetexPrecondition, MsetexTtl, PexpiretimeReply, PopEnd, PttlReply, RoleReply, Rule,
    ScoreBound, SetPrecondition, SetTtl, ShutdownSave, SlotMigration, SlotMigrationGroup, SortOpts,
    SortOrder, StreamId, StreamTrim, TrackingRedirect, TtlMode, TtlReply, TtlResult,
    UnixTimeMilliseconds, UnixTimeSeconds, Value, XclaimOpts, XinfoConsumer, XinfoGroup,
    XinfoStreamReply, XinfoStreamReplyFull, XpendingExtendedEntry, XpendingSummary, ZaddComparison,
    ZaddCondition, ZrangeBy, ZrangeWithscoresBy,
};
use crate::value::{
    AclList, AclUserRules, ClientList, CommandDocsList, CommandInfoList, FlatPairs, Migrated,
    NextCursor, OptionalKeyType, OrNever, OrZeroId, ScoredMembers,
};

use wit_bindgen::rt::Map;

mod command;
mod value;

struct Commands;

/// The number of values SORT returns for each element, one for each GET pattern, or the element.
fn sort_row_len(opts: &Option<SortOpts>) -> usize {
    opts.as_ref()
        .and_then(|opts| opts.get.as_ref())
        .map_or(1, Vec::len)
}

impl Guest for Commands {
    #[doc = " admin commands"]
    #[doc = " Delete all the specified ACL users and terminate all the connections that are authenticated with such users."]
    #[doc = ""]
    #[doc = " ACL DELUSER username [ username ... ]"]
    #[doc = " https://valkey.io/commands/acl-deluser/"]
    #[allow(async_fn_in_trait)]
    async fn acl_deluser(
        connection: &Connection,
        usernames: Vec<String>,
    ) -> Result<Integer, Error> {
        query(
            connection,
            ("ACL", "DELUSER", NonEmpty::new("usernames", usernames)),
        )
        .await
    }

    #[doc = " Generates a random password."]
    #[doc = ""]
    #[doc = " ACL GENPASS [ bits ]"]
    #[doc = " https://valkey.io/commands/acl-genpass/"]
    #[allow(async_fn_in_trait)]
    async fn acl_genpass(connection: &Connection, bits: Option<Integer>) -> Result<String, Error> {
        query(connection, ("ACL", "GENPASS", bits)).await
    }

    #[doc = " Create an ACL user with the specified rules or modify the rules of an existing user."]
    #[doc = ""]
    #[doc = " ACL SETUSER username [ rule ] [ [ rule ] ... ]"]
    #[doc = " https://valkey.io/commands/acl-setuser/"]
    #[allow(async_fn_in_trait)]
    async fn acl_setuser(
        connection: &Connection,
        username: String,
        rules: Option<Vec<Rule>>,
    ) -> Result<(), Error> {
        query(
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

    #[doc = " The ACL categories, or with a category, the commands in it."]
    #[doc = ""]
    #[doc = " ACL CAT [ category ]"]
    #[doc = " https://valkey.io/commands/acl-cat/"]
    #[allow(async_fn_in_trait)]
    async fn acl_cat(
        connection: &Connection,
        category: Option<String>,
    ) -> Result<Vec<String>, Error> {
        query(connection, ("ACL", "CAT", category)).await
    }

    #[doc = " Whether the user may run the command, without running it."]
    #[doc = ""]
    #[doc = " ACL DRYRUN username command [ arg [ arg ... ] ]"]
    #[doc = " https://valkey.io/commands/acl-dryrun/"]
    #[allow(async_fn_in_trait)]
    async fn acl_dryrun(
        connection: &Connection,
        username: String,
        command: Vec<String>,
    ) -> Result<AclDryrunResult, Error> {
        query(
            connection,
            ("ACL", "DRYRUN", username, NonEmpty::new("command", command)),
        )
        .await
    }

    #[doc = " The rules of the user, none when the user does not exist."]
    #[doc = ""]
    #[doc = " ACL GETUSER username"]
    #[doc = " https://valkey.io/commands/acl-getuser/"]
    #[allow(async_fn_in_trait)]
    async fn acl_getuser(
        connection: &Connection,
        username: String,
    ) -> Result<Option<Vec<Rule>>, Error> {
        let rules: Option<AclUserRules> = query(connection, ("ACL", "GETUSER", username)).await?;
        Ok(rules.map(|rules| rules.0))
    }

    #[doc = " The rules of every user, in the format of an ACL file."]
    #[doc = ""]
    #[doc = " ACL LIST"]
    #[doc = " https://valkey.io/commands/acl-list/"]
    #[allow(async_fn_in_trait)]
    async fn acl_list(connection: &Connection) -> Result<Vec<AclListEntry>, Error> {
        let users: AclList = query(connection, ("ACL", "LIST")).await?;
        Ok(users.0)
    }

    #[doc = " Replaces the users with those of the configured ACL file."]
    #[doc = ""]
    #[doc = " ACL LOAD"]
    #[doc = " https://valkey.io/commands/acl-load/"]
    #[allow(async_fn_in_trait)]
    async fn acl_load(connection: &Connection) -> Result<(), Error> {
        query(connection, ("ACL", "LOAD")).await
    }

    #[doc = " The recent security events, commands or authentications the rules denied, up to the count,"]
    #[doc = " 10 without one."]
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
        query(connection, ("ACL", "LOG", count)).await
    }

    #[doc = " Clears the security events."]
    #[doc = ""]
    #[doc = " See also `acl-log`."]
    #[doc = ""]
    #[doc = " ACL LOG RESET"]
    #[doc = " https://valkey.io/commands/acl-log/"]
    #[allow(async_fn_in_trait)]
    async fn acl_log_reset(connection: &Connection) -> Result<(), Error> {
        query(connection, ("ACL", "LOG", "RESET")).await
    }

    #[doc = " Saves the users to the configured ACL file."]
    #[doc = ""]
    #[doc = " ACL SAVE"]
    #[doc = " https://valkey.io/commands/acl-save/"]
    #[allow(async_fn_in_trait)]
    async fn acl_save(connection: &Connection) -> Result<(), Error> {
        query(connection, ("ACL", "SAVE")).await
    }

    #[doc = " The names of the users."]
    #[doc = ""]
    #[doc = " ACL USERS"]
    #[doc = " https://valkey.io/commands/acl-users/"]
    #[allow(async_fn_in_trait)]
    async fn acl_users(connection: &Connection) -> Result<Vec<String>, Error> {
        query(connection, ("ACL", "USERS")).await
    }

    #[doc = " The name of the user the connection is authenticated as."]
    #[doc = ""]
    #[doc = " ACL WHOAMI"]
    #[doc = " https://valkey.io/commands/acl-whoami/"]
    #[allow(async_fn_in_trait)]
    async fn acl_whoami(connection: &Connection) -> Result<String, Error> {
        query(connection, ("ACL", "WHOAMI")).await
    }

    #[doc = " Starts rewriting the append-only file in the background, returning the server's message."]
    #[doc = ""]
    #[doc = " BGREWRITEAOF"]
    #[doc = " https://valkey.io/commands/bgrewriteaof/"]
    #[allow(async_fn_in_trait)]
    async fn bgrewriteaof(connection: &Connection) -> Result<String, Error> {
        query(connection, "BGREWRITEAOF").await
    }

    #[doc = " Starts saving the databases to disk in the background, or schedules or cancels it, returning"]
    #[doc = " the server's message."]
    #[doc = ""]
    #[doc = " BGSAVE [ SCHEDULE | CANCEL ]"]
    #[doc = " https://valkey.io/commands/bgsave/"]
    #[allow(async_fn_in_trait)]
    async fn bgsave(
        connection: &Connection,
        operation: Option<BgsaveOperation>,
    ) -> Result<String, Error> {
        query(connection, ("BGSAVE", operation)).await
    }

    #[doc = " Closes the connections that match every one of the filters, returning how many."]
    #[doc = ""]
    #[doc = " CLIENT KILL filter [ filter ... ]"]
    #[doc = " https://valkey.io/commands/client-kill/"]
    #[allow(async_fn_in_trait)]
    async fn client_kill(
        connection: &Connection,
        filters: Vec<ClientFilter>,
    ) -> Result<Integer, Error> {
        query(
            connection,
            ("CLIENT", "KILL", NonEmpty::new("filters", filters)),
        )
        .await
    }

    #[doc = " The connections that match every one of the filters, all of them without filters, each a"]
    #[doc = " map of its fields, e.g. `id`, `addr` and `name`."]
    #[doc = ""]
    #[doc = " CLIENT LIST [ filter [ filter ... ] ]"]
    #[doc = " https://valkey.io/commands/client-list/"]
    #[allow(async_fn_in_trait)]
    async fn client_list(
        connection: &Connection,
        filters: Option<Vec<ClientFilter>>,
    ) -> Result<Vec<Map<String, String>>, Error> {
        let command = ("CLIENT", "LIST", NonEmpty::optional("filters", filters));
        let clients: ClientList = query(connection, command).await?;
        Ok(clients.0)
    }

    #[doc = " Whether the connection may be evicted to free memory, `ON` excludes it."]
    #[doc = ""]
    #[doc = " CLIENT NO-EVICT 〈 ON | OFF 〉"]
    #[doc = " https://valkey.io/commands/client-no-evict/"]
    #[allow(async_fn_in_trait)]
    async fn client_no_evict(connection: &Connection, enabled: bool) -> Result<(), Error> {
        query(
            connection,
            ("CLIENT", "NO-EVICT", if enabled { "ON" } else { "OFF" }),
        )
        .await
    }

    #[doc = " Suspends running the commands of other connections, writes only with `WRITE`, for the time."]
    #[doc = ""]
    #[doc = " CLIENT PAUSE timeout [ WRITE | ALL ]"]
    #[doc = " https://valkey.io/commands/client-pause/"]
    #[allow(async_fn_in_trait)]
    async fn client_pause(
        connection: &Connection,
        timeout: DurationMilliseconds,
        mode: Option<ClientPauseMode>,
    ) -> Result<(), Error> {
        query(connection, ("CLIENT", "PAUSE", timeout, mode)).await
    }

    #[doc = " Unblocks the connection with the ID from a blocking command, as if it timed out, or with an"]
    #[doc = " error, returning whether it was blocked."]
    #[doc = ""]
    #[doc = " CLIENT UNBLOCK client-id [ TIMEOUT | ERROR ]"]
    #[doc = " https://valkey.io/commands/client-unblock/"]
    #[allow(async_fn_in_trait)]
    async fn client_unblock(
        connection: &Connection,
        client_id: Integer,
        unblock_type: Option<ClientUnblockType>,
    ) -> Result<bool, Error> {
        query(connection, ("CLIENT", "UNBLOCK", client_id, unblock_type)).await
    }

    #[doc = " Resumes running the commands of connections paused with `client-pause`."]
    #[doc = ""]
    #[doc = " CLIENT UNPAUSE"]
    #[doc = " https://valkey.io/commands/client-unpause/"]
    #[allow(async_fn_in_trait)]
    async fn client_unpause(connection: &Connection) -> Result<(), Error> {
        query(connection, ("CLIENT", "UNPAUSE")).await
    }

    #[doc = " Assigns the slots to the node."]
    #[doc = ""]
    #[doc = " CLUSTER ADDSLOTS slot [ slot ... ]"]
    #[doc = " https://valkey.io/commands/cluster-addslots/"]
    #[allow(async_fn_in_trait)]
    async fn cluster_addslots(connection: &Connection, slots: Vec<u16>) -> Result<(), Error> {
        query(
            connection,
            ("CLUSTER", "ADDSLOTS", NonEmpty::new("slots", slots)),
        )
        .await
    }

    #[doc = " Assigns the ranges of slots, start and end both included, to the node."]
    #[doc = ""]
    #[doc = " CLUSTER ADDSLOTSRANGE start-slot end-slot [ start-slot end-slot ... ]"]
    #[doc = " https://valkey.io/commands/cluster-addslotsrange/"]
    #[allow(async_fn_in_trait)]
    async fn cluster_addslotsrange(
        connection: &Connection,
        ranges: Vec<(u16, u16)>,
    ) -> Result<(), Error> {
        query(
            connection,
            ("CLUSTER", "ADDSLOTSRANGE", NonEmpty::new("ranges", ranges)),
        )
        .await
    }

    #[doc = " Advances the node's config epoch, unless it is already the greatest in the cluster."]
    #[doc = ""]
    #[doc = " CLUSTER BUMPEPOCH"]
    #[doc = " https://valkey.io/commands/cluster-bumpepoch/"]
    #[allow(async_fn_in_trait)]
    async fn cluster_bumpepoch(connection: &Connection) -> Result<ClusterBumpepochResult, Error> {
        query(connection, ("CLUSTER", "BUMPEPOCH")).await
    }

    #[doc = " Cancels the slot migrations in progress."]
    #[doc = ""]
    #[doc = " CLUSTER CANCELSLOTMIGRATIONS"]
    #[doc = " https://valkey.io/commands/cluster-cancelslotmigrations/"]
    #[allow(async_fn_in_trait)]
    async fn cluster_cancelslotmigrations(connection: &Connection) -> Result<(), Error> {
        query(connection, ("CLUSTER", "CANCELSLOTMIGRATIONS")).await
    }

    #[doc = " The number of active failure reports for the node."]
    #[doc = ""]
    #[doc = " CLUSTER COUNT-FAILURE-REPORTS node-id"]
    #[doc = " https://valkey.io/commands/cluster-count-failure-reports/"]
    #[allow(async_fn_in_trait)]
    async fn cluster_count_failure_reports(
        connection: &Connection,
        node_id: String,
    ) -> Result<Integer, Error> {
        query(connection, ("CLUSTER", "COUNT-FAILURE-REPORTS", node_id)).await
    }

    #[doc = " Unassigns the slots from the node."]
    #[doc = ""]
    #[doc = " CLUSTER DELSLOTS slot [ slot ... ]"]
    #[doc = " https://valkey.io/commands/cluster-delslots/"]
    #[allow(async_fn_in_trait)]
    async fn cluster_delslots(connection: &Connection, slots: Vec<u16>) -> Result<(), Error> {
        query(
            connection,
            ("CLUSTER", "DELSLOTS", NonEmpty::new("slots", slots)),
        )
        .await
    }

    #[doc = " Unassigns the ranges of slots, start and end both included, from the node."]
    #[doc = ""]
    #[doc = " CLUSTER DELSLOTSRANGE start-slot end-slot [ start-slot end-slot ... ]"]
    #[doc = " https://valkey.io/commands/cluster-delslotsrange/"]
    #[allow(async_fn_in_trait)]
    async fn cluster_delslotsrange(
        connection: &Connection,
        ranges: Vec<(u16, u16)>,
    ) -> Result<(), Error> {
        query(
            connection,
            ("CLUSTER", "DELSLOTSRANGE", NonEmpty::new("ranges", ranges)),
        )
        .await
    }

    #[doc = " Fails over the primary of this replica, coordinated with it unless `FORCE` or `TAKEOVER`."]
    #[doc = ""]
    #[doc = " CLUSTER FAILOVER [ FORCE | TAKEOVER ]"]
    #[doc = " https://valkey.io/commands/cluster-failover/"]
    #[allow(async_fn_in_trait)]
    async fn cluster_failover(
        connection: &Connection,
        mode: Option<ClusterFailoverMode>,
    ) -> Result<(), Error> {
        query(connection, ("CLUSTER", "FAILOVER", mode)).await
    }

    #[doc = " Unassigns all the slots of the node."]
    #[doc = ""]
    #[doc = " CLUSTER FLUSHSLOTS"]
    #[doc = " https://valkey.io/commands/cluster-flushslots/"]
    #[allow(async_fn_in_trait)]
    async fn cluster_flushslots(connection: &Connection) -> Result<(), Error> {
        query(connection, ("CLUSTER", "FLUSHSLOTS")).await
    }

    #[doc = " Removes the node from the nodes this node knows of."]
    #[doc = ""]
    #[doc = " CLUSTER FORGET node-id"]
    #[doc = " https://valkey.io/commands/cluster-forget/"]
    #[allow(async_fn_in_trait)]
    async fn cluster_forget(connection: &Connection, node_id: String) -> Result<(), Error> {
        query(connection, ("CLUSTER", "FORGET", node_id)).await
    }

    #[doc = " The slot migrations in progress, and those recently finished."]
    #[doc = ""]
    #[doc = " CLUSTER GETSLOTMIGRATIONS"]
    #[doc = " https://valkey.io/commands/cluster-getslotmigrations/"]
    #[allow(async_fn_in_trait)]
    async fn cluster_getslotmigrations(
        connection: &Connection,
    ) -> Result<Vec<SlotMigration>, Error> {
        query(connection, ("CLUSTER", "GETSLOTMIGRATIONS")).await
    }

    #[doc = " Connects the node to another, adding it to the cluster."]
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
        query(connection, ("CLUSTER", "MEET", ip, port, cluster_bus_port)).await
    }

    #[doc = " Migrates the ranges of slots from this node to other nodes."]
    #[doc = ""]
    #[doc = " CLUSTER MIGRATESLOTS SLOTSRANGE start-slot end-slot [ start-slot end-slot ... ] NODE node-id [ AUTH username password ] [ SLOTSRANGE ... ]"]
    #[doc = " https://valkey.io/commands/cluster-migrateslots/"]
    #[allow(async_fn_in_trait)]
    async fn cluster_migrateslots(
        connection: &Connection,
        migrations: Vec<SlotMigrationGroup>,
    ) -> Result<(), Error> {
        query(
            connection,
            (
                "CLUSTER",
                "MIGRATESLOTS",
                NonEmpty::new("migrations", migrations),
            ),
        )
        .await
    }

    #[doc = " The replicas of the primary node, each a line in the format of CLUSTER NODES."]
    #[doc = ""]
    #[doc = " CLUSTER REPLICAS node-id"]
    #[doc = " https://valkey.io/commands/cluster-replicas/"]
    #[allow(async_fn_in_trait)]
    async fn cluster_replicas(
        connection: &Connection,
        node_id: String,
    ) -> Result<Vec<String>, Error> {
        query(connection, ("CLUSTER", "REPLICAS", node_id)).await
    }

    #[doc = " Makes the node a replica of the primary node, or with none, detaches it from its primary,"]
    #[doc = " `NO ONE`."]
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
        query(connection, ("CLUSTER", "REPLICATE", node)).await
    }

    #[doc = " Resets the node, forgetting the other nodes and its slots, `SOFT` without a mode."]
    #[doc = ""]
    #[doc = " CLUSTER RESET [ HARD | SOFT ]"]
    #[doc = " https://valkey.io/commands/cluster-reset/"]
    #[allow(async_fn_in_trait)]
    async fn cluster_reset(
        connection: &Connection,
        mode: Option<ClusterResetMode>,
    ) -> Result<(), Error> {
        query(connection, ("CLUSTER", "RESET", mode)).await
    }

    #[doc = " Saves the node's cluster configuration to disk."]
    #[doc = ""]
    #[doc = " CLUSTER SAVECONFIG"]
    #[doc = " https://valkey.io/commands/cluster-saveconfig/"]
    #[allow(async_fn_in_trait)]
    async fn cluster_saveconfig(connection: &Connection) -> Result<(), Error> {
        query(connection, ("CLUSTER", "SAVECONFIG")).await
    }

    #[doc = " Sets the config epoch of a new node."]
    #[doc = ""]
    #[doc = " CLUSTER SET-CONFIG-EPOCH config-epoch"]
    #[doc = " https://valkey.io/commands/cluster-set-config-epoch/"]
    #[allow(async_fn_in_trait)]
    async fn cluster_set_config_epoch(
        connection: &Connection,
        config_epoch: Integer,
    ) -> Result<(), Error> {
        query(connection, ("CLUSTER", "SET-CONFIG-EPOCH", config_epoch)).await
    }

    #[doc = " Changes the state of the slot on the node."]
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
        query(
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

    #[doc = " The entries of the command log of the type, up to the count, all of them for -1."]
    #[doc = ""]
    #[doc = " COMMANDLOG GET count 〈 slow | large-request | large-reply 〉"]
    #[doc = " https://valkey.io/commands/commandlog-get/"]
    #[allow(async_fn_in_trait)]
    async fn commandlog_get(
        connection: &Connection,
        count: Integer,
        type_: CommandlogType,
    ) -> Result<Vec<LogEntry>, Error> {
        query(connection, ("COMMANDLOG", "GET", count, type_)).await
    }

    #[doc = " The number of entries in the command log of the type."]
    #[doc = ""]
    #[doc = " COMMANDLOG LEN 〈 slow | large-request | large-reply 〉"]
    #[doc = " https://valkey.io/commands/commandlog-len/"]
    #[allow(async_fn_in_trait)]
    async fn commandlog_len(
        connection: &Connection,
        type_: CommandlogType,
    ) -> Result<Integer, Error> {
        query(connection, ("COMMANDLOG", "LEN", type_)).await
    }

    #[doc = " Clears the command log of the type."]
    #[doc = ""]
    #[doc = " COMMANDLOG RESET 〈 slow | large-request | large-reply 〉"]
    #[doc = " https://valkey.io/commands/commandlog-reset/"]
    #[allow(async_fn_in_trait)]
    async fn commandlog_reset(connection: &Connection, type_: CommandlogType) -> Result<(), Error> {
        query(connection, ("COMMANDLOG", "RESET", type_)).await
    }

    #[doc = " The values of the configuration parameters matching the glob-style patterns, by name."]
    #[doc = ""]
    #[doc = " CONFIG GET parameter [ parameter ... ]"]
    #[doc = " https://valkey.io/commands/config-get/"]
    #[allow(async_fn_in_trait)]
    async fn config_get(
        connection: &Connection,
        parameters: Vec<String>,
    ) -> Result<Map<String, String>, Error> {
        query(
            connection,
            ("CONFIG", "GET", NonEmpty::new("parameters", parameters)),
        )
        .await
    }

    #[doc = " Resets the server's statistics, e.g. those of INFO."]
    #[doc = ""]
    #[doc = " CONFIG RESETSTAT"]
    #[doc = " https://valkey.io/commands/config-resetstat/"]
    #[allow(async_fn_in_trait)]
    async fn config_resetstat(connection: &Connection) -> Result<(), Error> {
        query(connection, ("CONFIG", "RESETSTAT")).await
    }

    #[doc = " Rewrites the configuration file with the current configuration."]
    #[doc = ""]
    #[doc = " CONFIG REWRITE"]
    #[doc = " https://valkey.io/commands/config-rewrite/"]
    #[allow(async_fn_in_trait)]
    async fn config_rewrite(connection: &Connection) -> Result<(), Error> {
        query(connection, ("CONFIG", "REWRITE")).await
    }

    #[doc = " Sets the configuration parameters to the values, all or none of them."]
    #[doc = ""]
    #[doc = " CONFIG SET parameter value [ parameter value ... ]"]
    #[doc = " https://valkey.io/commands/config-set/"]
    #[allow(async_fn_in_trait)]
    async fn config_set(
        connection: &Connection,
        parameters_and_values: Vec<(String, String)>,
    ) -> Result<(), Error> {
        let parameters_and_values = NonEmpty::new("parameters-and-values", parameters_and_values);
        query(connection, ("CONFIG", "SET", parameters_and_values)).await
    }

    #[doc = " Fails over from this primary to a replica, the one at the target, or aborts a failover in"]
    #[doc = " progress."]
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
        query(connection, command).await
    }

    #[doc = " When the databases were last saved to disk."]
    #[doc = ""]
    #[doc = " LASTSAVE"]
    #[doc = " https://valkey.io/commands/lastsave/"]
    #[allow(async_fn_in_trait)]
    async fn lastsave(connection: &Connection) -> Result<UnixTimeSeconds, Error> {
        query(connection, "LASTSAVE").await
    }

    #[doc = " A report of the latency events, for people to read."]
    #[doc = ""]
    #[doc = " LATENCY DOCTOR"]
    #[doc = " https://valkey.io/commands/latency-doctor/"]
    #[allow(async_fn_in_trait)]
    async fn latency_doctor(connection: &Connection) -> Result<String, Error> {
        query(connection, ("LATENCY", "DOCTOR")).await
    }

    #[doc = " A graph of the latency of the event, as text."]
    #[doc = ""]
    #[doc = " LATENCY GRAPH event"]
    #[doc = " https://valkey.io/commands/latency-graph/"]
    #[allow(async_fn_in_trait)]
    async fn latency_graph(connection: &Connection, event: String) -> Result<String, Error> {
        query(connection, ("LATENCY", "GRAPH", event)).await
    }

    #[doc = " The latency histogram of the commands, all of them without commands, by command name."]
    #[doc = ""]
    #[doc = " LATENCY HISTOGRAM [ command [ command ... ] ]"]
    #[doc = " https://valkey.io/commands/latency-histogram/"]
    #[allow(async_fn_in_trait)]
    async fn latency_histogram(
        connection: &Connection,
        commands: Option<Vec<String>>,
    ) -> Result<Map<String, LatencyHistogramEntry>, Error> {
        query(
            connection,
            (
                "LATENCY",
                "HISTOGRAM",
                NonEmpty::optional("commands", commands),
            ),
        )
        .await
    }

    #[doc = " The samples of the latency of the event, when each was taken and the latency."]
    #[doc = ""]
    #[doc = " LATENCY HISTORY event"]
    #[doc = " https://valkey.io/commands/latency-history/"]
    #[allow(async_fn_in_trait)]
    async fn latency_history(
        connection: &Connection,
        event: String,
    ) -> Result<Vec<(UnixTimeSeconds, DurationMilliseconds)>, Error> {
        query(connection, ("LATENCY", "HISTORY", event)).await
    }

    #[doc = " The latest sample of the latency of each event."]
    #[doc = ""]
    #[doc = " LATENCY LATEST"]
    #[doc = " https://valkey.io/commands/latency-latest/"]
    #[allow(async_fn_in_trait)]
    async fn latency_latest(connection: &Connection) -> Result<Vec<LatencyLatestEntry>, Error> {
        query(connection, ("LATENCY", "LATEST")).await
    }

    #[doc = " Clears the samples of the events, all of them without events, returning how many events."]
    #[doc = ""]
    #[doc = " LATENCY RESET [ event [ event ... ] ]"]
    #[doc = " https://valkey.io/commands/latency-reset/"]
    #[allow(async_fn_in_trait)]
    async fn latency_reset(
        connection: &Connection,
        events: Option<Vec<String>>,
    ) -> Result<Integer, Error> {
        query(
            connection,
            ("LATENCY", "RESET", NonEmpty::optional("events", events)),
        )
        .await
    }

    #[doc = " The loaded modules."]
    #[doc = ""]
    #[doc = " MODULE LIST"]
    #[doc = " https://valkey.io/commands/module-list/"]
    #[allow(async_fn_in_trait)]
    async fn module_list(connection: &Connection) -> Result<Vec<ModuleInfo>, Error> {
        query(connection, ("MODULE", "LIST")).await
    }

    #[doc = " Loads the module at the path, with the arguments."]
    #[doc = ""]
    #[doc = " MODULE LOAD path [ arg [ arg ... ] ]"]
    #[doc = " https://valkey.io/commands/module-load/"]
    #[allow(async_fn_in_trait)]
    async fn module_load(
        connection: &Connection,
        path: String,
        args: Option<Vec<String>>,
    ) -> Result<(), Error> {
        query(
            connection,
            ("MODULE", "LOAD", path, NonEmpty::optional("args", args)),
        )
        .await
    }

    #[doc = " Loads the module at the path, with the configuration parameters and arguments."]
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
        query(connection, ("MODULE", "LOADEX", path, configs, args)).await
    }

    #[doc = " Unloads the module, by name."]
    #[doc = ""]
    #[doc = " MODULE UNLOAD name"]
    #[doc = " https://valkey.io/commands/module-unload/"]
    #[allow(async_fn_in_trait)]
    async fn module_unload(connection: &Connection, name: String) -> Result<(), Error> {
        query(connection, ("MODULE", "UNLOAD", name)).await
    }

    #[doc = " Makes the server a replica of the primary at the host and port, or with none, a primary,"]
    #[doc = " `NO ONE`, returning the server's message."]
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
        query(connection, ("REPLICAOF", primary)).await
    }

    #[doc = " The server's role in replication, and its state."]
    #[doc = ""]
    #[doc = " ROLE"]
    #[doc = " https://valkey.io/commands/role/"]
    #[allow(async_fn_in_trait)]
    async fn role(connection: &Connection) -> Result<RoleReply, Error> {
        query(connection, "ROLE").await
    }

    #[doc = " Saves the databases to disk, blocking the server until done."]
    #[doc = ""]
    #[doc = " SAVE"]
    #[doc = " https://valkey.io/commands/save/"]
    #[allow(async_fn_in_trait)]
    async fn save(connection: &Connection) -> Result<(), Error> {
        query(connection, "SAVE").await
    }

    #[doc = " Stops the server, saving to disk as configured, or as the save mode says."]
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
        now: bool,
        force: bool,
        safe: bool,
        failover: bool,
    ) -> Result<(), Error> {
        let command = (
            "SHUTDOWN",
            save,
            Flag("NOW", now),
            Flag("FORCE", force),
            Flag("SAFE", safe),
            Flag("FAILOVER", failover),
        );
        query(connection, command).await
    }

    #[doc = " Aborts a shutdown in progress."]
    #[doc = ""]
    #[doc = " See also `shutdown`."]
    #[doc = ""]
    #[doc = " SHUTDOWN ABORT"]
    #[doc = " https://valkey.io/commands/shutdown/"]
    #[allow(async_fn_in_trait)]
    async fn shutdown_abort(connection: &Connection) -> Result<(), Error> {
        query(connection, ("SHUTDOWN", "ABORT")).await
    }

    #[doc = " The entries of the slow log, up to the count, 10 without one, all of them for -1."]
    #[doc = ""]
    #[doc = " SLOWLOG GET [ count ]"]
    #[doc = " https://valkey.io/commands/slowlog-get/"]
    #[allow(async_fn_in_trait)]
    async fn slowlog_get(
        connection: &Connection,
        count: Option<Integer>,
    ) -> Result<Vec<LogEntry>, Error> {
        query(connection, ("SLOWLOG", "GET", count)).await
    }

    #[doc = " The number of entries in the slow log."]
    #[doc = ""]
    #[doc = " SLOWLOG LEN"]
    #[doc = " https://valkey.io/commands/slowlog-len/"]
    #[allow(async_fn_in_trait)]
    async fn slowlog_len(connection: &Connection) -> Result<Integer, Error> {
        query(connection, ("SLOWLOG", "LEN")).await
    }

    #[doc = " Clears the slow log."]
    #[doc = ""]
    #[doc = " SLOWLOG RESET"]
    #[doc = " https://valkey.io/commands/slowlog-reset/"]
    #[allow(async_fn_in_trait)]
    async fn slowlog_reset(connection: &Connection) -> Result<(), Error> {
        query(connection, ("SLOWLOG", "RESET")).await
    }

    #[doc = " The number of bits set to 1 in the string stored at key, in the range, 0 when the key does"]
    #[doc = " not exist."]
    #[doc = ""]
    #[doc = " BITCOUNT key [ start [ end [ BYTE | BIT ] ] ]"]
    #[doc = " https://valkey.io/commands/bitcount/"]
    #[allow(async_fn_in_trait)]
    async fn bitcount(
        connection: &Connection,
        key: String,
        range: Option<BitRange>,
    ) -> Result<Integer, Error> {
        query(connection, ("BITCOUNT", key, range)).await
    }

    #[doc = " Reads and writes integer fields of the string stored at key, returning the result of each"]
    #[doc = " operation in order, none for a write that failed with `OVERFLOW FAIL`."]
    #[doc = ""]
    #[doc = " See also `bitfield-ro`."]
    #[doc = ""]
    #[doc = " BITFIELD key [ GET encoding offset | [ OVERFLOW WRAP | SAT | FAIL ] 〈 SET encoding offset value | INCRBY encoding offset increment 〉 [ GET encoding offset | [ OVERFLOW WRAP | SAT | FAIL ] 〈 SET encoding offset value | INCRBY encoding offset increment 〉 ... ] ]"]
    #[doc = " https://valkey.io/commands/bitfield/"]
    #[allow(async_fn_in_trait)]
    async fn bitfield(
        connection: &Connection,
        key: String,
        operations: Option<Vec<BitfieldOperation>>,
    ) -> Result<Vec<Option<Integer>>, Error> {
        query(
            connection,
            (
                "BITFIELD",
                key,
                NonEmpty::optional("operations", operations),
            ),
        )
        .await
    }

    #[doc = " Reads integer fields of the string stored at key, the read-only variant of `bitfield`."]
    #[doc = ""]
    #[doc = " BITFIELD_RO key [ GET encoding offset [ GET encoding offset ... ] ]"]
    #[doc = " https://valkey.io/commands/bitfield_ro/"]
    #[allow(async_fn_in_trait)]
    async fn bitfield_ro(
        connection: &Connection,
        key: String,
        gets: Option<Vec<BitfieldGet>>,
    ) -> Result<Vec<Integer>, Error> {
        query(
            connection,
            ("BITFIELD_RO", key, NonEmpty::optional("gets", gets)),
        )
        .await
    }

    #[doc = " Stores the bitwise operation of the strings stored at the source keys in the destination,"]
    #[doc = " returning its length, the length of the longest source."]
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
        query(
            connection,
            ("BITOP", operation, destination, NonEmpty::new("keys", keys)),
        )
        .await
    }

    #[doc = " The position of the first bit set to the bit in the string stored at key, in the range,"]
    #[doc = " none when there is none."]
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
            query(connection, ("BITPOS", key, Bit(bit), range)).await?;
        Ok(position.0)
    }

    #[doc = " The bit at the offset of the string stored at key, false beyond its end, or when the key"]
    #[doc = " does not exist."]
    #[doc = ""]
    #[doc = " GETBIT key offset"]
    #[doc = " https://valkey.io/commands/getbit/"]
    #[allow(async_fn_in_trait)]
    async fn getbit(connection: &Connection, key: String, offset: u64) -> Result<bool, Error> {
        query(connection, ("GETBIT", key, offset)).await
    }

    #[doc = " Sets the bit at the offset of the string stored at key, growing it as needed, returning the"]
    #[doc = " previous bit."]
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
        query(connection, ("SETBIT", key, offset, Bit(value))).await
    }

    #[doc = " Authenticates the current connection."]
    #[doc = ""]
    #[doc = " AUTH [ username ] password"]
    #[doc = " https://valkey.io/commands/auth/"]
    #[allow(async_fn_in_trait)]
    async fn auth(
        connection: &Connection,
        username: Option<String>,
        password: String,
    ) -> Result<(), Error> {
        query(connection, ("AUTH", username, password)).await
    }

    #[doc = " Switch to a different protocol, optionally authenticating and setting the connection\'s name, or provide a contextual client report."]
    #[doc = ""]
    #[doc = " RESP2 is not fully supported by this client."]
    #[doc = ""]
    #[doc = " HELLO 3 [ AUTH username password ] [ SETNAME clientname ]"]
    #[doc = " https://valkey.io/commands/hello/"]
    #[allow(async_fn_in_trait)]
    async fn hello(
        connection: &Connection,
        auth: Option<(String, String)>,
        client_name: Option<String>,
    ) -> Result<Map<String, Value>, Error> {
        query(
            connection,
            (
                "HELLO",
                "3",
                auth.map(|auth| ("AUTH", auth)),
                client_name.map(|name| ("SETNAME", name)),
            ),
        )
        .await
    }

    #[doc = " Returns PONG if no argument is provided, otherwise return a copy of the argument as a bulk."]
    #[doc = ""]
    #[doc = " PING [ message ]"]
    #[doc = " https://valkey.io/commands/ping/"]
    #[allow(async_fn_in_trait)]
    async fn ping(connection: &Connection, message: Option<String>) -> Result<(), Error> {
        let reply: String = query(connection, ("PING", message.clone())).await?;
        if reply == message.unwrap_or("PONG".to_string()) {
            Ok(())
        } else {
            Err(Error::Client(format!("unexpected reply: {reply:?}")))
        }
    }

    #[doc = " Signals that the next command follows an ASK redirect, for a slot being imported."]
    #[doc = ""]
    #[doc = " It applies to the next command on the connection, which may be another caller's when the"]
    #[doc = " connection is shared. Use a connection of your own."]
    #[doc = ""]
    #[doc = " ASKING"]
    #[doc = " https://valkey.io/commands/asking/"]
    #[allow(async_fn_in_trait)]
    async fn asking(connection: &Connection) -> Result<(), Error> {
        query(connection, "ASKING").await
    }

    #[doc = " Whether the keys the next command reads are tracked, with tracking in `optin` or `optout`"]
    #[doc = " mode."]
    #[doc = ""]
    #[doc = " It applies to the next command on the connection, which may be another caller's when the"]
    #[doc = " connection is shared. Use a connection of your own."]
    #[doc = ""]
    #[doc = " CLIENT CACHING 〈 YES | NO 〉"]
    #[doc = " https://valkey.io/commands/client-caching/"]
    #[allow(async_fn_in_trait)]
    async fn client_caching(connection: &Connection, enabled: bool) -> Result<(), Error> {
        query(
            connection,
            ("CLIENT", "CACHING", if enabled { "YES" } else { "NO" }),
        )
        .await
    }

    #[doc = " Declares the capabilities the client supports, e.g. `redirect`."]
    #[doc = ""]
    #[doc = " CLIENT CAPA capability [ capability ... ]"]
    #[doc = " https://valkey.io/commands/client-capa/"]
    #[allow(async_fn_in_trait)]
    async fn client_capa(connection: &Connection, capabilities: Vec<String>) -> Result<(), Error> {
        query(
            connection,
            (
                "CLIENT",
                "CAPA",
                NonEmpty::new("capabilities", capabilities),
            ),
        )
        .await
    }

    #[doc = " The name of the connection, none when it has none."]
    #[doc = ""]
    #[doc = " CLIENT GETNAME"]
    #[doc = " https://valkey.io/commands/client-getname/"]
    #[allow(async_fn_in_trait)]
    async fn client_getname(connection: &Connection) -> Result<Option<String>, Error> {
        query(connection, ("CLIENT", "GETNAME")).await
    }

    #[doc = " Where the connection's tracking notifications are redirected."]
    #[doc = ""]
    #[doc = " CLIENT GETREDIR"]
    #[doc = " https://valkey.io/commands/client-getredir/"]
    #[allow(async_fn_in_trait)]
    async fn client_getredir(connection: &Connection) -> Result<TrackingRedirect, Error> {
        query(connection, ("CLIENT", "GETREDIR")).await
    }

    #[doc = " The ID of the connection, unique until the server restarts."]
    #[doc = ""]
    #[doc = " CLIENT ID"]
    #[doc = " https://valkey.io/commands/client-id/"]
    #[allow(async_fn_in_trait)]
    async fn client_id(connection: &Connection) -> Result<Integer, Error> {
        query(connection, ("CLIENT", "ID")).await
    }

    #[doc = " Whether the connection is an import source, which may write while the server is in import"]
    #[doc = " mode."]
    #[doc = ""]
    #[doc = " CLIENT IMPORT-SOURCE 〈 ON | OFF 〉"]
    #[doc = " https://valkey.io/commands/client-import-source/"]
    #[allow(async_fn_in_trait)]
    async fn client_import_source(connection: &Connection, enabled: bool) -> Result<(), Error> {
        query(connection, ("CLIENT", "IMPORT-SOURCE", OnOff(enabled))).await
    }

    #[doc = " The fields of the connection, as CLIENT LIST has them, e.g. `id`, `addr` and `name`."]
    #[doc = ""]
    #[doc = " CLIENT INFO"]
    #[doc = " https://valkey.io/commands/client-info/"]
    #[allow(async_fn_in_trait)]
    async fn client_info(connection: &Connection) -> Result<Map<String, String>, Error> {
        // a line of CLIENT LIST
        let clients: ClientList = query(connection, ("CLIENT", "INFO")).await?;
        clients
            .0
            .into_iter()
            .next()
            .ok_or_else(|| Error::Client("unexpected empty reply".to_string()))
    }

    #[doc = " Whether the connection's commands leave the access time of the keys they read unchanged."]
    #[doc = ""]
    #[doc = " It applies to the connection, so to every caller sharing it."]
    #[doc = ""]
    #[doc = " CLIENT NO-TOUCH 〈 ON | OFF 〉"]
    #[doc = " https://valkey.io/commands/client-no-touch/"]
    #[allow(async_fn_in_trait)]
    async fn client_no_touch(connection: &Connection, enabled: bool) -> Result<(), Error> {
        query(connection, ("CLIENT", "NO-TOUCH", OnOff(enabled))).await
    }

    #[doc = " Sets the name or version of the library of the connection, see CLIENT LIST."]
    #[doc = ""]
    #[doc = " CLIENT SETINFO 〈 LIB-NAME libname | LIB-VER libver 〉"]
    #[doc = " https://valkey.io/commands/client-setinfo/"]
    #[allow(async_fn_in_trait)]
    async fn client_setinfo(connection: &Connection, attr: ClientSetinfoAttr) -> Result<(), Error> {
        query(connection, ("CLIENT", "SETINFO", attr)).await
    }

    #[doc = " Sets the name of the connection."]
    #[doc = ""]
    #[doc = " CLIENT SETNAME connection-name"]
    #[doc = " https://valkey.io/commands/client-setname/"]
    #[allow(async_fn_in_trait)]
    async fn client_setname(connection: &Connection, connection_name: String) -> Result<(), Error> {
        query(connection, ("CLIENT", "SETNAME", connection_name)).await
    }

    #[doc = " Turns tracking on with the options, or off with none, for server assisted client side"]
    #[doc = " caching. Invalidations arrive as push messages, see `connection.pushes`."]
    #[doc = ""]
    #[doc = " It applies to the connection, so to every caller sharing it."]
    #[doc = ""]
    #[doc = " CLIENT TRACKING 〈 ON | OFF 〉 [ REDIRECT client-id ] [ PREFIX prefix [ PREFIX prefix ... ] ] [ BCAST ] [ OPTIN ] [ OPTOUT ] [ NOLOOP ]"]
    #[doc = " https://valkey.io/commands/client-tracking/"]
    #[allow(async_fn_in_trait)]
    async fn client_tracking(
        connection: &Connection,
        options: Option<ClientTrackingOptions>,
    ) -> Result<(), Error> {
        // ON with options, OFF without
        let status = OnOff(options.is_some());
        query(connection, ("CLIENT", "TRACKING", status, options)).await
    }

    #[doc = " The connection's tracking state."]
    #[doc = ""]
    #[doc = " CLIENT TRACKINGINFO"]
    #[doc = " https://valkey.io/commands/client-trackinginfo/"]
    #[allow(async_fn_in_trait)]
    async fn client_trackinginfo(
        connection: &Connection,
    ) -> Result<ClientTrackinginfoReply, Error> {
        query(connection, ("CLIENT", "TRACKINGINFO")).await
    }

    #[doc = " The number of commands of the server."]
    #[doc = ""]
    #[doc = " COMMAND COUNT"]
    #[doc = " https://valkey.io/commands/command-count/"]
    #[allow(async_fn_in_trait)]
    async fn command_count(connection: &Connection) -> Result<Integer, Error> {
        query(connection, ("COMMAND", "COUNT")).await
    }

    #[doc = " Every command of the server, a container followed by its subcommands."]
    #[doc = ""]
    #[doc = " COMMAND"]
    #[doc = " https://valkey.io/commands/command/"]
    #[allow(async_fn_in_trait)]
    async fn command(connection: &Connection) -> Result<Vec<CommandInfoReply>, Error> {
        let commands: CommandInfoList = query(connection, "COMMAND").await?;
        Ok(commands.0)
    }

    #[doc = " The documentation of the commands, all of them without names, a container followed by its"]
    #[doc = " subcommands, commands that do not exist left out."]
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
            query(connection, ("COMMAND", "DOCS", command_names)).await?;
        Ok(commands.0)
    }

    #[doc = " The commands, all of them without names, a container followed by its subcommands, commands"]
    #[doc = " that do not exist left out."]
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
            query(connection, ("COMMAND", "INFO", command_names)).await?;
        Ok(commands.0)
    }

    #[doc = " The keys of the command, its name and arguments."]
    #[doc = ""]
    #[doc = " COMMAND GETKEYS command [ arg [ arg ... ] ]"]
    #[doc = " https://valkey.io/commands/command-getkeys/"]
    #[allow(async_fn_in_trait)]
    async fn command_getkeys(
        connection: &Connection,
        command: Vec<String>,
    ) -> Result<Vec<String>, Error> {
        query(
            connection,
            ("COMMAND", "GETKEYS", NonEmpty::new("command", command)),
        )
        .await
    }

    #[doc = " The keys of the command, its name and arguments, and how the command uses each, e.g. `RW`"]
    #[doc = " and `access`."]
    #[doc = ""]
    #[doc = " COMMAND GETKEYSANDFLAGS command [ arg [ arg ... ] ]"]
    #[doc = " https://valkey.io/commands/command-getkeysandflags/"]
    #[allow(async_fn_in_trait)]
    async fn command_getkeysandflags(
        connection: &Connection,
        command: Vec<String>,
    ) -> Result<Vec<(String, Vec<String>)>, Error> {
        query(
            connection,
            (
                "COMMAND",
                "GETKEYSANDFLAGS",
                NonEmpty::new("command", command),
            ),
        )
        .await
    }

    #[doc = " The names of the commands of the server, those that match the filter with one."]
    #[doc = ""]
    #[doc = " COMMAND LIST [ FILTERBY 〈 MODULE module-name | ACLCAT category | PATTERN pattern 〉 ]"]
    #[doc = " https://valkey.io/commands/command-list/"]
    #[allow(async_fn_in_trait)]
    async fn command_list(
        connection: &Connection,
        filter: Option<CommandListFilter>,
    ) -> Result<Vec<String>, Error> {
        query(connection, ("COMMAND", "LIST", filter)).await
    }

    #[doc = " Returns the message."]
    #[doc = ""]
    #[doc = " ECHO message"]
    #[doc = " https://valkey.io/commands/echo/"]
    #[allow(async_fn_in_trait)]
    async fn echo(connection: &Connection, message: String) -> Result<String, Error> {
        query(connection, ("ECHO", message)).await
    }

    #[doc = " Allows the connection to read from a replica of a cluster, for the slots of its primary."]
    #[doc = ""]
    #[doc = " It applies to the connection, so to every caller sharing it."]
    #[doc = ""]
    #[doc = " READONLY"]
    #[doc = " https://valkey.io/commands/readonly/"]
    #[allow(async_fn_in_trait)]
    async fn readonly(connection: &Connection) -> Result<(), Error> {
        query(connection, "READONLY").await
    }

    #[doc = " Disallows the connection to read from a replica of a cluster, the default."]
    #[doc = ""]
    #[doc = " It applies to the connection, so to every caller sharing it."]
    #[doc = ""]
    #[doc = " READWRITE"]
    #[doc = " https://valkey.io/commands/readwrite/"]
    #[allow(async_fn_in_trait)]
    async fn readwrite(connection: &Connection) -> Result<(), Error> {
        query(connection, "READWRITE").await
    }

    #[doc = " Changes the database of the connection."]
    #[doc = ""]
    #[doc = " It applies to the connection, so to every caller sharing it."]
    #[doc = ""]
    #[doc = " SELECT index"]
    #[doc = " https://valkey.io/commands/select/"]
    #[allow(async_fn_in_trait)]
    async fn select(connection: &Connection, index: Integer) -> Result<(), Error> {
        query(connection, ("SELECT", index)).await
    }

    #[doc = " Waits until the connection's writes reached the number of replicas, or the timeout passed,"]
    #[doc = " returning how many replicas they reached. A timeout of zero waits indefinitely."]
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
        query(connection, ("WAIT", numreplicas, timeout)).await
    }

    #[doc = " Waits until the connection's writes are in the append-only file of the primary, and of the"]
    #[doc = " number of replicas, or the timeout passed, returning how many of each they reached. A timeout"]
    #[doc = " of zero waits indefinitely."]
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
        query(connection, ("WAITAOF", numlocal, numreplicas, timeout)).await
    }

    #[doc = " Adds the members at their positions to the geospatial index stored at key, or updates their"]
    #[doc = " positions, returning how many were added, or with `changed`, added or updated."]
    #[doc = ""]
    #[doc = " GEOADD key [ NX | XX ] [ CH ] longitude latitude member [ longitude latitude member ... ]"]
    #[doc = " https://valkey.io/commands/geoadd/"]
    #[allow(async_fn_in_trait)]
    async fn geoadd(
        connection: &Connection,
        key: String,
        condition: Option<GeoaddCondition>,
        changed: bool,
        coordinates_and_members: Vec<(GeoCoordinates, String)>,
    ) -> Result<Integer, Error> {
        query(
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

    #[doc = " The distance between the members, in meters or the unit, none when either is not a member."]
    #[doc = ""]
    #[doc = " GEODIST key member1 member2 [ m | km | ft | mi ]"]
    #[doc = " https://valkey.io/commands/geodist/"]
    #[allow(async_fn_in_trait)]
    async fn geodist(
        connection: &Connection,
        key: String,
        member1: String,
        member2: String,
        unit: Option<GeoUnit>,
    ) -> Result<Option<Double>, Error> {
        query(connection, ("GEODIST", key, member1, member2, unit)).await
    }

    #[doc = " The geohash of each of the members, none for a member that is not in the index."]
    #[doc = ""]
    #[doc = " GEOHASH key [ member [ member ... ] ]"]
    #[doc = " https://valkey.io/commands/geohash/"]
    #[allow(async_fn_in_trait)]
    async fn geohash(
        connection: &Connection,
        key: String,
        members: Option<Vec<String>>,
    ) -> Result<Vec<Option<String>>, Error> {
        query(
            connection,
            ("GEOHASH", key, NonEmpty::optional("members", members)),
        )
        .await
    }

    #[doc = " The position of each of the members, none for a member that is not in the index."]
    #[doc = ""]
    #[doc = " GEOPOS key [ member [ member ... ] ]"]
    #[doc = " https://valkey.io/commands/geopos/"]
    #[allow(async_fn_in_trait)]
    async fn geopos(
        connection: &Connection,
        key: String,
        members: Option<Vec<String>>,
    ) -> Result<Vec<Option<GeoCoordinates>>, Error> {
        query(
            connection,
            ("GEOPOS", key, NonEmpty::optional("members", members)),
        )
        .await
    }

    #[doc = " The members of the geospatial index stored at key in the shape."]
    #[doc = ""]
    #[doc = " See also `geosearch-with`."]
    #[doc = ""]
    #[doc = " GEOSEARCH key [ FROMMEMBER member | FROMLONLAT longitude latitude ] 〈 BYRADIUS radius 〈 m | km | ft | mi 〉 | BYBOX width height 〈 m | km | ft | mi 〉 | BYPOLYGON num-vertices longitude latitude [ longitude latitude ... ] 〉 [ ASC | DESC ] [ COUNT count [ ANY ] ]"]
    #[doc = " https://valkey.io/commands/geosearch/"]
    #[allow(async_fn_in_trait)]
    async fn geosearch(
        connection: &Connection,
        key: String,
        shape: GeoShape,
        order: Option<SortOrder>,
        count: Option<GeoCount>,
    ) -> Result<Vec<String>, Error> {
        query(connection, ("GEOSEARCH", key, shape, order, count)).await
    }

    #[doc = " The members of the geospatial index stored at key in the shape, with the details asked for."]
    #[doc = ""]
    #[doc = " See also `geosearch`."]
    #[doc = ""]
    #[doc = " GEOSEARCH key [ FROMMEMBER member | FROMLONLAT longitude latitude ] 〈 BYRADIUS radius 〈 m | km | ft | mi 〉 | BYBOX width height 〈 m | km | ft | mi 〉 | BYPOLYGON num-vertices longitude latitude [ longitude latitude ... ] 〉 [ ASC | DESC ] [ COUNT count [ ANY ] ] [ WITHCOORD ] [ WITHDIST ] [ WITHHASH ]"]
    #[doc = " https://valkey.io/commands/geosearch/"]
    #[allow(async_fn_in_trait)]
    async fn geosearch_with(
        connection: &Connection,
        key: String,
        shape: GeoShape,
        order: Option<SortOrder>,
        count: Option<GeoCount>,
        withcoord: bool,
        withdist: bool,
        withhash: bool,
    ) -> Result<Vec<GeoSearchResult>, Error> {
        query(
            connection,
            (
                "GEOSEARCH",
                key,
                shape,
                order,
                count,
                Flag("WITHCOORD", withcoord),
                Flag("WITHDIST", withdist),
                Flag("WITHHASH", withhash),
            ),
        )
        .await
    }

    #[doc = " Stores the members of the geospatial index stored at source in the shape, returning how"]
    #[doc = " many, with their positions, or with `storedist`, their distance from the center as the"]
    #[doc = " score."]
    #[doc = ""]
    #[doc = " GEOSEARCHSTORE destination source [ FROMMEMBER member | FROMLONLAT longitude latitude ] 〈 BYRADIUS radius 〈 m | km | ft | mi 〉 | BYBOX width height 〈 m | km | ft | mi 〉 | BYPOLYGON num-vertices longitude latitude [ longitude latitude ... ] 〉 [ ASC | DESC ] [ COUNT count [ ANY ] ] [ STOREDIST ]"]
    #[doc = " https://valkey.io/commands/geosearchstore/"]
    #[allow(async_fn_in_trait)]
    async fn geosearchstore(
        connection: &Connection,
        destination: String,
        source: String,
        shape: GeoShape,
        order: Option<SortOrder>,
        count: Option<GeoCount>,
        storedist: bool,
    ) -> Result<Integer, Error> {
        query(
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

    #[doc = " Removes the specified fields from the hash stored at key."]
    #[doc = ""]
    #[doc = " HDEL key field [ field ... ]"]
    #[doc = " https://valkey.io/commands/hdel/"]
    #[allow(async_fn_in_trait)]
    async fn hdel(
        connection: &Connection,
        key: String,
        fields: Vec<String>,
    ) -> Result<Integer, Error> {
        query(connection, ("HDEL", key, NonEmpty::new("fields", fields))).await
    }

    #[doc = " Returns if field is an existing field in the hash stored at key."]
    #[doc = ""]
    #[doc = " HEXISTS key field"]
    #[doc = " https://valkey.io/commands/hexists/"]
    #[allow(async_fn_in_trait)]
    async fn hexists(connection: &Connection, key: String, field: String) -> Result<bool, Error> {
        query(connection, ("HEXISTS", key, field)).await
    }

    #[doc = " When each of the hash fields expires, to the second."]
    #[doc = ""]
    #[doc = " See also `hpexpiretime`."]
    #[doc = ""]
    #[doc = " HEXPIRETIME key FIELDS numfields field [ field ... ]"]
    #[doc = " https://valkey.io/commands/hexpiretime/"]
    #[allow(async_fn_in_trait)]
    async fn hexpiretime(
        connection: &Connection,
        key: String,
        fields: Vec<String>,
    ) -> Result<Vec<ExpiretimeReply>, Error> {
        query(
            connection,
            ("HEXPIRETIME", key, Fields(NonEmpty::new("fields", fields))),
        )
        .await
    }

    #[doc = " The values of the hash fields, none for a field that does not exist, deleting the fields."]
    #[doc = ""]
    #[doc = " HGETDEL key FIELDS numfields field [ field ... ]"]
    #[doc = " https://valkey.io/commands/hgetdel/"]
    #[allow(async_fn_in_trait)]
    async fn hgetdel(
        connection: &Connection,
        key: String,
        fields: Vec<String>,
    ) -> Result<Vec<Option<String>>, Error> {
        query(
            connection,
            ("HGETDEL", key, Fields(NonEmpty::new("fields", fields))),
        )
        .await
    }

    #[doc = " The values of the hash fields, none for a field that does not exist, optionally setting or"]
    #[doc = " removing their expiration."]
    #[doc = ""]
    #[doc = " HGETEX key [ EX seconds | PX milliseconds | EXAT unix-time-seconds | PXAT unix-time-milliseconds | PERSIST ] FIELDS numfields field [ field ... ]"]
    #[doc = " https://valkey.io/commands/hgetex/"]
    #[allow(async_fn_in_trait)]
    async fn hgetex(
        connection: &Connection,
        key: String,
        ttl: Option<GetexTtl>,
        fields: Vec<String>,
    ) -> Result<Vec<Option<String>>, Error> {
        query(
            connection,
            ("HGETEX", key, ttl, Fields(NonEmpty::new("fields", fields))),
        )
        .await
    }

    #[doc = " Removes the expiration of the hash fields."]
    #[doc = ""]
    #[doc = " HPERSIST key FIELDS numfields field [ field ... ]"]
    #[doc = " https://valkey.io/commands/hpersist/"]
    #[allow(async_fn_in_trait)]
    async fn hpersist(
        connection: &Connection,
        key: String,
        fields: Vec<String>,
    ) -> Result<Vec<FieldPersistResult>, Error> {
        query(
            connection,
            ("HPERSIST", key, Fields(NonEmpty::new("fields", fields))),
        )
        .await
    }

    #[doc = " Manipulate the expiration time for existing hash fields, in milliseconds."]
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
    ) -> Result<Vec<TtlResult>, Error> {
        let command = (
            "HPEXPIRE",
            key,
            milliseconds,
            mode,
            Fields(NonEmpty::new("fields", fields)),
        );
        query(connection, command).await
    }

    #[doc = " Manipulate the expiration time for existing hash fields, as a Unix time in milliseconds."]
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
    ) -> Result<Vec<TtlResult>, Error> {
        let command = (
            "HPEXPIREAT",
            key,
            milliseconds,
            mode,
            Fields(NonEmpty::new("fields", fields)),
        );
        query(connection, command).await
    }

    #[doc = " When each of the hash fields expires, to the millisecond."]
    #[doc = ""]
    #[doc = " See also `hexpiretime`."]
    #[doc = ""]
    #[doc = " HPEXPIRETIME key FIELDS numfields field [ field ... ]"]
    #[doc = " https://valkey.io/commands/hpexpiretime/"]
    #[allow(async_fn_in_trait)]
    async fn hpexpiretime(
        connection: &Connection,
        key: String,
        fields: Vec<String>,
    ) -> Result<Vec<PexpiretimeReply>, Error> {
        query(
            connection,
            ("HPEXPIRETIME", key, Fields(NonEmpty::new("fields", fields))),
        )
        .await
    }

    #[doc = " The remaining time to live of each of the hash fields, to the millisecond."]
    #[doc = ""]
    #[doc = " See also `httl`."]
    #[doc = ""]
    #[doc = " HPTTL key FIELDS numfields field [ field ... ]"]
    #[doc = " https://valkey.io/commands/hpttl/"]
    #[allow(async_fn_in_trait)]
    async fn hpttl(
        connection: &Connection,
        key: String,
        fields: Vec<String>,
    ) -> Result<Vec<PttlReply>, Error> {
        query(
            connection,
            ("HPTTL", key, Fields(NonEmpty::new("fields", fields))),
        )
        .await
    }

    #[doc = " Sets the fields to their values in the hash stored at key, optionally setting their"]
    #[doc = " expiration, returning whether they were set, false when a condition prevented it."]
    #[doc = ""]
    #[doc = " HSETEX key [ NX | XX ] [ FNX | FXX ] [ EX seconds | PX milliseconds | EXAT unix-time-seconds | PXAT unix-time-milliseconds | KEEPTTL ] FIELDS numfields field value [ field value ... ]"]
    #[doc = " https://valkey.io/commands/hsetex/"]
    #[allow(async_fn_in_trait)]
    async fn hsetex(
        connection: &Connection,
        key: String,
        condition: Option<HsetexCondition>,
        field_condition: Option<HsetexFieldCondition>,
        ttl: Option<SetTtl>,
        field_and_values: Vec<(String, String)>,
    ) -> Result<bool, Error> {
        let command = (
            "HSETEX",
            key,
            condition,
            field_condition,
            ttl,
            Fields(NonEmpty::new("field-and-values", field_and_values)),
        );
        query(connection, command).await
    }

    #[doc = " The remaining time to live of each of the hash fields, to the second."]
    #[doc = ""]
    #[doc = " See also `hpttl`."]
    #[doc = ""]
    #[doc = " HTTL key FIELDS numfields field [ field ... ]"]
    #[doc = " https://valkey.io/commands/httl/"]
    #[allow(async_fn_in_trait)]
    async fn httl(
        connection: &Connection,
        key: String,
        fields: Vec<String>,
    ) -> Result<Vec<TtlReply>, Error> {
        query(
            connection,
            ("HTTL", key, Fields(NonEmpty::new("fields", fields))),
        )
        .await
    }

    #[doc = " Manipulate the expiration time for existing hash fields."]
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
    ) -> Result<Vec<TtlResult>, Error> {
        query(
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

    #[doc = " Manipulate the expiration time for existing hash fields."]
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
    ) -> Result<Vec<TtlResult>, Error> {
        query(
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

    #[doc = " Returns the value associated with field in the hash stored at key."]
    #[doc = ""]
    #[doc = " HGET key field"]
    #[doc = " https://valkey.io/commands/hget/"]
    #[allow(async_fn_in_trait)]
    async fn hget(
        connection: &Connection,
        key: String,
        field: String,
    ) -> Result<Option<String>, Error> {
        query(connection, ("HGET", key, field)).await
    }

    #[doc = " Returns all fields and values of the hash stored at key."]
    #[doc = ""]
    #[doc = " HGETALL key"]
    #[doc = " https://valkey.io/commands/hgetall/"]
    #[allow(async_fn_in_trait)]
    async fn hgetall(connection: &Connection, key: String) -> Result<Map<String, String>, Error> {
        query(connection, ("HGETALL", key)).await
    }

    #[doc = " Increments the number stored at field in the hash stored at key by increment."]
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
        query(connection, ("HINCRBY", key, field, increment)).await
    }

    #[doc = " Increment the specified field of a hash stored at key, and representing a floating point number, by the specified increment."]
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
        query(connection, ("HINCRBYFLOAT", key, field, increment)).await
    }

    #[doc = " Returns all field names in the hash stored at key."]
    #[doc = ""]
    #[doc = " HKEYS key"]
    #[doc = " https://valkey.io/commands/hkeys/"]
    #[allow(async_fn_in_trait)]
    async fn hkeys(connection: &Connection, key: String) -> Result<Vec<String>, Error> {
        query(connection, ("HKEYS", key)).await
    }

    #[doc = " Returns the number of fields contained in the hash stored at key."]
    #[doc = ""]
    #[doc = " HLEN key"]
    #[doc = " https://valkey.io/commands/hlen/"]
    #[allow(async_fn_in_trait)]
    async fn hlen(connection: &Connection, key: String) -> Result<Integer, Error> {
        query(connection, ("HLEN", key)).await
    }

    #[doc = " Returns the values associated with the specified fields in the hash stored at key."]
    #[doc = ""]
    #[doc = " HMGET key field [ field ... ]"]
    #[doc = " https://valkey.io/commands/hmget/"]
    #[allow(async_fn_in_trait)]
    async fn hmget(
        connection: &Connection,
        key: String,
        fields: Vec<String>,
    ) -> Result<Vec<Option<String>>, Error> {
        query(connection, ("HMGET", key, NonEmpty::new("fields", fields))).await
    }

    #[doc = " Sets the specified fields to their respective values in the hash stored at key."]
    #[doc = ""]
    #[doc = " HMSET key field value [ field value ... ]"]
    #[doc = " https://valkey.io/commands/hmset/"]
    #[allow(async_fn_in_trait)]
    async fn hmset(
        connection: &Connection,
        key: String,
        field_and_values: Vec<(String, String)>,
    ) -> Result<(), Error> {
        query(
            connection,
            (
                "HMSET",
                key,
                NonEmpty::new("field-and-values", field_and_values),
            ),
        )
        .await
    }

    #[doc = " Random fields from the hash value stored at key, up to the count."]
    #[doc = ""]
    #[doc = " Without a count, one field, or none when the key does not exist. A negative count may return"]
    #[doc = " the same field more than once, its absolute value is the number of fields returned."]
    #[doc = ""]
    #[doc = " Also see `hrandfield-withvalues`."]
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
            return query(connection, ("HRANDFIELD", key, count)).await;
        }
        // without a count the reply is the field itself, or null
        let field: Option<String> = query(connection, ("HRANDFIELD", key)).await?;
        Ok(field.into_iter().collect())
    }

    #[doc = " Random fields and their values from the hash value stored at key, up to the count."]
    #[doc = ""]
    #[doc = " A negative count may return the same field more than once, its absolute value is the number of"]
    #[doc = " fields returned."]
    #[doc = ""]
    #[doc = " Also see `hrandfield`."]
    #[doc = ""]
    #[doc = " HRANDFIELD key count WITHVALUES"]
    #[doc = " https://valkey.io/commands/hrandfield/"]
    #[allow(async_fn_in_trait)]
    async fn hrandfield_withvalues(
        connection: &Connection,
        key: String,
        count: Integer,
    ) -> Result<Vec<(String, String)>, Error> {
        query(connection, ("HRANDFIELD", key, count, "WITHVALUES")).await
    }

    #[doc = " Incrementally iterate over a collection of elements."]
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
    ) -> Result<(Option<Cursor>, Map<String, String>), Error> {
        let (cursor, items): (NextCursor, Map<String, String>) = query(
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

    #[doc = " Incrementally iterate over a collection of keys."]
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
        let (cursor, items): (NextCursor, Vec<String>) = query(
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

    #[doc = " Sets the specified fields to their respective values in the hash stored at key."]
    #[doc = ""]
    #[doc = " HSET key field value [ field value ... ]"]
    #[doc = " https://valkey.io/commands/hset/"]
    #[allow(async_fn_in_trait)]
    async fn hset(
        connection: &Connection,
        key: String,
        field_and_values: Vec<(String, String)>,
    ) -> Result<Integer, Error> {
        query(
            connection,
            (
                "HSET",
                key,
                NonEmpty::new("field-and-values", field_and_values),
            ),
        )
        .await
    }

    #[doc = " Sets field in the hash stored at key to value, only if field does not yet exist."]
    #[doc = ""]
    #[doc = " HSETNX key field value"]
    #[doc = " https://valkey.io/commands/hsetnx/"]
    #[allow(async_fn_in_trait)]
    async fn hsetnx(
        connection: &Connection,
        key: String,
        field: String,
        value: String,
    ) -> Result<bool, Error> {
        query(connection, ("HSETNX", key, field, value)).await
    }

    #[doc = " Returns the string length of the value associated with field in the hash stored at key."]
    #[doc = ""]
    #[doc = " HSTRLEN key field"]
    #[doc = " https://valkey.io/commands/hstrlen/"]
    #[allow(async_fn_in_trait)]
    async fn hstrlen(
        connection: &Connection,
        key: String,
        field: String,
    ) -> Result<Integer, Error> {
        query(connection, ("HSTRLEN", key, field)).await
    }

    #[doc = " All values in the hash stored at key."]
    #[doc = ""]
    #[doc = " HVALS key"]
    #[doc = " https://valkey.io/commands/hvals/"]
    #[allow(async_fn_in_trait)]
    async fn hvals(connection: &Connection, key: String) -> Result<Vec<String>, Error> {
        query(connection, ("HVALS", key)).await
    }

    #[doc = " Adds the elements to the HyperLogLog stored at key, creating it when it does not exist,"]
    #[doc = " returning whether its estimate may have changed."]
    #[doc = ""]
    #[doc = " Without elements, creates an empty HyperLogLog, true only when the key did not exist."]
    #[doc = ""]
    #[doc = " PFADD key [ element [ element ... ] ]"]
    #[doc = " https://valkey.io/commands/pfadd/"]
    #[allow(async_fn_in_trait)]
    async fn pfadd(
        connection: &Connection,
        key: String,
        elements: Option<Vec<String>>,
    ) -> Result<bool, Error> {
        query(
            connection,
            ("PFADD", key, NonEmpty::optional("elements", elements)),
        )
        .await
    }

    #[doc = " The approximate number of unique elements added to the HyperLogLogs, of their union for"]
    #[doc = " more than one, 0 for a key that does not exist."]
    #[doc = ""]
    #[doc = " PFCOUNT key [ key ... ]"]
    #[doc = " https://valkey.io/commands/pfcount/"]
    #[allow(async_fn_in_trait)]
    async fn pfcount(connection: &Connection, keys: Vec<String>) -> Result<Integer, Error> {
        query(connection, ("PFCOUNT", NonEmpty::new("keys", keys))).await
    }

    #[doc = " Merges the HyperLogLogs stored at the source keys into the one stored at destination,"]
    #[doc = " creating it when it does not exist."]
    #[doc = ""]
    #[doc = " Without source keys, creates an empty HyperLogLog when destination does not exist."]
    #[doc = ""]
    #[doc = " PFMERGE destkey [ sourcekey [ sourcekey ... ] ]"]
    #[doc = " https://valkey.io/commands/pfmerge/"]
    #[allow(async_fn_in_trait)]
    async fn pfmerge(
        connection: &Connection,
        destination: String,
        sources: Option<Vec<String>>,
    ) -> Result<(), Error> {
        query(
            connection,
            (
                "PFMERGE",
                destination,
                NonEmpty::optional("sources", sources),
            ),
        )
        .await
    }

    #[doc = " Moves an element from one end of the source list to an end of the destination list, waiting"]
    #[doc = " up to the timeout for the source to have one, returning it."]
    #[doc = ""]
    #[doc = " Returns none when the timeout passed. A timeout of zero waits indefinitely."]
    #[doc = ""]
    #[doc = " The connection waits too, replies to commands sent after it on the connection wait behind"]
    #[doc = " it. Use a connection of your own."]
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
    ) -> Result<Option<String>, Error> {
        query(
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

    #[doc = " Removes elements from an end of the first non-empty list, waiting up to the timeout for one,"]
    #[doc = " returning its key and the elements, one without a count."]
    #[doc = ""]
    #[doc = " Returns none when the timeout passed. A timeout of zero waits indefinitely."]
    #[doc = ""]
    #[doc = " The connection waits too, replies to commands sent after it on the connection wait behind"]
    #[doc = " it. Use a connection of your own."]
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
    ) -> Result<Option<(String, Vec<String>)>, Error> {
        query(
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

    #[doc = " Removes the first element of the first non-empty list, waiting up to the timeout for one,"]
    #[doc = " returning its key and the element."]
    #[doc = ""]
    #[doc = " Returns none when the timeout passed. A timeout of zero waits indefinitely."]
    #[doc = ""]
    #[doc = " The connection waits too, replies to commands sent after it on the connection wait behind"]
    #[doc = " it. Use a connection of your own."]
    #[doc = ""]
    #[doc = " BLPOP key [ key ... ] timeout"]
    #[doc = " https://valkey.io/commands/blpop/"]
    #[allow(async_fn_in_trait)]
    async fn blpop(
        connection: &Connection,
        keys: Vec<String>,
        timeout: DurationMilliseconds,
    ) -> Result<Option<(String, String)>, Error> {
        query(
            connection,
            (
                "BLPOP",
                NonEmpty::new("keys", keys),
                DecimalSeconds(timeout),
            ),
        )
        .await
    }

    #[doc = " Removes the last element of the first non-empty list, waiting up to the timeout for one,"]
    #[doc = " returning its key and the element."]
    #[doc = ""]
    #[doc = " Returns none when the timeout passed. A timeout of zero waits indefinitely."]
    #[doc = ""]
    #[doc = " The connection waits too, replies to commands sent after it on the connection wait behind"]
    #[doc = " it. Use a connection of your own."]
    #[doc = ""]
    #[doc = " BRPOP key [ key ... ] timeout"]
    #[doc = " https://valkey.io/commands/brpop/"]
    #[allow(async_fn_in_trait)]
    async fn brpop(
        connection: &Connection,
        keys: Vec<String>,
        timeout: DurationMilliseconds,
    ) -> Result<Option<(String, String)>, Error> {
        query(
            connection,
            (
                "BRPOP",
                NonEmpty::new("keys", keys),
                DecimalSeconds(timeout),
            ),
        )
        .await
    }

    #[doc = " Moves the last element of the source list to the head of the destination list, waiting up"]
    #[doc = " to the timeout for the source to have one, returning it."]
    #[doc = ""]
    #[doc = " Returns none when the timeout passed. A timeout of zero waits indefinitely."]
    #[doc = ""]
    #[doc = " The connection waits too, replies to commands sent after it on the connection wait behind"]
    #[doc = " it. Use a connection of your own."]
    #[doc = ""]
    #[doc = " See also `blmove`."]
    #[doc = ""]
    #[doc = " BRPOPLPUSH source destination timeout"]
    #[doc = " https://valkey.io/commands/brpoplpush/"]
    #[allow(async_fn_in_trait)]
    async fn brpoplpush(
        connection: &Connection,
        source: String,
        destination: String,
        timeout: DurationMilliseconds,
    ) -> Result<Option<String>, Error> {
        query(
            connection,
            ("BRPOPLPUSH", source, destination, DecimalSeconds(timeout)),
        )
        .await
    }

    #[doc = " The element at the index of the list stored at key, negative from the end, none when the"]
    #[doc = " index is out of range."]
    #[doc = ""]
    #[doc = " LINDEX key index"]
    #[doc = " https://valkey.io/commands/lindex/"]
    #[allow(async_fn_in_trait)]
    async fn lindex(
        connection: &Connection,
        key: String,
        index: Integer,
    ) -> Result<Option<String>, Error> {
        query(connection, ("LINDEX", key, index)).await
    }

    #[doc = " Inserts the element before or after the first occurrence of the pivot in the list stored at"]
    #[doc = " key."]
    #[doc = ""]
    #[doc = " LINSERT key 〈 BEFORE | AFTER 〉 pivot element"]
    #[doc = " https://valkey.io/commands/linsert/"]
    #[allow(async_fn_in_trait)]
    async fn linsert(
        connection: &Connection,
        key: String,
        position: LinsertPosition,
        pivot: String,
        element: String,
    ) -> Result<LinsertResult, Error> {
        query(connection, ("LINSERT", key, position, pivot, element)).await
    }

    #[doc = " The length of the list stored at key, 0 when the key does not exist."]
    #[doc = ""]
    #[doc = " LLEN key"]
    #[doc = " https://valkey.io/commands/llen/"]
    #[allow(async_fn_in_trait)]
    async fn llen(connection: &Connection, key: String) -> Result<Integer, Error> {
        query(connection, ("LLEN", key)).await
    }

    #[doc = " Moves an element from one end of the source list to an end of the destination list,"]
    #[doc = " returning it, none when the source does not exist."]
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
    ) -> Result<Option<String>, Error> {
        query(
            connection,
            ("LMOVE", source, destination, wherefrom, whereto),
        )
        .await
    }

    #[doc = " Removes elements from an end of the first non-empty list, returning its key and the"]
    #[doc = " elements, one without a count, none when every list is empty."]
    #[doc = ""]
    #[doc = " LMPOP numkeys key [ key ... ] 〈 LEFT | RIGHT 〉 [ COUNT count ]"]
    #[doc = " https://valkey.io/commands/lmpop/"]
    #[allow(async_fn_in_trait)]
    async fn lmpop(
        connection: &Connection,
        keys: Vec<String>,
        end: ListSide,
        count: Option<Integer>,
    ) -> Result<Option<(String, Vec<String>)>, Error> {
        let command = (
            "LMPOP",
            NumKeys(NonEmpty::new("keys", keys)),
            end,
            NamedOption("COUNT", count),
        );
        query(connection, command).await
    }

    #[doc = " Removes and returns the first elements of the list stored at key, up to the count, one"]
    #[doc = " without a count, none when the key does not exist."]
    #[doc = ""]
    #[doc = " LPOP key [ count ]"]
    #[doc = " https://valkey.io/commands/lpop/"]
    #[allow(async_fn_in_trait)]
    async fn lpop(
        connection: &Connection,
        key: String,
        count: Option<Integer>,
    ) -> Result<Vec<String>, Error> {
        match count {
            Some(count) => {
                // null when the key does not exist
                let elements: Option<Vec<String>> = query(connection, ("LPOP", key, count)).await?;
                Ok(elements.unwrap_or_default())
            }
            None => {
                // without a count the reply is the element itself, or null
                let element: Option<String> = query(connection, ("LPOP", key)).await?;
                Ok(element.into_iter().collect())
            }
        }
    }

    #[doc = " The positions of the element in the list stored at key, up to the count, 0 for every match,"]
    #[doc = " the first match without a count."]
    #[doc = ""]
    #[doc = " The rank skips matches, the second match for 2, counting from the end when negative. The"]
    #[doc = " max length limits how many elements are compared."]
    #[doc = ""]
    #[doc = " LPOS key element [ RANK rank ] [ COUNT num-matches ] [ MAXLEN len ]"]
    #[doc = " https://valkey.io/commands/lpos/"]
    #[allow(async_fn_in_trait)]
    async fn lpos(
        connection: &Connection,
        key: String,
        element: String,
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
            Some(_) => query(connection, command).await,
            None => {
                // without a count the reply is the position itself, or null
                let position: Option<Integer> = query(connection, command).await?;
                Ok(position.into_iter().collect())
            }
        }
    }

    #[doc = " Inserts the elements at the head of the list stored at key, creating it when it does not"]
    #[doc = " exist, returning its length after."]
    #[doc = ""]
    #[doc = " LPUSH key element [ element ... ]"]
    #[doc = " https://valkey.io/commands/lpush/"]
    #[allow(async_fn_in_trait)]
    async fn lpush(
        connection: &Connection,
        key: String,
        elements: Vec<String>,
    ) -> Result<Integer, Error> {
        query(
            connection,
            ("LPUSH", key, NonEmpty::new("elements", elements)),
        )
        .await
    }

    #[doc = " Inserts the elements at the head of the list stored at key, only when it exists, returning"]
    #[doc = " its length after, 0 when it does not exist."]
    #[doc = ""]
    #[doc = " LPUSHX key element [ element ... ]"]
    #[doc = " https://valkey.io/commands/lpushx/"]
    #[allow(async_fn_in_trait)]
    async fn lpushx(
        connection: &Connection,
        key: String,
        elements: Vec<String>,
    ) -> Result<Integer, Error> {
        query(
            connection,
            ("LPUSHX", key, NonEmpty::new("elements", elements)),
        )
        .await
    }

    #[doc = " The elements of the list stored at key from the start to the stop, both included, negative"]
    #[doc = " from the end."]
    #[doc = ""]
    #[doc = " LRANGE key start stop"]
    #[doc = " https://valkey.io/commands/lrange/"]
    #[allow(async_fn_in_trait)]
    async fn lrange(
        connection: &Connection,
        key: String,
        start: Integer,
        stop: Integer,
    ) -> Result<Vec<String>, Error> {
        query(connection, ("LRANGE", key, start, stop)).await
    }

    #[doc = " Removes occurrences of the element from the list stored at key, returning how many."]
    #[doc = ""]
    #[doc = " A positive count removes up to that many from the head, a negative count from the tail, and"]
    #[doc = " 0 removes them all."]
    #[doc = ""]
    #[doc = " LREM key count element"]
    #[doc = " https://valkey.io/commands/lrem/"]
    #[allow(async_fn_in_trait)]
    async fn lrem(
        connection: &Connection,
        key: String,
        count: Integer,
        element: String,
    ) -> Result<Integer, Error> {
        query(connection, ("LREM", key, count, element)).await
    }

    #[doc = " Sets the element at the index of the list stored at key, negative from the end."]
    #[doc = ""]
    #[doc = " LSET key index element"]
    #[doc = " https://valkey.io/commands/lset/"]
    #[allow(async_fn_in_trait)]
    async fn lset(
        connection: &Connection,
        key: String,
        index: Integer,
        element: String,
    ) -> Result<(), Error> {
        query(connection, ("LSET", key, index, element)).await
    }

    #[doc = " Trims the list stored at key to the elements from the start to the stop, both included,"]
    #[doc = " negative from the end."]
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
        query(connection, ("LTRIM", key, start, stop)).await
    }

    #[doc = " Removes and returns the last elements of the list stored at key, up to the count, one"]
    #[doc = " without a count, none when the key does not exist."]
    #[doc = ""]
    #[doc = " RPOP key [ count ]"]
    #[doc = " https://valkey.io/commands/rpop/"]
    #[allow(async_fn_in_trait)]
    async fn rpop(
        connection: &Connection,
        key: String,
        count: Option<Integer>,
    ) -> Result<Vec<String>, Error> {
        match count {
            Some(count) => {
                // null when the key does not exist
                let elements: Option<Vec<String>> = query(connection, ("RPOP", key, count)).await?;
                Ok(elements.unwrap_or_default())
            }
            None => {
                // without a count the reply is the element itself, or null
                let element: Option<String> = query(connection, ("RPOP", key)).await?;
                Ok(element.into_iter().collect())
            }
        }
    }

    #[doc = " Moves the last element of the source list to the head of the destination list, returning"]
    #[doc = " it, none when the source does not exist."]
    #[doc = ""]
    #[doc = " See also `lmove`."]
    #[doc = ""]
    #[doc = " RPOPLPUSH source destination"]
    #[doc = " https://valkey.io/commands/rpoplpush/"]
    #[allow(async_fn_in_trait)]
    async fn rpoplpush(
        connection: &Connection,
        source: String,
        destination: String,
    ) -> Result<Option<String>, Error> {
        query(connection, ("RPOPLPUSH", source, destination)).await
    }

    #[doc = " Inserts the elements at the tail of the list stored at key, creating it when it does not"]
    #[doc = " exist, returning its length after."]
    #[doc = ""]
    #[doc = " RPUSH key element [ element ... ]"]
    #[doc = " https://valkey.io/commands/rpush/"]
    #[allow(async_fn_in_trait)]
    async fn rpush(
        connection: &Connection,
        key: String,
        elements: Vec<String>,
    ) -> Result<Integer, Error> {
        query(
            connection,
            ("RPUSH", key, NonEmpty::new("elements", elements)),
        )
        .await
    }

    #[doc = " Inserts the elements at the tail of the list stored at key, only when it exists, returning"]
    #[doc = " its length after, 0 when it does not exist."]
    #[doc = ""]
    #[doc = " RPUSHX key element [ element ... ]"]
    #[doc = " https://valkey.io/commands/rpushx/"]
    #[allow(async_fn_in_trait)]
    async fn rpushx(
        connection: &Connection,
        key: String,
        elements: Vec<String>,
    ) -> Result<Integer, Error> {
        query(
            connection,
            ("RPUSHX", key, NonEmpty::new("elements", elements)),
        )
        .await
    }

    #[doc = " Removes the specified keys."]
    #[doc = ""]
    #[doc = " DEL key [ key ... ]"]
    #[doc = " https://valkey.io/commands/del/"]
    #[allow(async_fn_in_trait)]
    async fn del(connection: &Connection, keys: Vec<String>) -> Result<Integer, Error> {
        query(connection, ("DEL", NonEmpty::new("keys", keys))).await
    }

    #[doc = " If key exists."]
    #[doc = ""]
    #[doc = " EXISTS key [ key ... ]"]
    #[doc = " https://valkey.io/commands/exists/"]
    #[allow(async_fn_in_trait)]
    async fn exists(connection: &Connection, keys: Vec<String>) -> Result<Integer, Error> {
        query(connection, ("EXISTS", NonEmpty::new("keys", keys))).await
    }

    #[doc = " Set a timeout on key."]
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
        query(connection, ("EXPIRE", key, seconds, mode)).await
    }

    #[doc = " Set a timeout on key."]
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
        query(connection, ("EXPIREAT", key, seconds, mode)).await
    }

    #[doc = " Returns all keys matching pattern."]
    #[doc = ""]
    #[doc = " KEYS pattern"]
    #[doc = " https://valkey.io/commands/keys/"]
    #[allow(async_fn_in_trait)]
    async fn keys(connection: &Connection, pattern: String) -> Result<Vec<String>, Error> {
        query(connection, ("KEYS", pattern)).await
    }

    #[doc = " Removes all the keys of the slot, in the mode, the server's `lazyfree-lazy-user-flush`"]
    #[doc = " without one."]
    #[doc = ""]
    #[doc = " CLUSTER FLUSHSLOT slot [ ASYNC | SYNC ]"]
    #[doc = " https://valkey.io/commands/cluster-flushslot/"]
    #[allow(async_fn_in_trait)]
    async fn cluster_flushslot(
        connection: &Connection,
        slot: u16,
        mode: Option<FlushMode>,
    ) -> Result<(), Error> {
        query(connection, ("CLUSTER", "FLUSHSLOT", slot, mode)).await
    }

    #[doc = " Incrementally iterate over the keys of a cluster, slot by slot, or within the slot."]
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
        let (cursor, keys): (NextCursor, Vec<String>) = query(connection, command).await?;
        Ok((cursor.0, keys))
    }

    #[doc = " Copies the value of the source key to the destination key, in the database, or the current"]
    #[doc = " one, returning whether it was copied, false when the destination exists without `replace`."]
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
        query(
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

    #[doc = " The number of keys in the current database."]
    #[doc = ""]
    #[doc = " DBSIZE"]
    #[doc = " https://valkey.io/commands/dbsize/"]
    #[allow(async_fn_in_trait)]
    async fn dbsize(connection: &Connection) -> Result<Integer, Error> {
        query(connection, "DBSIZE").await
    }

    #[doc = " When the key expires, to the second."]
    #[doc = ""]
    #[doc = " See also `pexpiretime`."]
    #[doc = ""]
    #[doc = " EXPIRETIME key"]
    #[doc = " https://valkey.io/commands/expiretime/"]
    #[allow(async_fn_in_trait)]
    async fn expiretime(connection: &Connection, key: String) -> Result<ExpiretimeReply, Error> {
        query(connection, ("EXPIRETIME", key)).await
    }

    #[doc = " Removes all the keys of all the databases, in the mode, the server's"]
    #[doc = " `lazyfree-lazy-user-flush` without one."]
    #[doc = ""]
    #[doc = " FLUSHALL [ ASYNC | SYNC ]"]
    #[doc = " https://valkey.io/commands/flushall/"]
    #[allow(async_fn_in_trait)]
    async fn flushall(connection: &Connection, mode: Option<FlushMode>) -> Result<(), Error> {
        query(connection, ("FLUSHALL", mode)).await
    }

    #[doc = " Removes all the keys of the current database, in the mode, the server's"]
    #[doc = " `lazyfree-lazy-user-flush` without one."]
    #[doc = ""]
    #[doc = " FLUSHDB [ ASYNC | SYNC ]"]
    #[doc = " https://valkey.io/commands/flushdb/"]
    #[allow(async_fn_in_trait)]
    async fn flushdb(connection: &Connection, mode: Option<FlushMode>) -> Result<(), Error> {
        query(connection, ("FLUSHDB", mode)).await
    }

    #[doc = " Moves the keys to the database of another instance, returning whether any were moved, false"]
    #[doc = " when none of the keys exist."]
    #[doc = ""]
    #[doc = " With `copy` the keys are kept in this instance, with `replace` existing keys in the other"]
    #[doc = " instance are replaced."]
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
        copy: bool,
        replace: bool,
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
            Flag("COPY", copy),
            Flag("REPLACE", replace),
            auth,
            ("KEYS", NonEmpty::new("keys", keys)),
        );
        let migrated: Migrated = query(connection, command).await?;
        Ok(migrated.0)
    }

    #[doc = " Moves the key to the database, returning whether it was moved, false when it does not"]
    #[doc = " exist, or exists in the database without `replace`."]
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
        query(connection, ("MOVE", key, db, Flag("REPLACE", replace))).await
    }

    #[doc = " The internal encoding of the value stored at key, e.g. `listpack`, none when the key does"]
    #[doc = " not exist."]
    #[doc = ""]
    #[doc = " OBJECT ENCODING key"]
    #[doc = " https://valkey.io/commands/object-encoding/"]
    #[allow(async_fn_in_trait)]
    async fn object_encoding(
        connection: &Connection,
        key: String,
    ) -> Result<Option<String>, Error> {
        query(connection, ("OBJECT", "ENCODING", key)).await
    }

    #[doc = " The logarithmic access frequency counter of the key, none when it does not exist."]
    #[doc = ""]
    #[doc = " Only with an LFU `maxmemory-policy`, otherwise it fails."]
    #[doc = ""]
    #[doc = " OBJECT FREQ key"]
    #[doc = " https://valkey.io/commands/object-freq/"]
    #[allow(async_fn_in_trait)]
    async fn object_freq(connection: &Connection, key: String) -> Result<Option<Integer>, Error> {
        query(connection, ("OBJECT", "FREQ", key)).await
    }

    #[doc = " The time since the key was last accessed, none when it does not exist."]
    #[doc = ""]
    #[doc = " OBJECT IDLETIME key"]
    #[doc = " https://valkey.io/commands/object-idletime/"]
    #[allow(async_fn_in_trait)]
    async fn object_idletime(
        connection: &Connection,
        key: String,
    ) -> Result<Option<DurationSeconds>, Error> {
        query(connection, ("OBJECT", "IDLETIME", key)).await
    }

    #[doc = " The number of references to the value stored at key, none when the key does not exist."]
    #[doc = ""]
    #[doc = " OBJECT REFCOUNT key"]
    #[doc = " https://valkey.io/commands/object-refcount/"]
    #[allow(async_fn_in_trait)]
    async fn object_refcount(
        connection: &Connection,
        key: String,
    ) -> Result<Option<Integer>, Error> {
        query(connection, ("OBJECT", "REFCOUNT", key)).await
    }

    #[doc = " Removes the expiration of the key, returning whether it had one."]
    #[doc = ""]
    #[doc = " PERSIST key"]
    #[doc = " https://valkey.io/commands/persist/"]
    #[allow(async_fn_in_trait)]
    async fn persist(connection: &Connection, key: String) -> Result<bool, Error> {
        query(connection, ("PERSIST", key)).await
    }

    #[doc = " Set a timeout on key, in milliseconds."]
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
        query(connection, ("PEXPIRE", key, milliseconds, mode)).await
    }

    #[doc = " Set a timeout on key, as a Unix time in milliseconds."]
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
        query(connection, ("PEXPIREAT", key, milliseconds, mode)).await
    }

    #[doc = " When the key expires, to the millisecond."]
    #[doc = ""]
    #[doc = " See also `expiretime`."]
    #[doc = ""]
    #[doc = " PEXPIRETIME key"]
    #[doc = " https://valkey.io/commands/pexpiretime/"]
    #[allow(async_fn_in_trait)]
    async fn pexpiretime(connection: &Connection, key: String) -> Result<PexpiretimeReply, Error> {
        query(connection, ("PEXPIRETIME", key)).await
    }

    #[doc = " The remaining time to live of the key, to the millisecond."]
    #[doc = ""]
    #[doc = " See also `ttl`."]
    #[doc = ""]
    #[doc = " PTTL key"]
    #[doc = " https://valkey.io/commands/pttl/"]
    #[allow(async_fn_in_trait)]
    async fn pttl(connection: &Connection, key: String) -> Result<PttlReply, Error> {
        query(connection, ("PTTL", key)).await
    }

    #[doc = " A random key of the current database, none when it is empty."]
    #[doc = ""]
    #[doc = " RANDOMKEY"]
    #[doc = " https://valkey.io/commands/randomkey/"]
    #[allow(async_fn_in_trait)]
    async fn randomkey(connection: &Connection) -> Result<Option<String>, Error> {
        query(connection, "RANDOMKEY").await
    }

    #[doc = " Renames the key, replacing the new key when it exists."]
    #[doc = ""]
    #[doc = " RENAME key newkey"]
    #[doc = " https://valkey.io/commands/rename/"]
    #[allow(async_fn_in_trait)]
    async fn rename(connection: &Connection, key: String, newkey: String) -> Result<(), Error> {
        query(connection, ("RENAME", key, newkey)).await
    }

    #[doc = " Renames the key, only when the new key does not exist, returning whether it was renamed."]
    #[doc = ""]
    #[doc = " RENAMENX key newkey"]
    #[doc = " https://valkey.io/commands/renamenx/"]
    #[allow(async_fn_in_trait)]
    async fn renamenx(connection: &Connection, key: String, newkey: String) -> Result<bool, Error> {
        query(connection, ("RENAMENX", key, newkey)).await
    }

    #[doc = " Incrementally iterate over the keys of the current database."]
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
        let (cursor, keys): (NextCursor, Vec<String>) = query(connection, command).await?;
        Ok((cursor.0, keys))
    }

    #[doc = " Swaps the two databases, for connections using either."]
    #[doc = ""]
    #[doc = " SWAPDB index1 index2"]
    #[doc = " https://valkey.io/commands/swapdb/"]
    #[allow(async_fn_in_trait)]
    async fn swapdb(
        connection: &Connection,
        index1: Integer,
        index2: Integer,
    ) -> Result<(), Error> {
        query(connection, ("SWAPDB", index1, index2)).await
    }

    #[doc = " Updates the last access time of the keys, returning how many exist."]
    #[doc = ""]
    #[doc = " TOUCH key [ key ... ]"]
    #[doc = " https://valkey.io/commands/touch/"]
    #[allow(async_fn_in_trait)]
    async fn touch(connection: &Connection, keys: Vec<String>) -> Result<Integer, Error> {
        query(connection, ("TOUCH", NonEmpty::new("keys", keys))).await
    }

    #[doc = " The remaining time to live of the key, to the second."]
    #[doc = ""]
    #[doc = " See also `pttl`."]
    #[doc = ""]
    #[doc = " TTL key"]
    #[doc = " https://valkey.io/commands/ttl/"]
    #[allow(async_fn_in_trait)]
    async fn ttl(connection: &Connection, key: String) -> Result<TtlReply, Error> {
        query(connection, ("TTL", key)).await
    }

    #[doc = " The type of the value stored at key, none when the key does not exist."]
    #[doc = ""]
    #[doc = " TYPE key"]
    #[doc = " https://valkey.io/commands/type/"]
    #[allow(async_fn_in_trait)]
    async fn type_(connection: &Connection, key: String) -> Result<Option<KeyType>, Error> {
        let key_type: OptionalKeyType = query(connection, ("TYPE", key)).await?;
        Ok(key_type.0)
    }

    #[doc = " Removes the keys, freeing their memory in the background, returning how many existed."]
    #[doc = ""]
    #[doc = " UNLINK key [ key ... ]"]
    #[doc = " https://valkey.io/commands/unlink/"]
    #[allow(async_fn_in_trait)]
    async fn unlink(connection: &Connection, keys: Vec<String>) -> Result<Integer, Error> {
        query(connection, ("UNLINK", NonEmpty::new("keys", keys))).await
    }

    #[doc = " Subscribes the client to the given patterns."]
    #[doc = ""]
    #[doc = " PSUBSCRIBE pattern [ pattern ... ]"]
    #[doc = " https://valkey.io/commands/psubscribe/"]
    #[allow(async_fn_in_trait)]
    async fn psubscribe(connection: &Connection, patterns: Vec<String>) -> Result<(), Error> {
        emit(
            connection,
            ("PSUBSCRIBE", NonEmpty::new("patterns", patterns)),
        )
        .await
    }

    #[doc = " Posts a message to the given channel."]
    #[doc = ""]
    #[doc = " PUBLISH channel message"]
    #[doc = " https://valkey.io/commands/publish/"]
    #[allow(async_fn_in_trait)]
    async fn publish(
        connection: &Connection,
        channel: String,
        message: String,
    ) -> Result<Integer, Error> {
        query(connection, ("PUBLISH", channel, message)).await
    }

    #[doc = " Lists the currently active channels."]
    #[doc = ""]
    #[doc = " PUBSUB CHANNELS [ pattern ]"]
    #[doc = " https://valkey.io/commands/pubsub-channels/"]
    #[allow(async_fn_in_trait)]
    async fn pubsub_channels(
        connection: &Connection,
        pattern: Option<String>,
    ) -> Result<Vec<String>, Error> {
        query(connection, ("PUBSUB", "CHANNELS", pattern)).await
    }

    #[doc = " Returns the number of unique patterns that are subscribed to by clients."]
    #[doc = ""]
    #[doc = " PUBSUB NUMPAT"]
    #[doc = " https://valkey.io/commands/pubsub-numpat/"]
    #[allow(async_fn_in_trait)]
    async fn pubsub_numpat(connection: &Connection) -> Result<Integer, Error> {
        query(connection, ("PUBSUB", "NUMPAT")).await
    }

    #[doc = " Returns the number of subscribers (exclusive of clients subscribed to patterns) for the specified channels."]
    #[doc = ""]
    #[doc = " PUBSUB NUMSUB [ channel ] [ [ channel ] ... ]"]
    #[doc = " https://valkey.io/commands/pubsub-numsub/"]
    #[allow(async_fn_in_trait)]
    async fn pubsub_numsub(
        connection: &Connection,
        channels: Option<Vec<String>>,
    ) -> Result<Map<String, Integer>, Error> {
        query(
            connection,
            ("PUBSUB", "NUMSUB", NonEmpty::optional("channels", channels)),
        )
        .await
    }

    #[doc = " Lists the currently active shard channels."]
    #[doc = ""]
    #[doc = " PUBSUB SHARDCHANNELS [ pattern ]"]
    #[doc = " https://valkey.io/commands/pubsub-shardchannels/"]
    #[allow(async_fn_in_trait)]
    async fn pubsub_shardchannels(
        connection: &Connection,
        pattern: Option<String>,
    ) -> Result<Vec<String>, Error> {
        query(connection, ("PUBSUB", "SHARDCHANNELS", pattern)).await
    }

    #[doc = " Returns the number of subscribers for the specified shard channels."]
    #[doc = ""]
    #[doc = " PUBSUB SHARDNUMSUB [ shardchannel [ shardchannel ... ] ]"]
    #[doc = " https://valkey.io/commands/pubsub-shardnumsub/"]
    #[allow(async_fn_in_trait)]
    async fn pubsub_shardnumsub(
        connection: &Connection,
        shardchannels: Option<Vec<String>>,
    ) -> Result<Map<String, Integer>, Error> {
        query(
            connection,
            (
                "PUBSUB",
                "SHARDNUMSUB",
                NonEmpty::optional("shardchannels", shardchannels),
            ),
        )
        .await
    }

    #[doc = " Runs the Lua script, returning its result, the keys it accesses and its arguments are"]
    #[doc = " `KEYS` and `ARGV` in the script."]
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
        args: Option<Vec<String>>,
    ) -> Result<Value, Error> {
        query(connection, ("EVAL", script, KeysAndArgs(keys, args))).await
    }

    #[doc = " Runs the Lua script, which may not write, returning its result, see `eval`."]
    #[doc = ""]
    #[doc = " EVAL_RO script numkeys [ key [ key ... ] ] [ arg [ arg ... ] ]"]
    #[doc = " https://valkey.io/commands/eval_ro/"]
    #[allow(async_fn_in_trait)]
    async fn eval_ro(
        connection: &Connection,
        script: String,
        keys: Option<Vec<String>>,
        args: Option<Vec<String>>,
    ) -> Result<Value, Error> {
        query(connection, ("EVAL_RO", script, KeysAndArgs(keys, args))).await
    }

    #[doc = " Runs the Lua script cached with the SHA1 digest, see `script-load`, returning its result, see"]
    #[doc = " `eval`."]
    #[doc = ""]
    #[doc = " EVALSHA sha1 numkeys [ key [ key ... ] ] [ arg [ arg ... ] ]"]
    #[doc = " https://valkey.io/commands/evalsha/"]
    #[allow(async_fn_in_trait)]
    async fn evalsha(
        connection: &Connection,
        sha1: String,
        keys: Option<Vec<String>>,
        args: Option<Vec<String>>,
    ) -> Result<Value, Error> {
        query(connection, ("EVALSHA", sha1, KeysAndArgs(keys, args))).await
    }

    #[doc = " Runs the Lua script cached with the SHA1 digest, which may not write, returning its result,"]
    #[doc = " see `eval`."]
    #[doc = ""]
    #[doc = " EVALSHA_RO sha1 numkeys [ key [ key ... ] ] [ arg [ arg ... ] ]"]
    #[doc = " https://valkey.io/commands/evalsha_ro/"]
    #[allow(async_fn_in_trait)]
    async fn evalsha_ro(
        connection: &Connection,
        sha1: String,
        keys: Option<Vec<String>>,
        args: Option<Vec<String>>,
    ) -> Result<Value, Error> {
        query(connection, ("EVALSHA_RO", sha1, KeysAndArgs(keys, args))).await
    }

    #[doc = " Calls the function, see `function-load`, returning its result, the keys it accesses and its"]
    #[doc = " arguments are passed to it."]
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
        args: Option<Vec<String>>,
    ) -> Result<Value, Error> {
        query(connection, ("FCALL", function, KeysAndArgs(keys, args))).await
    }

    #[doc = " Calls the function, which must be flagged `no-writes`, returning its result, see `fcall`."]
    #[doc = ""]
    #[doc = " FCALL_RO function numkeys [ key [ key ... ] ] [ arg [ arg ... ] ]"]
    #[doc = " https://valkey.io/commands/fcall_ro/"]
    #[allow(async_fn_in_trait)]
    async fn fcall_ro(
        connection: &Connection,
        function: String,
        keys: Option<Vec<String>>,
        args: Option<Vec<String>>,
    ) -> Result<Value, Error> {
        query(connection, ("FCALL_RO", function, KeysAndArgs(keys, args))).await
    }

    #[doc = " Deletes the library and its functions."]
    #[doc = ""]
    #[doc = " FUNCTION DELETE library-name"]
    #[doc = " https://valkey.io/commands/function-delete/"]
    #[allow(async_fn_in_trait)]
    async fn function_delete(connection: &Connection, library_name: String) -> Result<(), Error> {
        query(connection, ("FUNCTION", "DELETE", library_name)).await
    }

    #[doc = " Deletes all the libraries, in the mode, the server's `lazyfree-lazy-user-flush` without one."]
    #[doc = ""]
    #[doc = " FUNCTION FLUSH [ ASYNC | SYNC ]"]
    #[doc = " https://valkey.io/commands/function-flush/"]
    #[allow(async_fn_in_trait)]
    async fn function_flush(connection: &Connection, mode: Option<FlushMode>) -> Result<(), Error> {
        query(connection, ("FUNCTION", "FLUSH", mode)).await
    }

    #[doc = " Kills the function that is running, as long as it has not written."]
    #[doc = ""]
    #[doc = " FUNCTION KILL"]
    #[doc = " https://valkey.io/commands/function-kill/"]
    #[allow(async_fn_in_trait)]
    async fn function_kill(connection: &Connection) -> Result<(), Error> {
        query(connection, ("FUNCTION", "KILL")).await
    }

    #[doc = " The libraries, those with names matching the glob-style pattern with one."]
    #[doc = ""]
    #[doc = " FUNCTION LIST [ LIBRARYNAME library-name-pattern ] [ WITHCODE ]"]
    #[doc = " https://valkey.io/commands/function-list/"]
    #[allow(async_fn_in_trait)]
    async fn function_list(
        connection: &Connection,
        library_name_pattern: Option<String>,
        withcode: bool,
    ) -> Result<Vec<Library>, Error> {
        query(
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

    #[doc = " Loads the library from its source code, replacing a library of the same name with"]
    #[doc = " `replace`, returning its name."]
    #[doc = ""]
    #[doc = " FUNCTION LOAD [ REPLACE ] function-code"]
    #[doc = " https://valkey.io/commands/function-load/"]
    #[allow(async_fn_in_trait)]
    async fn function_load(
        connection: &Connection,
        replace: bool,
        function_code: String,
    ) -> Result<String, Error> {
        query(
            connection,
            ("FUNCTION", "LOAD", Flag("REPLACE", replace), function_code),
        )
        .await
    }

    #[doc = " The function that is running, and the libraries and functions of each engine."]
    #[doc = ""]
    #[doc = " FUNCTION STATS"]
    #[doc = " https://valkey.io/commands/function-stats/"]
    #[allow(async_fn_in_trait)]
    async fn function_stats(connection: &Connection) -> Result<FunctionStatsReply, Error> {
        query(connection, ("FUNCTION", "STATS")).await
    }

    #[doc = " Whether each of the scripts is cached, by SHA1 digest."]
    #[doc = ""]
    #[doc = " SCRIPT EXISTS sha1 [ sha1 ... ]"]
    #[doc = " https://valkey.io/commands/script-exists/"]
    #[allow(async_fn_in_trait)]
    async fn script_exists(
        connection: &Connection,
        sha1s: Vec<String>,
    ) -> Result<Vec<bool>, Error> {
        query(
            connection,
            ("SCRIPT", "EXISTS", NonEmpty::new("sha1s", sha1s)),
        )
        .await
    }

    #[doc = " Removes all the scripts from the cache, in the mode, the server's `lazyfree-lazy-user-flush`"]
    #[doc = " without one."]
    #[doc = ""]
    #[doc = " SCRIPT FLUSH [ ASYNC | SYNC ]"]
    #[doc = " https://valkey.io/commands/script-flush/"]
    #[allow(async_fn_in_trait)]
    async fn script_flush(connection: &Connection, mode: Option<FlushMode>) -> Result<(), Error> {
        query(connection, ("SCRIPT", "FLUSH", mode)).await
    }

    #[doc = " Kills the script that is running, as long as it has not written."]
    #[doc = ""]
    #[doc = " SCRIPT KILL"]
    #[doc = " https://valkey.io/commands/script-kill/"]
    #[allow(async_fn_in_trait)]
    async fn script_kill(connection: &Connection) -> Result<(), Error> {
        query(connection, ("SCRIPT", "KILL")).await
    }

    #[doc = " Caches the script without running it, returning its SHA1 digest, see `evalsha`."]
    #[doc = ""]
    #[doc = " SCRIPT LOAD script"]
    #[doc = " https://valkey.io/commands/script-load/"]
    #[allow(async_fn_in_trait)]
    async fn script_load(connection: &Connection, script: String) -> Result<String, Error> {
        query(connection, ("SCRIPT", "LOAD", script)).await
    }

    #[doc = " The source of the cached script with the SHA1 digest."]
    #[doc = ""]
    #[doc = " SCRIPT SHOW sha1"]
    #[doc = " https://valkey.io/commands/script-show/"]
    #[allow(async_fn_in_trait)]
    async fn script_show(connection: &Connection, sha1: String) -> Result<String, Error> {
        query(connection, ("SCRIPT", "SHOW", sha1)).await
    }

    #[doc = " Unsubscribes the client from the given patterns, or from all of them if none is given."]
    #[doc = ""]
    #[doc = " PUNSUBSCRIBE [ pattern [ pattern ... ] ]"]
    #[doc = " https://valkey.io/commands/punsubscribe/"]
    #[allow(async_fn_in_trait)]
    async fn punsubscribe(
        connection: &Connection,
        patterns: Option<Vec<String>>,
    ) -> Result<(), Error> {
        emit(
            connection,
            ("PUNSUBSCRIBE", NonEmpty::optional("patterns", patterns)),
        )
        .await
    }

    #[doc = " Posts a message to the given shard channel."]
    #[doc = ""]
    #[doc = " SPUBLISH shardchannel message"]
    #[doc = " https://valkey.io/commands/spublish/"]
    #[allow(async_fn_in_trait)]
    async fn spublish(
        connection: &Connection,
        shardchannel: String,
        message: String,
    ) -> Result<Integer, Error> {
        query(connection, ("SPUBLISH", shardchannel, message)).await
    }

    #[doc = " Subscribes the client to the specified shard channels."]
    #[doc = ""]
    #[doc = " SSUBSCRIBE shardchannel [ shardchannel ... ]"]
    #[doc = " https://valkey.io/commands/ssubscribe/"]
    #[allow(async_fn_in_trait)]
    async fn ssubscribe(connection: &Connection, shardchannels: Vec<String>) -> Result<(), Error> {
        emit(
            connection,
            ("SSUBSCRIBE", NonEmpty::new("shardchannels", shardchannels)),
        )
        .await
    }

    #[doc = " Subscribes the client to the specified channels."]
    #[doc = ""]
    #[doc = " SUBSCRIBE channel [ channel ... ]"]
    #[doc = " https://valkey.io/commands/subscribe/"]
    #[allow(async_fn_in_trait)]
    async fn subscribe(connection: &Connection, channels: Vec<String>) -> Result<(), Error> {
        emit(
            connection,
            ("SUBSCRIBE", NonEmpty::new("channels", channels)),
        )
        .await
    }

    #[doc = " Unsubscribes the client from the given shard channels, or from all of them if none is given."]
    #[doc = ""]
    #[doc = " SUNSUBSCRIBE [ shardchannel ] [ [ shardchannel ] ... ]"]
    #[doc = " https://valkey.io/commands/sunsubscribe/"]
    #[allow(async_fn_in_trait)]
    async fn sunsubscribe(
        connection: &Connection,
        shardchannels: Option<Vec<String>>,
    ) -> Result<(), Error> {
        emit(
            connection,
            (
                "SUNSUBSCRIBE",
                NonEmpty::optional("shardchannels", shardchannels),
            ),
        )
        .await
    }

    #[doc = " Unsubscribes the client from the given channels, or from all of them if none is given."]
    #[doc = ""]
    #[doc = " UNSUBSCRIBE [ channel ] [ [ channel ] ... ]"]
    #[doc = " https://valkey.io/commands/unsubscribe/"]
    #[allow(async_fn_in_trait)]
    async fn unsubscribe(
        connection: &Connection,
        channels: Option<Vec<String>>,
    ) -> Result<(), Error> {
        emit(
            connection,
            ("UNSUBSCRIBE", NonEmpty::optional("channels", channels)),
        )
        .await
    }

    #[doc = " Appends the value at the end of the string."]
    #[doc = ""]
    #[doc = " APPEND key value"]
    #[doc = " https://valkey.io/commands/append/"]
    #[allow(async_fn_in_trait)]
    async fn append(connection: &Connection, key: String, value: String) -> Result<Integer, Error> {
        query(connection, ("APPEND", key, value)).await
    }

    #[doc = " Decrements the number stored at key by one."]
    #[doc = ""]
    #[doc = " DECR key"]
    #[doc = " https://valkey.io/commands/decr/"]
    #[allow(async_fn_in_trait)]
    async fn decr(connection: &Connection, key: String) -> Result<Integer, Error> {
        query(connection, ("DECR", key)).await
    }

    #[doc = " Decrements the number stored at key by decrement."]
    #[doc = ""]
    #[doc = " DECRBY key decrement"]
    #[doc = " https://valkey.io/commands/decrby/"]
    #[allow(async_fn_in_trait)]
    async fn decrby(
        connection: &Connection,
        key: String,
        decrement: Integer,
    ) -> Result<Integer, Error> {
        query(connection, ("DECRBY", key, decrement)).await
    }

    #[doc = " Deletes the key if its value matches the given string."]
    #[doc = ""]
    #[doc = " DELIFEQ key value"]
    #[doc = " https://valkey.io/commands/delifeq/"]
    #[allow(async_fn_in_trait)]
    async fn delifeq(connection: &Connection, key: String, value: String) -> Result<bool, Error> {
        query(connection, ("DELIFEQ", key, value)).await
    }

    #[doc = " Get the value of key."]
    #[doc = ""]
    #[doc = " GET key"]
    #[doc = " https://valkey.io/commands/get/"]
    #[allow(async_fn_in_trait)]
    async fn get(connection: &Connection, key: String) -> Result<Option<String>, Error> {
        query(connection, ("GET", key)).await
    }

    #[doc = " Get the value of key and delete the key."]
    #[doc = ""]
    #[doc = " GETDEL key"]
    #[doc = " https://valkey.io/commands/getdel/"]
    #[allow(async_fn_in_trait)]
    async fn getdel(connection: &Connection, key: String) -> Result<Option<String>, Error> {
        query(connection, ("GETDEL", key)).await
    }

    #[doc = " Get the value of key and optionally set its expiration."]
    #[doc = ""]
    #[doc = " GETEX key [ EX seconds | PX milliseconds | EXAT unix-time-seconds | PXAT unix-time-milliseconds | PERSIST ]"]
    #[doc = " https://valkey.io/commands/getex/"]
    #[allow(async_fn_in_trait)]
    async fn getex(
        connection: &Connection,
        key: String,
        ttl: Option<GetexTtl>,
    ) -> Result<Option<String>, Error> {
        query(connection, ("GETEX", key, ttl)).await
    }

    #[doc = " Returns the substring of the string value stored at key, determined by the offsets start and end."]
    #[doc = ""]
    #[doc = " GETRANGE key start end"]
    #[doc = " https://valkey.io/commands/getrange/"]
    #[allow(async_fn_in_trait)]
    async fn getrange(
        connection: &Connection,
        key: String,
        start: Integer,
        end: Integer,
    ) -> Result<String, Error> {
        query(connection, ("GETRANGE", key, start, end)).await
    }

    #[doc = " Atomically sets key to value and returns the old value stored at key."]
    #[doc = ""]
    #[doc = " GETSET key value"]
    #[doc = " https://valkey.io/commands/getset/"]
    #[allow(async_fn_in_trait)]
    async fn getset(
        connection: &Connection,
        key: String,
        value: String,
    ) -> Result<Option<String>, Error> {
        query(connection, ("GETSET", key, value)).await
    }

    #[doc = " Increments the number stored at key by one."]
    #[doc = ""]
    #[doc = " INCR key"]
    #[doc = " https://valkey.io/commands/incr/"]
    #[allow(async_fn_in_trait)]
    async fn incr(connection: &Connection, key: String) -> Result<Integer, Error> {
        query(connection, ("INCR", key)).await
    }

    #[doc = " Increments the number stored at key by increment."]
    #[doc = ""]
    #[doc = " INCRBY key increment"]
    #[doc = " https://valkey.io/commands/incrby/"]
    #[allow(async_fn_in_trait)]
    async fn incrby(
        connection: &Connection,
        key: String,
        increment: Integer,
    ) -> Result<Integer, Error> {
        query(connection, ("INCRBY", key, increment)).await
    }

    #[doc = " Increment the string representing a floating point number stored at key by the specified increment."]
    #[doc = ""]
    #[doc = " INCRBYFLOAT key increment"]
    #[doc = " https://valkey.io/commands/incrbyfloat/"]
    #[allow(async_fn_in_trait)]
    async fn incrbyfloat(
        connection: &Connection,
        key: String,
        increment: Double,
    ) -> Result<Double, Error> {
        query(connection, ("INCRBYFLOAT", key, increment)).await
    }

    #[doc = " The LCS command implements the longest common subsequence algorithm."]
    #[doc = ""]
    #[doc = " See also `lcs-len` and `lcs-idx`"]
    #[doc = ""]
    #[doc = " LCS key1 key2"]
    #[doc = " https://valkey.io/commands/lcs/"]
    #[allow(async_fn_in_trait)]
    async fn lcs(connection: &Connection, key1: String, key2: String) -> Result<String, Error> {
        query(connection, ("LCS", key1, key2)).await
    }

    #[doc = " The LCS command implements the longest common subsequence algorithm."]
    #[doc = ""]
    #[doc = " See also `lcs` and `lcs-idx`"]
    #[doc = ""]
    #[doc = " LCS key1 key2 LEN"]
    #[doc = " https://valkey.io/commands/lcs/"]
    #[allow(async_fn_in_trait)]
    async fn lcs_len(
        connection: &Connection,
        key1: String,
        key2: String,
    ) -> Result<Integer, Error> {
        query(connection, ("LCS", key1, key2, "LEN")).await
    }

    #[doc = " The LCS command implements the longest common subsequence algorithm."]
    #[doc = ""]
    #[doc = " See also `lcs` and `lcs-len`"]
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
        query(
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

    #[doc = " Returns the values of all specified keys."]
    #[doc = ""]
    #[doc = " MGET key [ key ... ]"]
    #[doc = " https://valkey.io/commands/mget/"]
    #[allow(async_fn_in_trait)]
    async fn mget(
        connection: &Connection,
        keys: Vec<String>,
    ) -> Result<Vec<Option<String>>, Error> {
        query(connection, ("MGET", NonEmpty::new("keys", keys))).await
    }

    #[doc = " Sets the given keys to their respective values."]
    #[doc = ""]
    #[doc = " MSET key value [ key value ... ]"]
    #[doc = " https://valkey.io/commands/mset/"]
    #[allow(async_fn_in_trait)]
    async fn mset(
        connection: &Connection,
        keys_and_values: Vec<(String, String)>,
    ) -> Result<(), Error> {
        query(
            connection,
            ("MSET", NonEmpty::new("keys-and-values", keys_and_values)),
        )
        .await
    }

    #[doc = " Sets multiple key-value pairs while also assigning a shared expiration time to all the keys atomically."]
    #[doc = ""]
    #[doc = " MSETEX numkeys key value [ key value ... ] [ NX | XX ] [ EX seconds | PX milliseconds | EXAT unix-time-seconds | PXAT unix-time-milliseconds | KEEPTTL ]"]
    #[doc = " https://valkey.io/commands/msetex/"]
    #[allow(async_fn_in_trait)]
    async fn msetex(
        connection: &Connection,
        keys_and_values: Vec<(String, String)>,
        precondition: Option<MsetexPrecondition>,
        ttl: Option<MsetexTtl>,
    ) -> Result<bool, Error> {
        query(
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

    #[doc = " Sets the given keys to their respective values."]
    #[doc = ""]
    #[doc = " MSETNX key value [ key value ... ]"]
    #[doc = " https://valkey.io/commands/msetnx/"]
    #[allow(async_fn_in_trait)]
    async fn msetnx(
        connection: &Connection,
        keys_and_values: Vec<(String, String)>,
    ) -> Result<bool, Error> {
        query(
            connection,
            ("MSETNX", NonEmpty::new("keys-and-values", keys_and_values)),
        )
        .await
    }

    #[doc = " Set key to hold the string value with expiration in milliseconds."]
    #[doc = ""]
    #[doc = " PSETEX key milliseconds value"]
    #[doc = " https://valkey.io/commands/psetex/"]
    #[allow(async_fn_in_trait)]
    async fn psetex(
        connection: &Connection,
        key: String,
        milliseconds: DurationMilliseconds,
        value: String,
    ) -> Result<(), Error> {
        query(connection, ("PSETEX", key, milliseconds, value)).await
    }

    #[doc = " Set key to hold the string value."]
    #[doc = ""]
    #[doc = " Returns false when the precondition is not met, the key is unchanged."]
    #[doc = ""]
    #[doc = " SET key value [ NX | XX | IFEQ comparison-value | IFNE comparison-not-equal ] [ EX seconds | PX milliseconds | EXAT unix-time-seconds | PXAT unix-time-milliseconds | KEEPTTL ]"]
    #[doc = " https://valkey.io/commands/set/"]
    #[allow(async_fn_in_trait)]
    async fn set(
        connection: &Connection,
        key: String,
        value: String,
        precondition: Option<SetPrecondition>,
        ttl: Option<SetTtl>,
    ) -> Result<bool, Error> {
        // OK once set, null when the precondition is not met
        let reply: Option<()> = query(connection, ("SET", key, value, precondition, ttl)).await?;
        Ok(reply.is_some())
    }

    #[doc = " Set key to hold the string value. Returning the previous value."]
    #[doc = ""]
    #[doc = " SET key value [ NX | XX | IFEQ comparison-value | IFNE comparison-not-equal ] GET [ EX seconds | PX milliseconds | EXAT unix-time-seconds | PXAT unix-time-milliseconds | KEEPTTL ]"]
    #[doc = " https://valkey.io/commands/set/"]
    #[allow(async_fn_in_trait)]
    async fn set_get(
        connection: &Connection,
        key: String,
        value: String,
        precondition: Option<SetPrecondition>,
        ttl: Option<SetTtl>,
    ) -> Result<Option<String>, Error> {
        query(connection, ("SET", key, value, precondition, "GET", ttl)).await
    }

    #[doc = " Set key to hold the string value with expiration in seconds."]
    #[doc = ""]
    #[doc = " SETEX key seconds value"]
    #[doc = " https://valkey.io/commands/setex/"]
    #[allow(async_fn_in_trait)]
    async fn setex(
        connection: &Connection,
        key: String,
        seconds: DurationSeconds,
        value: String,
    ) -> Result<(), Error> {
        query(connection, ("SETEX", key, seconds, value)).await
    }

    #[doc = " Set key to hold string value if key does not exist."]
    #[doc = ""]
    #[doc = " SETNX key value"]
    #[doc = " https://valkey.io/commands/setnx/"]
    #[allow(async_fn_in_trait)]
    async fn setnx(connection: &Connection, key: String, value: String) -> Result<bool, Error> {
        query(connection, ("SETNX", key, value)).await
    }

    #[doc = " Overwrites part of the string stored at key, starting at the specified offset, for the entire length of value."]
    #[doc = ""]
    #[doc = " SETRANGE key offset value"]
    #[doc = " https://valkey.io/commands/setrange/"]
    #[allow(async_fn_in_trait)]
    async fn setrange(
        connection: &Connection,
        key: String,
        offset: u32,
        value: String,
    ) -> Result<Integer, Error> {
        query(connection, ("SETRANGE", key, offset, value)).await
    }

    #[doc = " Returns the length of the string value stored at key."]
    #[doc = ""]
    #[doc = " STRLEN key"]
    #[doc = " hthttps://valkey.io/commands/strlen/"]
    #[allow(async_fn_in_trait)]
    async fn strlen(connection: &Connection, key: String) -> Result<Integer, Error> {
        query(connection, ("STRLEN", key)).await
    }

    #[doc = " Returns the substring of the string value stored at key, determined by the offsets start and end."]
    #[doc = ""]
    #[doc = " SUBSTR key start end"]
    #[doc = " https://valkey.io/commands/substr/"]
    #[allow(async_fn_in_trait)]
    async fn substr(
        connection: &Connection,
        key: String,
        start: Integer,
        end: Integer,
    ) -> Result<String, Error> {
        query(connection, ("SUBSTR", key, start, end)).await
    }

    #[doc = " Removes one or multiple messages from the Pending Entries List (PEL) of a stream consumer group."]
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
        query(connection, ("XACK", key, group, NonEmpty::new("ids", ids))).await
    }

    #[doc = " Appends the specified stream entry to the stream at the specified key."]
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
        field_and_values: Vec<(String, String)>,
    ) -> Result<Option<StreamId>, Error> {
        query(
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

    #[doc = " Transfers ownership of pending stream entries that match the specified criteria."]
    #[doc = ""]
    #[doc = " See also `xautoclaim-justid`"]
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
            Vec<(StreamId, Map<String, String>)>,
            Vec<StreamId>,
        ),
        Error,
    > {
        let (next, claimed, deleted): (OrZeroId, _, _) = query(
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

    #[doc = " Transfers ownership of pending stream entries that match the specified criteria."]
    #[doc = ""]
    #[doc = " See also `xautoclaim`"]
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
        let (next, claimed, deleted): (OrZeroId, _, _) = query(
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

    #[doc = " Changes the ownership of a pending message, so that the new owner is the consumer specified as the command argument."]
    #[doc = ""]
    #[doc = " Also see `xclaim-justid`"]
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
    ) -> Result<Vec<(StreamId, Map<String, String>)>, Error> {
        query(
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

    #[doc = " Changes the ownership of a pending message, so that the new owner is the consumer specified as the command argument."]
    #[doc = ""]
    #[doc = " Also see `xclaim`"]
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
        query(
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

    #[doc = " Removes the specified entries from a stream, and returns the number of entries deleted."]
    #[doc = ""]
    #[doc = " XDEL key ID [ ID ... ]"]
    #[doc = " https://valkey.io/commands/xdel/"]
    #[allow(async_fn_in_trait)]
    async fn xdel(
        connection: &Connection,
        key: String,
        ids: Vec<StreamId>,
    ) -> Result<Integer, Error> {
        query(connection, ("XDEL", key, NonEmpty::new("ids", ids))).await
    }

    #[doc = " Create a new consumer group uniquely identified by <groupname> for the stream stored at <key>."]
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
        query(
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

    #[doc = " Create a consumer named <consumername> in the consumer group <groupname> of the stream that\'s stored at <key>."]
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
        query(
            connection,
            ("XGROUP", "CREATECONSUMER", key, group, consumer),
        )
        .await
    }

    #[doc = " The XGROUP DELCONSUMER command deletes a consumer from the consumer group."]
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
        query(connection, ("XGROUP", "DELCONSUMER", key, group, consumer)).await
    }

    #[doc = " Completely destroys the consumer group."]
    #[doc = ""]
    #[doc = " XGROUP DESTROY key group"]
    #[doc = " https://valkey.io/commands/xgroup-destroy/"]
    #[allow(async_fn_in_trait)]
    async fn xgroup_destroy(
        connection: &Connection,
        key: String,
        group: String,
    ) -> Result<bool, Error> {
        query(connection, ("XGROUP", "DESTROY", key, group)).await
    }

    #[doc = " Set the last delivered ID for a consumer group."]
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
        query(
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

    #[doc = " Consumers that belong to the <groupname> consumer group of the stream stored at <key>."]
    #[doc = ""]
    #[doc = " XINFO CONSUMERS key group"]
    #[doc = " https://valkey.io/commands/xinfo-consumers/"]
    #[allow(async_fn_in_trait)]
    async fn xinfo_consumers(
        connection: &Connection,
        key: String,
        group: String,
    ) -> Result<Vec<XinfoConsumer>, Error> {
        query(connection, ("XINFO", "CONSUMERS", key, group)).await
    }

    #[doc = " Consumer groups of the stream stored at <key>."]
    #[doc = ""]
    #[doc = " XINFO GROUPS key"]
    #[doc = " https://valkey.io/commands/xinfo-groups/"]
    #[allow(async_fn_in_trait)]
    async fn xinfo_groups(connection: &Connection, key: String) -> Result<Vec<XinfoGroup>, Error> {
        query(connection, ("XINFO", "GROUPS", key)).await
    }

    #[doc = " Information about the stream stored at <key>."]
    #[doc = ""]
    #[doc = " See also `xinfo-stream-full`"]
    #[doc = ""]
    #[doc = " XINFO STREAM key"]
    #[doc = " https://valkey.io/commands/xinfo-stream/"]
    #[allow(async_fn_in_trait)]
    async fn xinfo_stream(connection: &Connection, key: String) -> Result<XinfoStreamReply, Error> {
        query(connection, ("XINFO", "STREAM", key)).await
    }

    #[doc = " Information about the stream stored at <key>."]
    #[doc = ""]
    #[doc = " See also `xinfo-stream`"]
    #[doc = ""]
    #[doc = " XINFO STREAM key FULL [ COUNT count ]"]
    #[doc = " https://valkey.io/commands/xinfo-stream/"]
    #[allow(async_fn_in_trait)]
    async fn xinfo_stream_full(
        connection: &Connection,
        key: String,
        count: Option<Integer>,
    ) -> Result<XinfoStreamReplyFull, Error> {
        query(
            connection,
            ("XINFO", "STREAM", key, "FULL", NamedOption("COUNT", count)),
        )
        .await
    }

    #[doc = " Number of entries inside a stream."]
    #[doc = ""]
    #[doc = " XLEN key"]
    #[doc = " https://valkey.io/commands/xlen/"]
    #[allow(async_fn_in_trait)]
    async fn xlen(connection: &Connection, key: String) -> Result<Integer, Error> {
        query(connection, ("XLEN", key)).await
    }

    #[doc = " List of pending messages."]
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
        query(connection, ("XPENDING", key, group)).await
    }

    #[doc = " List of pending messages."]
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
        query(
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

    #[doc = " Stream entries matching a given range of IDs."]
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
    ) -> Result<Vec<(StreamId, Map<String, String>)>, Error> {
        query(
            connection,
            ("XRANGE", key, start, end, NamedOption("COUNT", count)),
        )
        .await
    }

    #[doc = " Read data from one or multiple streams, only returning entries with an ID greater than the last received ID reported by the caller."]
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
    ) -> Result<Option<Map<String, Vec<(StreamId, Map<String, String>)>>>, Error> {
        query(
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

    #[doc = " Special version of the XREAD command with support for consumer groups."]
    #[doc = ""]
    #[doc = " The fields of an entry are none when it was deleted after it was delivered, e.g. when reading"]
    #[doc = " the pending entries with the ID `0`."]
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
    ) -> Result<Option<Map<String, Vec<(StreamId, Option<Map<String, String>>)>>>, Error> {
        query(
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

    #[doc = " XRANGE, but returning the entries in reverse order."]
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
    ) -> Result<Vec<(StreamId, Map<String, String>)>, Error> {
        query(
            connection,
            ("XREVRANGE", key, end, start, NamedOption("COUNT", count)),
        )
        .await
    }

    #[doc = " Internal command. It is used by a Valkey primary to replicate the last delivered ID of streams."]
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
        query(
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

    #[doc = " Trims the stream by evicting older entries."]
    #[doc = ""]
    #[doc = " XTRIM key 〈 MAXLEN | MINID 〉 [ = | ~ ] threshold [ LIMIT count ]"]
    #[doc = " https://valkey.io/commands/xtrim/"]
    #[allow(async_fn_in_trait)]
    async fn xtrim(
        connection: &Connection,
        key: String,
        trim: StreamTrim,
    ) -> Result<Integer, Error> {
        query(connection, ("XTRIM", key, trim)).await
    }

    #[doc = " Adds the members to the set stored at key, returning how many were not already members."]
    #[doc = ""]
    #[doc = " SADD key member [ member ... ]"]
    #[doc = " https://valkey.io/commands/sadd/"]
    #[allow(async_fn_in_trait)]
    async fn sadd(
        connection: &Connection,
        key: String,
        members: Vec<String>,
    ) -> Result<Integer, Error> {
        query(connection, ("SADD", key, NonEmpty::new("members", members))).await
    }

    #[doc = " The number of members in the set stored at key."]
    #[doc = ""]
    #[doc = " SCARD key"]
    #[doc = " https://valkey.io/commands/scard/"]
    #[allow(async_fn_in_trait)]
    async fn scard(connection: &Connection, key: String) -> Result<Integer, Error> {
        query(connection, ("SCARD", key)).await
    }

    #[doc = " The members of the first set that are not in any of the others."]
    #[doc = ""]
    #[doc = " SDIFF key [ key ... ]"]
    #[doc = " https://valkey.io/commands/sdiff/"]
    #[allow(async_fn_in_trait)]
    async fn sdiff(connection: &Connection, keys: Vec<String>) -> Result<Vec<String>, Error> {
        query(connection, ("SDIFF", NonEmpty::new("keys", keys))).await
    }

    #[doc = " Stores the members of the first set that are not in any of the others, returning how many."]
    #[doc = ""]
    #[doc = " SDIFFSTORE destination key [ key ... ]"]
    #[doc = " https://valkey.io/commands/sdiffstore/"]
    #[allow(async_fn_in_trait)]
    async fn sdiffstore(
        connection: &Connection,
        destination: String,
        keys: Vec<String>,
    ) -> Result<Integer, Error> {
        query(
            connection,
            ("SDIFFSTORE", destination, NonEmpty::new("keys", keys)),
        )
        .await
    }

    #[doc = " The members in all of the sets."]
    #[doc = ""]
    #[doc = " SINTER key [ key ... ]"]
    #[doc = " https://valkey.io/commands/sinter/"]
    #[allow(async_fn_in_trait)]
    async fn sinter(connection: &Connection, keys: Vec<String>) -> Result<Vec<String>, Error> {
        query(connection, ("SINTER", NonEmpty::new("keys", keys))).await
    }

    #[doc = " The number of members in all of the sets, counting stops at the limit."]
    #[doc = ""]
    #[doc = " SINTERCARD numkeys key [ key ... ] [ LIMIT limit ]"]
    #[doc = " https://valkey.io/commands/sintercard/"]
    #[allow(async_fn_in_trait)]
    async fn sintercard(
        connection: &Connection,
        keys: Vec<String>,
        limit: Option<Integer>,
    ) -> Result<Integer, Error> {
        query(
            connection,
            (
                "SINTERCARD",
                NumKeys(NonEmpty::new("keys", keys)),
                NamedOption("LIMIT", limit),
            ),
        )
        .await
    }

    #[doc = " Stores the members in all of the sets, returning how many."]
    #[doc = ""]
    #[doc = " SINTERSTORE destination key [ key ... ]"]
    #[doc = " https://valkey.io/commands/sinterstore/"]
    #[allow(async_fn_in_trait)]
    async fn sinterstore(
        connection: &Connection,
        destination: String,
        keys: Vec<String>,
    ) -> Result<Integer, Error> {
        query(
            connection,
            ("SINTERSTORE", destination, NonEmpty::new("keys", keys)),
        )
        .await
    }

    #[doc = " Whether the member is in the set stored at key, false when the key does not exist."]
    #[doc = ""]
    #[doc = " See also `sismember-xx`."]
    #[doc = ""]
    #[doc = " SISMEMBER key member"]
    #[doc = " https://valkey.io/commands/sismember/"]
    #[allow(async_fn_in_trait)]
    async fn sismember(
        connection: &Connection,
        key: String,
        member: String,
    ) -> Result<bool, Error> {
        query(connection, ("SISMEMBER", key, member)).await
    }

    #[doc = " Whether the member is in the set stored at key, none when the key does not exist."]
    #[doc = ""]
    #[doc = " See also `sismember`."]
    #[doc = ""]
    #[doc = " SISMEMBER key member XX"]
    #[doc = " https://valkey.io/commands/sismember/"]
    #[allow(async_fn_in_trait)]
    async fn sismember_xx(
        connection: &Connection,
        key: String,
        member: String,
    ) -> Result<Option<bool>, Error> {
        // -1 when the key does not exist
        let member: OrNever<bool> = query(connection, ("SISMEMBER", key, member, "XX")).await?;
        Ok(member.0)
    }

    #[doc = " The members of the set stored at key."]
    #[doc = ""]
    #[doc = " SMEMBERS key"]
    #[doc = " https://valkey.io/commands/smembers/"]
    #[allow(async_fn_in_trait)]
    async fn smembers(connection: &Connection, key: String) -> Result<Vec<String>, Error> {
        query(connection, ("SMEMBERS", key)).await
    }

    #[doc = " Whether each of the members is in the set stored at key."]
    #[doc = ""]
    #[doc = " SMISMEMBER key member [ member ... ]"]
    #[doc = " https://valkey.io/commands/smismember/"]
    #[allow(async_fn_in_trait)]
    async fn smismember(
        connection: &Connection,
        key: String,
        members: Vec<String>,
    ) -> Result<Vec<bool>, Error> {
        query(
            connection,
            ("SMISMEMBER", key, NonEmpty::new("members", members)),
        )
        .await
    }

    #[doc = " Moves the member from one set to another, false when it was not in the source set."]
    #[doc = ""]
    #[doc = " SMOVE source destination member"]
    #[doc = " https://valkey.io/commands/smove/"]
    #[allow(async_fn_in_trait)]
    async fn smove(
        connection: &Connection,
        source: String,
        destination: String,
        member: String,
    ) -> Result<bool, Error> {
        query(connection, ("SMOVE", source, destination, member)).await
    }

    #[doc = " Removes and returns random members of the set stored at key, up to the count."]
    #[doc = ""]
    #[doc = " Without a count, one member, or none when the key does not exist."]
    #[doc = ""]
    #[doc = " SPOP key [ count ]"]
    #[doc = " https://valkey.io/commands/spop/"]
    #[allow(async_fn_in_trait)]
    async fn spop(
        connection: &Connection,
        key: String,
        count: Option<Integer>,
    ) -> Result<Vec<String>, Error> {
        if count.is_some() {
            return query(connection, ("SPOP", key, count)).await;
        }
        // without a count the reply is the member itself, or null
        let member: Option<String> = query(connection, ("SPOP", key)).await?;
        Ok(member.into_iter().collect())
    }

    #[doc = " Random members of the set stored at key, up to the count."]
    #[doc = ""]
    #[doc = " Without a count, one member, or none when the key does not exist. A negative count may return"]
    #[doc = " the same member more than once, its absolute value is the number of members returned."]
    #[doc = ""]
    #[doc = " SRANDMEMBER key [ count ]"]
    #[doc = " https://valkey.io/commands/srandmember/"]
    #[allow(async_fn_in_trait)]
    async fn srandmember(
        connection: &Connection,
        key: String,
        count: Option<Integer>,
    ) -> Result<Vec<String>, Error> {
        if count.is_some() {
            return query(connection, ("SRANDMEMBER", key, count)).await;
        }
        // without a count the reply is the member itself, or null
        let member: Option<String> = query(connection, ("SRANDMEMBER", key)).await?;
        Ok(member.into_iter().collect())
    }

    #[doc = " Removes the members from the set stored at key, returning how many were members."]
    #[doc = ""]
    #[doc = " SREM key member [ member ... ]"]
    #[doc = " https://valkey.io/commands/srem/"]
    #[allow(async_fn_in_trait)]
    async fn srem(
        connection: &Connection,
        key: String,
        members: Vec<String>,
    ) -> Result<Integer, Error> {
        query(connection, ("SREM", key, NonEmpty::new("members", members))).await
    }

    #[doc = " Incrementally iterate over the members of the set stored at key."]
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
    ) -> Result<(Option<Cursor>, Vec<String>), Error> {
        let (cursor, items): (NextCursor, Vec<String>) = query(
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

    #[doc = " The members in any of the sets."]
    #[doc = ""]
    #[doc = " SUNION key [ key ... ]"]
    #[doc = " https://valkey.io/commands/sunion/"]
    #[allow(async_fn_in_trait)]
    async fn sunion(connection: &Connection, keys: Vec<String>) -> Result<Vec<String>, Error> {
        query(connection, ("SUNION", NonEmpty::new("keys", keys))).await
    }

    #[doc = " Stores the members in any of the sets, returning how many."]
    #[doc = ""]
    #[doc = " SUNIONSTORE destination key [ key ... ]"]
    #[doc = " https://valkey.io/commands/sunionstore/"]
    #[allow(async_fn_in_trait)]
    async fn sunionstore(
        connection: &Connection,
        destination: String,
        keys: Vec<String>,
    ) -> Result<Integer, Error> {
        query(
            connection,
            ("SUNIONSTORE", destination, NonEmpty::new("keys", keys)),
        )
        .await
    }

    #[doc = " Sorts the elements of the list, set or sorted set stored at key."]
    #[doc = ""]
    #[doc = " Each row is the element, or with `get`, the value of each pattern, none when its key does not"]
    #[doc = " exist."]
    #[doc = ""]
    #[doc = " SORT key [ BY pattern ] [ LIMIT offset count ] [ GET pattern ... ] [ ASC | DESC ] [ ALPHA ]"]
    #[doc = " https://valkey.io/commands/sort/"]
    #[allow(async_fn_in_trait)]
    async fn sort(
        connection: &Connection,
        key: String,
        opts: Option<SortOpts>,
    ) -> Result<Vec<Vec<Option<String>>>, Error> {
        let row = sort_row_len(&opts);
        let values: Vec<Option<String>> = query(connection, ("SORT", key, opts)).await?;
        Ok(values.chunks(row).map(<[_]>::to_vec).collect())
    }

    #[doc = " Sorts the elements of the list, set or sorted set stored at key, storing them as a list,"]
    #[doc = " returning how many."]
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
        query(connection, ("SORT", key, opts, "STORE", destination)).await
    }

    #[doc = " The read-only variant of `sort`, e.g. for replicas."]
    #[doc = ""]
    #[doc = " SORT_RO key [ BY pattern ] [ LIMIT offset count ] [ GET pattern ... ] [ ASC | DESC ] [ ALPHA ]"]
    #[doc = " https://valkey.io/commands/sort_ro/"]
    #[allow(async_fn_in_trait)]
    async fn sort_ro(
        connection: &Connection,
        key: String,
        opts: Option<SortOpts>,
    ) -> Result<Vec<Vec<Option<String>>>, Error> {
        let row = sort_row_len(&opts);
        let values: Vec<Option<String>> = query(connection, ("SORT_RO", key, opts)).await?;
        Ok(values.chunks(row).map(<[_]>::to_vec).collect())
    }

    #[doc = " Removes the members with the lowest or highest scores from the first non-empty sorted set,"]
    #[doc = " waiting up to the timeout for one, returning its key and the members and their scores."]
    #[doc = ""]
    #[doc = " Returns none when the timeout passed. A timeout of zero waits indefinitely."]
    #[doc = ""]
    #[doc = " The connection waits too, replies to commands sent after it on the connection wait behind"]
    #[doc = " it. Use a connection of your own."]
    #[doc = ""]
    #[doc = " BZMPOP timeout numkeys key [ key ... ] 〈 MIN | MAX 〉 [ COUNT count ]"]
    #[doc = " https://valkey.io/commands/bzmpop/"]
    #[allow(async_fn_in_trait)]
    async fn bzmpop(
        connection: &Connection,
        timeout: DurationMilliseconds,
        keys: Vec<String>,
        end: PopEnd,
        count: Option<Integer>,
    ) -> Result<Option<(String, Vec<(String, Double)>)>, Error> {
        let command = (
            "BZMPOP",
            DecimalSeconds(timeout),
            NumKeys(NonEmpty::new("keys", keys)),
            end,
            NamedOption("COUNT", count),
        );
        query(connection, command).await
    }

    #[doc = " Removes the member with the highest score from the first non-empty sorted set, waiting up to"]
    #[doc = " the timeout for one, returning its key, the member and its score."]
    #[doc = ""]
    #[doc = " Returns none when the timeout passed. A timeout of zero waits indefinitely."]
    #[doc = ""]
    #[doc = " The connection waits too, replies to commands sent after it on the connection wait behind"]
    #[doc = " it. Use a connection of your own."]
    #[doc = ""]
    #[doc = " BZPOPMAX key [ key ... ] timeout"]
    #[doc = " https://valkey.io/commands/bzpopmax/"]
    #[allow(async_fn_in_trait)]
    async fn bzpopmax(
        connection: &Connection,
        keys: Vec<String>,
        timeout: DurationMilliseconds,
    ) -> Result<Option<(String, String, Double)>, Error> {
        query(
            connection,
            (
                "BZPOPMAX",
                NonEmpty::new("keys", keys),
                DecimalSeconds(timeout),
            ),
        )
        .await
    }

    #[doc = " Removes the member with the lowest score from the first non-empty sorted set, waiting up to"]
    #[doc = " the timeout for one, returning its key, the member and its score."]
    #[doc = ""]
    #[doc = " Returns none when the timeout passed. A timeout of zero waits indefinitely."]
    #[doc = ""]
    #[doc = " The connection waits too, replies to commands sent after it on the connection wait behind"]
    #[doc = " it. Use a connection of your own."]
    #[doc = ""]
    #[doc = " BZPOPMIN key [ key ... ] timeout"]
    #[doc = " https://valkey.io/commands/bzpopmin/"]
    #[allow(async_fn_in_trait)]
    async fn bzpopmin(
        connection: &Connection,
        keys: Vec<String>,
        timeout: DurationMilliseconds,
    ) -> Result<Option<(String, String, Double)>, Error> {
        query(
            connection,
            (
                "BZPOPMIN",
                NonEmpty::new("keys", keys),
                DecimalSeconds(timeout),
            ),
        )
        .await
    }

    #[doc = " Adds the members with their scores to the sorted set stored at key, or updates their scores,"]
    #[doc = " returning how many were added, or with `changed`, added or updated."]
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
        score_and_members: Vec<(Double, String)>,
    ) -> Result<Option<Integer>, Error> {
        let command = (
            "ZADD",
            key,
            condition,
            comparison,
            Flag("CH", changed),
            NonEmpty::new("score-and-members", score_and_members),
        );
        query(connection, command).await
    }

    #[doc = " Increments the score of the member in the sorted set stored at key, returning the new score,"]
    #[doc = " none when the condition or comparison prevented it."]
    #[doc = ""]
    #[doc = " See also `zadd` and `zincrby`."]
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
        member: String,
    ) -> Result<Option<Double>, Error> {
        let command = (
            "ZADD", key, condition, comparison, "INCR", increment, member,
        );
        query(connection, command).await
    }

    #[doc = " The number of members in the sorted set stored at key."]
    #[doc = ""]
    #[doc = " ZCARD key"]
    #[doc = " https://valkey.io/commands/zcard/"]
    #[allow(async_fn_in_trait)]
    async fn zcard(connection: &Connection, key: String) -> Result<Integer, Error> {
        query(connection, ("ZCARD", key)).await
    }

    #[doc = " The number of members of the sorted set stored at key with scores in the range."]
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
        query(
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

    #[doc = " The members of the first sorted set that are not in any of the others."]
    #[doc = ""]
    #[doc = " See also `zdiff-withscores`."]
    #[doc = ""]
    #[doc = " ZDIFF numkeys key [ key ... ]"]
    #[doc = " https://valkey.io/commands/zdiff/"]
    #[allow(async_fn_in_trait)]
    async fn zdiff(connection: &Connection, keys: Vec<String>) -> Result<Vec<String>, Error> {
        query(connection, ("ZDIFF", NumKeys(NonEmpty::new("keys", keys)))).await
    }

    #[doc = " The members, and their scores, of the first sorted set that are not in any of the others."]
    #[doc = ""]
    #[doc = " See also `zdiff`."]
    #[doc = ""]
    #[doc = " ZDIFF numkeys key [ key ... ] WITHSCORES"]
    #[doc = " https://valkey.io/commands/zdiff/"]
    #[allow(async_fn_in_trait)]
    async fn zdiff_withscores(
        connection: &Connection,
        keys: Vec<String>,
    ) -> Result<Vec<(String, Double)>, Error> {
        query(
            connection,
            ("ZDIFF", NumKeys(NonEmpty::new("keys", keys)), "WITHSCORES"),
        )
        .await
    }

    #[doc = " Stores the members of the first sorted set that are not in any of the others, returning how"]
    #[doc = " many."]
    #[doc = ""]
    #[doc = " ZDIFFSTORE destination numkeys key [ key ... ]"]
    #[doc = " https://valkey.io/commands/zdiffstore/"]
    #[allow(async_fn_in_trait)]
    async fn zdiffstore(
        connection: &Connection,
        destination: String,
        keys: Vec<String>,
    ) -> Result<Integer, Error> {
        query(
            connection,
            (
                "ZDIFFSTORE",
                destination,
                NumKeys(NonEmpty::new("keys", keys)),
            ),
        )
        .await
    }

    #[doc = " Increments the score of the member in the sorted set stored at key, returning the new score."]
    #[doc = ""]
    #[doc = " ZINCRBY key increment member"]
    #[doc = " https://valkey.io/commands/zincrby/"]
    #[allow(async_fn_in_trait)]
    async fn zincrby(
        connection: &Connection,
        key: String,
        increment: Double,
        member: String,
    ) -> Result<Double, Error> {
        query(connection, ("ZINCRBY", key, increment, member)).await
    }

    #[doc = " The members in all of the sorted sets."]
    #[doc = ""]
    #[doc = " The weights, when given, multiply the scores of each sorted set, one for each key."]
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
        aggregate: Option<Aggregate>,
    ) -> Result<Vec<String>, Error> {
        query(
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

    #[doc = " The members, and their combined scores, in all of the sorted sets."]
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
        aggregate: Option<Aggregate>,
    ) -> Result<Vec<(String, Double)>, Error> {
        let command = (
            "ZINTER",
            NumKeys(NonEmpty::new("keys", keys)),
            NamedOption("WEIGHTS", NonEmpty::optional("weights", weights)),
            aggregate,
            "WITHSCORES",
        );
        query(connection, command).await
    }

    #[doc = " The number of members in all of the sorted sets, counting stops at the limit."]
    #[doc = ""]
    #[doc = " ZINTERCARD numkeys key [ key ... ] [ LIMIT limit ]"]
    #[doc = " https://valkey.io/commands/zintercard/"]
    #[allow(async_fn_in_trait)]
    async fn zintercard(
        connection: &Connection,
        keys: Vec<String>,
        limit: Option<Integer>,
    ) -> Result<Integer, Error> {
        query(
            connection,
            (
                "ZINTERCARD",
                NumKeys(NonEmpty::new("keys", keys)),
                NamedOption("LIMIT", limit),
            ),
        )
        .await
    }

    #[doc = " Stores the members in all of the sorted sets, with their combined scores, returning how many."]
    #[doc = ""]
    #[doc = " ZINTERSTORE destination numkeys key [ key ... ] [ WEIGHTS weight [ weight ... ] ] [ AGGREGATE SUM | MIN | MAX ]"]
    #[doc = " https://valkey.io/commands/zinterstore/"]
    #[allow(async_fn_in_trait)]
    async fn zinterstore(
        connection: &Connection,
        destination: String,
        keys: Vec<String>,
        weights: Option<Vec<Double>>,
        aggregate: Option<Aggregate>,
    ) -> Result<Integer, Error> {
        let command = (
            "ZINTERSTORE",
            destination,
            NumKeys(NonEmpty::new("keys", keys)),
            NamedOption("WEIGHTS", NonEmpty::optional("weights", weights)),
            aggregate,
        );
        query(connection, command).await
    }

    #[doc = " The number of members of the sorted set stored at key in the range, for members of equal"]
    #[doc = " score."]
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
        query(
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

    #[doc = " Removes the members with the lowest or highest scores from the first non-empty sorted set,"]
    #[doc = " returning its key and the members and their scores, none when every sorted set is empty."]
    #[doc = ""]
    #[doc = " ZMPOP numkeys key [ key ... ] 〈 MIN | MAX 〉 [ COUNT count ]"]
    #[doc = " https://valkey.io/commands/zmpop/"]
    #[allow(async_fn_in_trait)]
    async fn zmpop(
        connection: &Connection,
        keys: Vec<String>,
        end: PopEnd,
        count: Option<Integer>,
    ) -> Result<Option<(String, Vec<(String, Double)>)>, Error> {
        query(
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

    #[doc = " The score of each of the members of the sorted set stored at key, none for a member that is"]
    #[doc = " not in it."]
    #[doc = ""]
    #[doc = " ZMSCORE key member [ member ... ]"]
    #[doc = " https://valkey.io/commands/zmscore/"]
    #[allow(async_fn_in_trait)]
    async fn zmscore(
        connection: &Connection,
        key: String,
        members: Vec<String>,
    ) -> Result<Vec<Option<Double>>, Error> {
        query(
            connection,
            ("ZMSCORE", key, NonEmpty::new("members", members)),
        )
        .await
    }

    #[doc = " Removes and returns the members with the highest scores, and their scores, up to the count,"]
    #[doc = " one without a count."]
    #[doc = ""]
    #[doc = " ZPOPMAX key [ count ]"]
    #[doc = " https://valkey.io/commands/zpopmax/"]
    #[allow(async_fn_in_trait)]
    async fn zpopmax(
        connection: &Connection,
        key: String,
        count: Option<Integer>,
    ) -> Result<Vec<(String, Double)>, Error> {
        let members: ScoredMembers = query(connection, ("ZPOPMAX", key, count)).await?;
        Ok(members.0)
    }

    #[doc = " Removes and returns the members with the lowest scores, and their scores, up to the count,"]
    #[doc = " one without a count."]
    #[doc = ""]
    #[doc = " ZPOPMIN key [ count ]"]
    #[doc = " https://valkey.io/commands/zpopmin/"]
    #[allow(async_fn_in_trait)]
    async fn zpopmin(
        connection: &Connection,
        key: String,
        count: Option<Integer>,
    ) -> Result<Vec<(String, Double)>, Error> {
        let members: ScoredMembers = query(connection, ("ZPOPMIN", key, count)).await?;
        Ok(members.0)
    }

    #[doc = " Random members of the sorted set stored at key, up to the count."]
    #[doc = ""]
    #[doc = " Without a count, one member, or none when the key does not exist. A negative count may return"]
    #[doc = " the same member more than once, its absolute value is the number of members returned."]
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
    ) -> Result<Vec<String>, Error> {
        if count.is_some() {
            return query(connection, ("ZRANDMEMBER", key, count)).await;
        }
        // without a count the reply is the member itself, or null
        let member: Option<String> = query(connection, ("ZRANDMEMBER", key)).await?;
        Ok(member.into_iter().collect())
    }

    #[doc = " Random members of the sorted set stored at key, and their scores, up to the count."]
    #[doc = ""]
    #[doc = " A negative count may return the same member more than once, its absolute value is the number"]
    #[doc = " of members returned."]
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
    ) -> Result<Vec<(String, Double)>, Error> {
        query(connection, ("ZRANDMEMBER", key, count, "WITHSCORES")).await
    }

    #[doc = " The members of the sorted set stored at key in the range, from the lowest score, or the"]
    #[doc = " highest with `rev`."]
    #[doc = ""]
    #[doc = " The limit is a range of the members in the range, only with BYSCORE or BYLEX. With `xx`,"]
    #[doc = " returns none when the key does not exist."]
    #[doc = ""]
    #[doc = " Also covers the deprecated ZRANGEBYLEX, ZRANGEBYSCORE, ZREVRANGE, ZREVRANGEBYLEX and"]
    #[doc = " ZREVRANGEBYSCORE. See also `zrange-withscores`."]
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
    ) -> Result<Option<Vec<String>>, Error> {
        let range = Zrange { range, rev, limit };
        query(connection, ("ZRANGE", key, range, Flag("XX", xx))).await
    }

    #[doc = " The members of the sorted set stored at key in the range, and their scores, from the lowest"]
    #[doc = " score, or the highest with `rev`."]
    #[doc = ""]
    #[doc = " The limit is a range of the members in the range, only with BYSCORE. With `xx`, returns none"]
    #[doc = " when the key does not exist."]
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
    ) -> Result<Option<Vec<(String, Double)>>, Error> {
        let range = match range {
            ZrangeWithscoresBy::Rank(rank) => ZrangeBy::Rank(rank),
            ZrangeWithscoresBy::Score(score) => ZrangeBy::Score(score),
        };
        let range = Zrange { range, rev, limit };
        let command = ("ZRANGE", key, range, "WITHSCORES", Flag("XX", xx));
        query(connection, command).await
    }

    #[doc = " Stores the members of the sorted set stored at source in the range, returning how many."]
    #[doc = ""]
    #[doc = " The limit is a range of the members in the range, only with BYSCORE or BYLEX."]
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
        query(connection, ("ZRANGESTORE", destination, source, range)).await
    }

    #[doc = " The position of the member in the sorted set stored at key, from the lowest score, none when"]
    #[doc = " it is not a member."]
    #[doc = ""]
    #[doc = " See also `zrank-withscore` and `zrevrank`."]
    #[doc = ""]
    #[doc = " ZRANK key member"]
    #[doc = " https://valkey.io/commands/zrank/"]
    #[allow(async_fn_in_trait)]
    async fn zrank(
        connection: &Connection,
        key: String,
        member: String,
    ) -> Result<Option<Integer>, Error> {
        query(connection, ("ZRANK", key, member)).await
    }

    #[doc = " The position, and the score, of the member in the sorted set stored at key, from the lowest"]
    #[doc = " score, none when it is not a member."]
    #[doc = ""]
    #[doc = " See also `zrank`."]
    #[doc = ""]
    #[doc = " ZRANK key member WITHSCORE"]
    #[doc = " https://valkey.io/commands/zrank/"]
    #[allow(async_fn_in_trait)]
    async fn zrank_withscore(
        connection: &Connection,
        key: String,
        member: String,
    ) -> Result<Option<(Integer, Double)>, Error> {
        query(connection, ("ZRANK", key, member, "WITHSCORE")).await
    }

    #[doc = " Removes the members from the sorted set stored at key, returning how many were members."]
    #[doc = ""]
    #[doc = " ZREM key member [ member ... ]"]
    #[doc = " https://valkey.io/commands/zrem/"]
    #[allow(async_fn_in_trait)]
    async fn zrem(
        connection: &Connection,
        key: String,
        members: Vec<String>,
    ) -> Result<Integer, Error> {
        query(connection, ("ZREM", key, NonEmpty::new("members", members))).await
    }

    #[doc = " Removes the members of the sorted set stored at key in the range, for members of equal score,"]
    #[doc = " returning how many."]
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
        query(
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

    #[doc = " Removes the members of the sorted set stored at key at the positions, both included,"]
    #[doc = " negative from the end, returning how many."]
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
        query(connection, ("ZREMRANGEBYRANK", key, start, stop)).await
    }

    #[doc = " Removes the members of the sorted set stored at key with scores in the range, returning how"]
    #[doc = " many."]
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
        query(
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

    #[doc = " The position of the member in the sorted set stored at key, from the highest score, none"]
    #[doc = " when it is not a member."]
    #[doc = ""]
    #[doc = " See also `zrevrank-withscore` and `zrank`."]
    #[doc = ""]
    #[doc = " ZREVRANK key member"]
    #[doc = " https://valkey.io/commands/zrevrank/"]
    #[allow(async_fn_in_trait)]
    async fn zrevrank(
        connection: &Connection,
        key: String,
        member: String,
    ) -> Result<Option<Integer>, Error> {
        query(connection, ("ZREVRANK", key, member)).await
    }

    #[doc = " The position, and the score, of the member in the sorted set stored at key, from the highest"]
    #[doc = " score, none when it is not a member."]
    #[doc = ""]
    #[doc = " See also `zrevrank`."]
    #[doc = ""]
    #[doc = " ZREVRANK key member WITHSCORE"]
    #[doc = " https://valkey.io/commands/zrevrank/"]
    #[allow(async_fn_in_trait)]
    async fn zrevrank_withscore(
        connection: &Connection,
        key: String,
        member: String,
    ) -> Result<Option<(Integer, Double)>, Error> {
        query(connection, ("ZREVRANK", key, member, "WITHSCORE")).await
    }

    #[doc = " Incrementally iterate over the members of the sorted set stored at key, and their scores."]
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
    ) -> Result<(Option<Cursor>, Vec<(String, Double)>), Error> {
        // the members and scores are a flat array, the scores bulk strings
        let (cursor, members): (NextCursor, FlatPairs<String, Double>) = query(
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

    #[doc = " Incrementally iterate over the members of the sorted set stored at key."]
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
    ) -> Result<(Option<Cursor>, Vec<String>), Error> {
        let (cursor, items): (NextCursor, Vec<String>) = query(
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

    #[doc = " The score of the member in the sorted set stored at key, none when it is not a member."]
    #[doc = ""]
    #[doc = " ZSCORE key member"]
    #[doc = " https://valkey.io/commands/zscore/"]
    #[allow(async_fn_in_trait)]
    async fn zscore(
        connection: &Connection,
        key: String,
        member: String,
    ) -> Result<Option<Double>, Error> {
        query(connection, ("ZSCORE", key, member)).await
    }

    #[doc = " The members in any of the sorted sets."]
    #[doc = ""]
    #[doc = " The weights, when given, multiply the scores of each sorted set, one for each key."]
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
        aggregate: Option<Aggregate>,
    ) -> Result<Vec<String>, Error> {
        query(
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

    #[doc = " The members, and their combined scores, in any of the sorted sets."]
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
        aggregate: Option<Aggregate>,
    ) -> Result<Vec<(String, Double)>, Error> {
        let command = (
            "ZUNION",
            NumKeys(NonEmpty::new("keys", keys)),
            NamedOption("WEIGHTS", NonEmpty::optional("weights", weights)),
            aggregate,
            "WITHSCORES",
        );
        query(connection, command).await
    }

    #[doc = " Stores the members in any of the sorted sets, with their combined scores, returning how many."]
    #[doc = ""]
    #[doc = " ZUNIONSTORE destination numkeys key [ key ... ] [ WEIGHTS weight [ weight ... ] ] [ AGGREGATE SUM | MIN | MAX ]"]
    #[doc = " https://valkey.io/commands/zunionstore/"]
    #[allow(async_fn_in_trait)]
    async fn zunionstore(
        connection: &Connection,
        destination: String,
        keys: Vec<String>,
        weights: Option<Vec<Double>>,
        aggregate: Option<Aggregate>,
    ) -> Result<Integer, Error> {
        let command = (
            "ZUNIONSTORE",
            destination,
            NumKeys(NonEmpty::new("keys", keys)),
            NamedOption("WEIGHTS", NonEmpty::optional("weights", weights)),
            aggregate,
        );
        query(connection, command).await
    }
}

wit_bindgen::generate!({
    path: "../wit",
    world: "commands",
    // the resp types shared by every component
    with: {
        "componentized:valkey/resp@0.3.0-dev": resp_codec::bindings::componentized::valkey::resp,
    },
    generate_all,
});

export!(Commands);
