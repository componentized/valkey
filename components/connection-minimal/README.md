# connection-minimal

A connection component built without pub/sub, for a smaller component when subscriptions are not needed.

The interface is the same: `connection.subscription` fails with a client error, every push message is a dead letter, and SUBSCRIBE and the like may still be sent with `exchange`.
