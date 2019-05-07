TSKV
----

A wrapper on top on consul to manage versioned values.

```
usage: tskv [<flags>] <command> [<args> ...]

Flags:
  --help     Show context-sensitive help (also try --help-long and --help-man).
  --version  Show application version.

Commands:
  help [<command>...]
    Show help.

  get
    Get last set value of a key

  set [<flags>] <key> <value>
    Set a key

  rollback --tag=TAG
    Rollback value of key to a specified tag

  list-tag
    List tags

```
