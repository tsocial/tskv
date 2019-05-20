TSKV
----

A wrapper on top on consul to manage versioned values.

```
usage: tskv [<flags>] <command> [<args> ...]

Flags:
  --help           Show context-sensitive help (also try --help-long and --help-man).
  --consul=CONSUL  Consul address
  --version        Show application version.

Commands:
  help [<command>...]
    Show help.

  get <key>
    Get last set value of a key

  set [<flags>] <key> <value>
    Set a key

  rollback --tag=TAG <key>
    Rollback value of key to a specified tag

  list <key>
    List tags


```
### Build

Clone the rep and run `make cli_build` (linux) or `make cli_build_mac` (OSX).

### Example

```
$ ./tskv set env  <(echo "dev")

$ ./tskv list env
2019/05/09 02:09:53 [1557347985066727170 latest]

$ ./tskv set env  <(echo "dev")
$ ./tskv set env --tag=v2 <(echo "staging")

$ ./tskv list env
2019/05/09 02:10:30 [1557347985066727170 latest v2]

$ ./tskv get env
2019/05/09 02:10:33 staging

$ ./main set env --tag=v3 <(echo "production")

$ ./tskv get env
2019/05/09 02:10:52 production

$ ./tskv rollback env --tag=v2
$ ./tskv get env
2019/05/09 02:11:10 staging

$ ./tskv list env
2019/05/09 02:11:26 [1557347985066727170 1557348066542571612 latest v2 v3]

```
