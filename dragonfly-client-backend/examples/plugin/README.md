# Example of Backend Plugin

An example of HDFS plugin for the Dragonfly client backend.

## Build Plugin

Build the plugin and move it to the plugin directory. If use plugin in MacOS,
you should replace `libhdfs.so` with `libhdfs.dylib`.

```shell
cargo build --all && mv target/debug/libhdfs.so {plugin_dir}/backend/libhdfs.so
```

## Run Client with Plugin

```shell
$ cargo run --bin dfdaemon -- --config {config_dir}/config.yaml -l info --verbose
INFO  load [http]  builtin backend
INFO  load [https] builtin backend
INFO  load [hdfs]  plugin backend
```

## Download Task with Plugin

```shell
cargo run --bin dfget hdfs://example.com/file -O file
```
