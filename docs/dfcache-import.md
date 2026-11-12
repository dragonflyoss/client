# Import persistent cache content through a mounted UDS

`dfcache import` supports two transfer modes:

- By default, dfcache sends the absolute source path to dfdaemon. The source file must exist at the same path in dfdaemon's mount namespace. This preserves the existing host import behavior.
- With `--transfer-to-dfdaemon`, dfcache streams the file content over dfdaemon's download Unix domain socket (UDS). Use this mode when the source path is visible only inside a container or Kubernetes Pod.

The streaming protocol sends one metadata message followed by chunks of at most 1 MiB. It does not send the client filename, so Unix filenames that are not valid UTF-8 are supported. Dfdaemon validates the declared content length, stages the received content under `storage.dir/tmp`, calculates the persistent cache task ID using the same rules as a path import, and then uses the existing persistent cache upload and scheduler-registration flow. The staging file is removed after success or failure.

## Kubernetes Pod example

Mount the Node's dfdaemon socket directory into the Pod. The source file and dfdaemon's storage directory do not need to be shared with the Pod.

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: dfcache-import
spec:
  containers:
    - name: dfcache
      image: dragonflyoss/client:latest
      command: ["sleep", "infinity"]
      volumeMounts:
        - name: dfdaemon-download
          mountPath: /var/run/dragonfly
  volumes:
    - name: dfdaemon-download
      hostPath:
        path: /var/run/dragonfly
        type: Directory
```

Run the import inside the Pod:

```console
dfcache import \
  --endpoint /var/run/dragonfly/dfdaemon.sock \
  --transfer-to-dfdaemon \
  /path/inside/the/pod/file.bin
```

The endpoint must point to the mounted socket, and the Pod must have permission to access it. Both dfcache and dfdaemon must include the streaming content service. An older dfdaemon returns `UNIMPLEMENTED`; dfcache does not automatically retry with a path import because doing so could hide transfer failures or duplicate an import. Omit `--transfer-to-dfdaemon` only when the source path is directly visible to dfdaemon.
