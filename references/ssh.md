# SSH REPL Reference

## Spawn

```bash
t new remote ssh user@server
# with key
t new remote ssh -i ~/.ssh/key user@server
# with port
t new remote ssh -p 2222 user@server
# jump host
t new remote ssh -J bastion user@internal
```

## Why Persistent SSH Matters

One-shot SSH rebuilds everything:
```bash
ssh user@server 'cd /project && source .env && make test'   # every time
ssh user@server 'cd /project && source .env && make deploy'  # again
```

Persistent SSH sends deltas:
```bash
T_SESSION=remote t w 'cd /project'
T_SESSION=remote t w 'source .env'
T_SESSION=remote t w 'make test'      # cwd and env already set
T_SESSION=remote t w 'make deploy'    # still set
```

One TCP connection. One auth. No rebuild.

## Connection Keep-Alive

SSH connections can drop from idle timeout. Two defenses:

### Server-side (if you control it)
```
# /etc/ssh/sshd_config
ClientAliveInterval 60
ClientAliveCountMax 3
```

### Client-side
```bash
t new remote ssh -o ServerAliveInterval=60 -o ServerAliveCountMax=3 user@server
```

tmux/psmux also helps — if the SSH connection drops, the local session survives. Reconnect SSH inside the same session.

## State Inspection

```bash
T_SESSION=remote t w 'pwd'
T_SESSION=remote t w 'env | sort'
T_SESSION=remote t w 'whoami && hostname'
T_SESSION=remote t r 10
```

## Dump / Restore

SSH sessions can't be serialized. But the remote state can:

```bash
# dump remote env
T_SESSION=remote t w '{ export -p; echo "cd $(pwd)"; } > /tmp/state.sh'

# if connection dies, reconnect and restore
t new remote ssh user@server
T_SESSION=remote t w 'source /tmp/state.sh'
```

## Common Patterns

### Remote debugging
```bash
t new remote ssh user@server
T_SESSION=remote t w 'tail -f /var/log/app.log'
T_SESSION=remote t r 30                          # check logs
T_SESSION=remote t k ctrl-c                      # stop tail
T_SESSION=remote t w 'systemctl status myapp'
```

### Remote build
```bash
T_SESSION=remote t w 'cd /project && git pull'
T_SESSION=remote t w 'make -j$(nproc)'
T_SESSION=remote t W 'Build complete'            # wait for it
T_SESSION=remote t r 10
```

### Tunnel + service
```bash
# SSH with port forward in an agent-terminal session
t new tunnel ssh -L 8080:localhost:80 user@server
# now localhost:8080 is alive for as long as the session lives
```

### Multi-server
```bash
t new prod ssh deploy@prod-server
t new staging ssh deploy@staging-server
t new db ssh dba@db-server

T_SESSION=prod t w 'uptime'
T_SESSION=staging t w 'uptime'
T_SESSION=db t w 'psql -c "SELECT count(*) FROM users"'
```
