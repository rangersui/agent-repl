# R REPL Reference

## Spawn

```bash
t new r R --no-save --interactive
# or with Rscript for non-interactive
t new r R --vanilla --interactive
```

## State Inspection

```bash
T_SESSION=r t w 'ls()'                          # list objects
T_SESSION=r t w 'str(myvar)'                     # structure of object
T_SESSION=r t w 'sessionInfo()'                  # loaded packages
T_SESSION=r t w 'sapply(ls(), function(x) class(get(x)))'  # types
T_SESSION=r t w 'getwd()'                        # working directory
```

## Dump / Restore

R has native workspace serialization.

### Full workspace
```bash
T_SESSION=r t w 'save.image("/tmp/r_session.RData")'
# restore
T_SESSION=r t w 'load("/tmp/r_session.RData")'
```

### Selective save
```bash
T_SESSION=r t w 'save(model, results, config, file="/tmp/r_checkpoint.RData")'
# restore
T_SESSION=r t w 'load("/tmp/r_checkpoint.RData")'
```

What survives: all R objects — data frames, models, functions, lists, environments.
What doesn't: open connections, database handles, parallel cluster registrations.

## Common Patterns

```bash
# data analysis
T_SESSION=r t w 'library(tidyverse)'
T_SESSION=r t w 'df <- read_csv("data.csv")'
T_SESSION=r t w 'summary(df)'
T_SESSION=r t w 'df %>% group_by(category) %>% summarise(mean_val = mean(value))'

# modeling
T_SESSION=r t w 'model <- lm(y ~ x1 + x2, data=df)'
T_SESSION=r t w 'summary(model)'
T_SESSION=r t w 'save(model, file="/tmp/model.RData")'

# plotting (to file)
T_SESSION=r t w 'png("/tmp/plot.png")'
T_SESSION=r t w 'plot(df$x, df$y)'
T_SESSION=r t w 'dev.off()'
```
