This python package is a wrapper of Lucence benchmark. It basically prepares 
related resources and execute `ant run-task -dALG=...`. 

To run it, do 

```
sudo python driver.py
```

You need to use `sudo` because we need to drop page cache, which requires root.


You can configure the run in driver.py, at ExperimentWiki.conf(). There are
already several sets of configurations there. Enable your desired set of 
configuration by removing the `#` prefix.


