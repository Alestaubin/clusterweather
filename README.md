# ⛅ clusterweather

Which Slurm cluster should you work on today? clusterweather keeps one SSH connection
open to each cluster you use and shows a live forecast: how soon a new job of yours
would start on every GPU type, your fairshare, your lab's usage, and the best bet right now.

Press a number key for any cluster's full dashboard: fairshare and priority, your queue
and GPUs, lab leaderboards, the Hall of Shame, and usage history charts.

Built for the Digital Research Alliance of Canada clusters (Fir, Killarney, Narval,
Nibi, Rorqual, Trillium, Vulcan), but it only uses standard Slurm commands, so other
clusters work too.

## Install

You need Python 3.8+ and OpenSSH (macOS, Linux, or WSL on Windows). The easiest way is
[pipx](https://pipx.pypa.io), which puts the commands on your PATH in their own environment:

```bash
brew install pipx && pipx ensurepath        # macOS; see pipx's docs for Linux
pipx install git+https://github.com/alestaubin/clusterweather
```

## Use

```bash
clusterweather --demo     # try it with simulated clusters, no SSH
clusterweather            # first run asks for your lab, username and clusters
```

The first run saves a profile to `~/.config/clusterweather/profile.json`
(`clusterweather setup` edits it), then logs into each cluster. You approve one MFA
prompt per cluster and the connection stays open until your laptop sleeps or the network
drops. Press `l` to log in again, `1`-`9` to open a cluster's dashboard, `Tab` for the
next one, `w` to come back and `q` to quit. `clusterweather logout` closes the connections.

Forecasts are for a new job of yours: 🌞 now, ⛅ under 2 h, ☔ under 12 h, 🌀 longer.
They assume running jobs keep starting at the recent rate and ignore backfill, so they
lean pessimistic. Short jobs often start sooner.

## How it works

For each cluster, clusterweather opens an SSH control master (the mechanism the
Alliance recommends for reducing MFA prompts) and runs `slurmdash stream` on the login
node through it (`slurmdash.py`, the single-cluster engine inside the package). The slurmdash source is sent over the connection each time, so nothing
needs installing on the clusters. It queries Slurm there and sends back about 40 KB of
JSON per refresh (60 s by default).

## Updating

```bash
pipx reinstall clusterweather
```

That pulls the latest version from GitHub. Your profile and connections are untouched.

## Being a good neighbour

Login nodes are shared. The defaults poll the scheduler once a minute with cached,
incremental queries. Please don't lower the refresh much below that.

## Development

```bash
git clone https://github.com/alestaubin/clusterweather
cd clusterweather
pip install -e .                # edits take effect immediately
clusterweather --demo
```

Every push runs the app on synthetic data (see `.github/workflows/demo.yml`).
To release: bump `version` in `pyproject.toml`, commit, tag (`git tag v0.1.1`) and push
with `git push --tags`.

## License

MIT
