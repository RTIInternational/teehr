# Getting started

This walks through the `teehr` command-line tool: install it, sign in, look at the shared data,
and run a first evaluation.

## 1. Install

```
uv tool install "git+https://github.com/RTIInternational/teehr.git@v1"
teehr --help
```

(`pip install "git+https://github.com/RTIInternational/teehr.git@v1"` works too.)

## 2. Sign in

Pass the address of your TEEHR Cloud's API the first time:

```
teehr --api https://control-plane.teehr.rtiamanzi.org auth login
```

`teehr` shows a code and opens your browser. Sign in, check that the code on the page matches the
one in your terminal, and approve. `teehr` saves a **personal token** for this computer and makes
this API the default, so later commands don't need `--api`.

```
teehr auth status       # who you are, and which token this computer uses
teehr projects list     # the projects you can use, and your role in each
```

A local development cluster (KinD) uses `https://control-plane.teehr.local.app.garden` and a
self-signed certificate: sign in there with `--insecure`. See [Tokens and access](tokens.md) for
other computers, scripts and CI.

## 3. Look at the shared data

The `public` project holds data everyone can read: observations, model outputs and the crosswalks
between them.

```
teehr datasets list                                    # public's datasets
teehr datasets show public.usgs_streamflow              # what it is: role, variable, unit, columns
teehr datasets read public.usgs_streamflow -n 10
teehr datasets read public.usgs_streamflow -f "series_key = 'usgs-01646500'" -n 50
```

`datasets` commands read `public` unless you give a project with `-p`. Printed output stops at
1,000 rows (`-n` changes that); `--out file.parquet` (or `.csv`, `.arrow`) writes every row to a
file instead.

## 4. Write and check an evaluation

Evaluations live in a project where you have the `write` or `admin` role (`teehr projects list`
shows yours; below, `my_project` stands for one). `teehr` can't create projects yet: ask an admin,
or use `POST /v1/projects` in the API docs if you're allowed to create them.

Save this as `nwm-vs-usgs.yaml`:

```yaml
version: 1
name: nwm31-vs-usgs
inputs:
  primary:
    dataset: public.usgs_streamflow
  secondary:
    - dataset: public.nwm31_short_range
    - dataset: public.nwm31_medium_range_mem1
pair:
  relation: public.usgs_nwm31
aggregate:
  - group_by: [primary_series_key, secondary_dataset]
    metrics: [count, kling_gupta_efficiency, nash_sutcliffe_efficiency, relative_bias]
output:
  unit: "[cft_i]/s"
```

It pairs each USGS gage with its NWM reaches through the `usgs_nwm31` crosswalk, then computes
four metrics for each gage and NWM product. (Dataset names differ between installations:
`teehr datasets list` shows yours.)

Check it against the project's datasets, without saving or running anything:

```
teehr evaluations plan nwm-vs-usgs.yaml -p my_project
```

It prints what the evaluation will read and compute, or every problem it finds (an unknown
dataset, a metric name, a unit that doesn't convert).

## 5. Save it and run it

```
teehr evaluations save nwm31_vs_usgs nwm-vs-usgs.yaml -p my_project
teehr runs start nwm31_vs_usgs -p my_project --wait
```

Saving makes version 1; saving a changed spec makes version 2, and so on. A run reads the data
as it is now, computes, and stores the results. `--wait` follows it until it ends.

## 6. Read the results

```
teehr runs results <run id> -p my_project
teehr runs results <run id> -p my_project --out results.parquet
```

The run ID is in `teehr runs list nwm31_vs_usgs -p my_project`.

## Next

- [Concepts](concepts.md): what projects, datasets, evaluations and runs are.
- [Uploading data](uploads.md): evaluate your own data.
- [Evaluations and runs](evaluations.md): more of what a spec can do, and rerunning as new data
  arrives.
