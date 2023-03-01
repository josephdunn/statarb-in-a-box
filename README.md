# StatArb in a (Cardboard) Box

This is a one-day project I put together to illustrate some basic quant trading concepts. What it does:

* Downloads a few months of bar data from Binance
* Generates simple features
* Builds models iteratively
* Simulates a long/short market-neutral basket trading strategy

## What it doesn't do

* Place real trades
* Accurately model bid/ask spreads or other microstructure-variant effects
* Concern itself with execution
* Portfolio- or volatility-level optimization

## Usage

You'll need a recent [R](https://cran.r-project.org/) version with the `xts` and `scales` packages installed. You will also need a Python environment that will allow you to install a couple of packages, although you may already have them.

First, clone this repo and install required Python packages.

```
cd statarb-in-a-box && pip install -r pip-requirements.txt
```

Then download data.

```
mkdir data
python get_data.py
```

Finally, launch R and build features.

```R

R

> source('ls.R')
> allData <- readData()
> dataWithFeatures <- mclapply(allData, buildFeatsAndFuture, mc.cores=10) # set mc.cores to number of cores you want to use
```

Feature building takes a long time; on my desktop it's on the order of 2 hours. Once complete I recommend saving a copy of the data and features in case you lose your R session:

```R
> save(dataWithFeatures, file='dataWithFeatures.RData')
```

Then run a backtest.

```R
btres <- bt(dataWithFeatures)
```

`btres` will be a day-by-day list of `data.frame`s that you can combine by `rbind`ing like so:

```R
> btres <- do.call(rbind, btres)
```

Or you can use the included utility function `btResToXTS` to get an `xts` object containing returns by close time:

```R
> btres.xts <- btresToXTS(btres)
```

Which you can plot or analyze in other ways (e.g. in [PerformanceAnalytics](https://cran.r-project.org/web/packages/PerformanceAnalytics/index.html)). Finally, you can make a copy of your backtest results with commissions applied. The following returns a `btres`-style list with 10 bps of commissions applied *in toto* to each entry/exit pair:

```R
btres.withComms <- applyComms(btres, 0.001)
```

## How could this be improved?

Lots of ways. First see above regarding what it doesn't do. Then:

* The simulation completely ignores execution, which could be remedied by modeling executions (again, with some assumptions) on quote data or orderbook data, for instance from [Tardis.dev](https://tardis.dev/). Binance also provides trade data, although that data doesn't contain quotes, so at best some heuristics for bid/ask spread would have to be employed. Either way, we're talking data sizes in the multi- to hundreds of gigabytes range, so the backtest would need to be pulled out of the R script and implemented in Python (slow) or potentially a faster language.

* The features used aren't actually *good,* which you will discover if you try to adjust your backtest results by commissions. What's in here are merely things that are better than flying blind, and that can be built on bar data.

* The models this builds are taken at face value. Obviously this is not ideal, and there are lots of directions to go from here. Improve model parsimony, throw out bad models, employ things that utilize penalized max likelihood, look at multiple timeframes, etc.

* Some of the feature generation is abhorrently slow (e.g. everything that calls `rollScale`) and could be done in a much more clever way.

* Live trading

* Code cleanup: I've hardcoded various bits, e.g. column counts and indexes. Really, all feature columns should have names prefixed with `feat.` so colnames can be grepped over to get a vector of column indexes--similar to how `pred.` and `close.` column name prefixes are used. Also, loops are yucky; it would be trivial to pre-compute train/test periods and then `lapply` (or `mclapply`!) over that to get backtest results. 

## License

MIT license, see `LICENSE` for details
