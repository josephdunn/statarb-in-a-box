library(xts)
library(scales)
library(parallel)

datadir          <- 'data'
trainDays        <- 10
testDays         <- 1
basketSize       <- 10
intervals        <- c(1, 2, 5, 10, 15, 20, 25, 30, 40, 50)
future.intervals <- intervals

readData <- function()
{
    fileList <- list.files(datadir)
    syms <- sub('.csv', '', fileList, fixed=TRUE)
    return(setNames(lapply(syms, function(sym) {
               message('read ', sym)
               d <- read.csv(paste0(datadir, '/', sym, '.csv'), stringsAsFactors=FALSE)
               return(xts(d, .POSIXct(as.numeric(d$close_time)/1000)))
    }), syms))
}

rollScale <- function(x)
{
    last(rescale(as.vector(x)))
}

# to use: dataWithFeatures <- mclapply(alld, buildFeatsAndFuture, mc.cores=10)
buildFeatsAndFuture <- function(d)
{
    d$taker_sell_volume <- d$volume - d$taker_buy_volume         # sell volume
    d$buy_vol_pct <- d$taker_buy_volume / d$volume               # pct of vol that was buy
    d$buy_vol_pct_diff <- diff(d$buy_vol_pct)                    # change in pct of vol that was buy from last bar
    d$vol_diff_pct <- (d$volume - lag(d$volume)) / lag(d$volume) # change in total volume from last bar
    d$delta <- d$taker_buy_volume - d$taker_sell_volume          # this bar's delta (buy volume - sell volume)
    d$range <- log(d$high) - log(d$low)                          # range

    deltas <- setNames(do.call(cbind, lapply(intervals[-1], function(x)
    {
        rollapply(d$delta, x, sum)
    })), paste0('vol.delta.', intervals[-1]))

    d <- cbind(d, deltas)

    closeECDF <- setNames(xts(do.call(cbind, lapply(intervals, function(x)
    {
        rollapply(d$close, x, rollScale)
    })), index(d)), paste0('close.ecdf.', intervals))

    buyVolPctECDF <- setNames(xts(do.call(cbind, lapply(intervals[-1], function(x)
    {
        rollapply(d$buy_vol_pct, x, rollScale)
    })), index(d)), paste0('buy_vol_pct.ecdf.', intervals[-1]))

    future <- setNames(do.call(cbind, lapply(future.intervals, function(x)
    {
        log(lag(d$close, k = -x)) - log(d$close)
    })), paste0('future.', future.intervals))

    return(na.omit(cbind(d, closeECDF, buyVolPctECDF, future)))
}

buildModels <- function(d, future=30)
{
    trainIdxs <- 14:46
    lapply(d, function(x)
    {
        f <- as.formula(paste0('future.', future, ' ~ ', paste0(colnames(x)[trainIdxs], collapse=' + '), ' + 0'))
        lm(f, data=x)
    })
}

filterToDateStr <- function(d, dateStr)
{
    lapply(d, function(x) { x[dateStr] })
}

bt <- function(d, maxDays=NULL, ...)
{
    areNull <- unlist(lapply(d, is.null))
    if (any(areNull))
    {
        symsToDrop <- names(d)[areNull]
        message('dropping the following NULL symbols from data: ', paste(symsToDrop, collapse=', '))
        idxsToDrop <- sapply(symsToDrop, function(dropThisSym) { which(names(d) == dropThisSym) })
        message('drop list idxs:')
        print(idxsToDrop)
        d <- d[-idxsToDrop]
    }
    days <- sort(unique(as.Date(index(d[[first(names(d))]]))))

    pos <- list()
    posPrice <- list()
    posOpenTime <- list()
    dayRets <- list()

    trainStart <- 1
    trainStop <- trainStart + trainDays - 1
    testStart <- trainStop + 1
    testStop <- testStart + testDays - 1
    while (testStop <= length(days))
    {
        # clean data
        trainStr <- paste0(days[trainStart], '::', days[trainStop])
        testStr <- paste0(days[testStart], '::', days[testStop])
        thisPeriodTrainData <- filterToDateStr(d, trainStr)
        thisPeriodTestData <- filterToDateStr(d, testStr)

        trainDataRowCounts <- sapply(1:length(d), function(idx) { nrow(thisPeriodTrainData[[idx]]) })
        cutTrain <- mean(trainDataRowCounts) * 0.8
        testDataRowCounts <- sapply(1:length(d), function(idx) { nrow(thisPeriodTestData[[idx]]) })
        cutTest <- mean(testDataRowCounts) * 0.8

        toKeepInTrainRows <- sapply(1:length(d), function(idx) { trainDataRowCounts[idx] >= cutTrain })
        toKeepInTestRows <- sapply(1:length(d), function(idx) { testDataRowCounts[idx] >= cutTest })

        toKeepInTrainCols <- sapply(1:length(d), function(idx) { ncol(thisPeriodTrainData[[idx]]) == 56 })
        toKeepInTestCols <- sapply(1:length(d), function(idx) { ncol(thisPeriodTestData[[idx]]) == 56 })
        toKeep <- toKeepInTrainCols & toKeepInTestCols & toKeepInTrainRows & toKeepInTestRows

        thisPeriodTrainData <- thisPeriodTrainData[toKeep]
        thisPeriodTestData <- thisPeriodTestData[toKeep]

        # build models
        models <- buildModels(thisPeriodTrainData, ...)

        # predict everything
        predictions <- lapply(names(models), function(modName) {
            testData <- thisPeriodTestData[[modName]]$close
            testData$pred <- as.numeric(predict(models[[modName]], thisPeriodTestData[[modName]]))
            return(setNames(testData, paste0(c('close.', 'pred.'), modName)))
        })

        # get aligned test period preds and closes
        allTestPeriod <- na.omit(do.call(cbind, predictions))
        allPreds <- allTestPeriod[,grep('pred.', colnames(allTestPeriod), fixed=TRUE)]
        allCloses <- allTestPeriod[,grep('close.', colnames(allTestPeriod), fixed=TRUE)]

        # iterate test period
        message('for ', testStr, ' process ', nrow(allTestPeriod), ' rows')
        thisDayRets <- NULL
        testIdx <- 1
        while (testIdx <= nrow(allTestPeriod))
        {
            # trade
            predsMatrix <- t(allPreds[testIdx,])
            sortedPredsMatrix <- predsMatrix[order(predsMatrix),]
            toShort <- gsub('pred.', '', names(head(sortedPredsMatrix, basketSize)), fixed=TRUE)
            toBuy <- gsub('pred.', '', names(tail(sortedPredsMatrix, basketSize)), fixed=TRUE)

            for (sym in toShort)
            {
                closePrice <- as.numeric(allCloses[testIdx,which(colnames(allCloses) == paste0('close.', sym))])
                if (sym %in% pos)
                {
                    # we have a position in this symbol
                    if (pos[[sym]] == 1)
                    {
                        # we are selling this, so emitting PNL
                        thisDayRets <- rbind(thisDayRets, data.frame(symbol=sym, tradeDir=pos[[sym]], openTime=posOpenTime[[sym]], closeTime=index(allCloses[testIdx]), ret=(log(closePrice) - log(posPrice[[sym]]))))
                        pos[[sym]] <- -1
                        posPrice[[sym]] <- closePrice
                        posOpenTime[[sym]] <- index(allCloses[testIdx])
                    }
                    else if (pos[[sym]] == 0)
                    {
                        # we are flat, so make position
                        pos[[sym]] <- -1
                        posPrice[[sym]] <- closePrice
                        posOpenTime[[sym]] <- index(allCloses[testIdx])
                    }
                }
                else
                {
                    # make position
                    pos[[sym]] <- -1
                    posPrice[[sym]] <- closePrice
                    posOpenTime[[sym]] <- index(allCloses[testIdx])
                }
            }

            for (sym in toBuy)
            {
                closePrice <- as.numeric(allCloses[testIdx,which(colnames(allCloses) == paste0('close.', sym))])
                if (sym %in% pos)
                {
                    # we have a position in this symbol
                    if (pos[[sym]] == -1)
                    {
                        # we are buying this, so emitting PNL
                        thisDayRets <- rbind(thisDayRets, data.frame(symbol=sym, tradeDir=pos[[sym]], openTime=posOpenTime[[sym]], closeTime=index(allCloses[testIdx]), ret=(log(posPrice[[sym]]) - log(closePrice))))
                        pos[[sym]] <- 1
                        posPrice[[sym]] <- closePrice
                        posOpenTime[[sym]] <- index(allCloses[testIdx])
                    }
                    else if (pos[[sym]] == 0)
                    {
                        # we are flat, so make position
                        pos[[sym]] <- 1
                        posPrice[[sym]] <- closePrice
                        posOpenTime[[sym]] <- index(allCloses[testIdx])
                    }
                }
                else
                {
                    # make position
                    pos[[sym]] <- 1
                    posPrice[[sym]] <- closePrice
                    posOpenTime[[sym]] <- index(allCloses[testIdx])
                }
            }

            for (sym in names(pos))
            {
                if ((!(sym %in% toShort)) && (!(sym %in% toBuy)) && (pos[[sym]] != 0))
                {
                    closePrice <- as.numeric(allCloses[testIdx,which(colnames(allCloses) == paste0('close.', sym))])
                    if (pos[[sym]] == -1)
                    {
                        # we are buying this, so emitting PNL
                        thisDayRets <- rbind(thisDayRets, data.frame(symbol=sym, tradeDir=pos[[sym]], openTime=posOpenTime[[sym]], closeTime=index(allCloses[testIdx]), ret=(log(posPrice[[sym]]) - log(closePrice))))
                        pos[[sym]] <- 0
                        posPrice[[sym]] <- 0
                        posOpenTime[[sym]] <- 0
                    }
                    else if (pos[[sym]] == 1)
                    {
                        # we are selling this, so emitting PNL
                        thisDayRets <- rbind(thisDayRets, data.frame(symbol=sym, tradeDir=pos[[sym]], openTime=posOpenTime[[sym]], closeTime=index(allCloses[testIdx]), ret=(log(closePrice) - log(posPrice[[sym]]))))
                        pos[[sym]] <- 0
                        posPrice[[sym]] <- 0
                        posOpenTime[[sym]] <- 0
                    }
                }
            }

            # advance
            testIdx <- testIdx + 1
        }

        # close at EOD prices...
        for (sym in names(pos))
        {
            if (pos[[sym]] != 0)
            {
                closePrice <- as.numeric(allCloses[nrow(allCloses),which(colnames(allCloses) == paste0('close.', sym))])
                if (pos[[sym]] == -1)
                {
                    # we are buying this, so emitting PNL
                    thisDayRets <- rbind(thisDayRets, data.frame(symbol=sym, tradeDir=pos[[sym]], openTime=posOpenTime[[sym]], closeTime=index(allCloses[nrow(allCloses),])+1, ret=(log(posPrice[[sym]]) - log(closePrice))))
                    pos[[sym]] <- 0
                    posPrice[[sym]] <- 0
                    posOpenTime[[sym]] <- 0
                }
                else if (pos[[sym]] == 1)
                {
                    # we are selling this, so emitting PNL
                    thisDayRets <- rbind(thisDayRets, data.frame(symbol=sym, tradeDir=pos[[sym]], openTime=posOpenTime[[sym]], closeTime=index(allCloses[nrow(allCloses),])+1, ret=(log(closePrice) - log(posPrice[[sym]]))))
                    pos[[sym]] <- 0
                    posPrice[[sym]] <- 0
                    posOpenTime[[sym]] <- 0
                }
            }
        }
        dayRets[[length(dayRets)+1]] <- thisDayRets

        # advance
        trainStart <- trainStart + 1
        trainStop <- trainStart + trainDays - 1
        testStart <- trainStop + 1
        testStop <- testStart + testDays - 1
    }

    return(dayRets)
}

btResToXTS <- function(bt)
{
    combined <- do.call(rbind, bt)
    return(xts(combined$ret, combined$closeTime))
}

applyComms <- function(bt, comms=0.0005)
{
    lapply(bt, function(btd) { btd$ret <- btd$ret - comms; return(btd) })
}
