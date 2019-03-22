#' @importFrom tiledb tiledb_dense dim domain schema
DenseTileDBArraySeed <- function(x, ..., query_type="READ") {
    if (is(x, "DenseTilDBArraySeed")) {
        return(x)
    }
    if (!is(x, "tiledb_dense")) {
        x <- tiledb_dense(x, ..., query_type=query_type)
    }
    new("DenseTileDBArraySeed", dim=dim(domain(schema(x))), dimnames=list(NULL, NULL), .array=x)
}

#' @export
#' @importFrom methods show
setMethod("show", "DenseTileDBArraySeed", function(object) {
    cat(sprintf("%i x %i DenseTileDBArraySeed object\n", nrow(object), ncol(object)))
})

.get_array <- function(x) x@.array

#' @export
#' @importFrom DelayedArray extract_array
setMethod("extract_array", "DenseTileDBArraySeed", function(x, index) {
    d <- dim(x)
    x <- .get_array(x)
    for (i in seq_along(index)) {
        if (is.null(index[[i]])) index[[i]] <- seq_len(d[i])
    }

    # Hack to overcome zero-length indices.
    d2 <- lengths(index)
    if (any(d2==0L)) {
        return(array(numeric(0), dim=d2))
    }

    # Hack to overcome non-contiguous subset error.
    o <- order(d2)
    least <- o[1]
    least.index <- index[[least]]

    re.o <- unique(sort(least.index))
    diff.from.last <- which(diff(re.o)!=1L)
    all.starts <- c(1L, diff.from.last+1L)
    all.ends <- c(diff.from.last, length(re.o))

    full <- lapply(d, seq_len)
    collected <- vector("list", length(all.starts))
    index[[least]] <- seq_along(re.o)

    for (i in seq_along(all.starts)) {
        cur.start <- re.o[all.starts[i]]
        cur.end <- re.o[all.ends[i]]

        full[[least]] <- cur.start:cur.end
        tmp <- do.call(`[`, c(list(x), full, list(drop=FALSE)))

        index[[least]] <- seq_len(cur.end-cur.start+1L)
        tmp <- do.call(`[`, c(list(tmp), index, list(drop=FALSE)))
        collected[[i]] <- tmp
    }

    # TODO: generalize to arrays.
    if (least==1L) {
        out <- do.call(rbind, collected)
        out <- out[match(least.index, re.o),,drop=FALSE]
    } else {
        out <- do.call(cbind, collected)
        out <- out[,match(least.index, re.o),drop=FALSE]
    }
    out
})

#' @export
#' @importFrom DelayedArray DelayedArray
DenseTileDBArray <- function(x, ..., query_type="READ") {
    DelayedArray(DenseTileDBArraySeed(x, ..., query_type=query_type))
}

#' @export
#' @importFrom DelayedArray DelayedArray new_DelayedArray
setMethod("DelayedArray", "DenseTileDBArraySeed",
    function(seed) new_DelayedArray(seed, Class="DenseTileDBMatrix")
)
