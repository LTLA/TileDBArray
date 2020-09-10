#' @importFrom S4Vectors isSingleString
.common_checks <- function(object) {
    msg <- character(0)

    d <- dim(object)
    if (!all(d >= 0L)) {
        msg <- c(msg, "'dim' must contain non-negative integers")
    }

    if (!isSingleString(object@path)) {
        msg <- c(msg, "'path' must be a non-NA string")
    }

    if (!isSingleString(object@attr)) {
        msg <- c(msg, "'attr' must be a non-NA string")
    }

    if (!isSingleString(object@type)) {
        msg <- c(msg, "'type' must be a non-NA string")
    }

    s <- object@sparse
    if (length(s)!=1L || is.na(s)) {
        msg <- c(msg, "'sparse' must be a non-NA logical scalar")
    }

    msg
}

.pack_dimnames <- function(x) {
    as.integer(memCompress(serialize(x, NULL), "gzip"))
}

.unpack_dimnames <- function(x) {
    unserialize(memDecompress(as.raw(x), "gzip"))
}
