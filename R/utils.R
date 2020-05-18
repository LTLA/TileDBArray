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

.type.mapping <- c(double="FLOAT64", integer="INT32", logical="INT32")

.rev.type.mapping <- c(
    FLOAT64="double", FLOAT32="double", UINT32="double", INT64="double", UINT64="double",
    INT32="integer", INT16="integer", INT8="integer", UINT8="integer", UINT16="integer"
)

#' @importFrom base64enc base64encode 
.pack64 <- function(x) {
    con <- rawConnection(raw(0), "r+")
    on.exit(close(con))
    serialize(x, con)
    base64encode(memCompress(rawConnectionValue(con), "gzip"))
}

#' @importFrom base64enc base64decode
.unpack64 <- function(x) {
    unserialize(memDecompress(base64decode(x), "gzip"))
}
