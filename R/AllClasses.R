#' @export
setClass("TileDBArraySeed", 
    slots=c(
        dim="integer",
        dimnames="list", 
        path="character",
        sparse="logical",
        attr="character",
        type="character",
        extent="integer",
        offset="integer"
    )
)

#' @export
setClass("TileDBArray", contains="DelayedArray", slots=c(seed="TileDBArraySeed"))

#' @export
setClass("TileDBMatrix", contains="DelayedMatrix", slots=c(seed="TileDBArraySeed"))

#' @export
setClass("TileDBRealizationSink",
    contains="RealizationSink",
    slots=c(dim="integer", path="character", sparse="logical", attr="character", type="character", offset="integer")
)
