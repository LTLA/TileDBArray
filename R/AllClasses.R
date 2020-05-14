#' @export
setClass("TileDBArraySeed", slots=c(dim="integer", dimnames="list", path="character"))

#' @export
#' @importClassesFrom DelayedArray DelayedMatrix
setClass("TileDBArray", contains="DelayedArray", slots=c(seed="TileDBArraySeed"))

#' @export
#' @importClassesFrom DelayedArray DelayedMatrix
setClass("TileDBMatrix", contains="DelayedMatrix", slots=c(seed="TileDBArraySeed"))

#' @export
#' @importClassesFrom DelayedArray RealizationSink
setClass("TileDBRealizationSink",
    contains="RealizationSink",
    slots=c(dim="integer", type="character", path="character")
)
