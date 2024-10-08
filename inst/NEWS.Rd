\name{TileDBArrayNews}
\title{TileDBArray News}
\encoding{UTF-8}

\section{Version 1.16.0}{\itemize{
\item Minor fix for \code{as.data.frame=} deprecation in \code{tiledb_array()}.

\item Support other datatypes for the dimensions and storage when configuring a \code{TileDBRealizationSink}.
This is achieved via the new \code{storagetype=} and \code{dimtype=} arguments.
Also added \code{getTileDBDimType()} and \code{setTileDBDimType()} to globally define the choice of dimension datatype.

\item Bugfix to \code{TileDBArraySeed} to correctly handle dimension domains that do not start at 1.
This requires a modification to the class to record the domain offset.

\item Added a \code{offset=} option to \code{TileDBRealizationSink()} to create arrays with dimension domains that do not start at 1.
This requires a modification to the associated class to record the domain offset.
}}

\section{Version 1.2.0}{\itemize{
\item
Add more schema options during TileDBArray construction.

\item 
Use \code{tiledb_array()} for both sparse and dense arrays.
}}

\section{Version 1.0.0}{\itemize{
\item
New package \pkg{TileDBArray} implementing a DelayedArray backend for TileDB.
}}

