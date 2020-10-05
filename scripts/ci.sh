#!/bin/bash

## -- Env vars to set R for 'R-devel' in container, and no suggested packages
export RHOME=/usr/local/lib/R
export _R_CHECK_FORCE_SUGGESTS_=false

## -- Determine current package name and version
PKG_NAME=$(awk '/Package:/ {print $2}' DESCRIPTION)
PKG_VER=$(awk '/Version:/ {print $2}' DESCRIPTION)
PKG_TGZ="${PKG_NAME}_${PKG_VER}.tar.gz"

## -- Update packages and install build dependencies
apt update -qq
apt upgrade -q
apt install -y --no-install-recommends libpcre2-dev liblzma-dev libbz2-dev libicu-dev libblas-dev liblapack-dev

## -- Build littler under r-devel and link it as to /usr/local/bin (shadowing the existing one)
RDscript -e 'install.packages("littler")'
ln -sf /usr/local/lib/R/site-library/littler/bin/r /usr/local/bin/r

## -- use littler and littler scripts to install BioConductor development packages
install.r Rcpp tiledb BiocManager Matrix lattice testthat
r -l BiocManager -e 'install(version="devel", ask=FALSE, update=TRUE)'
r -l BiocManager -e 'install(c("DelayedArray", "S4Vectors"), ask=FALSE)'

## -- now that everything is prepared, create a tarball and check it
RD CMD build --no-manual --no-build-vignettes .
RD CMD check --no-manual --no-vignettes ${PKG_TGZ}
