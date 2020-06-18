#include "Rcpp.h"
#include <algorithm>

// [[Rcpp::export(rng=false)]]
Rcpp::List remap_indices(Rcpp::List starts, Rcpp::List ends, Rcpp::List positions) {
    const size_t n_dims = starts.size();

    // Setting up a stack of integer vectors for quick reference.
    std::vector<Rcpp::IntegerVector> Starts, Ends, Positions;
    for (size_t d = 0; d < n_dims; ++d) {
        Starts.push_back(starts[d]);
        Ends.push_back(ends[d]);
        Positions.push_back(positions[d]);
    }

    // Figuring out the total required length.
    const size_t n_values = (n_dims ? Starts.front().size() : 0);
    Rcpp::IntegerVector n_copies(n_values, 1);

    for (size_t d = 0; d < n_dims; ++d) {
        auto curstarts = Starts[d];
        auto curends = Ends[d];
        auto sIt = curstarts.begin();
        auto eIt = curends.begin();

        for (size_t v = 0; v < n_values; ++v, ++sIt, ++eIt) {
            n_copies[v] *= (*eIt - *sIt);            
        }
    }

    const size_t n_output = std::accumulate(n_copies.begin(), n_copies.end(), 0);

    // Creating the output vectors.
    Rcpp::IntegerMatrix out_indices(n_output, n_dims);
    std::vector<Rcpp::IntegerMatrix::iterator> out_iIt;
    {   
        auto startIt = out_indices.begin();
        for (size_t d = 0; d<n_dims; ++d, startIt += n_output) {
            out_iIt.push_back(startIt);
        }
    }

    // Expanding the entries.
    for (size_t v = 0; v < n_values; ++v) {
        int rep_seq = n_copies[v];
        int rep_val = 1;

        for (size_t d = 0; d < n_dims; ++d) {
            auto& iIt = out_iIt[d];
            const auto s = Starts[d][v], e = Ends[d][v];
            const auto len = e - s;
            rep_seq /= len;

            auto posStart = Positions[d].begin() + s, posEnd = Positions[d].begin() + e;
            for (auto rs = 0; rs < rep_seq; ++rs) {
                for (auto posCopy = posStart; posCopy != posEnd; ++posCopy, iIt += rep_val) {
                    std::fill(iIt, iIt + rep_val, *posCopy);
                }
            }

            rep_val *= len;
        }
    } 

    return Rcpp::List::create(
        Rcpp::Named("indices") = out_indices,
        Rcpp::Named("expand") = n_copies 
    );
}
