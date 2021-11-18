\name{txomeai_get_all}
\alias{txomeai_get_all}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
%%  ~~function to do ... ~~
Used to collect all queries with the same name into the same object.
}
\description{
%%  ~~ A concise (1-5 lines) description of what the function does. ~~
This function collects all queries from txomeai_ls with the provided name. 
}
\usage{
txomeai_get_all(txomeai, tableName)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{txomeai}{
%%     ~~Describe \code{txomeai} here~~
The connection object returned by txomeai_connect.
}
  \item{tableName}{
%%     ~~Describe \code{tableName} here~~
A string that matches the 'name' column in txomeai_ls.
}
}
\value{
A data.table or list with all query results.
  \item{list}{A list with keys as the name and the raw query data.}
  \item{data.table}{A data.table with all non-empty results and a key column.}
}
\examples{
domain = "https://txomeai.oceangenomics.com"
path = "api/pipeline-output/c444dfda-de51-4053-8cb7-881dd1b2734d/2021-10-25T185916/report/index.html"
report = txomeai_connect(paste(domain, path, sep="/"))
report_ls = txomeai_ls(report)
report_sv = txomeai_get_all(report, "StructuralVariants")
}
% Add one or more standard keywords, see file 'KEYWORDS' in the
% R documentation directory (show via RShowDoc("KEYWORDS")):
% \keyword{ ~kwd1 }
% \keyword{ ~kwd2 }
% Use only one keyword per line.
% For non-standard keywords, use \concept instead of \keyword:
% \concept{ ~cpt1 }
% \concept{ ~cpt2 }
% Use only one concept per line.
