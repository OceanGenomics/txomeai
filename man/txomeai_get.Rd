\name{txomeai_get}
\alias{txomeai_get}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
%%  ~~function to do ... ~~
Get data from the Txome.AI anaylsis report.
}
\description{Returns the data from a single txomeai_ls row.}
\usage{
txomeai_get(txomeai, tableName, tableKey)
}
\arguments{
  \item{txomeai}{The connection object returned from txomeai_connect.}
  \item{tableName}{A name column from the table returned by txomeai_ls.}
  \item{tableKey}{A single row from the table returned by txomeai_ls.}
}
\value{
  Returns a data.table with the requested results.
}
\examples{
domain = "https://txomeai.oceangenomics.com"
path = "api/pipeline-output/c444dfda-de51-4053-8cb7-881dd1b2734d/2021-10-25T185916/report/index.html"
report = txomeai_connect(paste(domain, path, sep="/"))
report_ls = txomeai_ls(report)
report_samples = txomeai_get(report, "sampleName")
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
