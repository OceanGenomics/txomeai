\name{txomeai_get}
\alias{txomeai_get}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
%%  ~~function to do ... ~~
Get a single row query from txomeai_ls.
}
\description{Returns the data from a single txomeai_ls row.}
\usage{
txomeai_get(name, key, txomeai)
}
\arguments{
  \item{name}{A name column from the table returned by txomeai_ls.}
  \item{key}{A single row from the table returned by txomeai_ls.}
  \item{txomeai}{The connection object returned from txomeai_connect.}
}
\value{
  Returns a list with the query data.
  \item{sample_name}{The input sample filename.}
  \item{table_name}{The 'name' from the txomeai_ls table. }
  \item{sample_id}{The automatically assigned sample id string. }
  \item{data_type}{A string with values "json", "text", or "table"}
  \item{data}{The desired data.}
}
\examples{
domain = "https://txomeai.oceangenomics.com"
path = "api/pipeline-output/c444dfda-de51-4053-8cb7-881dd1b2734d/2021-10-25T185916/report/index.html"
report = txomeai_connect(paste(domain, path, sep="/"))
report_ls = txomeai_ls(report)
report_samples = txomeai_get(report_ls[report_ls$name == "sampleName",], report)
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
