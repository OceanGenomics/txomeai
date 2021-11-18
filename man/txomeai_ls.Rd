\name{txomeai_ls}
\alias{txomeai_ls}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
%%  ~~function to do ... ~~
This function returns a table of all available data in the report
}
\description{
Returns a table with columns key, name, and description.
Each row represents a data query available in the report. 
}
\usage{
txomeai_ls(txomeai)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{txomeai}{The report connection object}
}
\value{
  A data.table that each row represents a data query from the report.
}
\references{
%% ~put references to the literature/web site here ~
https://txomeai.oceangenomics.com/
}
\examples{
domain = "https://txomeai.oceangenomics.com"
path = "api/pipeline-output/c444dfda-de51-4053-8cb7-881dd1b2734d/2021-10-25T185916/report/index.html"
report = txomeai_connect(paste(domain, path, sep="/"))
report_ls = txomeai_ls(report)
head(report_ls)
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
