\name{txomeai_get}
\alias{txomeai_get}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
%%  ~~function to do ... ~~
Get a single row query from txomeai_ls.
}
\description{
%%  ~~ A concise (1-5 lines) description of what the function does. ~~
Returns the data from a single txomeai_ls row.
}
\usage{
txomeai_get(txomeai, ls_row)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{txomeai}{
%%     ~~Describe \code{txomeai} here~~
The connection object returned from txomeai_connect.
}
  \item{ls_row}{
%%     ~~Describe \code{ls_row} here~~
A single row from the table returned by txomeai_ls.
}
}
\details{
%%  ~~ If necessary, more details than the description above ~~
details
}
\value{
  Returns the raw data for the provided query.
%%  ~Describe the value returned
%%  If it is a LIST, use
%%  \item{comp1 }{Description of 'comp1'}
%%  \item{comp2 }{Description of 'comp2'}
%% ...
}
\references{
%% ~put references to the literature/web site here ~
references
}
\author{
%%  ~~who you are~~
Joshua P. Katz
}
\note{
%%  ~~further notes~~
note
}
\seealso{
%% ~~objects to See Also as \code{\link{help}}, ~~~
seealso
}
\examples{
##---- Should be DIRECTLY executable !! ----
##-- ==>  Define data, use random,
##--	or do  help(data=index)  for the standard data sets.

report = txomeai_connect("https://txomeai.oceangenomics.com/api/pipeline-output/c444dfda-de51-4053-8cb7-881dd1b2734d/2021-10-25T185916/report/index.html", dir = ".")
report_ls = txomeai_ls(report)
report_samples = txomeai_get(report, report_ls[report_ls$name == "sampleName",])
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
