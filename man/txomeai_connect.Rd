\name{txomeai_connect}
\alias{txomeai_connect}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
%%  ~~function to do ... ~~
Build a connection object to an analysis report
}
\description{
%%  ~~ A concise (1-5 lines) description of what the function does. ~~
Build the connection object to a report used to access all data from the report.
The list returned from this function is used for all API operations but it not specifically useful to the user.
This function also requires user interaction to input username and password.
}
\usage{
txomeai_connect(url, dir = ".")
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{url}{
%%     ~~Describe \code{url} here~~
A string with the url of the report to connect to.
}
  \item{dir}{
%%     ~~Describe \code{dir} here~~
The local directory path to save cached data to.
Delete this directory if you want your API queries to re-run, this should only be required very rarely. 
}
}
\details{
%%  ~~ If necessary, more details than the description above ~~
details
}
\value{
    The return value of this function is unimportant to the user.
}
\references{
https://txomeai.oceangenomics.com/
}
\author{
%%  ~~who you are~~
Joshua P. Katz
}
\note{
%%  ~~further notes~~
note
}

%% ~Make other sections like Warning with \section{Warning }{....} ~

\seealso{
%% ~~objects to See Also as \code{\link{help}}, ~~~
seealso
}
\examples{
##---- Should be DIRECTLY executable !! ----
##-- ==>  Define data, use random,
##--	or do  help(data=index)  for the standard data sets.
report = txomeai_connect("https://txomeai.oceangenomics.com/api/pipeline-output/c444dfda-de51-4053-8cb7-881dd1b2734d/2021-10-25T185916/report/index.html")
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
