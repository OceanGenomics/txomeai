\name{txomeai_connect}
\alias{txomeai_connect}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
%%  ~~function to do ... ~~
Build a connection object to an analysis report
}
\description{
%%  ~~ A concise (1-5 lines) description of what the function does. ~~
Builds the connection object to a report used for all queries.
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
Change or delete this directory if you want your API queries to re-run, this should only be required very rarely. 
}
}
\details{
%%  ~~ If necessary, more details than the description above ~~
This function requires user interaction to input your https://txomeai.oceangenomics.com/ username and password.
}
\value{
  The list returned from this function is used for all API operations and is generally not useful to the user.
  \item{url}{The URL used to access the API data.}
  \item{CAS}{The ID of the analysis.}
  \item{instance}{The ID of the report.}
  \item{dir}{The path to the query save location.}
  \item{data}{A table of all the report page results.}
  \item{meta}{A list of tables associated to each report page.}
  \item{sample}{A list of tables associated to each report page.}
  \item{ls}{A summary table of all the available data.}
}
\references{
https://txomeai.oceangenomics.com/
}
\author{
%%  ~~who you are~~
Joshua P. Katz
}
\examples{
domain = "https://txomeai.oceangenomics.com"
path = "api/pipeline-output/c444dfda-de51-4053-8cb7-881dd1b2734d/2021-10-25T185916/report/index.html"
report = txomeai_connect(paste(domain, path, sep="/"))
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
