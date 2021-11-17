\name{txomeai_login}
\alias{txomeai_login}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
%%  ~~function to do ... ~~
This function logs into oceangenomics.com with your username and password.
}
\description{
%%  ~~ A concise (1-5 lines) description of what the function does. ~~
This function is only needed if your cookie with oceangenomics.com has expired and you want to refresh it.
}
\usage{
txomeai_login(txomeai)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{txomeai}{
%%     ~~Describe \code{txomeai} here~~
The connection object that cookie has expired. 
}
}
\details{
%%  ~~ If necessary, more details than the description above ~~
details
}
\value{
  If TRUE then the session is logged in, FALSE otherwise.
%%  ~Describe the value returned
%%  If it is a LIST, use
%%  \item{comp1 }{Description of 'comp1'}
%%  \item{comp2 }{Description of 'comp2'}
%% ...
}
\references{
%% ~put references to the literature/web site here ~
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
report = txomeai_connect("https://txomeai.oceangenomics.com/api/pipeline-output/c444dfda-de51-4053-8cb7-881dd1b2734d/2021-10-25T185916/report/index.html", dir = ".")
txomeai_login(report)
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
