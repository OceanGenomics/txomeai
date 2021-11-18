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
\arguments{
  \item{txomeai}{The connection object that cookie has expired. }
}
\value{
  If TRUE then the session is logged in, FALSE otherwise.
}
\references{
%% ~put references to the literature/web site here ~
https://txomeai.oceangenomics.com/
}
\examples{
domain = "https://txomeai.oceangenomics.com"
path = "api/pipeline-output/c444dfda-de51-4053-8cb7-881dd1b2734d/2021-10-25T185916/report/index.html"
report = txomeai_connect(paste(domain, path, sep="/"))
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
