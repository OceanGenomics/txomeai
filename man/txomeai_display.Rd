\name{txomeai_display}
\alias{txomeai_display}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
%%  ~~function to do ... ~~
Display a svg diagram.
}
\description{
%%  ~~ A concise (1-5 lines) description of what the function does. ~~
This function will display a downloaded SVG in whatever way is available.
}
\usage{
txomeai_display(txomeai, ls_row)
}
\arguments{
  \item{txomeai}{The connection object that cookie has expired. }
  \item{ls_row}{An asset row from the txomeai_ls table. }
}
\value{
  The SVG path if download is successful, null otherwise.
}
\examples{
domain = "https://txomeai.oceangenomics.com"
path = "api/pipeline-output/c444dfda-de51-4053-8cb7-881dd1b2734d/2021-10-25T185916/report/index.html"
report = txomeai_connect(paste(domain, path, sep="/"))
report_ls = txomeai_ls(report)
assets = report_ls[report_ls$key == "assets", ]
txomeai_display(report, assets[1,])
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
