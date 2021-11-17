\name{txomeai_update_glossary}
\alias{txomeai_update_glossary}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
%%  ~~function to do ... ~~
Updates the gloassary of available data.
}
\description{
%%  ~~ A concise (1-5 lines) description of what the function does. ~~
This function will redownload all index files in case an update happened recently.
}
\usage{
txomeai_update_glossary(txomeai)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{txomeai}{
%%     ~~Describe \code{txomeai} here~~
The report connection object returned by txomeai_connect.
}
}
\details{
%%  ~~ If necessary, more details than the description above ~~
details
}
\value{
The updated report connection object.
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

## The function is currently defined as
function (txomeai) 
{
    txomeai$meta = vector("list", length(txomeai$data$app))
    txomeai$sample = vector("list", length(txomeai$data$app))
    for (i in 1:length(txomeai$data$app)) {
        app = txomeai$data[i, "app"]
        r = download_file(txomeai, paste(app, "meta.csv", sep = "."))
        if (r$status_code == 200 & file.info(r$path)$size > 0) {
            txomeai$meta[[i]] = read.csv(r$path, header = T)
        }
        else {
            txomeai$meta[[i]] = data.table()
        }
        r = download_file(txomeai, paste(app, "sample.csv", sep = "."))
        if (r$status_code == 200 & file.info(r$path)$size > 0) {
            txomeai$sample[[i]] = read.csv(r$path, header = T)
        }
        else {
            txomeai$sample[[i]] = data.table()
        }
    }
    names(txomeai$meta) = txomeai$data$app
    names(txomeai$sample) = txomeai$data$app
    txomeai$ls = txomeai_ls(txomeai)
    return(txomeai)
  }
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
