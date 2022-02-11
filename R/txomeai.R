#' Used to login to the Ocean Genomics server
#'
#' @importFrom getPass getPass
#' @param txomeai The connection object
#' @noRd
txomeai_login <- function(txomeai)
{
    login <- txomeai$url
    login$path <- "api/accounts/password-login"
    resp <- httr::POST(urltools::url_compose(login), 
        body=list(username=readline("Enter username: "), 
        password=getPass::getPass("Enter password: ")), 
        encode="json")
    if(resp$status_code == 200)
    {
        auth$is_authenticated <- TRUE
    }
    else if(resp$status_code == 401) 
    {
        auth$is_authenticated <- FALSE
        stop("Username or password are incorrect.")
    }
    else
    {
        auth$is_authenticated <- FALSE
        stop("Unexpected login http code: ", resp$status_code)
    }
}

#' Build a connection object to a RNA-seq analysis report
#'
#' @export
#' @importFrom urltools url_parse
#' @importFrom BiocFileCache bfccache
#' @importFrom utils read.csv
#' @param url The report URL
#' @return A constructed list connection object.
#'    \item{ls}{A summary table of all the available data.}
#'    \item{sample_meta}{A table with sample meta data.}
#'    \item{comparative_meta}{A table with comparative analysis meta data.}
#'    \item{url}{The URL used to access the API data.}
#'    \item{CAS}{The ID of the analysis.}
#'    \item{instance}{The ID of the report.}
#'    \item{dir}{The path to the query cache location.}
#'    \item{assets}{(Experimental) A table of svg plots in the report. }
#' @references https://txomeai.oceangenomics.com/
#' @examples dontrun
#' # Basic connection example
#' domain <- "https://txomeai.oceangenomics.com"
#' path <- paste0("api/pipeline-output/c444dfda-de51-4053-8cb7-881dd1b2734d/",
#'    "2021-10-25T185916/report/index.html")
#' report <- txomeai_connect(paste(domain, path, sep="/"))
#' # View all the available analysis tables
#' unique(report$ls$name)
#' # View sample meta data table
#' report$sample_meta
#' # View comparative analysis meta data table
#' report$comparative_meta
txomeai_connect <- function(url) 
{
    txomeai <- list()
    # Validate and construct URL
    txomeai$url <- urltools::url_parse(url)
    txomeai$CAS <- ""
    txomeai$instance <- ""
    if(!grepl("oceangenomics.com", txomeai$url$domain, fixed=TRUE) & 
        !grepl("transcriptome.ai", txomeai$url$domain, fixed=TRUE))
    {
        stop("URL domain is unexpected.")
    }

    parts <- unlist(strsplit(txomeai$url$path, "/"))
    if(length(parts) < 6) 
    {
        stop("URL path length is unexpected.")
    }
    meta <- get_cas_and_instance(parts)
    if(is.null(meta$cas) || is.null(meta$instance))
    {
        stop("Expected elements not found in path.")
    }
    workingCAS <- meta$cas
    workingInstance <- meta$instance
    txomeai$CAS <- workingCAS
    txomeai$instance <- workingInstance
    txomeai$dir <- init_dir(BiocFileCache::bfccache(), 
        workingCAS, workingInstance)
    # Test that the dir has been setup appropriately
    if(file.access(txomeai$dir, 0) != 0 || 
        file.access(txomeai$dir, 2) != 0 || 
        file.access(txomeai$dir, 4) != 0)
    {
        stop("Insufficent access to working directory:", txomeai$dir)
    }
    txomeai$url$path <- paste("api", "pipeline-output", workingCAS, workingInstance, "report", "json", sep="/")

    # Check for API data
    response <- download_file(txomeai, "data.csv")

    if(response$status_code == 200)
    {
        txomeai$data <- read.csv(response$path, header=TRUE)
    }
    else if (response$status_code == 404) 
    {
        stop("API data is not available for this report. ",
            "If it's an older report, re-run to generate API data.")
    }
    else 
    {
        stop("API query unexpected response: ", response$status_code)
    }
    return(update_glossary(txomeai))
}

#' Get data from the Txome.AI anaylsis report.
#'
#' @export
#' @import data.table
#' @param connection The connection object returned from txomeai_connect.
#' @param tableName A name column value from the ls table (see txomeai_connect ls table).
#' @param tableKey An optional key value from the ls table (see txomeai_connect ls table).
#' @return A data.table with all results.
#' @examples dontrun
#' domain <- "https://txomeai.oceangenomics.com"
#' path <- paste0("api/pipeline-output/c444dfda-de51-4053-8cb7-881dd1b2734d/",
#'    "2021-10-25T185916/report/index.html")
#' report <- txomeai_connect(paste(domain, path, sep="/"))
#' report_fastp <- txomeai_get(report, "FastpJSON")
txomeai_get <- function(connection, tableName, tableKey=NULL)
{
    to_return <- NULL
    header <- NULL
    types <- NULL
    # name and row_count must be initilized to pass R CMD check 
    row_count <- NULL
    name <- NULL
    sub <- NULL
    key <- NULL
    # Error checking
    if(!(tableName %in% connection$ls$name))
    {
        stop(sprintf("Table name '%s' not found.", tableName))
    }
    if(!is.null(tableKey) && !(tableKey %in% connection$ls$key))
    {
        stop(sprintf("Table key '%s' not found.", tableKey))
    }

    if(is.null(tableKey))
    {
        sub <- connection$ls[name == tableName, ]
    } else if (is.na(tableKey)) {
        sub <- connection$ls[name == tableName & is.na(key), ]
    } else {
        sub <- connection$ls[name == tableName & key == tableKey,]
    }

    if(nrow(sub) == 0) {
        stop("Requested data not found.")
    }
    
    # Handle meta data and assets
    if(nrow(sub) == 1 && is.na(sub$key[[1]]))
    {
        return(data.table(fetch(sub$name[[1]], sub$key[[1]], connection)))
    } else if (nrow(sub) == 1 && sub$key[[1]] == "assets"){
        return(txomeai_display(connection, sub[1,]))
    }
    
    # Get raw list results for each row and add to data.table as column 'get'
    sub$get <- apply(sub, 
        FUN=function(x,r){return(fetch(x["name"], x["key"], r));}, 
        MARGIN=1, connection)
    # Return results if data_type isn't table
    if(sub$get[[1]]$data_type != "table")
    {
        return(sub)
    }
    # Filter all empty table get results
    get_row_count <- function(x){ return(nrow(x[[1]]$data$rows));}
    sub[,row_count := get_row_count(get), by=key]
    if(nrow(sub[row_count > 0,]) == 0)
    {
        to_return <- data.table(matrix(ncol=length(sub$get[[1]]$data$header), nrow=0))
        colnames(to_return) <- sub$get[[1]]$data$header
        return(to_return)
    }
    sub <- sub[row_count > 0,]

    # Build the output data.table from the raw data rows
    to_return <- data.table(do.call("rbind", lapply(sub$get, FUN=function(x){return(x$data$rows);})))
    # Build the key column for the output table
    key_col <- rep(sub$key, sub$row_count)
    # Set column types
    num_cols <- colnames(to_return)[sub$get[[1]]$data$types == "numeric"]
    if(length(num_cols) > 0) {
        to_return[, (num_cols) := lapply(.SD, as.numeric), .SDcols = num_cols]
    }
    # Set column names
    colnames(to_return) <- sub$get[[1]]$data$header
    # Add id column
    to_return$key <- key_col
    return(to_return) 
}


#' Display a svg diagram.
#'
#' @export
#' @import magick
#' @description An experimental function to display a report SVG in whatever way is available.
#' @param connection The connection object.
#' @param row A row from the connection$assets table.
#' @return The SVG path if download is successful, null otherwise.
#' @examples dontrun
#' domain <- "https://txomeai.oceangenomics.com"
#' path <- "api/pipeline-output/c444dfda-de51-4053-8cb7-881dd1b2734d/2021-10-25T185916/report/index.html"
#' report <- txomeai_connect(paste(domain, path, sep="/"))
#' txomeai_display(report, report$assets[1,])
txomeai_display <- function(connection, row)
{
    if(!all(colnames(connection$ls) %in% colnames(row)))
    {
        print("Input row did not have the expected format")
        return()
    }
    r <- download_asset(connection, row$name)
    if(r$status_code != 200)
    {
        cat("Failed to download svg file with HTML code: ", r$status_code, "\n")
        return()
    }
    img <- image_read(r$path)
    displayed <- tryCatch(
        {
            image_display(img)
            TRUE
        },
        error=function(cond)
        {
            message("image_display: ", cond)
            return(FALSE)
        },
        warning=function(cond)
        {
            message("image_display: ", cond)
            return(TRUE)
        }
    )
    if(displayed)
    {
        return(r$path)
    }
    displayed <- tryCatch(
        {
            image_browse(img)
            TRUE
        },
        error=function(cond)
        {
            message("image_browse: ", cond)
            return(FALSE)
        },
        warning=function(cond)
        {
            message("image_browse: ", cond)
            return(TRUE)
        }
    )
    return(r$path)
}
