#' Gloabl used to only authenticate when necessary
#' @noRd 
auth <- new.env()
auth$is_authenticated = FALSE

#' Download an API file
#' 
#' @param filename the filename of the file to download.
#' @param key a string that uniquely identifies the correct file. 
#' @return a list with $status_code and $path to downloaded file.
#' @noRd
download_file <- function(txomeai, filename, key="", overwrite=FALSE)
{
    downloadURL <- txomeai$url
    downloadURL$path <- paste(downloadURL$path, filename, sep="/")
    key_dir <- txomeai$dir
    if(!is.na(key) && nchar(key) > 0)
    {
        key_dir <- file.path(txomeai$dir, key)
        downloadURL$path <- paste(txomeai$url$path, key, filename, sep="/")
    }
    if(!dir.exists(key_dir))
    {
        dir.create(key_dir)
    }
    outfile <- file.path(key_dir, filename)
    resp <- NULL
    if(!file.exists(outfile) || overwrite)
    {
        # Only require authentication when accessing a non-cached file
        if(!auth$is_authenticated)
        {
            txomeai_login(txomeai)
        }
        r <- httr::GET(urltools::url_compose(downloadURL), httr::write_disk(outfile, overwrite=TRUE))
        if(r$status_code != 200 && file.exists(outfile))
        {
            file.remove(outfile)
        }
        resp <- list(status_code=r$status_code, path=outfile)
    }
    else
    {
        resp <- list(status_code=200, path=outfile)
    }
    return(resp)
}

#' Download a SVG file
#'
#' @param txomeai the connection object
#' @param filename the filename of the file to download.
#' @return a list with $status_code and $path to downloaded file.
#' @noRd
download_asset <- function(txomeai, filename)
{
    asset <- txomeai
    asset$url$path <- sub("json", "assets", asset$url$path, fixed=TRUE)
    return(download_file(asset, filename))
}

#' Used to collect the raw message data parsed into R list
#'
#' @importFrom jsonlite fromJSON
#' @param name The name column value from the ls table
#' @param key The key column value from the ls table
#' @param txomeai The report connection object
#' @return a list containing the raws data
#' @noRd
fetch <- function(name, key, txomeai)
{
    if(is.na(key))
    {
        return(get_sample_meta(txomeai, name))
    }
    file <- name
    if(substring(name, nchar(name)-7) != ".json.gz")
    {
        file <- paste(name, "json.gz", sep=".")
    }

    r <- download_file(txomeai, file, key)
    if(r$status_code == 200)
    {
        sub <- jsonlite::fromJSON(r$path)
        return(sub)
    }
    else
    {
        message("Failed to download: ", r$status_code)
        return(NULL)
    }
    return(r)
}

#' Determine if currently logged into txomeai
#'
#' @import httr
#' @param txomeai the connection object
#' @return true if connected, false otherwise
#' @noRd
test_auth <- function(txomeai)
{
    list_cas <- txomeai$url
    list_cas$path <- "api/accounts/current"
    resp <- httr::GET(urltools::url_compose(list_cas))
    if(resp$status_code == 200)
    {
        return(TRUE)
    }
    if(resp$status_code == 401 || resp$status_code == 404)
    {
        return(FALSE)
    } 
    message("Unexpected HTTP response: ", resp$status_code, "\n")
    return(FALSE)
}

#' Initilize the dir directory in a cross-platform manner
#'
#' @param dir the provided dir path
#' @param cas the connected cas
#' @return the normalized dir path
#' @noRd
init_dir <- function(dir, cas, inst)
{
    dir <- gsub("/$|\\\\$", "", dir, perl=TRUE)
    if(!dir.exists(dir))
    {
        dir.create(dir)
    }
    cas_dir <- file.path(dir, cas)
    if(!dir.exists(cas_dir))
    {
        dir.create(cas_dir)
    }
    inst_dir <- file.path(cas_dir, inst)
    if(!dir.exists(inst_dir))
    {
        dir.create(inst_dir)
    }
    return(inst_dir)
}

#' Use to return sample table raw results
#'
#' @param txomeai the connected report object
#' @param table the name of the table to download.
#' @return NULL or a data.table with sample meta data
#' @noRd
get_sample_meta <- function(txomeai, table)
{
    col_index <- vapply(txomeai$sample, FUN=function(x){return(table %in% colnames(x));}, FUN.VALUE=TRUE)
    if(all(!col_index))
    {
        return(NULL)
    }
    else 
    {
        return(txomeai$sample[col_index][[1]][,c("sample", table)])
    }
}

#' Use to get the CAS and Instance ID from url
#'
#' @param url_path A vector of the urls path elements
#' @return A list containing "cas" and "instance"
#' @noRd
get_cas_and_instance <- function(url_path) 
{
    to_return <- list(cas=NULL, instance=NULL)
    cas_i <- grep("\\w+-\\w+-\\w+-\\w+-\\w+", url_path, perl=TRUE)
    inst_i <- grep("\\d+-\\d+-\\d+T\\d+", url_path, perl=TRUE)
    if(cas_i > 0)
    {
        to_return$cas <- url_path[cas_i]
    } 
    if(inst_i > 0)
    {
        to_return$instance <- url_path[inst_i]
    }
    return(to_return)
}

#' Contruct the connection list object to the local test server
#'
#' @param url The report URL to connect to
#' @param dir (OPTIONAL) The directory to save data to.
#' @return The constructed connection object
#' @noRd
local_connect <- function(url, dir=".") 
{
    txomeai <- list()
    # Validate and construct URL
    txomeai$url <- urltools::url_parse(url)
    txomeai$CAS <- ""
    txomeai$instance <- ""

    parts <- unlist(strsplit(txomeai$url$path, "/"))
    workingCAS <- parts[2]
    workingInstance <- parts[3]
    txomeai$CAS <- workingCAS
    txomeai$instance <- workingInstance
    txomeai$dir <- init_dir(dir, workingCAS, workingInstance)
    # Test that the dir has been setup appropriately
    if(file.access(txomeai$dir, 0) != 0 || file.access(txomeai$dir, 2) != 0 || file.access(txomeai$dir, 4) != 0)
    {
        message("Insufficent access to working directory:", txomeai$dir)
        return()
    }
    parts[length(parts)] <- "json"
    txomeai$url$path <- paste(parts, collapse="/")
    # Check for API data
    response <- download_file(txomeai, "data.csv")

    if(response$status_code == 200)
    {
        txomeai$data <- read.csv(response$path, header=TRUE)
    }
    else if (response$status_code == 404) 
    {
        message("API data is not available for this report. If it's an older report, re-run to generate API data.")
        return()
    }
    else 
    {
        message("Query error: ", response$status_code, "\n")
        return()
    }
    return(update_glossary(txomeai))
}


#' This function returns a table of all available data in the report
#'
#' @description
#' Returns a table with columns key, name, and description.
#' Each row represents a data query available in the report. 
#' @param txomeai The report connection object
#' @return A data.table that each row represents a data query from the report.
#' @examples
#' domain <- "https://txomeai.oceangenomics.com"
#' path <- "api/pipeline-output/c444dfda-de51-4053-8cb7-881dd1b2734d/2021-10-25T185916/report/index.html"
#' report <- txomeai_connect(paste(domain, path, sep="/"))
#' head(report$ls)
#' @noRd
build_ls <- function(txomeai) 
{
    if(!is.null(txomeai$ls))
    {
        return(txomeai$ls)
    }
    table_header <- c("key", "name", "description", "path")
    tables <- data.table(matrix(ncol=4,nrow=0))
    for(s in txomeai$sample)
    {
        if(length(colnames(s)) == 0)
        {
            next
        }
        for(c in colnames(s))
        {
            for(i in seq_len(length(s[,1])))
            {
                # Testing if the CAS is in the path works for web analysis
                # Testing if the value path starts with sample works for local analysis
                if(grepl(txomeai$CAS, s[i,c], fixed=TRUE) || grepl(paste0(s[i,"sample"],"/"), s[i,c], fixed=TRUE))
                {
                    tables <- rbind(tables, list(s[i,"sample"], c, s[i,"sampleName"], s[i,c]))
                }
                else
                {
                    tables <- rbind(tables, list(NA, c, "Meta data", NA))
                    break
                }
            }
        }
    }
    for(m in txomeai$meta)
    {
        if(length(m$tableName) > 0)
        {
            for(i in seq_len(length(m$tableName)))
            {
                if(m$stepName[i] == "all") {
                    tables <- rbind(tables, list(m$stepName[i], m$tableName[i], "Step run against all samples", m$apiQueryPath[i]))
                } else if (m$stepName[i] == "assets") {
                    parts = unlist(strsplit(m$apiQueryPath[i], "/", fixed=TRUE))
                    tables <- rbind(tables, list(m$stepName[i], parts[length(parts)], "An image file", m$apiQueryPath[i]))
                } else {
                    tables <- rbind(tables, list(m$stepName[i], m$tableName[i], sprintf("%s vs %s", m$set1[i], m$set2[i]), m$apiQueryPath[i]))
                }
            }
        }
    }
    tables <- unique(tables)
    colnames(tables) <- table_header
    tables$row <- seq_len(length(tables$key))
    return(unique(tables))
}

#' Use to build our file index for a report
#'
#' @param txomeai The report connection object
#' @return the connection object with the built index
#' @noRd
update_glossary <- function(txomeai)
{
    txomeai$meta <- vector("list", length(txomeai$data$app))
    txomeai$sample <- vector("list", length(txomeai$data$app))
    for(i in seq_len(length(txomeai$data$app)))
    {
        txomeai$meta[[i]] <- data.frame()
        txomeai$sample[[i]] <- data.frame()
        app <- txomeai$data[i, "app"]
        r <- download_file(txomeai, paste(app, "meta.csv", sep="."))
        if(r$status_code == 200 & file.info(r$path)$size > 0)
        {
            m <- read.csv(r$path, header=TRUE)
            if(all(c("tableName", "stepName", "filename", "set1", "set2") %in% colnames(m))){
                txomeai$meta[[i]] <- m
            } 
        }

        r <- download_file(txomeai, paste(app, "sample.csv", sep="."))
        if(r$status_code == 200 & file.info(r$path)$size > 0)
        {
            s <- read.csv(r$path, header=TRUE)
            if(all(c("sample","sampleName") %in% colnames(s))){
                txomeai$sample[[i]] <- s
            }
        }
    }
    names(txomeai$meta) <- txomeai$data$app
    names(txomeai$sample) <- txomeai$data$app
    txomeai$ls <- build_ls(txomeai)
    txomeai$sample_meta <- build_meta_table(txomeai)
    txomeai$comparative_meta <- build_comp_table(txomeai)
    txomeai$ls <- txomeai$ls[!is.na(txomeai$ls$key),]
    txomeai$assets <- txomeai$ls[txomeai$ls$key == "assets",]
    txomeai$ls <- txomeai$ls[txomeai$ls$key != "assets",]
    txomeai$data <- NULL
    txomeai$sample <- NULL
    txomeai$meta <- NULL
    return(txomeai)
}

#' Construct the comparative analysis meta data table.
#'
#' @param txomeai The in-construction connection object. 
#' @return a comparative analysis meta data.table
#' @noRd
build_comp_table <- function(txomeai)
{
    row_count = vapply(txomeai$meta, FUN=nrow, FUN.VALUE=0)
    meta_set = txomeai$meta[row_count > 0]
    if(length(meta_set) == 0)
    {
        return(data.frame(stepName=c(), set1=c(), set2=c()))
    }
    to_return = meta_set[[1]][,c("stepName", "set1", "set2")]
    for(m in meta_set)
    {
        to_return = rbind(to_return, m[,c("stepName", "set1", "set2")])
    }
    return(unique(to_return))
}

#' Construct the sample related meta data table.
#'
#' @param txomeai The connection object
#' @return the sample meta data.table
#' @noRd
build_meta_table <- function(txomeai)
{
    name <- NULL
    meta_rows <- txomeai$ls[is.na(key),]
    meta_rows <- meta_rows[name != "sample" & name != "sampleName",]
    metaData <- get_sample_meta(txomeai, "sampleName")
    for(m in unlist(meta_rows[,"name"]))
    {
        metaData <- merge(metaData, get_sample_meta(txomeai, m), by="sample")
    }
    return(metaData)
}

#' Uses to test the API
#'
#' @param url The url to connect to
#' @param output The file to write results to
#' @return the connection object with the built index
#' @noRd
test_txomeai <- function(url, output="Results.Rhistory")
{
    key <- NULL
    outfile <- paste(dir, output, sep="/")
    conn <- file(outfile)
    report <- tryCatch(
        txomeai_connect(url),
        error=function(cond)
        {
            message("Failed on txomeai_connect: ")
            message(cond, "\n")
            return(NULL)
        },
        warning=function(cond)
        {
            message("Warning on txomeai_connect: ")
            message(cond, "\n")
            return(NULL)
        }
    )
    sink(conn, append=TRUE)
    sink(conn, append=TRUE, type="message")
    if(is.null(report))
    {
        sink()
        sink(type="message")
        close(conn)
        message("Connect failed without error or a warning.")
        return(FALSE)
    }

    tables <- tryCatch(
        build_ls(report),
        error=function(cond)
        {
            message("Failed on txome_ls:")
            message(cond, "\n")
            return(NULL)
        },
        warning=function(cond)
        {
            message("Warnings on build_ls: ")
            message(cond, "\n")
            return(NULL)
        }
    )
    # Remove svg assets from test
    tables <- tables[key != "assets",]
    if(is.null(tables) | length(tables) == 0)
    {
        if(length(tables) == 0)
        {
            message("No tables were returned.")
        }
        sink()
        sink(type="message")
        close(conn)
        return(FALSE)
    }
    all_passed <- TRUE
    for(i in seq_len(length(tables$key)))
    {
        all_passed <- all_passed & tryCatch(
            {
                message(paste("Start testing table: ", tables[i,], "\n"))
                f <- txomeai_get(tables[i,], report)
                TRUE
            },
            error=function(cond)
            {
                message(paste("txomeai_get failed on: ", tables[i,], "\n"))
                message("Error:")
                message(cond, "\n")
                return(FALSE)
            },
            warning=function(cond)
            {
                message(paste("Warning during processing table: ", tables[i,], "\n"))
                message("Warning: ")
                message(cond, "\n")
                return(FALSE)
            },
            finally=message(paste("Finish testing table: ", tables[i,], "\n"))
        )
    }
    sink()
    sink(type="message")
    close(conn)
    return(all_passed)
}
