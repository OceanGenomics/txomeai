# API Interface Functions

#' Download an API file
#' 
#' @param filename the filename of the file to download.
#' @param key a string that uniquely identifies the correct file. 
#' @return a list with $status_code and $path to downloaded file.
download_file = function(txomeai, filename, key="", overwrite=FALSE)
{
    downloadURL = txomeai$url
    downloadURL$path = paste(downloadURL$path, filename, sep="/")
    key_dir = txomeai$dir
    if(!is.na(key) && nchar(key) > 0)
    {
        key_dir = file.path(txomeai$dir, key)
        downloadURL$path = paste(txomeai$url$path, key, filename, sep="/")
    }
    if(!dir.exists(key_dir))
    {
        dir.create(key_dir)
    }
    outfile = file.path(key_dir, filename)
    resp = NULL
    if(!file.exists(outfile) || overwrite)
    {
        r = httr::GET(urltools::url_compose(downloadURL), httr::write_disk(outfile, overwrite=TRUE))
        if(r$status_code != 200 && file.exists(outfile))
        {
            file.remove(outfile)
        }
        resp = list(status_code=r$status_code, path=outfile)
    }
    else
    {
        resp = list(status_code=200, path=outfile)
    }
    return(resp)
}

#' Download a SVG file
#'
#' @param txomeai the connection object
#' @param filename the filename of the file to download.
#' @return a list with $status_code and $path to downloaded file.
download_asset = function(txomeai, filename)
{
    asset = txomeai
    asset$url$path = sub("json", "assets", asset$url$path, fixed=TRUE)
    return(download_file(asset, filename))
}

#' Used to collect the raw message data parsed into R list
#'
#' @param name The name column value from the ls table
#' @param key The key column value from the ls table
#' @param txomeai The report connection object
#' @param ls_row A row from the txomeai_ls datatable
#' @return a list containing the raws data
fetch = function(name, key, txomeai)
{
    if(is.na(key))
    {
        return(get_sample_meta(txomeai, name))
    }
    file = paste(name, "json.gz", sep=".")
    r = download_file(txomeai, file, key)
    if(r$status_code == 200)
    {
        sub = jsonlite::fromJSON(r$path)
        message("Downloaded successfully: ", file)
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
#' @param txomeai the connection object
#' @return true if connected, false otherwise
test_auth = function(txomeai)
{
    list_cas = txomeai$url
    list_cas$path = "api/accounts/current"
    resp = httr::GET(urltools::url_compose(list_cas))
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
init_dir = function(dir, cas, inst)
{
    dir = gsub("/$|\\\\$", "", dir, perl=TRUE)
    if(!dir.exists(dir))
    {
        dir.create(dir)
    }
    cas_dir = file.path(dir, cas)
    if(!dir.exists(cas_dir))
    {
        dir.create(cas_dir)
    }
    inst_dir = file.path(cas_dir, inst)
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
get_sample_meta = function(txomeai, table)
{
    col_index = vapply(txomeai$sample, FUN=function(x){return(table %in% colnames(x));}, FUN.VALUE=TRUE)
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
get_cas_and_instance = function(url_path) 
{
    to_return = list(cas=NULL, instance=NULL)
    cas_i = grep("\\w+-\\w+-\\w+-\\w+-\\w+", url_path, perl=TRUE)
    inst_i = grep("\\d+-\\d+-\\d+T\\d+", url_path, perl=TRUE)
    if(cas_i > 0)
    {
        to_return$cas = url_path[cas_i]
    } 
    if(inst_i > 0)
    {
        to_return$instance = url_path[inst_i]
    }
    return(to_return)
}

#' Contruct the connection list object to the local test server
#'
#' @param url The report URL to connect to
#' @param dir (OPTIONAL) The directory to save data to.
#' @return The constructed connection object
local_connect = function(url, dir=".") 
{
    txomeai = list()
    # Validate and construct URL
    txomeai$url = urltools::url_parse(url)
    txomeai$CAS = ""
    txomeai$instance = ""

    parts = unlist(strsplit(txomeai$url$path, "/"))
    workingCAS = parts[2]
    workingInstance = parts[3]
    txomeai$CAS = workingCAS
    txomeai$instance = workingInstance
    txomeai$dir = init_dir(dir, workingCAS, workingInstance)
    # Test that the dir has been setup appropriately
    if(file.access(txomeai$dir, 0) != 0 || file.access(txomeai$dir, 2) != 0 || file.access(txomeai$dir, 4) != 0)
    {
        message("Insufficent access to working directory:", txomeai$dir)
        return()
    }
    parts[length(parts)] = "json"
    txomeai$url$path = paste(parts, collapse="/")
    # Check for API data
    response = download_file(txomeai, "data.csv")

    if(response$status_code == 200)
    {
        txomeai$data = read.csv(response$path, header=TRUE)
    }
    else if (response$status_code == 404) 
    {
        message("API data is not available for this report. If it's an older report, re-run to generate API data.")
        return()
    }
    else 
    {
        message("Query failed: ", response$status_code, "\n")
        return()
    }
    return(update_glossary(txomeai))
}

#' Use to build our file index for a report
#'
#' @param txomeai The report connection object
#' @return the connection object with the built index
update_glossary = function(txomeai)
{
    txomeai$meta = vector("list", length(txomeai$data$app))
    txomeai$sample = vector("list", length(txomeai$data$app))
    for(i in seq_len(length(txomeai$data$app)))
    {
        app = txomeai$data[i, "app"]
        r = download_file(txomeai, paste(app, "meta.csv", sep="."))
        if(r$status_code == 200 & file.info(r$path)$size > 0)
        {
            txomeai$meta[[i]] = read.csv(r$path, header=TRUE)
        }
        else 
        {
            txomeai$meta[[i]] = data.frame()
        }

        r = download_file(txomeai, paste(app, "sample.csv", sep="."))
        if(r$status_code == 200 & file.info(r$path)$size > 0)
        {
            txomeai$sample[[i]] = read.csv(r$path, header=TRUE)
        }
        else 
        {
            txomeai$sample[[i]] = data.frame()
        }
    }
    names(txomeai$meta) = txomeai$data$app
    names(txomeai$sample) = txomeai$data$app
    txomeai$ls = txomeai_ls(txomeai)
    return(txomeai)
}

#' Uses to test the API
#'
#' @param url The url to connect to
#' @param output The file to write results to
#' @return the connection object with the built index
test_txomeai = function(url, output="Results.Rhistory")
{
    key = NULL
    outfile = paste(dir, output, sep="/")
    conn = file(outfile)
    report = tryCatch(
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

    tables = tryCatch(
        txomeai_ls(report),
        error=function(cond)
        {
            message("Failed on txome_ls:")
            message(cond, "\n")
            return(NULL)
        },
        warning=function(cond)
        {
            message("Warnings on txomeai_ls: ")
            message(cond, "\n")
            return(NULL)
        }
    )
    # Remove svg assets from test
    tables = tables[key != "assets",]
    if(is.null(tables) | length(tables) == 0)
    {
        if(length(tables) == 0)
        {
            message("No tables were returned from txomeai_ls.")
        }
        sink()
        sink(type="message")
        close(conn)
        return(FALSE)
    }
    all_passed = TRUE
    for(i in seq_len(length(tables$key)))
    {
        all_passed = all_passed & tryCatch(
            {
                message(paste("Start testing table: ", tables[i,], "\n"))
                f = txomeai_get(tables[i,], report)
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

#
# User functions
#

#' Used to login, only use this function if you have a connection object and authentication is failing
#'
#' @param txomeai The connection object
#' @return True if login was successful, false otherwsie.
txomeai_login = function(txomeai)
{
    login = txomeai$url
    login$path = "api/accounts/password-login"
    resp = httr::POST(urltools::url_compose(login), body=list(username=readline("Enter username: "), password=getPass::getPass("Enter password: ")), encode="json")
    if(resp$status_code == 200)
    {
        return(TRUE)
    }
    else
    {
        message("Failed to login with http code: ", resp$status_code)
        return(FALSE)
    }
}

#' Contruct the connection list object and download or read the glossary
#'
#' @param url The report URL to connect to
#' @return The constructed connection object
txomeai_connect = function(url) 
{
    txomeai = list()
    # Validate and construct URL
    txomeai$url = urltools::url_parse(url)
    txomeai$CAS = ""
    txomeai$instance = ""
    if(!grepl("oceangenomics.com", txomeai$url$domain, fixed=TRUE) & !grepl("transcriptome.ai", txomeai$url$domain, fixed=TRUE))
    {
        message("Error: URL destination is unexpected.")
        return()
    }

    parts = unlist(strsplit(txomeai$url$path, "/"))
    if(length(parts) < 6) 
    {
        message("Error: URL path is unexpected.")
        return()
    }
    meta = get_cas_and_instance(parts)
    if(is.null(meta$cas) || is.null(meta$instance))
    {
        message("Error: Expected elements not found in path.")
        return()
    }
    workingCAS = meta$cas
    workingInstance = meta$instance
    txomeai$CAS = workingCAS
    txomeai$instance = workingInstance
    txomeai$dir = init_dir(BiocFileCache::bfccache(), workingCAS, workingInstance)
    # Test that the dir has been setup appropriately
    if(file.access(txomeai$dir, 0) != 0 || file.access(txomeai$dir, 2) != 0 || file.access(txomeai$dir, 4) != 0)
    {
        message("Insufficent access to working directory:", txomeai$dir)
        return()
    }
    txomeai$url$path = paste("api", "pipeline-output", workingCAS, workingInstance, "report", "json", sep="/")

    if(!test_auth(txomeai))
    {        
        if(!txomeai_login(txomeai))
        {
            message("Login failed\n")
            return()
        }
    }
    
    # Check for API data
    response = download_file(txomeai, "data.csv")

    if(response$status_code == 200)
    {
        txomeai$data = read.csv(response$path, header=TRUE)
    }
    else if (response$status_code == 404) 
    {
        message("API data is not available for this report. If it's an older report, re-run to generate API data.")
        return()
    }
    else 
    {
        message("Query failed: ", response$status_code, "\n")
        return()
    }
    return(update_glossary(txomeai))
}

#' Use to create a list of the available tables
#'
#' @param txomeai The report connection object
#' @return a data frame containing the available data
txomeai_ls = function(txomeai) 
{
    if(!is.null(txomeai$ls))
    {
        return(txomeai$ls)
    }
    table_header = c("key", "name", "description")
    tables = data.table(matrix(ncol=3,nrow=0))
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
                    tables = rbind(tables, list(s[i,"sample"], c, s[i,"sampleName"]))
                }
                else
                {
                    tables = rbind(tables, list(NA, c, "Meta data"))
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
                    tables = rbind(tables, list(m$stepName[i], m$tableName[i], "Step run against all samples"))
                } else if (m$stepName[i] == "assets") {
                    tables = rbind(tables, list(m$stepName[i], m$filename[i], "An image file"))
                } else {
                    tables = rbind(tables, list(m$stepName[i], m$tableName[i], sprintf("%s vs %s", m$set1[i], m$set2[i])))
                }
            }
        }
    }
    tables = unique(tables)
    colnames(tables) = table_header
    tables$row = seq_len(length(tables$key))
    return(unique(tables))
}

#' Used for collecting any data from the report
#'
#' @param txomeai the report connection object
#' @param tableName the name from the ls table
#' @param tableKey optionally provide the key value for a specific request
#' @return a data.table with all results
txomeai_get = function(txomeai, tableName, tableKey=NULL)
{
    to_return = NULL
    header = NULL
    types = NULL
    # name and row_count must be initilized to pass R CMD check 
    row_count = NULL
    name = NULL
    sub = NULL
    key = NULL
    # Error checking
    if(!(tableName %in% txomeai$ls$name))
    {
        message(sprintf("Error: table name '%s' not found.", tableName))
        return(NULL)
    }
    if(!is.null(tableKey) && !(tableKey %in% txomeai$ls$key))
    {
        message(sprintf("Error: table key '%s' not found.", tableKey))
        return(NULL)
    }

    if(is.null(tableKey))
    {
        sub = txomeai$ls[name == tableName, ]
    } else if (is.na(tableKey)) {
        sub = txomeai$ls[name == tableName & is.na(key), ]
    } else {
        sub = txomeai$ls[name == tableName & key == tableKey,]
    }

    if(nrow(sub) == 0) {
        message("Failed to find requested data.")
        return(NULL)
    }
    
    # Handle meta data and assets
    if(nrow(sub) == 1 && is.na(sub$key[[1]]))
    {
        return(data.table(fetch(sub$name[[1]], sub$key[[1]], txomeai)))
    } else if (nrow(sub) == 1 && sub$key[[1]] == "assets"){
        return(txomeai_display(txomeai, sub[1,]))
    }
    
    # Get raw list results for each row and add to data.table as column 'get'
    sub$get = apply(sub, FUN=function(x,r){return(fetch(x["name"], x["key"], r));}, MARGIN=1, txomeai)
    # Return results if data_type isn't table
    if(sub$get[[1]]$data_type != "table")
    {
        return(sub)
    }
    # Filter all empty table get results
    get_row_count = function(x){ return(nrow(x[[1]]$data$rows));}
    sub[,row_count := get_row_count(get), by=key]
    if(nrow(sub[row_count > 0,]) == 0)
    {
        to_return = data.table(matrix(ncol=length(sub$get[[1]]$data$header), nrow=0))
        colnames(to_return) = sub$get[[1]]$data$header
        return(to_return)
    }
    sub = sub[row_count > 0,]

    # Build the output data.table from the raw data rows
    to_return = data.table(do.call("rbind", lapply(sub$get, FUN=function(x){return(x$data$rows);})))
    # Build the key column for the output table
    key_col = rep(sub$key, sub$row_count)
    # Set column types
    num_cols = colnames(to_return)[sub$get[[1]]$data$types == "numeric"]
    to_return[, (num_cols) := lapply(.SD, as.numeric), .SDcols = num_cols]
    # Set column names
    colnames(to_return) = sub$get[[1]]$data$header
    # Add id column
    to_return$key = key_col
    return(to_return) 
}


#' Use to display a svg file
#'
#' @param txomeai the report connection object
#' @param ls_row the row from the ls table to display
#' @return path to the downloaded svg file if download successful, NULL otherwsie
txomeai_display = function(txomeai, ls_row)
{
    if(!all(colnames(txomeai$ls) %in% colnames(ls_row)))
    {
        print("Input row did not have the expected format")
        return()
    }
    r = download_asset(txomeai, ls_row$name)
    if(r$status_code != 200)
    {
        cat("Failed to download svg file with HTML code: ", r$status_code, "\n")
        return()
    }
    img = image_read(r$path)
    displayed = tryCatch(
        {
            image_display(img)
            TRUE
        },
        error=function(cond)
        {
            message("Error on image_display: ")
            message(cond, "\n")
            return(FALSE)
        },
        warning=function(cond)
        {
            message("Warnings on image_display: ")
            message(cond, "\n")
            return(TRUE)
        }
    )
    if(displayed)
    {
        return(r$path)
    }
    displayed = tryCatch(
        {
            image_browse(img)
            TRUE
        },
        error=function(cond)
        {
            message("Error on image_browse: ")
            message(cond, "\n")
            return(FALSE)
        },
        warning=function(cond)
        {
            message("Warnings on image_browse: ")
            message(cond, "\n")
            return(TRUE)
        }
    )
    return(r$path)
}
