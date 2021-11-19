#library(httr)
#library(urltools)
#library(jsonlite)
#library(getPass)
#library(data.table)
#library(magick)

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
    asset$url$path = sub("json", "assets", asset$url$path, fixed=T)
    return(download_file(asset, filename))
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

#' Determines if a sample column is meta data or external data
#'
#' @param txomeai the connection object
#' @param sample_col the vector of the sample.csv column values
#' @return True if the values are external data, false if they are meta data
is_sample_meta_data = function(txomeai, sample_col)
{
    if(length(sample_col) == 0)
    {
        return(FALSE)
    }
    if(length(grep(txomeai$CAS, as.character(sample_col[1]), fixed=TRUE)) > 0)
    {
        return(FALSE)
    }
    return(TRUE)
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

#' Handle the downloaded json.gz file
#'
#' @param txomeai the txomeai object to handle memory caching
#' @param input the *.json.gz filepath
#' @param table the table name to extract
#' @return a list containing metadata, raw data, and processes data
process_data_json = function(txomeai, input, table)
{
    sub = jsonlite::fromJSON(input)
    if(sub$data_type[1] == "table")
    {
        header = unlist(sub$data$header)
        types = unlist(sub$data$types)
        if(length(sub$data$rows) == 0)
        {
            sub$data = data.table(matrix(ncol=length(header), nrow=0))
        }
        else
        {
            sub$data = data.table(matrix(sub$data$rows,ncol=length(header), nrow=length(sub$data$rows[,1])))
        }
        numeric_cols = c()
        for(i in 1:length(header))
        {
            if(header[i] == "")
            {
                header[i] = paste0("V", i)
            }
            if(types[i] == "numeric")
            {
                numeric_cols = c(numeric_cols, header[i])
            }
        }
        colnames(sub$data) = header
        if(length(numeric_cols) > 0)
        {
            sub$data[, (numeric_cols) := lapply(.SD, as.numeric), .SDcols = numeric_cols]
        }
        return(sub)
    }
    return(sub)
}

#' Download and return a raw data object
#'
#' @param txomeai the connection object
#' @param ls_row the row from txomeai$ls to download
#' @return the raw list parsed from the json data
fetch = function(txomeai, ls_row)
{
    if(is.na(ls_row$key))
    {
        return(get_sample_meta(txomeai, ls_row$name))
    }
    file = paste(ls_row$name, "json.gz", sep=".")
    r = download_file(txomeai, file, ls_row$key)
    if(r$status_code == 200)
    {
        message("Downloaded successfully: ", file)
        return(process_data_json(txomeai, r$path, table))
    }
    else
    {
        message("Failed to download: ", r$status_code)
        return(NULL)
    }
    return(r)
}

#' Use to return sample table raw results
#'
#' @param txomeai the connected report object
#' @param table the name of the table to download.
#' @return NULL or a data.table with sample meta data
get_sample_meta = function(txomeai, table)
{
    for(i in 1:length(txomeai$sample))
    {
        if(table %in% colnames(txomeai$sample[[i]]))
        {
            df = txomeai$sample[[i]][,c("sample", table)]
            return(df)
        }
    }
    return(NULL)
}

#' Use to get the CAS and Instance ID from url
#'
#' @param url_path A vector of the urls path elements
#' @return A list containing "cas" and "instance"
get_cas_and_instance = function(url_path) 
{
    to_return = list(cas=NULL, instance=NULL)
    for(p in url_path)
    {
        if(grepl("\\w+-\\w+-\\w+-\\w+-\\w+", p, perl=TRUE))
        {
            to_return$cas = p
        }
        if(grepl("\\d+-\\d+-\\d+T\\d+", p, perl=TRUE))
        {
            to_return$instance = p
        }
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
        txomeai$data = read.csv(response$path, header=T)
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
    return(txomeai_update_glossary(txomeai))
}

test_txomeai = function(url, dir=".", output="Results.Rhistory")
{
    outfile = paste(dir, output, sep="/")
    conn = file(outfile)
    report = tryCatch(
        txomeai_connect(url, dir=dir),
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
    for(i in 1:length(tables$key))
    {
        all_passed = all_passed & tryCatch(
            {
                message(paste("Start testing table: ", tables[i,], "\n"))
                f = txomeai_get(report, tables[i,])
                TRUE
            },
            error=function(cond)
            {
                message(paste("txomeai_getn failed on: ", tables[i,], "\n"))
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
#' @param dir (OPTIONAL) The directory to save data to.
#' @return The constructed connection object
txomeai_connect = function(url, dir=".") 
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
    txomeai$dir = init_dir(dir, workingCAS, workingInstance)
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
        txomeai$data = read.csv(response$path, header=T)
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
    return(txomeai_update_glossary(txomeai))
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
            for(i in 1:length(s[,1]))
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
            for(i in 1:length(m$tableName))
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
    tables$row = 1:length(tables$key)
    return(unique(tables))
}

#' Use to build our file index for a report
#'
#' @param txomeai The report connection object
#' @return the connection object with the built index
txomeai_update_glossary = function(txomeai)
{
    txomeai$meta = vector("list", length(txomeai$data$app))
    txomeai$sample = vector("list", length(txomeai$data$app))
    for(i in 1:length(txomeai$data$app))
    {
        app = txomeai$data[i, "app"]
        r = download_file(txomeai, paste(app, "meta.csv", sep="."))
        if(r$status_code == 200 & file.info(r$path)$size > 0)
        {
            txomeai$meta[[i]] = read.csv(r$path, header=T)
        }
        else 
        {
            txomeai$meta[[i]] = data.table()
        }

        r = download_file(txomeai, paste(app, "sample.csv", sep="."))
        if(r$status_code == 200 & file.info(r$path)$size > 0)
        {
            txomeai$sample[[i]] = read.csv(r$path, header=T)
        }
        else 
        {
            txomeai$sample[[i]] = data.table()
        }
    }
    names(txomeai$meta) = txomeai$data$app
    names(txomeai$sample) = txomeai$data$app
    txomeai$ls = txomeai_ls(txomeai)
    return(txomeai)
}

#' Used to collect the raw message data parsed into R list
#'
#' @param txomeai The report connection object
#' @param ls_row A row from the txomeai_ls datatable
#' @return a list containing the raws data
txomeai_get = function(txomeai, ls_row)
{
    if(!is.data.table(ls_row))
    {
        message("Error: Unexpected subset data")
    }
    if(!all(c("key", "name", "description") %in% colnames(ls_row)))
    {
        message("Error: Expected columns not found in subset data")
    }
    return(fetch(txomeai, ls_row))
}

#' Used for building a single result from multiple queries
#'
#' @param txomeai the report connection object
#' @param tableName the name from the ls table
#' @return a data.table with all results
txomeai_get_all = function(txomeai, tableName)
{
    to_return = NULL
    header = NULL
    types = NULL
    #sub = subset(txomeai$ls, name == tableName)
    # name must be initilized to pass R CMD check 
    name = NULL
    sub = txomeai$ls[name == tableName, ]
    if(length(sub$key) == 0)
    {
        message(sprintf("Error: no tables found with name %s\n", name))
        return()
    }
    if(length(sub$key) == 1)
    {
        return(txomeai_get(txomeai, sub))
    }
    for(i in 1:length(sub$key))
    {
        raw = txomeai_get(txomeai, sub[i,])
        if(raw$data_type == "table")
        {
            old_head = colnames(raw$data)
            raw$data = cbind(raw$data, sub[i,"key"])
            colnames(raw$data) = c(old_head, "key")
            if(is.null(to_return))
            {
                to_return = raw$data
                header = colnames(to_return)
                types = sapply(to_return, class)
            }
            else
            {
                t = sapply(raw$data, class)
                if(!all(t == "character"))
                {
                    types = t
                }
                to_return = rbind(to_return, raw$data)
            }
        }
        else
        {
            if(is.null(to_return))
            {
                to_return = list()
                to_return[[i]] = raw$data
            }
            else
            {
                to_return[[i]] = raw$data
            }
        }

    }
    if(is.data.table(to_return))
    {
        colnames(to_return) = header
        if(!all(types == "character"))
        {
            num_cols = header[types != "character"]
            to_return[, (num_cols) := lapply(.SD, as.numeric), .SDcols = num_cols]
        }
    }
    else
    {
        names(to_return) = sub$key
    }
    return(to_return)
}


#' Use to display a svg file
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
