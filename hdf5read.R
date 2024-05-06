library(rhdf5)


setwd("C:/Users/vickyz/Documents/Booth Classes/Spring 2024/BIG DATA/FINAL PROJECT")
# 
# file_list = list.files(path = "C:/Users/vickyz/Documents/Booth Classes/Spring 2024/BIG DATA/FINAL PROJECT/Song Data", pattern = NULL, all.files = FALSE,
#                        full.names = FALSE, recursive = FALSE,
#                        ignore.case = TRUE, include.dirs = FALSE, no.. = TRUE)
# 
# 
# #data frame definition
# song_matrix = data.frame(
#   duration = numeric(0),
#   key = numeric(0),
#   loudness = numeric(0),
#   tempo = numeric(0),
#   time_signature = numeric(0),
#   artist_hotttnesss = numeric(0),
#   words_artist = numeric(0),
#   length_artist = numeric(0),
#   words_album = numeric(0),
#   length_album = numeric(0),
#   words_title = numeric(0),
#   length_title = numeric(0),
#   song_hotttnesss = numeric(0),
#   stringsAsFactors = FALSE
# )
# 
# #define number of loops based on list length
# length_list = length(file_list)
# 
# #define master genre list
# genre_list <- character(0)

## ---THIS CODE WAS RAN PRIOR AND EXPORTED TO CSV TO REDUCE COMPUTATION TIME------

# #develop master list of artist terms
# for(i in 1:length_list){
# artist_terms = h5read(file = file_list[i], name = "metadata")
# genres = artist_terms$artist_terms
# 
# genre_list = union(genre_list,genres)
# h5closeAll()
# }

## Write Genre Master List Data to CSV file given long run time
#write.csv(genre_list, file = "song_genres.csv", row.names = FALSE)

##-------------------------------------------------------------------------------

# Read csv file created with all song genres
genre_list = read.csv("C:/Users/vickyz/Documents/Booth Classes/Spring 2024/BIG DATA/FINAL PROJECT/song_genres.csv", header = TRUE)
genre_list <- genre_list$x
# #define master location list
# loc_list <- character(0)
# 
# #develop master list of artist locations
# for(i in 1:length_list){
#   artist_loc = h5read(file = file_list[i], name = "metadata/songs")
#   location = artist_loc$artist_location
#   
#   loc_list = union(loc_list,location)
# }
 
# #update dataframe to include master word list
# for (genre in genre_list) {
#   song_matrix[[genre]] <- integer(nrow(song_matrix))  # Creating an integer column per genre
# }
# 
# library(parallel)
# 
# # Detect the number of cores
# no_cores <- detectCores() - 1  # Leave one core free for system processes
# 
# # Create a cluster
# cl <- makeCluster(no_cores)
# 
# 
#   #populate dataframe with genre info
#   for (i in 1:length_list) {
#     metadata = h5read(file = file_list[i], name = "metadata")
#     for (genre in metadata$artist_terms) {
#       if (genre %in% genre_list) {
#         song_matrix[i, genre] <- 1
#         
#       }
#     } 
#     if (i %% 100 == 0) {  # Print every 100 iterations
#       cat("Processing genre", i, ":", genre_list[i], "\n")
#     }
#   }
#   

# #------------------------------------------------------------------------------
# 
# if (!requireNamespace("foreach", quietly = TRUE)) {
#   install.packages("foreach")
# }
# if (!requireNamespace("doParallel", quietly = TRUE)) {
#   install.packages("doParallel")
# }
# 
# library(foreach)
# library(doParallel)
# 
# no_cores <- detectCores() - 1  # Leave one core free
# cl <- makeCluster(no_cores)
# registerDoParallel(cl)
# 
# results <- foreach(i = 1:length_list, .packages = c("rhdf5")) %dopar% {
#   # Read metadata
#   metadata = h5read(file = file_list[i], name = "metadata")
#   
#   # Create a vector to hold genre assignments for this file
#   genres <- integer(length(genre_list))
#   names(genres) <- genre_list
#   
#   # Populate genre information
#   for (genre in metadata$artist_terms) {
#     if (genre %in% genre_list) {
#       genres[genre] <- 1
#     }
#   }
#   
#   # Return genres vector for this iteration
#   return(genres)
# }
# 
# ## Write Genre Master List Data to CSV file given long run time
# write.csv(song_matrix, file = "song_matrix.csv", row.names = FALSE)
# 
# # Combine results into a matrix
# song_matrix <- do.call(rbind, results)
# 
# stopCluster(cl)


#---------------------------------------------------------------------------

# Read csv file created with all song genres
#song_matrix = read.csv("C:/Users/vickyz/Documents/Booth Classes/Spring 2024/BIG DATA/FINAL PROJECT/song_matrix.csv", header = TRUE)
# 
# #Specify columns for second dataframe update
# columns_to_update <- c("duration", "key", "loudness", "tempo", "time_signature",
#                        "artist_hotttnesss", "words_artist", "length_artist",
#                        "words_album", "length_album", "words_title",
#                        "length_title", "song_hotttnesss")
# 
# 
# #update dataframe with analysis info
# for(i in 1:length_list){
# 
# analysis = h5read(file = file_list[i], name = "analysis/songs")
# metadata_songs = h5read(file = file_list[i], name = "metadata/songs")
# 
# art_words = sapply(metadata_songs$artist_name, function(x) length(strsplit(x, "\\s+")[[1]]))
# alb_words = sapply(metadata_songs$release, function(x) length(strsplit(x, "\\s+")[[1]]))
# song_words = sapply(metadata_songs$title, function(x) length(strsplit(x, "\\s+")[[1]]))
# 
# update_data <- c(
#   analysis$duration,
#   analysis$key,
#   analysis$loudness,
#   analysis$tempo,
#   analysis$time_signature,
#   metadata_songs$artist_hotttnesss,
#   art_words,
#   nchar(metadata_songs$artist_name),
#   alb_words,
#   nchar(metadata_songs$release),
#   song_words,
#   nchar(metadata_songs$title),
#   metadata_songs$song_hotttnesss)
# 
# # Update the dataframe for the specific columns only
# song_matrix[i, columns_to_update] <- update_data
# 
# h5closeAll()
# 
#   if (i %% 100 == 0) {  # Print every 100 iterations
#   cat("Processing Song:", i)
#   }
# }
# stopCluster(cl)
# 
# # # ## Write Song Master List Data to CSV file given long run time
# write.csv(song_matrix, file = "song_matrix.csv", row.names = FALSE)

# Read csv file created with all song data
song_matrix = read.csv("C:/Users/vickyz/Documents/Booth Classes/Spring 2024/BIG DATA/FINAL PROJECT/song_matrix.csv", header = TRUE)

#####--------SECTION BREAK AFTER IMPORTING DATA AND CONVERTING TO CSV FOR ACCESSIBILITY-----------------


### ------------------- SONG LIST & TRACK ID TEST------------------------------------
# song_list = data.frame(
#   title = numeric(0),
#   artist = numeric(0),
#   ID = numeric(0),
#   stringsAsFactors = FALSE
# )
# 
# update_col <- c("title", "artist","ID")
# for(i in 1:length_list){
#   
# metadata_songs = h5read(file = file_list[i], name = "metadata/songs")
# 
# update_list = c(
#   metadata_songs$title,
#   metadata_songs$artist_name,
#   metadata_songs$song_id
# )
# 
# song_list[i,update_col] = update_list
# 
# h5closeAll()
# }

# Read csv file created with all song data
song_list = read.csv("C:/Users/vickyz/Documents/Booth Classes/Spring 2024/BIG DATA/FINAL PROJECT/song_list.csv", header = TRUE)

###------------ACCESSING SPOTIFY API AND DOWNLOADING SONG PLAY DATA FOR REGRESSION-------------


library(httr)
library(jsonlite)

##Define Spotify API access function
## Function to get Spotify access token
# get_spotify_access_token <- function(client_id, client_secret) {
#   endpoint <- "https://accounts.spotify.com/api/token"
#   credentials <- paste(client_id, client_secret, sep = ":")
#   encoded_credentials <- base64enc::base64encode(charToRaw(credentials))
#   
#   response <- POST(endpoint, authenticate(client_id, client_secret),
#                    body = list(grant_type = "client_credentials"),
#                    encode = "form",
#                    add_headers(Authorization = paste("Basic", encoded_credentials)))
#   access_token <- content(response)$access_token
#   print(paste("Access Token Retrieved:", access_token))  # Debug print
#   return(access_token)
# }

## Example call to retrieve access token
# client_id <- "cb24a13f419a4a35a7c3959f8a2d1429"
# client_secret <- "df4a4f90e2924546b226a89a5a18a073"
# access_token <- get_spotify_access_token(client_id, client_secret)
access_token = "BQAgxEZoU-FI3AOjTUmvBxk8BnxYSg_175AJz3-MGqpgmWPARqamojErJVGy-w_azCZ1qNLVADNLBsaCFnuSF-5fMFYNKqedxsj0S1szSJVAdw2WnS8"

#Function for Searching for Spotify API Track ID Based on Song Title
get_spotify_track_id <- function(song_title, artist_name, access_token) {
  # Construct the search query
  query <- URLencode(sprintf("track:%s artist:%s", song_title, artist_name), reserved = TRUE)
  endpoint <- paste0("https://api.spotify.com/v1/search?q=", query, "&type=track&limit=1")
  
  retry <- TRUE
  attempts <- 0
  
  while (retry && attempts < 5) {
    # Make the API call
    response <- GET(endpoint, add_headers(Authorization = paste("Bearer", access_token)))
    status <- status_code(response)
    
    # Check the response status and handle errors
    if (status == 200) {
      retry <- FALSE  # Exit loop on success
    } else {
      print(paste("Failed to fetch track ID for:", song_title, "Status code:", status))
      if (status == 429) {
        # Extract retry-after header and wait if present
        retry_after <- as.numeric(headers(response)[["retry-after"]])
        if (!is.na(retry_after)) {
          print(paste("Retrying after", retry_after, "seconds"))
          Sys.sleep(retry_after)
          attempts <- attempts + 1
        } else {
          retry <- FALSE  # No retry-after header, stop retrying
        }
      } else {
        retry <- FALSE  # Non-retryable status code
      }
    }
  }
  
  if (status != 200) {
    return(NA)  # Return NA if the API call fails after retries
  }
  
  # Parse the response content
  track_data <- fromJSON(rawToChar(response$content))
  
  # Check if there are any tracks returned
  if (length(track_data$tracks$items) > 0) {
    track_id <- track_data$tracks$items[[1]]$id
    return(track_id)
  } else {
    print("No tracks found for the query.")
    return(NA)  # Return NA if no tracks are found
  }
}


## Function for retrieving Song Data for each song
get_track_details <- function(track_id, access_token) {
  endpoint <- paste0("https://api.spotify.com/v1/tracks/", track_id)
  response <- GET(endpoint, add_headers(Authorization = paste("Bearer", access_token)))
  track_details <- fromJSON(rawToChar(response$content))
  return(track_details)
}



##Script to actually run through and pull data from Spotify API
client_id <- "cb24a13f419a4a35a7c3959f8a2d1429"
client_secret <- "df4a4f90e2924546b226a89a5a18a073"
access_token <- get_spotify_access_token(client_id, client_secret)

# Assume song_list is a dataframe with columns for song titles and artist names
# Ensure your `song_list` is correctly formatted and populated

# Initialize an empty list to collect results
results_list <- list()


# Loop over a subset of song_list
for (i in 1135:length_list) {
  song_title <- song_list[i, 1]  # Assuming first column is song titles
  artist_name <- song_list[i, 2]  # Assuming second column is artist names
  
  track_id <- get_spotify_track_id(song_title, artist_name, access_token)
  if (!is.na(track_id) && length(track_id) > 0) {  # Check that track_id is not NA and has length
    track_details <- get_track_details(track_id, access_token)
    results_list[[i]] <- list(songtitle = song_title, pop = track_details$popularity)
  } else {
    results_list[[i]] <- list(songtitle = song_title, pop = NA)
  }
  if (i %% 10 == 0) {  # Print every 10 iterations
    cat("Processing Song:", i)
    }
}

# Convert the list of results to a dataframe
pop_list <- do.call(rbind, lapply(results_list, as.data.frame, stringsAsFactors = FALSE))


## LETS START DATA ANALYSIS AND REGRESSION



/analysis/songs
duration #number
key #number
loudness #number
tempo #number
time_signature #number

/metadata
artist_terms #regress each genre from total genre list

/metadata/songs
artist_hotttnesss #number
## Data missing for songs - artist_location #create list of locations
artist_name (consider words used in name) #regress each word from total word list
artist_name (# of words) #number
length(artist_name) #number
release (consider words used in album name) #regress each word from total word list
release (# of words) #number
length(release) #number
**song_hotttnesss ##This is what we regress against
title (consider words used in song name) #regress each word from total word list
title (# of words) #number
length(title) #number

