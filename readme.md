The aim of this project is to make an exploratory analysis of common questions and discussions in spanish reddit subs, and use that information to make clusters of articles that will be pubished and/or updated in one of my niche domains.

The project is also made (partially) public aiming to make it part of my portfolio.

## Reddit data

Even though Reddit API is limited now, it's possible to make a practically indistinguishable-from-human scraper using Puppetteer.

In this case I wanted to avoid the messy world of scrapping. There was Pushift for this, it seems to kinda abandoned given the last changes in the Reddit API.

I found [someone who provides massive torrents](https://www.reddit.com/r/pushshift/comments/11ef9if/separate_dump_files_for_the_top_20k_subreddits/) for this purpose. You basically need to select what subs do you want to download inside the UI of your torrent client..

## Handling massive zst files

Typically I wouldn't mind too much and put everything on RAM thanks to my workstation having more than enough, but at the moment I don't have linux installed there, and ZST seems to be a PITA in Windows + JS for some reason.

Tried to debug it a bit but it was tiresome, so I ended up using my x260 laptop for that. The problem is that this machine comes with a mobile i5 and 8gb of Ram, so I can't just load everything into Ram.

For this purpose I just decompress everything (in parallel) to the `./decompress` folder using `zstd`, and then stream the resulting files into SQLite.

### ¿Why SQLite and not some OLAP DB, like DuckDB or some other more mature?

The files are massive, but once you load everything into a DB, it's ok for the most part.

Also, I want to explore the data with several tools like GUI DB IDEs, or Knime, and sadly the support for DuckDB is still kinda buggy, given the test I made.

In case I really really need OLAP, DuckDB allows to use SQLite as source of data.

Other OLAP solutions require to set up servers and such, and right now I have too much in my table to handle yet another server, I want to avoid this for now.

